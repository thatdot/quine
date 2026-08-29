package com.thatdot.quine.compiler.cypher

import java.util.concurrent.ConcurrentLinkedQueue

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

import org.apache.pekko.stream.scaladsl.{Flow, Keep, Sink}
import org.apache.pekko.stream.{KillSwitches, Materializer, UniqueKillSwitch}
import org.apache.pekko.util.Timeout

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import com.thatdot.common.logging.Log.LogConfig
import com.thatdot.quine.graph.MasterStream.SqResultsExecToken
import com.thatdot.quine.graph.StandingQueryPattern.MultipleValuesQueryPattern
import com.thatdot.quine.graph.behavior.MultipleValuesStandingQueryBehavior
import com.thatdot.quine.graph.cypher.Expr
import com.thatdot.quine.graph.{
  GraphService,
  PatternOrigin,
  QuineIdLongProvider,
  StandingQueryId,
  StandingQueryResult,
  defaultNamespaceId,
}
import com.thatdot.quine.model.QuineValue
import com.thatdot.quine.persistor.{EventEffectOrder, InMemoryPersistor}

/** What a MultipleValues standing query settles on, compared against the same query run as an ordinary one.
  *
  * A standing query and an ad-hoc query are two implementations of one thing: the rows a pattern matches against the
  * graph as it stands. One of them is incremental, distributed across nodes, and carries state through sleep; the
  * other walks the graph and answers. So the ad-hoc interpreter can serve as an oracle for the standing one, which
  * is worth far more than any expectation written by hand: it is not a restatement of what the code does, and it
  * does not have to be maintained as the implementation changes underneath it.
  *
  * Each case is run in both directions, because they exercise different code and are not equally likely to be
  * wrong: registering against a graph that already holds the data replays the node's current state into a freshly
  * created state, and registering first makes every match arrive as an event.
  */
class StandingQueryConvergenceOracle extends AnyFunSuite with BeforeAndAfterAll {

  implicit private val idProvider: QuineIdLongProvider = QuineIdLongProvider()
  implicit private val logConfig: LogConfig = LogConfig.permissive
  implicit private val timeout: Timeout = Timeout(10.seconds)

  private val graph: GraphService = Await.result(
    GraphService(
      "standing-query-convergence-oracle",
      effectOrder = EventEffectOrder.PersistorFirst,
      persistorMaker = InMemoryPersistor.persistorMaker,
      idProvider = idProvider,
    ),
    10.seconds,
  )

  implicit private def materializer: Materializer = graph.materializer

  /** How long a standing query is given to stop reporting before the run is called inconclusive.
    *
    * Generous on purpose: this is not a statement about how fast the query should be, only a bound past which a
    * silent partial answer would be worse than a failure. The whole suite shares one machine, and these cases are
    * the slowest thing on it.
    */
  private val QuiesceTimeoutMillis: Long = 120000

  private val QuiescePollMillis: Long = 10

  /** 50ms of quiet, once the reports already add up to the answer the case says the pattern has. */
  private val QuietSamplesOnceAnswered: Int = 5

  /** 250ms of quiet otherwise, which is what the largest case needs before it has finished reporting. */
  private val QuietSamplesWhileIncomplete: Int = 25

  override def afterAll(): Unit = {
    Await.result(graph.shutdown(), 20.seconds)
    ()
  }

  import StandingQueryConvergenceOracle.{Row, Settled}

  private def runAdHoc(queryText: String): Vector[Row] = {
    val running = queryCypherValues(queryText, defaultNamespaceId, cacheCompilation = false)(graph)
    val columns = running.columns.map(_.name)
    Await
      .result(running.results.runWith(Sink.seq), 20.seconds)
      .toVector
      .map { row =>
        columns
          .zip(row)
          .map { case (column, value) =>
            column -> Expr.toQuineValue(value).fold(throw _, identity)
          }
          .toMap
      }
  }

  /** What the standing query currently says it matches: its positive reports, less anything it has withdrawn. */
  private def foldToCurrentMatches(reported: Iterable[StandingQueryResult]): Vector[Row] = {
    val (positive, negative) = reported.toVector.partition(_.meta.isPositiveMatch)
    negative.map(_.data).foldLeft(positive.map(_.data)) { (remaining, withdrawn) =>
      remaining.indexOf(withdrawn) match {
        case -1 => remaining // withdrawing something never reported; the comparison below will show it
        case i => remaining.patch(i, Nil, 1)
      }
    }
  }

  /** Run the standing query until it stops changing, then report what it settled on.
    *
    * Quiescence is measured rather than waited out: what matters is that nothing more arrives, and a fixed sleep
    * either wastes time or is a race depending on the machine.
    */
  private def standingMatches(
    queryText: String,
    populate: (() => Unit) => Any,
    dataFirst: Boolean,
    expectedRows: Int,
  ): Settled = {
    val sqId = StandingQueryId.fresh()
    val reported = new ConcurrentLinkedQueue[StandingQueryResult]()
    val graphPattern = compileStandingQueryGraphPattern(queryText)(graph.idProvider, logConfig)
    val compiled = graphPattern.compiledMultipleValuesStandingQuery(graph.labelsProperty, graph.idProvider)

    // Let whatever has been reported stop changing. Quiescence is measured rather than waited out: what matters is
    // that nothing more arrives, and a fixed sleep either wastes time or is a race depending on the machine.
    //
    // How long quiet has to last depends on what the quiet means. Once the reports add up to the answer this case
    // says the pattern has, a short quiet is enough to say nothing more is coming, and that is the case nearly
    // every run takes. Until then it has to be generous, because "not finished" and "not going to" look identical
    // from here, and a case expecting no rows at all is never in the first situation, since its answer is what
    // it would hold before reporting anything.
    //
    // Giving up is recorded rather than ignored. A run that was still reporting when the deadline arrived has a
    // partial answer, which compared against the ad-hoc one looks exactly like a query that reported the wrong
    // rows, so the two are told apart here, where the difference is still known, rather than in the mismatch.
    var gaveUpWaiting = false
    def quiesce(target: Option[Int]): Unit = {
      def answered: Boolean =
        target.exists(rows => rows > 0 && foldToCurrentMatches(reported.asScala.toVector).size == rows)
      var stableFor = 0
      var lastSeen = -1
      var settled = false
      val deadline = System.currentTimeMillis + QuiesceTimeoutMillis
      while (!settled && System.currentTimeMillis < deadline) {
        Thread.sleep(QuiescePollMillis)
        val seen = reported.size
        if (seen == lastSeen) stableFor += 1 else { stableFor = 0; lastSeen = seen }
        settled = stableFor >= (if (answered) QuietSamplesOnceAnswered else QuietSamplesWhileIncomplete)
      }
      if (!settled) gaveUpWaiting = true
    }

    // The reports during population are not being compared against anything yet (in the one case that quiesces
    // mid-populate the answer is different at each pause), so there is no target to wait for there.
    if (dataFirst) { val _ = populate(() => quiesce(None)) }

    val outputs: Map[String, Sink[StandingQueryResult, UniqueKillSwitch]] = Map(
      "oracle" -> Flow[StandingQueryResult]
        .viaMat(KillSwitches.single)(Keep.right)
        .map { result =>
          reported.offer(result)
          SqResultsExecToken("oracle")
        }
        .to(Sink.ignore),
    )
    val standingQueries = graph.standingQueries(defaultNamespaceId).get
    standingQueries.createStandingQuery(
      s"oracle-${sqId.uuid}",
      MultipleValuesQueryPattern(compiled, includeCancellation = true, PatternOrigin.DirectSqV4),
      outputs,
      sqId = sqId,
    )
    Await.result(standingQueries.propagateStandingQueries(Some(4)), 20.seconds)

    if (!dataFirst) { val _ = populate(() => quiesce(None)) }

    try {
      quiesce(Some(expectedRows))
      val settled = reported.asScala.toVector
      val (positive, negative) = settled.partition(_.meta.isPositiveMatch)
      assert(
        !gaveUpWaiting,
        s"the standing query was still reporting after ${QuiesceTimeoutMillis / 1000} seconds, so what it settled " +
        s"on is not known: ${positive.size} matches and ${negative.size} withdrawals so far",
      )
      Settled(foldToCurrentMatches(settled), positive.size, negative.size)
    } finally {
      Await.result(standingQueries.cancelStandingQuery(sqId).get, 20.seconds)
      ()
    }
  }

  /** Compare as bags: a pattern reports one row per way it matches, so how many times a row appears is part of the
    * answer, and the order it appears in is not.
    */
  private def assertAgrees(actual: Settled, expected: Vector[Row], clue: String): Unit = {
    def counted(rows: Vector[Row]) = rows.groupBy(identity).view.mapValues(_.size).toMap
    // Whether the missing rows were never reported or were reported and then withdrawn is the whole difference
    // between a query that is behind and one that is wrong, and the counts are the only place it still shows.
    assert(
      counted(actual.rows) == counted(expected),
      s"$clue (${actual.matches} matches and ${actual.withdrawals} withdrawals reported)",
    )
    ()
  }

  /** Both directions of one case.
    *
    * Each direction gets its own corner of the graph, with `_X` in the setup and the query standing for a suffix
    * unique to that test, so that neither can see the other's data, and the number of rows either side should
    * produce is a fixed number this test can state. Without that, an oracle can agree with itself about nothing at all.
    *
    * @param expectedRows how many rows the pattern matches, which pins the oracle rather than the implementation
    */
  private def oracleCase(
    name: String,
    setup: String,
    standingQuery: String,
    expectedRows: Int,
    bothDirections: Boolean = true,
  ): Unit = {
    def scoped(text: String, variant: String): String = text.replace("_X", s"_$variant")

    def check(variant: String, dataFirst: Boolean): Unit = {
      val query = scoped(standingQuery, variant)
      val populate = (_: () => Unit) => runAdHoc(scoped(setup, variant)).size
      val actual = standingMatches(query, populate, dataFirst, expectedRows)
      val expected = runAdHoc(query)
      assert(expected.size == expectedRows, "the ad-hoc query did not match what this case says it should")
      assertAgrees(actual, expected, "the standing query did not settle on what the ad-hoc query answers")
    }

    test(s"$name, registered against data that is already there")(check("a", dataFirst = true))
    if (bothDirections) test(s"$name, registered before the data arrives")(check("b", dataFirst = false))
  }

  // Each case works in its own corner of the graph, with its own edge type and property keys, so that one case's
  // data is never matched by another's pattern, and both sides see the same rows.

  oracleCase(
    "a bound property on a single node",
    setup = "CREATE (:A1_X {p1_X: 'x'}), (:A1_X {p1_X: 'y'}), (:A1_X {q1_X: 'ignored'})",
    standingQuery = "MATCH (n) WHERE n.p1_X IS NOT NULL RETURN n.p1_X",
    expectedRows = 2,
  )

  oracleCase(
    "a value carried across one edge",
    setup = """CREATE (a {p2_X: 'a'})-[:e2_X]->(b {q2_X: 'b'}),
              |       (c {p2_X: 'c'})-[:e2_X]->(d {q2_X: 'd'})""".stripMargin,
    standingQuery = "MATCH (n)-[:e2_X]->(m) WHERE n.p2_X IS NOT NULL AND m.q2_X IS NOT NULL RETURN n.p2_X, m.q2_X",
    expectedRows = 2,
  )

  oracleCase(
    "one node matching along several edges, so a row per edge",
    setup = """CREATE (hub {p3_X: 'hub'})-[:e3_X]->({q3_X: '1'}),
              |       (hub)-[:e3_X]->({q3_X: '2'}),
              |       (hub)-[:e3_X]->({q3_X: '3'})""".stripMargin,
    standingQuery = "MATCH (n)-[:e3_X]->(m) WHERE n.p3_X IS NOT NULL AND m.q3_X IS NOT NULL RETURN n.p3_X, m.q3_X",
    expectedRows = 3,
  )

  oracleCase(
    "a chain of two edges",
    setup = """CREATE (a {p4_X: 'a'})-[:e4_X]->(b {q4_X: 'b'})-[:f4_X]->(c {r4_X: 'c'}),
              |       (d {p4_X: 'd'})-[:e4_X]->(e {q4_X: 'e'})""".stripMargin,
    standingQuery = """MATCH (n)-[:e4_X]->(m)-[:f4_X]->(o)
                      |WHERE n.p4_X IS NOT NULL AND m.q4_X IS NOT NULL AND o.r4_X IS NOT NULL
                      |RETURN n.p4_X, o.r4_X""".stripMargin,
    expectedRows = 1,
  )

  oracleCase(
    "a pattern that matches nothing",
    setup = "CREATE (:A5_X {other5_X: 'x'})",
    standingQuery = "MATCH (n)-[:e5_X]->(m) WHERE n.p5_X IS NOT NULL AND m.q5_X IS NOT NULL RETURN n.p5_X",
    expectedRows = 0,
  )

  oracleCase(
    "several edges out of one node into several out of another, so a row per pair",
    setup = """CREATE (a {p6_X: 'a'})-[:e6_X]->(b {q6_X: 'b'}),
              |       (a)-[:e6_X]->(c {q6_X: 'c'}),
              |       (a)-[:f6_X]->(d {r6_X: 'd'}),
              |       (a)-[:f6_X]->(e {r6_X: 'e'})""".stripMargin,
    standingQuery = """MATCH (m)<-[:e6_X]-(n)-[:f6_X]->(o)
                      |WHERE n.p6_X IS NOT NULL AND m.q6_X IS NOT NULL AND o.r6_X IS NOT NULL
                      |RETURN m.q6_X, o.r6_X""".stripMargin,
    expectedRows = 4,
  )

  // The cases below are the ones that can tell a bag from a set. Everything above returns rows that differ from one
  // another, so an implementation that collapsed duplicates would agree with the oracle anyway, and how many times
  // a row is produced is part of the answer, not a detail of it.

  oracleCase(
    "several edges producing the very same row",
    setup = """CREATE (hub {p7_X: 'hub'})-[:e7_X]->({q7_X: '1'}),
              |       (hub)-[:e7_X]->({q7_X: '2'}),
              |       (hub)-[:e7_X]->({q7_X: '3'})""".stripMargin,
    // Only the near node's property is returned, so each of the three edges yields an identical row.
    standingQuery = "MATCH (n)-[:e7_X]->(m) WHERE n.p7_X IS NOT NULL AND m.q7_X IS NOT NULL RETURN n.p7_X",
    expectedRows = 3,
  )

  oracleCase(
    "two edges whose far nodes carry the same value",
    setup = """CREATE (a {p8_X: 'a'})-[:e8_X]->({q8_X: 'same'}),
              |       (a)-[:e8_X]->({q8_X: 'same'})""".stripMargin,
    standingQuery = "MATCH (n)-[:e8_X]->(m) WHERE n.p8_X IS NOT NULL AND m.q8_X IS NOT NULL RETURN n.p8_X, m.q8_X",
    expectedRows = 2,
  )

  oracleCase(
    "a crossed pattern where both branches produce the same row",
    setup = """CREATE (a {p9_X: 'a'})-[:e9_X]->({q9_X: 'l'}),
              |       (a)-[:e9_X]->({q9_X: 'l'}),
              |       (a)-[:f9_X]->({r9_X: 'r'}),
              |       (a)-[:f9_X]->({r9_X: 'r'})""".stripMargin,
    // Two indistinguishable rows on each side of the cross, so the product is four indistinguishable rows.
    standingQuery = """MATCH (m)<-[:e9_X]-(n)-[:f9_X]->(o)
                      |WHERE n.p9_X IS NOT NULL AND m.q9_X IS NOT NULL AND o.r9_X IS NOT NULL
                      |RETURN m.q9_X, o.r9_X""".stripMargin,
    expectedRows = 4,
  )

  /** Enough edges out of one node that describing it to a newly registered query part spans several pages of the
    * replay, so that the case below is answered by a description that was consumed a page at a time.
    */
  private val edgesPastOnePage: Int = MultipleValuesStandingQueryBehavior.initialEventPageSize + 200

  oracleCase(
    "a node with more edges than one page of a replay describes",
    setup = s"""CREATE (hub {p10_X: 'hub'})
               |WITH hub UNWIND range(1, $edgesPastOnePage) AS i
               |CREATE (hub)-[:e10_X]->({q10_X: toString(i)})""".stripMargin,
    standingQuery = "MATCH (n)-[:e10_X]->(m) WHERE n.p10_X IS NOT NULL AND m.q10_X IS NOT NULL RETURN n.p10_X, m.q10_X",
    expectedRows = edgesPastOnePage,
    // Describing a node that already holds its edges is the only thing that pages them: a node that gains its edges
    // afterwards is told about each one as it arrives and never fills a page. Running the other direction here would
    // build a node this size to test nothing about page boundaries.
    bothDirections = false,
  )

  /** An edge taken away and put back, with the query settled in between.
    *
    * Not written as an `oracleCase` because the whole of it is what happens in the middle. Removing the last
    * matching edge to a node cancels the far side's part of the pattern, and putting the edge back subscribes to it
    * again, to a part named by its constraints, so the second subscription is by the same name as the first. What
    * this catches is anything the first one left behind that makes the second a duplicate to be skipped: the match
    * would then come back only if the far node happened to change on its own account, which in an ordinary graph
    * may be never.
    */
  test("an edge taken away and put back is matched again") {
    val query = "MATCH (n)-[:e11_X]->(m) WHERE n.p11_X IS NOT NULL AND m.q11_X IS NOT NULL RETURN n.p11_X, m.q11_X"
    val actual = standingMatches(
      query,
      quiesce => {
        val _ = runAdHoc("CREATE (a {p11_X: 'a'})-[:e11_X]->(b {q11_X: 'b'})")
        quiesce()
        val _ = runAdHoc("MATCH (n {p11_X: 'a'})-[e:e11_X]->(m {q11_X: 'b'}) DELETE e")
        quiesce()
        runAdHoc("MATCH (n {p11_X: 'a'}), (m {q11_X: 'b'}) CREATE (n)-[:e11_X]->(m)")
      },
      dataFirst = false,
      expectedRows = 1,
    )
    val expected = runAdHoc(query)
    assert(expected.size == 1, "the ad-hoc query did not match what this case says it should")
    assertAgrees(actual, expected, "the standing query did not match again once the edge came back")
  }
}

object StandingQueryConvergenceOracle {

  /** One row of an answer, in the one shape both sides can be read into. */
  private type Row = Map[String, QuineValue]

  /** What a standing query settled on, and how it got there. */
  final private case class Settled(rows: Vector[Row], matches: Int, withdrawals: Int)
}
