package com.thatdot.quine.compiler.cypher

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContextExecutor, Future, Promise}

import org.apache.pekko.pattern.Patterns
import org.apache.pekko.stream.scaladsl.{Keep, Sink}

import org.scalatest.Assertion

import com.thatdot.common.logging.Pretty.PrettyHelper
import com.thatdot.common.quineid.QuineId
import com.thatdot.quine.graph.cypher.{CypherException, Expr, Value}
import com.thatdot.quine.model.{EdgeDirection, Milliseconds, QuineValue}

class HistoricalQueryTests extends CypherHarness("historical-query-tests") {
  implicit val ec: ExecutionContextExecutor = graph.system.dispatcher
  var t0: Milliseconds = Milliseconds(0L)
  var t1: Milliseconds = Milliseconds(0L)
  var t2: Milliseconds = Milliseconds(0L)
  var t3: Milliseconds = Milliseconds(0L)
  var t4: Milliseconds = Milliseconds(0L)
  var t5: Milliseconds = Milliseconds(0L)
  val qid: QuineId = idProv.customIdToQid(0L)
  val getNodeCypherQuery: String = s"""MATCH (n) WHERE strId(n) = "${qid.pretty}" RETURN n"""
  lazy val literalOps: graph.LiteralOps = graph.literalOps(cypherHarnessNamespace)

  override def beforeAll(): Unit =
    Await.result(
      for {
        _ <- Patterns.retry(
          () => Future(graph.requiredGraphIsReady()),
          attempts = 100,
          delay = 200.millis,
          graph.system.scheduler,
          graph.system.dispatcher,
        )
        _ = (t0 = Milliseconds.currentTime())
        _ <- nextMillisecond()
        _ <- literalOps.setProp(qid, "prop1", QuineValue.Integer(1L))
        _ <- nextMillisecond()
        _ = (t1 = Milliseconds.currentTime())
        _ <- nextMillisecond()
        _ <- literalOps.setProp(qid, "prop2", QuineValue.Integer(2L))
        _ <- nextMillisecond()
        _ = (t2 = Milliseconds.currentTime())
        _ <- nextMillisecond()
        _ <- graph.requestNodeSleep(cypherHarnessNamespace, qid)
        _ <- nextMillisecond()
        _ = (t3 = Milliseconds.currentTime())
        _ <- nextMillisecond()
        _ <- literalOps.setProp(qid, "prop3", QuineValue.Integer(3L))
        _ <- nextMillisecond()
        _ = (t4 = Milliseconds.currentTime())
        _ <- nextMillisecond()
        _ <- graph.requestNodeSleep(cypherHarnessNamespace, qid)
        _ <- nextMillisecond()
        _ = (t5 = Milliseconds.currentTime())
      } yield (),
      timeout.duration,
    )

  /** Complete once the millisecond clock has advanced past the moment this was called.
    *
    * The `history.*` procedures report change times at millisecond granularity, so two events
    * recorded within the same millisecond cannot be ordered relative to each other in the results.
    * Sequencing the next graph write behind this future is what makes each event land in a distinct
    * millisecond. Unlike a fixed delay it waits exactly as long as the clock actually needs, and it
    * does so without blocking a thread.
    */
  def nextMillisecond(): Future[Unit] = {
    val start = Milliseconds.currentTime().millis
    def poll(): Future[Unit] =
      if (Milliseconds.currentTime().millis > start) Future.unit
      else {
        val promise = Promise[Unit]()
        graph.system.scheduler.scheduleOnce(1 millisecond)(promise.success(()))
        promise.future.flatMap(_ => poll())
      }
    poll()
  }

  def assertPropertiesAtTime(time: Milliseconds, expected: Map[Symbol, Value]): Future[Assertion] = {
    val queryResults = queryCypherValues(
      getNodeCypherQuery,
      namespace = cypherHarnessNamespace,
      atTime = Some(time),
    )(graph)
    queryResults.results
      .toMat(Sink.seq)(Keep.right)
      .run()
      .map(r => assert(r == Vector(Vector(Expr.Node(qid, Set.empty, expected)))))
  }

  it("should rebuild a node at each past moment, across the sleeps that snapshot it") {
    val prop1 = Symbol("prop1") -> Expr.Integer(1L)
    val prop2 = Symbol("prop2") -> Expr.Integer(2L)
    val prop3 = Symbol("prop3") -> Expr.Integer(3L)

    // The node sleeps after t2 and again after t4, so each moment is reached by a different route:
    // t1 and t2 replay a live node's journal, t3 rebuilds from the first snapshot, t4 replays
    // forward from it, and t5 rebuilds from the second. The moments either side of a sleep expect
    // the same properties precisely because the sleep must not change what is reported.
    val moments = Seq(
      ("before any events", t0, Map.empty[Symbol, Value]),
      ("after the first event", t1, Map(prop1)),
      ("after the second event", t2, Map(prop1, prop2)),
      ("after the first sleep", t3, Map(prop1, prop2)),
      ("after the event following that sleep", t4, Map(prop1, prop2, prop3)),
      ("after the last sleep", t5, Map(prop1, prop2, prop3)),
    )

    moments.foldLeft(Future.successful(succeed)) { case (acc, (label, moment, expected)) =>
      acc.flatMap(_ => assertPropertiesAtTime(moment, expected).transform(identity, e => new AssertionError(label, e)))
    }
  }

  /** The (previousValue, value) pairs a `history.propertyChanges` call reports, in the order the
    * procedure emitted them.
    *
    * Deliberately no `ORDER BY`: these procedures walk the journal in order and are specified to
    * report in order, so sorting the rows before looking at them would throw away the only evidence
    * of that. Every assertion on the sequence returned here is an assertion about emission order.
    */
  def propRows(node: QuineId, call: String): Future[Seq[(Value, Value)]] = {
    val queryText = s"""MATCH (n) WHERE strId(n) = "${node.pretty}"
                       |CALL $call YIELD key, value, previousValue, changeTime
                       |RETURN previousValue, value, changeTime""".stripMargin
    queryCypherValues(queryText, cypherHarnessNamespace)(graph).results
      .toMat(Sink.seq)(Keep.right)
      .run()
      .map(_.map(row => (row(0), row(1))))
  }

  /** Rows of a `history.*` call, in emission order. See [[propRows]] on the absent `ORDER BY`.
    *
    * `atTime` pins the whole query to a past moment, as the `at-time` request parameter does.
    */
  def rowsOf(queryText: String, atTime: Option[Milliseconds] = None): Future[Seq[Vector[Value]]] =
    queryCypherValues(queryText, cypherHarnessNamespace, atTime = atTime)(graph).results
      .toMat(Sink.seq)(Keep.right)
      .run()

  /** Assert that a query fails at runtime with a message naming the problem, rather than merely
    * failing somehow: an unqualified `recoverToSucceededIf` passes even when a query is rejected for
    * a reason the test knows nothing about.
    */
  def assertFailsWith(queryText: String, expectedInMessage: String): Future[Assertion] =
    recoverToExceptionIf[CypherException.Runtime](rowsOf(queryText)).map { err =>
      assert(
        err.getMessage.toLowerCase.contains(expectedInMessage.toLowerCase),
        s"Expected a message mentioning '$expectedInMessage', got: ${err.getMessage}",
      )
    }

  /** Assert the `(action, edgeType)` pairs an edge procedure reports, in emission order.
    *
    * The two edge procedures take the same leading arguments and report the same columns, so both
    * use this. What differs is what they count as a change: `history.edgeChanges` reports each half
    * edge the queried node recorded, while `history.edgeChangesBetween` reports only the moments an
    * edge became or stopped being whole.
    */
  def assertEdgeChanges(
    procedure: String,
    node: QuineId,
    other: QuineId,
    edgeTypeFilter: Option[String],
    expectedChanges: Seq[(String, String)],
  ): Future[Assertion] = {
    val edgeTypeParam = edgeTypeFilter.fold("null")(edgeType => s""""$edgeType"""")
    // No `ORDER BY`: the rows are asserted as a sequence, so their order is the procedure's
    val queryText = s"""MATCH (n) WHERE strId(n) = "${node.pretty}"
                       |MATCH (o) WHERE strId(o) = "${other.pretty}"
                       |CALL $procedure(n, o, $edgeTypeParam, null)
                       |YIELD action, edgeType, changeTime
                       |RETURN action, edgeType, changeTime""".stripMargin
    rowsOf(queryText).map { results =>
      val reported = results.map(row => (row(0), row(1)))
      val expected = expectedChanges.map { case (action, edgeType) => (Expr.Str(action), Expr.Str(edgeType)) }
      assert(reported == expected, s"Expected $expected, got $reported")
      succeed
    }
  }

  describe("history.nodeChanges procedure") {
    it("should report property and edge changes together from one reading of the journal") {
      val testNodeId = idProv.customIdToQid(800L)
      val otherNodeId = idProv.customIdToQid(801L)

      for {
        _ <- literalOps.setProp(testNodeId, "state", QuineValue.Str("open"))
        _ <- nextMillisecond()
        _ <- literalOps.addEdge(testNodeId, otherNodeId, "LINKED")
        _ <- nextMillisecond()
        _ <- literalOps.setProp(testNodeId, "state", QuineValue.Str("closed"))
        _ <- graph.requestNodeSleep(cypherHarnessNamespace, testNodeId)
        _ <- graph.requestNodeSleep(cypherHarnessNamespace, otherNodeId)

        query = s"""MATCH (n) WHERE strId(n) = "${testNodeId.pretty}"
                   |CALL history.nodeChanges(n)
                   |YIELD kind, detail, changeTime
                   |RETURN kind, detail, changeTime ORDER BY changeTime""".stripMargin
        rows <- queryCypherValues(query, cypherHarnessNamespace)(graph).results
          .toMat(Sink.seq)(Keep.right)
          .run()
      } yield {
        assert(rows.size == 3, s"Expected both kinds of change, got ${rows.size}: $rows")
        assert(
          rows.map(_(0)) == Seq(Expr.Str("property"), Expr.Str("edge"), Expr.Str("property")),
          s"Expected the edge change between the two property changes, got ${rows.map(_(0))}",
        )

        // `kind` says how to read `detail`, and its fields are reachable directly
        def detail(row: Vector[Value]): Map[String, Value] = row(1).asInstanceOf[Expr.Map].map.toMap
        assert(detail(rows(0))("key") == Expr.Str("state"), s"${detail(rows(0))}")
        assert(detail(rows(0))("value") == Expr.Str("open"), s"${detail(rows(0))}")
        assert(detail(rows(0))("previousValue") == Expr.Null, s"${detail(rows(0))}")

        assert(detail(rows(1))("action") == Expr.Str("added"), s"${detail(rows(1))}")
        assert(detail(rows(1))("edgeType") == Expr.Str("LINKED"), s"${detail(rows(1))}")
        assert(detail(rows(1))("other") == Expr.Str(otherNodeId.pretty), s"${detail(rows(1))}")
        assert(detail(rows(1))("direction") == Expr.Str("Outgoing"), s"${detail(rows(1))}")

        // previousValue is carried across the intervening edge change
        assert(detail(rows(2))("previousValue") == Expr.Str("open"), s"${detail(rows(2))}")
        assert(detail(rows(2))("value") == Expr.Str("closed"), s"${detail(rows(2))}")
      }
    }

    it("should apply the window and the limit across both kinds of change together") {
      val testNodeId = idProv.customIdToQid(804L)
      val otherNodeId = idProv.customIdToQid(805L)
      var midpoint: Milliseconds = Milliseconds(0L)

      def call(opts: String) =
        s"""MATCH (n) WHERE strId(n) = "${testNodeId.pretty}"
           |CALL history.nodeChanges(n$opts) YIELD kind, detail
           |RETURN kind, detail""".stripMargin

      for {
        _ <- literalOps.setProp(testNodeId, "before", QuineValue.Integer(1L))
        _ <- nextMillisecond()
        _ <- literalOps.addEdge(testNodeId, otherNodeId, "BEFORE")
        _ <- nextMillisecond()
        _ = (midpoint = Milliseconds.currentTime())
        _ <- nextMillisecond()
        _ <- literalOps.setProp(testNodeId, "after", QuineValue.Integer(2L))
        _ <- nextMillisecond()
        _ <- literalOps.addEdge(testNodeId, otherNodeId, "AFTER")
        _ <- graph.requestNodeSleep(cypherHarnessNamespace, testNodeId)
        _ <- graph.requestNodeSleep(cypherHarnessNamespace, otherNodeId)

        all <- rowsOf(call(""))
        windowed <- rowsOf(call(s", {since: ${midpoint.millis}}"))
        through <- rowsOf(call(s", {through: ${midpoint.millis}}"))
        limited <- rowsOf(call(", {limit: 2}"))
      } yield {
        assert(all.size == 4, s"Expected all four changes, got ${all.size}")

        // The window cuts the interleaved timeline, so it must drop one change of each kind
        assert(windowed.size == 2, s"Expected the two changes after the midpoint, got ${windowed.size}")
        assert(
          windowed.map(_(0)) == Seq(Expr.Str("property"), Expr.Str("edge")),
          s"Expected one of each kind after the midpoint, got ${windowed.map(_(0))}",
        )

        // The other bound cuts the same timeline the other way, so the two halves partition it
        assert(
          through.map(_(0)) == Seq(Expr.Str("property"), Expr.Str("edge")),
          s"Expected one of each kind before the midpoint, got ${through.map(_(0))}",
        )
        assert(through ++ windowed == all, s"The two bounds should partition the timeline: $through then $windowed")

        // The limit counts rows of either kind, rather than applying to each kind separately
        assert(limited.size == 2, s"Expected the limit to cap the combined stream, got ${limited.size}")
        assert(limited == all.take(2), s"Expected the first two changes, got $limited")
      }
    }

    it("should read the journal once where the separate procedures read it twice") {
      val testNodeId = idProv.customIdToQid(802L)
      val otherNodeId = idProv.customIdToQid(803L)
      val combined = graph.metrics.journalWalkMetrics("history.nodeChanges")
      val propOnly = graph.metrics.journalWalkMetrics("history.propertyChanges")
      val edgeOnly = graph.metrics.journalWalkMetrics("history.edgeChanges")

      for {
        _ <- literalOps.setProp(testNodeId, "a", QuineValue.Integer(1L))
        _ <- literalOps.addEdge(testNodeId, otherNodeId, "E")
        _ <- graph.requestNodeSleep(cypherHarnessNamespace, testNodeId)
        _ <- graph.requestNodeSleep(cypherHarnessNamespace, otherNodeId)

        combinedBefore = combined.journalEventsRead.getCount
        separateBefore = propOnly.journalEventsRead.getCount + edgeOnly.journalEventsRead.getCount

        _ <- queryCypherValues(
          s"""MATCH (n) WHERE strId(n) = "${testNodeId.pretty}"
             |CALL history.nodeChanges(n) YIELD kind RETURN kind""".stripMargin,
          cypherHarnessNamespace,
        )(graph).results.toMat(Sink.seq)(Keep.right).run()
        combinedRead = combined.journalEventsRead.getCount - combinedBefore

        _ <- queryCypherValues(
          s"""MATCH (n) WHERE strId(n) = "${testNodeId.pretty}"
             |CALL history.propertyChanges(n) YIELD key RETURN key""".stripMargin,
          cypherHarnessNamespace,
        )(graph).results.toMat(Sink.seq)(Keep.right).run()
        _ <- queryCypherValues(
          s"""MATCH (n) WHERE strId(n) = "${testNodeId.pretty}"
             |CALL history.edgeChanges(n) YIELD action RETURN action""".stripMargin,
          cypherHarnessNamespace,
        )(graph).results.toMat(Sink.seq)(Keep.right).run()
        separateRead = propOnly.journalEventsRead.getCount + edgeOnly.journalEventsRead.getCount - separateBefore
      } yield
      // Asking the two procedures separately walks the same journal twice; this walks it once.
      assert(
        separateRead == combinedRead * 2,
        s"Expected the separate calls to read twice as much, got $separateRead against $combinedRead",
      )
    }
  }

  describe("history.* metrics") {
    it("should record the work done and the rows reported, so read amplification is visible") {
      val testNodeId = idProv.customIdToQid(700L)
      val metrics = graph.metrics.journalWalkMetrics("history.propertyChanges")

      for {
        // Ten journal events, of which a filtered call reports one. The gap between the two
        // counters is the whole point: a call can be cheap to answer and expensive to run.
        _ <- (1 to 10).foldLeft(Future.successful(())) { (acc, i) =>
          acc.flatMap(_ => literalOps.setProp(testNodeId, s"k$i", QuineValue.Integer(i.toLong)))
        }
        _ <- graph.requestNodeSleep(cypherHarnessNamespace, testNodeId)

        eventsBefore = metrics.journalEventsRead.getCount
        rowsBefore = metrics.rowsReported.getCount
        timedBefore = metrics.timer.getCount

        rows <- propRows(testNodeId, """history.propertyChanges(n, "k7")""")
      } yield {
        assert(rows.size == 1, s"Expected the one matching change, got ${rows.size}")
        assert(
          metrics.rowsReported.getCount - rowsBefore == 1L,
          s"Expected 1 row counted, got ${metrics.rowsReported.getCount - rowsBefore}",
        )
        assert(
          metrics.journalEventsRead.getCount - eventsBefore == 10L,
          s"Expected all 10 journal events walked, got ${metrics.journalEventsRead.getCount - eventsBefore}",
        )
        assert(
          metrics.timer.getCount - timedBefore == 1L,
          s"Expected the call timed once, got ${metrics.timer.getCount - timedBefore}",
        )
      }
    }

    it("should read less of the journal when a window or a limit narrows the call") {
      val testNodeId = idProv.customIdToQid(701L)
      val metrics = graph.metrics.journalWalkMetrics("history.propertyChanges")
      val changes = 60
      var midpoint: Milliseconds = Milliseconds(0L)

      def eventsReadBy(call: String): Future[Long] = {
        val before = metrics.journalEventsRead.getCount
        propRows(testNodeId, call).map(_ => metrics.journalEventsRead.getCount - before)
      }

      for {
        _ <- (1 to changes).foldLeft(Future.successful(())) { (acc, i) =>
          for {
            _ <- acc
            _ = if (i == changes / 2) midpoint = Milliseconds.currentTime()
            _ <- literalOps.setProp(testNodeId, "p", QuineValue.Integer(i.toLong))
            _ <- nextMillisecond()
          } yield ()
        }
        _ <- graph.requestNodeSleep(cypherHarnessNamespace, testNodeId)

        unbounded <- eventsReadBy("""history.propertyChanges(n, "p")""")
        windowed <- eventsReadBy(s"""history.propertyChanges(n, "p", {since: ${midpoint.millis}})""")
        limited <- eventsReadBy("""history.propertyChanges(n, "p", {limit: 1})""")
      } yield {
        assert(unbounded == changes.toLong, s"Expected the whole journal walked, got $unbounded")

        // `since` is handed to the persistor as the start of the range to read, so the events
        // before it never reach the stream at all. The midpoint is captured just before writing
        // change 30, and every change sits in its own millisecond, so exactly changes 30..60 remain.
        // Filtering after the read instead would walk all 60.
        val expectedWindowed = changes - changes / 2 + 1
        assert(
          windowed == expectedWindowed.toLong,
          s"A since bound should start the read at the bound: expected $expectedWindowed events, got $windowed",
        )

        // A limit cancels back through the journal read rather than reading everything and
        // discarding the surplus. One row costs exactly one event, whatever the journal's length.
        assert(
          limited == 1L,
          s"A limit of 1 should read one event and stop, but $limited events were read of $unbounded",
        )
      }
    }

    it("should stop both journals early when a limit narrows a whole-edge call") {
      val nodeA = idProv.customIdToQid(702L)
      val nodeB = idProv.customIdToQid(703L)
      val metrics = graph.metrics.journalWalkMetrics("history.edgeChangesBetween")
      val edges = 30

      def call(opts: String) =
        s"""MATCH (a) WHERE strId(a) = "${nodeA.pretty}"
           |MATCH (b) WHERE strId(b) = "${nodeB.pretty}"
           |CALL history.edgeChangesBetween(a, b, null, null$opts)
           |YIELD action RETURN action""".stripMargin

      def eventsReadBy(opts: String): Future[(Long, Int)] = {
        val before = metrics.journalEventsRead.getCount
        rowsOf(call(opts)).map(rows => (metrics.journalEventsRead.getCount - before, rows.size))
      }

      for {
        _ <- (1 to edges).foldLeft(Future.successful(())) { (acc, i) =>
          acc.flatMap(_ => literalOps.addEdge(nodeA, nodeB, s"E_$i"))
        }
        _ <- graph.requestNodeSleep(cypherHarnessNamespace, nodeA)
        _ <- graph.requestNodeSleep(cypherHarnessNamespace, nodeB)

        (unboundedRead, unboundedRows) <- eventsReadBy("")
        (limitedRead, limitedRows) <- eventsReadBy(", {limit: 1}")
      } yield {
        // Each whole edge is one half edge in each node's journal, so the unbounded call reads
        // every event of both
        assert(unboundedRows == edges, s"Expected $edges whole edges, got $unboundedRows")
        assert(unboundedRead == 2L * edges, s"Expected both journals read in full, got $unboundedRead")

        // The cap is applied to whole-edge rows, which are assembled from two journals merged by
        // timestamp — so cancelling has to travel back through the merge and stop both reads, not
        // just the one that produced the last event. The cost of a row is the two half edges behind
        // it plus the merge's one-event lookahead, and crucially does not grow with the journals.
        assert(limitedRows == 1, s"Expected the one row, got $limitedRows")
        assert(
          limitedRead <= 6L,
          s"A limit of 1 should read a handful of events, but $limitedRead were read of $unboundedRead",
        )
      }
    }
  }

  describe("history.* options") {
    it("should accept only the options each procedure actually understands") {
      val nodeId = idProv.customIdToQid(710L)
      val otherId = idProv.customIdToQid(711L)

      // `unreadableAsNull` says how to report a property value that will not deserialize, so it
      // means nothing to a procedure that reports no values. Each procedure states the keys it
      // recognises, and the rest are rejected rather than ignored — an ignored option is one the
      // caller believes is in force.
      val valueReporting = Seq(
        s"""CALL history.propertyChanges(n, null, {unreadableAsNull: true})""",
        s"""CALL history.nodeChanges(n, {unreadableAsNull: true})""",
      )
      val edgeOnly = Seq(
        s"""CALL history.edgeChanges(n, o, null, null, {unreadableAsNull: true})""",
        s"""CALL history.edgeChangesBetween(n, o, null, null, {unreadableAsNull: true})""",
      )

      def query(call: String) =
        s"""MATCH (n) WHERE strId(n) = "${nodeId.pretty}"
           |MATCH (o) WHERE strId(o) = "${otherId.pretty}"
           |$call YIELD changeTime RETURN changeTime""".stripMargin

      for {
        _ <- literalOps.setProp(nodeId, "p", QuineValue.Integer(1L))
        _ <- literalOps.addEdge(nodeId, otherId, "E")
        _ <- graph.requestNodeSleep(cypherHarnessNamespace, nodeId)
        _ <- graph.requestNodeSleep(cypherHarnessNamespace, otherId)

        // The procedures that report values accept it
        _ <- valueReporting.foldLeft(Future.successful(succeed)) { case (acc, call) =>
          acc.flatMap(_ => rowsOf(query(call)).map(rows => assert(rows.nonEmpty, s"$call reported nothing")))
        }
        // The ones that report only edges reject it, naming the key and listing what they do take
        assertion <- edgeOnly.foldLeft(Future.successful(succeed)) { case (acc, call) =>
          acc.flatMap(_ => assertFailsWith(query(call), "does not recognise the option(s) `unreadableAsNull`"))
        }
      } yield assertion
    }
  }

  describe("history.propertyChanges procedure") {
    it("should report what each property was set to before the change") {
      val testNodeId = idProv.customIdToQid(500L)

      for {
        _ <- literalOps.setProp(testNodeId, "p", QuineValue.Integer(1L))
        _ <- nextMillisecond()
        _ <- literalOps.setProp(testNodeId, "p", QuineValue.Integer(2L))
        _ <- nextMillisecond()
        _ <- literalOps.removeProp(testNodeId, "p")
        _ <- nextMillisecond()
        _ <- literalOps.setProp(testNodeId, "p", QuineValue.Integer(3L))
        _ <- graph.requestNodeSleep(cypherHarnessNamespace, testNodeId)

        rows <- propRows(testNodeId, """history.propertyChanges(n, "p")""")
      } yield {
        // Every change reads as a setting; a removal is a setting to null. `previousValue` is what
        // the property held immediately before, and is null only for the first reported setting.
        // The rows are unsorted, so asserting them as a sequence also pins the emission order.
        assert(rows.size == 4, s"Expected 4 changes, got ${rows.size}")
        assert(rows(0) == (Expr.Null, Expr.Integer(1L)), s"first set: got ${rows(0)}")
        assert(rows(1) == (Expr.Integer(1L), Expr.Integer(2L)), s"overwrite: got ${rows(1)}")
        assert(rows(2) == (Expr.Integer(2L), Expr.Null), s"removal: got ${rows(2)}")
        assert(rows(3) == (Expr.Null, Expr.Integer(3L)), s"set after removal: got ${rows(3)}")
      }
    }

    it("should take previousValue from a removal even when it opens the window") {
      val testNodeId = idProv.customIdToQid(501L)
      var afterFirstSet: Milliseconds = Milliseconds(0L)

      for {
        _ <- literalOps.setProp(testNodeId, "q", QuineValue.Str("before"))
        _ <- nextMillisecond()
        _ <- { afterFirstSet = Milliseconds.currentTime(); Future.successful(()) }
        _ <- nextMillisecond()
        _ <- literalOps.removeProp(testNodeId, "q")
        _ <- graph.requestNodeSleep(cypherHarnessNamespace, testNodeId)

        // The window starts after the set, so the removal is the first thing seen for this key.
        // A removal records what it removed, so the prior value is still known.
        rows <- propRows(testNodeId, s"""history.propertyChanges(n, "q", {since: ${afterFirstSet.millis}})""")
      } yield {
        assert(rows.size == 1, s"Expected only the removal in the window, got ${rows.size}: $rows")
        assert(rows(0) == (Expr.Str("before"), Expr.Null), s"got ${rows(0)}")
      }
    }

    it("should bound reported changes by the since and through options") {
      val testNodeId = idProv.customIdToQid(502L)
      var t1: Milliseconds = Milliseconds(0L)
      var t2: Milliseconds = Milliseconds(0L)

      for {
        _ <- literalOps.setProp(testNodeId, "w", QuineValue.Integer(1L))
        _ <- nextMillisecond()
        // `t1` is read before the second change, so that change is at or after it whatever the
        // clock does; `t2` is read after the third, so that change is at or before it. Reading
        // either the other way round would leave the boundary case up to how the clock happened to
        // fall between two statements.
        _ <- { t1 = Milliseconds.currentTime(); Future.successful(()) }
        _ <- literalOps.setProp(testNodeId, "w", QuineValue.Integer(2L))
        _ <- nextMillisecond()
        _ <- literalOps.setProp(testNodeId, "w", QuineValue.Integer(3L))
        _ <- { t2 = Milliseconds.currentTime(); Future.successful(()) }
        _ <- nextMillisecond()
        _ <- literalOps.setProp(testNodeId, "w", QuineValue.Integer(4L))
        _ <- graph.requestNodeSleep(cypherHarnessNamespace, testNodeId)

        sinceOnly <- propRows(testNodeId, s"""history.propertyChanges(n, "w", {since: ${t1.millis}})""")
        throughOnly <- propRows(testNodeId, s"""history.propertyChanges(n, "w", {through: ${t2.millis}})""")
        window <- propRows(
          testNodeId,
          s"""history.propertyChanges(n, "w", {since: ${t1.millis}, through: ${t2.millis}})""",
        )
        limited <- propRows(testNodeId, """history.propertyChanges(n, "w", {limit: 2})""")
      } yield {
        // Bounds are inclusive of the whole millisecond they name
        assert(sinceOnly.map(_._2) == Seq(Expr.Integer(2L), Expr.Integer(3L), Expr.Integer(4L)), s"$sinceOnly")
        assert(throughOnly.map(_._2) == Seq(Expr.Integer(1L), Expr.Integer(2L), Expr.Integer(3L)), s"$throughOnly")
        assert(window.map(_._2) == Seq(Expr.Integer(2L), Expr.Integer(3L)), s"$window")
        assert(limited.map(_._2) == Seq(Expr.Integer(1L), Expr.Integer(2L)), s"$limited")
      }
    }

    it("should take the earlier of the query's moment and the through bound, whichever that is") {
      val testNodeId = idProv.customIdToQid(506L)
      var afterFirst: Milliseconds = Milliseconds(0L)
      var afterSecond: Milliseconds = Milliseconds(0L)

      def query(through: Milliseconds) =
        s"""MATCH (n) WHERE strId(n) = "${testNodeId.pretty}"
           |CALL history.propertyChanges(n, "b", {through: ${through.millis}}) YIELD value
           |RETURN value""".stripMargin

      for {
        _ <- literalOps.setProp(testNodeId, "b", QuineValue.Integer(1L))
        _ <- nextMillisecond()
        _ = (afterFirst = Milliseconds.currentTime())
        _ <- nextMillisecond()
        _ <- literalOps.setProp(testNodeId, "b", QuineValue.Integer(2L))
        _ <- nextMillisecond()
        _ = (afterSecond = Milliseconds.currentTime())
        _ <- nextMillisecond()
        _ <- literalOps.setProp(testNodeId, "b", QuineValue.Integer(3L))
        _ <- graph.requestNodeSleep(cypherHarnessNamespace, testNodeId)

        // A procedure cannot report further ahead than the query containing it, so the two bounds
        // are combined by taking the earlier. Both arms are exercised: here the query's moment is
        // the earlier one...
        queryEarlier <- queryCypherValues(query(afterSecond), cypherHarnessNamespace, atTime = Some(afterFirst))(
          graph,
        ).results.toMat(Sink.seq)(Keep.right).run()
        // ...and here the option is
        optionEarlier <- queryCypherValues(query(afterFirst), cypherHarnessNamespace, atTime = Some(afterSecond))(
          graph,
        ).results.toMat(Sink.seq)(Keep.right).run()
      } yield {
        assert(
          queryEarlier.map(_(0)) == Seq(Expr.Integer(1L)),
          s"The query's moment is earlier, so it should win: ${queryEarlier.map(_(0))}",
        )
        assert(
          optionEarlier.map(_(0)) == Seq(Expr.Integer(1L)),
          s"The through option is earlier, so it should win: ${optionEarlier.map(_(0))}",
        )
      }
    }

    it("should treat a through bound in the future as no bound at all") {
      val testNodeId = idProv.customIdToQid(504L)
      val farFuture = Milliseconds.currentTime().millis + 1000000L

      for {
        _ <- literalOps.setProp(testNodeId, "f", QuineValue.Integer(1L))
        _ <- nextMillisecond()
        _ <- literalOps.setProp(testNodeId, "f", QuineValue.Integer(2L))
        _ <- graph.requestNodeSleep(cypherHarnessNamespace, testNodeId)

        // A node refuses a moment it has not reached, so a future bound must not be asked of it.
        // There is nothing after the present to report, so the answer matches an unbounded call.
        future <- propRows(testNodeId, s"""history.propertyChanges(n, "f", {through: $farFuture})""")
        unbounded <- propRows(testNodeId, """history.propertyChanges(n, "f")""")
      } yield {
        assert(future.map(_._2) == Seq(Expr.Integer(1L), Expr.Integer(2L)), s"future through: $future")
        assert(future == unbounded, s"a future through should read the same as no through: $future vs $unbounded")
      }
    }

    it("should fail on a value it cannot deserialize, unless asked to report it as null") {
      val testNodeId = idProv.customIdToQid(505L)
      // 0xC1 is the one byte MessagePack designates as never valid, so this is unreadable by
      // construction rather than by accident of encoding
      val corrupt = Array[Byte](0xC1.toByte)

      for {
        _ <- literalOps.setProp(testNodeId, "d", QuineValue.Str("readable"))
        _ <- nextMillisecond()
        _ <- literalOps.setPropBytes(testNodeId, "d", corrupt)
        _ <- graph.requestNodeSleep(cypherHarnessNamespace, testNodeId)

        // Reporting an unreadable value as null by default would be indistinguishable from a
        // removal, so the default is to fail — and to say which option relaxes it
        _ <- assertFailsWith(
          s"""MATCH (n) WHERE strId(n) = "${testNodeId.pretty}"
             |CALL history.propertyChanges(n, "d") YIELD value RETURN value""".stripMargin,
          "unreadableAsNull",
        )
        rows <- propRows(testNodeId, """history.propertyChanges(n, "d", {unreadableAsNull: true})""")

        // The same option, on the procedure that reports both kinds of change
        nodeRows <- rowsOf(
          s"""MATCH (n) WHERE strId(n) = "${testNodeId.pretty}"
             |CALL history.nodeChanges(n, {unreadableAsNull: true}) YIELD kind, detail
             |RETURN kind, detail""".stripMargin,
        )
      } yield {
        // The readable change is still reported in full; only the damaged one reads as null, and
        // the rest of the journal past it is still walked
        assert(rows.size == 2, s"Expected both changes, got ${rows.size}: $rows")
        assert(rows(0) == (Expr.Null, Expr.Str("readable")), s"got ${rows(0)}")
        assert(rows(1) == (Expr.Str("readable"), Expr.Null), s"got ${rows(1)}")

        assert(nodeRows.size == 2, s"Expected both changes from history.nodeChanges, got ${nodeRows.size}")
        val lastDetail = nodeRows(1)(1).asInstanceOf[Expr.Map].map.toMap
        assert(lastDetail("value") == Expr.Null, s"Expected the unreadable value as null, got ${lastDetail("value")}")
      }
    }

    it("should reject a malformed options map") {
      val testNodeId = idProv.customIdToQid(503L)
      def options(opts: String): String =
        s"""MATCH (n) WHERE strId(n) = "${testNodeId.pretty}"
           |CALL history.propertyChanges(n, "x", $opts) YIELD value RETURN value""".stripMargin

      // Each malformed option is rejected on its own terms rather than being ignored or coerced.
      // Silently accepting any of these would widen or narrow the window being audited without
      // the caller knowing, which is the failure the whole options map is strict about.
      val cases = Seq(
        options("{snce: 1}") -> "snce", // a mistyped key, named back so the typo is visible
        options("{since: 200, through: 100}") -> "after", // a window that runs backwards
        options("""{since: "yesterday"}""") -> "since", // not a millisecond timestamp
        options("{limit: 0}") -> "limit", // a limit that could only return nothing
        options("{limit: -1}") -> "limit",
        options("""{limit: "two"}""") -> "limit",
        options("{unreadableAsNull: 1}") -> "unreadableAsNull", // not a boolean
      )

      cases.foldLeft(Future.successful(succeed)) { case (acc, (queryText, expected)) =>
        acc.flatMap(_ => assertFailsWith(queryText, expected))
      }
    }

    it("should report each key's own changes, and nothing for a key that was never set") {
      // `prop3` is written after the node has slept, so narrowing to it also covers reading a key
      // whose only change lies beyond a snapshot.
      val cases = Seq(
        "prop1" -> Some(Expr.Integer(1L)),
        "prop2" -> Some(Expr.Integer(2L)),
        "prop3" -> Some(Expr.Integer(3L)),
        "nonexistent" -> None,
      )

      cases.foldLeft(Future.successful(succeed)) { case (acc, (key, expected)) =>
        acc.flatMap { _ =>
          rowsOf(s"""MATCH (n) WHERE strId(n) = "${qid.pretty}"
                    |CALL history.propertyChanges(n, "$key")
                    |YIELD key, value, changeTime
                    |RETURN key, value, changeTime""".stripMargin).map { results =>
            expected match {
              case None =>
                assert(results.isEmpty, s"Expected no changes for $key, got $results")
              case Some(value) =>
                assert(results.size == 1, s"Expected 1 change for $key, got ${results.size}")
                assert(results(0)(0) == Expr.Str(key), s"Expected key $key, got ${results(0)(0)}")
                assert(results(0)(1) == value, s"Expected value $value for $key, got ${results(0)(1)}")
                assert(
                  results(0)(2).asInstanceOf[Expr.Integer].long >= t0.millis,
                  s"Change for $key predates the fixture: ${results(0)(2)}",
                )
            }
            succeed
          }
        }
      }
    }

    it("should return every property's changes when no property key is given") {
      val testNodeId = idProv.customIdToQid(400L)

      for {
        _ <- literalOps.setProp(testNodeId, "alpha", QuineValue.Integer(1L))
        _ <- nextMillisecond()
        _ <- literalOps.setProp(testNodeId, "beta", QuineValue.Str("two"))
        _ <- nextMillisecond()
        _ <- literalOps.setProp(testNodeId, "alpha", QuineValue.Integer(3L))
        _ <- graph.requestNodeSleep(cypherHarnessNamespace, testNodeId)

        // Both the 1-arg form and an explicit null filter mean "every property"
        oneArgQuery = s"""MATCH (n) WHERE strId(n) = "${testNodeId.pretty}"
                         |CALL history.propertyChanges(n)
                         |YIELD key, value, changeTime
                         |RETURN key, value, changeTime
                         |ORDER BY changeTime""".stripMargin
        oneArgResults <- queryCypherValues(oneArgQuery, cypherHarnessNamespace)(graph).results
          .toMat(Sink.seq)(Keep.right)
          .run()

        nullQuery = s"""MATCH (n) WHERE strId(n) = "${testNodeId.pretty}"
                       |CALL history.propertyChanges(n, null)
                       |YIELD key, value, changeTime
                       |RETURN key, value, changeTime
                       |ORDER BY changeTime""".stripMargin
        nullResults <- queryCypherValues(nullQuery, cypherHarnessNamespace)(graph).results
          .toMat(Sink.seq)(Keep.right)
          .run()

        // Filtering to one key must still narrow the results
        filteredQuery = s"""MATCH (n) WHERE strId(n) = "${testNodeId.pretty}"
                           |CALL history.propertyChanges(n, "alpha")
                           |YIELD key, value, changeTime
                           |RETURN key, value, changeTime
                           |ORDER BY changeTime""".stripMargin
        filteredResults <- queryCypherValues(filteredQuery, cypherHarnessNamespace)(graph).results
          .toMat(Sink.seq)(Keep.right)
          .run()
      } yield {
        val expected = Seq(
          (Expr.Str("alpha"), Expr.Integer(1L)),
          (Expr.Str("beta"), Expr.Str("two")),
          (Expr.Str("alpha"), Expr.Integer(3L)),
        )
        assert(oneArgResults.size == 3, s"Expected 3 changes across all properties, got ${oneArgResults.size}")
        oneArgResults.zip(expected).foreach { case (row, (expectedKey, expectedValue)) =>
          assert(row(0) == expectedKey, s"Expected key $expectedKey, got ${row(0)}")
          assert(row(1) == expectedValue, s"Expected value $expectedValue, got ${row(1)}")
        }

        // A null filter is equivalent to omitting the argument
        assert(nullResults == oneArgResults, "A null property key should behave like the 1-arg form")

        assert(filteredResults.size == 2, s"Expected 2 changes for alpha, got ${filteredResults.size}")
        assert(filteredResults.forall(_(0) == Expr.Str("alpha")), "Filtered results should only contain alpha")
      }
    }

    it("should respect the query's historical moment") {
      val testNodeId = idProv.customIdToQid(101L)
      // Captured after the second change and before the third, so a query pinned here must see
      // exactly the first two.
      var boundary: Milliseconds = Milliseconds(0L)

      for {
        _ <- literalOps.setProp(testNodeId, "temporal", QuineValue.Integer(100L))
        _ <- nextMillisecond()
        _ <- literalOps.setProp(testNodeId, "temporal", QuineValue.Integer(200L))
        _ <- nextMillisecond()
        _ = (boundary = Milliseconds.currentTime())
        _ <- nextMillisecond()
        _ <- literalOps.setProp(testNodeId, "temporal", QuineValue.Integer(300L))
        _ <- graph.requestNodeSleep(cypherHarnessNamespace, testNodeId)

        queryText =
          s"""MATCH (n) WHERE strId(n) = "${testNodeId.pretty}"
             |CALL history.propertyChanges(n, "temporal") YIELD value, changeTime
             |RETURN value, changeTime ORDER BY changeTime""".stripMargin
        allResults <- queryCypherValues(queryText, cypherHarnessNamespace)(graph).results
          .toMat(Sink.seq)(Keep.right)
          .run()
        boundedResults <- queryCypherValues(queryText, cypherHarnessNamespace, atTime = Some(boundary))(graph).results
          .toMat(Sink.seq)(Keep.right)
          .run()
      } yield {
        assert(
          allResults.map(_(0)) == Seq(Expr.Integer(100L), Expr.Integer(200L), Expr.Integer(300L)),
          s"Unbounded query should see every change, got ${allResults.map(_(0))}",
        )
        assert(
          boundedResults.map(_(0)) == Seq(Expr.Integer(100L), Expr.Integer(200L)),
          s"Query pinned to $boundary should not see the later change, got ${boundedResults.map(_(0))}",
        )
        assert(
          boundedResults.map(_(1).asInstanceOf[Expr.Integer].long).forall(_ <= boundary.millis),
          "Every reported change should be at or before the query's moment",
        )
      }
    }

    it("should name the argument actually at fault") {
      val testNodeId = idProv.customIdToQid(108L)

      // Asserting on the position named, not merely that something failed: the procedure's own name
      // appears in every error it raises, so matching on that would accept any failure at all.
      for {
        _ <- assertFailsWith(
          """CALL history.propertyChanges("invalid_node", "test_prop") YIELD value RETURN value""",
          "first argument",
        )
        assertion <- assertFailsWith(
          s"""MATCH (n) WHERE strId(n) = "${testNodeId.pretty}"
             |CALL history.propertyChanges(n, 123) YIELD value RETURN value""".stripMargin,
          "second argument",
        )
      } yield assertion
    }

    it("should report a journal longer than any one batch, in order and without gaps") {
      val testNodeId = idProv.customIdToQid(109L)
      val numChanges = 100

      for {
        // Written one at a time, each in its own millisecond, so the expected result is the exact
        // sequence 1..100 rather than a set that happens to have the right members
        _ <- (1 to numChanges).foldLeft(Future.successful(())) { (acc, i) =>
          for {
            _ <- acc
            _ <- literalOps.setProp(testNodeId, "large_history", QuineValue.Integer(i.toLong))
            _ <- nextMillisecond()
          } yield ()
        }
        _ <- graph.requestNodeSleep(cypherHarnessNamespace, testNodeId)

        rows <- propRows(testNodeId, """history.propertyChanges(n, "large_history")""")
      } yield {
        // Unsorted, so this pins the order the journal was walked in, not merely its contents
        val values = rows.map(_._2)
        assert(
          values == (1L to numChanges.toLong).map(Expr.Integer(_)),
          s"Expected 1..$numChanges in order, got ${values.take(10)}... (${values.size} rows)",
        )
        // Each change also carries what the previous one set, all the way down the journal
        assert(
          rows.map(_._1) == (Expr.Null +: (1L until numChanges.toLong).map(Expr.Integer(_))),
          "Each change should carry the previous value",
        )
      }
    }
  }

  describe("history.nodeAt and history.queryAt procedures") {
    it("should read a node as it stood at a chosen moment") {
      val testNodeId = idProv.customIdToQid(600L)
      var boundary: Milliseconds = Milliseconds(0L)

      for {
        _ <- literalOps.setProp(testNodeId, "balance", QuineValue.Integer(100L))
        _ <- nextMillisecond()
        _ = (boundary = Milliseconds.currentTime())
        _ <- nextMillisecond()
        _ <- literalOps.setProp(testNodeId, "balance", QuineValue.Integer(999L))
        _ <- graph.requestNodeSleep(cypherHarnessNamespace, testNodeId)

        queryText = s"""MATCH (n) WHERE strId(n) = "${testNodeId.pretty}"
                       |CALL history.nodeAt(n, ${boundary.millis}) YIELD node AS past
                       |RETURN past.balance AS then, n.balance AS now""".stripMargin
        results <- queryCypherValues(queryText, cypherHarnessNamespace)(graph).results
          .toMat(Sink.seq)(Keep.right)
          .run()
      } yield {
        assert(results.size == 1, s"Expected one row, got ${results.size}")
        assert(results(0)(0) == Expr.Integer(100L), s"Value at the chosen moment, got ${results(0)(0)}")
        assert(results(0)(1) == Expr.Integer(999L), s"Value now, got ${results(0)(1)}")
      }
    }

    it("should report the whole node, edges included, as it stood then") {
      val nodeId = idProv.customIdToQid(610L)
      val keptNeighbour = idProv.customIdToQid(611L)
      val goneNeighbour = idProv.customIdToQid(612L)
      var boundary: Milliseconds = Milliseconds(0L)

      for {
        _ <- literalOps.setProp(nodeId, "state", QuineValue.Str("then"))
        _ <- literalOps.addEdge(nodeId, keptNeighbour, "KEPT")
        _ <- literalOps.addEdge(nodeId, goneNeighbour, "GONE")
        _ <- nextMillisecond()
        _ <- { boundary = Milliseconds.currentTime(); Future.successful(()) }
        _ <- nextMillisecond()
        // Both change after the boundary, so neither may show in the reported state
        _ <- literalOps.setProp(nodeId, "state", QuineValue.Str("now"))
        _ <- literalOps.removeEdge(nodeId, goneNeighbour, "GONE")
        _ <- graph.requestNodeSleep(cypherHarnessNamespace, nodeId)

        query = s"""MATCH (n) WHERE strId(n) = "${nodeId.pretty}"
                   |CALL history.nodeAt(n, ${boundary.millis}) YIELD node, edges
                   |RETURN node.state AS stateThen, edges""".stripMargin
        rows <- queryCypherValues(query, cypherHarnessNamespace)(graph).results
          .toMat(Sink.seq)(Keep.right)
          .run()
      } yield {
        assert(rows.size == 1, s"Expected one row, got ${rows.size}")
        assert(rows(0)(0) == Expr.Str("then"), s"Property as it stood then, got ${rows(0)(0)}")

        val edges = rows(0)(1)
          .asInstanceOf[Expr.List]
          .list
          .map(_.asInstanceOf[Expr.Map].map.toMap)
        val byType = edges.map(e => e("edgeType").asInstanceOf[Expr.Str].string).toSet
        // The edge removed after the boundary was still held then, so it must appear
        assert(byType == Set("KEPT", "GONE"), s"Expected both edges held at that moment, got $byType")
        assert(
          edges.forall(_("direction") == Expr.Str("Outgoing")),
          s"Expected outgoing halves, got ${edges.map(_("direction"))}",
        )
        assert(
          edges.map(_("other").asInstanceOf[Expr.Str].string).toSet ==
            Set(keptNeighbour.pretty, goneNeighbour.pretty),
          s"Expected both neighbours named, got ${edges.map(_("other"))}",
        )
      }
    }

    it("should use a moment that has not arrived exactly as given") {
      val testNodeId = idProv.customIdToQid(601L)
      // Passed through unchanged rather than rewritten to the present: a moment just ahead cannot
      // be told apart from clock skew. Only what has already happened can be reported, so the
      // answer matches the present anyway.
      val farFuture = Milliseconds.currentTime().millis + 1000000L

      for {
        _ <- literalOps.setProp(testNodeId, "v", QuineValue.Integer(7L))
        _ <- graph.requestNodeSleep(cypherHarnessNamespace, testNodeId)

        queryText = s"""MATCH (n) WHERE strId(n) = "${testNodeId.pretty}"
                       |CALL history.nodeAt(n, $farFuture) YIELD node AS future
                       |RETURN future.v""".stripMargin
        results <- queryCypherValues(queryText, cypherHarnessNamespace)(graph).results
          .toMat(Sink.seq)(Keep.right)
          .run()
      } yield {
        assert(results.size == 1, s"Expected one row, got ${results.size}")
        assert(results(0)(0) == Expr.Integer(7L), s"Should report what has already happened, got ${results(0)(0)}")
      }
    }

    it("should apply the moment to every node a query reaches, not just the one it starts from") {
      val start = idProv.customIdToQid(620L)
      val neighbour = idProv.customIdToQid(621L)
      var boundary: Milliseconds = Milliseconds(0L)

      def queryAt(moment: Milliseconds) =
        s"""CALL history.queryAt(
           |  'MATCH (a)-[:LINK]->(b) WHERE strId(a) = "${start.pretty}" RETURN b.state AS state',
           |  ${moment.millis}
           |) YIELD value RETURN value.state""".stripMargin

      for {
        // The far node's property is what the moment has to reach. A query pinned to the past that
        // traversed correctly but then read the neighbour's *present* properties would pass any
        // test that only looks at the node it started from.
        _ <- literalOps.setProp(start, "name", QuineValue.Str("start"))
        _ <- literalOps.addEdge(start, neighbour, "LINK")
        _ <- literalOps.setProp(neighbour, "state", QuineValue.Str("then"))
        _ <- nextMillisecond()
        _ = (boundary = Milliseconds.currentTime())
        _ <- nextMillisecond()
        _ <- literalOps.setProp(neighbour, "state", QuineValue.Str("now"))
        _ <- graph.requestNodeSleep(cypherHarnessNamespace, start)
        _ <- graph.requestNodeSleep(cypherHarnessNamespace, neighbour)

        past <- rowsOf(queryAt(boundary))
        present <- rowsOf(queryAt(Milliseconds.currentTime()))
      } yield {
        assert(
          past.map(_(0)) == Seq(Expr.Str("then")),
          s"The neighbour's property must be read at the query's moment too, got ${past.map(_(0))}",
        )
        // The same query at the present reads the later value, so the above is the moment working
        // rather than the traversal failing to reach the neighbour at all
        assert(present.map(_(0)) == Seq(Expr.Str("now")), s"At the present: ${present.map(_(0))}")
      }
    }

    it("should not traverse an edge that did not exist at the query's moment") {
      val start = idProv.customIdToQid(622L)
      val neighbour = idProv.customIdToQid(623L)
      var beforeEdge: Milliseconds = Milliseconds(0L)

      def queryAt(moment: Milliseconds) =
        s"""CALL history.queryAt(
           |  'MATCH (a)-[:LATER]->(b) WHERE strId(a) = "${start.pretty}" RETURN strId(b) AS other',
           |  ${moment.millis}
           |) YIELD value RETURN value.other""".stripMargin

      for {
        // Both nodes exist before the boundary; only the edge between them comes later. An edge is
        // followed only when both nodes held their half, so a query pinned before it was written
        // must find no path — reporting one would invent a relationship that did not exist yet.
        _ <- literalOps.setProp(start, "p", QuineValue.Integer(1L))
        _ <- literalOps.setProp(neighbour, "p", QuineValue.Integer(2L))
        _ <- nextMillisecond()
        _ = (beforeEdge = Milliseconds.currentTime())
        _ <- nextMillisecond()
        _ <- literalOps.addEdge(start, neighbour, "LATER")
        _ <- graph.requestNodeSleep(cypherHarnessNamespace, start)
        _ <- graph.requestNodeSleep(cypherHarnessNamespace, neighbour)

        before <- rowsOf(queryAt(beforeEdge))
        after <- rowsOf(queryAt(Milliseconds.currentTime()))
      } yield {
        assert(before.isEmpty, s"The edge did not exist yet, so nothing should match: $before")
        assert(after.map(_(0)) == Seq(Expr.Str(neighbour.pretty)), s"Once written it matches: ${after.map(_(0))}")
      }
    }

    it("should report a node's labels as they stood at the chosen moment") {
      val testNodeId = idProv.customIdToQid(624L)
      var boundary: Milliseconds = Milliseconds(0L)

      for {
        // Labels are part of what a node was, and are reported by `history.nodeAt` alongside its
        // properties, so they have to be rebuilt at the moment too
        _ <- literalOps.setLabels(testNodeId, Set("Draft"))
        _ <- nextMillisecond()
        _ = (boundary = Milliseconds.currentTime())
        _ <- nextMillisecond()
        _ <- literalOps.setLabels(testNodeId, Set("Published"))
        _ <- graph.requestNodeSleep(cypherHarnessNamespace, testNodeId)

        past <- rowsOf(s"""MATCH (n) WHERE strId(n) = "${testNodeId.pretty}"
                          |CALL history.nodeAt(n, ${boundary.millis}) YIELD node
                          |RETURN labels(node) AS labelsThen, labels(n) AS labelsNow""".stripMargin)
      } yield {
        assert(past.size == 1, s"Expected one row, got ${past.size}")
        assert(past(0)(0) == Expr.List(Vector(Expr.Str("Draft"))), s"Labels then: ${past(0)(0)}")
        assert(past(0)(1) == Expr.List(Vector(Expr.Str("Published"))), s"Labels now: ${past(0)(1)}")
      }
    }

    it("should report nothing of a node at a moment before it had anything") {
      val testNodeId = idProv.customIdToQid(625L)
      val beforeAnything: Milliseconds = Milliseconds.currentTime()

      for {
        _ <- nextMillisecond()
        _ <- literalOps.setProp(testNodeId, "born", QuineValue.Integer(1L))
        _ <- literalOps.addEdge(testNodeId, idProv.customIdToQid(626L), "AFTER")
        _ <- graph.requestNodeSleep(cypherHarnessNamespace, testNodeId)

        // Every node conceptually exists at every moment, so this is not an error — but the node
        // had no properties and no edges then, and reporting the ones it has now would be a plain
        // leak of the present into a view of the past
        rows <- rowsOf(s"""MATCH (n) WHERE strId(n) = "${testNodeId.pretty}"
                          |CALL history.nodeAt(n, ${beforeAnything.millis}) YIELD node, edges
                          |RETURN node.born AS born, edges""".stripMargin)
      } yield {
        assert(rows.size == 1, s"Expected one row, got ${rows.size}")
        assert(rows(0)(0) == Expr.Null, s"The property was not set yet, got ${rows(0)(0)}")
        assert(rows(0)(1) == Expr.List(Vector.empty), s"No edges were held yet, got ${rows(0)(1)}")
      }
    }

    it("should run a whole query at a chosen moment, taking the moment from data") {
      val testNodeId = idProv.customIdToQid(602L)
      var boundary: Milliseconds = Milliseconds(0L)

      for {
        _ <- literalOps.setProp(testNodeId, "score", QuineValue.Integer(1L))
        _ <- nextMillisecond()
        _ = (boundary = Milliseconds.currentTime())
        _ <- nextMillisecond()
        _ <- literalOps.setProp(testNodeId, "score", QuineValue.Integer(2L))
        // The moment is read out of the graph rather than written into the query, which is the
        // thing the at-time request parameter cannot express.
        _ <- literalOps.setProp(testNodeId, "auditAt", QuineValue.Integer(boundary.millis))
        _ <- graph.requestNodeSleep(cypherHarnessNamespace, testNodeId)

        queryText = s"""MATCH (n) WHERE strId(n) = "${testNodeId.pretty}"
                       |CALL history.queryAt(
                       |  'MATCH (m) WHERE strId(m) = $$id RETURN m.score AS score',
                       |  n.auditAt,
                       |  {id: strId(n)}
                       |) YIELD value
                       |RETURN value.score AS scoreThen, n.score AS scoreNow""".stripMargin
        results <- queryCypherValues(queryText, cypherHarnessNamespace)(graph).results
          .toMat(Sink.seq)(Keep.right)
          .run()
      } yield {
        assert(results.size == 1, s"Expected one row, got ${results.size}")
        assert(results(0)(0) == Expr.Integer(1L), s"Score at the audit moment, got ${results(0)(0)}")
        assert(results(0)(1) == Expr.Integer(2L), s"Score now, got ${results(0)(1)}")
      }
    }

    it("should name the argument actually at fault, not always the second one") {
      val testNodeId = idProv.customIdToQid(604L)

      // Each position is checked on its own. Reporting a fixed position instead would blame the
      // second argument for a bad first or third one, and blame it with a value that is perfectly
      // valid for the thing it was accused of being.
      for {
        // First argument is not a query string, though the second is a fine timestamp
        _ <- assertFailsWith(
          """CALL history.queryAt(123, 456) YIELD value RETURN value""",
          "first argument",
        )
        // Third argument is not a map, though the first two are fine
        _ <- assertFailsWith(
          """CALL history.queryAt('MATCH (n) RETURN n', 1, "not a map") YIELD value RETURN value""",
          "third argument",
        )
        // Second argument genuinely wrong, the case that was already reported correctly
        _ <- assertFailsWith(
          """CALL history.queryAt('MATCH (n) RETURN n', "not a timestamp") YIELD value RETURN value""",
          "second argument",
        )
        // Too many arguments is an arity problem, so it is a signature error rather than a
        // complaint about any one argument's type
        assertion <- recoverToSucceededIf[CypherException.WrongSignature] {
          rowsOf(
            s"""MATCH (n) WHERE strId(n) = "${testNodeId.pretty}"
               |CALL history.queryAt('MATCH (m) RETURN m', 1, {}, "extra") YIELD value RETURN value""".stripMargin,
          )
        }
      } yield assertion
    }

    it("should not read past the moment the containing query is pinned to") {
      val testNodeId = idProv.customIdToQid(606L)
      var early: Milliseconds = Milliseconds(0L)
      var late: Milliseconds = Milliseconds(0L)

      for {
        _ <- literalOps.setProp(testNodeId, "v", QuineValue.Integer(1L))
        _ <- nextMillisecond()
        _ = (early = Milliseconds.currentTime())
        _ <- nextMillisecond()
        _ <- literalOps.setProp(testNodeId, "v", QuineValue.Integer(2L))
        _ <- nextMillisecond()
        _ = (late = Milliseconds.currentTime())
        _ <- graph.requestNodeSleep(cypherHarnessNamespace, testNodeId)

        // Both procedures are asked for `late` from inside a query pinned to `early`. That query is
        // a view of the graph as it stood at `early`, so reading ahead to `late` would put a value
        // that did not yet exist into it. The two moments combine by taking the earlier, which is
        // what the journal-reading procedures already do with their `through` option.
        nodeAtQuery = s"""MATCH (n) WHERE strId(n) = "${testNodeId.pretty}"
                         |CALL history.nodeAt(n, ${late.millis}) YIELD node RETURN node.v""".stripMargin
        queryAtQuery = s"""CALL history.queryAt(
                          |  'MATCH (m) WHERE strId(m) = "${testNodeId.pretty}" RETURN m.v AS v',
                          |  ${late.millis}
                          |) YIELD value RETURN value.v""".stripMargin

        nodeAtPinned <- rowsOf(nodeAtQuery, atTime = Some(early))
        queryAtPinned <- rowsOf(queryAtQuery, atTime = Some(early))
        // Unpinned, the same calls do reach `late`, so the clamp above is the pinning's doing and
        // not the procedures failing to read that far
        nodeAtFree <- rowsOf(nodeAtQuery)
        queryAtFree <- rowsOf(queryAtQuery)
      } yield {
        assert(nodeAtPinned.map(_(0)) == Seq(Expr.Integer(1L)), s"history.nodeAt: ${nodeAtPinned.map(_(0))}")
        assert(queryAtPinned.map(_(0)) == Seq(Expr.Integer(1L)), s"history.queryAt: ${queryAtPinned.map(_(0))}")
        assert(nodeAtFree.map(_(0)) == Seq(Expr.Integer(2L)), s"history.nodeAt unpinned: ${nodeAtFree.map(_(0))}")
        assert(queryAtFree.map(_(0)) == Seq(Expr.Integer(2L)), s"history.queryAt unpinned: ${queryAtFree.map(_(0))}")
      }
    }

    it("should reject history.nodeAt's arguments by position, and the wrong arity by signature") {
      val testNodeId = idProv.customIdToQid(605L)

      for {
        // A string is not a node
        _ <- assertFailsWith(
          """CALL history.nodeAt("not a node", 1) YIELD node RETURN node""",
          "first argument",
        )
        // ...and a string is not a timestamp, though the first argument here is fine
        _ <- assertFailsWith(
          s"""MATCH (n) WHERE strId(n) = "${testNodeId.pretty}"
             |CALL history.nodeAt(n, "yesterday") YIELD node RETURN node""".stripMargin,
          "second argument",
        )
        // A missing moment is an arity problem, so it is a signature error rather than a complaint
        // about any one argument's type
        assertion <- recoverToSucceededIf[CypherException.WrongSignature] {
          rowsOf(
            s"""MATCH (n) WHERE strId(n) = "${testNodeId.pretty}"
               |CALL history.nodeAt(n) YIELD node RETURN node""".stripMargin,
          )
        }
      } yield assertion
    }

    it("should refuse to run a query that writes") {
      val testNodeId = idProv.customIdToQid(603L)
      val writingQuery =
        s"""MATCH (n) WHERE strId(n) = "${testNodeId.pretty}"
           |CALL history.queryAt('MATCH (m) SET m.x = 1 RETURN m', 1) YIELD value RETURN value""".stripMargin

      recoverToExceptionIf[CypherException.Runtime] {
        queryCypherValues(writingQuery, cypherHarnessNamespace)(graph).results.toMat(Sink.seq)(Keep.right).run()
      }.map { err =>
        assert(
          err.getMessage.contains("must be read-only"),
          s"Expected a read-only complaint, got: ${err.getMessage}",
        )
      }
    }
  }

  describe("history.edgeChanges procedure") {
    def assertHalfEdgeChanges(
      node: QuineId,
      other: QuineId,
      edgeTypeFilter: Option[String],
      expectedChanges: Seq[(String, String)], // (action, edgeType) pairs
    ): Future[Assertion] =
      assertEdgeChanges("history.edgeChanges", node, other, edgeTypeFilter, expectedChanges)

    it("should report a half edge's lifecycle from either end of the edge") {
      val sourceNodeId = idProv.customIdToQid(1L)
      val targetNodeId = idProv.customIdToQid(301L)

      for {
        // One directed edge, so the source holds an outgoing half and the target an incoming one.
        // Both halves have the same lifetime, and each node's journal records its own.
        _ <- literalOps.addEdge(sourceNodeId, targetNodeId, "RELATED_TO")
        _ <- nextMillisecond()
        _ <- literalOps.removeEdge(sourceNodeId, targetNodeId, "RELATED_TO")
        _ <- graph.requestNodeSleep(cypherHarnessNamespace, sourceNodeId)
        _ <- graph.requestNodeSleep(cypherHarnessNamespace, targetNodeId)

        // Asked from the source
        _ <- assertHalfEdgeChanges(
          sourceNodeId,
          targetNodeId,
          Some("RELATED_TO"),
          Seq("added" -> "RELATED_TO", "removed" -> "RELATED_TO"),
        )
        // ...and from the target, whose half is the incoming one. Reporting nothing here is the
        // failure this asymmetry is most prone to.
        assertion <- assertHalfEdgeChanges(
          targetNodeId,
          sourceNodeId,
          Some("RELATED_TO"),
          Seq("added" -> "RELATED_TO", "removed" -> "RELATED_TO"),
        )
      } yield assertion
    }

    it("should filter by edge type, with a null filter and an omitted one both meaning every type") {
      val otherNodeId = idProv.customIdToQid(102L)
      def call(args: String): String =
        s"""MATCH (source) WHERE strId(source) = "${qid.pretty}"
           |MATCH (target) WHERE strId(target) = "${otherNodeId.pretty}"
           |CALL history.edgeChanges($args)
           |YIELD action, edgeType, changeTime
           |RETURN action, edgeType, changeTime""".stripMargin

      for {
        _ <- literalOps.addEdge(qid, otherNodeId, "FRIEND")
        _ <- nextMillisecond()
        _ <- literalOps.addEdge(qid, otherNodeId, "COLLEAGUE")
        _ <- nextMillisecond()
        _ <- literalOps.addEdge(qid, otherNodeId, "NEIGHBOR")
        _ <- graph.requestNodeSleep(cypherHarnessNamespace, qid)

        friendResults <- rowsOf(call("""source, target, "FRIEND", null"""))
        nullFilterResults <- rowsOf(call("source, target, null, null"))
        // The trailing optional arguments genuinely omitted, rather than passed as null
        omittedResults <- rowsOf(call("source, target"))
      } yield {
        assert(friendResults.size == 1, s"Expected 1 FRIEND half edge, got ${friendResults.size}")
        assert(friendResults(0)(1) == Expr.Str("FRIEND"), "Should only return FRIEND edge type")
        assert(friendResults(0)(0) == Expr.Str("added"), "Should show FRIEND edge was added")

        // Reported in the order written, so this pins order as well as membership
        val expected = Seq("FRIEND", "COLLEAGUE", "NEIGHBOR").map(Expr.Str(_))
        assert(nullFilterResults.map(_(1)) == expected, s"null filter: got ${nullFilterResults.map(_(1))}")
        assert(nullFilterResults.forall(_(0) == Expr.Str("added")), "All should be 'added' actions")
        assert(
          omittedResults == nullFilterResults,
          s"Omitting the filter should read the same as passing null: $omittedResults vs $nullFilterResults",
        )
      }
    }

    it("should report a dangling incoming half edge whose other half never existed") {
      val sourceNodeId = idProv.customIdToQid(304L)
      val targetNodeId = idProv.customIdToQid(305L)

      for {
        // Only ever write the half edge that the target of a directed edge would hold. Nothing
        // completes it, which is exactly the kind of partial state this procedure exists to surface.
        _ <- literalOps.addHalfEdge(targetNodeId, sourceNodeId, "DANGLING", EdgeDirection.Incoming)
        _ <- graph.requestNodeSleep(cypherHarnessNamespace, targetNodeId)

        assertion <- assertHalfEdgeChanges(
          targetNodeId,
          sourceNodeId,
          Some("DANGLING"),
          Seq("added" -> "DANGLING"),
        )
      } yield assertion
    }

    it("should not report half edges pointing at some other node") {
      val sourceNodeId = idProv.customIdToQid(306L)
      val targetNodeId = idProv.customIdToQid(307L)
      val unrelatedNodeId = idProv.customIdToQid(308L)

      for {
        _ <- literalOps.addEdge(sourceNodeId, unrelatedNodeId, "ELSEWHERE")
        _ <- nextMillisecond()
        _ <- literalOps.addEdge(unrelatedNodeId, sourceNodeId, "ELSEWHERE")
        _ <- graph.requestNodeSleep(cypherHarnessNamespace, sourceNodeId)
        _ <- graph.requestNodeSleep(cypherHarnessNamespace, unrelatedNodeId)

        // Neither half edge points at the target, so neither is reported
        assertion <- assertHalfEdgeChanges(
          sourceNodeId,
          targetNodeId,
          Some("ELSEWHERE"),
          Seq.empty,
        )
      } yield assertion
    }

    it("should report every direction of half edge between one pair, and which node each points at") {
      val sourceNodeId = idProv.customIdToQid(320L)
      val targetNodeId = idProv.customIdToQid(321L)

      for {
        // Three separate half edges accumulate on the source, all pointing at the target: one of
        // each direction. Direction is part of what an edge is, so none of them displaces another.
        _ <- literalOps.addEdge(sourceNodeId, targetNodeId, "COLUMNS")
        _ <- nextMillisecond()
        _ <- literalOps.addEdge(targetNodeId, sourceNodeId, "COLUMNS")
        _ <- nextMillisecond()
        _ <- literalOps.addEdge(sourceNodeId, targetNodeId, "COLUMNS", isDirected = false)
        _ <- graph.requestNodeSleep(cypherHarnessNamespace, sourceNodeId)
        _ <- graph.requestNodeSleep(cypherHarnessNamespace, targetNodeId)

        results <- rowsOf(s"""MATCH (source) WHERE strId(source) = "${sourceNodeId.pretty}"
                             |MATCH (target) WHERE strId(target) = "${targetNodeId.pretty}"
                             |CALL history.edgeChanges(source, target, "COLUMNS", null)
                             |YIELD action, edgeType, direction, other, changeTime
                             |RETURN action, edgeType, direction, other, changeTime""".stripMargin)
      } yield {
        assert(results.size == 3, s"Expected all three half edges, got ${results.size}")
        assert(results.forall(_(0) == Expr.Str("added")), s"All should be additions, got ${results.map(_(0))}")
        assert(results.forall(_(1) == Expr.Str("COLUMNS")), s"All should be COLUMNS, got ${results.map(_(1))}")
        assert(results.forall(_(3) == Expr.Str(targetNodeId.pretty)), "Every row should point at the target")
        // Reported in the order written, each labelled as the source holds it
        assert(
          results.map(_(2)) == Seq(Expr.Str("Outgoing"), Expr.Str("Incoming"), Expr.Str("Undirected")),
          s"Expected one half edge of each direction, got ${results.map(_(2))}",
        )
      }
    }

    it("should accept every spelling of a direction and report the canonical one") {
      val nodeA = idProv.customIdToQid(360L)
      val nodeB = idProv.customIdToQid(361L)
      def call(direction: String): String =
        s"""MATCH (a) WHERE strId(a) = "${nodeA.pretty}"
           |MATCH (b) WHERE strId(b) = "${nodeB.pretty}"
           |CALL history.edgeChanges(a, b, "SPELLED", "$direction") YIELD direction
           |RETURN direction""".stripMargin

      // Every spelling the REST API accepts, so a direction reads the same whichever surface it
      // came through. Only "in" was otherwise exercised, leaving eight aliases unverified.
      val spellings = Seq(
        ("Outgoing", "Outgoing"),
        ("outgoing", "Outgoing"),
        ("out", "Outgoing"),
        ("Incoming", "Incoming"),
        ("incoming", "Incoming"),
        ("in", "Incoming"),
        ("Undirected", "Undirected"),
        ("undirected", "Undirected"),
        ("un", "Undirected"),
      )

      for {
        _ <- literalOps.addEdge(nodeA, nodeB, "SPELLED")
        _ <- nextMillisecond()
        _ <- literalOps.addEdge(nodeB, nodeA, "SPELLED")
        _ <- nextMillisecond()
        _ <- literalOps.addEdge(nodeA, nodeB, "SPELLED", isDirected = false)
        _ <- graph.requestNodeSleep(cypherHarnessNamespace, nodeA)
        _ <- graph.requestNodeSleep(cypherHarnessNamespace, nodeB)

        assertion <- spellings.foldLeft(Future.successful(succeed)) { case (acc, (spelling, canonical)) =>
          acc.flatMap { _ =>
            rowsOf(call(spelling)).map { rows =>
              assert(rows.size == 1, s"'$spelling' should select exactly one half edge, got ${rows.size}")
              assert(
                rows(0)(0) == Expr.Str(canonical),
                s"'$spelling' should report back as '$canonical', got ${rows(0)(0)}",
              )
              succeed
            }
          }
        }
      } yield assertion
    }

    it("should accept the three-argument form, and a null far node meaning any") {
      val centreNodeId = idProv.customIdToQid(362L)
      val firstNeighbour = idProv.customIdToQid(363L)
      val secondNeighbour = idProv.customIdToQid(364L)

      for {
        _ <- literalOps.addEdge(centreNodeId, firstNeighbour, "WANTED")
        _ <- nextMillisecond()
        _ <- literalOps.addEdge(centreNodeId, secondNeighbour, "WANTED")
        _ <- nextMillisecond()
        _ <- literalOps.addEdge(centreNodeId, firstNeighbour, "UNWANTED")
        _ <- graph.requestNodeSleep(cypherHarnessNamespace, centreNodeId)

        // (node, other, edgeType): the arity between the two- and four-argument forms
        threeArg <- rowsOf(s"""MATCH (n) WHERE strId(n) = "${centreNodeId.pretty}"
                              |MATCH (o) WHERE strId(o) = "${firstNeighbour.pretty}"
                              |CALL history.edgeChanges(n, o, "WANTED") YIELD edgeType, other
                              |RETURN edgeType, other""".stripMargin)

        // A null far node keeps the type filter while leaving the other end open, which no other
        // arrangement of arguments can express
        nullOther <- rowsOf(s"""MATCH (n) WHERE strId(n) = "${centreNodeId.pretty}"
                               |CALL history.edgeChanges(n, null, "WANTED") YIELD edgeType, other
                               |RETURN edgeType, other""".stripMargin)
      } yield {
        assert(threeArg.size == 1, s"Expected the one WANTED edge to that neighbour, got ${threeArg.size}")
        assert(threeArg(0)(1) == Expr.Str(firstNeighbour.pretty), s"got ${threeArg(0)(1)}")

        assert(nullOther.size == 2, s"Expected both WANTED edges regardless of far node, got ${nullOther.size}")
        assert(nullOther.forall(_(0) == Expr.Str("WANTED")), s"UNWANTED should be filtered out: ${nullOther.map(_(0))}")
        assert(
          nullOther.map(_(1)) == Seq(Expr.Str(firstNeighbour.pretty), Expr.Str(secondNeighbour.pretty)),
          s"got ${nullOther.map(_(1))}",
        )
      }
    }

    it("should accept a list of edge types, reporting an edge matching any one of them") {
      val centreNodeId = idProv.customIdToQid(370L)
      val neighbour = idProv.customIdToQid(371L)

      for {
        _ <- literalOps.addEdge(centreNodeId, neighbour, "KNOWS")
        _ <- nextMillisecond()
        _ <- literalOps.addEdge(centreNodeId, neighbour, "WORKS_WITH")
        _ <- nextMillisecond()
        _ <- literalOps.addEdge(centreNodeId, neighbour, "IGNORED")
        _ <- graph.requestNodeSleep(cypherHarnessNamespace, centreNodeId)
        _ <- graph.requestNodeSleep(cypherHarnessNamespace, neighbour)

        // The value-level counterpart of `-[:KNOWS|WORKS_WITH]->`: any one of the named types
        // matches, and the type not named is left out
        rows <- rowsOf(s"""MATCH (n) WHERE strId(n) = "${centreNodeId.pretty}"
                          |CALL history.edgeChanges(n, null, ["KNOWS", "WORKS_WITH"])
                          |YIELD edgeType RETURN edgeType""".stripMargin)
        // A single string keeps meaning exactly one type
        single <- rowsOf(s"""MATCH (n) WHERE strId(n) = "${centreNodeId.pretty}"
                            |CALL history.edgeChanges(n, null, "KNOWS")
                            |YIELD edgeType RETURN edgeType""".stripMargin)
        // A one-element list means the same as that string on its own
        singleton <- rowsOf(s"""MATCH (n) WHERE strId(n) = "${centreNodeId.pretty}"
                               |CALL history.edgeChanges(n, null, ["KNOWS"])
                               |YIELD edgeType RETURN edgeType""".stripMargin)
      } yield {
        assert(
          rows.map(_(0)) == Seq(Expr.Str("KNOWS"), Expr.Str("WORKS_WITH")),
          s"Expected both named types in the order written, got ${rows.map(_(0))}",
        )
        assert(single.map(_(0)) == Seq(Expr.Str("KNOWS")), s"got ${single.map(_(0))}")
        assert(singleton == single, s"A one-element list should match the bare string: $singleton vs $single")
      }
    }

    it("should match no edge type for an empty list, without failing the query") {
      val centreNodeId = idProv.customIdToQid(372L)
      val neighbour = idProv.customIdToQid(376L)

      for {
        _ <- literalOps.addEdge(centreNodeId, neighbour, "PRESENT")
        _ <- graph.requestNodeSleep(cypherHarnessNamespace, centreNodeId)

        // "any of nothing" matches nothing, exactly as `x IN []` does. The list is usually computed
        // rather than written out, so failing here would take down a standing query whose
        // expression happened to yield nothing, and matching everything would silently widen it.
        empty <- rowsOf(s"""MATCH (n) WHERE strId(n) = "${centreNodeId.pretty}"
                           |CALL history.edgeChanges(n, null, []) YIELD edgeType RETURN edgeType""".stripMargin)
        // The edge is really there, so the empty result is the filter's doing and not an empty node
        unfiltered <- rowsOf(s"""MATCH (n) WHERE strId(n) = "${centreNodeId.pretty}"
                                |CALL history.edgeChanges(n, null, null) YIELD edgeType RETURN edgeType""".stripMargin)
        // A computed empty list behaves the same as a literal one
        computed <- rowsOf(s"""MATCH (n) WHERE strId(n) = "${centreNodeId.pretty}"
                              |WITH n, [t IN ["PRESENT"] WHERE t = "no-such-type"] AS types
                              |CALL history.edgeChanges(n, null, types) YIELD edgeType RETURN edgeType""".stripMargin)
      } yield {
        assert(empty.isEmpty, s"An empty list should match no edge type, got $empty")
        assert(unfiltered.map(_(0)) == Seq(Expr.Str("PRESENT")), s"got ${unfiltered.map(_(0))}")
        assert(computed.isEmpty, s"A computed empty list should match no edge type, got $computed")
      }
    }

    it("should answer an empty list without reading the journal at all") {
      val nodeA = idProv.customIdToQid(377L)
      val nodeB = idProv.customIdToQid(378L)
      val halfEdgeMetrics = graph.metrics.journalWalkMetrics("history.edgeChanges")
      val wholeEdgeMetrics = graph.metrics.journalWalkMetrics("history.edgeChangesBetween")

      for {
        // A journal with plenty in it, so reading it would be plainly visible in the counter
        _ <- (1 to 10).foldLeft(Future.successful(())) { (acc, i) =>
          acc.flatMap(_ => literalOps.addEdge(nodeA, nodeB, s"TYPE_$i"))
        }
        _ <- graph.requestNodeSleep(cypherHarnessNamespace, nodeA)
        _ <- graph.requestNodeSleep(cypherHarnessNamespace, nodeB)

        halfBefore = halfEdgeMetrics.journalEventsRead.getCount
        wholeBefore = wholeEdgeMetrics.journalEventsRead.getCount
        timedBefore = halfEdgeMetrics.timer.getCount

        _ <- rowsOf(s"""MATCH (a) WHERE strId(a) = "${nodeA.pretty}"
                       |CALL history.edgeChanges(a, null, []) YIELD edgeType RETURN edgeType""".stripMargin)
        _ <- rowsOf(s"""MATCH (a) WHERE strId(a) = "${nodeA.pretty}"
                       |MATCH (b) WHERE strId(b) = "${nodeB.pretty}"
                       |CALL history.edgeChangesBetween(a, b, []) YIELD edgeType RETURN edgeType""".stripMargin)
      } yield {
        // The answer follows from the arguments, so neither journal is opened — and `edgeBetween`
        // would otherwise have opened two
        assert(
          halfEdgeMetrics.journalEventsRead.getCount == halfBefore,
          s"history.edgeChanges read ${halfEdgeMetrics.journalEventsRead.getCount - halfBefore} events for an " +
          "answer that cannot depend on them",
        )
        assert(
          wholeEdgeMetrics.journalEventsRead.getCount == wholeBefore,
          s"history.edgeChangesBetween read ${wholeEdgeMetrics.journalEventsRead.getCount - wholeBefore} events for " +
          "an answer that cannot depend on them",
        )
        // Still counted as a call, so a short-circuited query is visible rather than missing
        assert(
          halfEdgeMetrics.timer.getCount - timedBefore == 1L,
          s"Expected the call still timed, got ${halfEdgeMetrics.timer.getCount - timedBefore}",
        )
      }
    }

    it("should reject a list holding something that is not an edge type") {
      val centreNodeId = idProv.customIdToQid(373L)

      assertFailsWith(
        s"""MATCH (n) WHERE strId(n) = "${centreNodeId.pretty}"
           |CALL history.edgeChanges(n, null, ["KNOWS", 7]) YIELD edgeType RETURN edgeType""".stripMargin,
        "list of edge type strings",
      )
    }

    it("should take options only in the last argument position, padding with nulls to reach it") {
      val centreNodeId = idProv.customIdToQid(380L)
      val neighbour = idProv.customIdToQid(381L)
      var afterSecond: Milliseconds = Milliseconds(0L)

      for {
        _ <- literalOps.addEdge(centreNodeId, neighbour, "FIRST")
        _ <- nextMillisecond()
        _ <- literalOps.addEdge(centreNodeId, neighbour, "SECOND")
        _ <- nextMillisecond()
        _ = (afterSecond = Milliseconds.currentTime())
        _ <- nextMillisecond()
        _ <- literalOps.addEdge(centreNodeId, neighbour, "THIRD")
        _ <- graph.requestNodeSleep(cypherHarnessNamespace, centreNodeId)

        // Arguments are positional, as everywhere else in Cypher, so options sit in the last slot
        // and the skipped filters are passed as null rather than left out
        limited <- rowsOf(s"""MATCH (n) WHERE strId(n) = "${centreNodeId.pretty}"
                             |CALL history.edgeChanges(n, null, null, null, {limit: 2})
                             |YIELD edgeType RETURN edgeType""".stripMargin)
        // The window bounds it the same way, and both ends of the window are honoured
        windowed <- rowsOf(s"""MATCH (n) WHERE strId(n) = "${centreNodeId.pretty}"
                              |CALL history.edgeChanges(n, null, null, null, {through: ${afterSecond.millis}})
                              |YIELD edgeType RETURN edgeType""".stripMargin)
        unlimited <- rowsOf(s"""MATCH (n) WHERE strId(n) = "${centreNodeId.pretty}"
                               |CALL history.edgeChanges(n, null, null, null, null)
                               |YIELD edgeType RETURN edgeType""".stripMargin)

        // A map in an earlier position is that position's argument, not the options map. Here it
        // lands where the far node belongs, so it is rejected as one rather than quietly applied.
        _ <- assertFailsWith(
          s"""MATCH (n) WHERE strId(n) = "${centreNodeId.pretty}"
             |CALL history.edgeChanges(n, {limit: 2}) YIELD edgeType RETURN edgeType""".stripMargin,
          "second argument",
        )
      } yield {
        assert(limited.map(_(0)) == Seq(Expr.Str("FIRST"), Expr.Str("SECOND")), s"got ${limited.map(_(0))}")
        assert(windowed.map(_(0)) == Seq(Expr.Str("FIRST"), Expr.Str("SECOND")), s"got ${windowed.map(_(0))}")
        // A null options argument means every default, so it reads the same as omitting the slot
        assert(
          unlimited.map(_(0)) == Seq(Expr.Str("FIRST"), Expr.Str("SECOND"), Expr.Str("THIRD")),
          s"got ${unlimited.map(_(0))}",
        )
      }
    }

    it("should report every half edge of a node when no target is given") {
      val centreNodeId = idProv.customIdToQid(322L)
      val firstNeighbour = idProv.customIdToQid(323L)
      val secondNeighbour = idProv.customIdToQid(324L)

      for {
        _ <- literalOps.addEdge(centreNodeId, firstNeighbour, "SPOKE")
        _ <- nextMillisecond()
        _ <- literalOps.addEdge(centreNodeId, secondNeighbour, "SPOKE")
        _ <- nextMillisecond()
        _ <- literalOps.removeEdge(centreNodeId, firstNeighbour, "SPOKE")
        _ <- graph.requestNodeSleep(cypherHarnessNamespace, centreNodeId)

        oneArgQuery = s"""MATCH (n) WHERE strId(n) = "${centreNodeId.pretty}"
                         |CALL history.edgeChanges(n)
                         |YIELD action, edgeType, other, changeTime
                         |RETURN action, edgeType, other, changeTime
                         |ORDER BY changeTime""".stripMargin
        results <- queryCypherValues(oneArgQuery, cypherHarnessNamespace)(graph).results
          .toMat(Sink.seq)(Keep.right)
          .run()
      } yield {
        val expected = Seq(
          ("added", firstNeighbour.pretty),
          ("added", secondNeighbour.pretty),
          ("removed", firstNeighbour.pretty),
        )
        assert(results.size == 3, s"Expected 3 half edge changes across both neighbours, got ${results.size}")
        results.zip(expected).foreach { case (row, (action, other)) =>
          assert(row(0) == Expr.Str(action), s"Expected action $action, got ${row(0)}")
          assert(row(2) == Expr.Str(other), s"Expected other $other, got ${row(2)}")
        }
        succeed
      }
    }
  }

  describe("history.edgeChangesBetween procedure") {
    def assertWholeEdgeChanges(
      node: QuineId,
      other: QuineId,
      edgeTypeFilter: Option[String],
      expectedChanges: Seq[(String, String)], // (action, edgeType) pairs
    ): Future[Assertion] =
      assertEdgeChanges("history.edgeChangesBetween", node, other, edgeTypeFilter, expectedChanges)

    it("should not call a directed self loop whole until both of its halves exist") {
      val selfNodeId = idProv.customIdToQid(350L)
      // Captured between the two halves, so a completion reported at the first half fails here
      var betweenHalves: Milliseconds = Milliseconds(0L)

      for {
        // A node at both ends of a directed edge holds both halves itself. Until the second one is
        // written the edge is no more whole than any other half-written edge.
        _ <- literalOps.addHalfEdge(selfNodeId, selfNodeId, "SELF_DIR", EdgeDirection.Outgoing)
        _ <- nextMillisecond()
        _ <- { betweenHalves = Milliseconds.currentTime(); Future.successful(()) }
        _ <- nextMillisecond()
        _ <- literalOps.addHalfEdge(selfNodeId, selfNodeId, "SELF_DIR", EdgeDirection.Incoming)
        _ <- graph.requestNodeSleep(cypherHarnessNamespace, selfNodeId)

        query = s"""MATCH (n) WHERE strId(n) = "${selfNodeId.pretty}"
                   |CALL history.edgeChangesBetween(n, n, "SELF_DIR", null)
                   |YIELD action, direction, changeTime
                   |RETURN action, direction, changeTime ORDER BY changeTime""".stripMargin
        rows <- queryCypherValues(query, cypherHarnessNamespace)(graph).results
          .toMat(Sink.seq)(Keep.right)
          .run()
      } yield {
        assert(rows.size == 1, s"A self loop is one edge, so expected one row, got ${rows.size}: $rows")
        assert(rows(0)(0) == Expr.Str("added"), s"got ${rows(0)(0)}")
        assert(
          rows(0)(2).asInstanceOf[Expr.Integer].long > betweenHalves.millis,
          "The edge became whole when the second half was written, not the first",
        )
      }
    }

    it("should not call a directed self loop whole when its incoming half is written first") {
      val selfNodeId = idProv.customIdToQid(352L)
      var betweenHalves: Milliseconds = Milliseconds(0L)

      for {
        // The mirror of the previous case. Neither half of a directed loop is the whole edge, so
        // whichever is recorded first leaves it incomplete until the other arrives.
        _ <- literalOps.addHalfEdge(selfNodeId, selfNodeId, "SELF_REV", EdgeDirection.Incoming)
        _ <- nextMillisecond()
        _ <- { betweenHalves = Milliseconds.currentTime(); Future.successful(()) }
        _ <- nextMillisecond()
        _ <- literalOps.addHalfEdge(selfNodeId, selfNodeId, "SELF_REV", EdgeDirection.Outgoing)
        _ <- graph.requestNodeSleep(cypherHarnessNamespace, selfNodeId)

        query = s"""MATCH (n) WHERE strId(n) = "${selfNodeId.pretty}"
                   |CALL history.edgeChangesBetween(n, n, "SELF_REV", null)
                   |YIELD action, direction, changeTime
                   |RETURN action, direction, changeTime ORDER BY changeTime""".stripMargin
        rows <- queryCypherValues(query, cypherHarnessNamespace)(graph).results
          .toMat(Sink.seq)(Keep.right)
          .run()
      } yield {
        assert(rows.size == 1, s"A self loop is one edge, so expected one row, got ${rows.size}: $rows")
        assert(rows(0)(0) == Expr.Str("added"), s"got ${rows(0)(0)}")
        assert(rows(0)(1) == Expr.Str("Outgoing"), s"A loop runs from the node to itself: ${rows(0)(1)}")
        assert(
          rows(0)(2).asInstanceOf[Expr.Integer].long > betweenHalves.millis,
          "The edge became whole when the second half was written, not the first",
        )
      }
    }

    it("should treat an undirected self loop's single half edge as the whole edge") {
      val selfNodeId = idProv.customIdToQid(351L)

      for {
        // Both halves of an undirected edge are spelled the same way, so a node at both ends holds
        // one half edge that is the entire edge. There is no second half to wait for.
        _ <- literalOps.addHalfEdge(selfNodeId, selfNodeId, "SELF_UN", EdgeDirection.Undirected)
        _ <- graph.requestNodeSleep(cypherHarnessNamespace, selfNodeId)

        query = s"""MATCH (n) WHERE strId(n) = "${selfNodeId.pretty}"
                   |CALL history.edgeChangesBetween(n, n, "SELF_UN", null)
                   |YIELD action, direction, changeTime
                   |RETURN action, direction, changeTime ORDER BY changeTime""".stripMargin
        rows <- queryCypherValues(query, cypherHarnessNamespace)(graph).results
          .toMat(Sink.seq)(Keep.right)
          .run()
      } yield {
        assert(rows.size == 1, s"Expected the loop reported once, got ${rows.size}: $rows")
        assert(rows(0)(0) == Expr.Str("added"), s"got ${rows(0)(0)}")
        assert(rows(0)(1) == Expr.Str("Undirected"), s"got ${rows(0)(1)}")
      }
    }

    it("should report nothing for an edge that was never whole, whether half-written or absent") {
      val centreNodeId = idProv.customIdToQid(330L)
      val danglingNeighbour = idProv.customIdToQid(333L)

      for {
        // Only the centre's half is ever written, so this edge never becomes whole
        _ <- literalOps.addHalfEdge(centreNodeId, danglingNeighbour, "DANGLE", EdgeDirection.Outgoing)
        _ <- graph.requestNodeSleep(cypherHarnessNamespace, centreNodeId)

        _ <- assertWholeEdgeChanges(
          centreNodeId,
          danglingNeighbour,
          Some("DANGLE"),
          Seq.empty,
        )
        // The baseline: an edge type neither node ever recorded at all. Distinguished from the
        // above because one reads a journal that holds a matching half and rejects it, while this
        // reads a journal with nothing of the sort in it.
        assertion <- assertWholeEdgeChanges(
          centreNodeId,
          danglingNeighbour,
          Some("NONEXISTENT"),
          Seq.empty,
        )
      } yield assertion
    }

    it("should report every direction of edge between the two nodes, and filter to one") {
      val nodeA = idProv.customIdToQid(340L)
      val nodeB = idProv.customIdToQid(341L)

      for {
        _ <- literalOps.addEdge(nodeA, nodeB, "DIR")
        _ <- nextMillisecond()
        _ <- literalOps.addEdge(nodeB, nodeA, "DIR")
        _ <- nextMillisecond()
        _ <- literalOps.addEdge(nodeA, nodeB, "DIR", isDirected = false)
        _ <- graph.requestNodeSleep(cypherHarnessNamespace, nodeA)
        _ <- graph.requestNodeSleep(cypherHarnessNamespace, nodeB)

        // The direction an edge runs is part of what the edge is, not a restriction on reading it,
        // so with no filter all three are reported, each labelled as node A holds it.
        allQuery = s"""MATCH (a) WHERE strId(a) = "${nodeA.pretty}"
                      |MATCH (b) WHERE strId(b) = "${nodeB.pretty}"
                      |CALL history.edgeChangesBetween(a, b, "DIR", null)
                      |YIELD action, direction, changeTime
                      |RETURN action, direction, changeTime ORDER BY changeTime""".stripMargin
        all <- queryCypherValues(allQuery, cypherHarnessNamespace)(graph).results
          .toMat(Sink.seq)(Keep.right)
          .run()

        incomingQuery = s"""MATCH (a) WHERE strId(a) = "${nodeA.pretty}"
                           |MATCH (b) WHERE strId(b) = "${nodeB.pretty}"
                           |CALL history.edgeChangesBetween(a, b, "DIR", "in")
                           |YIELD action, direction, changeTime
                           |RETURN action, direction, changeTime ORDER BY changeTime""".stripMargin
        incoming <- queryCypherValues(incomingQuery, cypherHarnessNamespace)(graph).results
          .toMat(Sink.seq)(Keep.right)
          .run()
      } yield {
        assert(
          all.map(_(1)) == Seq(Expr.Str("Outgoing"), Expr.Str("Incoming"), Expr.Str("Undirected")),
          s"Expected all three directions, got ${all.map(_(1))}",
        )
        assert(all.forall(_(0) == Expr.Str("added")), s"All should be additions, got ${all.map(_(0))}")

        // A short spelling is accepted, and the canonical capitalised one is reported back
        assert(incoming.size == 1, s"Expected only the incoming edge, got ${incoming.size}")
        assert(incoming(0)(1) == Expr.Str("Incoming"), s"Expected Incoming, got ${incoming(0)(1)}")
      }
    }

    it("should keep opposing directed edges of the same type separate") {
      val nodeA = idProv.customIdToQid(204L)
      val nodeB = idProv.customIdToQid(205L)

      for {
        // A -> B is created and never touched again
        _ <- literalOps.addEdge(nodeA, nodeB, "SAME")
        _ <- nextMillisecond()
        // B -> A is a *different* edge, created and then removed
        _ <- literalOps.addEdge(nodeB, nodeA, "SAME")
        _ <- nextMillisecond()
        _ <- literalOps.removeEdge(nodeB, nodeA, "SAME")
        _ <- graph.requestNodeSleep(cypherHarnessNamespace, nodeA)
        _ <- graph.requestNodeSleep(cypherHarnessNamespace, nodeB)

        // Both edges are reported, but as separate lifecycles: the removal of B -> A must not read
        // as a removal of A -> B, which still exists.
        query = s"""MATCH (a) WHERE strId(a) = "${nodeA.pretty}"
                   |MATCH (b) WHERE strId(b) = "${nodeB.pretty}"
                   |CALL history.edgeChangesBetween(a, b, "SAME", null)
                   |YIELD action, direction, changeTime
                   |RETURN action, direction, changeTime ORDER BY changeTime""".stripMargin
        rows <- queryCypherValues(query, cypherHarnessNamespace)(graph).results
          .toMat(Sink.seq)(Keep.right)
          .run()
      } yield {
        val seen = rows.map(r => (r(0), r(1)))
        assert(
          seen == Seq(
            (Expr.Str("added"), Expr.Str("Outgoing")),
            (Expr.Str("added"), Expr.Str("Incoming")),
            (Expr.Str("removed"), Expr.Str("Incoming")),
          ),
          s"Expected the outgoing edge to survive the incoming one's removal, got $seen",
        )
      }
    }

    it("should keep a directed edge separate from an undirected edge of the same type") {
      val sourceNodeId = idProv.customIdToQid(206L)
      val targetNodeId = idProv.customIdToQid(207L)

      for {
        _ <- literalOps.addEdge(sourceNodeId, targetNodeId, "BOTH")
        _ <- nextMillisecond()
        _ <- literalOps.addEdge(sourceNodeId, targetNodeId, "BOTH", isDirected = false)
        _ <- nextMillisecond()
        _ <- literalOps.removeEdge(sourceNodeId, targetNodeId, "BOTH", isDirected = false)
        _ <- graph.requestNodeSleep(cypherHarnessNamespace, sourceNodeId)
        _ <- graph.requestNodeSleep(cypherHarnessNamespace, targetNodeId)

        // The directed edge stays; only the undirected one is removed
        assertion <- assertWholeEdgeChanges(
          sourceNodeId,
          targetNodeId,
          Some("BOTH"),
          Seq("added" -> "BOTH", "added" -> "BOTH", "removed" -> "BOTH"),
        )
      } yield assertion
    }

    it("should report one clean lifecycle when both halves are torn down back to back") {
      val sourceNodeId = idProv.customIdToQid(342L)
      val targetNodeId = idProv.customIdToQid(343L)

      for {
        // Build a complete edge, then tear both halves down back to back. Whether the two removals
        // land in the same millisecond is up to the clock and cannot be forced from here, so this
        // pins the lifecycle rather than the tie order: the edge is reported whole once and broken
        // once either way. Cross-node tie ordering is instead made deterministic by construction,
        // by the total ordering `completeEdgeChanges` merges on.
        _ <- literalOps.addHalfEdge(sourceNodeId, targetNodeId, "SAME_MILLI", EdgeDirection.Outgoing)
        _ <- literalOps.addHalfEdge(targetNodeId, sourceNodeId, "SAME_MILLI", EdgeDirection.Incoming)
        _ <- nextMillisecond()
        _ <- literalOps.removeHalfEdge(targetNodeId, sourceNodeId, "SAME_MILLI", EdgeDirection.Incoming)
        _ <- literalOps.removeHalfEdge(sourceNodeId, targetNodeId, "SAME_MILLI", EdgeDirection.Outgoing)
        _ <- graph.requestNodeSleep(cypherHarnessNamespace, sourceNodeId)
        _ <- graph.requestNodeSleep(cypherHarnessNamespace, targetNodeId)

        // Whichever half is seen first, the edge is whole once and then broken once
        assertion <- assertWholeEdgeChanges(
          sourceNodeId,
          targetNodeId,
          Some("SAME_MILLI"),
          Seq("added" -> "SAME_MILLI", "removed" -> "SAME_MILLI"),
        )
      } yield assertion
    }

    it("should require the target's matching half edge, not just any half edge back to the source") {
      val sourceNodeId = idProv.customIdToQid(209L)
      val targetNodeId = idProv.customIdToQid(210L)

      for {
        // The source half of a directed edge source -> target
        _ <- literalOps.addHalfEdge(sourceNodeId, targetNodeId, "PARTIAL", EdgeDirection.Outgoing)
        _ <- nextMillisecond()
        // An outgoing half edge on the target is the source half of the *opposite* edge, so it does
        // not complete source -> target
        _ <- literalOps.addHalfEdge(targetNodeId, sourceNodeId, "PARTIAL", EdgeDirection.Outgoing)
        _ <- graph.requestNodeSleep(cypherHarnessNamespace, sourceNodeId)
        _ <- graph.requestNodeSleep(cypherHarnessNamespace, targetNodeId)
        assertion <- assertWholeEdgeChanges(sourceNodeId, targetNodeId, Some("PARTIAL"), Seq.empty)
      } yield assertion
    }

    it("should report a whole edge built and torn down through both halves at once") {
      val otherNodeId = idProv.customIdToQid(10L)

      for {
        // `addEdge` and `removeEdge` write both halves in one operation, so the edge is never
        // half-written. This is the ordinary path, against the tests either side of it that drive
        // each half separately.
        _ <- literalOps.addEdge(qid, otherNodeId, "COMPLETE_EDGE")
        _ <- nextMillisecond()
        _ <- literalOps.removeEdge(qid, otherNodeId, "COMPLETE_EDGE")
        _ <- graph.requestNodeSleep(cypherHarnessNamespace, qid)
        _ <- graph.requestNodeSleep(cypherHarnessNamespace, otherNodeId)

        assertion <- assertWholeEdgeChanges(
          qid,
          otherNodeId,
          Some("COMPLETE_EDGE"),
          Seq("added" -> "COMPLETE_EDGE", "removed" -> "COMPLETE_EDGE"),
        )
      } yield assertion
    }

    it("should accept a list of edge types, keeping each matching edge's lifecycle separate") {
      val nodeA = idProv.customIdToQid(374L)
      val nodeB = idProv.customIdToQid(375L)

      for {
        _ <- literalOps.addEdge(nodeA, nodeB, "KNOWS")
        _ <- nextMillisecond()
        _ <- literalOps.addEdge(nodeA, nodeB, "WORKS_WITH")
        _ <- nextMillisecond()
        _ <- literalOps.addEdge(nodeA, nodeB, "IGNORED")
        _ <- nextMillisecond()
        // Removing one of the named types must not read as removing the other
        _ <- literalOps.removeEdge(nodeA, nodeB, "KNOWS")
        _ <- graph.requestNodeSleep(cypherHarnessNamespace, nodeA)
        _ <- graph.requestNodeSleep(cypherHarnessNamespace, nodeB)

        rows <- rowsOf(s"""MATCH (a) WHERE strId(a) = "${nodeA.pretty}"
                          |MATCH (b) WHERE strId(b) = "${nodeB.pretty}"
                          |CALL history.edgeChangesBetween(a, b, ["KNOWS", "WORKS_WITH"])
                          |YIELD action, edgeType RETURN action, edgeType""".stripMargin)
      } yield assert(
        rows.map(r => (r(0), r(1))) == Seq(
          (Expr.Str("added"), Expr.Str("KNOWS")),
          (Expr.Str("added"), Expr.Str("WORKS_WITH")),
          (Expr.Str("removed"), Expr.Str("KNOWS")),
        ),
        s"Expected both named types tracked separately and IGNORED left out, got ${rows.map(r => (r(0), r(1)))}",
      )
    }

    it("should report every edge type between the two nodes when the filter is null") {
      val otherNodeId = idProv.customIdToQid(40L)

      for {
        // Several types between the same pair, so a null filter has something to widen to
        _ <- literalOps.addEdge(qid, otherNodeId, "FRIEND")
        _ <- nextMillisecond()
        _ <- literalOps.addEdge(qid, otherNodeId, "COLLEAGUE")
        _ <- nextMillisecond()
        _ <- literalOps.addEdge(qid, otherNodeId, "NEIGHBOR")
        _ <- graph.requestNodeSleep(cypherHarnessNamespace, qid)
        _ <- graph.requestNodeSleep(cypherHarnessNamespace, otherNodeId)

        // A null edge type filter reports every type, in the order the edges were created
        assertion <- assertWholeEdgeChanges(
          qid,
          otherNodeId,
          None,
          Seq("added" -> "FRIEND", "added" -> "COLLEAGUE", "added" -> "NEIGHBOR"),
        )
      } yield assertion
    }

    it("should re-form an edge each time the missing half comes back") {
      val sourceNodeId = idProv.customIdToQid(103L)
      val targetNodeId = idProv.customIdToQid(104L)

      for {
        // The edge is broken and repaired twice, alternating which half goes: first the source's,
        // then the target's. Whichever half is missing, the edge is not whole.
        _ <- literalOps.addHalfEdge(sourceNodeId, targetNodeId, "RAPID", EdgeDirection.Outgoing)
        _ <- nextMillisecond()
        _ <- literalOps.addHalfEdge(targetNodeId, sourceNodeId, "RAPID", EdgeDirection.Incoming)
        _ <- nextMillisecond()
        _ <- literalOps.removeHalfEdge(sourceNodeId, targetNodeId, "RAPID", EdgeDirection.Outgoing)
        _ <- nextMillisecond()
        _ <- literalOps.addHalfEdge(sourceNodeId, targetNodeId, "RAPID", EdgeDirection.Outgoing)
        _ <- nextMillisecond()
        _ <- literalOps.removeHalfEdge(targetNodeId, sourceNodeId, "RAPID", EdgeDirection.Incoming)
        _ <- nextMillisecond()
        _ <- literalOps.addHalfEdge(targetNodeId, sourceNodeId, "RAPID", EdgeDirection.Incoming)

        _ <- graph.requestNodeSleep(cypherHarnessNamespace, sourceNodeId)
        _ <- graph.requestNodeSleep(cypherHarnessNamespace, targetNodeId)

        results <- assertWholeEdgeChanges(
          sourceNodeId,
          targetNodeId,
          Some("RAPID"),
          // Every write is separated by a millisecond, so the sequence is fully determined rather
          // than dependent on the clock: the edge forms when the second half arrives, breaks each
          // time either half goes, and re-forms each time the missing half is restored.
          Seq(
            "added" -> "RAPID",
            "removed" -> "RAPID",
            "added" -> "RAPID",
            "removed" -> "RAPID",
            "added" -> "RAPID",
          ),
        )
      } yield results
    }

    it("should time an edge from its second half's arrival to its first half's departure") {
      val sourceNodeId = idProv.customIdToQid(105L)
      val targetNodeId = idProv.customIdToQid(106L)
      var targetAddTime: Milliseconds = Milliseconds(0L)
      var sourceRemoveTime: Milliseconds = Milliseconds(0L)
      var targetRemoveTime: Milliseconds = Milliseconds(0L)

      for {
        // The edge becomes whole at the *later* of the two additions and breaks at the *earlier* of
        // the two removals, so the moments to bracket are the target's addition and the source's
        // removal. Each is read just before the write it names.
        _ <- literalOps.addHalfEdge(sourceNodeId, targetNodeId, "TIMESTAMP_TEST", EdgeDirection.Outgoing)
        _ <- nextMillisecond()

        _ = (targetAddTime = Milliseconds.currentTime())
        _ <- literalOps.addHalfEdge(targetNodeId, sourceNodeId, "TIMESTAMP_TEST", EdgeDirection.Incoming)
        _ <- nextMillisecond()

        _ = (sourceRemoveTime = Milliseconds.currentTime())
        _ <- literalOps.removeHalfEdge(sourceNodeId, targetNodeId, "TIMESTAMP_TEST", EdgeDirection.Outgoing)
        _ <- nextMillisecond()

        _ = (targetRemoveTime = Milliseconds.currentTime())
        _ <- literalOps.removeHalfEdge(targetNodeId, sourceNodeId, "TIMESTAMP_TEST", EdgeDirection.Incoming)

        _ <- graph.requestNodeSleep(cypherHarnessNamespace, sourceNodeId)
        _ <- graph.requestNodeSleep(cypherHarnessNamespace, targetNodeId)

        results <- rowsOf(s"""MATCH (source) WHERE strId(source) = "${sourceNodeId.pretty}"
                             |MATCH (target) WHERE strId(target) = "${targetNodeId.pretty}"
                             |CALL history.edgeChangesBetween(source, target, "TIMESTAMP_TEST", null)
                             |YIELD action, edgeType, changeTime
                             |RETURN action, edgeType, changeTime""".stripMargin)
      } yield {
        assert(
          results.map(_(0)) == Seq(Expr.Str("added"), Expr.Str("removed")),
          s"Expected one whole-edge lifecycle, got ${results.map(_(0))}",
        )

        val edgeCreationTime = results(0)(2).asInstanceOf[Expr.Integer].long
        val edgeRemovalTime = results(1)(2).asInstanceOf[Expr.Integer].long

        // Each moment is bracketed on both sides, so reporting the wrong half's timestamp fails.
        // The four writes are a millisecond apart, so these windows do not overlap.
        assert(
          edgeCreationTime >= targetAddTime.millis && edgeCreationTime < sourceRemoveTime.millis,
          s"The edge became whole when the second half was added, not the first: $edgeCreationTime " +
          s"should be in [${targetAddTime.millis}, ${sourceRemoveTime.millis})",
        )
        assert(
          edgeRemovalTime >= sourceRemoveTime.millis && edgeRemovalTime < targetRemoveTime.millis,
          s"The edge broke when the first half was removed, not the second: $edgeRemovalTime " +
          s"should be in [${sourceRemoveTime.millis}, ${targetRemoveTime.millis})",
        )
      }
    }

    it("should report nothing when the two halves take turns and never overlap") {
      val sourceNodeId = idProv.customIdToQid(50L)
      val targetNodeId = idProv.customIdToQid(51L)

      for {
        // Both halves exist at some point, and each is written and withdrawn twice — but never
        // while the other is present. Each half on its own is plenty for `history.edgeChanges`;
        // this procedure needs them to overlap, and they never do.
        _ <- literalOps.addHalfEdge(sourceNodeId, targetNodeId, "NEVER_COMPLETE", EdgeDirection.Outgoing)
        _ <- nextMillisecond()
        _ <- literalOps.removeHalfEdge(sourceNodeId, targetNodeId, "NEVER_COMPLETE", EdgeDirection.Outgoing)
        _ <- nextMillisecond()

        _ <- literalOps.addHalfEdge(targetNodeId, sourceNodeId, "NEVER_COMPLETE", EdgeDirection.Incoming)
        _ <- nextMillisecond()
        _ <- literalOps.removeHalfEdge(targetNodeId, sourceNodeId, "NEVER_COMPLETE", EdgeDirection.Incoming)
        _ <- nextMillisecond()

        // ...and again with the two halves in the opposite order
        _ <- literalOps.addHalfEdge(targetNodeId, sourceNodeId, "NEVER_COMPLETE", EdgeDirection.Incoming)
        _ <- nextMillisecond()
        _ <- literalOps.removeHalfEdge(targetNodeId, sourceNodeId, "NEVER_COMPLETE", EdgeDirection.Incoming)
        _ <- nextMillisecond()

        _ <- literalOps.addHalfEdge(sourceNodeId, targetNodeId, "NEVER_COMPLETE", EdgeDirection.Outgoing)
        _ <- nextMillisecond()
        _ <- literalOps.removeHalfEdge(sourceNodeId, targetNodeId, "NEVER_COMPLETE", EdgeDirection.Outgoing)

        _ <- graph.requestNodeSleep(cypherHarnessNamespace, sourceNodeId)
        _ <- graph.requestNodeSleep(cypherHarnessNamespace, targetNodeId)

        _ <- assertWholeEdgeChanges(sourceNodeId, targetNodeId, Some("NEVER_COMPLETE"), Seq.empty)

        // The halves really were written, so the empty result above is this procedure's pairing
        // rule and not an empty journal: the source's own two came and went.
        assertion <- assertEdgeChanges(
          "history.edgeChanges",
          sourceNodeId,
          targetNodeId,
          Some("NEVER_COMPLETE"),
          Seq(
            "added" -> "NEVER_COMPLETE",
            "removed" -> "NEVER_COMPLETE",
            "added" -> "NEVER_COMPLETE",
            "removed" -> "NEVER_COMPLETE",
          ),
        )
      } yield assertion
    }

    it("should accept options in its last argument position, bounding and capping what it reports") {
      val nodeA = idProv.customIdToQid(390L)
      val nodeB = idProv.customIdToQid(391L)
      var afterSecond: Milliseconds = Milliseconds(0L)

      def call(opts: String) =
        s"""MATCH (a) WHERE strId(a) = "${nodeA.pretty}"
           |MATCH (b) WHERE strId(b) = "${nodeB.pretty}"
           |CALL history.edgeChangesBetween(a, b, null, null, $opts)
           |YIELD edgeType RETURN edgeType""".stripMargin

      for {
        // Three whole edges, each formed in its own millisecond, so a window and a cap both have
        // an unambiguous answer
        _ <- literalOps.addEdge(nodeA, nodeB, "ONE")
        _ <- nextMillisecond()
        _ <- literalOps.addEdge(nodeA, nodeB, "TWO")
        _ <- nextMillisecond()
        _ = (afterSecond = Milliseconds.currentTime())
        _ <- nextMillisecond()
        _ <- literalOps.addEdge(nodeA, nodeB, "THREE")
        _ <- graph.requestNodeSleep(cypherHarnessNamespace, nodeA)
        _ <- graph.requestNodeSleep(cypherHarnessNamespace, nodeB)

        unbounded <- rowsOf(call("null"))
        limited <- rowsOf(call("{limit: 2}"))
        through <- rowsOf(call(s"{through: ${afterSecond.millis}}"))
        since <- rowsOf(call(s"{since: ${afterSecond.millis}}"))
      } yield {
        assert(
          unbounded.map(_(0)) == Seq(Expr.Str("ONE"), Expr.Str("TWO"), Expr.Str("THREE")),
          s"A null options argument means every default: ${unbounded.map(_(0))}",
        )
        // The cap applies to whole-edge rows, and cancels the reads behind them
        assert(limited.map(_(0)) == Seq(Expr.Str("ONE"), Expr.Str("TWO")), s"${limited.map(_(0))}")
        assert(through.map(_(0)) == Seq(Expr.Str("ONE"), Expr.Str("TWO")), s"${through.map(_(0))}")
        // Only the third edge forms inside this window. The two formed before it are not reported
        // again, because nothing about them changed inside the window.
        assert(since.map(_(0)) == Seq(Expr.Str("THREE")), s"${since.map(_(0))}")
      }
    }

    it("should report a change inside the window to an edge that was built before it") {
      val nodeA = idProv.customIdToQid(395L)
      val nodeB = idProv.customIdToQid(396L)
      var windowStart: Milliseconds = Milliseconds(0L)

      def call(procedure: String, opts: String) =
        s"""MATCH (a) WHERE strId(a) = "${nodeA.pretty}"
           |MATCH (b) WHERE strId(b) = "${nodeB.pretty}"
           |CALL $procedure(a, b, "WINDOWED", null$opts)
           |YIELD action, changeTime RETURN action, changeTime""".stripMargin

      for {
        // The edge is whole before the window opens and is dropped inside it. Neither of the halves
        // that built it is in the journal read, so the removal has to stand on its own: a half edge
        // cannot be dropped unless it was held, and dropping either half breaks the edge. Both
        // halves are removed here and both removals are inside the window, so this also pins that
        // one edge breaking is reported once rather than once per half.
        _ <- literalOps.addEdge(nodeA, nodeB, "WINDOWED")
        _ <- nextMillisecond()
        _ = (windowStart = Milliseconds.currentTime())
        _ <- nextMillisecond()
        _ <- literalOps.removeEdge(nodeA, nodeB, "WINDOWED")
        _ <- graph.requestNodeSleep(cypherHarnessNamespace, nodeA)
        _ <- graph.requestNodeSleep(cypherHarnessNamespace, nodeB)

        unbounded <- rowsOf(call("history.edgeChangesBetween", ""))
        windowed <- rowsOf(call("history.edgeChangesBetween", s", {since: ${windowStart.millis}}"))
        // The half edge view has no such state to carry, so it is the control: whatever the window
        // does to it is what the window alone does.
        halves <- rowsOf(call("history.edgeChanges", s", {since: ${windowStart.millis}}"))
      } yield {
        val addTime = unbounded(0)(1).asInstanceOf[Expr.Integer].long
        val removeTime = unbounded(1)(1).asInstanceOf[Expr.Integer].long
        // Pin the fixture itself, so a failure below cannot be blamed on the window landing in the
        // wrong place
        assert(
          addTime < windowStart.millis && windowStart.millis < removeTime,
          s"The window should open between the two changes: $addTime < ${windowStart.millis} < $removeTime",
        )

        assert(
          unbounded.map(_(0)) == Seq(Expr.Str("added"), Expr.Str("removed")),
          s"Unbounded: ${unbounded.map(_(0))}",
        )
        assert(halves.map(_(0)) == Seq(Expr.Str("removed")), s"Half edge control: ${halves.map(_(0))}")
        assert(
          windowed.map(_(0)) == Seq(Expr.Str("removed")),
          s"The removal is inside the window and must be reported, got ${windowed.map(_(0))}",
        )
        assert(
          windowed.map(_(1)) == Seq(Expr.Integer(removeTime)),
          s"...at the moment it happened, got ${windowed.map(_(1))}",
        )
      }
    }

    it("should miss an addition whose two halves fall either side of the since bound") {
      val nodeA = idProv.customIdToQid(399L)
      val nodeB = idProv.customIdToQid(400L)
      var windowStart: Milliseconds = Milliseconds(0L)

      def call(opts: String) =
        s"""MATCH (a) WHERE strId(a) = "${nodeA.pretty}"
           |MATCH (b) WHERE strId(b) = "${nodeB.pretty}"
           |CALL history.edgeChangesBetween(a, b, "LATE_HALF", null$opts)
           |YIELD action RETURN action""".stripMargin

      for {
        // The edge does become whole inside the window, but only one of the two additions that make
        // it whole is inside it. An addition is reported only when both halves are seen being
        // added, so this is not reported: the window holds half a creation, not a creation. Two
        // contiguous windows can therefore miss an edge's creation between them, which is the
        // documented cost of narrowing with `since`.
        _ <- literalOps.addHalfEdge(nodeA, nodeB, "LATE_HALF", EdgeDirection.Outgoing)
        _ <- nextMillisecond()
        _ = (windowStart = Milliseconds.currentTime())
        _ <- nextMillisecond()
        _ <- literalOps.addHalfEdge(nodeB, nodeA, "LATE_HALF", EdgeDirection.Incoming)
        _ <- graph.requestNodeSleep(cypherHarnessNamespace, nodeA)
        _ <- graph.requestNodeSleep(cypherHarnessNamespace, nodeB)

        windowed <- rowsOf(call(s", {since: ${windowStart.millis}}"))
        // Unbounded, both additions are read and the edge is reported, so the empty result above is
        // the window's doing rather than the pairing rule failing outright
        unbounded <- rowsOf(call(""))
      } yield {
        assert(windowed.isEmpty, s"Only half the creation is in the window, so nothing is reported: $windowed")
        assert(unbounded.map(_(0)) == Seq(Expr.Str("added")), s"Unbounded: ${unbounded.map(_(0))}")
      }
    }

    it("should report an edge breaking inside the window when only one half is dropped") {
      val nodeA = idProv.customIdToQid(401L)
      val nodeB = idProv.customIdToQid(402L)
      var windowStart: Milliseconds = Milliseconds(0L)

      for {
        // An edge breaks as soon as either half goes, and the surviving half is never removed, so
        // there is no second removal to wait for. A removal is evidence in itself that its own half
        // was held, which is what makes this reportable where the addition above is not.
        _ <- literalOps.addEdge(nodeA, nodeB, "ONE_SIDED")
        _ <- nextMillisecond()
        _ = (windowStart = Milliseconds.currentTime())
        _ <- nextMillisecond()
        _ <- literalOps.removeHalfEdge(nodeA, nodeB, "ONE_SIDED", EdgeDirection.Outgoing)
        _ <- graph.requestNodeSleep(cypherHarnessNamespace, nodeA)
        _ <- graph.requestNodeSleep(cypherHarnessNamespace, nodeB)

        windowed <- rowsOf(s"""MATCH (a) WHERE strId(a) = "${nodeA.pretty}"
                              |MATCH (b) WHERE strId(b) = "${nodeB.pretty}"
                              |CALL history.edgeChangesBetween(a, b, "ONE_SIDED", null, {since: ${windowStart.millis}})
                              |YIELD action RETURN action""".stripMargin)
      } yield assert(
        windowed.map(_(0)) == Seq(Expr.Str("removed")),
        s"Dropping either half breaks the edge, so it must be reported: ${windowed.map(_(0))}",
      )
    }

    it("should over-report a windowed removal of a half edge that never had a counterpart") {
      val nodeA = idProv.customIdToQid(397L)
      val nodeB = idProv.customIdToQid(398L)
      var windowStart: Milliseconds = Milliseconds(0L)

      def call(opts: String) =
        s"""MATCH (a) WHERE strId(a) = "${nodeA.pretty}"
           |MATCH (b) WHERE strId(b) = "${nodeB.pretty}"
           |CALL history.edgeChangesBetween(a, b, "LONELY", null$opts)
           |YIELD action RETURN action""".stripMargin

      for {
        // No edge ever existed here: one half was written and is now being tidied up. A windowed
        // call cannot tell that from an edge being taken apart, because the counterpart it would
        // have to check was never written and so left no event to read. The removal is reported
        // rather than dropped, which is the other half of the documented `since` skew — additions
        // are lost, removals are invented.
        _ <- literalOps.addHalfEdge(nodeA, nodeB, "LONELY", EdgeDirection.Outgoing)
        _ <- nextMillisecond()
        _ = (windowStart = Milliseconds.currentTime())
        _ <- nextMillisecond()
        _ <- literalOps.removeHalfEdge(nodeA, nodeB, "LONELY", EdgeDirection.Outgoing)
        _ <- graph.requestNodeSleep(cypherHarnessNamespace, nodeA)
        _ <- graph.requestNodeSleep(cypherHarnessNamespace, nodeB)

        windowed <- rowsOf(call(s", {since: ${windowStart.millis}}"))
        // The same data read unbounded is exact: the addition of the lone half is seen, so the edge
        // is known never to have been whole and nothing is reported
        unbounded <- rowsOf(call(""))
      } yield {
        assert(windowed.map(_(0)) == Seq(Expr.Str("removed")), s"Windowed: ${windowed.map(_(0))}")
        assert(unbounded.isEmpty, s"Unbounded, the edge is known never to have been whole: $unbounded")
      }
    }

    it("should accept every spelling of a direction and report the canonical one") {
      val nodeA = idProv.customIdToQid(392L)
      val nodeB = idProv.customIdToQid(393L)
      def call(direction: String): String =
        s"""MATCH (a) WHERE strId(a) = "${nodeA.pretty}"
           |MATCH (b) WHERE strId(b) = "${nodeB.pretty}"
           |CALL history.edgeChangesBetween(a, b, "SPELLED", "$direction") YIELD direction
           |RETURN direction""".stripMargin

      // This procedure has its own copy of the direction parsing, so the aliases are checked here
      // as well as on `history.edgeChanges`
      val spellings = Seq(
        ("Outgoing", "Outgoing"),
        ("outgoing", "Outgoing"),
        ("out", "Outgoing"),
        ("Incoming", "Incoming"),
        ("incoming", "Incoming"),
        ("in", "Incoming"),
        ("Undirected", "Undirected"),
        ("undirected", "Undirected"),
        ("un", "Undirected"),
      )

      for {
        _ <- literalOps.addEdge(nodeA, nodeB, "SPELLED")
        _ <- nextMillisecond()
        _ <- literalOps.addEdge(nodeB, nodeA, "SPELLED")
        _ <- nextMillisecond()
        _ <- literalOps.addEdge(nodeA, nodeB, "SPELLED", isDirected = false)
        _ <- graph.requestNodeSleep(cypherHarnessNamespace, nodeA)
        _ <- graph.requestNodeSleep(cypherHarnessNamespace, nodeB)

        assertion <- spellings.foldLeft(Future.successful(succeed)) { case (acc, (spelling, canonical)) =>
          acc.flatMap { _ =>
            rowsOf(call(spelling)).map { rows =>
              assert(rows.size == 1, s"'$spelling' should select exactly one edge, got ${rows.size}")
              assert(rows(0)(0) == Expr.Str(canonical), s"'$spelling' should report as '$canonical': ${rows(0)(0)}")
              succeed
            }
          }
        }
      } yield assertion
    }

    it("should report a directed self loop as outgoing whichever direction is asked for") {
      val selfNodeId = idProv.customIdToQid(394L)
      def call(direction: String) =
        s"""MATCH (n) WHERE strId(n) = "${selfNodeId.pretty}"
           |CALL history.edgeChangesBetween(n, n, "LOOP", $direction) YIELD direction
           |RETURN direction""".stripMargin

      for {
        _ <- literalOps.addHalfEdge(selfNodeId, selfNodeId, "LOOP", EdgeDirection.Outgoing)
        _ <- nextMillisecond()
        _ <- literalOps.addHalfEdge(selfNodeId, selfNodeId, "LOOP", EdgeDirection.Incoming)
        _ <- graph.requestNodeSleep(cypherHarnessNamespace, selfNodeId)

        unfiltered <- rowsOf(call("null"))
        outgoing <- rowsOf(call(""""out""""))
        incoming <- rowsOf(call(""""in""""))
      } yield {
        // A loop runs from the node to itself, so the whole edge is spelled `Outgoing` and the
        // incoming half is only the other side of it. Asking for `Incoming` therefore matches
        // nothing, rather than reporting the same single edge a second time under another name.
        assert(unfiltered.map(_(0)) == Seq(Expr.Str("Outgoing")), s"${unfiltered.map(_(0))}")
        assert(outgoing.map(_(0)) == Seq(Expr.Str("Outgoing")), s"${outgoing.map(_(0))}")
        assert(incoming.isEmpty, s"A loop is not separately incoming, got ${incoming.map(_(0))}")
      }
    }

    it("should name the argument actually at fault") {
      val testNodeId1 = idProv.customIdToQid(110L)
      val testNodeId2 = idProv.customIdToQid(111L)
      def call(args: String): String =
        s"""MATCH (source) WHERE strId(source) = "${testNodeId1.pretty}"
           |MATCH (target) WHERE strId(target) = "${testNodeId2.pretty}"
           |CALL history.edgeChangesBetween($args) YIELD action RETURN action""".stripMargin

      // Asserting on the position named, not merely that something failed: the procedure's own name
      // appears in every error it raises, so matching on that would accept any failure at all.
      for {
        // A string is not a node
        _ <- assertFailsWith(
          """CALL history.edgeChangesBetween("invalid_source", "invalid_target", "TEST_EDGE", null)
            |YIELD action RETURN action""".stripMargin,
          "first argument",
        )
        // ...and a number is not an edge type
        _ <- assertFailsWith(call("source, target, 123, null"), "third argument")
        // An unrecognised direction is rejected rather than silently matching everything, which
        // would quietly widen the edges reported. The rejected spelling is named back so the
        // mistake is visible.
        _ <- assertFailsWith(call("""source, target, "E", "sideways""""), "sideways")
        assertion <- assertFailsWith(call("""source, target, "E", "sideways""""), "fourth argument")
      } yield assertion
    }

    it("should track many edges' lifecycles independently across one pair of nodes") {
      val sourceNodeId = idProv.customIdToQid(112L)
      val targetNodeId = idProv.customIdToQid(113L)
      val numEdgeTypes = 100

      for {
        // Create a large number of edge changes with a deterministic pattern:
        // Add all edges, then remove every other one, then add some back
        _ <- (1 to numEdgeTypes).foldLeft(Future.successful(())) { (acc, i) =>
          for {
            _ <- acc
            _ <- literalOps.addEdge(sourceNodeId, targetNodeId, s"EDGE_$i")
            _ <- if (i % 10 == 0) nextMillisecond() else Future.successful(()) // Periodic small pause
          } yield ()
        }

        // Remove every other edge (deterministic pattern)
        _ <- (1 to numEdgeTypes by 2).foldLeft(Future.successful(())) { (acc, i) =>
          for {
            _ <- acc
            _ <- literalOps.removeEdge(sourceNodeId, targetNodeId, s"EDGE_$i")
            _ <- if (i % 20 == 1) nextMillisecond() else Future.successful(()) // Periodic small pause
          } yield ()
        }

        // Add back some edges (deterministic subset)
        _ <- (1 to numEdgeTypes by 4).foldLeft(Future.successful(())) { (acc, i) =>
          for {
            _ <- acc
            _ <- literalOps.addEdge(sourceNodeId, targetNodeId, s"EDGE_$i")
            _ <- if (i % 20 == 1) nextMillisecond() else Future.successful(()) // Periodic small pause
          } yield ()
        }

        _ <- graph.requestNodeSleep(cypherHarnessNamespace, sourceNodeId)
        _ <- graph.requestNodeSleep(cypherHarnessNamespace, targetNodeId)

        completeEdgeResults <- rowsOf(s"""MATCH (source) WHERE strId(source) = "${sourceNodeId.pretty}"
                                         |MATCH (target) WHERE strId(target) = "${targetNodeId.pretty}"
                                         |CALL history.edgeChangesBetween(source, target, null, null)
                                         |YIELD action, edgeType, changeTime
                                         |RETURN action, edgeType, changeTime""".stripMargin)

        // The same journal read as half edges, for comparison: every whole-edge change corresponds
        // to a half edge change on this node, so the half edge count is the upper bound
        halfEdgeResults <- rowsOf(s"""MATCH (source) WHERE strId(source) = "${sourceNodeId.pretty}"
                                     |MATCH (target) WHERE strId(target) = "${targetNodeId.pretty}"
                                     |CALL history.edgeChanges(source, target, null, null)
                                     |YIELD action, edgeType, changeTime
                                     |RETURN action, edgeType, changeTime""".stripMargin)
      } yield {
        // The write pattern is deterministic, so the exact result count is known:
        // - every edge added once            -> numEdgeTypes "added"
        // - every other edge removed         -> numEdgeTypes/2 "removed"
        // - every fourth edge added back     -> numEdgeTypes/4 "added"
        val expectedResults = numEdgeTypes + (numEdgeTypes / 2) + (numEdgeTypes / 4)
        assert(
          completeEdgeResults.size == expectedResults,
          s"Expected exactly $expectedResults complete edge changes, got ${completeEdgeResults.size}",
        )
        assert(
          halfEdgeResults.size == expectedResults,
          s"Every whole-edge change has a half edge change behind it on this node, " +
          s"so expected $expectedResults, got ${halfEdgeResults.size}",
        )

        // Verify data integrity
        val actions = completeEdgeResults.map(_(0).asInstanceOf[Expr.Str].string)
        assert(actions.forall(a => a == "added" || a == "removed"), "All actions should be 'added' or 'removed'")
        assert(
          actions.count(_ == "added") == numEdgeTypes + (numEdgeTypes / 4),
          s"Expected ${numEdgeTypes + (numEdgeTypes / 4)} additions, got ${actions.count(_ == "added")}",
        )
        assert(
          actions.count(_ == "removed") == numEdgeTypes / 2,
          s"Expected ${numEdgeTypes / 2} removals, got ${actions.count(_ == "removed")}",
        )

        // Every edge type in the test set should be represented
        val edgeTypes = completeEdgeResults.map(_(1).asInstanceOf[Expr.Str].string).toSet
        val expectedEdgeTypes = (1 to numEdgeTypes).map(i => s"EDGE_$i").toSet
        assert(edgeTypes == expectedEdgeTypes, "All edge types should appear in the results")

        // Unsorted, so this is the procedure's own emission order: two journals merged by timestamp
        // across 175 interleaved changes must still come out ascending
        val times = completeEdgeResults.map(_(2).asInstanceOf[Expr.Integer].long)
        assert(times == times.sorted, "Results should be in chronological order")
        assert(times.forall(_ >= t0.millis), "All timestamps should be after test start")
        succeed
      }
    }
  }
}
