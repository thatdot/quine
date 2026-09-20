package com.thatdot.quine.graph

import java.util.concurrent.{
  ConcurrentHashMap,
  ConcurrentLinkedQueue,
  ConcurrentMap,
  ConcurrentNavigableMap,
  CountDownLatch,
  TimeUnit,
}

import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.concurrent.{Await, ExecutionContext}
import scala.jdk.CollectionConverters._

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.scaladsl.{Flow, Keep, Sink}
import org.apache.pekko.stream.{KillSwitches, Materializer, UniqueKillSwitch}
import org.apache.pekko.util.Timeout

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.thatdot.common.quineid.QuineId
import com.thatdot.quine.graph.messaging.ShardMessage
import com.thatdot.quine.model.DomainGraphNode.DomainGraphNodeId
import com.thatdot.quine.model.{
  DependsUpon,
  DomainEdge,
  DomainNodeEquiv,
  EdgeDirection,
  GenericEdge,
  PropertyComparisonFunctions,
  QuineValue,
  SingleBranch,
}
import com.thatdot.quine.persistor.{
  EventEffectOrder,
  InMemoryPersistor,
  PersistenceConfig,
  PrimePersistor,
  StatelessPrimePersistor,
}
import com.thatdot.quine.util.TestLogging._

/** One standing query's cancellation must not make another report the wrong thing.
  *
  * Answers are memoized by pattern, and patterns are content-addressed, so two standing queries whose patterns
  * share a sub-pattern share every answer along it. A peer, though, maintains an answer for the standing queries
  * it was told about when it was asked -- not for the pattern. Those two facts together are what makes this go
  * wrong:
  *
  *   1. the root asks the leaf about the shared child, naming only the query that exists at the time;
  *   2. a second query arrives whose pattern needs the same child. Answered from the memoized answer, the leaf
  *      never hears that a second query now depends on it;
  *   3. the first query is cancelled. The leaf drops the root's subscription, because no query it knows about is
  *      still running;
  *   4. the leaf's answer then changes -- and it does not tell the root, which is no longer subscribed;
  *   5. the root goes on reporting from an answer that stopped being maintained.
  *
  * The failure is *coincidental*: nothing about the second query was cancelled, and the answer it is given is
  * wrong only because it happens to share a sub-pattern with a query that was.
  *
  * Verified against `main`, where it fails with `1 was not equal to 2`: the second query gets its initial match
  * and is then never told of the change. Two things on this branch prevent it, and either alone is enough -- the
  * ask a pattern makes on its own behalf when it finds a shared answer, and the re-ask when a subscriber arrives
  * naming a query the entry has not seen. This guards against losing both.
  *
  * Made observable as a **missing** result rather than a wrong one. Removing `region` from the leaf should drop
  * the second query's answer to false, and putting it back should raise it again -- a second match. A root still
  * holding the stale `true` never sees the fall, so the rise is not a change to it, and that second match never
  * arrives. Standing queries here do not report cancellations, so the fall itself emits nothing; the rise after
  * it is what can be counted.
  */
class StaleSharedAnswerTest extends AnyFunSuite with Matchers {

  implicit private val timeout: Timeout = Timeout(30.seconds)
  private val namespace: NamespaceId = defaultNamespaceId
  private val idProvider: QuineIdLongProvider = QuineIdLongProvider()

  /** The single duration here, and a safety valve: it fails a test that can never reach its result rather than
    * letting the suite hang. Nothing about pass or fail depends on its value.
    */
  private val awaitTimeout: FiniteDuration = 30.seconds

  private val root: QuineId = idProvider.customIdToQid(1L)
  private val leaf: QuineId = idProvider.customIdToQid(2L)

  private def hasProperty(key: String): DomainNodeEquiv =
    DomainNodeEquiv(None, Map(Symbol(key) -> ((PropertyComparisonFunctions.Wildcard, None))), Set.empty)

  /** Two roots over the same `region` child. Distinct roots matter: cancelling a query on one unregisters that
    * root pattern while the shared child stays registered for the other.
    */
  private def twoHopFrom(rootProp: String): SingleBranch = SingleBranch(
    hasProperty(rootProp),
    nextBranches = List(
      DomainEdge(
        GenericEdge(Symbol("to"), EdgeDirection.Outgoing),
        DependsUpon,
        SingleBranch(hasProperty("region"), nextBranches = Nil),
      ),
    ),
  )

  /** A standing query's output. Waiting is woken by each arrival, never by a clock. */
  private class Results(label: String) {
    private val captured = new ConcurrentLinkedQueue[StandingQueryResult]()
    private val arrived = new CountDownLatch(Int.MaxValue)

    val sink: Sink[StandingQueryResult, UniqueKillSwitch] =
      Flow[StandingQueryResult]
        .viaMat(KillSwitches.single)(Keep.right)
        .map { r =>
          captured.add(r)
          arrived.countDown()
          MasterStream.SqResultsExecToken(s"stale-shared-answer-$label")
        }
        .to(Sink.ignore)

    def positiveMatches: Int = captured.asScala.count(_.meta.isPositiveMatch)

    /** Block until `n` positive matches have arrived, returning how many did. */
    def awaitPositiveMatches(n: Int): Int = {
      val deadline = System.nanoTime() + awaitTimeout.toNanos
      while (positiveMatches < n && System.nanoTime() < deadline) {
        val _ = arrived.await(50, TimeUnit.MILLISECONDS)
      }
      positiveMatches
    }
  }

  /** Persistor state, so that a second graph over the same maps is a restart rather than a fresh system. */
  private class Shared {
    val journals: ConcurrentMap[QuineId, ConcurrentNavigableMap[EventTime, NodeChangeEvent]] = new ConcurrentHashMap()
    val domainIndexEvents: ConcurrentMap[QuineId, ConcurrentNavigableMap[EventTime, DomainIndexEvent]] =
      new ConcurrentHashMap()
    val snapshots: ConcurrentMap[QuineId, ConcurrentNavigableMap[EventTime, Array[Byte]]] = new ConcurrentHashMap()
    val domainGraphNodes: ConcurrentMap[DomainGraphNodeId, com.thatdot.quine.model.DomainGraphNode] =
      new ConcurrentHashMap()
  }

  private def makeGraph(
    name: String,
    shared: Shared = new Shared,
    config: PersistenceConfig = PersistenceConfig(),
  ): GraphService = {
    def persistorMaker(system: ActorSystem): PrimePersistor =
      new StatelessPrimePersistor(
        config,
        None,
        (pc, ns) =>
          new InMemoryPersistor(
            journals = shared.journals,
            domainIndexEvents = shared.domainIndexEvents,
            snapshots = shared.snapshots,
            domainGraphNodes = shared.domainGraphNodes,
            persistenceConfig = pc,
            namespace = ns,
          ),
      )(Materializer.matFromSystem(system), logConfig)
    val g = Await.result(
      GraphService(
        name,
        effectOrder = EventEffectOrder.PersistorFirst,
        persistorMaker = persistorMaker,
        idProvider = idProvider,
        declineSleepWhenWriteWithinMillis = 0L,
      ),
      awaitTimeout,
    )
    g.requiredGraphIsReady()
    g
  }

  private def register(
    g: GraphService,
    branch: SingleBranch,
    results: Results,
    sqId: StandingQueryId = StandingQueryId.fresh(),
  ): StandingQueryId = {
    val dgnPackage = branch.toDomainGraphNodePackage
    Await.result(
      g.dgnRegistry.registerAndPersistDomainGraphNodePackage(dgnPackage, sqId, skipPersistor = true),
      awaitTimeout,
    )
    val sqns = g.standingQueries(namespace).getOrElse(fail("default namespace should exist"))
    sqns.createStandingQuery(
      name = s"q-${sqId.uuid}",
      pattern = StandingQueryPattern.DomainGraphNodeStandingQueryPattern(
        dgnId = dgnPackage.dgnId,
        formatReturnAsStr = false,
        aliasReturnAs = Symbol("id"),
        includeCancellation = false,
        origin = PatternOrigin.DirectDgb,
      ),
      outputs = Map("capture" -> results.sink),
      sqId = sqId,
    )
    Await.result(sqns.propagateStandingQueries(None), awaitTimeout)
    sqId
  }

  private def sleepsCompleted(g: GraphService): Long =
    g.metrics.metricRegistry.getCounters.asScala.collect {
      case (name, counter) if name.endsWith("sleep-counters.slept-success") => counter.getCount
    }.sum

  private def awakeNodes(g: GraphService): Set[QuineId] =
    Await.result(
      g.relayAsk(
        g.shardFromNode(root).quineRef,
        ShardMessage.SampleAwakeNodes(namespace, limit = None, atTime = None, _),
      ).flatMap(_.map(_.quineId).runWith(Sink.collection[QuineId, Set[QuineId]])(g.materializer))(
        ExecutionContext.parasitic,
      ),
      awaitTimeout,
    )

  /** Evict every awake node, waiting for each eviction rather than for a duration. Under a snapshot threshold this
    * is what writes a snapshot, which is the precondition for a restore that reads a snapshot *and* a journal tail.
    */
  private def sleepAll(g: GraphService): Unit = {
    val awake = awakeNodes(g)
    List(root, mid, leaf).filter(awake.contains).foreach { qid =>
      val before = sleepsCompleted(g)
      Await.result(g.requestNodeSleep(namespace, qid), awaitTimeout)
      val deadline = System.nanoTime() + awaitTimeout.toNanos
      while (sleepsCompleted(g) == before && System.nanoTime() < deadline) Thread.sleep(5)
      if (sleepsCompleted(g) == before) fail(s"node $qid never slept when asked")
    }
  }

  private def cancel(g: GraphService, sqId: StandingQueryId): Unit = {
    val sqns = g.standingQueries(namespace).getOrElse(fail("default namespace should exist"))
    sqns.cancelStandingQuery(sqId).foreach(f => Await.result(f, awaitTimeout))
    Await.result(sqns.propagateStandingQueries(None), awaitTimeout)
  }

  test("cancelling one standing query does not stop another, sharing a sub-pattern, from being told of a change") {
    val graph = makeGraph("stale-shared-answer")
    try {
      val ops = graph.literalOps(namespace)
      Await.result(ops.setProp(root, "kind", QuineValue.Str("k")), awaitTimeout)
      Await.result(ops.setProp(root, "other", QuineValue.Str("o")), awaitTimeout)
      Await.result(ops.setProp(leaf, "region", QuineValue.Str("r")), awaitTimeout)
      Await.result(ops.addEdge(root, leaf, "to"), awaitTimeout)

      // The first query asks the leaf about `region`, naming only itself.
      val firstResults = new Results("first")
      val first = register(graph, twoHopFrom("kind"), firstResults)
      withClue("the first query should match, or nothing below is set up: ")(
        firstResults.awaitPositiveMatches(1) shouldBe 1,
      )

      // The second query's pattern needs the same `region` child, which the root already has an answer for. The
      // leaf has to be told that this query now depends on it too, or step 4 below goes wrong.
      val secondResults = new Results("second")
      val _ = register(graph, twoHopFrom("other"), secondResults)
      withClue("the second query should match from the shared answer: ")(
        secondResults.awaitPositiveMatches(1) shouldBe 1,
      )

      cancel(graph, first)

      // The leaf stops being a `region`, so the second query's answer should fall. Nothing is emitted for a fall,
      // which is why the rise below is what gets counted.
      Await.result(ops.removeProp(leaf, "region"), awaitTimeout)
      // And back again: a rise the second query is owed a report for.
      Await.result(ops.setProp(leaf, "region", QuineValue.Str("r2")), awaitTimeout)

      withClue(
        "the second query is owed a second match: if the leaf stopped telling the root when the first query was " +
        "cancelled, the root never saw the fall, so this rise is not a change to it and never arrives: ",
      )(secondResults.awaitPositiveMatches(2) shouldBe 2)

      withClue("the cancelled query should have been told nothing further: ")(firstResults.positiveMatches shouldBe 1)
    } finally {
      val _ = Await.result(graph.shutdown(), awaitTimeout)
    }
  }

  private val mid: QuineId = idProvider.customIdToQid(3L)

  /** `mid` with an edge to a `region`: the sub-pattern the three-hop shares, and a standing query in its own
    * right. Content-addressed, so it is literally the same DGN in both.
    */
  private val midToRegion: SingleBranch = SingleBranch(
    hasProperty("mid"),
    nextBranches = List(
      DomainEdge(
        GenericEdge(Symbol("to"), EdgeDirection.Outgoing),
        DependsUpon,
        SingleBranch(hasProperty("region"), nextBranches = Nil),
      ),
    ),
  )

  /** `kind` with an edge to [[midToRegion]]: a three-hop whose middle section is the query above. */
  private val threeHop: SingleBranch = SingleBranch(
    hasProperty("kind"),
    nextBranches = List(DomainEdge(GenericEdge(Symbol("to"), EdgeDirection.Outgoing), DependsUpon, midToRegion)),
  )

  /** Ben's reproduction, from a review of this work: the same coincidental staleness, but across a restart.
    *
    * Three nodes, root -> mid -> leaf. A three-hop query covers all of it; a second query covers just the
    * `mid -> region` half, which is *structurally shared* -- content addressing makes it the same DGN in both.
    * Both match. The three-hop is then cancelled and Quine is restarted, which is what makes this different from
    * the live case above: the surviving query's state has to come back from a snapshot plus the journal written
    * after it. Then the middle node stops matching and starts again, and the surviving query is owed a report.
    *
    * `snapshotAfterEvents` is deliberately mid-range rather than 0 or off: the reported failure needed a restore
    * that reads a snapshot *and* a journal tail, which is the only configuration that produces one.
    */
  test("a query sharing a sub-pattern still reports after the query above it is cancelled and Quine restarts") {
    val shared = new Shared
    val threshold = PersistenceConfig(snapshotAfterEvents = 4)
    val survivor = StandingQueryId.fresh()

    val first = makeGraph("ben-repro-before", shared, threshold)
    try {
      val ops = first.literalOps(namespace)
      Await.result(ops.setProp(root, "kind", QuineValue.Str("k")), awaitTimeout)
      Await.result(ops.setProp(mid, "mid", QuineValue.Str("m")), awaitTimeout)
      Await.result(ops.setProp(leaf, "region", QuineValue.Str("r")), awaitTimeout)
      Await.result(ops.addEdge(root, mid, "to"), awaitTimeout)
      Await.result(ops.addEdge(mid, leaf, "to"), awaitTimeout)

      val threeHopResults = new Results("three-hop")
      val threeHopId = register(first, threeHop, threeHopResults)
      withClue("the three-hop should match: ")(threeHopResults.awaitPositiveMatches(1) shouldBe 1)

      val survivorResults = new Results("survivor-before")
      val _ = register(first, midToRegion, survivorResults, survivor)
      withClue("the shared sub-pattern query should match: ")(survivorResults.awaitPositiveMatches(1) shouldBe 1)

      // Evict everything so that the threshold writes snapshots. Without this nothing sleeps during a short test,
      // no snapshot is ever written, and the restore below reads the journal alone -- which is not the reported
      // configuration. Verified: before this step the snapshot count was zero.
      sleepAll(first)
      withClue("a snapshot must exist, or this tests a journal-only restore rather than the reported case: ")(
        shared.snapshots.size should be > 0,
      )

      // Cancelling now puts journal events *after* those snapshots, so the restore has a snapshot plus a tail.
      cancel(first, threeHopId)
    } finally {
      val _ = Await.result(first.shutdown(), awaitTimeout)
    }

    // Restart. Only the surviving query is installed, under the same id the journals refer to.
    val second = makeGraph("ben-repro-after", shared, threshold)
    try {
      val survivorResults = new Results("survivor-after")
      val _ = register(second, midToRegion, survivorResults, survivor)
      val ops = second.literalOps(namespace)

      // No match is expected from the registration itself. The subscription came back from the journal, so the
      // node finds the query already subscribed, journals nothing and reports nothing -- correctly, since that
      // match was delivered before the restart and re-reporting it would be a duplicate. Which means every match
      // counted below was earned by the change that follows.
      //
      // The middle node stops matching, then matches again -- as reported, it is *B* that changes, not the leaf.
      // That exercises the middle node's own subscription bookkeeping: what it last told the surviving query, and
      // whether that query is still one of its subscribers at all after the query above it was cancelled.
      Await.result(ops.removeProp(mid, "mid"), awaitTimeout)
      Await.result(ops.setProp(mid, "mid", QuineValue.Str("m2")), awaitTimeout)

      withClue(
        "the surviving query is owed a match after the middle node stops matching and matches again: ",
      )(survivorResults.awaitPositiveMatches(1) shouldBe 1)
    } finally {
      val _ = Await.result(second.shutdown(), awaitTimeout)
    }
  }
}
