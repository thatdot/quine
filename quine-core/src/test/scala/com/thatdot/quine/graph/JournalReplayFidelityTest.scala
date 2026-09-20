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
import com.thatdot.quine.graph.messaging.LiteralMessage.{DistinctIdIndexState, DistinctIdSubscriberState}
import com.thatdot.quine.graph.messaging.ShardMessage
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

/** Replaying a node's journal must rebuild exactly the state the node had.
  *
  * Everything a DistinctId query depends on -- which patterns a node serves, who is subscribed for which
  * standing queries, what answer it last reported, and what its peers told it -- has to come back from the
  * journal alone. A snapshot may only ever make that cheaper, never possible.
  *
  * ==Why this is fast and why it has no timers==
  *
  * Two decisions do all the work.
  *
  * A node is restored by *building a second graph over the same persistor* rather than by sleeping the node. The
  * first graph is shut down, the second reads the journal the first wrote, and the node wakes on first access.
  * That removes the need to detect that a sleep happened, and removes any dependency on eviction policy.
  *
  * Every wait is on an event, so there is no duration anywhere that affects whether a test passes:
  *
  *   - a write completes when `literalOps` returns; the future is the event.
  *   - a query is installed when `propagateStandingQueries` returns.
  *   - propagation across an edge is finished when the *result it produces* arrives at the query's output. That
  *     is a real happens-after: the result cannot be emitted before the root has heard from its neighbour.
  *   - a restore is finished when `logState` returns, because a node's constructor completes its restore before
  *     the actor serves any message.
  *
  * [[awaitTimeout]] is the single safety valve. It decides nothing about meaning; it exists so a test that can
  * never reach its event fails rather than hanging.
  */
class JournalReplayFidelityTest extends AnyFunSuite with Matchers {

  implicit private val timeout: Timeout = Timeout(30.seconds)
  private val namespace: NamespaceId = defaultNamespaceId
  private val idProvider: QuineIdLongProvider = QuineIdLongProvider()

  /** The only duration in this suite, and it is a safety valve rather than a tuning knob. */
  private val awaitTimeout: FiniteDuration = 30.seconds

  private val root: QuineId = idProvider.customIdToQid(1L)
  private val leaf: QuineId = idProvider.customIdToQid(2L)

  private def hasProperty(key: String): DomainNodeEquiv =
    DomainNodeEquiv(None, Map(Symbol(key) -> ((PropertyComparisonFunctions.Wildcard, None))), Set.empty)

  /** A `kind` with an edge to a `region`: the shape whose state spans two nodes, so the root's bookkeeping
    * includes what the leaf told it. A one-hop pattern would leave the peer side of the index untested.
    */
  private val twoHop: SingleBranch = SingleBranch(
    hasProperty("kind"),
    nextBranches = List(
      DomainEdge(
        GenericEdge(Symbol("to"), EdgeDirection.Outgoing),
        DependsUpon,
        SingleBranch(hasProperty("region"), nextBranches = Nil),
      ),
    ),
  )

  /** A different root over the same `region` child: cancelling a query on this one unregisters its root pattern
    * while leaving the shared child registered, which is what makes a replay unable to re-derive the root's ask.
    */
  private val twoHopOther: SingleBranch = SingleBranch(
    hasProperty("other"),
    nextBranches = List(
      DomainEdge(
        GenericEdge(Symbol("to"), EdgeDirection.Outgoing),
        DependsUpon,
        SingleBranch(hasProperty("region"), nextBranches = Nil),
      ),
    ),
  )

  /** One standing query's output, with a latch per expected result so a test waits on arrivals, not on time. */
  private class Results {
    private val captured = new ConcurrentLinkedQueue[StandingQueryResult]()
    private val arrived = new CountDownLatch(Int.MaxValue)
    private val matches = new java.util.concurrent.atomic.AtomicInteger(0)

    val sink: Sink[StandingQueryResult, UniqueKillSwitch] =
      Flow[StandingQueryResult]
        .viaMat(KillSwitches.single)(Keep.right)
        .map { r =>
          captured.add(r)
          if (r.meta.isPositiveMatch) { val _ = matches.incrementAndGet() }
          arrived.countDown()
          MasterStream.SqResultsExecToken("journal-replay-fidelity")
        }
        .to(Sink.ignore)

    def positiveMatches: Int = matches.get()

    /** Block until `n` positive matches have arrived. Fails if they never do. */
    def awaitPositiveMatches(n: Int): Unit = {
      val deadline = System.nanoTime() + awaitTimeout.toNanos
      while (positiveMatches < n && System.nanoTime() < deadline) {
        val _ = arrived.await(50, TimeUnit.MILLISECONDS)
      }
      if (positiveMatches < n)
        fail(s"only $positiveMatches of $n expected results arrived; the graph never finished propagating")
    }
  }

  /** One graph over a given persistor state. Building a second over the same maps is what performs a restore. */
  private class Graph(name: String, shared: Shared, persistenceConfig: PersistenceConfig) {
    val graph: GraphService = {
      def persistorMaker(system: ActorSystem): PrimePersistor =
        new StatelessPrimePersistor(
          persistenceConfig,
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

    /** Install a standing query for `twoHop` under `sqId`, and wait for it to be installed. */
    def register(sqId: StandingQueryId, results: Results, branch: SingleBranch = twoHop): Unit = {
      val dgnPackage = branch.toDomainGraphNodePackage
      Await.result(
        graph.dgnRegistry.registerAndPersistDomainGraphNodePackage(dgnPackage, sqId, skipPersistor = true),
        awaitTimeout,
      )
      val sqns = graph.standingQueries(namespace).getOrElse(fail("default namespace should exist"))
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
    }

    /** Everything about a node that must survive a restore. Reading it is also what wakes the node. */
    def stateOf(qid: QuineId): NodeState = {
      val state = Await.result(graph.literalOps(namespace).logState(qid), awaitTimeout)
      def normSubs(
        rs: List[DistinctIdSubscriberState],
      ): Set[(Long, Option[QuineId], Option[StandingQueryId], Set[StandingQueryId], Option[Boolean])] =
        rs.map(r => (r.dgnId, r.subscriberNode, r.subscriberQuery, r.forQueries.toSet, r.lastResult)).toSet
      def normIndex(rs: List[DistinctIdIndexState]): Set[(Long, QuineId, Set[StandingQueryId], Option[Boolean])] =
        rs.map(r => (r.dgnId, r.peer, r.forQueries.toSet, r.answer)).toSet
      NodeState(
        state.properties,
        state.edges,
        normSubs(state.sqStateResults.subscribers),
        normIndex(state.sqStateResults.subscriptions),
      )
    }

    def cancel(sqId: StandingQueryId): Unit = {
      val sqns = graph.standingQueries(namespace).getOrElse(fail("default namespace should exist"))
      sqns.cancelStandingQuery(sqId).foreach(f => Await.result(f, awaitTimeout))
      Await.result(sqns.propagateStandingQueries(None), awaitTimeout)
    }

    private def sleepsCompleted: Long =
      graph.metrics.metricRegistry.getCounters.asScala.collect {
        case (metric, counter) if metric.endsWith("sleep-counters.slept-success") => counter.getCount
      }.sum

    private def awakeNodes(): Set[QuineId] =
      Await.result(
        graph
          .relayAsk(
            graph.shardFromNode(root).quineRef,
            ShardMessage.SampleAwakeNodes(namespace, limit = None, atTime = None, _),
          )
          .flatMap(_.map(_.quineId).runWith(Sink.collection[QuineId, Set[QuineId]])(graph.materializer))(
            ExecutionContext.parasitic,
          ),
        awaitTimeout,
      )

    /** Evict every awake node, waiting for each eviction to have actually happened.
      *
      * `requestNodeSleep` completing says the request was accepted, not that the node slept, so the sleep counter
      * moving is what is waited for. Polling it is not a claim about elapsed time: the loop ends when the count
      * changes, and only the safety valve involves a duration at all.
      */
    def sleepAll(): Unit = {
      val awake = awakeNodes()
      List(root, leaf).filter(awake.contains).foreach { qid =>
        val before = sleepsCompleted
        Await.result(graph.requestNodeSleep(namespace, qid), awaitTimeout)
        val deadline = System.nanoTime() + awaitTimeout.toNanos
        while (sleepsCompleted == before && System.nanoTime() < deadline) Thread.sleep(10)
        if (sleepsCompleted == before) fail(s"node $qid never slept when asked; the restore would not be one")
      }
    }

    def shutdown(): Unit = { val _ = Await.result(graph.shutdown(), awaitTimeout) }
  }

  /** Persistor state, shared between the graph that writes it and the graph that restores from it. */
  private class Shared {
    val journals: ConcurrentMap[QuineId, ConcurrentNavigableMap[EventTime, NodeChangeEvent]] = new ConcurrentHashMap()
    val domainIndexEvents: ConcurrentMap[QuineId, ConcurrentNavigableMap[EventTime, DomainIndexEvent]] =
      new ConcurrentHashMap()
    val snapshots: ConcurrentMap[QuineId, ConcurrentNavigableMap[EventTime, Array[Byte]]] = new ConcurrentHashMap()
    val domainGraphNodes: ConcurrentMap[
      com.thatdot.quine.model.DomainGraphNode.DomainGraphNodeId,
      com.thatdot.quine.model.DomainGraphNode,
    ] = new ConcurrentHashMap()
    def snapshotCount: Int = snapshots.values.asScala.map(_.size).sum
  }

  private case class NodeState(
    properties: Map[Symbol, String],
    edges: Set[com.thatdot.quine.model.HalfEdge],
    subscribers: Set[(Long, Option[QuineId], Option[StandingQueryId], Set[StandingQueryId], Option[Boolean])],
    subscriptions: Set[(Long, QuineId, Set[StandingQueryId], Option[Boolean])],
  )

  /** Drive a matching two-hop pattern to a reported match, and return the state of both nodes at that point.
    *
    * The match arriving is the event that says the chain is complete: the root cannot report until the leaf has
    * answered it, so there is nothing left in flight when this returns.
    */
  private def buildMatchedState(g: Graph, sqId: StandingQueryId, results: Results): Map[QuineId, NodeState] = {
    val ops = g.graph.literalOps(namespace)
    Await.result(ops.setProp(root, "kind", QuineValue.Str("k")), awaitTimeout)
    Await.result(ops.setProp(leaf, "region", QuineValue.Str("r")), awaitTimeout)
    Await.result(ops.addEdge(root, leaf, "to"), awaitTimeout)
    g.register(sqId, results)
    results.awaitPositiveMatches(1)
    Map(root -> g.stateOf(root), leaf -> g.stateOf(leaf))
  }

  /** No snapshot is ever written, so a restore has only the journal to work from. */
  private val journalOnly = PersistenceConfig(snapshotAfterEvents = Int.MaxValue)

  /** A snapshot on every sleep, which is what makes a snapshot available to the restore. */
  private val snapshotEverySleep = PersistenceConfig(snapshotAfterEvents = 0)

  /** A threshold in between, so that a node can carry a snapshot *and* journal events written after it. Verified
    * by each test that uses it, because a short test evicts nothing on its own and would otherwise write no
    * snapshot at all -- leaving a journal-only restore wearing a threshold's clothes.
    */
  private val snapshotThenTail = PersistenceConfig(snapshotAfterEvents = 4)

  test("a node restored from its journal alone holds exactly the state it held before") {
    val shared = new Shared
    val sqId = StandingQueryId.fresh()

    val before = {
      val g = new Graph("fidelity-write", shared, journalOnly)
      try buildMatchedState(g, sqId, new Results)
      finally g.shutdown()
    }

    withClue("no snapshot should have been written, or this tests the snapshot rather than the journal: ")(
      shared.snapshotCount shouldBe 0,
    )

    // A second graph over the same journal. The query is installed again because a standing query lives in the
    // graph rather than in the journal; the node's own bookkeeping is what has to come back on its own.
    val g2 = new Graph("fidelity-restore", shared, journalOnly)
    try {
      g2.register(sqId, new Results)
      withClue("root restored from its journal: ")(g2.stateOf(root) shouldBe before(root))
      withClue("leaf restored from its journal: ")(g2.stateOf(leaf) shouldBe before(leaf))
    } finally g2.shutdown()
  }

  test("a snapshot restores the same state the journal alone restores") {
    val sqId = StandingQueryId.fresh()

    // The same schedule twice over, differing only in whether a snapshot is written.
    def restoredWith(config: PersistenceConfig): Map[QuineId, NodeState] = {
      val shared = new Shared
      val before = {
        val g = new Graph(s"optimisation-write-${config.snapshotAfterEvents}", shared, config)
        try buildMatchedState(g, sqId, new Results)
        finally g.shutdown()
      }
      val g2 = new Graph(s"optimisation-restore-${config.snapshotAfterEvents}", shared, config)
      try {
        g2.register(sqId, new Results)
        val restored = Map(root -> g2.stateOf(root), leaf -> g2.stateOf(leaf))
        // Each config must first be faithful to its own pre-restore state, or comparing the two proves only that
        // they are wrong in the same way.
        withClue(s"root, snapshotAfterEvents=${config.snapshotAfterEvents}: ")(restored(root) shouldBe before(root))
        withClue(s"leaf, snapshotAfterEvents=${config.snapshotAfterEvents}: ")(restored(leaf) shouldBe before(leaf))
        restored
      } finally g2.shutdown()
    }

    val viaJournal = restoredWith(journalOnly)
    val viaSnapshot = restoredWith(snapshotEverySleep)

    withClue("root: a snapshot changed what was restored, so it is not only an optimisation: ")(
      viaSnapshot(root) shouldBe viaJournal(root),
    )
    withClue("leaf: a snapshot changed what was restored, so it is not only an optimisation: ")(
      viaSnapshot(leaf) shouldBe viaJournal(leaf),
    )
  }

  /** The shape that actually lost state.
    *
    * Two patterns share the `region` child. Both match, so the root holds the leaf's answer about `region`. The
    * first query is then cancelled, which unregisters *its* root pattern while leaving the shared child in place
    * for the second. Replaying the root therefore cannot re-derive the ask that produced the held answer: the
    * pattern that made it is gone from the registry and cannot be evaluated, so the leaf's answer replays with no
    * index entry to land in.
    *
    * The second query still depends on that answer, so losing it is a node coming back less than it was.
    */
  test("a shared answer survives a restore when the query that first asked for it has been cancelled") {
    val shared = new Shared
    val first = StandingQueryId.fresh()
    val second = StandingQueryId.fresh()

    val before = {
      val g = new Graph("shared-write", shared, journalOnly)
      try {
        val ops = g.graph.literalOps(namespace)
        Await.result(ops.setProp(root, "kind", QuineValue.Str("k")), awaitTimeout)
        Await.result(ops.setProp(root, "other", QuineValue.Str("o")), awaitTimeout)
        Await.result(ops.setProp(leaf, "region", QuineValue.Str("r")), awaitTimeout)
        Await.result(ops.addEdge(root, leaf, "to"), awaitTimeout)

        val firstResults = new Results
        g.register(first, firstResults, twoHop)
        firstResults.awaitPositiveMatches(1)

        val secondResults = new Results
        g.register(second, secondResults, twoHopOther)
        secondResults.awaitPositiveMatches(1)

        // Both matches have been reported, so the root holds the leaf's answer. Cancelling now leaves that answer
        // in place for the second query while retiring the pattern that first asked for it.
        g.cancel(first)
        Map(root -> g.stateOf(root), leaf -> g.stateOf(leaf))
      } finally g.shutdown()
    }

    val g2 = new Graph("shared-restore", shared, journalOnly)
    try {
      // Only the surviving query is installed, which is the situation a restart after a cancellation is in.
      g2.register(second, new Results, twoHopOther)
      withClue("root restored from its journal, holding the shared answer: ")(g2.stateOf(root) shouldBe before(root))
      withClue("leaf restored from its journal: ")(g2.stateOf(leaf) shouldBe before(leaf))
    } finally g2.shutdown()
  }

  /** The same fidelity requirement, but through eviction rather than a restart.
    *
    * A restart exercises a cold start: nothing in memory, the registry freshly populated. Evicting the node
    * instead exercises the path a running system actually takes -- the sleep-time snapshot decision, and
    * `reconcileDistinctIdStateAtWake` on the way back. Both must give the node back unchanged.
    */
  test("a node evicted and woken holds exactly the state it held before, from its journal alone") {
    val shared = new Shared
    val sqId = StandingQueryId.fresh()
    val g = new Graph("evict-journal", shared, journalOnly)
    try {
      val before = buildMatchedState(g, sqId, new Results)
      g.sleepAll()
      withClue("no snapshot should have been written, or this tests the snapshot rather than the journal: ")(
        shared.snapshotCount shouldBe 0,
      )
      // Reading is what wakes each node, and a node finishes its restore in its constructor before serving the read.
      withClue("root after eviction: ")(g.stateOf(root) shouldBe before(root))
      withClue("leaf after eviction: ")(g.stateOf(leaf) shouldBe before(leaf))
    } finally g.shutdown()
  }

  test("a node evicted with a snapshot written holds exactly the state it held before") {
    val shared = new Shared
    val sqId = StandingQueryId.fresh()
    val g = new Graph("evict-snapshot", shared, snapshotEverySleep)
    try {
      val before = buildMatchedState(g, sqId, new Results)
      g.sleepAll()
      withClue("a snapshot should have been written, or this is the journal-only case again: ")(
        shared.snapshotCount should be > 0,
      )
      withClue("root after eviction: ")(g.stateOf(root) shouldBe before(root))
      withClue("leaf after eviction: ")(g.stateOf(leaf) shouldBe before(leaf))
    } finally g.shutdown()
  }

  test("a node restored from a snapshot plus the journal written after it holds exactly the state it held before") {
    val shared = new Shared
    val sqId = StandingQueryId.fresh()
    val g = new Graph("snapshot-plus-tail", shared, snapshotThenTail)
    try {
      val results = new Results
      val _ = buildMatchedState(g, sqId, results)

      // Evict, which under a threshold is what writes the snapshot. Everything after this point is journal that a
      // restore has to apply *on top of* that snapshot -- the configuration neither of the tests above produces.
      g.sleepAll()
      // Pinned rather than "at least one": the threshold is what decides whether a snapshot is written at all, so
      // a loose assertion would pass just as happily on a configuration that wrote none for one of the two nodes
      // and silently gave that node a journal-only restore. Both nodes cross a threshold of four here.
      withClue("both nodes should have snapshotted, or one of them is not testing snapshot-plus-tail: ")(
        shared.snapshotCount shouldBe 2,
      )

      // Journal a change after the snapshot, and let the query's report be the event that says it has landed.
      val ops = g.graph.literalOps(namespace)
      Await.result(ops.removeProp(leaf, "region"), awaitTimeout)
      Await.result(ops.setProp(leaf, "region", QuineValue.Str("again")), awaitTimeout)
      results.awaitPositiveMatches(2)
      withClue("the answer should fall and rise, which is what puts events after the snapshot: ")(
        results.positiveMatches shouldBe 2,
      )

      val before = Map(root -> g.stateOf(root), leaf -> g.stateOf(leaf))
      g.sleepAll()
      withClue("root, from snapshot plus tail: ")(g.stateOf(root) shouldBe before(root))
      withClue("leaf, from snapshot plus tail: ")(g.stateOf(leaf) shouldBe before(leaf))
    } finally g.shutdown()
  }

  test("a node whose edge was removed before the snapshot does not come back subscribed across it") {
    val shared = new Shared
    val sqId = StandingQueryId.fresh()
    val g = new Graph("edge-removed-then-restored", shared, snapshotEverySleep)
    try {
      val _ = buildMatchedState(g, sqId, new Results)
      val ops = g.graph.literalOps(namespace)

      // Removing the edge is what makes the leaf unreachable for the pattern, so the root should withdraw its
      // subscription and drop what the leaf told it. That withdrawal has to survive the restore too.
      Await.result(ops.removeEdge(root, leaf, "to"), awaitTimeout)
      val withoutEdge = g.stateOf(root)
      withClue("the root should hold nothing about the leaf once no edge reaches it: ")(
        withoutEdge.subscriptions.filter(_._2 == leaf) shouldBe empty,
      )

      g.sleepAll()
      withClue("root after the restore still holds nothing about the leaf: ")(g.stateOf(root) shouldBe withoutEdge)
    } finally g.shutdown()
  }

  test("a restart does not re-report a match that was already reported") {
    val shared = new Shared
    val sqId = StandingQueryId.fresh()

    val before = {
      val g = new Graph("no-reemit-before", shared, snapshotThenTail)
      try {
        val results = new Results
        val state = buildMatchedState(g, sqId, results)
        withClue("exactly one match before the restart: ")(results.positiveMatches shouldBe 1)
        g.sleepAll()
        state
      } finally g.shutdown()
    }

    // The same query, under the same id the journals refer to, on a graph that restores from what the first wrote.
    val g2 = new Graph("no-reemit-after", shared, snapshotThenTail)
    try {
      val afterResults = new Results
      g2.register(sqId, afterResults)
      // Reading is what wakes each node, so by the time these return the restore has happened and anything the
      // nodes were going to report has been reported.
      withClue("root restored unchanged: ")(g2.stateOf(root) shouldBe before(root))
      withClue("leaf restored unchanged: ")(g2.stateOf(leaf) shouldBe before(leaf))

      // The match was delivered before the restart. A node that comes back knowing what it last reported has
      // nothing new to say, so the restart is silent -- re-reporting would be a duplicate the consumer cannot
      // tell from a real second match.
      withClue("a restart must not re-emit a match already delivered: ")(afterResults.positiveMatches shouldBe 0)
    } finally g2.shutdown()
  }
}
