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
import scala.concurrent.{Await, ExecutionContext, Future}
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
import com.thatdot.quine.model.DomainGraphNode.DomainGraphNodeId
import com.thatdot.quine.model.{
  DependsUpon,
  DomainEdge,
  DomainGraphNode,
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

/** What removing an edge does to the subscriptions it was carrying.
  *
  * An answer is held at a peer because some edge matching a pattern's domain edge reached that peer. Removing an
  * edge is therefore the dual of adding one: adding subscribes, removing withdraws, and
  * `withdrawSubscriptionsUnreachableAfter` does the second as the edge event is applied rather than leaving it
  * for some later cancellation to tidy up.
  *
  * The decision it makes is not per edge but per *child pattern*: an answer goes only when no edge of any pattern
  * subscribed here still reaches that peer for it. Three things follow, and none of them is visible from a graph
  * with one edge, one pattern and one peer:
  *
  *   - a peer reached by an edge that is still there keeps its answer when a *different* peer's edge goes;
  *   - a child pattern reached by two different domain edges keeps its answer while either edge remains, even
  *     when the two edges belong to different standing queries;
  *   - an answer this node holds about *itself* is left alone, because the guard `peer != qid` excludes it. The
  *     case is exercised here for what it must not break rather than for the state it leaves: a node that is its
  *     own peer must still report the right answer, and must come back from its journal as it was.
  *
  * Timing throughout is on arrivals, as in `JournalReplayFidelityTest`: a write completes when `literalOps`
  * returns, a propagation is finished when the result it produces reaches the query's output, and a restore is
  * finished when `logState` returns, because a node completes its restore in its constructor. [[awaitTimeout]]
  * is the single safety valve and decides nothing about meaning.
  */
class DistinctIdEdgeWithdrawalTest extends AnyFunSuite with Matchers {

  implicit private val timeout: Timeout = Timeout(30.seconds)
  private val namespace: NamespaceId = defaultNamespaceId
  private val idProvider: QuineIdLongProvider = QuineIdLongProvider()
  private val awaitTimeout: FiniteDuration = 30.seconds

  private val root: QuineId = idProvider.customIdToQid(1L)
  private val peerA: QuineId = idProvider.customIdToQid(2L)
  private val peerB: QuineId = idProvider.customIdToQid(3L)

  private def hasProperty(key: String): DomainNodeEquiv =
    DomainNodeEquiv(None, Map(Symbol(key) -> ((PropertyComparisonFunctions.Wildcard, None))), Set.empty)

  private val regionBranch: SingleBranch = SingleBranch(hasProperty("region"), nextBranches = Nil)
  private val regionDgn: DomainGraphNodeId = regionBranch.toDomainGraphNodePackage.dgnId

  /** A root with one domain edge of the given type to the shared `region` child. Two of these with different
    * edge types are two standing queries that reach one answer by two different routes.
    */
  private def rootedOn(prop: String, edgeType: String): SingleBranch = SingleBranch(
    hasProperty(prop),
    nextBranches = List(DomainEdge(GenericEdge(Symbol(edgeType), EdgeDirection.Outgoing), DependsUpon, regionBranch)),
  )

  private val kindVia: SingleBranch = rootedOn("kind", "to")
  private val otherViaAlt: SingleBranch = rootedOn("other", "alt")

  /** The same shape, but declaring that the edge may be satisfied by the node's own half-edge.
    *
    * `circularMatchAllowed` defaults to false and every other pattern in this file leaves it there, which is
    * why a self-edge otherwise does not satisfy a domain edge at all: `hasUniqueGenEdges` discounts the
    * circular half-edge from the count. Turning it on is the only way to reach the case the `peer != qid` guard
    * in `withdrawSubscriptionsUnreachableAfter` exists for.
    */
  private val kindViaCircular: SingleBranch = SingleBranch(
    hasProperty("kind"),
    nextBranches = List(
      DomainEdge(
        GenericEdge(Symbol("to"), EdgeDirection.Outgoing),
        DependsUpon,
        regionBranch,
        circularMatchAllowed = true,
      ),
    ),
  )

  private class Results {
    private val captured = new ConcurrentLinkedQueue[StandingQueryResult]()
    private val arrived = new CountDownLatch(Int.MaxValue)

    val sink: Sink[StandingQueryResult, UniqueKillSwitch] =
      Flow[StandingQueryResult]
        .viaMat(KillSwitches.single)(Keep.right)
        .map { r =>
          captured.add(r)
          arrived.countDown()
          MasterStream.SqResultsExecToken("distinctid-edge-withdrawal")
        }
        .to(Sink.ignore)

    def positiveMatches: Int = captured.asScala.count(_.meta.isPositiveMatch)

    def awaitPositiveMatches(n: Int): Unit = {
      val deadline = System.nanoTime() + awaitTimeout.toNanos
      while (positiveMatches < n && System.nanoTime() < deadline) {
        val _ = arrived.await(50, TimeUnit.MILLISECONDS)
      }
      if (positiveMatches < n)
        fail(s"only $positiveMatches of $n expected results arrived; the graph never finished propagating")
    }
  }

  /** Persistor state, so that a second graph over the same maps performs a restore. */
  private class Shared {
    val journals: ConcurrentMap[QuineId, ConcurrentNavigableMap[EventTime, NodeChangeEvent]] = new ConcurrentHashMap()
    val domainIndexEvents: ConcurrentMap[QuineId, ConcurrentNavigableMap[EventTime, DomainIndexEvent]] =
      new ConcurrentHashMap()
    val snapshots: ConcurrentMap[QuineId, ConcurrentNavigableMap[EventTime, Array[Byte]]] = new ConcurrentHashMap()
    val domainGraphNodes: ConcurrentMap[DomainGraphNodeId, DomainGraphNode] = new ConcurrentHashMap()
  }

  private case class NodeState(
    properties: Map[Symbol, String],
    edges: Set[com.thatdot.quine.model.HalfEdge],
    subscribers: Set[(Long, Option[QuineId], Option[StandingQueryId], Set[StandingQueryId], Option[Boolean])],
    subscriptions: Set[(Long, QuineId, Set[StandingQueryId], Option[Boolean])],
  )

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

    def ops: com.thatdot.quine.graph.LiteralOpsGraph#LiteralOps = graph.literalOps(namespace)

    def register(
      branch: SingleBranch,
      results: Results,
      sqId: StandingQueryId = StandingQueryId.fresh(),
    ): StandingQueryId = {
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
      sqId
    }

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

    /** What this node holds of its peers' answers, as (child pattern, peer). */
    def heldBy(qid: QuineId): Set[(Long, QuineId)] = stateOf(qid).subscriptions.map(r => (r._1, r._2))

    /** Who is subscribed to this node, as (pattern, subscribing node). */
    def subscribersOf(qid: QuineId): Set[(Long, QuineId)] =
      stateOf(qid).subscribers.flatMap { case (dgnId, node, _, _, _) => node.map(dgnId -> _) }

    private def sleepsCompleted: Long =
      graph.metrics.metricRegistry.getCounters.asScala.collect {
        case (metric, counter) if metric.endsWith("sleep-counters.slept-success") => counter.getCount
      }.sum

    private def awakeNodes(): Set[QuineId] =
      // Every shard, not just the one this node happens to live on: `SampleAwakeNodes` answers only for the
      // shard it is sent to, and a reading that missed a shard would silently turn "evict this node" into a
      // no-op and every claim about its restore into a claim about a node that never slept.
      Await.result(
        Future
          .traverse(graph.shards.toList) { shard =>
            graph
              .relayAsk(shard.quineRef, ShardMessage.SampleAwakeNodes(namespace, limit = None, atTime = None, _))
              .flatMap(_.map(_.quineId).runWith(Sink.collection[QuineId, Set[QuineId]])(graph.materializer))(
                ExecutionContext.parasitic,
              )
          }(implicitly, ExecutionContext.parasitic)
          .map(_.foldLeft(Set.empty[QuineId])(_ union _))(ExecutionContext.parasitic),
        awaitTimeout,
      )

    /** Evict every node this suite uses, waiting for each eviction to have happened. */
    def sleepAll(): Unit = {
      val awake = awakeNodes()
      List(root, peerA, peerB).filter(awake.contains).foreach { qid =>
        val before = sleepsCompleted
        Await.result(graph.requestNodeSleep(namespace, qid), awaitTimeout)
        val deadline = System.nanoTime() + awaitTimeout.toNanos
        while (sleepsCompleted == before && System.nanoTime() < deadline) Thread.sleep(10)
        if (sleepsCompleted == before) fail(s"node $qid never slept when asked; the restore would not be one")
      }
    }

    /** Evict exactly these nodes, waiting until each has actually left every shard's awake set. */
    def sleepOnly(qids: QuineId*): Unit = qids.foreach { qid =>
      val deadline = System.nanoTime() + awaitTimeout.toNanos
      while (awakeNodes().contains(qid) && System.nanoTime() < deadline) {
        Await.result(graph.requestNodeSleep(namespace, qid), awaitTimeout)
        Thread.sleep(25)
      }
      if (awakeNodes().contains(qid)) fail(s"node $qid never slept when asked; the schedule would be a lie")
    }

    /** Block until a condition the rest of the case depends on holds, failing if it never does. */
    def awaiting(what: String)(condition: => Boolean): Unit = {
      val deadline = System.nanoTime() + awaitTimeout.toNanos
      while (!condition && System.nanoTime() < deadline) Thread.sleep(10)
      if (!condition) fail(s"never reached: $what")
    }

    def shutdown(): Unit = { val _ = Await.result(graph.shutdown(), awaitTimeout) }
  }

  /** A snapshot on every sleep and a journal-only policy both, since the withdrawal has to survive either. */
  private val policies: List[(String, PersistenceConfig)] = List(
    "snapshot-every-sleep" -> PersistenceConfig(snapshotAfterEvents = 0),
    "journal-only" -> PersistenceConfig(snapshotAfterEvents = Int.MaxValue),
  )

  policies.foreach { case (label, config) =>
    test(s"[$label] removing the edge to one peer leaves the answers held for every other peer") {
      val shared = new Shared
      val g = new Graph(s"withdraw-one-peer-$label", shared, config)
      try {
        val ops = g.ops
        Await.result(ops.setProp(root, "kind", QuineValue.Str("k")), awaitTimeout)
        Await.result(ops.setProp(peerA, "region", QuineValue.Str("a")), awaitTimeout)
        Await.result(ops.setProp(peerB, "region", QuineValue.Str("b")), awaitTimeout)
        Await.result(ops.addEdge(root, peerA, "to"), awaitTimeout)
        Await.result(ops.addEdge(root, peerB, "to"), awaitTimeout)

        val results = new Results
        val _ = g.register(kindVia, results)
        results.awaitPositiveMatches(1)
        g.awaiting("the root holds both peers' answers")(
          g.heldBy(root) == Set((regionDgn, peerA), (regionDgn, peerB)),
        )

        Await.result(ops.removeEdge(root, peerA, "to"), awaitTimeout)

        g.awaiting("the root stopped holding peer A's answer") {
          g.heldBy(root) == Set((regionDgn, peerB))
        }
        withClue("peer A was told to stop maintaining it: ")(
          g.awaiting("peer A dropped the root as a subscriber")(!g.subscribersOf(peerA).contains((regionDgn, root))),
        )
        withClue("peer B was not told anything, because its edge is still there: ")(
          g.subscribersOf(peerB) should contain((regionDgn, root)),
        )

        val before = List(root, peerA, peerB).map(q => q -> g.stateOf(q)).toMap
        g.sleepAll()
        List(root, peerA, peerB).foreach { q =>
          withClue(s"node $q after the restore: ")(g.stateOf(q) shouldBe before(q))
        }
      } finally g.shutdown()
    }

    test(s"[$label] an answer another pattern's edge still reaches is kept when one edge is removed") {
      // The union in `stillReachable`: two standing queries reach the same content-addressed child by different
      // domain edges, and the root holds one answer for both. Removing the edge one of them uses must not take
      // the answer the other still gets to.
      val shared = new Shared
      val g = new Graph(s"withdraw-shared-child-$label", shared, config)
      try {
        val ops = g.ops
        Await.result(ops.setProp(root, "kind", QuineValue.Str("k")), awaitTimeout)
        Await.result(ops.setProp(root, "other", QuineValue.Str("o")), awaitTimeout)
        Await.result(ops.setProp(peerA, "region", QuineValue.Str("a")), awaitTimeout)
        Await.result(ops.addEdge(root, peerA, "to"), awaitTimeout)
        Await.result(ops.addEdge(root, peerA, "alt"), awaitTimeout)

        val viaTo = new Results
        val viaAlt = new Results
        val _ = g.register(kindVia, viaTo)
        viaTo.awaitPositiveMatches(1)
        val _ = g.register(otherViaAlt, viaAlt)
        viaAlt.awaitPositiveMatches(1)

        Await.result(ops.removeEdge(root, peerA, "to"), awaitTimeout)
        g.awaiting("the root finished handling the removal")(g.stateOf(root).edges.size == 1)

        withClue("the `alt` edge still reaches the same child pattern, so the answer stays: ")(
          g.heldBy(root) shouldBe Set((regionDgn, peerA)),
        )
        withClue("and the peer was not told to stop maintaining it: ")(
          g.subscribersOf(peerA) should contain((regionDgn, root)),
        )

        // The consequence that matters: the surviving query is still told when the peer's answer moves. A
        // withdrawal that had gone too far would leave this second match never arriving.
        Await.result(ops.removeProp(peerA, "region"), awaitTimeout)
        Await.result(ops.setProp(peerA, "region", QuineValue.Str("a2")), awaitTimeout)
        viaAlt.awaitPositiveMatches(2)
        withClue("the query whose edge was removed must not match again: ")(viaTo.positiveMatches shouldBe 1)

        val before = List(root, peerA).map(q => q -> g.stateOf(q)).toMap
        g.sleepAll()
        List(root, peerA).foreach { q =>
          withClue(s"node $q after the restore: ")(g.stateOf(q) shouldBe before(q))
        }
      } finally g.shutdown()
    }

    test(s"[$label] removing the last edge to a peer withdraws the answer, and the withdrawal survives a restore") {
      val shared = new Shared
      val g = new Graph(s"withdraw-last-edge-$label", shared, config)
      try {
        val ops = g.ops
        Await.result(ops.setProp(root, "kind", QuineValue.Str("k")), awaitTimeout)
        Await.result(ops.setProp(peerA, "region", QuineValue.Str("a")), awaitTimeout)
        Await.result(ops.addEdge(root, peerA, "to"), awaitTimeout)
        Await.result(ops.addEdge(root, peerA, "alt"), awaitTimeout)

        val viaTo = new Results
        val viaAlt = new Results
        val _ = g.register(kindVia, viaTo)
        viaTo.awaitPositiveMatches(1)
        val _ = g.register(otherViaAlt, viaAlt)

        Await.result(ops.removeEdge(root, peerA, "to"), awaitTimeout)
        Await.result(ops.removeEdge(root, peerA, "alt"), awaitTimeout)

        g.awaiting("the root stopped holding the peer's answer")(g.heldBy(root).isEmpty)
        g.awaiting("the peer dropped the root as a subscriber")(!g.subscribersOf(peerA).contains((regionDgn, root)))

        val before = List(root, peerA).map(q => q -> g.stateOf(q)).toMap
        g.sleepAll()
        List(root, peerA).foreach { q =>
          withClue(s"node $q came back subscribed across an edge it no longer has: ")(g.stateOf(q) shouldBe before(q))
        }
      } finally g.shutdown()
    }

    test(s"[$label] a node that is its own peer keeps what it holds about itself, and restores as it was") {
      // `withdrawSubscriptionsUnreachableAfter` excludes `peer == qid`. A `Single` pattern whose domain edge
      // allows a circular match is the case where that exclusion is conservative: the answer this node holds
      // about itself *was* reached by an edge, so removing that edge leaves an entry nothing reaches. What is
      // asserted is the consequence that has to hold anyway -- a restore reproduces whatever the live node was
      // left holding, so the leftover is at least not a difference between a live node and a rebuilt one.
      val shared = new Shared
      val g = new Graph(s"withdraw-self-edge-$label", shared, config)
      try {
        val ops = g.ops
        Await.result(ops.setProp(root, "kind", QuineValue.Str("k")), awaitTimeout)
        Await.result(ops.setProp(root, "region", QuineValue.Str("r")), awaitTimeout)
        Await.result(ops.addEdge(root, root, "to"), awaitTimeout)

        val results = new Results
        val _ = g.register(kindViaCircular, results)
        results.awaitPositiveMatches(1)
        withClue("the node holds its own answer, which is what the guard then declines to withdraw: ")(
          g.heldBy(root) shouldBe Set((regionDgn, root)),
        )

        Await.result(ops.removeEdge(root, root, "to"), awaitTimeout)
        g.awaiting("the root finished handling the removal")(g.stateOf(root).edges.isEmpty)
        withClue("the guard leaves what this node holds about itself where it is: ")(
          g.heldBy(root) shouldBe Set((regionDgn, root)),
        )

        val before = g.stateOf(root)
        g.sleepAll()
        withClue("the self-subscribed root after the restore: ")(g.stateOf(root) shouldBe before)
      } finally g.shutdown()
    }

    test(s"[$label] an answer falls when the last edge satisfying a mandatory domain edge is removed") {
      // Recorded as a failing case rather than asserted, and not a regression from this branch: nothing below
      // is reached on `main` either, and neither of the two places responsible was touched by it.
      //
      // A `DomainEdge` with `circularMatchAllowed = true` is never checked for existence.
      // `ReverseOrderedEdgeCollection.hasUniqueGenEdges` partitions the required edges into circular-allowed
      // and circular-disallowed and only checks the second group, so a pattern whose every domain edge allows
      // a circular match satisfies `localTestBranch` with no edges at all. `edgesSatisfiedByIndex` then finds
      // that the multiplicity filter has dropped every domain edge -- zero matching half-edges is below
      // `MandatoryConstraint.min` of one -- reaches `edgeResolutions.isEmpty`, and reads that as "no edge
      // requirements to satisfy", returning `Some(true)` rather than "a mandatory requirement is
      // unsatisfiable".
      //
      // Together the root goes on reporting `true` after its last matching edge is gone. Because the answer
      // never falls, re-adding the edge is not a change either, so the next genuine match on that root is
      // never reported. Verified directly: with `circularMatchAllowed = false` the same schedule takes the
      // answer to `Some(false)`; with it true the answer stays `Some(true)`, whether or not the edge removed
      // was a self-edge.
      //
      // Which of the two places should own the fix is a semantic decision about what a circular-allowed
      // requirement means when no edge satisfies it, so it is recorded here rather than patched.
      pending
      val shared = new Shared
      val g = new Graph(s"circular-answer-falls-$label", shared, config)
      try {
        val ops = g.ops
        Await.result(ops.setProp(root, "kind", QuineValue.Str("k")), awaitTimeout)
        Await.result(ops.setProp(root, "region", QuineValue.Str("r")), awaitTimeout)
        Await.result(ops.addEdge(root, root, "to"), awaitTimeout)

        val results = new Results
        val _ = g.register(kindViaCircular, results)
        results.awaitPositiveMatches(1)

        Await.result(ops.removeEdge(root, root, "to"), awaitTimeout)
        g.awaiting("the root finished handling the removal")(g.stateOf(root).edges.isEmpty)
        withClue("the root no longer satisfies the pattern, so its subscribers are owed a `false`: ")(
          g.stateOf(root).subscribers.map(_._5) shouldBe Set(Some(false)),
        )

        // The fall emits nothing, so the rise after it is what can be counted: a node still holding the stale
        // `true` sees the re-added edge as no change, and this second match never arrives.
        Await.result(ops.addEdge(root, root, "to"), awaitTimeout)
        results.awaitPositiveMatches(2)
      } finally g.shutdown()
    }

    test(s"[$label] a peer asleep when the edge is removed keeps the root as a subscriber until the query ends") {
      // The asymmetry in the lazy-cancellation argument, pinned because it is not what the design comments lead
      // you to expect and because it is reached by the ordinary API rather than by an exotic path.
      //
      // A *query* cancellation converges without any message: both ends judge the subscription by the same test
      // -- does a query it was taken out for still run -- so each reaches the same answer at its own next wake.
      // An *edge* removal has no such symmetry. Only the root knows the edge is gone; the peer's own rule cannot
      // see it, because the queries named on the subscription are all still running. The one thing that would
      // tell the peer is the `CancelDomainNodeSubscription` the root sends, and
      // `NodeActorMailbox.shouldIgnoreWhenSleeping` discards that when the peer is not awake.
      //
      // `literalOps.removeEdge` writes the reciprocal half-edge too, which *does* wake the peer -- but by then
      // the cancellation has already been dropped, and nothing on the peer's side removes a subscriber in
      // response to its own edge event. So a peer that was asleep at that instant keeps the record.
      //
      // Nothing wrong is reported: the root refuses any answer for an entry it no longer holds, and journals
      // nothing for the refusal. What it costs is a subscriber record that outlives its use, and -- because a
      // subscription result is not a message the mailbox drops -- a spurious wake of the root every time the
      // peer's answer moves. Both last until the query is cancelled.
      val shared = new Shared
      val g = new Graph(s"withdraw-peer-asleep-$label", shared, config)
      try {
        val ops = g.ops
        Await.result(ops.setProp(root, "kind", QuineValue.Str("k")), awaitTimeout)
        Await.result(ops.setProp(peerA, "region", QuineValue.Str("a")), awaitTimeout)
        Await.result(ops.addEdge(root, peerA, "to"), awaitTimeout)

        val results = new Results
        val _ = g.register(kindVia, results)
        results.awaitPositiveMatches(1)
        g.awaiting("the peer has the root as a subscriber")(g.subscribersOf(peerA).contains((regionDgn, root)))

        g.sleepOnly(peerA)
        Await.result(ops.removeEdge(root, peerA, "to"), awaitTimeout)
        g.awaiting("the root withdrew its side")(g.heldBy(root).isEmpty)

        withClue("the peer never saw the cancellation, so it still lists the root: ")(
          g.subscribersOf(peerA) should contain((regionDgn, root)),
        )
        // The consequence that is not a wrong answer: the peer still reports to a root that refuses to record it.
        Await.result(ops.removeProp(peerA, "region"), awaitTimeout)
        Await.result(ops.setProp(peerA, "region", QuineValue.Str("a2")), awaitTimeout)
        withClue("and the root records nothing from it, so no result follows: ")(g.heldBy(root) shouldBe empty)
        withClue("the query matched once, when the edge was there, and not again: ")(results.positiveMatches shouldBe 1)
      } finally g.shutdown()
    }

    test(s"[$label] a circular match is not made by a domain edge that disallows one") {
      // Why the case above needs `circularMatchAllowed`, stated rather than left implicit: with the default,
      // `hasUniqueGenEdges` discounts the node's own half-edge, the pattern does not match locally, and nothing
      // downstream is ever asked. A reader who removed the flag from the case above would otherwise get a test
      // that passed while exercising none of it.
      val shared = new Shared
      val g = new Graph(s"self-edge-not-circular-$label", shared, config)
      try {
        val ops = g.ops
        Await.result(ops.setProp(root, "kind", QuineValue.Str("k")), awaitTimeout)
        Await.result(ops.setProp(root, "region", QuineValue.Str("r")), awaitTimeout)
        Await.result(ops.addEdge(root, root, "to"), awaitTimeout)

        val results = new Results
        val _ = g.register(kindVia, results)
        // An anchor that *does* match the same query, so the absence below is read after the query has run.
        Await.result(ops.setProp(peerA, "region", QuineValue.Str("a")), awaitTimeout)
        Await.result(ops.setProp(peerB, "kind", QuineValue.Str("k")), awaitTimeout)
        Await.result(ops.addEdge(peerB, peerA, "to"), awaitTimeout)
        results.awaitPositiveMatches(1)

        withClue("the root does not match locally, so it asks nobody -- not even itself: ")(
          g.heldBy(root) shouldBe empty,
        )
        withClue("and the only match is the one across two nodes: ")(results.positiveMatches shouldBe 1)
      } finally g.shutdown()
    }
  }
}
