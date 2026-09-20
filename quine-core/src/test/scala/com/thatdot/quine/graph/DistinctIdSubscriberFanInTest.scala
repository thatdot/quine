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

/** One node subscribed to by two others for the same pattern, and only one of their queries cancelled.
  *
  * `dropDeadSubscriptions` judges each subscriber by the queries *it* named rather than by the union across all
  * of them, and the comment on it says why: the union would keep one subscriber alive on another's query. That
  * is a claim about a subscription map with more than one node in it, so no graph shaped like a chain can test
  * it -- every other DistinctId suite has at most one node subscriber per pattern on any node.
  *
  * Two roots over the same leaf is the ordinary way to reach it: patterns are content-addressed, so both roots
  * ask the leaf about the identical `region` child, and the leaf ends up holding `Left(rootA) -> {qA}` and
  * `Left(rootB) -> {qB}` against that one pattern. Cancelling `qA` must retire exactly one of them.
  *
  * Nothing tells the leaf that `qA` has stopped. Cancelling deletes the query, the root that depended on it
  * retires its own side without sending anything, and the leaf reaches the same conclusion for itself from the
  * queries its subscribers named -- at the propagate that follows the cancellation if it is awake, or at its
  * next wake if it is not. Both are driven here, because they are different code paths into the same rule.
  */
class DistinctIdSubscriberFanInTest extends AnyFunSuite with Matchers {

  implicit private val timeout: Timeout = Timeout(30.seconds)
  private val namespace: NamespaceId = defaultNamespaceId
  private val idProvider: QuineIdLongProvider = QuineIdLongProvider()

  /** The single duration here, and a safety valve rather than a tuning knob. */
  private val awaitTimeout: FiniteDuration = 30.seconds

  private val rootA: QuineId = idProvider.customIdToQid(1L)
  private val rootB: QuineId = idProvider.customIdToQid(2L)
  private val leaf: QuineId = idProvider.customIdToQid(3L)

  private def hasProperty(key: String): DomainNodeEquiv =
    DomainNodeEquiv(None, Map(Symbol(key) -> ((PropertyComparisonFunctions.Wildcard, None))), Set.empty)

  private val regionBranch: SingleBranch = SingleBranch(hasProperty("region"), nextBranches = Nil)
  private val regionDgn: DomainGraphNodeId = regionBranch.toDomainGraphNodePackage.dgnId

  private def rootedOn(prop: String): SingleBranch = SingleBranch(
    hasProperty(prop),
    nextBranches = List(DomainEdge(GenericEdge(Symbol("to"), EdgeDirection.Outgoing), DependsUpon, regionBranch)),
  )

  private val patternA: SingleBranch = rootedOn("kinda")
  private val patternB: SingleBranch = rootedOn("kindb")

  private class Results {
    private val captured = new ConcurrentLinkedQueue[StandingQueryResult]()
    private val arrived = new CountDownLatch(Int.MaxValue)

    val sink: Sink[StandingQueryResult, UniqueKillSwitch] =
      Flow[StandingQueryResult]
        .viaMat(KillSwitches.single)(Keep.right)
        .map { r =>
          captured.add(r)
          arrived.countDown()
          MasterStream.SqResultsExecToken("distinctid-fan-in")
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

    private def sqns: StandingQueryOpsGraph#NamespaceStandingQueries =
      graph.standingQueries(namespace).getOrElse(fail("default namespace should exist"))

    def register(branch: SingleBranch, results: Results): StandingQueryId = {
      val sqId = StandingQueryId.fresh()
      val dgnPackage = branch.toDomainGraphNodePackage
      Await.result(
        graph.dgnRegistry.registerAndPersistDomainGraphNodePackage(dgnPackage, sqId, skipPersistor = true),
        awaitTimeout,
      )
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

    /** Cancel, and optionally tell the awake nodes. Without the propagate, a node finds out at its next wake. */
    def cancel(sqId: StandingQueryId, propagate: Boolean): Unit = {
      sqns.cancelStandingQuery(sqId).foreach(f => Await.result(f, awaitTimeout))
      if (propagate) Await.result(sqns.propagateStandingQueries(None), awaitTimeout)
    }

    def stateOf(qid: QuineId): NodeState = {
      val state = Await.result(ops.logState(qid), awaitTimeout)
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

    /** The nodes subscribed to `qid` for `dgnId`, and the queries each of them named. */
    def nodeSubscribersOf(qid: QuineId, dgnId: DomainGraphNodeId): Map[QuineId, Set[StandingQueryId]] =
      stateOf(qid).subscribers.collect {
        case (dgn, Some(subscriber), _, queries, _) if dgn == dgnId => subscriber -> queries
      }.toMap

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

    /** Evict each node, and wait until it is actually gone from every shard's awake set.
      *
      * The condition is the node's own absence rather than a move in the graph-wide sleep counter: the counter
      * moving says *a* node slept, which is not the same claim, and a schedule that asked the wrong node to
      * sleep would read as satisfied. `requestNodeSleep` can also be declined -- a node accessed a moment ago
      * is kept -- and this is where that has to show as a failure rather than as a quietly skipped step.
      */
    def sleep(qids: QuineId*): Unit = qids.foreach { qid =>
      val deadline = System.nanoTime() + awaitTimeout.toNanos
      while (awakeNodes().contains(qid) && System.nanoTime() < deadline) {
        Await.result(graph.requestNodeSleep(namespace, qid), awaitTimeout)
        Thread.sleep(25)
      }
      if (awakeNodes().contains(qid)) fail(s"node $qid never slept when asked; the schedule would be a lie")
    }

    /** Sleep and wake every node until two consecutive readings agree, and hand back that state.
      *
      * Bounded, and the bound is an assertion rather than a timeout: a sequence of restores that never repeats
      * itself is a node whose journal keeps changing what its own replay produces, which is worth failing on.
      */
    def restoreUntilStable(qids: QuineId*): Map[QuineId, NodeState] = {
      var previous = qids.map(q => q -> stateOf(q)).toMap
      var settled = false
      var cycles = 0
      while (!settled && cycles < 8) {
        sleep(qids: _*)
        val next = qids.map(q => q -> stateOf(q)).toMap
        settled = next == previous
        previous = next
        cycles += 1
      }
      if (!settled) fail(s"restoring never reached a fixed point in $cycles cycles; last reading was $previous")
      previous
    }

    def awaiting(what: String)(condition: => Boolean): Unit = {
      val deadline = System.nanoTime() + awaitTimeout.toNanos
      while (!condition && System.nanoTime() < deadline) Thread.sleep(10)
      if (!condition) fail(s"never reached: $what")
    }

    def shutdown(): Unit = { val _ = Await.result(graph.shutdown(), awaitTimeout) }
  }

  private val policies: List[(String, PersistenceConfig)] = List(
    "journal-only" -> PersistenceConfig(snapshotAfterEvents = Int.MaxValue),
    "snapshot-every-sleep" -> PersistenceConfig(snapshotAfterEvents = 0),
  )

  /** Both roots matching through the one leaf, with each query reported once. */
  private def bothMatching(g: Graph, resultsA: Results, resultsB: Results): (StandingQueryId, StandingQueryId) = {
    val ops = g.ops
    Await.result(ops.setProp(rootA, "kinda", QuineValue.Str("a")), awaitTimeout)
    Await.result(ops.setProp(rootB, "kindb", QuineValue.Str("b")), awaitTimeout)
    Await.result(ops.setProp(leaf, "region", QuineValue.Str("r")), awaitTimeout)
    Await.result(ops.addEdge(rootA, leaf, "to"), awaitTimeout)
    Await.result(ops.addEdge(rootB, leaf, "to"), awaitTimeout)
    val qA = g.register(patternA, resultsA)
    resultsA.awaitPositiveMatches(1)
    val qB = g.register(patternB, resultsB)
    resultsB.awaitPositiveMatches(1)
    g.awaiting("the leaf holds both roots as subscribers for the shared child") {
      g.nodeSubscribersOf(leaf, regionDgn).keySet == Set(rootA, rootB)
    }
    (qA, qB)
  }

  for {
    (policyLabel, config) <- policies
    (awakeLabel, leafAwakeAtCancel) <- List("leaf awake" -> true, "leaf asleep" -> false)
  } test(
    s"[$policyLabel/$awakeLabel] cancelling one of two queries retires only the subscriber that named it",
  ) {
    val shared = new Shared
    val g = new Graph(s"fan-in-${policyLabel}-${if (leafAwakeAtCancel) "awake" else "asleep"}", shared, config)
    try {
      val resultsA = new Results
      val resultsB = new Results
      val (qA, _) = bothMatching(g, resultsA, resultsB)
      withClue("each subscriber named exactly its own query: ")(
        g.nodeSubscribersOf(leaf, regionDgn).view.mapValues(_.size).toMap shouldBe Map(rootA -> 1, rootB -> 1),
      )

      if (!leafAwakeAtCancel) g.sleep(leaf)
      g.cancel(qA, propagate = leafAwakeAtCancel)
      // Reading the leaf both wakes it, when it is asleep, and waits for the propagate when it is not.
      g.awaiting("the leaf retired the cancelled query's subscriber") {
        g.nodeSubscribersOf(leaf, regionDgn).keySet == Set(rootB)
      }
      withClue("the surviving subscriber still names its own query: ")(
        g.nodeSubscribersOf(leaf, regionDgn)(rootB) should have size 1,
      )

      // The rule is about results, not bookkeeping: the leaf must still tell root B when its answer moves.
      val ops = g.ops
      Await.result(ops.removeProp(leaf, "region"), awaitTimeout)
      Await.result(ops.setProp(leaf, "region", QuineValue.Str("r2")), awaitTimeout)
      resultsB.awaitPositiveMatches(2)
      withClue("the cancelled query is owed nothing further: ")(resultsA.positiveMatches shouldBe 1)

      // A restore reproduces the state of a node that has *settled*, and settling takes more than one cycle
      // here: without a propagate, root A is still holding a subscription for the cancelled query when the
      // cancellation happens, its first wake is where it finds out, and that wake journals a retirement which
      // changes what the *next* fold does. See the case below, which is what this is accounting for rather
      // than hiding. So the reference is a fixed point reached by iterating, and that it is one is asserted.
      val before = g.restoreUntilStable(rootA, rootB, leaf)
      g.sleep(rootA, rootB, leaf)
      List(rootA, rootB, leaf).foreach { q =>
        withClue(s"node $q after one more restore from the settled state: ")(g.stateOf(q) shouldBe before(q))
      }
      withClue("and the node that depended on the cancelled query holds nothing for it: ")(
        before(rootA).subscriptions.flatMap(_._3) should not contain qA,
      )
    } finally g.shutdown()
  }

  test("an answer kept by a wake's over-approximation is dropped by the replay of the retirement that wake wrote") {
    // Two features of the design meet here, and the result is that one sleep/wake cycle is not always a fixed
    // point. Neither produces a wrong result -- nothing on root A consumes the answer either way -- but a
    // restore-equality assertion taken after a single cycle will see it, which is why the case above takes two.
    //
    // Root A matches pattern A and holds the leaf's answer about the shared `region` child. Pattern B is also
    // registered on root A, and root A does not match it locally, so root A never asks the leaf on pattern B's
    // behalf. Cancel the first query with nobody told, and:
    //
    //   - at the first wake, the fold records the leaf's answer from the journal with no query attribution --
    //     a result carries none, and the pattern that asked is gone from the registry. After the fold,
    //     `NodeParentIndex.reconstruct` maps the `region` child to pattern B, because reconstruct is about
    //     which patterns *have* that child and not about which ones match, so `recordMissingQueryIds`
    //     attributes the answer to the surviving query and `dropAnswersNoLiveQueryNeeds` keeps it. That is the
    //     documented over-approximation.
    //   - that same wake journals the retirement of the cancelled query's subscription. At the *next* wake the
    //     fold replays it, and `retireSubscription` runs `dropAnswersNoLiveQueryNeeds` mid-fold, where the
    //     parent index is still empty: pattern A cannot be evaluated and pattern B does not match locally, so
    //     nothing has claimed the child yet. An entry recording no queries and claimed by no parent is
    //     dropped -- correctly, by that rule, and before the rule that would have rescued it gets to run.
    //
    // The second state is the better one: root A has no use for that answer. What is pinned is that the
    // sequence converges and stays converged.
    val shared = new Shared
    val g = new Graph("fan-in-over-approximation", shared, PersistenceConfig(snapshotAfterEvents = Int.MaxValue))
    try {
      val resultsA = new Results
      val resultsB = new Results
      val (qA, _) = bothMatching(g, resultsA, resultsB)
      g.sleep(leaf)
      g.cancel(qA, propagate = false)

      g.sleep(rootA, rootB, leaf)
      val afterFirstRestore = g.stateOf(rootA)
      g.sleep(rootA, rootB, leaf)
      val afterSecondRestore = g.stateOf(rootA)
      g.sleep(rootA, rootB, leaf)
      val afterThirdRestore = g.stateOf(rootA)

      withClue("the answer the first wake attributed to the surviving query is gone by the second: ")(
        afterSecondRestore.subscriptions.size should be <= afterFirstRestore.subscriptions.size,
      )
      withClue("and the second restore is a fixed point: ")(afterThirdRestore shouldBe afterSecondRestore)
      withClue("neither state holds anything for the cancelled query: ")(
        (afterFirstRestore.subscriptions.flatMap(_._3) ++ afterSecondRestore.subscriptions.flatMap(_._3))
        should not contain qA,
      )
      withClue("and no result was produced by any of it: ")(resultsA.positiveMatches shouldBe 1)
    } finally g.shutdown()
  }

  test("cancelling both queries leaves the leaf with no subscribers and nothing held anywhere") {
    val shared = new Shared
    val g = new Graph("fan-in-both-cancelled", shared, PersistenceConfig(snapshotAfterEvents = Int.MaxValue))
    try {
      val resultsA = new Results
      val resultsB = new Results
      val (qA, qB) = bothMatching(g, resultsA, resultsB)

      g.cancel(qA, propagate = true)
      g.cancel(qB, propagate = true)
      g.awaiting("the leaf retired both subscribers")(g.nodeSubscribersOf(leaf, regionDgn).isEmpty)
      withClue("and neither root still holds the leaf's answer: ")(
        (g.stateOf(rootA).subscriptions ++ g.stateOf(rootB).subscriptions) shouldBe empty,
      )

      g.sleep(rootA, rootB, leaf)
      val before = List(rootA, rootB, leaf).map(q => q -> g.stateOf(q)).toMap
      withClue("nothing is left holding either query: ")(
        before.values.flatMap(s => s.subscribers.flatMap(_._4) ++ s.subscriptions.flatMap(_._3)) shouldBe empty,
      )
      g.sleep(rootA, rootB, leaf)
      List(rootA, rootB, leaf).foreach { q =>
        withClue(s"a cancelled query must not come back with the journal on node $q: ")(
          g.stateOf(q) shouldBe before(q),
        )
      }
    } finally g.shutdown()
  }
}
