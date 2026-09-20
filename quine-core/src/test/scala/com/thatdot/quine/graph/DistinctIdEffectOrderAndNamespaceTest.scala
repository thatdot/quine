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

/** DistinctId replay fidelity under the two settings every other suite holds fixed.
  *
  * ==Effect order==
  *
  * `persistAndApplyEventsEffectsInMemory` does two quite different things with a DistinctId event depending on
  * `EventEffectOrder`. Persistor-first pauses the node's mailbox, writes the row, and applies the effect from the
  * success callback, so a write that fails applies nothing. Memory-first applies the effect at once and then
  * writes with an unbounded retry, so between those two points the node's state is ahead of its journal. Every
  * other DistinctId suite runs persistor-first, which leaves the whole of memory-first untested for this
  * bookkeeping even though a node restored from a journal written that way has to come back the same.
  *
  * ==Namespace==
  *
  * Three of the decisions in `DomainNodeIndexBehavior` are namespaced: `withRunningQueries` and
  * `childAnswersAreMaintained` both ask `graph.standingQueries(namespace)` whether a query still runs and treat
  * an absent namespace as "cannot judge", and `conditionallyReplyToAll` reports through the namespace's own
  * result queues. Every other suite runs in the default namespace, where `standingQueries` is never absent and
  * the namespace argument can be wrong without any test noticing.
  *
  * Both are run against the same three claims that `JournalReplayFidelityTest` makes for the default namespace
  * under persistor-first, so a difference here is a difference the setting made and nothing else.
  */
class DistinctIdEffectOrderAndNamespaceTest extends AnyFunSuite with Matchers {

  implicit private val timeout: Timeout = Timeout(30.seconds)
  private val idProvider: QuineIdLongProvider = QuineIdLongProvider()

  /** The single duration in this suite, and a safety valve rather than a tuning knob. */
  private val awaitTimeout: FiniteDuration = 30.seconds

  private val root: QuineId = idProvider.customIdToQid(1L)
  private val leaf: QuineId = idProvider.customIdToQid(2L)

  private def hasProperty(key: String): DomainNodeEquiv =
    DomainNodeEquiv(None, Map(Symbol(key) -> ((PropertyComparisonFunctions.Wildcard, None))), Set.empty)

  private val regionBranch: SingleBranch = SingleBranch(hasProperty("region"), nextBranches = Nil)
  private val regionDgn: DomainGraphNodeId = regionBranch.toDomainGraphNodePackage.dgnId

  private def twoHopFrom(prop: String): SingleBranch = SingleBranch(
    hasProperty(prop),
    nextBranches = List(DomainEdge(GenericEdge(Symbol("to"), EdgeDirection.Outgoing), DependsUpon, regionBranch)),
  )

  private val twoHop: SingleBranch = twoHopFrom("kind")
  private val twoHopOther: SingleBranch = twoHopFrom("other")

  private class Results {
    private val captured = new ConcurrentLinkedQueue[StandingQueryResult]()
    private val arrived = new CountDownLatch(Int.MaxValue)

    val sink: Sink[StandingQueryResult, UniqueKillSwitch] =
      Flow[StandingQueryResult]
        .viaMat(KillSwitches.single)(Keep.right)
        .map { r =>
          captured.add(r)
          arrived.countDown()
          MasterStream.SqResultsExecToken("distinctid-effect-order-namespace")
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

  /** Persistor state, kept per namespace so that two namespaces over one store do not share rows, and shared
    * between the graph that writes it and the graph that restores from it.
    */
  private class Shared {
    private class Maps {
      val journals: ConcurrentMap[QuineId, ConcurrentNavigableMap[EventTime, NodeChangeEvent]] =
        new ConcurrentHashMap()
      val domainIndexEvents: ConcurrentMap[QuineId, ConcurrentNavigableMap[EventTime, DomainIndexEvent]] =
        new ConcurrentHashMap()
      val snapshots: ConcurrentMap[QuineId, ConcurrentNavigableMap[EventTime, Array[Byte]]] = new ConcurrentHashMap()
    }
    private val byNamespace = new ConcurrentHashMap[NamespaceId, Maps]()
    val domainGraphNodes: ConcurrentMap[DomainGraphNodeId, DomainGraphNode] = new ConcurrentHashMap()

    def persistorFor(pc: PersistenceConfig, ns: NamespaceId): InMemoryPersistor = {
      val maps = byNamespace.computeIfAbsent(ns, _ => new Maps)
      new InMemoryPersistor(
        journals = maps.journals,
        domainIndexEvents = maps.domainIndexEvents,
        snapshots = maps.snapshots,
        domainGraphNodes = domainGraphNodes,
        persistenceConfig = pc,
        namespace = ns,
      )
    }

    def snapshotCount(ns: NamespaceId): Int =
      Option(byNamespace.get(ns)).fold(0)(_.snapshots.values.asScala.map(_.size).sum)
  }

  private case class NodeState(
    properties: Map[Symbol, String],
    edges: Set[com.thatdot.quine.model.HalfEdge],
    subscribers: Set[(Long, Option[QuineId], Option[StandingQueryId], Set[StandingQueryId], Option[Boolean])],
    subscriptions: Set[(Long, QuineId, Set[StandingQueryId], Option[Boolean])],
  )

  private class Graph(
    name: String,
    shared: Shared,
    persistenceConfig: PersistenceConfig,
    effectOrder: EventEffectOrder,
    val namespace: NamespaceId,
  ) {
    val graph: GraphService = {
      def persistorMaker(system: ActorSystem): PrimePersistor =
        new StatelessPrimePersistor(persistenceConfig, None, shared.persistorFor)(
          Materializer.matFromSystem(system),
          logConfig,
        )
      val g = Await.result(
        GraphService(
          name,
          effectOrder = effectOrder,
          persistorMaker = persistorMaker,
          idProvider = idProvider,
          declineSleepWhenWriteWithinMillis = 0L,
        ),
        awaitTimeout,
      )
      g.requiredGraphIsReady()
      // A namespace other than the default has to be made before anything in it can run. Making the default one
      // again reports no change, so this is unconditional.
      val _ = Await.result(g.createNamespace(namespace), awaitTimeout)
      g
    }

    def ops: com.thatdot.quine.graph.LiteralOpsGraph#LiteralOps = graph.literalOps(namespace)

    private def sqns: StandingQueryOpsGraph#NamespaceStandingQueries =
      graph.standingQueries(namespace).getOrElse(fail(s"namespace $namespace should exist"))

    def register(sqId: StandingQueryId, results: Results, branch: SingleBranch = twoHop): Unit = {
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
    }

    def cancel(sqId: StandingQueryId): Unit = {
      sqns.cancelStandingQuery(sqId).foreach(f => Await.result(f, awaitTimeout))
      Await.result(sqns.propagateStandingQueries(None), awaitTimeout)
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

  /** Drive a matching two-hop pattern to a reported match. The match arriving says the chain is complete: the
    * root cannot report until the leaf has answered it, so nothing is left in flight.
    */
  private def buildMatchedState(g: Graph, sqId: StandingQueryId, results: Results): Map[QuineId, NodeState] = {
    val ops = g.ops
    Await.result(ops.setProp(root, "kind", QuineValue.Str("k")), awaitTimeout)
    Await.result(ops.setProp(leaf, "region", QuineValue.Str("r")), awaitTimeout)
    Await.result(ops.addEdge(root, leaf, "to"), awaitTimeout)
    g.register(sqId, results)
    results.awaitPositiveMatches(1)
    Map(root -> g.stateOf(root), leaf -> g.stateOf(leaf))
  }

  private val journalOnly = PersistenceConfig(snapshotAfterEvents = Int.MaxValue)
  private val snapshotEverySleep = PersistenceConfig(snapshotAfterEvents = 0)

  private val effectOrders: List[(String, EventEffectOrder)] =
    List("persistor-first" -> EventEffectOrder.PersistorFirst, "memory-first" -> EventEffectOrder.MemoryFirst)

  private val namespaces: List[(String, NamespaceId)] =
    List("default-namespace" -> defaultNamespaceId, "named-namespace" -> NamespaceId("other"))

  for {
    (orderLabel, effectOrder) <- effectOrders
    (nsLabel, ns) <- namespaces
    label = s"$orderLabel/$nsLabel"
    // An actor system name may not contain a slash, so the clue and the system name differ.
    systemLabel = s"$orderLabel-$nsLabel"
  } {

    test(s"[$label] a node evicted and woken holds exactly the state it held, from its journal alone") {
      val shared = new Shared
      val g = new Graph(s"fidelity-$systemLabel", shared, journalOnly, effectOrder, ns)
      try {
        val before = buildMatchedState(g, StandingQueryId.fresh(), new Results)
        g.sleepAll()
        withClue("no snapshot should have been written, or this tests the snapshot rather than the journal: ")(
          shared.snapshotCount(ns) shouldBe 0,
        )
        // Reading is what wakes each node, and a node finishes its restore in its constructor.
        withClue("root after eviction: ")(g.stateOf(root) shouldBe before(root))
        withClue("leaf after eviction: ")(g.stateOf(leaf) shouldBe before(leaf))
      } finally g.shutdown()
    }

    test(s"[$label] a restart over the same store does not re-report a match already delivered") {
      val shared = new Shared
      val sqId = StandingQueryId.fresh()
      val before = {
        val g = new Graph(s"no-reemit-before-$systemLabel", shared, snapshotEverySleep, effectOrder, ns)
        try {
          val results = new Results
          val state = buildMatchedState(g, sqId, results)
          withClue("exactly one match before the restart: ")(results.positiveMatches shouldBe 1)
          g.sleepAll()
          state
        } finally g.shutdown()
      }

      val g2 = new Graph(s"no-reemit-after-$systemLabel", shared, snapshotEverySleep, effectOrder, ns)
      try {
        val afterResults = new Results
        g2.register(sqId, afterResults)
        withClue("root restored unchanged: ")(g2.stateOf(root) shouldBe before(root))
        withClue("leaf restored unchanged: ")(g2.stateOf(leaf) shouldBe before(leaf))
        withClue("a restart must not re-emit a match already delivered: ")(afterResults.positiveMatches shouldBe 0)
      } finally g2.shutdown()
    }

    test(s"[$label] a query sharing a child keeps reporting after the query that first asked is cancelled") {
      // The shape that lost state: two patterns share the `region` child, the first is cancelled, and the
      // answer the second still depends on has to survive both the cancellation and the restore. What makes it
      // observable is the rise after a fall, since a cancellation itself reports nothing.
      val shared = new Shared
      val first = StandingQueryId.fresh()
      val second = StandingQueryId.fresh()
      val g = new Graph(s"shared-child-$systemLabel", shared, journalOnly, effectOrder, ns)
      try {
        val ops = g.ops
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

        g.cancel(first)
        g.sleepAll()

        withClue("the survivor still holds the leaf's answer after the restore: ")(
          g.stateOf(root).subscriptions.map(r => (r._1, r._2)) shouldBe Set((regionDgn, leaf)),
        )

        Await.result(ops.removeProp(leaf, "region"), awaitTimeout)
        Await.result(ops.setProp(leaf, "region", QuineValue.Str("r2")), awaitTimeout)
        withClue("the surviving query is owed the rise: ")(secondResults.awaitPositiveMatches(2))
        withClue("the cancelled query is owed nothing further: ")(firstResults.positiveMatches shouldBe 1)
      } finally g.shutdown()
    }
  }
}
