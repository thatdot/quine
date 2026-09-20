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
import com.thatdot.quine.graph.messaging.LiteralMessage.{
  AddHalfEdgeCommand,
  DistinctIdIndexState,
  DistinctIdSubscriberState,
}
import com.thatdot.quine.graph.messaging.{ShardMessage, SpaceTimeQuineId}
import com.thatdot.quine.model.DomainGraphNode.DomainGraphNodeId
import com.thatdot.quine.model.{
  DependsUpon,
  DomainEdge,
  DomainGraphNode,
  DomainNodeEquiv,
  EdgeDirection,
  GenericEdge,
  HalfEdge,
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

/** A self-loop in a DistinctId pattern, which is a shape no other suite covers.
  *
  * A self-loop in a standing query pattern -- `(n)-[:loop]->(n)` -- is compiled by
  * `GraphQueryPattern.compiledDomainGraphBranch` into a `CircularEdge` on the node's `DomainNodeEquiv`, not into
  * a `DomainEdge`. It is therefore a *local* feature: `localTestBranch` checks it through `hasCircularEdges`,
  * which requires both half-edges for a directed loop, and `WatchableEventType.extractWatchableEvents` registers
  * an edge watch for it so that adding or removing the loop re-evaluates the pattern. Nothing else in the
  * DistinctId bookkeeping is involved -- no peer is asked about it, because the node is its own peer and knows
  * the answer. It is reachable from ordinary Cypher -- `GraphQueryPattern.compiledDomainGraphBranch` turns
  * `(n)-[:r]->(n)` into exactly this -- and had no coverage at all.
  */
class DistinctIdCircularEdgeTest extends AnyFunSuite with Matchers {

  implicit private val timeout: Timeout = Timeout(30.seconds)
  private val namespace: NamespaceId = defaultNamespaceId
  private val idProvider: QuineIdLongProvider = QuineIdLongProvider()

  /** The single duration here, and a safety valve rather than a tuning knob. */
  private val awaitTimeout: FiniteDuration = 30.seconds

  private val root: QuineId = idProvider.customIdToQid(1L)
  private val leaf: QuineId = idProvider.customIdToQid(2L)

  private def hasProperty(key: String): DomainNodeEquiv =
    DomainNodeEquiv(None, Map(Symbol(key) -> ((PropertyComparisonFunctions.Wildcard, None))), Set.empty)

  private val regionBranch: SingleBranch = SingleBranch(hasProperty("region"), nextBranches = Nil)
  private val regionDgn: DomainGraphNodeId = regionBranch.toDomainGraphNodePackage.dgnId

  /** A `kind` that also loops back to itself on `loop`, with an edge on to a `region`.
    *
    * The loop is declared the way the Cypher compiler declares one, as a directed `CircularEdge` on the node
    * equivalence rather than as a `DomainEdge`. The downstream hop is kept so that the pattern still spans two
    * nodes, which is what puts the loop alongside the peer bookkeeping rather than on its own.
    */
  private val loopingKind: SingleBranch = SingleBranch(
    DomainNodeEquiv(
      None,
      Map(Symbol("kind") -> ((PropertyComparisonFunctions.Wildcard, None))),
      Set(Symbol("loop") -> true),
    ),
    nextBranches = List(DomainEdge(GenericEdge(Symbol("to"), EdgeDirection.Outgoing), DependsUpon, regionBranch)),
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
          MasterStream.SqResultsExecToken("distinctid-pattern-shape")
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

    /** Write one half-edge, which `literalOps.addEdge` cannot do: it always writes the reciprocal too. */
    def addHalfEdge(on: QuineId, edge: HalfEdge): Unit = {
      val _ = Await.result(
        graph.relayAsk(SpaceTimeQuineId(on, namespace, None), AddHalfEdgeCommand(edge, _)),
        awaitTimeout,
      )
    }

    def awaiting(what: String)(condition: => Boolean): Unit = {
      val deadline = System.nanoTime() + awaitTimeout.toNanos
      while (!condition && System.nanoTime() < deadline) Thread.sleep(10)
      if (!condition) fail(s"never reached: $what")
    }

    def shutdown(): Unit = { val _ = Await.result(graph.shutdown(), awaitTimeout) }
  }

  private val journalOnly = PersistenceConfig(snapshotAfterEvents = Int.MaxValue)
  private val snapshotEverySleep = PersistenceConfig(snapshotAfterEvents = 0)

  private val policies: List[(String, PersistenceConfig)] =
    List("journal-only" -> journalOnly, "snapshot-every-sleep" -> snapshotEverySleep)

  // -- circular edges -------------------------------------------------------------------------------------------

  policies.foreach { case (label, config) =>
    test(s"[$label] a pattern with a circular edge matches only while the loop is there") {
      val shared = new Shared
      val g = new Graph(s"circular-edge-$label", shared, config)
      try {
        val ops = g.ops
        Await.result(ops.setProp(root, "kind", QuineValue.Str("k")), awaitTimeout)
        Await.result(ops.setProp(leaf, "region", QuineValue.Str("r")), awaitTimeout)
        Await.result(ops.addEdge(root, leaf, "to"), awaitTimeout)

        val results = new Results
        val _ = g.register(loopingKind, results)
        // Everything but the loop is in place, so nothing should match yet. The anchor for reading that is the
        // match produced by adding the loop below: results arrive in order on one stream.
        Await.result(ops.addEdge(root, root, "loop"), awaitTimeout)
        results.awaitPositiveMatches(1)
        withClue("exactly the one match, made by the loop arriving: ")(results.positiveMatches shouldBe 1)

        // Removing the loop makes the node stop matching. The fall emits nothing, so the rise after it is what
        // can be counted: a node that had not registered an edge watch for the loop would never notice either
        // change, and this second match would not arrive.
        Await.result(ops.removeEdge(root, root, "loop"), awaitTimeout)
        g.awaiting("the root finished handling the removal")(!g.stateOf(root).edges.exists(_.other == root))
        Await.result(ops.addEdge(root, root, "loop"), awaitTimeout)
        results.awaitPositiveMatches(2)

        withClue("the downstream hop is unaffected by the loop: ")(
          g.stateOf(root).subscriptions.map(r => (r._1, r._2)) shouldBe Set((regionDgn, leaf)),
        )

        val before = List(root, leaf).map(q => q -> g.stateOf(q)).toMap
        g.sleepAll()
        List(root, leaf).foreach(q => withClue(s"node $q after the restore: ")(g.stateOf(q) shouldBe before(q)))
        withClue("and the restore reports nothing, because nothing changed: ")(results.positiveMatches shouldBe 2)
      } finally g.shutdown()
    }
  }

  test("a directed circular edge needs both half-edges, so one alone does not match") {
    // `hasCircularEdges` requires the outgoing *and* the incoming half-edge for a directed loop. `addEdge`
    // writes both, so the only way to reach the half-present case is to write one half directly. Without this
    // the requirement would read as satisfied by either half and nothing would say otherwise.
    val shared = new Shared
    val g = new Graph("circular-edge-one-half", shared, journalOnly)
    try {
      val ops = g.ops
      Await.result(ops.setProp(root, "kind", QuineValue.Str("k")), awaitTimeout)
      Await.result(ops.setProp(leaf, "region", QuineValue.Str("r")), awaitTimeout)
      Await.result(ops.addEdge(root, leaf, "to"), awaitTimeout)

      val results = new Results
      val _ = g.register(loopingKind, results)
      // `literalOps.addEdge` writes both halves, so the half-present case is only reachable by writing one
      // half-edge directly.
      g.addHalfEdge(root, HalfEdge(Symbol("loop"), EdgeDirection.Outgoing, root))
      g.awaiting("the outgoing half of the loop is there")(
        g.stateOf(root).edges.exists(e => e.other == root && e.edgeType == Symbol("loop")),
      )
      withClue("half a directed loop does not satisfy the requirement: ")(results.positiveMatches shouldBe 0)

      g.addHalfEdge(root, HalfEdge(Symbol("loop"), EdgeDirection.Incoming, root))
      results.awaitPositiveMatches(1)
    } finally g.shutdown()
  }
}
