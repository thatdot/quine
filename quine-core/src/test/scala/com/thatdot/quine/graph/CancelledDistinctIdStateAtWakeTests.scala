package com.thatdot.quine.graph

import java.util.UUID

import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.Sink
import org.apache.pekko.util.Timeout

import cats.data.NonEmptyList
import com.codahale.metrics.{MetricRegistry, SharedMetricRegistries}
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import org.scalatest.Assertion
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.thatdot.common.quineid.QuineId
import com.thatdot.quine.graph.behavior.DomainNodeIndexBehavior.{DomainNodeIndex, SubscribersToThisNodeUtil}
import com.thatdot.quine.graph.edges.ReverseOrderedEdgeCollection
import com.thatdot.quine.graph.messaging.LiteralMessage.SqStateResults
import com.thatdot.quine.graph.metrics.HostQuineMetrics
import com.thatdot.quine.model.{
  DependsUpon,
  DomainEdge,
  DomainNodeEquiv,
  EdgeDirection,
  GenericEdge,
  HalfEdge,
  PropertyComparisonFunctions,
  PropertyValue,
  QuineValue,
  SingleBranch,
}
import com.thatdot.quine.persistor.{
  EventEffectOrder,
  InMemoryPersistor,
  NamespacedPersistenceAgent,
  PersistenceConfig,
  StatelessPrimePersistor,
}
import com.thatdot.quine.util.TestLogging._

/** What a waking node does with DistinctId bookkeeping whose query it cannot find.
  *
  * Same question `CancelledStandingQueryStateAtWakeTests` asks of MultipleValues state. A subscription is dead
  * once none of the queries named for its pattern runs, whether the subscriber is a query or a node: a node names
  * the queries it depends on this node for, and a running one keeps the pattern registered. A dead subscription
  * goes with the answers it was computed from. An answer about a pattern no live subscription here has as a child
  * belongs to nobody and goes too, or a later pattern with that child would take it as current.
  *
  * Not finding a query is either a cancellation or a restore that has not finished, and the two are the same
  * observation from the node, so the wake asks the graph whether it has finished restoring first.
  */
class CancelledDistinctIdStateAtWakeTests extends AnyFunSuite with Matchers {

  implicit private val timeout: Timeout = Timeout(10.seconds)

  private val node: QuineId = QuineId(Array(1.toByte))
  private val peer: QuineId = QuineId(Array(2.toByte))
  private val liveQuery: StandingQueryId = StandingQueryId(new UUID(0L, 1L))
  private val unknownQuery: StandingQueryId = StandingQueryId(new UUID(0L, 2L))

  private def hasProperty(key: String): DomainNodeEquiv =
    DomainNodeEquiv(None, Map(Symbol(key) -> ((PropertyComparisonFunctions.Wildcard, None))), Set.empty)

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
  private val twoHopDgn: Long = twoHop.toDomainGraphNodePackage.dgnId
  private val regionDgn: Long = SingleBranch(hasProperty("region"), nextBranches = Nil).toDomainGraphNodePackage.dgnId
  private val orphanDgn: Long = SingleBranch(hasProperty("orphan"), nextBranches = Nil).toDomainGraphNodePackage.dgnId
  private val goneDgn: Long = SingleBranch(hasProperty("gone"), nextBranches = Nil).toDomainGraphNodePackage.dgnId

  /** A graph, optionally standing in for one whose standing query restore has not finished. See the MultipleValues
    * test for why the window is overridden rather than arranged. `seed` writes the node's stored state.
    */
  private def withGraph(restoredOverride: Option[Boolean])(seed: NamespacedPersistenceAgent => Unit)(
    check: GraphService => Assertion,
  ): Assertion = {
    val metricRegistry = new MetricRegistry
    val _ = SharedMetricRegistries.add(HostQuineMetrics.MetricsRegistryName, metricRegistry)
    val system = ActorSystem(
      s"cancelled-distinctid-state-at-wake-${restoredOverride.fold("real")(_.toString)}",
      ConfigFactory
        .load()
        .withValue("pekko.actor.provider", ConfigValueFactory.fromAnyRef("local"))
        .withValue(
          "pekko.extensions",
          ConfigValueFactory.fromIterable(
            java.util.Arrays.asList("com.thatdot.quine.graph.messaging.NodeActorMailboxExtension"),
          ),
        ),
    )
    val primePersistor = new StatelessPrimePersistor(
      PersistenceConfig(),
      None,
      (pc, ns) => new InMemoryPersistor(persistenceConfig = pc, namespace = ns),
    )(Materializer.matFromSystem(system), logConfig)
    Await.result(primePersistor.syncVersion(), 10.seconds)

    val graph = new GraphService(
      system,
      primePersistor,
      IdentityIdProvider,
      shardCount = 1,
      inMemorySoftNodeLimit = Some(50000),
      inMemoryHardNodeLimit = Some(75000),
      effectOrder = EventEffectOrder.PersistorFirst,
      declineSleepWhenWriteWithinMillis = 0L,
      declineSleepWhenAccessWithinMillis = 0L,
      labelsProperty = Symbol("__LABEL"),
      edgeCollectionFactory = new ReverseOrderedEdgeCollection(_),
      metrics = HostQuineMetrics(enableDebugMetrics = false, metricRegistry, omitDefaultNamespace = false),
    ) {
      override def standingQueriesRestored: Boolean =
        restoredOverride.getOrElse(super.standingQueriesRestored)
    }

    try {
      // A live query whose child pattern is `region`, so that pattern is in the registry.
      val dgnPackage = twoHop.toDomainGraphNodePackage
      Await.result(
        graph.dgnRegistry.registerAndPersistDomainGraphNodePackage(dgnPackage, liveQuery, skipPersistor = true),
        10.seconds,
      )
      graph
        .standingQueries(defaultNamespaceId)
        .getOrElse(fail("default namespace should exist"))
        .createStandingQuery(
          name = "live",
          pattern = StandingQueryPattern.DomainGraphNodeStandingQueryPattern(
            dgnId = dgnPackage.dgnId,
            formatReturnAsStr = false,
            aliasReturnAs = Symbol("id"),
            includeCancellation = false,
            origin = PatternOrigin.DirectDgb,
          ),
          outputs = Map.empty,
          sqId = liveQuery,
        )
      seed(graph.namespacePersistor(defaultNamespaceId).getOrElse(fail("default namespace persistor")))
      check(graph)
    } finally {
      val _ = Await.result(graph.shutdown(), 30.seconds)
    }
  }

  private def at(seq: Long): EventTime = EventTime(1_000_000L, timestampSequence = seq)

  /** Exactly what a node would have journaled: a peer subscribed to it on `region`, which the live query keeps
    * registered, naming a query this graph has never been told about; the same peer subscribed on `gone`, a
    * pattern nothing registers; that unknown query itself subscribed on `region`; and an answer from the peer
    * for a pattern this node never asked about.
    */
  private def seedJournal(persistor: NamespacedPersistenceAgent): Unit =
    Await.result(
      persistor.persistDomainIndexEvents(
        node,
        NonEmptyList.of(
          NodeEvent.WithTime(DomainIndexEvent.CreateDomainNodeSubscription(regionDgn, peer, Set(unknownQuery)), at(0)),
          NodeEvent.WithTime(DomainIndexEvent.CreateDomainNodeSubscription(goneDgn, peer, Set(unknownQuery)), at(1)),
          NodeEvent.WithTime(
            DomainIndexEvent.CreateDomainStandingQuerySubscription(regionDgn, unknownQuery, Set(unknownQuery)),
            at(2),
          ),
          NodeEvent.WithTime(DomainIndexEvent.DomainNodeSubscriptionResult(peer, orphanDgn, result = true), at(3)),
        ),
      ),
      10.seconds,
    )

  /** A snapshot of the node as the root of the live query, matched locally with an edge to the peer, holding the
    * peer's answers: about `region`, the live query's child; and about `gone` and `orphan`, children of nothing
    * subscribed here.
    */
  private def seedSnapshot(persistor: NamespacedPersistenceAgent): Unit = {
    val snapshot = NodeSnapshot(
      at(0),
      Map(Symbol("kind") -> PropertyValue(QuineValue.Str("x"))),
      List(HalfEdge(Symbol("to"), EdgeDirection.Outgoing, peer)),
      mutable.Map(
        twoHopDgn -> SubscribersToThisNodeUtil
          .DistinctIdSubscription(
            latestAnswer = Some(true),
            queriesPerSubscriber = Map(Right(liveQuery) -> Set(liveQuery)),
          ),
      ),
      mutable.Map(
        peer -> mutable.Map(
          regionDgn -> DomainNodeIndex.DomainIndexResult(Some(true), Set(liveQuery)),
          // Asked for on behalf of a query this graph does not run, which is what makes them answers nothing
          // live depends on. An empty set would mean something else -- an entry written before the queries were
          // recorded -- and would be judged by the fallback rule rather than by query liveness.
          goneDgn -> DomainNodeIndex.DomainIndexResult(Some(true), Set(unknownQuery)),
          orphanDgn -> DomainNodeIndex.DomainIndexResult(Some(true), Set(unknownQuery)),
        ),
      ),
    )
    Await.result(persistor.persistSnapshot(node, at(0), NodeSnapshot.snapshotCodec.format.write(snapshot)), 10.seconds)
  }

  /** Wake the node and read its bookkeeping back. The wake settles it in the constructor, before the actor serves
    * any other message, so the first reply already reflects it.
    */
  private def wakeAndRead(graph: GraphService): SqStateResults =
    Await.result(graph.literalOps(defaultNamespaceId).logState(node), 20.seconds).sqStateResults

  private def journaledOn(graph: GraphService): List[DomainIndexEvent] = {
    val persistor = graph.namespacePersistor(defaultNamespaceId).getOrElse(fail("default namespace persistor"))
    Await
      .result(
        persistor
          .getDomainIndexEventsWithTime(node, EventTime.MinValue, EventTime.MaxValue)
          .runWith(Sink.seq)(Materializer.matFromSystem(graph.system)),
        10.seconds,
      )
      .map(_.event)
      .toList
  }

  private def held(state: SqStateResults): Set[(Long, QuineId)] = state.subscriptions.map(r => (r.dgnId, r.peer)).toSet

  /** What a wake here is expected to write: its subscription for the live query, which the seeded state lacks, and
    * a record of each subscription it retires. The retirement is written because *when* a node found the query gone
    * is not recoverable from the query's absence afterwards, and a replay needs it; see
    * [[DomainIndexEvent.CancelDomainStandingQuerySubscription]].
    */
  private def subscribesLiveQuery(event: DomainIndexEvent): Boolean = event match {
    case DomainIndexEvent.CreateDomainStandingQuerySubscription(_, `liveQuery`, _) => true
    case _ => false
  }

  /** Either kind of retirement: a standing query's subscription, or another node's. Both are recorded, because
    * when this node found the queries behind one gone is not recoverable from their absence afterwards.
    */
  private def expectedOfEveryWake(event: DomainIndexEvent): Boolean = event match {
    case _: DomainIndexEvent.CancelDomainStandingQuerySubscription => true
    case _: DomainIndexEvent.CancelDomainNodeSubscription => true
    case other => subscribesLiveQuery(other)
  }

  test("a graph that has restored its standing queries drops the subscriptions no running query depends on") {
    withGraph(restoredOverride = None)(seedJournal) { graph =>
      assert(graph.standingQueriesRestored, "the graph never declared its standing queries restored")
      val before = journaledOn(graph)
      val state = wakeAndRead(graph)
      withClue(
        "a node subscribed for a query that does not run is dropped, whether or not its pattern is registered: ",
      )(
        state.subscribers
          .flatMap(r => r.subscriberNode.map((r.dgnId, _))) should contain noneOf ((regionDgn, peer), (goneDgn, peer)),
      )
      withClue("an answer for a pattern this node never asked about: ")(
        held(state) should not contain ((orphanDgn, peer)),
      )
      val written = journaledOn(graph).drop(before.size)
      withClue("nothing was written beyond the live query's subscription and the retirements: ")(
        written.filterNot(expectedOfEveryWake) shouldBe empty,
      )
      withClue("and the retirement of the cancelled query's subscription was recorded: ")(
        written.collect { case e: DomainIndexEvent.CancelDomainStandingQuerySubscription =>
          e.alreadyCancelledSubscriber
        } should contain(unknownQuery),
      )
    }
  }

  test("a graph still restoring its standing queries leaves alone subscriptions it cannot judge") {
    withGraph(restoredOverride = Some(false))(seedJournal) { graph =>
      val before = journaledOn(graph)
      val state = wakeAndRead(graph)
      withClue("a subscription this graph cannot yet tell from a live one was dropped: ")(
        state.subscribers
          .flatMap(r => r.subscriberNode.map((r.dgnId, _))) should contain allOf ((regionDgn, peer), (goneDgn, peer)),
      )
      withClue("and nothing was written, beyond subscribing for the live query: ")(
        journaledOn(graph).drop(before.size).filterNot(subscribesLiveQuery) shouldBe empty,
      )
    }
  }

  test("an answer is kept while a live subscription here has its pattern as a child, and dropped otherwise") {
    withGraph(restoredOverride = None)(seedSnapshot) { graph =>
      val state = wakeAndRead(graph)
      withClue("the live query's child: ")(held(state) should contain((regionDgn, peer)))
      withClue("children of nothing subscribed here: ")(
        held(state) should contain noneOf ((goneDgn, peer), (orphanDgn, peer)),
      )
      Thread.sleep(500)
      withClue("nothing to ask the peer about, so nothing written: ")(journaledOn(graph) shouldBe empty)
    }
  }

  test("a graph still restoring its standing queries keeps every answer and asks nobody") {
    withGraph(restoredOverride = Some(false))(seedSnapshot) { graph =>
      val state = wakeAndRead(graph)
      held(state) should contain allOf ((regionDgn, peer), (goneDgn, peer), (orphanDgn, peer))
      Thread.sleep(500)
      journaledOn(graph) shouldBe empty
    }
  }
}
