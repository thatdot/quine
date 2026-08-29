package com.thatdot.quine.graph

import java.util.UUID

import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.Materializer
import org.apache.pekko.util.Timeout

import com.codahale.metrics.{MetricRegistry, SharedMetricRegistries}
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import org.scalatest.Assertion
import org.scalatest.concurrent.Eventually
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.time.{Millis, Seconds, Span}

import com.thatdot.common.quineid.QuineId
import com.thatdot.quine.graph.behavior.MultipleValuesStandingQueryPartSubscription
import com.thatdot.quine.graph.cypher.LocalPropertyState
import com.thatdot.quine.graph.edges.ReverseOrderedEdgeCollection
import com.thatdot.quine.graph.messaging.StandingQueryMessage.MultipleValuesStandingQuerySubscriber
import com.thatdot.quine.graph.metrics.HostQuineMetrics
import com.thatdot.quine.persistor.codecs.MultipleValuesStandingQueryStateCodec
import com.thatdot.quine.persistor.{
  EventEffectOrder,
  InMemoryPersistor,
  NamespacedPersistenceAgent,
  PersistenceConfig,
  StatelessPrimePersistor,
}
import com.thatdot.quine.util.TestLogging._

/** What a waking node does with standing query state whose query it cannot find.
  *
  * Not finding a query means one of two things, and they are not distinguishable from the state itself: the query was
  * cancelled while the node slept, or this graph has not finished restoring its queries yet. The first is worth
  * deleting the state for; doing it to the second destroys state that is perfectly good, and nothing on the wake path
  * refuses a wake that arrives during a restore. So the deletion asks the graph whether it has finished, and these
  * are the two answers.
  */
class CancelledStandingQueryStateAtWakeTests extends AnyFunSuite with Eventually {

  implicit private val timeout: Timeout = Timeout(10.seconds)

  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(10, Seconds), interval = Span(20, Millis))

  private val node: QuineId = QuineId(Array(1.toByte))
  private val sqId: StandingQueryId = StandingQueryId(new UUID(0L, 1L))
  private val partId: MultipleValuesStandingQueryPartId = MultipleValuesStandingQueryPartId(new UUID(0L, 2L))

  /** A state of a query this graph has never been told about, exactly as a node would have left it. */
  private val stateBytes: Array[Byte] = {
    val subscription = MultipleValuesStandingQueryPartSubscription(
      partId,
      sqId,
      mutable.Set[MultipleValuesStandingQuerySubscriber](
        MultipleValuesStandingQuerySubscriber.GlobalSubscriber(sqId),
      ),
    )
    MultipleValuesStandingQueryStateCodec.format.write(subscription -> LocalPropertyState(partId))
  }

  /** A graph, optionally standing in for one whose standing query restore has not finished.
    *
    * The window is overridden rather than arranged, because arranging it means holding up the restore that declares
    * it, which also holds up the persistor this test has to read. Passing `None` leaves the graph's own flag
    * alone, so the ordinary case is the real thing rather than an assertion about a stub.
    */
  private def withGraph(
    restoredOverride: Option[Boolean],
  )(check: (GraphService, NamespacedPersistenceAgent) => Assertion): Assertion = {
    val metricRegistry = new MetricRegistry
    val _ = SharedMetricRegistries.add(HostQuineMetrics.MetricsRegistryName, metricRegistry)
    val system = ActorSystem(
      s"cancelled-sq-state-at-wake-${restoredOverride.fold("real")(_.toString)}",
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
      maxCatchUpSleepMillis = 2000L,
      labelsProperty = Symbol("__LABEL"),
      edgeCollectionFactory = new ReverseOrderedEdgeCollection(_),
      metrics = HostQuineMetrics(enableDebugMetrics = false, metricRegistry, omitDefaultNamespace = false),
    ) {
      // Left alone unless a test is standing in for the window before the restore finished, so the ordinary case
      // below runs against the flag the graph sets for itself, which is what says the declaration is wired up.
      override def standingQueriesRestored: Boolean =
        restoredOverride.getOrElse(super.standingQueriesRestored)
    }

    try {
      val persistor = graph.namespacePersistor(defaultNamespaceId).get
      Await.result(persistor.setMultipleValuesStandingQueryState(sqId, node, partId, Some(stateBytes)), 10.seconds)
      // Nothing has woken this node yet, so what it holds is only what was just written under it.
      assert(statesOnDisk(persistor).contains(sqId -> partId))
      check(graph, persistor)
    } finally {
      val _ = Await.result(graph.shutdown(), 30.seconds)
    }
  }

  private def statesOnDisk(
    persistor: NamespacedPersistenceAgent,
  ): Set[(StandingQueryId, MultipleValuesStandingQueryPartId)] =
    Await.result(persistor.getMultipleValuesStandingQueryStates(node), 10.seconds).keySet

  /** Wake the node, and return once it is awake: a reply means its constructor ran, which is where the deletion is
    * decided. What the decision then does to disk is not waited on, which is why the assertions differ in shape.
    */
  private def wake(graph: GraphService): Unit = {
    val _ = Await.result(graph.literalOps(defaultNamespaceId).getProps(node), 20.seconds)
  }

  test("a graph that has restored its standing queries deletes the state of one it cannot find") {
    withGraph(restoredOverride = None) { (graph, persistor) =>
      assert(graph.standingQueriesRestored, "the graph never declared its standing queries restored")
      wake(graph)
      eventually {
        assert(
          !statesOnDisk(persistor).contains(sqId -> partId),
          "the node woke, found a state whose query is gone, and left it to be found again at every future wake",
        )
      }
    }
  }

  test("a graph still restoring its standing queries leaves alone the state of one it cannot find") {
    withGraph(restoredOverride = Some(false)) { (graph, persistor) =>
      wake(graph)
      // The node is awake, so the decision has been made; a deletion would already have been issued. Asserting it
      // stays across a window that comfortably covers the other test's deletion is what says none was.
      (0 until 50).foreach { _ =>
        assert(
          statesOnDisk(persistor).contains(sqId -> partId),
          "the node deleted state for a query this graph had not restored yet, which it cannot tell from cancelled",
        )
        Thread.sleep(10)
      }
      assert(statesOnDisk(persistor).contains(sqId -> partId))
    }
  }
}
