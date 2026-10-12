package com.thatdot.quine.graph

import java.util.UUID

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters._

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.scaladsl.{Flow, Keep, Sink}
import org.apache.pekko.stream.{KillSwitches, Materializer, UniqueKillSwitch}
import org.apache.pekko.util.Timeout

import com.codahale.metrics.{MetricRegistry, SharedMetricRegistries}
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import org.scalatest.Assertion
import org.scalatest.funsuite.AnyFunSuite

import com.thatdot.quine.graph.StandingQueryPattern.MultipleValuesQueryPattern
import com.thatdot.quine.graph.cypher.MultipleValuesStandingQuery
import com.thatdot.quine.graph.edges.ReverseOrderedEdgeCollection
import com.thatdot.quine.graph.metrics.HostQuineMetrics
import com.thatdot.quine.persistor.{EventEffectOrder, InMemoryPersistor, PersistenceConfig, StatelessPrimePersistor}
import com.thatdot.quine.util.TestLogging._

/** Whether a standing query's metrics outlive the query.
  *
  * [[com.thatdot.quine.graph.metrics.HostQuineMetricsStandingQueryTest]] covers what removal retires; this
  * covers whether removal is reached at all, from each way a standing query can stop existing. That is the
  * half a developer can forget, so each of those ways gets its own test rather than being taken on trust
  * from the one that cancels explicitly.
  */
class StandingQueryMetricsLifecycleTests extends AnyFunSuite {

  implicit private val timeout: Timeout = Timeout(10.seconds)

  private val sqName = "watcher"
  private val sqId: StandingQueryId = StandingQueryId(new UUID(0L, 1L))

  /** A pattern that compiles to nothing in particular: these tests never feed the query any data, they only
    * start and stop it.
    */
  private val pattern = MultipleValuesQueryPattern(
    MultipleValuesStandingQuery.UnitSq.instance,
    includeCancellation = true,
    PatternOrigin.DirectSqV4,
  )

  private def withGraph(check: (GraphService, MetricRegistry) => Assertion): Assertion = {
    val metricRegistry = new MetricRegistry
    val _ = SharedMetricRegistries.add(HostQuineMetrics.MetricsRegistryName, metricRegistry)
    val system = ActorSystem(
      s"sq-metrics-lifecycle-${UUID.randomUUID()}",
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
    )

    try check(graph, metricRegistry)
    finally {
      val _ = Await.result(graph.shutdown(), 30.seconds)
    }
  }

  /** Every registered metric name belonging to the standing query these tests start. */
  private def sqMetricNames(registry: MetricRegistry): Set[String] =
    registry.getMetrics.asScala.keys
      .filter(_.contains(s"${HostQuineMetrics.StandingQueryMetricComponent}."))
      .filter(n => n.endsWith(s".$sqName") || n.endsWith(s".${sqId.uuid}"))
      .toSet

  /** A sink that consumes results and does nothing with them -- the outputs a query is given are beside
    * the point here, but it must have at least the kill switch shape the API requires.
    */
  private def ignoringOutput: Sink[StandingQueryResult, UniqueKillSwitch] =
    Flow[StandingQueryResult]
      .viaMat(KillSwitches.single)(Keep.right)
      .toMat(Sink.ignore)(Keep.left)

  private def createSq(sqns: StandingQueryOpsGraph#NamespaceStandingQueries): RunningStandingQuery =
    sqns
      .createStandingQuery(
        name = sqName,
        pattern = pattern,
        outputs = Map("out" -> ignoringOutput),
        shouldCalculateResultHashCode = true,
        sqId = sqId,
      )
      ._1

  test("starting a standing query registers its metrics") {
    withGraph { (graph, registry) =>
      val sqns = graph.standingQueries(defaultNamespaceId).get
      val _ = createSq(sqns)

      // Every name-keyed metric; the states histogram belongs to nodes persisting state, which none do here.
      assert(sqMetricNames(registry).size == 5)
    }
  }

  test("cancelling a standing query retires its metrics") {
    withGraph { (graph, registry) =>
      val sqns = graph.standingQueries(defaultNamespaceId).get
      val _ = createSq(sqns)
      assert(sqMetricNames(registry).nonEmpty)

      val _ = Await.result(sqns.cancelStandingQuery(sqId).get, 10.seconds)

      assert(sqMetricNames(registry).isEmpty)
    }
  }

  // The cancellation response is built from the query's metrics, and the metrics are gone by the time it is
  // built. This is what makes retiring them that early safe, so it is asserted rather than assumed.
  test("the cancellation still reports the query's final counts") {
    withGraph { (graph, _) =>
      val sqns = graph.standingQueries(defaultNamespaceId).get
      val runningSq = createSq(sqns)
      runningSq.metrics.resultMeter.mark(7)
      runningSq.metrics.resultHashCode.add(42)

      val cancelled = Await.result(sqns.cancelStandingQuery(sqId).get, 10.seconds)

      assert(cancelled.resultRates.getCount == 7, "the reported rates came from a meter the registry replaced")
      assert(cancelled.outputHashCode == 42)
    }
  }

  // Deleting a namespace cancels its queries wholesale, without naming any of them.
  test("deleting a namespace retires its standing queries' metrics") {
    withGraph { (graph, registry) =>
      val ns = NamespaceId("doomed")
      assert(Await.result(graph.createNamespace(ns), 10.seconds))
      val _ = createSq(graph.standingQueries(ns).get)
      assert(sqMetricNames(registry).nonEmpty)

      assert(Await.result(graph.deleteNamespace(ns), 10.seconds))

      assert(sqMetricNames(registry).isEmpty)
    }
  }
}
