package com.thatdot.quine.graph

import java.util.concurrent.ConcurrentHashMap
import java.util.function.Supplier

import scala.collection.concurrent
import scala.concurrent.Future
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import akka.actor._

import com.codahale.metrics.{MetricRegistry, SharedMetricRegistries}
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import com.typesafe.scalalogging.StrictLogging

import com.thatdot.quine.graph.edgecollection.{EdgeCollection, ReverseOrderedEdgeCollection}
import com.thatdot.quine.model._
import com.thatdot.quine.persistor.PersistenceAgent

class GraphService(
  val system: ActorSystem,
  val persistor: PersistenceAgent,
  val idProvider: QuineIdProvider,
  val shardCount: Int,
  val inMemorySoftNodeLimit: Option[Int],
  val inMemoryHardNodeLimit: Option[Int],
  val declineSleepWhenWriteWithinMillis: Long,
  val declineSleepWhenAccessWithinMillis: Long,
  val labelsProperty: Symbol,
  val edgeCollectionFactory: Supplier[EdgeCollection],
  val metrics: HostQuineMetrics
) extends StrictLogging
    with StaticShardGraph
    with LiteralOpsGraph
    with CypherOpsGraph
    with StandingQueryOpsGraph {

  initializeNestedObjects()

  def nodeClass: Class[NodeActor] = classOf[NodeActor]

  def initialShardInMemoryLimit: Option[InMemoryNodeLimit] =
    InMemoryNodeLimit.fromOptions(inMemorySoftNodeLimit, inMemoryHardNodeLimit)

  val runningStandingQueries: concurrent.Map[StandingQueryId, RunningStandingQuery] =
    new ConcurrentHashMap[StandingQueryId, RunningStandingQuery]().asScala

  /* By initializing this last, it will be `false` during the construction and only true
   * once object construction finishes
   */
  @volatile var isReady = true

  override def shutdown(): Future[Unit] = {
    isReady = false
    for {
      _ <- shutdownStandingQueries()
      _ <- super.shutdown()
    } yield ()
  }
}

object GraphService {

  def apply(
    name: String = "graph-service",
    persistor: ActorSystem => PersistenceAgent,
    idProvider: QuineIdProvider,
    shardCount: Int = 4,
    inMemorySoftNodeLimit: Option[Int] = Some(50000),
    inMemoryHardNodeLimit: Option[Int] = Some(75000),
    declineSleepWhenWriteWithinMillis: Long = 100L,
    declineSleepWhenAccessWithinMillis: Long = 0L,
    labelsProperty: Symbol = Symbol("__LABEL"),
    edgeCollectionFactory: Supplier[EdgeCollection] = () => new ReverseOrderedEdgeCollection,
    metricRegistry: MetricRegistry = new MetricRegistry
  ): Future[GraphService] =
    try {
      // Must happen before instantiating the actor system extensions
      SharedMetricRegistries.add(HostQuineMetrics.MetricsRegistryName, metricRegistry)

      val baseConfig = ConfigFactory
        .load()
        .withValue(
          "akka.jvm-shutdown-hooks",
          ConfigValueFactory.fromAnyRef(false)
        )
        .withValue(
          "akka.actor.provider",
          ConfigValueFactory.fromAnyRef("local")
        )
        .withValue(
          "akka.extensions",
          ConfigValueFactory.fromIterable(List("com.thatdot.quine.graph.messaging.NodeActorMailboxExtension").asJava)
        )
      val system = ActorSystem(name, baseConfig)
      val persistenceAgent = persistor(system)
      import system.dispatcher

      for {
        _ <- persistenceAgent.syncVersion()
      } yield new GraphService(
        system,
        persistenceAgent,
        idProvider,
        shardCount,
        inMemorySoftNodeLimit,
        inMemoryHardNodeLimit,
        declineSleepWhenWriteWithinMillis,
        declineSleepWhenAccessWithinMillis,
        labelsProperty,
        edgeCollectionFactory,
        HostQuineMetrics(metricRegistry)
      )
    } catch {
      case NonFatal(e) => Future.failed(e)
    }
}
