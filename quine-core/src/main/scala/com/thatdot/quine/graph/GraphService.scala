package com.thatdot.quine.graph

import java.util.function.Supplier

import scala.collection.compat.immutable.ArraySeq
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future}
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import akka.actor._

import com.codahale.metrics.{MetricRegistry, SharedMetricRegistries}
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}

import com.thatdot.quine.graph.edgecollection.{EdgeCollection, ReverseOrderedEdgeCollection}
import com.thatdot.quine.graph.messaging.LocalShardRef
import com.thatdot.quine.model._
import com.thatdot.quine.persistor.{EventEffectOrder, PersistenceAgent}
import com.thatdot.quine.util.QuineDispatchers

class GraphService(
  val system: ActorSystem,
  val persistor: PersistenceAgent,
  val idProvider: QuineIdProvider,
  val shardCount: Int,
  val inMemorySoftNodeLimit: Option[Int],
  val inMemoryHardNodeLimit: Option[Int],
  val effectOrder: EventEffectOrder,
  val declineSleepWhenWriteWithinMillis: Long,
  val declineSleepWhenAccessWithinMillis: Long,
  val maxCatchUpSleepMillis: Long,
  val labelsProperty: Symbol,
  val edgeCollectionFactory: Supplier[EdgeCollection],
  val metrics: HostQuineMetrics
) extends StaticShardGraph
    with LiteralOpsGraph
    with AlgorithmGraph
    with CypherOpsGraph
    with StandingQueryOpsGraph {

  initializeNestedObjects()

  val dispatchers = new QuineDispatchers(system)

  def nodeClass: Class[NodeActor] = classOf[NodeActor]

  def initialShardInMemoryLimit: Option[InMemoryNodeLimit] =
    InMemoryNodeLimit.fromOptions(inMemorySoftNodeLimit, inMemoryHardNodeLimit)

  val shards: ArraySeq[LocalShardRef] = initializeShards()

  /** asynchronous construction effect: load Domain Graph Nodes and Standing Queries from the persistor
    */
  Await.result(
    persistor
      .getDomainGraphNodes()
      .flatMap { domainGraphNodes =>
        persistor.getStandingQueries.map { sqs =>
          sqs foreach { sq =>
            // update the references for every domain graph node used by this standing query
            sq.query match {
              case dgnPattern: StandingQueryPattern.DomainGraphNodeStandingQueryPattern =>
                dgnRegistry.registerDomainGraphNodePackage(
                  DomainGraphNodePackage(dgnPattern.dgnId, domainGraphNodes.get(_)),
                  sq.id
                )
              case _ =>
            }
            startStandingQuery(
              sqId = sq.id,
              name = sq.name,
              pattern = sq.query,
              outputs = Map.empty,
              queueBackpressureThreshold = sq.queueBackpressureThreshold,
              queueMaxSize = sq.queueMaxSize,
              shouldCalculateResultHashCode = sq.shouldCalculateResultHashCode
            )
          }
          val dgnsLen = domainGraphNodes.size
          val sqsLen = sqs.size
          if (dgnsLen + sqsLen > 0) logger.info(s"Restored $dgnsLen domain graph nodes and $sqsLen standing queries")
        }(shardDispatcherEC)
      }(shardDispatcherEC),
    10 seconds
  )

  // Provide the [[PersistenceAgent]] with the ready-to-use graph
  persistor.ready(this)

  /* By initializing this last, it will be `false` during the construction and only true
   * once object construction finishes
   */
  @volatile var isReady = true

  override def shutdown(): Future[Unit] = {
    isReady = false
    shutdownStandingQueries()
      .flatMap(_ => super.shutdown())(shardDispatcherEC)
  }
}

object GraphService {

  def apply(
    name: String = "graph-service",
    persistor: ActorSystem => PersistenceAgent,
    idProvider: QuineIdProvider,
    shardCount: Int = 4,
    effectOrder: EventEffectOrder,
    inMemorySoftNodeLimit: Option[Int] = Some(50000),
    inMemoryHardNodeLimit: Option[Int] = Some(75000),
    declineSleepWhenWriteWithinMillis: Long = 100L,
    declineSleepWhenAccessWithinMillis: Long = 0L,
    maxCatchUpSleepMillis: Long = 2000L,
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
        effectOrder,
        declineSleepWhenWriteWithinMillis,
        declineSleepWhenAccessWithinMillis,
        maxCatchUpSleepMillis,
        labelsProperty,
        edgeCollectionFactory,
        HostQuineMetrics(metricRegistry)
      )
    } catch {
      case NonFatal(e) => Future.failed(e)
    }
}
