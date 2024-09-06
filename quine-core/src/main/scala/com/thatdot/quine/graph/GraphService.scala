package com.thatdot.quine.graph

import scala.collection.immutable.ArraySeq
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import org.apache.pekko.actor._
import org.apache.pekko.util.Timeout

import com.codahale.metrics.{MetricRegistry, SharedMetricRegistries}
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}

import com.thatdot.quine.graph.edges.{ReverseOrderedEdgeCollection, SyncEdgeCollection}
import com.thatdot.quine.graph.messaging.LocalShardRef
import com.thatdot.quine.graph.messaging.ShardMessage.{CreateNamespace, DeleteNamespace}
import com.thatdot.quine.model._
import com.thatdot.quine.persistor.{EventEffectOrder, PrimePersistor}
import com.thatdot.quine.util.Log._
import com.thatdot.quine.util.Log.implicits._
import com.thatdot.quine.util.QuineDispatchers

class GraphService(
  val system: ActorSystem,
  val namespacePersistor: PrimePersistor,
  val idProvider: QuineIdProvider,
  val shardCount: Int,
  val inMemorySoftNodeLimit: Option[Int],
  val inMemoryHardNodeLimit: Option[Int],
  val effectOrder: EventEffectOrder,
  val declineSleepWhenWriteWithinMillis: Long,
  val declineSleepWhenAccessWithinMillis: Long,
  val maxCatchUpSleepMillis: Long,
  val labelsProperty: Symbol,
  val edgeCollectionFactory: QuineId => SyncEdgeCollection,
  val metrics: HostQuineMetrics
)(implicit val logConfig: LogConfig)
    extends StaticShardGraph
    with LiteralOpsGraph
    with AlgorithmGraph
    with CypherOpsGraph
    with StandingQueryOpsGraph
    with QuinePatternOpsGraph {

  initializeNestedObjects()

  val dispatchers = new QuineDispatchers(system)

  type Node = NodeActor
  type Snapshot = NodeSnapshot
  type NodeConstructorRecord = NodeConstructorArgs
  val nodeStaticSupport: StaticNodeSupport[NodeActor, NodeSnapshot, NodeConstructorArgs] = StaticNodeActorSupport

  def initialShardInMemoryLimit: Option[InMemoryNodeLimit] =
    InMemoryNodeLimit.fromOptions(inMemorySoftNodeLimit, inMemoryHardNodeLimit)

  val shards: ArraySeq[LocalShardRef] = initializeShards()(logConfig)

  /** asynchronous construction effect: load Domain Graph Nodes and Standing Queries from the persistor
    */
  Await.result(
    namespacePersistor
      .getDomainGraphNodes()
      .flatMap { domainGraphNodes =>
        namespacePersistor
          .getAllStandingQueries()
          .map {
            _ foreach { case (namespace, sqs) =>
              sqs.foreach { sq =>
                standingQueries(namespace).foreach { sqns =>
                  // do pre-run checks and initialization for the standing query (if its namespace still exists)
                  sq.queryPattern match {
                    // in the case of a DGN query, register the DGN package against the in-memory registry
                    case dgnPattern: StandingQueryPattern.DomainGraphNodeStandingQueryPattern =>
                      dgnRegistry.registerDomainGraphNodePackage(
                        DomainGraphNodePackage(dgnPattern.dgnId, domainGraphNodes.get(_)),
                        sq.id
                      )
                    // in the case of an SQv4 query, do a final verification that the pattern origin is sane
                    case StandingQueryPattern.MultipleValuesQueryPattern(_, _, PatternOrigin.DirectSqV4) =>
                      // no additional validation is needed for MVSQs that use a SQV4 origin
                      ()
                    case StandingQueryPattern
                          .MultipleValuesQueryPattern(_, _, PatternOrigin.GraphPattern(pattern, cypherOriginal))
                        if pattern.distinct =>
                      // For an MVSQ based on a GraphPattern, warn the user that DISTINCT is not yet supported in MVSQ.
                      // QU-568
                      logger.warn(
                        cypherOriginal match {
                          case Some(cypherQuery) =>
                            log"Read a GraphPattern for a MultipleValues query with a DISTINCT clause. This is not yet supported. Query was: '${cypherQuery}'"
                          case None =>
                            log"Read a GraphPattern for a MultipleValues query that specifies `distinct`. This is not yet supported. Query pattern was: ${pattern.toString}"
                        }
                      )
                    case StandingQueryPattern
                          .MultipleValuesQueryPattern(_, _, PatternOrigin.GraphPattern(_, _)) =>
                      // this is an MVSQ based on a GraphPattern, but it doesn't illegally specify DISTINCT. No further action needed.
                      ()
                    case StandingQueryPattern.QuinePatternQueryPattern(_, _, _) =>
                      // no additional validation is needed for QuinePattern SQs
                      ()
                  }
                  sqns.startStandingQuery(
                    sqId = sq.id,
                    name = sq.name,
                    pattern = sq.queryPattern,
                    outputs = Map.empty,
                    queueBackpressureThreshold = sq.queueBackpressureThreshold,
                    queueMaxSize = sq.queueMaxSize,
                    shouldCalculateResultHashCode = sq.shouldCalculateResultHashCode
                  )
                }
              }
              val dgnsLen = domainGraphNodes.size
              val sqsLen = sqs.size
              if (dgnsLen + sqsLen > 0)
                logger.info(log"Restored ${Safe(dgnsLen)} domain graph nodes and ${Safe(sqsLen)} standing queries")
            }
          }(shardDispatcherEC)
      }(shardDispatcherEC),
    10.seconds
  )

  // Provide the [[PersistenceAgent]] with the ready-to-use graph
  namespacePersistor.declareReady(this)

  /* By initializing this last, it will be `false` during the construction and only true
   * once object construction finishes
   */
  @volatile private var _isReady = true
  def isReady: Boolean = _isReady

  def isAppLoaded: Boolean = true

  override def shutdown(): Future[Unit] = {
    _isReady = false
    implicit val ec = nodeDispatcherEC
    Future
      .sequence(
        getNamespaces.map(ns => standingQueries(ns).fold(Future.unit)(_.shutdownStandingQueries()))
      )
      .flatMap { _ =>
        super.shutdown()
      }(ExecutionContext.parasitic)
  }

  /** Make a new namespace. The outer future indicates success or failure. The inner Boolean indicates whether a
    * change was made.
    */
  def createNamespace(namespace: NamespaceId)(implicit timeout: Timeout): Future[Boolean] = {
    val didChange = !getNamespaces.contains(namespace)
    if (didChange) {
      namespaceCache += namespace
      namespacePersistor.createNamespace(namespace)
      addStandingQueryNamespace(namespace)
      askAllShards(CreateNamespace(namespace, _)).map(_.exists(_.didHaveEffect))(shardDispatcherEC)
    } else Future.successful(false)
  }

  /** Remove an existing namespace. The outer future indicates success or failure. The inner Boolean indicates whether
    * a change was made.
    */
  def deleteNamespace(namespace: NamespaceId)(implicit timeout: Timeout): Future[Boolean] = {
    val didChange = getNamespaces.contains(namespace)
    if (didChange) {
      removeStandingQueryNamespace(namespace)
      namespaceCache -= namespace
      askAllShards(DeleteNamespace(namespace, _))
        .map { _ =>
          namespacePersistor
            .deleteNamespace(namespace)
        }(nodeDispatcherEC)
        .map(_ => true)(ExecutionContext.parasitic)
    } else Future.successful(false)
  }

  /** Get a set of existing namespaces. This is served by a local cache and meant to be fast and inexpensive.
    * `getNamespaces.contains(myNamespace)` can be called before every operation that uses a non-default namespace to
    * ensure the namespace exists, or otherwise fail fast before other actions.
    */
  def getNamespaces: collection.Set[NamespaceId] = namespaceCache
}

object GraphService {

  def apply(
    name: String = "graph-service",
    persistorMaker: ActorSystem => PrimePersistor,
    idProvider: QuineIdProvider,
    shardCount: Int = 4,
    effectOrder: EventEffectOrder,
    inMemorySoftNodeLimit: Option[Int] = Some(50000),
    inMemoryHardNodeLimit: Option[Int] = Some(75000),
    declineSleepWhenWriteWithinMillis: Long = 100L,
    declineSleepWhenAccessWithinMillis: Long = 0L,
    maxCatchUpSleepMillis: Long = 2000L,
    labelsProperty: Symbol = Symbol("__LABEL"),
    edgeCollectionFactory: QuineId => SyncEdgeCollection = new ReverseOrderedEdgeCollection(_),
    metricRegistry: MetricRegistry = new MetricRegistry,
    enableDebugMetrics: Boolean = false
  )(implicit logConfig: LogConfig): Future[GraphService] =
    try {
      // Must happen before instantiating the actor system extensions
      SharedMetricRegistries.add(HostQuineMetrics.MetricsRegistryName, metricRegistry)

      val baseConfig = ConfigFactory
        .load()
        .withValue(
          "pekko.actor.provider",
          ConfigValueFactory.fromAnyRef("local")
        )
        .withValue(
          "pekko.extensions",
          ConfigValueFactory.fromIterable(List("com.thatdot.quine.graph.messaging.NodeActorMailboxExtension").asJava)
        )
      val system = ActorSystem(name, baseConfig)
      val namespacePersistor = persistorMaker(system)
      import system.dispatcher

      for {
        _ <- namespacePersistor.syncVersion()
      } yield new GraphService(
        system,
        namespacePersistor,
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
        HostQuineMetrics(enableDebugMetrics, metricRegistry)
      )
    } catch {
      case NonFatal(e) => Future.failed(e)
    }
}
