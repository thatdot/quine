package com.thatdot.quine.app.v2api.definitions

import java.util.Properties

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters.CollectionHasAsScala
import scala.util.Either

import org.apache.pekko.stream.{Materializer, StreamDetachedException}
import org.apache.pekko.util.Timeout

import cats.data.{EitherT, NonEmptyList}
import cats.implicits._
import shapeless.{:+:, CNil, Coproduct}

import com.thatdot.api.v2.ErrorResponse.{BadRequest, NotFound, ServerError}
import com.thatdot.api.v2.ErrorType
import com.thatdot.common.logging.Log._
import com.thatdot.common.quineid.QuineId
import com.thatdot.quine.app.config.BaseConfig
import com.thatdot.quine.app.model.ingest.util.KafkaSettingsValidator
import com.thatdot.quine.app.model.ingest.util.KafkaSettingsValidator.ErrorString
import com.thatdot.quine.app.model.ingest2.KafkaIngest
import com.thatdot.quine.app.model.ingest2.V2IngestEntities.{QuineIngestConfiguration => V2IngestConfiguration}
import com.thatdot.quine.app.model.ingest2.source.QuineValueIngestQuery
import com.thatdot.quine.app.routes._
import com.thatdot.quine.app.v2api.converters._
import com.thatdot.quine.app.v2api.definitions.ApiUiStyling.{SampleQuery, UiNodeAppearance, UiNodeQuickQuery}
import com.thatdot.quine.app.v2api.definitions.ingest2.{ApiIngest, DeadLetterQueueOutput}
import com.thatdot.quine.app.v2api.definitions.outputs.QuineDestinationSteps
import com.thatdot.quine.app.v2api.definitions.query.{standing => ApiStanding}
import com.thatdot.quine.app.v2api.endpoints.V2AdministrationEndpointEntities.{TGraphHashCode, TQuineInfo}
import com.thatdot.quine.app.{BaseApp, BuildInfo, SchemaCache}
import com.thatdot.quine.compiler.cypher
import com.thatdot.quine.exceptions.NamespaceNotFoundException
import com.thatdot.quine.graph.cypher.CypherException
import com.thatdot.quine.graph.{
  AlgorithmGraph,
  BaseGraph,
  CypherOpsGraph,
  InMemoryNodeLimit,
  InvalidQueryPattern,
  LiteralOpsGraph,
  MemberIdx,
  NamespaceId,
  StandingQueryOpsGraph,
  namespaceToString,
}
import com.thatdot.quine.model.Milliseconds
import com.thatdot.quine.persistor.PersistenceAgent
import com.thatdot.quine.{BuildInfo => QuineBuildInfo, routes => V1}

sealed trait ProductVersion
object ProductVersion {
  case object Novelty extends ProductVersion

  case object Oss extends ProductVersion

  case object Enterprise extends ProductVersion
}

trait ApplicationApiMethods {
  val graph: BaseGraph with LiteralOpsGraph with CypherOpsGraph
  val app: BaseApp with SchemaCache with QueryUiConfigurationState
  def productVersion: ProductVersion
  implicit def timeout: Timeout
  implicit val logConfig: LogConfig
  implicit def materializer: Materializer = graph.materializer
  val config: BaseConfig

  def emptyConfigExample: BaseConfig

  def isReady: Boolean = graph.isReady

  def isLive = true

  // --------------------- Admin Endpoints ------------------------
  def performShutdown(): Future[Unit] = {
    graph.system.terminate()
    Future.successful(())
  }

  def graphHashCode(atTime: Option[Milliseconds], namespace: NamespaceId): Future[TGraphHashCode] =
    graph.requiredGraphIsReadyFuture {
      val at = atTime.getOrElse(Milliseconds.currentTime())
      graph
        .getGraphHashCode(namespace, Some(at))
        .map(elt => TGraphHashCode(elt.toString, at.millis))(ExecutionContext.parasitic)
    }

  def buildInfo: TQuineInfo = {
    val gitCommit: Option[String] = QuineBuildInfo.gitHeadCommit
      .map(_ + (if (QuineBuildInfo.gitUncommittedChanges) "-DIRTY" else ""))
    TQuineInfo(
      BuildInfo.version,
      gitCommit,
      QuineBuildInfo.gitHeadCommitDate,
      QuineBuildInfo.javaVmName + " " + QuineBuildInfo.javaVersion + " (" + QuineBuildInfo.javaVendor + ")",
      javaRuntimeVersion = Runtime.version().toString,
      javaAvailableProcessors = sys.runtime.availableProcessors(),
      javaMaxMemory = sys.runtime.maxMemory(),
      PersistenceAgent.CurrentVersion.shortString,
      quineType = productVersion.toString,
    )
  }

  def metaData(implicit ec: ExecutionContext): Future[Map[String, String]] =
    graph.namespacePersistor.getAllMetaData().flatMap { m =>
      Future.successful(m.view.mapValues(new String(_)).toMap)
    }

  def metrics(memberIdx: Option[MemberIdx]): Future[V1.MetricsReport] =
    Future.successful(GenerateMetrics.metricsReport(graph))

  def shardSizes(resizes: Map[Int, V1.ShardInMemoryLimit]): Future[Map[Int, V1.ShardInMemoryLimit]] =
    graph
      .shardInMemoryLimits(resizes.fmap(l => InMemoryNodeLimit(l.softLimit, l.hardLimit)))
      .map(_.collect { case (shardIdx, Some(InMemoryNodeLimit(soft, hard))) =>
        shardIdx -> V1.ShardInMemoryLimit(soft, hard)
      })(ExecutionContext.parasitic)

  def requestNodeSleep(quineId: QuineId, namespaceId: NamespaceId): Future[Unit] =
    graph.requiredGraphIsReadyFuture(
      graph.requestNodeSleep(namespaceId, quineId),
    )

  def getSamplesQueries(implicit ctx: ExecutionContext): Future[Vector[SampleQuery]] =
    graph.requiredGraphIsReadyFuture(app.getSampleQueries).map(_.map(UiStylingToApi.apply))
  def getNodeAppearances(implicit ctx: ExecutionContext): Future[Vector[UiNodeAppearance]] =
    graph.requiredGraphIsReadyFuture(app.getNodeAppearances.map(_.map(UiStylingToApi.apply)))
  def getQuickQueries(implicit ctx: ExecutionContext): Future[Vector[UiNodeQuickQuery]] =
    graph.requiredGraphIsReadyFuture(app.getQuickQueries.map(_.map(UiStylingToApi.apply)))

  def analyze(queryText: String, parameters: Seq[String]): QueryEffects = {
    val compiled = cypher.compile(queryText, parameters)
    QueryEffects(
      isReadOnly = compiled.isReadOnly,
      canContainAllNodeScan = compiled.canContainAllNodeScan,
    )
  }

//  def isReadOnly(queryText: String, parameters: Seq[String]): Boolean = analyze(queryText, parameters).isReadOnly //cypher.compile(queryText, parameters).isReadOnly
}
// --------------------- End Admin Endpoints ------------------------

// retained functionality methods from in v1 route definitions
import com.thatdot.quine.app.routes.{AlgorithmMethods => V1AlgorithmMethods}

/** Encapsulates access to the running components of quine for individual endpoints. */
trait QuineApiMethods
    extends ApplicationApiMethods
    with V1AlgorithmMethods
    with CypherApiMethods
    with DebugApiMethods
    with AlgorithmApiMethods {

  override val graph: BaseGraph with LiteralOpsGraph with StandingQueryOpsGraph with CypherOpsGraph with AlgorithmGraph
  override val app: BaseApp
    with StandingQueryStoreV1
    with StandingQueryInterfaceV2
    with IngestStreamState
    with QueryUiConfigurationState
    with SchemaCache

  def thisMemberIdx: Int

  private def mkPauseOperationError(
    operation: String,
  ): PartialFunction[Throwable, Either[BadRequest, Nothing]] = {
    case _: StreamDetachedException =>
      // A StreamDetachedException always occurs when the ingest has failed
      Left(BadRequest.apply(s"Cannot $operation a failed ingest."))
    case e: IngestApiEntities.PauseOperationException =>
      Left(BadRequest.apply(s"Cannot $operation a ${e.statusMsg} ingest."))
  }

  //  endpoint business logic functionality.
  def getProperties: Future[Map[String, String]] = {
    val props: Properties = System.getProperties
    Future.successful(props.keySet.asScala.map(s => s.toString -> props.get(s).toString).toMap[String, String])
  }

  def getNamespaces: Future[List[String]] = Future.apply {
    graph.requiredGraphIsReady()
    app.getNamespaces.map(namespaceToString).toList
  }(ExecutionContext.parasitic)

  def createNamespace(namespace: String): Future[Boolean] =
    app.createNamespace(Some(Symbol(namespace)))

  def deleteNamespace(namespace: String): Future[Boolean] =
    app.deleteNamespace(Some(Symbol(namespace)))

  def listAllStandingQueries: Future[List[ApiStanding.StandingQuery.RegisteredStandingQuery]] = {
    implicit val executor: ExecutionContext = ExecutionContext.parasitic
    Future
      .sequence(app.getNamespaces.map(app.getStandingQueriesV2))
      .map(_.toList.flatten)
  }

  // --------------------- Standing Query Endpoints ------------------------
  def listStandingQueries(namespaceId: NamespaceId): Future[List[ApiStanding.StandingQuery.RegisteredStandingQuery]] =
    graph.requiredGraphIsReadyFuture {
      app.getStandingQueriesV2(namespaceId)
    }

  def propagateStandingQuery(
    includeSleeping: Boolean,
    namespaceId: NamespaceId,
    wakeUpParallelism: Int,
  ): Future[Unit] =
    graph
      .standingQueries(namespaceId)
      .fold(Future.successful[Unit](())) {
        _.propagateStandingQueries(Some(wakeUpParallelism).filter(_ => includeSleeping))
          .map(_ => ())(ExecutionContext.parasitic)
      }

  /** Default timeout for Kafka bootstrap server connectivity checks */
  private val KafkaConnectivityTimeout: FiniteDuration = 5.seconds

  private def validateDestinationSteps(
    destinationSteps: QuineDestinationSteps,
  )(implicit ec: ExecutionContext): Future[Option[NonEmptyList[ErrorString]]] =
    destinationSteps match {
      case k: QuineDestinationSteps.Kafka =>
        KafkaSettingsValidator.validatePropertiesWithConnectivity(
          properties = k.kafkaProperties.view.mapValues(_.toString).toMap,
          bootstrapServers = k.bootstrapServers,
          timeout = KafkaConnectivityTimeout,
        )
      case _ => Future.successful(None)
    }

  private def validateWorkflow(
    workflow: ApiStanding.StandingQueryResultWorkflow,
  )(implicit ec: ExecutionContext): Future[Option[NonEmptyList[ErrorString]]] =
    Future
      .sequence(workflow.destinations.toList.map(validateDestinationSteps))
      .map(_.foldLeft(Option.empty[NonEmptyList[ErrorString]])(_ |+| _))

  /** Validate DLQ Kafka destinations for an ingest configuration.
    * Checks connectivity to bootstrap servers for any Kafka DLQ destinations.
    */
  private def validateDlqDestinations(
    ingestConfig: V2IngestConfiguration,
  )(implicit ec: ExecutionContext): Future[Option[NonEmptyList[ErrorString]]] = {
    val kafkaDestinations = ingestConfig.onRecordError.deadLetterQueueSettings.destinations.collect {
      case k: DeadLetterQueueOutput.Kafka => k
    }
    if (kafkaDestinations.isEmpty) {
      Future.successful(None)
    } else {
      Future
        .sequence(kafkaDestinations.map { k =>
          KafkaSettingsValidator
            .checkBootstrapConnectivity(k.bootstrapServers, KafkaConnectivityTimeout)
            .map(_.map(errors => errors.map(e => s"DLQ Kafka destination: $e")))
        })
        .map(_.foldLeft(Option.empty[NonEmptyList[ErrorString]])(_ |+| _))
    }
  }

  /** Validate Kafka ingest source connectivity.
    * Checks connectivity to bootstrap servers for Kafka ingest sources.
    */
  private def validateIngestSource(
    ingestConfig: V2IngestConfiguration,
  )(implicit ec: ExecutionContext): Future[Option[NonEmptyList[ErrorString]]] =
    ingestConfig.source match {
      case k: KafkaIngest =>
        KafkaSettingsValidator
          .checkBootstrapConnectivity(k.bootstrapServers, KafkaConnectivityTimeout)
          .map(_.map(errors => errors.map(e => s"Kafka ingest source: $e")))
      case _ => Future.successful(None)
    }

  private type ErrSq = BadRequest :+: NotFound :+: CNil
  private def asBadRequest(msg: String): ErrSq = Coproduct[ErrSq](BadRequest(msg))
  private def asBadRequest(msg: ErrorType): ErrSq = Coproduct[ErrSq](BadRequest(msg))
  private def asNotFound(msg: String): ErrSq = Coproduct[ErrSq](NotFound(msg))

  def addSQOutput(
    name: String,
    outputName: String,
    namespaceId: NamespaceId,
    workflow: ApiStanding.StandingQueryResultWorkflow,
  ): Future[Either[ErrSq, Unit]] =
    graph.requiredGraphIsReadyFuture {
      implicit val ec: ExecutionContext = graph.shardDispatcherEC
      validateWorkflow(workflow).flatMap {
        case Some(errors) =>
          Future.successful(Left(asBadRequest(s"Cannot create output `$outputName`: ${errors.toList.mkString(", ")}")))

        case None =>
          app
            .addStandingQueryOutputV2(name, outputName, namespaceId, workflow)
            .map {
              case StandingQueryInterfaceV2.Result.Success =>
                Right(())
              case StandingQueryInterfaceV2.Result.AlreadyExists(name) =>
                Left(asBadRequest(s"There is already a Standing Query output named '$name'"))
              case StandingQueryInterfaceV2.Result.NotFound(queryName) =>
                Left(asBadRequest(s"No Standing Query named '$queryName' can be found."))
            }
      }
    }

  def setSampleQueries(newSampleQueries: Vector[SampleQuery]): Future[Unit] =
    graph.requiredGraphIsReadyFuture(app.setSampleQueries(newSampleQueries.map(ApiToUiStyling.apply)))

  def setQuickQueries(newQuickQueries: Vector[UiNodeQuickQuery]): Future[Unit] =
    graph.requiredGraphIsReadyFuture(app.setQuickQueries(newQuickQueries.map(ApiToUiStyling.apply)))

  def setNodeAppearances(newNodeAppearances: Vector[UiNodeAppearance]): Future[Unit] =
    graph.requiredGraphIsReadyFuture(app.setNodeAppearances(newNodeAppearances.map(ApiToUiStyling.apply)))

  def deleteSQOutput(
    name: String,
    outputName: String,
    namespaceId: NamespaceId,
  ): Future[Either[NotFound, ApiStanding.StandingQueryResultWorkflow]] = graph.requiredGraphIsReadyFuture {
    implicit val exc = ExecutionContext.parasitic
    app
      .removeStandingQueryOutputV2(name, outputName, namespaceId)
      .map(
        _.toRight(NotFound(s"Standing Query, $name, does not exist")),
      )
  }

  def createSQ(
    name: String,
    namespaceId: NamespaceId,
    shouldCalculateResultHashCode: Boolean = false,
    sq: ApiStanding.StandingQuery.StandingQueryDefinition,
  ): Future[Either[ErrSq, ApiStanding.StandingQuery.RegisteredStandingQuery]] = {
    implicit val ctx: ExecutionContext = graph.nodeDispatcherEC
    graph
      .requiredGraphIsReadyFuture {
        try app
          .addStandingQueryV2(name, namespaceId, sq)
          .flatMap {
            case StandingQueryInterfaceV2.Result.AlreadyExists(_) =>
              Future.successful(Left(asBadRequest(s"There is already a Standing Query named '$name'")))
            case StandingQueryInterfaceV2.Result.NotFound(_) =>
              Future.successful(Left(asBadRequest(s"Namespace not found: $namespaceId")))
            case StandingQueryInterfaceV2.Result.Success =>
              app.getStandingQueryV2(name, namespaceId).map {
                case Some(value) => Right(value)
                case None => sys.error("Standing Query not found after adding, this should not happen.")
              }
          } catch {
          case iqp: InvalidQueryPattern => Future.successful(Left(asBadRequest(iqp.message)))
          case cypherException: CypherException =>
            Future.successful(Left(asBadRequest(ErrorType.CypherError(cypherException.pretty))))
        }
      }
      .recoverWith { case _: NamespaceNotFoundException =>
        Future.successful(Left(asNotFound(s"Namespace, $namespaceId, Not Found")))
      }
  }

  def deleteSQ(
    name: String,
    namespaceId: NamespaceId,
  ): Future[Either[NotFound, ApiStanding.StandingQuery.RegisteredStandingQuery]] =
    app
      .cancelStandingQueryV2(name, namespaceId)
      .map(
        _.toRight(NotFound(s"Standing Query, $name, does not exist")),
      )(ExecutionContext.parasitic)

  def getSQ(
    name: String,
    namespaceId: NamespaceId,
  ): Future[Either[NotFound, ApiStanding.StandingQuery.RegisteredStandingQuery]] =
    app
      .getStandingQueryV2(name, namespaceId)
      .map(
        _.toRight(NotFound(s"Standing Query, $name, does not exist")),
      )(ExecutionContext.parasitic)

  // --------------------- Ingest Endpoints ------------------------

  protected type ErrC = ServerError :+: BadRequest :+: CNil
  protected type Warnings = Set[String]

  def createIngestStream[Conf](
    ingestStreamName: String,
    ns: NamespaceId,
    ingestStreamConfig: Conf,
    memberIdx: Option[Int],
  )(implicit
    ec: ExecutionContext,
    configOf: ApiToIngest.OfApiMethod[V2IngestConfiguration, Conf],
  ): Future[Either[ErrC, (ApiIngest.IngestStreamInfoWithName, Warnings)]] = {
    val ingestConfig = configOf(ingestStreamConfig)

    def asBadRequest(errors: Seq[String]): ErrC =
      Coproduct[ErrC](BadRequest.ofErrorStrings(errors.toList))
    def asServerError(msg: String): ErrC =
      Coproduct[ErrC](ServerError(msg))

    val result = for {
      _ <- EitherT(validateIngestSource(ingestConfig).map {
        case Some(errors) => Left(asBadRequest(errors.toList))
        case None => Right(())
      })
      _ <- EitherT(validateDlqDestinations(ingestConfig).map {
        case Some(errors) => Left(asBadRequest(errors.toList))
        case None => Right(())
      })
      warnings <- EitherT(
        app
          .addV2IngestStream(
            name = ingestStreamName,
            settings = ingestConfig,
            intoNamespace = ns,
            timeout = timeout,
            memberIdx = memberIdx.getOrElse(thisMemberIdx),
          )
          .map(_.leftMap(asBadRequest))
          .map(_.map(_ => QuineValueIngestQuery.getQueryWarnings(ingestConfig.query, ingestConfig.parameter))),
      )
      stream <- EitherT.fromOptionF(
        ingestStreamStatus(ingestStreamName, ns, memberIdx),
        asServerError("Ingest was not found after creation"),
      )
    } yield (stream, warnings)

    result.value
  }

  def deleteIngestStream(
    ingestName: String,
    namespaceId: NamespaceId,
    memberIdx: Option[Int],
  ): Future[Option[ApiIngest.IngestStreamInfoWithName]] =
    app
      .removeV2IngestStream(ingestName, namespaceId, memberIdx.getOrElse(thisMemberIdx))
      .map { maybeIngest =>
        maybeIngest.map(IngestToApi.apply)
      }(ExecutionContext.parasitic)

  def pauseIngestStream(
    ingestName: String,
    namespaceId: NamespaceId,
    memberIdx: Option[Int],
  ): Future[Either[BadRequest, Option[ApiIngest.IngestStreamInfoWithName]]] =
    app
      .pauseV2IngestStream(ingestName, namespaceId, memberIdx.getOrElse(thisMemberIdx))
      .map {
        case None => Right(None)
        case Some(ingest) =>
          Right(Some(IngestToApi(ingest)))
      }(ExecutionContext.parasitic)
      .recover(mkPauseOperationError("pause"))(ExecutionContext.parasitic)

  def unpauseIngestStream(
    ingestName: String,
    namespaceId: NamespaceId,
    memberIdx: Option[Int],
  ): Future[Either[BadRequest, Option[ApiIngest.IngestStreamInfoWithName]]] =
    app
      .unpauseV2IngestStream(ingestName, namespaceId, memberIdx.getOrElse(thisMemberIdx))
      .map {
        case None => Right(None)
        case Some(ingest) =>
          Right(Some(IngestToApi(ingest)))
      }(ExecutionContext.parasitic)
      .recover(mkPauseOperationError("resume"))(ExecutionContext.parasitic)

  def ingestStreamStatus(
    ingestName: String,
    namespaceId: NamespaceId,
    memberIdx: Option[Int],
  ): Future[Option[ApiIngest.IngestStreamInfoWithName]] =
    graph.requiredGraphIsReadyFuture {
      app
        .getV2IngestStream(ingestName, namespaceId, memberIdx.getOrElse(thisMemberIdx))
        .map(maybeIngestInfo => maybeIngestInfo.map(IngestToApi.apply))(graph.nodeDispatcherEC)
    }

  def listIngestStreams(
    namespaceId: NamespaceId,
    memberIdx: Option[MemberIdx],
  ): Future[Seq[ApiIngest.IngestStreamInfoWithName]] =
    graph.requiredGraphIsReadyFuture {
      app
        .getV2IngestStreams(namespaceId, memberIdx.getOrElse(thisMemberIdx))
        .map(_.map { case (name, ingest) =>
          IngestToApi.apply(ingest.withName(name))
        }.toSeq)(ExecutionContext.parasitic)
    }

}
