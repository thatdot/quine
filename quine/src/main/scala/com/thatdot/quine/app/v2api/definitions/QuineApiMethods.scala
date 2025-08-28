package com.thatdot.quine.app.v2api.definitions

import java.nio.file.{FileAlreadyExistsException, FileSystemException, InvalidPathException}
import java.util.Properties

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters.CollectionHasAsScala
import scala.util.control.NonFatal
import scala.util.{Either, Failure, Success, Try}

import org.apache.pekko.Done
import org.apache.pekko.stream.scaladsl.Sink
import org.apache.pekko.stream.{Materializer, StreamDetachedException}
import org.apache.pekko.util.Timeout

import cats.data.NonEmptyList
import cats.implicits.toFunctorOps
import io.circe.Json
import shapeless.{:+:, CNil, Coproduct}

import com.thatdot.api.v2.outputs.DestinationSteps
import com.thatdot.common.logging.Log._
import com.thatdot.common.quineid.QuineId
import com.thatdot.quine.app.config.BaseConfig
import com.thatdot.quine.app.model.ingest.util.KafkaSettingsValidator
import com.thatdot.quine.app.model.ingest.util.KafkaSettingsValidator.ErrorString
import com.thatdot.quine.app.model.ingest2.V2IngestEntities
import com.thatdot.quine.app.model.ingest2.V2IngestEntities.{QuineIngestConfiguration => V2IngestConfiguration}
import com.thatdot.quine.app.model.ingest2.source.QuineValueIngestQuery
import com.thatdot.quine.app.routes._
import com.thatdot.quine.app.util.QuineLoggables._
import com.thatdot.quine.app.v2api.converters._
import com.thatdot.quine.app.v2api.definitions.ApiUiStyling.{SampleQuery, UiNodeAppearance, UiNodeQuickQuery}
import com.thatdot.quine.app.v2api.definitions.ErrorResponse.{BadRequest, NotFound, ServerError}
import com.thatdot.quine.app.v2api.definitions.ErrorResponseHelpers.toServerError
import com.thatdot.quine.app.v2api.definitions.ingest2.ApiIngest
import com.thatdot.quine.app.v2api.definitions.query.standing.QuineSupportedDestinationSteps.{
  CoreDestinationSteps,
  QuineAdditionalDestinationSteps,
}
import com.thatdot.quine.app.v2api.definitions.query.{standing => ApiStanding}
import com.thatdot.quine.app.v2api.endpoints.V2AdministrationEndpointEntities.{TGraphHashCode, TQuineInfo}
import com.thatdot.quine.app.v2api.endpoints.V2AlgorithmEndpointEntities.TSaveLocation
import com.thatdot.quine.app.v2api.endpoints.V2CypherEndpointEntities.{
  TCypherQuery,
  TCypherQueryResult,
  TUiEdge,
  TUiNode,
}
import com.thatdot.quine.app.v2api.endpoints.V2DebugEndpointEntities.{TEdgeDirection, TLiteralNode, TRestHalfEdge}
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
  NamespaceId,
  StandingQueryOpsGraph,
  namespaceToString,
}
import com.thatdot.quine.model.{HalfEdge, Milliseconds, QuineValue}
import com.thatdot.quine.persistor.PersistenceAgent
import com.thatdot.quine.util.SwitchMode
import com.thatdot.quine.{BuildInfo => QuineBuildInfo, model, routes => V1}

sealed trait ProductVersion
object ProductVersion {
  case object Novelty extends ProductVersion

  case object Oss extends ProductVersion

  case object Enterprise extends ProductVersion
}

trait ApplicationApiMethods {
  val graph: BaseGraph with LiteralOpsGraph with CypherOpsGraph with StandingQueryOpsGraph
  val app: BaseApp with SchemaCache
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
        .map(TGraphHashCode(_, at.millis))(ExecutionContext.parasitic)
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

  def metrics: V1.MetricsReport = GenerateMetrics.metricsReport(graph)

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

}
// --------------------- End Admin Endpoints ------------------------

// retained functionality methods from in v1 route definitions
import com.thatdot.quine.app.routes.{AlgorithmMethods => V1AlgorithmMethods}

/** Encapsulates access to the running components of quine for individual endpoints. */
trait QuineApiMethods extends ApplicationApiMethods with V1AlgorithmMethods {

  override val graph: BaseGraph with LiteralOpsGraph with StandingQueryOpsGraph with CypherOpsGraph with AlgorithmGraph
  override val app: BaseApp
    with StandingQueryStoreV1
    with StandingQueryInterfaceV2
    with IngestStreamState
    with QueryUiConfigurationState
    with SchemaCache

  def thisMemberIdx: Int

  // duplicated from, com.thatdot.quine.app.routes.IngestApiMethods
  private def stream2Info(
    conf: IngestStreamWithControl[V2IngestEntities.IngestSource],
  ): Future[ApiIngest.IngestStreamInfo] =
    conf.status.map { status =>
      ApiIngest.IngestStreamInfo(
        IngestToApi(status),
        conf.terminated().value collect { case Failure(exception) => exception.toString },
        IngestToApi(conf.settings),
        IngestToApi(conf.metrics.toEndpointResponse),
      )
    }(graph.shardDispatcherEC)

  private def setIngestStreamPauseState(
    name: String,
    namespace: NamespaceId,
    newState: SwitchMode,
  )(implicit logConfig: LogConfig): Future[Option[ApiIngest.IngestStreamInfoWithName]] =
    app.getIngestStreamFromState(name, namespace) match {
      case None => Future.successful(None)
      case Some(ingest: IngestStreamWithControl[UnifiedIngestConfiguration]) =>
        ingest.initialStatus match {
          case V1.IngestStreamStatus.Completed =>
            Future.failed(IngestApiEntities.PauseOperationException.Completed)
          case V1.IngestStreamStatus.Terminated =>
            Future.failed(IngestApiEntities.PauseOperationException.Terminated)
          case V1.IngestStreamStatus.Failed =>
            Future.failed(IngestApiEntities.PauseOperationException.Failed)
          case _ =>
            val flippedValve = ingest.valve().flatMap(_.flip(newState))(graph.nodeDispatcherEC)
            val ingestStatus = flippedValve.flatMap { _ =>
              // HACK: set the ingest's "initial status" to "Paused". `stream2Info` will use this as the stream status
              // when the valve is closed but the stream is not terminated. However, this assignment is not threadsafe,
              // and this directly violates the semantics of `initialStatus`. This should be fixed in a future refactor.
              ingest.initialStatus = V1.IngestStreamStatus.Paused
              stream2Info(ingest.copy(settings = V2IngestEntities.IngestSource(ingest.settings)))
            }(graph.nodeDispatcherEC)
            ingestStatus.map(status => Some(status.withName(name)))(ExecutionContext.parasitic)
        }
    }

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

  private def validateCoreDestinationSteps(
    destinationSteps: CoreDestinationSteps,
  ): Option[NonEmptyList[ErrorString]] =
    destinationSteps.steps match {
      case k: DestinationSteps.Kafka =>
        KafkaSettingsValidator.validateProperties(k.kafkaProperties.view.mapValues(_.toString).toMap)
      case _ => None
    }

  private def validateQuineDestinationSteps(
    destinationSteps: QuineAdditionalDestinationSteps,
  ): Option[NonEmptyList[ErrorString]] = None

  private def validateWorkflow(workflow: ApiStanding.StandingQueryResultWorkflow): Option[NonEmptyList[ErrorString]] = {
    import cats.implicits.catsSyntaxSemigroup
    workflow.destinations
      .collect {
        case steps: CoreDestinationSteps => validateCoreDestinationSteps(steps)
        case steps: QuineAdditionalDestinationSteps => validateQuineDestinationSteps(steps)
      }
      .reduce(_.combine(_))
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
      validateWorkflow(workflow) match {
        case Some(errors) =>
          Future.successful(Left(asBadRequest(s"Cannot create output `$outputName`: ${errors.toList.mkString(", ")}")))

        case _ =>
          app
            .addStandingQueryOutputV2(name, outputName, namespaceId, workflow)
            .map {
              case StandingQueryInterfaceV2.Result.Success =>
                Right(())
              case StandingQueryInterfaceV2.Result.AlreadyExists(name) =>
                Left(asBadRequest(s"There is already a Standing Query output named '$name'"))
              case StandingQueryInterfaceV2.Result.NotFound(queryName) =>
                Left(asBadRequest(s"No Standing Query named '$queryName' can be found."))
            }(graph.shardDispatcherEC)

      }
    }

  def getSamplesQueries(implicit ctx: ExecutionContext): Future[Vector[SampleQuery]] =
    graph.requiredGraphIsReadyFuture(app.getSampleQueries).map(_.map(UiStylingToApi.apply))

  def setSampleQueries(newSampleQueries: Vector[SampleQuery]): Future[Unit] =
    graph.requiredGraphIsReadyFuture(app.setSampleQueries(newSampleQueries.map(ApiToUiStyling.apply)))

  def getQuickQueries(implicit ctx: ExecutionContext): Future[Vector[UiNodeQuickQuery]] =
    graph.requiredGraphIsReadyFuture(app.getQuickQueries.map(_.map(UiStylingToApi.apply)))

  def getNodeAppearances(implicit ctx: ExecutionContext): Future[Vector[UiNodeAppearance]] =
    graph.requiredGraphIsReadyFuture(app.getNodeAppearances.map(_.map(UiStylingToApi.apply)))

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

  // --------------------- Cypher Endpoints ------------------------

  // The Query UI relies heavily on a couple Cypher endpoints for making queries.
  private def catchCypherException[A](futA: => Future[A]): Future[Either[BadRequest, A]] =
    Future
      .fromTry(Try(futA))
      .flatten
      .transform {
        case Success(a) => Success(Right(a))
        case Failure(qce: CypherException) => Success(Left(BadRequest(ErrorType.CypherError(qce.pretty))))
        case Failure(err) => Failure(err)
      }(ExecutionContext.parasitic)

  //TODO On missing namespace
  //TODO timeout handling
  val cypherMethods = new OSSQueryUiCypherMethods(graph)

  def cypherPost(
    atTime: Option[Milliseconds],
    timeout: FiniteDuration,
    namespaceId: NamespaceId,
    query: TCypherQuery,
  ): Future[Either[BadRequest, TCypherQueryResult]] =
    graph.requiredGraphIsReadyFuture {
      catchCypherException {
        val (columns, results, isReadOnly, _) =
          cypherMethods.queryCypherGeneric(
            V1.CypherQuery(query.text, query.parameters),
            namespaceId,
            atTime,
          ) // TODO read canContainAllNodeScan
        results
          .via(Util.completionTimeoutOpt(timeout, allowTimeout = isReadOnly))
          .named(s"cypher-query-atTime-${atTime.fold("none")(_.millis.toString)}")
          .runWith(Sink.seq)(graph.materializer)
          .map(TCypherQueryResult(columns, _))(ExecutionContext.parasitic)
      }
    }

  //private def ifNamespaceFound(namespaceId: NamespaceId) = ???

  def cypherNodesPost(
    atTime: Option[Milliseconds],
    timeout: FiniteDuration,
    namespaceId: NamespaceId,
    query: TCypherQuery,
  ): Future[Either[BadRequest, Seq[TUiNode]]] =
    graph.requiredGraphIsReadyFuture {
      catchCypherException {
        val (results, isReadOnly, _) =
          cypherMethods.queryCypherNodes(
            V1.CypherQuery(query.text, query.parameters),
            namespaceId,
            atTime,
          ) // TODO read canContainAllNodeScan
        results
          .via(Util.completionTimeoutOpt(timeout, allowTimeout = isReadOnly))
          .named(s"cypher-nodes-query-atTime-${atTime.fold("none")(_.millis.toString)}")
          .map(node => TUiNode(node.id, node.hostIndex, node.label, node.properties))
          .runWith(Sink.seq)(graph.materializer)
      }
    }

  def cypherEdgesPost(
    atTime: Option[Milliseconds],
    timeout: FiniteDuration,
    namespaceId: NamespaceId,
    query: TCypherQuery,
  ): Future[Either[BadRequest, Seq[TUiEdge]]] =
    graph.requiredGraphIsReadyFuture {
      catchCypherException {
        val (results, isReadOnly, _) =
          cypherMethods.queryCypherEdges(
            V1.CypherQuery(query.text, query.parameters),
            namespaceId,
            atTime,
          ) // TODO read canContainAllNodeScan
        results
          .via(Util.completionTimeoutOpt(timeout, allowTimeout = isReadOnly))
          .named(s"cypher-edges-query-atTime-${atTime.fold("none")(_.millis.toString)}")
          .map(edge => TUiEdge(edge.from, edge.edgeType, edge.to, edge.isDirected))
          .runWith(Sink.seq)(graph.materializer)
      }
    }

  // --------------------- Algorithm Endpoints ------------------------

  /** Note: Duplicate implementation of [[AlgorithmRoutesImpl.algorithmSaveRandomWalksRoute]] */
  def algorithmSaveRandomWalks(
    lengthOpt: Option[Int],
    countOpt: Option[Int],
    queryOpt: Option[String],
    returnParamOpt: Option[Double],
    inOutParamOpt: Option[Double],
    seedOpt: Option[String],
    namespaceId: NamespaceId,
    atTime: Option[Milliseconds],
    parallelism: Int,
    saveLocation: TSaveLocation,
  ): Either[ServerError :+: BadRequest :+: CNil, Option[String]] = {

    graph.requiredGraphIsReady()
    if (!graph.getNamespaces.contains(namespaceId)) Right(None)
    else {
      val defaultFileName =
        generateDefaultFileName(atTime, lengthOpt, countOpt, queryOpt, returnParamOpt, inOutParamOpt, seedOpt)
      val fileName = saveLocation.fileName(defaultFileName)
      Try {
        require(!lengthOpt.exists(_ < 1), "walk length cannot be less than one.")
        require(!countOpt.exists(_ < 0), "walk count cannot be less than zero.")
        require(!inOutParamOpt.exists(_ < 0d), "in-out parameter cannot be less than zero.")
        require(!returnParamOpt.exists(_ < 0d), "return parameter cannot be less than zero.")
        require(parallelism >= 1, "parallelism cannot be less than one.")
        val saveSink = saveLocation.toSink(fileName)
        saveSink -> compileWalkQuery(queryOpt)
      }.map { case (sink, compiledQuery) =>
        graph.algorithms
          .saveRandomWalks(
            sink,
            compiledQuery,
            lengthOpt.getOrElse(AlgorithmGraph.defaults.walkLength),
            countOpt.getOrElse(AlgorithmGraph.defaults.walkCount),
            returnParamOpt.getOrElse(AlgorithmGraph.defaults.returnParam),
            inOutParamOpt.getOrElse(AlgorithmGraph.defaults.inOutParam),
            seedOpt,
            namespaceId,
            atTime,
            parallelism,
          )
        Some(fileName)
      }.toEither
        .left
        .map {
          case _: InvalidPathException | _: FileAlreadyExistsException | _: SecurityException |
              _: FileSystemException =>
            Coproduct[ServerError :+: BadRequest :+: CNil](
              BadRequest(s"Invalid file name: $fileName"),
            ) // Return a Bad Request Error
          case e: CypherException =>
            Coproduct[ServerError :+: BadRequest :+: CNil](
              BadRequest(ErrorType.CypherError(s"Invalid query: ${e.getMessage}")),
            )
          case e: IllegalArgumentException =>
            Coproduct[ServerError :+: BadRequest :+: CNil](BadRequest(e.getMessage))
          case NonFatal(e) =>
            Coproduct[ServerError :+: BadRequest :+: CNil](
              toServerError(e),
            ) // Return an Internal Server Error
          case other =>
            Coproduct[ServerError :+: BadRequest :+: CNil](
              toServerError(other),
            ) // This might expose more than we want
        }
    }
  }

  /** Note: Duplicate implementation of [[AlgorithmRoutesImpl.algorithmRandomWalkRoute]] */
  def algorithmRandomWalk(
    qid: QuineId,
    lengthOpt: Option[Int],
    queryOpt: Option[String],
    returnParamOpt: Option[Double],
    inOutParamOpt: Option[Double],
    seedOpt: Option[String],
    namespaceId: NamespaceId,
    atTime: Option[Milliseconds],
  ): Future[Either[ServerError :+: BadRequest :+: CNil, List[String]]] = {

    val errors: Either[ServerError :+: BadRequest :+: CNil, List[String]] = Try {
      require(!lengthOpt.exists(_ < 1), "walk length cannot be less than one.")
      require(!inOutParamOpt.exists(_ < 0d), "in-out parameter cannot be less than zero.")
      require(!returnParamOpt.exists(_ < 0d), "return parameter cannot be less than zero.")
      Nil
    }.toEither.left
      .map {
        case e: CypherException =>
          Coproduct[ServerError :+: BadRequest :+: CNil](BadRequest(s"Invalid query: ${e.getMessage}"))
        case e: IllegalArgumentException =>
          Coproduct[ServerError :+: BadRequest :+: CNil](BadRequest(e.getMessage))
        case NonFatal(e) =>
          Coproduct[ServerError :+: BadRequest :+: CNil](
            toServerError(e),
          ) // Return an Internal Server Error
        case other =>
          Coproduct[ServerError :+: BadRequest :+: CNil](
            toServerError(other),
          ) // this might expose more than we want
      }
    if (errors.isLeft)
      Future.successful[Either[ServerError :+: BadRequest :+: CNil, List[String]]](errors)
    else {

      graph.requiredGraphIsReady()

      graph.algorithms
        .randomWalk(
          qid,
          compileWalkQuery(queryOpt),
          lengthOpt.getOrElse(AlgorithmGraph.defaults.walkLength),
          returnParamOpt.getOrElse(AlgorithmGraph.defaults.returnParam),
          inOutParamOpt.getOrElse(AlgorithmGraph.defaults.inOutParam),
          None,
          seedOpt,
          namespaceId,
          atTime,
        )
        .map(w => Right(w.acc))(ExecutionContext.parasitic)

    }
  }

  // --------------------- Debug Endpoints ------------------------

  private def toApiEdgeDirection(dir: model.EdgeDirection): TEdgeDirection = dir match {
    case model.EdgeDirection.Outgoing => TEdgeDirection.Outgoing
    case model.EdgeDirection.Incoming => TEdgeDirection.Incoming
    case model.EdgeDirection.Undirected => TEdgeDirection.Undirected
  }

  private def toModelEdgeDirection(dir: TEdgeDirection): model.EdgeDirection = dir match {
    case TEdgeDirection.Outgoing => model.EdgeDirection.Outgoing
    case TEdgeDirection.Incoming => model.EdgeDirection.Incoming
    case TEdgeDirection.Undirected => model.EdgeDirection.Undirected
  }

  def debugOpsPropertyGet(
    qid: QuineId,
    propKey: String,
    atTime: Option[Milliseconds],
    namespaceId: NamespaceId,
  ): Future[Option[Json]] =
    graph.requiredGraphIsReadyFuture {
      graph
        .literalOps(namespaceId)
        .getProps(qid, atTime)
        .map(m =>
          m.get(Symbol(propKey))
            .map(_.deserialized.get)
            .map(qv => QuineValue.toJson(qv)(graph.idProvider, logConfig)),
        )(
          graph.nodeDispatcherEC,
        )
    }

  def debugOpsGet(qid: QuineId, atTime: Option[Milliseconds], namespaceId: NamespaceId): Future[TLiteralNode[QuineId]] =
    graph.requiredGraphIsReadyFuture {
      val propsF = graph.literalOps(namespaceId).getProps(qid, atTime = atTime)
      val edgesF = graph.literalOps(namespaceId).getEdges(qid, atTime = atTime)
      propsF
        .zip(edgesF)
        .map { case (props, edges) =>
          TLiteralNode(
            props.map { case (k, v) =>
              k.name -> QuineValue.toJson(v.deserialized.get)(graph.idProvider, logConfig)
            },
            edges.toSeq.map { case HalfEdge(t, d, o) => TRestHalfEdge(t.name, toApiEdgeDirection(d), o) },
          )
        }(graph.nodeDispatcherEC)
    }

  def debugOpsVerbose(qid: QuineId, atTime: Option[Milliseconds], namespaceId: NamespaceId): Future[String] =
    graph.requiredGraphIsReadyFuture {
      graph
        .literalOps(namespaceId)
        .logState(qid, atTime)
        //TODO: ToString -> see DebugOpsRoutes.nodeInternalStateSchema
        .map(_.toString)(graph.nodeDispatcherEC)
    }

  def debugOpsEdgesGet(
    qid: QuineId,
    atTime: Option[Milliseconds],
    limit: Option[Int],
    edgeDirOpt: Option[TEdgeDirection],
    otherOpt: Option[QuineId],
    edgeTypeOpt: Option[String],
    namespaceId: NamespaceId,
  ): Future[Vector[TRestHalfEdge[QuineId]]] =
    graph.requiredGraphIsReadyFuture {
      val edgeDirOpt2 = edgeDirOpt.map(toModelEdgeDirection)
      graph
        .literalOps(namespaceId)
        .getEdges(qid, edgeTypeOpt.map(Symbol.apply), edgeDirOpt2, otherOpt, limit, atTime)
        .map(_.toVector.map { case HalfEdge(t, d, o) => TRestHalfEdge(t.name, toApiEdgeDirection(d), o) })(
          graph.nodeDispatcherEC,
        )

    }

  def debugOpsHalfEdgesGet(
    qid: QuineId,
    atTime: Option[Milliseconds],
    limit: Option[Int],
    edgeDirOpt: Option[TEdgeDirection],
    otherOpt: Option[QuineId],
    edgeTypeOpt: Option[String],
    namespaceId: NamespaceId,
  ): Future[Vector[TRestHalfEdge[QuineId]]] =
    graph.requiredGraphIsReadyFuture {
      val edgeDirOpt2 = edgeDirOpt.map(toModelEdgeDirection)
      graph
        .literalOps(namespaceId)
        .getHalfEdges(qid, edgeTypeOpt.map(Symbol.apply), edgeDirOpt2, otherOpt, limit, atTime)
        .map(_.toVector.map { case HalfEdge(t, d, o) => TRestHalfEdge(t.name, toApiEdgeDirection(d), o) })(
          graph.nodeDispatcherEC,
        )
    }

  // --------------------- Ingest Endpoints ------------------------

  private type ErrC = ServerError :+: BadRequest :+: CNil
  private type Warnings = Set[String]

  /** Wraps Create ingest in logic to check in ingest stream already exists and return stream info */
  def handleCreateIngest[Conf](
    ingestStreamName: String,
    ns: NamespaceId,
    ingestStreamConfig: Conf,
  )(implicit
    ec: ExecutionContext,
    configOf: ApiToIngest.OfApiMethod[V2IngestConfiguration, Conf],
  ): Future[Either[ErrC, (ApiIngest.IngestStreamInfoWithName, Warnings)]] = {

    def asBadRequest(msg: String): ErrC = Coproduct[ErrC](BadRequest(msg))
    def asServerError(msg: String): ErrC = Coproduct[ErrC](ServerError(msg))

    for {
      existingIngests <- listIngestStreams(ns)

      // Create the stream
      eitherCreated = {
        if (existingIngests.contains(ingestStreamName))
          Left(asBadRequest(s"Ingest Stream, $ingestStreamName, already exists"))
        else
          createIngestStream(ingestStreamName, ingestStreamConfig, ns).left
            .map(e => Coproduct[ErrC](e))
      }

      // Current stream info
      optStream <- ingestStreamStatus(ingestStreamName, ns)

      // Collect stream info and warnings.  Fail if stream cannot be found at this point.
      result = eitherCreated.flatMap(warnings =>
        optStream match {
          case Some(stream) =>
            Right((stream, warnings)): Either[ErrC, (ApiIngest.IngestStreamInfoWithName, Set[String])]
          case None =>
            Left(asServerError("Ingest was not found after creation"))
        },
      )

    } yield result

  }

  def createIngestStream[Conf](
    ingestName: String,
    settings: Conf,
    namespaceId: NamespaceId,
  )(implicit configOf: ApiToIngest.OfApiMethod[V2IngestConfiguration, Conf]): Either[BadRequest, Set[String]] =
    try app
      .addV2IngestStream(
        ingestName,
        configOf(settings),
        namespaceId,
        None, // this ingest is being created, not restored, so it has no previous status
        shouldResumeRestoredIngests = true,
        timeout,
        shouldSaveMetadata = true,
        Some(thisMemberIdx),
      )
      .toEither
      .map(_ => QuineValueIngestQuery.getQueryWarnings(configOf(settings).query, configOf(settings).parameter))
      .left
      .map(s => BadRequest.ofErrors(s.toList))
    catch {
      case e: CypherException => Left(BadRequest(ErrorType.CypherError(e.getMessage)))
    }

  def deleteIngestStream(
    ingestName: String,
    namespaceId: NamespaceId,
  ): Future[Option[ApiIngest.IngestStreamInfoWithName]] =
    graph.requiredGraphIsReadyFuture {

      app.removeIngestStream(ingestName, namespaceId) match {
        case None => Future.successful(None)
        case Some(control @ IngestStreamWithControl(settings, metrics, _, terminated, close, _, _)) =>
          val finalStatus = control.status.map { previousStatus =>
            import com.thatdot.quine.routes.IngestStreamStatus._
            previousStatus match {
              // in these cases, the ingest was healthy and runnable/running
              case Running | Paused | Restored => Terminated
              // in these cases, the ingest was not running/runnable
              case Completed | Failed | Terminated => previousStatus
            }
          }(ExecutionContext.parasitic)

          val terminationMessage: Future[Option[String]] = {
            // start terminating the ingest
            close()
            // future will return when termination finishes
            terminated()
              .flatMap(t =>
                t
                  .map({ case Done => None })(graph.shardDispatcherEC)
                  .recover({ case e =>
                    Some(e.toString)
                  })(graph.shardDispatcherEC),
              )(graph.shardDispatcherEC)
          }

          finalStatus
            .zip(terminationMessage)
            .map { case (newStatus, message) =>
              Some(
                ApiIngest.IngestStreamInfoWithName(
                  ingestName,
                  IngestToApi(newStatus),
                  message,
                  IngestToApi(V2IngestEntities.IngestSource(settings)),
                  IngestToApi(metrics.toEndpointResponse),
                ),
              )
            }(graph.shardDispatcherEC)
      }
    }

  def pauseIngestStream(
    ingestName: String,
    namespaceId: NamespaceId,
  ): Future[Either[BadRequest, Option[ApiIngest.IngestStreamInfoWithName]]] =
    graph.requiredGraphIsReadyFuture {
      setIngestStreamPauseState(ingestName, namespaceId, SwitchMode.Close)
        .map(Right(_))(ExecutionContext.parasitic)
        .recover(mkPauseOperationError("pause"))(ExecutionContext.parasitic)
    }

  def unpauseIngestStream(
    ingestName: String,
    namespaceId: NamespaceId,
  ): Future[Either[BadRequest, Option[ApiIngest.IngestStreamInfoWithName]]] =
    graph.requiredGraphIsReadyFuture {
      setIngestStreamPauseState(ingestName, namespaceId, SwitchMode.Open)
        .map(Right(_))(ExecutionContext.parasitic)
        .recover(mkPauseOperationError("resume"))(ExecutionContext.parasitic)
    }

  def ingestStreamStatus(
    ingestName: String,
    namespaceId: NamespaceId,
  ): Future[Option[ApiIngest.IngestStreamInfoWithName]] =
    graph.requiredGraphIsReadyFuture {
      app.getV2IngestStream(ingestName, namespaceId) match {
        case None => Future.successful(None)
        case Some(stream) => stream2Info(stream).map(s => Some(s.withName(ingestName)))(ExecutionContext.parasitic)
      }
    }

  def listIngestStreams(namespaceId: NamespaceId): Future[Map[String, ApiIngest.IngestStreamInfo]] =
    graph.requiredGraphIsReadyFuture {
      Future
        .traverse(
          app.getV2IngestStreams(namespaceId).toList,
        ) { case (name, ingest) =>
          stream2Info(ingest).map(name -> _)(graph.shardDispatcherEC)
        }(implicitly, graph.shardDispatcherEC)
        .map(_.toMap)(graph.shardDispatcherEC)
    }

  def isReadOnly(queryText: String): Boolean = cypher.compile(queryText).isReadOnly
}
