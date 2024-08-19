package com.thatdot.quine.app.v2api.definitions

import java.nio.file.{FileAlreadyExistsException, FileSystemException, InvalidPathException}
import java.util.Properties

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters.CollectionHasAsScala
import scala.util.control.NonFatal
import scala.util.{Either, Failure, Success, Try}

import org.apache.pekko.Done
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.Sink
import org.apache.pekko.util.Timeout

import cats.data.NonEmptyList
import cats.implicits.toFunctorOps
import io.circe.Json

import com.thatdot.quine.app.config.BaseConfig
import com.thatdot.quine.app.ingest.util.KafkaSettingsValidator
import com.thatdot.quine.app.ingest.util.KafkaSettingsValidator.ErrorString
import com.thatdot.quine.app.routes._
import com.thatdot.quine.app.v2api.endpoints.V2AdministrationEndpointEntities.{TGraphHashCode, TQuineInfo}
import com.thatdot.quine.app.v2api.endpoints.V2AlgorithmEndpointEntities.TSaveLocation
import com.thatdot.quine.app.v2api.endpoints.V2DebugEndpointEntities.{TEdgeDirection, TLiteralNode, TRestHalfEdge}
import com.thatdot.quine.app.{BaseApp, BuildInfo, NamespaceNotFoundException}
import com.thatdot.quine.graph.EventTime.logConfig
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
  namespaceToString
}
import com.thatdot.quine.model.{HalfEdge, Milliseconds, QuineId, QuineValue}
import com.thatdot.quine.persistor.PersistenceAgent
import com.thatdot.quine.routes.StandingQueryResultOutputUserDef.WriteToKafka
import com.thatdot.quine.routes._
import com.thatdot.quine.util.SwitchMode
import com.thatdot.quine.{BuildInfo => QuineBuildInfo, model}

/** Access to application components for api endpoints */
trait ApplicationApiInterface extends AlgorithmMethods with IngestApiMethods {
  val graph: BaseGraph with LiteralOpsGraph with StandingQueryOpsGraph with CypherOpsGraph with AlgorithmGraph

  implicit def timeout: Timeout

  implicit def materializer: Materializer = graph.materializer

  val quineApp: BaseApp with StandingQueryStore with IngestStreamState

  val config: BaseConfig

  def thisMemberIdx: Int

  //  endpoint business logic functionality.
  def getProperties: Future[Map[String, String]] = {
    val props: Properties = System.getProperties
    Future.successful(props.keySet.asScala.map(s => s.toString -> props.get(s).toString).toMap[String, String])
  }

  def getNamespaces: Future[List[String]] = Future.apply {
    graph.requiredGraphIsReady()
    quineApp.getNamespaces.map(namespaceToString).toList
  }(ExecutionContext.parasitic)

  def createNamespace(namespace: String): Future[Boolean] =
    quineApp.createNamespace(Some(Symbol(namespace)))

  def deleteNamespace(namespace: String): Future[Boolean] =
    quineApp.deleteNamespace(Some(Symbol(namespace)))

  def listAllStandingQueries: Future[List[RegisteredStandingQuery]] = {
    implicit val executor = ExecutionContext.parasitic
    Future.sequence(quineApp.getNamespaces.map(quineApp.getStandingQueries)).map(_.toList.flatten)
  }

  // --------------------- Admin Endpoints ------------------------
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
      PersistenceAgent.CurrentVersion.shortString
    )
  }

  def emptyConfigExample: BaseConfig

  def isReady = graph.isReady

  def isLive = true

  def performShutdown(): Future[Unit] = graph.system.terminate().map(_ => ())(ExecutionContext.parasitic)

  def metaData: Future[Map[String, String]] = {
    implicit val ec = ExecutionContext.parasitic
    graph.namespacePersistor.getAllMetaData().flatMap { m =>
      Future.successful(m.view.mapValues(new String(_)).toMap)
    }
  }

  def metrics: MetricsReport = GenerateMetrics.metricsReport(graph)

  def shardSizes(resizes: Map[Int, ShardInMemoryLimit]): Future[Map[Int, ShardInMemoryLimit]] =
    graph
      .shardInMemoryLimits(resizes.fmap(l => InMemoryNodeLimit(l.softLimit, l.hardLimit)))
      .map(_.collect { case (shardIdx, Some(InMemoryNodeLimit(soft, hard))) =>
        shardIdx -> ShardInMemoryLimit(soft, hard)
      })(ExecutionContext.parasitic)

  def requestNodeSleep(quineId: QuineId, namespaceId: NamespaceId): Future[Unit] =
    graph.requiredGraphIsReadyFuture(
      graph.requestNodeSleep(namespaceId, quineId)
    )

  // --------------------- Standing Query Endpoints ------------------------

  def listStandingQueries(namespaceId: NamespaceId): Future[List[RegisteredStandingQuery]] =
    graph.requiredGraphIsReadyFuture {
      quineApp.getStandingQueries(namespaceId)
    }

  def propagateStandingQuery(
    includeSleeping: Option[Boolean],
    namespaceId: NamespaceId,
    wakeUpParallelism: Int
  ): Future[Unit] =
    graph
      .standingQueries(namespaceId)
      .fold(Future.successful[Unit](())) {
        _.propagateStandingQueries(Some(wakeUpParallelism).filter(_ => includeSleeping.getOrElse(false)))
          .map(_ => ())(ExecutionContext.parasitic)
      }

  private def validateOutputDef(outputDef: StandingQueryResultOutputUserDef): Option[NonEmptyList[ErrorString]] =
    outputDef match {
      case k: WriteToKafka => KafkaSettingsValidator.validateOutput(k.kafkaProperties)
      case _ => None
    }

  def addSQOutput(
    name: String,
    outputName: String,
    namespaceId: NamespaceId,
    sqResultOutput: StandingQueryResultOutputUserDef
  ): Future[Either[CustomError, Unit]] = graph.requiredGraphIsReadyFuture {
    validateOutputDef(sqResultOutput) match {
      case Some(errors) =>
        Future.successful(Left(BadRequest(s"Cannot create output `$outputName`: ${errors.toList.mkString(",")}")))

      case _ =>
        quineApp
          .addStandingQueryOutput(name, outputName, namespaceId, sqResultOutput)
          .map {
            case Some(false) => Left(BadRequest(s"There is already a standing query output named '$outputName'"))
            case _ => Right(())
          }(graph.shardDispatcherEC)
    }
  }

  def deleteSQOutput(
    name: String,
    outputName: String,
    namespaceId: NamespaceId
  ): Future[Option[StandingQueryResultOutputUserDef]] = graph.requiredGraphIsReadyFuture {
    quineApp.removeStandingQueryOutput(name, outputName, namespaceId)
  }

  def createSQ(
    name: String,
    namespaceId: NamespaceId,
    sq: StandingQueryDefinition
  ): Future[Either[CustomError, Option[Unit]]] =
    graph.requiredGraphIsReadyFuture {
      try quineApp
        .addStandingQuery(name, namespaceId, sq)
        .map {
          case false => Left(BadRequest(s"There is already a standing query named '$name'"))
          case true => Right(Some(()))
        }(graph.nodeDispatcherEC)
        .recoverWith { case _: NamespaceNotFoundException =>
          Future.successful(Right(None))
        }(graph.nodeDispatcherEC)
      catch {
        case iqp: InvalidQueryPattern => Future.successful(Left(BadRequest(iqp.message)))
        case cypherException: CypherException => Future.successful(Left(BadRequest(cypherException.pretty)))
      }
    }

  def deleteSQ(name: String, namespaceId: NamespaceId): Future[Option[RegisteredStandingQuery]] =
    quineApp.cancelStandingQuery(name, namespaceId)

  def getSQ(name: String, namespaceId: NamespaceId): Future[Option[RegisteredStandingQuery]] =
    quineApp.getStandingQuery(name, namespaceId)

  // --------------------- Cypher Endpoints ------------------------

  // The Query UI relies heavily on a couple Cypher endpoints for making queries.
  private def catchCypherException[A](futA: => Future[A]): Future[Either[CustomError, A]] =
    Future
      .fromTry(Try(futA))
      .flatten
      .transform {
        case Success(a) => Success(Right(a))
        case Failure(qce: CypherException) => Success(Left(BadRequest(qce.pretty)))
        case Failure(err) => Failure(err)
      }(ExecutionContext.parasitic)

  //TODO On missing namespace
  //TODO timeout handling
  val cypherMethods = new OSSQueryUiCypherMethods(graph)

  def cypherPost(
    atTime: Option[Milliseconds],
    timeout: FiniteDuration,
    namespaceId: NamespaceId,
    query: CypherQuery
  ): Future[Either[CustomError, CypherQueryResult]] =
    graph.requiredGraphIsReadyFuture {
      catchCypherException {
        val (columns, results, isReadOnly, _) =
          cypherMethods.queryCypherGeneric(query, namespaceId, atTime) // TODO read canContainAllNodeScan
        results
          .via(Util.completionTimeoutOpt(timeout, allowTimeout = isReadOnly))
          .named(s"cypher-query-atTime-${atTime.fold("none")(_.millis.toString)}")
          .runWith(Sink.seq)(graph.materializer)
          .map(CypherQueryResult(columns, _))(ExecutionContext.parasitic)
      }
    }

  //private def ifNamespaceFound(namespaceId: NamespaceId) = ???
  def cypherNodesPost(
    atTime: Option[Milliseconds],
    timeout: FiniteDuration,
    namespaceId: NamespaceId,
    query: CypherQuery
  ): Future[Either[CustomError, Seq[UiNode[QuineId]]]] =
    graph.requiredGraphIsReadyFuture {
      catchCypherException {
        val (results, isReadOnly, _) =
          cypherMethods.queryCypherNodes(query, namespaceId, atTime) // TODO read canContainAllNodeScan
        results
          .via(Util.completionTimeoutOpt(timeout, allowTimeout = isReadOnly))
          .named(s"cypher-nodes-query-atTime-${atTime.fold("none")(_.millis.toString)}")
          .runWith(Sink.seq)(graph.materializer)
      }
    }

  def cypherEdgesPost(
    atTime: Option[Milliseconds],
    timeout: FiniteDuration,
    namespaceId: NamespaceId,
    query: CypherQuery
  ): Future[Either[CustomError, Seq[UiEdge[QuineId]]]] =
    graph.requiredGraphIsReadyFuture {
      catchCypherException {
        val (results, isReadOnly, _) =
          cypherMethods.queryCypherEdges(query, namespaceId, atTime) // TODO read canContainAllNodeScan
        results
          .via(Util.completionTimeoutOpt(timeout, allowTimeout = isReadOnly))
          .named(s"cypher-edges-query-atTime-${atTime.fold("none")(_.millis.toString)}")
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
    saveLocation: TSaveLocation
  ): Either[CustomError, Option[String]] = {

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
            parallelism
          )
        Some(fileName)
      }.toEither
        .left
        .map {
          case _: InvalidPathException | _: FileAlreadyExistsException | _: SecurityException |
              _: FileSystemException =>
            BadRequest(s"Invalid file name: $fileName") // Return a Bad Request Error
          case e: CypherException => BadRequest(s"Invalid query: ${e.getMessage}")
          case e: IllegalArgumentException => BadRequest(e.getMessage)
          case NonFatal(e) => throw e // Return an Internal Server Error
          case other => throw other // This might expose more than we want
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
    atTime: Option[Milliseconds]
  ): Future[Either[CustomError, Option[List[String]]]] = {

    val errors: Either[CustomError, Option[List[String]]] = Try {
      require(!lengthOpt.exists(_ < 1), "walk length cannot be less than one.")
      require(!inOutParamOpt.exists(_ < 0d), "in-out parameter cannot be less than zero.")
      require(!returnParamOpt.exists(_ < 0d), "return parameter cannot be less than zero.")
      Some(Nil)
    }.toEither.left
      .map {
        case e: CypherException => BadRequest(s"Invalid query: ${e.getMessage}")
        case e: IllegalArgumentException => BadRequest(e.getMessage)
        case NonFatal(e) => throw e // Return an Internal Server Error
        case other => throw other // this might expose more than we want
      }
    if (errors.isLeft) Future.successful[Either[CustomError, Option[List[String]]]](errors)
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
          atTime
        )
        .map(w => Right(Some(w.acc)))(ExecutionContext.parasitic)

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
    namespaceId: NamespaceId
  ): Future[Option[Json]] =
    graph.requiredGraphIsReadyFuture {
      graph
        .literalOps(namespaceId)
        .getProps(qid, atTime)
        .map(m =>
          m.get(Symbol(propKey)).map(_.deserialized.get).map(qv => QuineValue.toJson(qv)(graph.idProvider, logConfig))
        )(
          graph.nodeDispatcherEC
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
            props.map { case (k, v) => k.name -> QuineValue.toJson(v.deserialized.get)(graph.idProvider, logConfig) },
            edges.toSeq.map { case HalfEdge(t, d, o) => TRestHalfEdge(t.name, toApiEdgeDirection(d), o) }
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
    namespaceId: NamespaceId
  ): Future[Vector[TRestHalfEdge[QuineId]]] =
    graph.requiredGraphIsReadyFuture {
      val edgeDirOpt2 = edgeDirOpt.map(toModelEdgeDirection)
      graph
        .literalOps(namespaceId)
        .getEdges(qid, edgeTypeOpt.map(Symbol.apply), edgeDirOpt2, otherOpt, limit, atTime)
        .map(_.toVector.map { case HalfEdge(t, d, o) => TRestHalfEdge(t.name, toApiEdgeDirection(d), o) })(
          graph.nodeDispatcherEC
        )

    }

  def debugOpsHalfEdgesGet(
    qid: QuineId,
    atTime: Option[Milliseconds],
    limit: Option[Int],
    edgeDirOpt: Option[TEdgeDirection],
    otherOpt: Option[QuineId],
    edgeTypeOpt: Option[String],
    namespaceId: NamespaceId
  ): Future[Vector[TRestHalfEdge[QuineId]]] =
    graph.requiredGraphIsReadyFuture {
      val edgeDirOpt2 = edgeDirOpt.map(toModelEdgeDirection)
      graph
        .literalOps(namespaceId)
        .getHalfEdges(qid, edgeTypeOpt.map(Symbol.apply), edgeDirOpt2, otherOpt, limit, atTime)
        .map(_.toVector.map { case HalfEdge(t, d, o) => TRestHalfEdge(t.name, toApiEdgeDirection(d), o) })(
          graph.nodeDispatcherEC
        )
    }

  // --------------------- Ingest Endpoints ------------------------

  def createIngestStream(
    ingestName: String,
    namespaceId: NamespaceId,
    ingestConfig: IngestStreamConfiguration
  ): Either[CustomError, Unit] = {

    // return an error on failure. None on success.
    def addSettings(
      name: String,
      intoNamespace: NamespaceId,
      settings: IngestStreamConfiguration
    ): Either[CustomError, Unit] = quineApp.addIngestStream(
      name,
      settings,
      intoNamespace,
      previousStatus = None, // this ingest is being created, not restored, so it has no previous status
      shouldResumeRestoredIngests = false,
      timeout,
      memberIdx = None
    ) match {
      case Success(true) => Right(())
      case Success(false) =>
        Left(
          BadRequest(
            s"Cannot create ingest stream `$name` (a stream with this name already exists)"
          )
        )
      case Failure(_: NamespaceNotFoundException) => Left(NotFound(""))
      case Failure(err) => Left(BadRequest(s"Failed to create ingest stream `$name`: ${err.getMessage}"))
    }

    graph.requiredGraphIsReady()
    //TODO check if ingest stream already exists => 400  s"Cannot create ingest stream `$name` (a stream with this name already exists)"

    ingestConfig match {
      case kafkaSettings: KafkaIngest =>
        KafkaSettingsValidator.validateInput(
          kafkaSettings.kafkaProperties,
          kafkaSettings.groupId,
          kafkaSettings.offsetCommitting
        ) match {
          case Some(errors) =>
            Left(BadRequest(s"Cannot create ingest stream `$ingestName`: ${errors.toList.mkString(",")}"))

          case None => addSettings(ingestName, namespaceId, kafkaSettings)
        }
      case otherSettings: IngestStreamConfiguration =>
        addSettings(ingestName, namespaceId, otherSettings)
    }
  }

  def deleteIngestStream(ingestName: String, namespaceId: NamespaceId): Future[Option[IngestStreamInfoWithName]] =
    graph.requiredGraphIsReadyFuture {

      quineApp.removeIngestStream(ingestName, namespaceId) match {
        case None => Future.successful(None)
        case Some(control @ IngestStreamWithControl(settings, metrics, _, terminated, close, _, _)) =>
          val finalStatus = control.status.map { previousStatus =>
            import IngestStreamStatus._
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
                  })(graph.shardDispatcherEC)
              )(graph.shardDispatcherEC)
          }

          finalStatus
            .zip(terminationMessage)
            .map { case (newStatus, message) =>
              Some(
                IngestStreamInfoWithName(
                  ingestName,
                  newStatus,
                  message,
                  settings,
                  metrics.toEndpointResponse
                )
              )
            }(graph.shardDispatcherEC)
      }
    }

  def pauseIngestStream(
    ingestName: String,
    namespaceId: NamespaceId
  ): Future[Either[CustomError, Option[IngestStreamInfoWithName]]] =
    graph.requiredGraphIsReadyFuture {
      setIngestStreamPauseState(ingestName, namespaceId, SwitchMode.Close)
        .map(Right(_))(ExecutionContext.parasitic)
        .recover(mkPauseOperationError("pause", BadRequest))(ExecutionContext.parasitic)
    }

  def unpauseIngestStream(
    ingestName: String,
    namespaceId: NamespaceId
  ): Future[Either[CustomError, Option[IngestStreamInfoWithName]]] =
    graph.requiredGraphIsReadyFuture {
      setIngestStreamPauseState(ingestName, namespaceId, SwitchMode.Open)
        .map(Right(_))(ExecutionContext.parasitic)
        .recover(mkPauseOperationError("resume", BadRequest))(ExecutionContext.parasitic)
    }

  def ingestStreamStatus(ingestName: String, namespaceId: NamespaceId): Future[Option[IngestStreamInfoWithName]] =
    graph.requiredGraphIsReadyFuture {
      quineApp.getIngestStream(ingestName, namespaceId) match {
        case None => Future.successful(None)
        case Some(stream) => stream2Info(stream).map(s => Some(s.withName(ingestName)))(graph.shardDispatcherEC)
      }
    }

  def listIngestStreams(namespaceId: NamespaceId): Future[Map[String, IngestStreamInfo]] =
    graph.requiredGraphIsReadyFuture {
      Future
        .traverse(
          quineApp.getIngestStreams(namespaceId).toList
        ) { case (name, ingest) =>
          stream2Info(ingest).map(name -> _)(graph.shardDispatcherEC)
        }(implicitly, graph.shardDispatcherEC)
        .map(_.toMap)(graph.shardDispatcherEC)
    }
}
