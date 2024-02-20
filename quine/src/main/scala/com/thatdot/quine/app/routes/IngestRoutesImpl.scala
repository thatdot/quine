package com.thatdot.quine.app.routes

import java.time.Instant
import java.time.temporal.ChronoUnit.MILLIS

import scala.compat.ExecutionContexts
import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.control.NoStackTrace
import scala.util.{Failure, Success, Try}

import org.apache.pekko.http.scaladsl.server.Directives._
import org.apache.pekko.http.scaladsl.server.Route
import org.apache.pekko.stream.scaladsl.Sink
import org.apache.pekko.stream.{Materializer, StreamDetachedException}
import org.apache.pekko.util.Timeout
import org.apache.pekko.{Done, NotUsed}

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.codahale.metrics.Metered
import com.typesafe.scalalogging.LazyLogging
import endpoints4s.Invalid
import io.circe.Json

import com.thatdot.quine.app.ingest.util.KafkaSettingsValidator
import com.thatdot.quine.graph.{BaseGraph, MemberIdx, NamespaceId}
import com.thatdot.quine.routes._
import com.thatdot.quine.util.{SwitchMode, ValveSwitch}

trait IngestStreamState {

  def addIngestStream(
    name: String,
    settings: IngestStreamConfiguration,
    intoNamespace: NamespaceId,
    restoredStatus: Option[IngestStreamStatus],
    shouldRestoreIngest: Boolean,
    timeout: Timeout,
    shouldSaveMetadata: Boolean = true,
    memberIdx: Option[MemberIdx] = None
  ): Try[Boolean]

  def getIngestStream(
    name: String,
    namespace: NamespaceId
  ): Option[IngestStreamWithControl[IngestStreamConfiguration]]

  def getIngestStreams(namespace: NamespaceId): Map[String, IngestStreamWithControl[IngestStreamConfiguration]]

  protected def getIngestStreamsWithStatus(namespace: NamespaceId): Future[Map[String, IngestStreamWithStatus]]

  def removeIngestStream(
    name: String,
    namespace: NamespaceId
  ): Option[IngestStreamWithControl[IngestStreamConfiguration]]
}

/** Adds to the ingest stream configuration extra information that will be
  * materialized only once the ingest stream is running and which may be
  * needed for stopping the stream
  *
  * @param optWs (for websocket ingest streams only) a Sink and IngestMeter via which additional records may be injected into this ingest stream
  */
final private[thatdot] case class IngestStreamWithControl[+Conf](
  settings: Conf,
  metrics: IngestMetrics,
  valve: IO[ValveSwitch],
  terminated: IO[Future[Done]],
  close: IO[Unit],
  var restoredStatus: Option[IngestStreamStatus] = None,
  var optWs: Option[(Sink[Json, NotUsed], IngestMeter)] = None
) extends LazyLogging {

  // Returns a simpler version of status. Only possible values are completed, failed, or running
  private def checkTerminated(implicit materializer: Materializer): Future[IngestStreamStatus] = {
    implicit val ec: ExecutionContext = materializer.executionContext
    for {
      terminated <- terminated.unsafeToFuture()
      result = terminated.value match {
        case Some(Success(Done)) => IngestStreamStatus.Completed
        case Some(Failure(e)) =>
          // If exception occurs, it means that the ingest stream has failed
          logger.warn(s"Ingest stream: ${settings} failed.", e)
          IngestStreamStatus.Failed
        case None => IngestStreamStatus.Running
      }
    } yield result
  }

  private def pendingStatusFuture(
    valveSwitch: ValveSwitch
  )(implicit materializer: Materializer): Future[IngestStreamStatus] = {
    /* Add a timeout to work around <https://github.com/akka/akka-stream-contrib/issues/119>
     *
     * Race the actual call to `getMode` with a timeout action
     */
    val theStatus = Promise[IngestStreamStatus]()
    theStatus.completeWith(
      valveSwitch
        .getMode()
        .map {
          case SwitchMode.Open => IngestStreamStatus.Running
          case SwitchMode.Close => restoredStatus getOrElse IngestStreamStatus.Paused
        }(materializer.executionContext)
        .recover { case _: org.apache.pekko.stream.StreamDetachedException =>
          IngestStreamStatus.Terminated
        }(materializer.executionContext)
    )
    materializer.system.scheduler.scheduleOnce(1.second) {
      val _ = theStatus.trySuccess(IngestStreamStatus.Terminated)
    }(materializer.executionContext)
    theStatus.future
  }

  def status(implicit materializer: Materializer): Future[IngestStreamStatus] = {

    val getPendingStatus: IO[IngestStreamStatus] =
      for {
        vs <- valve
        status <- IO.fromFuture(IO.pure(pendingStatusFuture(vs)))
      } yield status

    val getPendingStatusWithTimeout: IO[IngestStreamStatus] =
      getPendingStatus.timeoutTo(50.milliseconds, IO.pure(IngestStreamStatus.Running))

    val checkTerminatedStatus: IO[IngestStreamStatus] =
      IO.fromFuture(IO.pure(checkTerminated))

    val resultStatus: IO[IngestStreamStatus] =
      for {
        terminated <- checkTerminatedStatus
        result <- terminated match {
          case IngestStreamStatus.Completed => IO.pure(IngestStreamStatus.Completed)
          case IngestStreamStatus.Failed => IO.pure(IngestStreamStatus.Failed)
          case _ => getPendingStatusWithTimeout
        }
      } yield result

    resultStatus.unsafeToFuture()
  }
}

final private[thatdot] case class IngestMetrics(
  startTime: Instant,
  private var completionTime: Option[Instant],
  private var meter: IngestMetered
) {
  def stop(completedAt: Instant): Unit = {
    completionTime = Some(completedAt)
    meter = IngestMetered.freeze(meter)
  }

  def millisSinceStart(t: Instant): Long = MILLIS.between(startTime, t)

  private def meterToIngestRates(meter: Metered) =
    RatesSummary(
      meter.getCount,
      meter.getOneMinuteRate,
      meter.getFiveMinuteRate,
      meter.getFifteenMinuteRate,
      meter.getMeanRate
    )

  def toEndpointResponse: IngestStreamStats = IngestStreamStats(
    ingestedCount = meter.getCount,
    rates = meterToIngestRates(meter.counts),
    byteRates = meterToIngestRates(meter.bytes),
    startTime = startTime,
    totalRuntime = millisSinceStart(completionTime getOrElse Instant.now)
  )
}

/** The Pekko HTTP implementation of [[IngestRoutes]] */
trait IngestRoutesImpl
    extends IngestRoutes
    with endpoints4s.pekkohttp.server.Endpoints
    with com.thatdot.quine.app.routes.exts.circe.JsonEntitiesFromSchemas
    with com.thatdot.quine.app.routes.exts.ServerQuineEndpoints {

  implicit def timeout: Timeout
  implicit def materializer: Materializer
  def graph: BaseGraph

  private def stream2Info(conf: IngestStreamWithControl[IngestStreamConfiguration]): Future[IngestStreamInfo] =
    conf.status.map { status =>
      IngestStreamInfo(
        status,
        conf.terminated.unsafeToFuture().value collect { case Failure(exception) => exception.toString },
        conf.settings,
        conf.metrics.toEndpointResponse
      )
    }(graph.shardDispatcherEC)

  val quineApp: IngestStreamState

  /** Try to register a new ingest stream */
  private val ingestStreamStartRoute: Route = {
    def addSettings(name: String, intoNamespace: NamespaceId, settings: IngestStreamConfiguration) =
      quineApp.addIngestStream(
        name,
        settings,
        intoNamespace,
        None,
        shouldRestoreIngest = false,
        timeout,
        memberIdx = None
      ) match {
        case Success(false) =>
          Left(
            endpoints4s.Invalid(
              s"Cannot create ingest stream `$name` (a stream with this name already exists)"
            )
          )
        case Success(true) => Right(())
        case Failure(err) => Left(endpoints4s.Invalid(s"Failed to create ingest stream `$name`: ${err.getMessage}"))
      }

    ingestStreamStart.implementedBy {
      case (ingestName, namespaceParam, settings: KafkaIngest) =>
        val namespace = namespaceFromParam(namespaceParam)
        KafkaSettingsValidator(settings.kafkaProperties, settings.groupId, settings.offsetCommitting).validate() match {
          case Some(errors) =>
            Left(
              endpoints4s.Invalid(
                s"Cannot create ingest stream `$ingestName`: ${errors.toList.mkString(",")}"
              )
            )
          case None => addSettings(ingestName, namespace, settings)
        }
      case (ingestName, namespaceParam, settings) =>
        val namespace = namespaceFromParam(namespaceParam)
        addSettings(ingestName, namespace, settings)

    }
  }

  /** Try to stop an ingest stream */
  private val ingestStreamStopRoute = ingestStreamStop.implementedByAsync { case (ingestName, namespaceParam) =>
    quineApp.removeIngestStream(ingestName, namespaceFromParam(namespaceParam)) match {
      case None => Future.successful(None)
      case Some(control @ IngestStreamWithControl(settings, metrics, valve @ _, terminated, close, _, _)) =>
        val finalStatus = control.status.map { previousStatus =>
          import IngestStreamStatus._
          previousStatus match {
            // in these cases, the ingest was healthy and runnable/running
            case Running | Paused | Restored => Terminated
            // in these cases, the ingest was not running/runnable
            case Completed | Failed | Terminated => previousStatus
          }
        }(ExecutionContexts.parasitic)

        val terminationMessage: Future[Option[String]] = {
          // start terminating the ingest
          close.unsafeRunAndForget()
          // future will return when termination finishes
          terminated
            .unsafeToFuture()
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

  /** Query out a particular ingest stream */
  private val ingestStreamLookupRoute = ingestStreamLookup.implementedByAsync { case (ingestName, namespaceParam) =>
    quineApp.getIngestStream(ingestName, namespaceFromParam(namespaceParam)) match {
      case None => Future.successful(None)
      case Some(stream) => stream2Info(stream).map(s => Some(s.withName(ingestName)))(graph.shardDispatcherEC)
    }
  }

  /** List out all of the currently active ingest streams */
  private val ingestStreamListRoute = ingestStreamList.implementedByAsync { namespaceParam =>
    Future
      .traverse(
        quineApp.getIngestStreams(namespaceFromParam(namespaceParam)).toList
      ) { case (name, ingest) =>
        stream2Info(ingest).map(name -> _)(graph.shardDispatcherEC)
      }(implicitly, graph.shardDispatcherEC)
      .map(_.toMap)(graph.shardDispatcherEC)
  }

  sealed private case class PauseOperationException(statusMsg: String) extends Exception with NoStackTrace

  private object PauseOperationException {
    object Completed extends PauseOperationException("completed")
    object Terminated extends PauseOperationException("terminated")
    object Failed extends PauseOperationException("failed")
  }
  private[this] def setIngestStreamPauseState(
    name: String,
    namespace: NamespaceId,
    newState: SwitchMode
  ): Future[Option[IngestStreamInfoWithName]] =
    quineApp.getIngestStream(name, namespace) match {
      case None => Future.successful(None)
      case Some(ingest: IngestStreamWithControl[IngestStreamConfiguration]) =>
        ingest.restoredStatus match {
          case Some(IngestStreamStatus.Completed) => Future.failed(PauseOperationException.Completed)
          case Some(IngestStreamStatus.Terminated) => Future.failed(PauseOperationException.Terminated)
          case Some(IngestStreamStatus.Failed) => Future.failed(PauseOperationException.Failed)
          case _ =>
            val flippedValve = ingest.valve.unsafeToFuture().flatMap(_.flip(newState))(graph.nodeDispatcherEC)
            val ingestStatus = flippedValve.flatMap { _ =>
              ingest.restoredStatus = None; // FIXME not threadsafe
              stream2Info(ingest)
            }(graph.nodeDispatcherEC)
            ingestStatus.map(status => Some(status.withName(name)))(ExecutionContexts.parasitic)
        }
    }

  private def mkPauseOperationError(operation: String): PartialFunction[Throwable, Either[Invalid, Nothing]] = {
    case _: StreamDetachedException =>
      // A StreamDetachedException always occurs when the ingest has failed
      Left(endpoints4s.Invalid(s"Cannot ${operation} a failed ingest."))
    case e: PauseOperationException =>
      Left(endpoints4s.Invalid(s"Cannot ${operation} a ${e.statusMsg} ingest."))
  }

  private val ingestStreamPauseRoute = ingestStreamPause.implementedByAsync { case (ingestName, namespaceParam) =>
    setIngestStreamPauseState(ingestName, namespaceFromParam(namespaceParam), SwitchMode.Close)
      .map(Right(_))(ExecutionContexts.parasitic)
      .recover(mkPauseOperationError("pause"))(ExecutionContexts.parasitic)
  }

  private val ingestStreamUnpauseRoute = ingestStreamUnpause.implementedByAsync { case (ingestName, namespaceParam) =>
    setIngestStreamPauseState(ingestName, namespaceFromParam(namespaceParam), SwitchMode.Open)
      .map(Right(_))(ExecutionContexts.parasitic)
      .recover(mkPauseOperationError("resume"))(ExecutionContexts.parasitic)
  }

  final val ingestRoutes: Route = {
    ingestStreamStartRoute ~
    ingestStreamStopRoute ~
    ingestStreamLookupRoute ~
    ingestStreamListRoute ~
    ingestStreamPauseRoute ~
    ingestStreamUnpauseRoute
  }
}
