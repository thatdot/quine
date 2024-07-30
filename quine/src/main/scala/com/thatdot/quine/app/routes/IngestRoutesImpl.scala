package com.thatdot.quine.app.routes

import java.time.Instant
import java.time.temporal.ChronoUnit.MILLIS

import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success, Try}

import org.apache.pekko.http.scaladsl.server.Directives._
import org.apache.pekko.http.scaladsl.server.Route
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.Sink
import org.apache.pekko.util.Timeout
import org.apache.pekko.{Done, NotUsed, pattern}

import com.codahale.metrics.Metered
import io.circe.Json

import com.thatdot.quine.app.NamespaceNotFoundException
import com.thatdot.quine.app.ingest.util.KafkaSettingsValidator
import com.thatdot.quine.graph.{MemberIdx, NamespaceId}
import com.thatdot.quine.routes._
import com.thatdot.quine.util.Log._
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
final case class IngestStreamWithControl[+Conf](
  settings: Conf,
  metrics: IngestMetrics,
  valve: () => Future[ValveSwitch],
  terminated: () => Future[Future[Done]],
  var close: () => Unit = () => (),
  var restoredStatus: Option[IngestStreamStatus] = None,
  var optWs: Option[(Sink[Json, NotUsed], IngestMeter)] = None
)(implicit logConfig: LogConfig)
    extends LazySafeLogging {

  // Returns a simpler version of status. Only possible values are completed, failed, or running
  private def checkTerminated(implicit materializer: Materializer): Future[IngestStreamStatus] = {
    implicit val ec: ExecutionContext = materializer.executionContext
    terminated().map(term =>
      term.value match {
        case Some(Success(Done)) => IngestStreamStatus.Completed
        case Some(Failure(e)) =>
          // If exception occurs, it means that the ingest stream has failed
          logger.warn(log"Ingest stream: ${settings.toString} failed." withException e)
          IngestStreamStatus.Failed
        case None => IngestStreamStatus.Running
      }
    )
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

    implicit val ec: ExecutionContext = materializer.executionContext
    val getPendingStatus: Future[IngestStreamStatus] =
      for {
        vs <- valve()
        status <- pendingStatusFuture(vs)
      } yield status

    val timeout = pattern.after(200.millis)(Future.successful(IngestStreamStatus.Running))(materializer.system)
    val getPendingStatusWithTimeout = Future.firstCompletedOf(Seq(getPendingStatus, timeout))

    for {
      terminated <- checkTerminated
      result <- terminated match {
        case IngestStreamStatus.Completed => Future.successful(IngestStreamStatus.Completed)
        case IngestStreamStatus.Failed => Future.successful(IngestStreamStatus.Failed)
        case _ => getPendingStatusWithTimeout
      }
    } yield result
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
    with IngestApiMethods
    with endpoints4s.pekkohttp.server.Endpoints
    with com.thatdot.quine.app.routes.exts.circe.JsonEntitiesFromSchemas
    with com.thatdot.quine.app.routes.exts.ServerQuineEndpoints {

  implicit def timeout: Timeout
  implicit def materializer: Materializer

  val quineApp: IngestStreamState

  /** Try to register a new ingest stream.
    * The Either represents a bad request on the Left, and the inner Option represents Some(success) or that the
    * namespace was not found (404).
    */
  implicit protected def logConfig: LogConfig
  private val ingestStreamStartRoute: Route = {
    val http404: Either[ClientErrors, Option[Nothing]] = Right(None)
    def http400(errors: ClientErrors): Either[ClientErrors, Option[Nothing]] = Left(errors)
    def httpSuccess[A](a: A): Either[ClientErrors, Option[A]] = Right(Some(a))
    def addSettings(
      name: String,
      intoNamespace: NamespaceId,
      settings: IngestStreamConfiguration
    ): Either[ClientErrors, Option[Unit]] =
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
          http400(
            endpoints4s.Invalid(
              s"Cannot create ingest stream `$name` (a stream with this name already exists)"
            )
          )
        case Success(true) => httpSuccess(())
        case Failure(_: NamespaceNotFoundException) => http404
        case Failure(err) => http400(endpoints4s.Invalid(s"Failed to create ingest stream `$name`: ${err.getMessage}"))
      }

    ingestStreamStart.implementedBy {
      case (ingestName, namespaceParam, settings: KafkaIngest) =>
        graph.requiredGraphIsReady()
        val namespace = namespaceFromParam(namespaceParam)
        KafkaSettingsValidator.validateInput(
          settings.kafkaProperties,
          settings.groupId,
          settings.offsetCommitting
        ) match {
          case Some(errors) =>
            http400(
              endpoints4s.Invalid(
                s"Cannot create ingest stream `$ingestName`: ${errors.toList.mkString(",")}"
              )
            )
          case None => addSettings(ingestName, namespace, settings)
        }
      case (ingestName, namespaceParam, settings) =>
        graph.requiredGraphIsReady()
        val namespace = namespaceFromParam(namespaceParam)
        addSettings(ingestName, namespace, settings)
    }
  }

  /** Try to stop an ingest stream */
  private val ingestStreamStopRoute = ingestStreamStop.implementedByAsync { case (ingestName, namespaceParam) =>
    graph.requiredGraphIsReadyFuture {
      quineApp.removeIngestStream(ingestName, namespaceFromParam(namespaceParam)) match {
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
  }

  /** Query out a particular ingest stream */
  private val ingestStreamLookupRoute = ingestStreamLookup.implementedByAsync { case (ingestName, namespaceParam) =>
    graph.requiredGraphIsReadyFuture {
      quineApp.getIngestStream(ingestName, namespaceFromParam(namespaceParam)) match {
        case None => Future.successful(None)
        case Some(stream) => stream2Info(stream).map(s => Some(s.withName(ingestName)))(graph.shardDispatcherEC)
      }
    }
  }

  /** List out all of the currently active ingest streams */
  private val ingestStreamListRoute = ingestStreamList.implementedByAsync { namespaceParam =>
    graph.requiredGraphIsReadyFuture {
      Future
        .traverse(
          quineApp.getIngestStreams(namespaceFromParam(namespaceParam)).toList
        ) { case (name, ingest) =>
          stream2Info(ingest).map(name -> _)(graph.shardDispatcherEC)
        }(implicitly, graph.shardDispatcherEC)
        .map(_.toMap)(graph.shardDispatcherEC)
    }
  }

  private val ingestStreamPauseRoute = ingestStreamPause.implementedByAsync { case (ingestName, namespaceParam) =>
    graph.requiredGraphIsReadyFuture {
      setIngestStreamPauseState(ingestName, namespaceFromParam(namespaceParam), SwitchMode.Close)
        .map(Right(_))(ExecutionContext.parasitic)
        .recover(mkPauseOperationError("pause", endpoints4s.Invalid(_)))(ExecutionContext.parasitic)
    }
  }

  private val ingestStreamUnpauseRoute = ingestStreamUnpause.implementedByAsync { case (ingestName, namespaceParam) =>
    graph.requiredGraphIsReadyFuture {
      setIngestStreamPauseState(ingestName, namespaceFromParam(namespaceParam), SwitchMode.Open)
        .map(Right(_))(ExecutionContext.parasitic)
        .recover(mkPauseOperationError("resume", endpoints4s.Invalid(_)))(ExecutionContext.parasitic)
    }
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
