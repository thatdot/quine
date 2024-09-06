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
import com.thatdot.quine.app.v2api.endpoints.V2IngestEntities.{IngestConfiguration => V2IngestConfiguration}
import com.thatdot.quine.graph.{MemberIdx, NamespaceId, defaultNamespaceId}
import com.thatdot.quine.routes._
import com.thatdot.quine.util.Log._
import com.thatdot.quine.util.{SwitchMode, ValveSwitch}

trait IngestStreamState {

  /** Store ingests allowing for either v1 or v2 types. */
  type UnifiedIngestConfiguration = Either[V2IngestConfiguration, IngestStreamConfiguration]

  type IngestName = String
  @volatile
  protected var ingestStreams: Map[NamespaceId, Map[IngestName, IngestStreamWithControl[UnifiedIngestConfiguration]]] =
    Map(defaultNamespaceId -> Map.empty)

  /** Add an ingest stream to the running application. The ingest may be new or restored from persistence.
    *
    * TODO these two concerns should be separated into two methods, or at least two signatures -- there are too many
    *   dependencies between parameters.
    * @param name                         Name of the stream to add
    * @param settings                     Configuration for the stream
    * @param intoNamespace                Namespace into which the stream should ingest data
    * @param previousStatus               Some previous status of the stream, if it was restored from persistence.
    *                                     None for new ingests
    * @param shouldResumeRestoredIngests  If restoring an ingest, should the ingest be resumed? When `previousStatus`
    *                                     is None, this has no effect.
    * @param timeout                      How long to allow for the attempt to persist the stream to the metadata table
    *                                     (when shouldSaveMetadata = true). Has no effect if !shouldSaveMetadata
    * @param shouldSaveMetadata           Whether the application should persist this stream to the metadata table.
    *                                     This should be false when restoring from persistence (i.e., from the metadata
    *                                     table) and true otherwise.
    * @param memberIdx                    The cluster member index on which this ingest is being created
    * @return Success(true) when the operation was successful, or a Failure otherwise
    */
  def addIngestStream(
    name: String,
    settings: IngestStreamConfiguration,
    intoNamespace: NamespaceId,
    previousStatus: Option[IngestStreamStatus],
    shouldResumeRestoredIngests: Boolean,
    timeout: Timeout,
    shouldSaveMetadata: Boolean = true,
    memberIdx: Option[MemberIdx] = None,
    // support switching between v1 and v2 ingest. This will only be true when
    // called from the v2 api when starting quine with the v2 enabled flag set.
    // This should be removed when v1 api is removed.
    useV2Ingest: Boolean = false
  ): Try[Boolean]

  def getIngestStream(
    name: String,
    namespace: NamespaceId
  ): Option[IngestStreamWithControl[IngestStreamConfiguration]] = getIngestStreams(namespace).get(name)

  def getIngestStreams(namespace: NamespaceId): Map[String, IngestStreamWithControl[IngestStreamConfiguration]]

  protected def getIngestStreamsFromState(
    namespace: NamespaceId
  )(implicit logConfig: LogConfig): Map[IngestName, IngestStreamWithControl[IngestStreamConfiguration]] =
    ingestStreams
      .getOrElse(namespace, Map.empty)
      .view
      .mapValues((isc: IngestStreamWithControl[UnifiedIngestConfiguration]) =>
        isc.settings match {
          case Left(v1) => isc.copy(settings = v1.asV1IngestStreamConfiguration)
          case Right(v2) => isc.copy(settings = v2)
        }
      )
      .toMap

  protected def getIngestStreamsWithStatus(namespace: NamespaceId): Future[Map[String, IngestStreamWithStatus]]

  //TODO MAP + PERISTENCE
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

/** Adds to the ingest stream configuration extra information that will be
  * materialized only once the ingest stream is running and which may be
  * needed for stopping the stream
  *
  * @param settings      the product-specific stream configuration being managed
  * @param metrics       the metrics handle for this ingest stream
  * @param valve         asynchronous function to get a handle to the ingest's pause valve. Because of the possibility
  *                      that stream materialization is attempted multiple times, this function is not idempotent
  * @param terminated    asynchronous function to get a handle to the ingest's termination signal. Because of the
  *                      possibility that stream materialization is attempted multiple times, this function is not
  *                      idempotent
  * @param initialStatus the status of the ingest stream when it was first created. This is `Running` for newly-created
  *                      ingests, but may have any value except `Terminated` for ingests restored from persistence.
  *                      To get the ingest's current status, use `status` instead. This should be a val but it's
  *                      used to patch in a rendered status in setIngestStreamPauseState
  * @param close         Callback to request the ingest stream to stop. Once this is called, `terminated`'s inner future
  *                      will eventually complete. This should be a val but it's constructed out of order by Novelty
  *                      streams.
  * @param optWs         HACK: opaque stash of additional information for Novelty websocket streams. This should be
  *                      refactored out of this class.
  */
final case class IngestStreamWithControl[+Conf](
  settings: Conf,
  metrics: IngestMetrics,
  valve: () => Future[ValveSwitch],
  terminated: () => Future[Future[Done]],
  var close: () => Unit,
  var initialStatus: IngestStreamStatus,
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
          case SwitchMode.Close =>
            // NB this may return an incorrect or outdated status due to thread-unsafe updates to initialStatus and
            // incomplete information about terminal states across restarts. See discussion and linked diagram on
            // QU-2003.
            initialStatus
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
        previousStatus = None, // this ingest is being created, not restored, so it has no previous status
        shouldResumeRestoredIngests = false,
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
        case Some(
              control @ IngestStreamWithControl(
                settings,
                metrics,
                valve @ _,
                terminated,
                close,
                initialStatus @ _,
                optWs @ _
              )
            ) =>
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
