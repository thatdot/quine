package com.thatdot.quine.app.routes

import java.time.Instant
import java.time.temporal.ChronoUnit.MILLIS

import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.Sink
import org.apache.pekko.{Done, NotUsed, pattern}

import com.codahale.metrics.Metered
import io.circe.Json

import com.thatdot.common.logging.Log.{LazySafeLogging, LogConfig, Loggable, SafeLoggableInterpolator}
import com.thatdot.quine.app.ingest.QuineIngestSource
import com.thatdot.quine.routes.{IngestStreamStats, IngestStreamStatus, RatesSummary}
import com.thatdot.quine.util.{SwitchMode, ValveSwitch}

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
final case class IngestStreamWithControl[+Conf: Loggable](
  settings: Conf,
  metrics: IngestMetrics,
  valve: () => Future[ValveSwitch],
  terminated: () => Future[Future[Done]],
  var close: () => Unit,
  var initialStatus: IngestStreamStatus,
  var optWs: Option[(Sink[Json, NotUsed], IngestMeter)] = None,
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
          logger.warn(log"Ingest stream failed: $settings" withException e)
          IngestStreamStatus.Failed
        case None => IngestStreamStatus.Running
      },
    )
  }

  private def pendingStatusFuture(
    valveSwitch: ValveSwitch,
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
        }(materializer.executionContext),
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

object IngestStreamWithControl {
  def apply[Conf: Loggable](
    conf: Conf,
    metrics: IngestMetrics,
    quineIngestSource: QuineIngestSource,
    initialStatus: IngestStreamStatus,
  )(implicit logConfig: LogConfig): IngestStreamWithControl[Conf] =
    IngestStreamWithControl(
      conf,
      metrics,
      () => quineIngestSource.getControl.map(_.valveHandle)(ExecutionContext.parasitic),
      () => quineIngestSource.getControl.map(_.termSignal)(ExecutionContext.parasitic),
      close = () => {
        quineIngestSource.getControl.flatMap(c => c.terminate())(ExecutionContext.parasitic)
        () // Intentional fire and forget
      },
      initialStatus,
    )
}

final case class IngestMetrics(
  startTime: Instant,
  private var completionTime: Option[Instant],
  private var meter: IngestMetered,
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
      meter.getMeanRate,
    )

  def toEndpointResponse: IngestStreamStats = IngestStreamStats(
    ingestedCount = meter.getCount,
    rates = meterToIngestRates(meter.counts),
    byteRates = meterToIngestRates(meter.bytes),
    startTime = startTime,
    totalRuntime = millisSinceStart(completionTime getOrElse Instant.now),
  )
}
