package com.thatdot.quine.app.routes

import java.time.Instant
import java.time.temporal.ChronoUnit.MILLIS

import scala.compat.ExecutionContexts
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.stream.Materializer
import akka.stream.contrib.{SwitchMode, ValveSwitch}
import akka.stream.scaladsl.Sink
import akka.util.Timeout
import akka.{Done, NotUsed}

import com.codahale.metrics.Metered
import com.typesafe.scalalogging.Logger

import com.thatdot.quine.routes._

trait IngestStreamState {

  def addIngestStream(
    name: String,
    settings: IngestStreamConfiguration,
    wasRestoredFromStorage: Boolean,
    timeout: Timeout
  ): Try[Boolean]

  def getIngestStream(name: String): Option[IngestStreamWithControl[IngestStreamConfiguration]]

  def getIngestStreams(): Map[String, IngestStreamWithControl[IngestStreamConfiguration]]

  def removeIngestStream(
    name: String
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
  valve: Future[ValveSwitch],
  var optWs: Option[(Sink[ujson.Value, NotUsed], IngestMeter)] = None,
  var restored: Boolean = false,
  var close: () => Unit = () => (),
  var terminated: Future[Done] = Future.failed(new Exception("Stream never started"))
) {
  def status: Future[IngestStreamStatus] =
    terminated.value match {
      case Some(Success(Done)) => Future.successful(IngestStreamStatus.Completed)
      case Some(Failure(_)) => Future.successful(IngestStreamStatus.Failed)
      case None =>
        valve.value match {
          case Some(Success(valve)) =>
            valve
              .getMode()
              .map {
                case SwitchMode.Open => IngestStreamStatus.Running
                case SwitchMode.Close if restored => IngestStreamStatus.Restored
                case SwitchMode.Close => IngestStreamStatus.Paused
              }(ExecutionContexts.parasitic)

          case _ =>
            Future.successful(IngestStreamStatus.Running)
        }
    }

  /** Register hooks to freeze metrics and log once the ingest stream terminates
    *
    * @param name name of the ingest stream
    * @param logger where to log about completion
    */
  def registerTerminationHooks(name: String, logger: Logger)(implicit ec: ExecutionContext): Unit =
    terminated.onComplete {
      case Failure(err) =>
        val now = Instant.now
        metrics.stop(now)
        logger.error(
          s"Ingest stream '$name' has failed after ${metrics.millisSinceStart(now)}ms",
          err
        )

      case Success(_) =>
        val now = Instant.now
        metrics.stop(now)
        logger.info(
          s"Ingest stream '$name' successfully completed after ${metrics.millisSinceStart(now)}ms"
        )
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

/** The Akka HTTP implementation of [[IngestRoutes]] */
trait IngestRoutesImpl
    extends IngestRoutes
    with endpoints4s.akkahttp.server.Endpoints
    with endpoints4s.akkahttp.server.JsonEntitiesFromSchemas
    with exts.ServerQuineEndpoints {

  implicit def timeout: Timeout
  implicit def ec: ExecutionContext
  implicit def materializer: Materializer

  private def stream2Info(conf: IngestStreamWithControl[IngestStreamConfiguration]): Future[IngestStreamInfo] =
    conf.status.map { status =>
      IngestStreamInfo(
        status,
        conf.terminated.value collect { case Failure(exception) => exception.toString },
        conf.settings,
        conf.metrics.toEndpointResponse
      )
    }

  val serviceState: IngestStreamState

  /** Try to register a new ingest stream */
  private val ingestStreamStartRoute = ingestStreamStart.implementedBy { case (name, settings) =>
    serviceState.addIngestStream(name, settings, wasRestoredFromStorage = false, timeout) match {
      case Success(false) =>
        Left(
          endpoints4s.Invalid(
            s"Cannot create ingest stream `$name` (a stream with this name already exists)"
          )
        )

      case Success(true) => Right(())
      case Failure(err) => Left(endpoints4s.Invalid(s"Failed to create ingest stream `$name`: $err"))
    }
  }

  /** Try to stop an ingest stream */
  private val ingestStreamStopRoute = ingestStreamStop.implementedByAsync { (name: String) =>
    serviceState.removeIngestStream(name) match {
      case None => Future.successful(None)
      case Some(control @ IngestStreamWithControl(settings, metrics, valve @ _, _, _, close, terminated)) =>
        for {
          previousStatus <- control.status
          _ = close() // start terminating the ingest
          message <- terminated map { case Done => None } recover { case e =>
            Some(e.toString)
          } // wait to respond to the API call until termination finishes
        } yield {
          val newStatus = {
            import IngestStreamStatus._
            previousStatus match {
              // in these cases, the ingest was healthy and runnable/running
              case Running | Paused | Restored => Terminated
              // in these cases, the ingest was not running/runnable
              case Completed | Failed | Terminated => previousStatus
            }
          }
          Some(
            IngestStreamInfoWithName(
              name,
              newStatus,
              message,
              settings,
              metrics.toEndpointResponse
            )
          )
        }
    }
  }

  /** Query out a particular ingest stream */
  private val ingestStreamLookupRoute = ingestStreamLookup.implementedByAsync { (name: String) =>
    serviceState.getIngestStream(name) match {
      case None => Future.successful(None)
      case Some(stream) => stream2Info(stream).map(s => Some(s.withName(name)))
    }
  }

  /** List out all of the currently active ingest streams */
  private val ingestStreamListRoute = ingestStreamList.implementedByAsync { _ =>
    Future
      .traverse(
        serviceState.getIngestStreams(): TraversableOnce[(String, IngestStreamWithControl[IngestStreamConfiguration])]
      ) { case (name, ingest) =>
        stream2Info(ingest).map(name -> _)
      }
      .map(_.toMap)
  }

  private[this] def setIngestStreamPauseState(
    name: String,
    newState: SwitchMode
  ): Future[Option[IngestStreamInfoWithName]] =
    serviceState.getIngestStream(name) match {
      case None => Future.successful(None)
      case Some(ingest: IngestStreamWithControl[IngestStreamConfiguration]) =>
        for {
          valve <- ingest.valve
          _ <- valve.flip(newState)
          _ = (ingest.restored = false)
          info <- stream2Info(ingest)
        } yield Some(info.withName(name))
    }

  private val ingestStreamPauseRoute = ingestStreamPause.implementedByAsync { (name: String) =>
    setIngestStreamPauseState(name, SwitchMode.Close)
  }

  private val ingestStreamUnpauseRoute = ingestStreamUnpause.implementedByAsync { (name: String) =>
    setIngestStreamPauseState(name, SwitchMode.Open)
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
