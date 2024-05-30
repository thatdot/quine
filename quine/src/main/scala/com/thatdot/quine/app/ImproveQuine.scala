package com.thatdot.quine.app

import java.net.NetworkInterface
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.time.Instant
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.util.{Base64, UUID}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._
import scala.util.{Success, Try}

import org.apache.pekko.actor.{ActorSystem, Scheduler}
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.model.{HttpEntity, HttpMethods, HttpRequest, Uri}
import org.apache.pekko.pattern.retry

import com.typesafe.scalalogging.StrictLogging
import io.circe.generic.auto._
import io.circe.syntax._

import com.thatdot.quine.app.ImproveQuine.{
  InstanceHeartbeat,
  InstanceStarted,
  TelemetryRequest,
  sinksFromStandingQueries,
  sourcesFromIngestStreams
}
import com.thatdot.quine.app.routes.{IngestStreamState, IngestStreamWithControl, StandingQueryStore}
import com.thatdot.quine.graph.defaultNamespaceId
import com.thatdot.quine.routes.{IngestStreamConfiguration, RegisteredStandingQuery}

object ImproveQuine {

  /** Type for the category of a telemetry event */
  sealed abstract class Event(val slug: String)

  /** Telemetry event when the application first starts */
  case object InstanceStarted extends Event("instance.started")

  /** Telemetry event sent during a regular interval */
  case object InstanceHeartbeat extends Event("instance.heartbeat")

  case class TelemetryData(
    event: String,
    service: String,
    version: String,
    host_hash: String,
    time: String,
    session_id: String,
    uptime: Long,
    persistor: String,
    sources: Option[List[String]],
    sinks: Option[List[String]],
    apiKey: Option[String]
  )

  val eventUri: Uri = Uri("https://improve.quine.io/event")
  case class TelemetryRequest(
    event: Event,
    service: String,
    version: String,
    hostHash: String,
    sessionId: UUID,
    uptime: Long,
    persistor: String,
    sources: Option[List[String]],
    sinks: Option[List[String]],
    apiKey: Option[String]
  )(implicit system: ActorSystem)
      extends StrictLogging {
    implicit private val executionContext: ExecutionContext = system.dispatcher
    implicit private val scheduler: Scheduler = system.scheduler

    def run(): Future[Unit] = {
      val now = java.time.OffsetDateTime.now().format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)

      val telemetryData = TelemetryData(
        event.slug,
        service,
        version,
        hostHash,
        now,
        sessionId.toString,
        uptime,
        persistor,
        sources,
        sinks,
        apiKey
      )

      val body = telemetryData.asJson.noSpaces

      val send = () =>
        Http()
          .singleRequest(
            HttpRequest(
              method = HttpMethods.POST,
              uri = eventUri,
              entity = HttpEntity(body)
            )
          )

      logger.info(s"Sending anonymous usage data: $body")
      retry(send, 3, 5.seconds)
        .transform(_ => Success(()))
    }
  }

  def sourcesFromIngestStreams(
    ingestStreams: Map[String, IngestStreamWithControl[IngestStreamConfiguration]]
  ): List[String] =
    ingestStreams.values
      .map(_.settings.slug)
      .toSet
      .toList

  def sinksFromStandingQueries(standingQueries: List[RegisteredStandingQuery]): List[String] =
    standingQueries
      .flatMap(_.outputs.values)
      .map(_.slug)
      .distinct

}

class ImproveQuine(
  service: String,
  version: String,
  persistor: String,
  app: Option[StandingQueryStore with IngestStreamState],
  apiKey: Option[String] = None
)(implicit system: ActorSystem) {

  private val invalidMacAddresses = Set(
    Array.fill[Byte](6)(0x00),
    Array.fill[Byte](6)(0xFF.toByte)
  ).map(ByteBuffer.wrap)

  private def hostMac(): Array[Byte] =
    NetworkInterface.getNetworkInterfaces.asScala
      .filter(_.getHardwareAddress != null)
      .map(nic => ByteBuffer.wrap(nic.getHardwareAddress))
      .filter(address => !invalidMacAddresses.contains(address))
      .toVector
      .sorted
      .headOption
      .getOrElse(ByteBuffer.wrap(Array.emptyByteArray))
      .array()

  private val prefixBytes = "Quine_".getBytes(StandardCharsets.UTF_8)

  private def hostHash(): String = Try {
    val mac = hostMac()
    val encoder = Base64.getEncoder
    // Salt the input to prevent a SHA256 of a MAC address from matching another system using a SHA256 of a MAC
    // address for extra anonymity.
    val prefixedBytes = Array.concat(prefixBytes, mac)
    val hash = MessageDigest.getInstance("SHA-256").digest(prefixedBytes)
    encoder.encodeToString(hash)
  }.getOrElse("host_unavailable")

  private def getSources: Option[List[String]] =
    app
      .map(_.getIngestStreams(defaultNamespaceId))
      .map(sourcesFromIngestStreams)

  private def getSinks: Future[Option[List[String]]] = app match {
    case Some(a) =>
      a.getStandingQueries(defaultNamespaceId)
        .map(sinksFromStandingQueries)(ExecutionContext.parasitic)
        .map(Some(_))(ExecutionContext.parasitic)
    case None =>
      Future.successful(None)
  }

  private val sessionId: UUID = UUID.randomUUID()
  private val startTime: Instant = Instant.now()

  private val oneOffHeartbeatIntervals: List[FiniteDuration] = List(
    15.minutes,
    1.hours,
    3.hours,
    6.hours,
    12.hours
  )
  private val regularHeartbeatInterval: FiniteDuration = 1.day

  private def send(
    event: ImproveQuine.Event,
    sources: Option[List[String]],
    sinks: Option[List[String]]
  )(implicit system: ActorSystem): Future[Unit] = {
    val uptime: Long = startTime.until(Instant.now(), ChronoUnit.SECONDS)
    TelemetryRequest(
      event,
      service,
      version,
      hostHash(),
      sessionId,
      uptime,
      persistor,
      sources,
      sinks,
      apiKey
    )
      .run()
  }

  private def startup(sources: Option[List[String]], sinks: Option[List[String]]): Future[Unit] =
    send(InstanceStarted, sources, sinks)

  private def heartbeat(sources: Option[List[String]], sinks: Option[List[String]]): Future[Unit] =
    send(InstanceHeartbeat, sources, sinks)

  private val heartbeatRunnable: Runnable = new Runnable {
    implicit val ec: ExecutionContext = system.dispatcher
    def run(): Unit = {
      val sources = getSources
      for {
        sinks <- getSinks
        res <- heartbeat(
          sources = sources,
          sinks = sinks
        )
      } yield res // Fire and forget heartbeat Future
      ()
    }
  }

  /** Fire and forget function to send startup telemetry and schedule regular heartbeat events.
    */
  def startTelemetry(): Unit = {
    implicit val ec: ExecutionContext = system.dispatcher
    val sources = getSources
    for {
      sinks <- getSinks
      // Send the "instance.started" event
      _ <- startup(sources, sinks)
    } yield ()
    // Schedule initial and regular "instance.heartbeat" events
    oneOffHeartbeatIntervals.foreach { i =>
      system.scheduler.scheduleOnce(i, heartbeatRunnable)
    }
    system.scheduler.scheduleAtFixedRate(regularHeartbeatInterval, regularHeartbeatInterval)(heartbeatRunnable)
    () // Intentionally discard the cancellables for the scheduled heartbeats.
    // In future these could be retained if desired.
  }

}
