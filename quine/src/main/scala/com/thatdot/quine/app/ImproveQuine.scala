package com.thatdot.quine.app

import java.net.NetworkInterface
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.time.Instant
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.util.Base64.Encoder
import java.util.{Base64, UUID}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._
import scala.util.{Success, Try}

import org.apache.pekko.actor.{ActorSystem, Scheduler}
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.model.{HttpEntity, HttpMethods, HttpRequest, Uri}
import org.apache.pekko.pattern.retry

import io.circe.generic.auto._
import io.circe.syntax._

import com.thatdot.quine.app.ImproveQuine.{
  InstanceHeartbeat,
  InstanceStarted,
  RecipeInfo,
  TelemetryRequest,
  sinksFromStandingQueries,
  sourcesFromIngestStreams,
}
import com.thatdot.quine.app.routes.{IngestStreamState, IngestStreamWithControl, StandingQueryStore}
import com.thatdot.quine.graph.defaultNamespaceId
import com.thatdot.quine.routes.{IngestStreamConfiguration, RegisteredStandingQuery, StandingQueryResultOutputUserDef}
import com.thatdot.quine.util.Log._

object ImproveQuine {

  /** Type for the category of a telemetry event */
  sealed abstract class Event(val slug: String)

  /** Telemetry event when the application first starts */
  case object InstanceStarted extends Event("instance.started")

  /** Telemetry event sent during a regular interval */
  case object InstanceHeartbeat extends Event("instance.heartbeat")

  case class RecipeInfo(recipe_name_hash: String, recipe_contents_hash: String)

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
    recipe: Boolean,
    recipe_canonical_name: Option[String],
    recipe_info: Option[RecipeInfo],
    apiKey: Option[String],
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
    recipeUsed: Boolean,
    recipeCanonicalName: Option[String],
    recipeInfo: Option[RecipeInfo],
    apiKey: Option[String],
  )(implicit system: ActorSystem)
      extends StrictSafeLogging {
    implicit private val executionContext: ExecutionContext = system.dispatcher
    implicit private val scheduler: Scheduler = system.scheduler

    def run()(implicit logConfig: LogConfig): Future[Unit] = {
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
        recipeUsed,
        recipeCanonicalName,
        recipeInfo,
        apiKey,
      )

      val body = telemetryData.asJson.noSpaces

      val send = () =>
        Http()
          .singleRequest(
            HttpRequest(
              method = HttpMethods.POST,
              uri = eventUri,
              entity = HttpEntity(body),
            ),
          )

      logger.info(log"Sending anonymous usage data: ${Safe(body)}")
      retry(send, 3, 5.seconds)
        .transform(_ => Success(()))
    }
  }

  def sourcesFromIngestStreams(
    ingestStreams: Map[String, IngestStreamWithControl[IngestStreamConfiguration]],
  ): List[String] =
    ingestStreams.values
      .map(_.settings.slug)
      .toSet
      .toList

  def unrollCypherOutput(output: StandingQueryResultOutputUserDef): List[StandingQueryResultOutputUserDef] =
    output match {
      case cypherOutput: StandingQueryResultOutputUserDef.CypherQuery =>
        cypherOutput.andThen match {
          case None => List(cypherOutput)
          case Some(nextOutput) => cypherOutput :: unrollCypherOutput(nextOutput)
        }
      case otherOutput => List(otherOutput)
    }

  def sinksFromStandingQueries(standingQueries: List[RegisteredStandingQuery]): List[String] =
    standingQueries
      .flatMap(_.outputs.values)
      .flatMap(unrollCypherOutput)
      .map(_.slug)
      .distinct

}

class ImproveQuine(
  service: String,
  version: String,
  persistor: String,
  app: Option[StandingQueryStore with IngestStreamState],
  recipe: Option[Recipe] = None,
  recipeCanonicalName: Option[String] = None,
  apiKey: Option[String] = None,
)(implicit system: ActorSystem, logConfig: LogConfig)
    extends LazySafeLogging {

  // Since MessageDigest is not thread-safe, each function should create an instance its own use
  def hasherInstance(): MessageDigest = MessageDigest.getInstance("SHA-256")
  val base64: Encoder = Base64.getEncoder

  def recipeContentsHash(recipe: Recipe): Array[Byte] = {
    val sha256: MessageDigest = hasherInstance()
    // Since this is not mission-critical, letting the JVM object hash function do the heavy lifting
    sha256.update(recipe.ingestStreams.hashCode().toByte)
    sha256.update(recipe.standingQueries.hashCode().toByte)
    sha256.update(recipe.nodeAppearances.hashCode().toByte)
    sha256.update(recipe.quickQueries.hashCode().toByte)
    sha256.update(recipe.sampleQueries.hashCode().toByte)
    sha256.update(recipe.statusQuery.hashCode().toByte)
    sha256.digest()
  }

  val recipeUsed: Boolean = recipe.isDefined
  val recipeInfo: Option[RecipeInfo] = recipe
    .map { r =>
      val sha256: MessageDigest = hasherInstance()
      RecipeInfo(
        base64.encodeToString(sha256.digest(r.title.getBytes(StandardCharsets.UTF_8))),
        base64.encodeToString(recipeContentsHash(r)),
      )
    }

  private val invalidMacAddresses = Set(
    Array.fill[Byte](6)(0x00),
    Array.fill[Byte](6)(0xFF.toByte),
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
    val sha256: MessageDigest = hasherInstance()
    val mac = hostMac()
    // Salt the input to prevent a SHA256 of a MAC address from matching another system using a SHA256 of a MAC
    // address for extra anonymity.
    val prefixedBytes = Array.concat(prefixBytes, mac)
    val hash = sha256.digest(prefixedBytes)
    base64.encodeToString(hash)
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
    12.hours,
  )
  private val regularHeartbeatInterval: FiniteDuration = 1.day

  private def send(
    event: ImproveQuine.Event,
    sources: Option[List[String]],
    sinks: Option[List[String]],
  )(implicit system: ActorSystem, logConfig: LogConfig): Future[Unit] = {
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
      recipeUsed,
      recipeCanonicalName,
      recipeInfo,
      apiKey,
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
          sinks = sinks,
        )
      } yield res // Fire and forget heartbeat Future
      ()
    }
  }

  /** Fire and forget function to send startup telemetry and schedule regular heartbeat events.
    */
  def startTelemetry(): Unit = {
    logger.info(safe"Starting usage telemetry")
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
