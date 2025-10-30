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

import com.thatdot.common.logging.Log.{LazySafeLogging, LogConfig, Safe, SafeLoggableInterpolator, StrictSafeLogging}
import com.thatdot.quine.app.routes.IngestStreamWithControl
import com.thatdot.quine.routes.{IngestStreamConfiguration, RegisteredStandingQuery, StandingQueryResultOutputUserDef}

/** Schedules and sends reportable activity telemetry. Performs core value derivations
  * and serves as a helper for the same purpose via controllers in clustered settings.
  */
class ImproveQuine(
  service: String,
  version: String,
  persistorSlug: String,
  getSources: () => Future[Option[List[String]]],
  getSinks: () => Future[Option[List[String]]],
  recipe: Option[Recipe] = None,
  recipeCanonicalName: Option[String] = None,
  apiKey: () => Option[String] = () => None,
)(implicit system: ActorSystem, logConfig: LogConfig)
    extends LazySafeLogging {
  import ImproveQuine._

  /** Since MessageDigest is not thread-safe, each function should create an instance its own use */
  private def hasherInstance(): MessageDigest = MessageDigest.getInstance("SHA-256")
  private val base64: Encoder = Base64.getEncoder

  private def recipeContentsHash(recipe: Recipe): Array[Byte] = {
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

  private val recipeUsed: Boolean = recipe.isDefined
  private val recipeInfo: Option[RecipeInfo] = recipe
    .map { r =>
      val sha256: MessageDigest = hasherInstance()
      RecipeInfo(
        base64.encodeToString(sha256.digest(r.title.getBytes(StandardCharsets.UTF_8))),
        base64.encodeToString(recipeContentsHash(r)),
      )
    }

  private val invalidMacAddresses: Set[ByteBuffer] = Set(
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

  private val prefixBytes: Array[Byte] = "Quine_".getBytes(StandardCharsets.UTF_8)

  private def hostHash(): String = Try {
    val sha256: MessageDigest = hasherInstance()
    val mac = hostMac()
    // Salt the input to prevent a SHA256 of a MAC address from matching another system using a SHA256 of a MAC
    // address for extra anonymity.
    val prefixedBytes = Array.concat(prefixBytes, mac)
    val hash = sha256.digest(prefixedBytes)
    base64.encodeToString(hash)
  }.getOrElse("host_unavailable")

  protected val sessionId: UUID = UUID.randomUUID()
  protected val startTime: Instant = Instant.now()

  protected def send(
    event: Event,
    sources: Option[List[String]],
    sinks: Option[List[String]],
    sessionStartedAt: Instant = startTime,
    sessionIdentifier: UUID = sessionId,
  )(implicit system: ActorSystem, logConfig: LogConfig): Future[Unit] = TelemetryRequest(
    event = event,
    service = service,
    version = version,
    hostHash = hostHash(),
    sessionId = sessionIdentifier,
    uptime = sessionStartedAt.until(Instant.now(), ChronoUnit.SECONDS),
    persistor = persistorSlug,
    sources = sources,
    sinks = sinks,
    recipeUsed = recipeUsed,
    recipeCanonicalName = recipeCanonicalName,
    recipeInfo = recipeInfo,
    apiKey = apiKey(), // Call the function to get the current value
  ).run()

  def startup(
    sources: Option[List[String]],
    sinks: Option[List[String]],
    sessionStartedAt: Instant = startTime,
    sessionIdentifier: UUID = sessionId,
  ): Future[Unit] = send(InstanceStarted, sources, sinks, sessionStartedAt, sessionIdentifier)

  def heartbeat(
    sources: Option[List[String]],
    sinks: Option[List[String]],
    sessionStartedAt: Instant = startTime,
    sessionIdentifier: UUID = sessionId,
  ): Future[Unit] = send(InstanceHeartbeat, sources, sinks, sessionStartedAt, sessionIdentifier)

  /** A runnable for use in an actor system schedule that fires-and-forgets the heartbeat Future */
  private val heartbeatRunnable: Runnable = () => {
    implicit val ec: ExecutionContext = ExecutionContext.parasitic
    val _ = for {
      sources <- getSources()
      sinks <- getSinks()
      _ <- heartbeat(sources, sinks)
    } yield ()
  }

  /** Fire and forget function to send startup telemetry and schedule regular heartbeat events. */
  def startTelemetry(): Unit = {
    logger.info(safe"Starting usage telemetry")
    implicit val ec: ExecutionContext = ExecutionContext.parasitic
    for {
      sources <- getSources()
      sinks <- getSinks()
      _ <- startup(sources, sinks)
    } yield ()
    // Schedule run-up "instance.heartbeat" events
    runUpIntervals.foreach(system.scheduler.scheduleOnce(_, heartbeatRunnable))
    // Schedule regular "instance.heartbeat" events
    system.scheduler.scheduleAtFixedRate(regularHeartbeatInterval, regularHeartbeatInterval)(heartbeatRunnable)
    // Intentionally discard the cancellables for the scheduled heartbeats.
    // In future these could be retained if desired.
    ()
  }

}

object ImproveQuine {

  val runUpIntervals: List[FiniteDuration] = List(
    15.minutes,
    1.hours,
    3.hours,
    6.hours,
    12.hours,
  )
  val regularHeartbeatInterval: FiniteDuration = 1.day

  /** Type for the category of a telemetry event */
  sealed abstract class Event(val slug: String)

  /** Telemetry event when the application first starts */
  private case object InstanceStarted extends Event("instance.started")

  /** Telemetry event sent during a regular interval */
  private case object InstanceHeartbeat extends Event("instance.heartbeat")

  private case class RecipeInfo(recipe_name_hash: String, recipe_contents_hash: String)

  private case class TelemetryData(
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

  private val eventUri: Uri = Uri("https://improve.quine.io/event")
  private case class TelemetryRequest(
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

  private def unrollCypherOutput(output: StandingQueryResultOutputUserDef): List[StandingQueryResultOutputUserDef] =
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
