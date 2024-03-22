package com.thatdot.quine.app

import java.net.NetworkInterface
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.time.format.DateTimeFormatter
import java.util.Base64

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._
import scala.util.{Success, Try}

import org.apache.pekko.actor.{ActorSystem, Scheduler}
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.model.{HttpEntity, HttpMethods, HttpRequest, Uri}
import org.apache.pekko.pattern.retry

import com.typesafe.scalalogging.StrictLogging
import io.circe.syntax.EncoderOps

object ImproveQuine {

  val eventUri: Uri = Uri("https://improve.quine.io/event")
  case class TelemetryRequest(
    service: String,
    version: String,
    hostHash: String,
    extraArgs: Map[String, String]
  )(implicit system: ActorSystem)
      extends StrictLogging {
    implicit private val executionContext: ExecutionContext = system.dispatcher
    implicit private val scheduler: Scheduler = system.scheduler

    def run(): Future[Unit] = {
      val now = java.time.OffsetDateTime.now().format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)

      val telemetryData = Map[String, String](
        "service" -> service,
        "version" -> version,
        "host_hash" -> hostHash,
        "event" -> "instance.started",
        "time" -> now
      ) ++ extraArgs

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

  def reportTelemetry(
    service: String,
    quineVersion: String,
    /* extra values some callers may wish to add to telemetry.*/
    extraArgs: Map[String, String] = Map.empty[String, String]
  )(implicit system: ActorSystem): Future[Unit] =
    TelemetryRequest(service, quineVersion, hostHash(), extraArgs).run()

}
