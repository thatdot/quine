package com.thatdot.quine.app.v2api.definitions.query.standing

import java.time.Instant

import io.circe.generic.extras.Configuration
import io.circe.generic.extras.semiauto.{deriveConfiguredDecoder, deriveConfiguredEncoder}
import io.circe.{Decoder, Encoder}
import sttp.tapir.Schema.annotations.{description, title}

import com.thatdot.api.v2.RatesSummary
import com.thatdot.api.v2.schema.V2ApiConfiguration._

@title(StandingQueryStats.title)
final case class StandingQueryStats(
  @description("Results per second over different time periods.")
  rates: RatesSummary,
  @description("Time (in ISO-8601 UTC time) when the Standing Query was started.")
  startTime: Instant,
  @description("Time (in milliseconds) that that the Standing Query has been running.")
  totalRuntime: Long,
  @description("How many Standing Query Results are buffered and waiting to be emitted.")
  bufferSize: Int,
  @description("Accumulated output hash code.")
  outputHashCode: Long,
)

object StandingQueryStats {
  val title: String = "Statistics About a Running Standing Query"

  implicit val circeConfig: Configuration = typeDiscriminatorConfig.asCirce
  implicit val instantEncoder: Encoder[Instant] = Encoder.encodeString.contramap(_.toString)
  implicit val instantDecoder: Decoder[Instant] = Decoder.decodeString.map(Instant.parse)
  implicit val encoder: Encoder[StandingQueryStats] = deriveConfiguredEncoder
  implicit val decoder: Decoder[StandingQueryStats] = deriveConfiguredDecoder
}
