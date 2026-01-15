package com.thatdot.quine.app.v2api.definitions.query.standing

import java.time.Instant

import io.circe.generic.extras.semiauto.{deriveConfiguredDecoder, deriveConfiguredEncoder}
import io.circe.{Decoder, Encoder}
import sttp.tapir.Schema
import sttp.tapir.Schema.annotations.{description, title}

import com.thatdot.api.v2.RatesSummary
import com.thatdot.api.v2.TypeDiscriminatorConfig.instances.circeConfig
import com.thatdot.api.v2.codec.ThirdPartyCodecs.jdk.{instantDecoder, instantEncoder}

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

  implicit val encoder: Encoder[StandingQueryStats] = deriveConfiguredEncoder
  implicit val decoder: Decoder[StandingQueryStats] = deriveConfiguredDecoder
  implicit lazy val schema: Schema[StandingQueryStats] = Schema.derived
}
