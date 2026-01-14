package com.thatdot.api.v2

import io.circe.generic.extras.Configuration
import io.circe.generic.extras.semiauto.{deriveConfiguredDecoder, deriveConfiguredEncoder}
import io.circe.{Decoder, Encoder}
import sttp.tapir.Schema
import sttp.tapir.Schema.annotations.{description, title}

import com.thatdot.api.v2.schema.V2ApiConfiguration._

@title("Rates Summary")
@description("Summary statistics about a metered rate.")
final case class RatesSummary(
  @description("Number of items metered") count: Long,
  @description("Approximate rate per second in the last minute") oneMinute: Double,
  @description("Approximate rate per second in the last five minutes") fiveMinute: Double,
  @description("Approximate rate per second in the last fifteen minutes") fifteenMinute: Double,
  @description("Approximate rate per second since the meter was started") overall: Double,
)

object RatesSummary {
  implicit val circeConfig: Configuration = typeDiscriminatorConfig.asCirce
  implicit val encoder: Encoder[RatesSummary] = deriveConfiguredEncoder
  implicit val decoder: Decoder[RatesSummary] = deriveConfiguredDecoder
  implicit lazy val schema: Schema[RatesSummary] = Schema.derived[RatesSummary]
}
