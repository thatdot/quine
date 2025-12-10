package com.thatdot.api.v2

import io.circe.generic.semiauto._
import io.circe.{Decoder, Encoder}
import sttp.tapir.Schema.annotations.{description, title}

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
  implicit val encoder: Encoder[RatesSummary] = deriveEncoder
  implicit val decoder: Decoder[RatesSummary] = deriveDecoder
}
