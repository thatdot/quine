package com.thatdot.api.v2

import io.circe.generic.extras.semiauto.{deriveConfiguredDecoder, deriveConfiguredEncoder}
import io.circe.{Decoder, Encoder}
import sttp.tapir.Schema
import sttp.tapir.Schema.annotations.{description, encodedExample, title}

import com.thatdot.api.v2.TypeDiscriminatorConfig.instances.circeConfig

@title("AWS Region")
@description(
  "AWS region code. e.g. `us-west-2`. If not provided, defaults according to the default AWS region provider chain. See: <https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/region-selection.html#automatically-determine-the-aws-region-from-the-environment>.",
)
final case class AwsRegion(
  @encodedExample("us-west-2")
  region: String,
)

object AwsRegion {
  implicit val encoder: Encoder[AwsRegion] = deriveConfiguredEncoder
  implicit val decoder: Decoder[AwsRegion] = deriveConfiguredDecoder
  implicit lazy val schema: Schema[AwsRegion] = Schema.derived
}
