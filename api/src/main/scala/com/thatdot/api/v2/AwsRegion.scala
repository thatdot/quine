package com.thatdot.api.v2

import io.circe.{Decoder, Encoder}
import sttp.tapir.Schema
import sttp.tapir.Schema.annotations.{description, encodedExample, title}

@title("AWS Region")
@description(
  "AWS region code. e.g. `us-west-2`. If not provided, defaults according to the default AWS region provider chain. See: <https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/region-selection.html#automatically-determine-the-aws-region-from-the-environment>.",
)
final case class AwsRegion(
  @encodedExample("us-west-2")
  region: String,
)

object AwsRegion {
  implicit val encoder: Encoder[AwsRegion] = Encoder.encodeString.contramap(_.region)
  implicit val decoder: Decoder[AwsRegion] = Decoder.decodeString.map(AwsRegion(_))
  implicit lazy val schema: Schema[AwsRegion] = Schema.string[AwsRegion]
}
