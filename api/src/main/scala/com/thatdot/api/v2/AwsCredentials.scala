package com.thatdot.api.v2

import io.circe.generic.extras.Configuration
import io.circe.generic.extras.semiauto.{deriveConfiguredDecoder, deriveConfiguredEncoder}
import io.circe.{Decoder, Encoder}
import sttp.tapir.Schema
import sttp.tapir.Schema.annotations.{description, encodedExample, title}

import com.thatdot.api.v2.schema.V2ApiConfiguration._

@title("AWS Credentials")
@description(
  "Explicit AWS access key and secret to use. If not provided, defaults to environmental credentials according to the default AWS credential chain. See: <https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html#credentials-default>.",
)
final case class AwsCredentials(
  @encodedExample("ATIAXNKBTSB57V2QF11X")
  accessKeyId: String,
  @encodedExample("MDwbQe5XT4uOA3jQB/FhPaZpJdFkW13ryAL29bAk")
  secretAccessKey: String,
)

object AwsCredentials {
  implicit val circeConfig: Configuration = typeDiscriminatorConfig.asCirce
  implicit val encoder: Encoder[AwsCredentials] = deriveConfiguredEncoder
  implicit val decoder: Decoder[AwsCredentials] = deriveConfiguredDecoder
  implicit lazy val schema: Schema[AwsCredentials] = Schema.derived
}
