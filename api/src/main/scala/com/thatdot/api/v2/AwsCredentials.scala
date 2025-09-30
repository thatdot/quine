package com.thatdot.api.v2

import org.polyvariant.sttp.oauth2.Secret
import sttp.tapir.Schema.annotations.{description, encodedExample, title}

@title("AWS Credentials")
@description(
  "Explicit AWS access key and secret to use. If not provided, defaults to environmental credentials according to the default AWS credential chain. See: <https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html#credentials-default>.",
)
final case class AwsCredentials(
  @encodedExample("ATIAXNKBTSB57V2QF11X")
  accessKeyId: String,
  @encodedExample("MDwbQe5XT4uOA3jQB/FhPaZpJdFkW13ryAL29bAk")
  secretAccessKey: Secret[String],
)
