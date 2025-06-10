package com.thatdot.api.v2

import sttp.tapir.Schema.annotations.{description, title}

@title("AWS Credentials")
@description(
  "Explicit AWS access key and secret to use. If not provided, defaults to environmental credentials according to the default AWS credential chain. See: <https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html#credentials-default>.",
)
final case class AwsCredentials(accessKeyId: String, secretAccessKey: String)
