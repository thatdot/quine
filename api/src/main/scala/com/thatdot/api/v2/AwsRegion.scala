package com.thatdot.api.v2

import sttp.tapir.Schema.annotations.{description, title}

@title("AWS Region")
@description(
  "AWS region code. e.g. `us-west-2`. If not provided, defaults according to the default AWS region provider chain. See: <https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/region-selection.html#automatically-determine-the-aws-region-from-the-environment>.",
)
final case class AwsRegion(region: String)
