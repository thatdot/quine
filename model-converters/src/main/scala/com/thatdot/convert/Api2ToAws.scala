package com.thatdot.convert

import com.thatdot.{api, aws}

/** Conversions from values in the API2 model to the corresponding values in the internal AWS model. */
object Api2ToAws {
  def apply(c: api.v2.AwsCredentials): aws.model.AwsCredentials =
    aws.model.AwsCredentials(c.accessKeyId, c.secretAccessKey)

  def apply(r: api.v2.AwsRegion): aws.model.AwsRegion = aws.model.AwsRegion(r.region)
}
