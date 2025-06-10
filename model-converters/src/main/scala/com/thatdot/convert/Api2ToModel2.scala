package com.thatdot.convert

import com.thatdot.{api, aws, model}

/** Conversions from API models that are not otherwise scoped to a
  *  sub-package of [[com.thatdot.model.v2]] to
  *  corresponding internal models.
  */
object Api2ToModel2 {

  def apply(rates: api.v2.RatesSummary): model.v2.RatesSummary = model.v2.RatesSummary(
    count = rates.count,
    oneMinute = rates.oneMinute,
    fiveMinute = rates.fiveMinute,
    fifteenMinute = rates.fifteenMinute,
    overall = rates.overall,
  )

  def apply(c: api.v2.AwsCredentials): aws.model.AwsCredentials =
    aws.model.AwsCredentials(c.accessKeyId, c.secretAccessKey)

  def apply(r: api.v2.AwsRegion): aws.model.AwsRegion = aws.model.AwsRegion(r.region)

}
