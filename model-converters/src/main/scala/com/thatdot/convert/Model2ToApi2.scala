package com.thatdot.convert

import com.thatdot.{api, aws, model}

/** Conversions from internal models that are not otherwise scoped to a
  *  sub-package of [[com.thatdot.model.v2]] to
  *  corresponding  API models.
  */
object Model2ToApi2 {

  def apply(rates: model.v2.RatesSummary): api.v2.RatesSummary = api.v2.RatesSummary(
    count = rates.count,
    oneMinute = rates.oneMinute,
    fiveMinute = rates.fiveMinute,
    fifteenMinute = rates.fifteenMinute,
    overall = rates.overall,
  )

  def apply(c: aws.model.AwsCredentials): api.v2.AwsCredentials =
    api.v2.AwsCredentials(c.accessKeyId, c.secretAccessKey)

  def apply(r: aws.model.AwsRegion): api.v2.AwsRegion = api.v2.AwsRegion(r.region)

}
