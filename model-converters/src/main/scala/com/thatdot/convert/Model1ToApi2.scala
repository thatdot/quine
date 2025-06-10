package com.thatdot.convert

import com.thatdot.api
import com.thatdot.quine.{routes => V1}

object Model1ToApi2 {

  def apply(rates: V1.RatesSummary): api.v2.RatesSummary = api.v2.RatesSummary(
    count = rates.count,
    oneMinute = rates.oneMinute,
    fiveMinute = rates.fiveMinute,
    fifteenMinute = rates.fifteenMinute,
    overall = rates.overall,
  )

  def apply(c: V1.AwsCredentials): api.v2.AwsCredentials = api.v2.AwsCredentials(
    accessKeyId = c.accessKeyId,
    secretAccessKey = c.secretAccessKey,
  )

  def apply(r: V1.AwsRegion): api.v2.AwsRegion = api.v2.AwsRegion(r.region)
}
