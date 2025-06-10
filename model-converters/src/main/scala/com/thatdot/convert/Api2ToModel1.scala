package com.thatdot.convert

import com.thatdot.api
import com.thatdot.quine.{routes => V1}

object Api2ToModel1 {

  def apply(rates: api.v2.RatesSummary): V1.RatesSummary = V1.RatesSummary(
    count = rates.count,
    oneMinute = rates.oneMinute,
    fiveMinute = rates.fiveMinute,
    fifteenMinute = rates.fifteenMinute,
    overall = rates.overall,
  )

  def apply(c: api.v2.AwsCredentials): V1.AwsCredentials = V1.AwsCredentials(
    accessKeyId = c.accessKeyId,
    secretAccessKey = c.secretAccessKey,
  )

  def apply(r: api.v2.AwsRegion): V1.AwsRegion = V1.AwsRegion(r.region)
}
