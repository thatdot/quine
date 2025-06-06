package com.thatdot.quine.app.v2api.converters

import com.thatdot.quine.app.v2api.{definitions => Api}
import com.thatdot.quine.app.{model => Internal}
import com.thatdot.quine.{routes => V1}

/** Conversions from API models that are not otherwise scoped to a
  *  sub-package of [[com.thatdot.quine.app.v2api.definitions]] to
  *  corresponding internal models.
  */
object ApiToInternal {

  def apply(rates: Api.RatesSummary): Internal.RatesSummary = Internal.RatesSummary(
    count = rates.count,
    oneMinute = rates.oneMinute,
    fiveMinute = rates.fiveMinute,
    fifteenMinute = rates.fifteenMinute,
    overall = rates.overall,
  )

  // Lingering V1 interop for Ingest V2
  def toV1(rates: Api.RatesSummary): V1.RatesSummary = V1.RatesSummary(
    count = rates.count,
    oneMinute = rates.oneMinute,
    fiveMinute = rates.fiveMinute,
    fifteenMinute = rates.fifteenMinute,
    overall = rates.overall,
  )

  def apply(c: Api.AwsCredentials): Internal.AwsCredentials = Internal.AwsCredentials(c.accessKeyId, c.secretAccessKey)

  // Lingering V1 interop for Ingest V2
  def toV1(c: Api.AwsCredentials): V1.AwsCredentials = V1.AwsCredentials(
    accessKeyId = c.accessKeyId,
    secretAccessKey = c.secretAccessKey,
  )

  def apply(r: Api.AwsRegion): Internal.AwsRegion = Internal.AwsRegion(r.region)

  // Lingering V1 interop for Ingest V2
  def toV1(r: Api.AwsRegion): V1.AwsRegion = V1.AwsRegion(r.region)

}
