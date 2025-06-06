package com.thatdot.quine.app.v2api.converters

import com.thatdot.quine.app.v2api.{definitions => Api}
import com.thatdot.quine.app.{model => Internal}
import com.thatdot.quine.{routes => V1}

/** Conversions from internal models that are not otherwise scoped to a
  *  sub-package of [[com.thatdot.quine.app.model]] to  corresponding
  *  API models.
  */
object InternalToApi {

  def apply(rates: Internal.RatesSummary): Api.RatesSummary = Api.RatesSummary(
    count = rates.count,
    oneMinute = rates.oneMinute,
    fiveMinute = rates.fiveMinute,
    fifteenMinute = rates.fifteenMinute,
    overall = rates.overall,
  )

  // Lingering V1 interop for Ingest V2
  def fromV1(rates: V1.RatesSummary): Api.RatesSummary = Api.RatesSummary(
    count = rates.count,
    oneMinute = rates.oneMinute,
    fiveMinute = rates.fiveMinute,
    fifteenMinute = rates.fifteenMinute,
    overall = rates.overall,
  )

  def apply(c: Internal.AwsCredentials): Api.AwsCredentials = Api.AwsCredentials(c.accessKeyId, c.secretAccessKey)

  // Lingering V1 interop for Ingest V2
  def fromV1(c: V1.AwsCredentials): Api.AwsCredentials = Api.AwsCredentials(
    accessKeyId = c.accessKeyId,
    secretAccessKey = c.secretAccessKey,
  )

  def apply(r: Internal.AwsRegion): Api.AwsRegion = Api.AwsRegion(r.region)

  // Lingering V1 interop for Ingest V2
  def fromV1(r: V1.AwsRegion): Api.AwsRegion = Api.AwsRegion(r.region)
}
