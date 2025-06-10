package com.thatdot.model.v2

final case class RatesSummary(
  count: Long,
  oneMinute: Double,
  fiveMinute: Double,
  fifteenMinute: Double,
  overall: Double,
)
