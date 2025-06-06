package com.thatdot.quine.app.model

final case class RatesSummary(
  count: Long,
  oneMinute: Double,
  fiveMinute: Double,
  fifteenMinute: Double,
  overall: Double,
)
