package com.thatdot.quine.app.model.outputs2.query.standing

import java.time.Instant

import com.thatdot.quine.app.model.RatesSummary

final case class StandingQueryStats(
  rates: RatesSummary,
  startTime: Instant,
  totalRuntime: Long,
  bufferSize: Int,
  outputHashCode: Long,
)

object StandingQueryStats {
  val title: String = "Statistics About a Running Standing Query"
}
