package com.thatdot.quine.app.v2api.definitions.query.standing

import java.time.Instant

import sttp.tapir.Schema.annotations.{description, title}

import com.thatdot.quine.app.v2api.definitions.RatesSummary

@title(StandingQueryStats.title)
final case class StandingQueryStats(
  @description("Results per second over different time periods")
  rates: RatesSummary,
  @description("Time (in ISO-8601 UTC time) when the standing query was started")
  startTime: Instant,
  @description("Time (in milliseconds) that that the standing query has been running")
  totalRuntime: Long,
  @description("How many standing query results are buffered and waiting to be emitted")
  bufferSize: Int,
  @description("Accumulated output hash code")
  outputHashCode: Long,
)

object StandingQueryStats {
  val title: String = "Statistics About a Running Standing Query"
}
