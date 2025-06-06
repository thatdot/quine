package com.thatdot.quine.app.model.outputs2.query.standing

import java.util.UUID

object StandingQuery {

  final case class StandingQueryDefinition(
    pattern: StandingQueryPattern,
    outputs: Map[String, StandingQueryResultWorkflow],
    includeCancellations: Boolean = false,
    inputBufferSize: Int = 32, // should match [[StandingQuery.DefaultQueueBackpressureThreshold]]
    shouldCalculateResultHashCode: Boolean = false,
  )

  final case class RegisteredStandingQuery(
    name: String,
    internalId: UUID,
    pattern: Option[StandingQueryPattern], // TODO: remove Option once we remove DGB SQs
    outputs: Map[String, StandingQueryResultWorkflow],
    includeCancellations: Boolean,
    inputBufferSize: Int,
    stats: Map[String, StandingQueryStats],
  )

}
