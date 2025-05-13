package com.thatdot.quine.app.v2api.definitions.query.standing

import java.util.UUID

import sttp.tapir.Schema.annotations.{default, description, title}

object StandingQuery {

  @title("Standing Query")
  @description("Standing Query")
  final case class StandingQueryDefinition(
    pattern: StandingQueryPattern,
    @description(
      s"A map of named standing query outs - see the ${StandingQueryResultOutputUserDef.title} schema for the values",
    )
    outputs: Map[String, StandingQueryResultOutputUserDef],
    @description("Whether or not to include cancellations in the results of this query")
    @default(false)
    includeCancellations: Boolean = false,
    @description("how many standing query results to buffer before backpressuring")
    @default(32)
    inputBufferSize: Int = 32, // should match [[StandingQuery.DefaultQueueBackpressureThreshold]]
  )

  @title("Registered Standing Query")
  @description("Registered Standing Query.")
  final case class RegisteredStandingQuery(
    name: String,
    @description("Unique identifier for the query, generated when the query is registered")
    internalId: UUID,
    @description("Query or pattern to answer in a standing fashion")
    pattern: Option[StandingQueryPattern], // TODO: remove Option once we remove DGB SQs
    @description(
      s"output sinks into which all new standing query results should be enqueued - see ${StandingQueryResultOutputUserDef.title}",
    )
    outputs: Map[String, StandingQueryResultOutputUserDef],
    @description("Whether or not to include cancellations in the results of this query")
    includeCancellations: Boolean,
    @description("how many standing query results to buffer on each host before backpressuring")
    inputBufferSize: Int,
    @description(s"Statistics on progress of running the standing query, per host - see ${StandingQueryStats.title}")
    stats: Map[String, StandingQueryStats],
  )

}
