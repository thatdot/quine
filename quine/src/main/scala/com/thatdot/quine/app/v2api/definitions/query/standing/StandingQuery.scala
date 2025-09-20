package com.thatdot.quine.app.v2api.definitions.query.standing

import java.util.UUID

import sttp.tapir.Schema.annotations.{default, description, title}

object StandingQuery {

  @title("Standing Query")
  @description("Standing Query.")
  final case class StandingQueryDefinition(
    @description("Unique name for this Standing Query.")
    name: String,
    pattern: StandingQueryPattern,
    // Cannot get `@default` to work here, despite a working example in `DestinationSteps.Kafka#kafkaProperties`.
    @description(
      s"""${StandingQueryResultWorkflow.apiTitle}s as named outputs. Defaults to an empty list (`[]`).
         |The values are each:
         |${StandingQueryResultWorkflow.apiDescription}""".stripMargin,
    )
    outputs: Seq[StandingQueryResultWorkflow] = Seq.empty,
    @description("Whether or not to include cancellations in the results of this query.")
    @default(false)
    includeCancellations: Boolean = false,
    @description("How many Standing Query results to buffer before backpressuring.")
    @default(32)
    /** @see [[com.thatdot.quine.graph.StandingQueryInfo.DefaultQueueBackpressureThreshold]] */
    inputBufferSize: Int = 32,
  )

  @title("Registered Standing Query")
  @description("Registered Standing Query.")
  final case class RegisteredStandingQuery(
    name: String,
    @description("Unique identifier for the query, generated when the query is registered.")
    internalId: UUID,
    @description("Query or pattern to answer in a standing fashion.")
    pattern: Option[StandingQueryPattern], // TODO: remove Option once we remove DGB SQs
    // Cannot get `@default` to work here, despite a working example in `DestinationSteps.Kafka#kafkaProperties`.
    @description(
      s"""${StandingQueryResultWorkflow.apiTitle}s as named outputs. Defaults to an empty list (`[]`).
         |The values are each:
         |${StandingQueryResultWorkflow.apiDescription}""".stripMargin,
    )
    outputs: Seq[StandingQueryResultWorkflow] = Seq.empty,
    @description("Whether or not to include cancellations in the results of this query.")
    includeCancellations: Boolean,
    @description("How many Standing Query results to buffer on each host before backpressuring.")
    inputBufferSize: Int,
    @description(s"Statistics on progress of running the Standing Query, per host - see ${StandingQueryStats.title}")
    stats: Map[String, StandingQueryStats],
  )

}
