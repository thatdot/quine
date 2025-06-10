package com.thatdot.quine.app.v2api.definitions.outputs

import sttp.tapir.Schema.annotations.{default, description, title}

@title("Destination Steps")
@description("Steps that transform results on their way to a destination.")
sealed trait QuineDestinationSteps

object QuineDestinationSteps {

  /** Each result is passed into a Cypher query as a parameter
    *
    * @param query what to execute for every standing query result
    * @param parameter name of the parameter associated with SQ results
    * @param parallelism how many queries to run at once
    * @param allowAllNodeScan to prevent unintentional resource use, if the Cypher query possibly contains an all node scan, then this parameter must be true
    */
  @title("Run Cypher Query")
  @description(
    """For each result, assigns the result as `parameter` and runs `query`,
      |running at most `parallelism` queries simultaneously.""".stripMargin,
  )
  final case class CypherQuery(
    @description(CypherQuery.queryDescription)
    query: String,
    @description("Name of the Cypher parameter holding the standing query result")
    @default("that")
    parameter: String = "that",
    @description("maximum number of standing query results being processed at once")
    @default(com.thatdot.quine.routes.IngestRoutes.defaultWriteParallelism)
    parallelism: Int = com.thatdot.quine.routes.IngestRoutes.defaultWriteParallelism,
    @description(
      """Send the result of the Cypher query to another standing query output (in order to provide chained
        |transformation and actions). The data returned by this query will be passed as the `data` object
        |of the new StandingQueryResult (see \"Standing Query Result Output\")""".stripMargin,
    )
    @description(
      """To prevent unintentional resource use, if the Cypher query possibly contains an all node scan,
        |then this parameter must be true""".stripMargin,
    )
    @default(false)
    allowAllNodeScan: Boolean = false,
    @description(
      """Whether queries that raise a potentially-recoverable error should be retried. If set to true (the default),
        |such errors will be retried until they succeed. Additionally, if the query is not idempotent, the query's
        |effects may occur multiple times in the case of external system failure. Query idempotency
        |can be checked with the EXPLAIN keyword. If set to false, results and effects will not be duplicated,
        |but may be dropped in the case of external system failure""".stripMargin,
    )
    @default(true)
    shouldRetry: Boolean = true,
  ) extends QuineDestinationSteps

  object CypherQuery {
    val queryDescription: String = "Cypher query to execute on standing query result"
  }

  @title("Publish to Slack Webhook")
  @description(
    "Sends a message to Slack via a configured webhook URL. See <https://api.slack.com/messaging/webhooks>.",
  )
  final case class Slack(
    hookUrl: String,
    @default(false)
    onlyPositiveMatchData: Boolean = false,
    @description("Number of seconds to wait between messages; minimum 1")
    @default(20)
    intervalSeconds: Int = 20,
  ) extends QuineDestinationSteps
}
