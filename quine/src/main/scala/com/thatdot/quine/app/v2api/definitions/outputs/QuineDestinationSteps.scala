package com.thatdot.quine.app.v2api.definitions.outputs

import sttp.tapir.Schema.annotations.{default, description, encodedExample, title}

@title("Destination Steps")
@description("Steps that transform results on their way to a destination.")
sealed trait QuineDestinationSteps

object QuineDestinationSteps {
  import com.thatdot.quine.app.util.StringOps.syntax._

  /** @param query what to execute for every standing query result or other provided data
    * @param parameter name of the parameter associated with SQ results
    * @param parallelism how many queries to run at once
    * @param allowAllNodeScan to prevent unintentional resource use, if the Cypher query possibly contains an all node scan, then this parameter must be true
    */
  @title("Run Cypher Query")
  @description(
    """Runs the `query`, where the given `parameter` is used to reference the data that is passed in.
      |Runs at most `parallelism` queries simultaneously.""".asOneLine,
  )
  final case class CypherQuery(
    @description(CypherQuery.queryDescription)
    @encodedExample(CypherQuery.exampleQuery)
    query: String,
    @description("Name of the Cypher parameter to assign incoming data to.")
    @default("that")
    parameter: String = "that",
    @description("Maximum number of queries of this kind allowed to run at once.")
    @default(com.thatdot.quine.routes.IngestRoutes.defaultWriteParallelism)
    parallelism: Int = com.thatdot.quine.routes.IngestRoutes.defaultWriteParallelism,
    @description(
      """To prevent unintentional resource use, if the Cypher query may contain an all-node scan,
        |this parameter must be `true`.""".asOneLine,
    )
    @default(false)
    allowAllNodeScan: Boolean = false,
    @description(
      """Whether queries that raise a potentially-recoverable error should be retried. If set to `true` (the default),
        |such errors will be retried until they succeed. ⚠️ Note that if the query is not idempotent, the query's
        |effects may occur multiple times in the case of external system failure. Query idempotency
        |can be checked with the EXPLAIN keyword. If set to `false`, results and effects will not be duplicated,
        |but may be dropped in the case of external system failure""".asOneLine,
    )
    @default(true)
    shouldRetry: Boolean = true,
  ) extends QuineDestinationSteps

  object CypherQuery {
    val queryDescription: String = "Cypher query to execute on Standing Query result"
    val exampleQuery: String = "MATCH (n) WHERE $that.id = n.id RETURN (n)"
  }

  @title("Publish to Slack Webhook")
  @description(
    "Sends a message to Slack via a configured webhook URL. See <https://api.slack.com/messaging/webhooks>.",
  )
  final case class Slack(
    @encodedExample("https://hooks.slack.com/services/T00000000/B00000000/XXXXXXXXXXXXXXXXXXXXXXXX")
    hookUrl: String,
    @default(false)
    onlyPositiveMatchData: Boolean = false,
    @description("Number of seconds to wait between messages; minimum 1.")
    @default(20)
    intervalSeconds: Int = 20,
  ) extends QuineDestinationSteps
}
