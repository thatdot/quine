package com.thatdot.quine.app.v2api.definitions.query.standing

import sttp.tapir.Schema.annotations.{default, description, title}

import com.thatdot.api.v2.{AwsCredentials, AwsRegion}
import com.thatdot.quine.routes.OutputFormat // Will be irrelevant when this file is soon deleted

// Shall be deleted when Outputs V2 is used in API V2
/** Output sink for processing standing query results */
@title(StandingQueryResultOutputUserDef.title)
@description(
  """A destination to which StandingQueryResults should be routed.
    |
    |A StandingQueryResult is an object with 2 sub-objects: `meta` and `data`. The `meta` object consists of:
    | - a boolean `isPositiveMatch`
    |
    |On a positive match, the `data` object consists of the data returned by the Standing Query.
    |
    |For example, a StandingQueryResult may look like the following:
    |
    |```
    |{"meta": {"isPositiveMatch": true}, "data": {"strId(n)": "a0f93a88-ecc8-4bd5-b9ba-faa6e9c5f95d"}}
    |```
    |
    |While a cancellation of that result might look like the following:
    |
    |```
    |{"meta": {"isPositiveMatch": false}, "data": {}}
    |```
    |""".stripMargin,
)
sealed trait StandingQueryResultOutputUserDef {
  import com.thatdot.quine.app.v2api.definitions.query.standing.StandingQueryResultOutputUserDef.SequencedCypherQuery
  def sequence: List[SequencedCypherQuery]
}

object StandingQueryResultOutputUserDef {
  val title = "Standing Query Result Output"

  @title("POST to HTTP[S] Webhook")
  @description(
    "Makes an HTTP[S] POST for each result. For the format of the result, see \"Standing Query Result Output\".",
  )
  final case class PostToEndpoint(
    url: String,
    @default(8)
    parallelism: Int = 8,
    @default(false)
    onlyPositiveMatchData: Boolean = false,
    @description(
      "A list of Cypher Queries to be run sequentially and then fed into this output",
    )
    @default(List.empty)
    sequence: List[SequencedCypherQuery] = List.empty,
    @default(StandingQueryOutputStructure.WithMetadata())
    structure: StandingQueryOutputStructure = StandingQueryOutputStructure.WithMetadata(),
  ) extends StandingQueryResultOutputUserDef

  @title("Publish to Kafka Topic")
  @description(
    """Publishes a JSON record for each result to the provided Apache Kafka topic.
      |For the format of the result record, see "Standing Query Result Output".""".stripMargin,
  )
  final case class WriteToKafka(
    topic: String,
    bootstrapServers: String,
    @default(OutputFormat.JSON)
    format: OutputFormat = OutputFormat.JSON,
    @description(
      """Map of Kafka producer properties.
        |See <https://docs.confluent.io/platform/current/installation/configuration/producer-configs.html>""".stripMargin,
    )
    kafkaProperties: Map[String, String] = Map.empty[String, String],
    @description(
      "A list of Cypher Queries to be run sequentially and then fed into this output",
    )
    @default(List.empty)
    sequence: List[SequencedCypherQuery] = List.empty,
    @default(StandingQueryOutputStructure.WithMetadata())
    structure: StandingQueryOutputStructure = StandingQueryOutputStructure.WithMetadata(),
  ) extends StandingQueryResultOutputUserDef

  @title("Publish to Kinesis Data Stream")
  @description(
    """Publishes a JSON record for each result to the provided Kinesis stream.
      |For the format of the result record, see "StandingQueryResult".""".stripMargin,
  )
  final case class WriteToKinesis(
    credentials: Option[AwsCredentials],
    region: Option[AwsRegion],
    streamName: String,
    @default(OutputFormat.JSON)
    format: OutputFormat = OutputFormat.JSON,
    kinesisParallelism: Option[Int],
    kinesisMaxBatchSize: Option[Int],
    kinesisMaxRecordsPerSecond: Option[Int],
    kinesisMaxBytesPerSecond: Option[Int],
    @description("A list of Cypher Queries to be run sequentially and then fed into this output")
    @default(List.empty)
    sequence: List[SequencedCypherQuery] = List.empty,
    @default(StandingQueryOutputStructure.WithMetadata())
    structure: StandingQueryOutputStructure = StandingQueryOutputStructure.WithMetadata(),
  ) extends StandingQueryResultOutputUserDef

  @title("Publish to SNS Topic")
  @description(
    """Publishes an AWS SNS record to the provided topic containing JSON for each result.
      |For the format of the result, see "Standing Query Result Output".
      |
      |**Double check your credentials and topic ARN.** If writing to SNS fails, the write will
      |be retried indefinitely. If the error is unfixable (eg, the topic or credentials
      |cannot be found), the outputs will never be emitted and the Standing Query this output
      |is attached to may stop running.""".stripMargin,
  )
  final case class WriteToSNS(
    credentials: Option[AwsCredentials],
    region: Option[AwsRegion],
    @description("ARN of the topic to publish to")
    topic: String,
    @description("A list of Cypher Queries to be run sequentially and then fed into this output")
    @default(List.empty)
    sequence: List[SequencedCypherQuery] = List.empty,
    @default(StandingQueryOutputStructure.WithMetadata())
    structure: StandingQueryOutputStructure = StandingQueryOutputStructure.WithMetadata(),
  ) extends StandingQueryResultOutputUserDef

  @title("Log JSON to Console")
  @description("Prints each result as a single-line JSON object to stdout on the Quine server.")
  final case class PrintToStandardOut(
    @default(PrintToStandardOut.LogLevel.Info)
    logLevel: PrintToStandardOut.LogLevel = PrintToStandardOut.LogLevel.Info,
    @default(PrintToStandardOut.LogMode.Complete)
    logMode: PrintToStandardOut.LogMode = PrintToStandardOut.LogMode.Complete,
    @description("A list of Cypher Queries to be run sequentially and then fed into this output")
    @default(List.empty)
    sequence: List[SequencedCypherQuery] = List.empty,
    @default(StandingQueryOutputStructure.WithMetadata())
    structure: StandingQueryOutputStructure = StandingQueryOutputStructure.WithMetadata(),
  ) extends StandingQueryResultOutputUserDef

  object PrintToStandardOut {

    /** @see [[StandingQuerySchemas.logModeSchema]]
      */
    sealed abstract class LogMode

    object LogMode {
      case object Complete extends LogMode
      case object FastSampling extends LogMode

      val modes: Seq[LogMode] = Vector(Complete, FastSampling)
    }

    sealed abstract class LogLevel
    object LogLevel {
      case object Trace extends LogLevel
      case object Debug extends LogLevel
      case object Info extends LogLevel
      case object Warn extends LogLevel
      case object Error extends LogLevel

      val levels: Seq[LogLevel] = Vector(Trace, Debug, Info, Warn, Error)
    }
  }

  @title("Log JSON to File")
  @description(
    """Writes each result as a single-line JSON record.
      |For the format of the result, see "Standing Query Result Output".""".stripMargin,
  )
  final case class WriteToFile(
    path: String,
    @description("A list of Cypher Queries to be run sequentially and then fed into this output")
    @default(List.empty)
    sequence: List[SequencedCypherQuery] = List.empty,
    @default(StandingQueryOutputStructure.WithMetadata())
    structure: StandingQueryOutputStructure = StandingQueryOutputStructure.WithMetadata(),
  ) extends StandingQueryResultOutputUserDef

  @title("Publish to Slack Webhook")
  @description(
    "Sends a message to Slack via a configured webhook URL. See <https://api.slack.com/messaging/webhooks>.",
  )
  final case class PostToSlack(
    hookUrl: String,
    @default(false)
    onlyPositiveMatchData: Boolean = false,
    @description("Number of seconds to wait between messages; minimum 1")
    @default(20)
    intervalSeconds: Int = 20,
    @description("A list of Cypher Queries to be run sequentially and then fed into this output")
    @default(List.empty)
    sequence: List[SequencedCypherQuery] = List.empty,
  ) extends StandingQueryResultOutputUserDef

  @title("Broadcast to Reactive Stream")
  @description(
    """Creates a 1 to many reactive stream output that other thatDot products can subscribe to.
      |Warning: Reactive Stream outputs do not function correctly when running in a cluster.""".stripMargin,
  )
  final case class ReactiveStream(
    @description("The address to bind the reactive stream server on")
    @default("localhost")
    address: String = "localhost",
    @description("The port to bind the reactive stream server on")
    port: Int,
    @description("A list of Cypher Queries to be run sequentially and then fed into this output")
    @default(List.empty)
    sequence: List[SequencedCypherQuery] = List.empty,
  ) extends StandingQueryResultOutputUserDef

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
    @description("Cypher query to execute on standing query result")
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
    @description("A list of Cypher Queries to be run sequentially and then fed into this output")
    @default(List.empty)
    sequence: List[SequencedCypherQuery] = List.empty,
    @default(StandingQueryOutputStructure.WithMetadata())
    structure: StandingQueryOutputStructure = StandingQueryOutputStructure.WithMetadata(),
  ) extends StandingQueryResultOutputUserDef

  //Separate from CypherQuery to prevent recursive structures in the API
  final case class SequencedCypherQuery(
    @description("Cypher query to execute on standing query result")
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
  )

  @title("Drop")
  final case class Drop(
    @description("A list of Cypher Queries to be run sequentially and then fed into this output")
    @default(List.empty)
    sequence: List[SequencedCypherQuery] = List.empty,
  ) extends StandingQueryResultOutputUserDef
}
