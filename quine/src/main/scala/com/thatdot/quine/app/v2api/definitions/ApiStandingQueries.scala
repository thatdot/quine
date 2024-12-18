package com.thatdot.quine.app.v2api.definitions

import java.time.Instant
import java.util.UUID

import sttp.tapir.Schema.annotations.{description, title}

import com.thatdot.quine.app.v2api.definitions.ApiIngest.{AwsCredentials, AwsRegion, RatesSummary}
import com.thatdot.quine.app.v2api.definitions.ApiStandingQueries.StandingQueryResultOutputUserDef.SequencedCypherQuery

object ApiStandingQueries {

  @title("Structure of Output Data")
  sealed trait StandingQueryOutputStructure
  object StandingQueryOutputStructure {

    @title("WithMetadata")
    @description(
      "Output the result wrapped in an object with a field for the metadata and a field for the query result",
    )
    final case class WithMetadata() extends StandingQueryOutputStructure

    @title("Bare")
    @description(
      "Output the result as is with no metadata. Warning: if this is used with `includeCancellations=true`" +
      "then there will be no way to determine the difference between positive and negative matches",
    )
    final case class Bare() extends StandingQueryOutputStructure
  }
  @title("Standing Query")
  @description("Standing Query")
  final case class StandingQueryDefinition(
    pattern: StandingQueryPattern,
    @description(
      s"A map of named standing query outs - see the ${StandingQueryResultOutputUserDef.title} schema for the values",
    )
    outputs: Map[String, StandingQueryResultOutputUserDef],
    @description("Whether or not to include cancellations in the results of this query")
    includeCancellations: Boolean = false,
    @description("how many standing query results to buffer before backpressuring")
    inputBufferSize: Int = 32, // should match [[StandingQuery.DefaultQueueBackpressureThreshold]]
    @description("For debug and test only")
    shouldCalculateResultHashCode: Boolean = false,
  )

  @title("Standing Query Result Output Format")
  sealed trait OutputFormat

  object OutputFormat {
    @title("JSON")
    case object JSON extends OutputFormat
    @title("Protobuf")
    final case class Protobuf(
      @description(
        "URL (or local filename) of the Protobuf .desc file to load that contains the desired typeName to serialize to",
      ) schemaUrl: String,
      @description(
        "message type name to use (from the given .desc file) as the message type",
      ) typeName: String,
    ) extends OutputFormat
  }

  @title(com.thatdot.quine.routes.StandingQueryStats.title)
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
      parallelism: Int = 8,
      onlyPositiveMatchData: Boolean = false,
      @description(
        "A list of Cypher Queries to be run sequentially and then fed into this output",
      )
      sequence: List[SequencedCypherQuery] = List.empty,
      structure: StandingQueryOutputStructure = StandingQueryOutputStructure.WithMetadata(),
    ) extends StandingQueryResultOutputUserDef

    @title("Publish to Kafka Topic")
    @description(
      "Publishes a JSON record for each result to the provided Apache Kafka topic. For the format of the result record, see \"Standing Query Result Output\".",
    )
    final case class WriteToKafka(
      topic: String,
      bootstrapServers: String,
      format: OutputFormat = OutputFormat.JSON,
      @description(
        "Map of Kafka producer properties. See <https://docs.confluent.io/platform/current/installation/configuration/producer-configs.html>",
      )
      kafkaProperties: Map[String, String] = Map.empty[String, String],
      @description(
        "A list of Cypher Queries to be run sequentially and then fed into this output",
      )
      sequence: List[SequencedCypherQuery] = List.empty,
      structure: StandingQueryOutputStructure = StandingQueryOutputStructure.WithMetadata(),
    ) extends StandingQueryResultOutputUserDef

    @title("Publish to Kinesis Data Stream")
    @description(
      "Publishes a JSON record for each result to the provided Kinesis stream. For the format of the result record, see \"StandingQueryResult\".",
    )
    final case class WriteToKinesis(
      credentials: Option[AwsCredentials],
      region: Option[AwsRegion],
      streamName: String,
      format: OutputFormat = OutputFormat.JSON,
      kinesisParallelism: Option[Int],
      kinesisMaxBatchSize: Option[Int],
      kinesisMaxRecordsPerSecond: Option[Int],
      kinesisMaxBytesPerSecond: Option[Int],
      @description("A list of Cypher Queries to be run sequentially and then fed into this output")
      sequence: List[SequencedCypherQuery] = List.empty,
      structure: StandingQueryOutputStructure = StandingQueryOutputStructure.WithMetadata(),
    ) extends StandingQueryResultOutputUserDef

    @title("Publish to SNS Topic")
    @description(
      text = """|Publishes an AWS SNS record to the provided topic containing JSON for each result.
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
      @description("ARN of the topic to publish to") topic: String,
      @description("A list of Cypher Queries to be run sequentially and then fed into this output")
      sequence: List[SequencedCypherQuery] = List.empty,
      structure: StandingQueryOutputStructure = StandingQueryOutputStructure.WithMetadata(),
    ) extends StandingQueryResultOutputUserDef

    @title("Log JSON to Console")
    @description("Prints each result as a single-line JSON object to stdout on the Quine server.")
    final case class PrintToStandardOut(
      logLevel: PrintToStandardOut.LogLevel = PrintToStandardOut.LogLevel.Info,
      logMode: PrintToStandardOut.LogMode = PrintToStandardOut.LogMode.Complete,
      @description("A list of Cypher Queries to be run sequentially and then fed into this output")
      sequence: List[SequencedCypherQuery] = List.empty,
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
      "Writes each result as a single-line JSON record. For the format of the result, see \"Standing Query Result Output\".",
    )
    final case class WriteToFile(
      path: String,
      @description("A list of Cypher Queries to be run sequentially and then fed into this output")
      sequence: List[SequencedCypherQuery] = List.empty,
      structure: StandingQueryOutputStructure = StandingQueryOutputStructure.WithMetadata(),
    ) extends StandingQueryResultOutputUserDef

    @title("Publish to Slack Webhook")
    @description(
      "Sends a message to Slack via a configured webhook URL. See <https://api.slack.com/messaging/webhooks>.",
    )
    final case class PostToSlack(
      hookUrl: String,
      onlyPositiveMatchData: Boolean = false,
      @description("Number of seconds to wait between messages; minimum 1") intervalSeconds: Int = 20,
      @description("A list of Cypher Queries to be run sequentially and then fed into this output")
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
      "For each result, assigns the result as `parameter` and runs `query`, running at most `parallelism` queries simultaneously.",
    )
    final case class CypherQuery(
      @description("Cypher query to execute on standing query result") query: String,
      @description("Name of the Cypher parameter holding the standing query result") parameter: String = "that",
      @description("maximum number of standing query results being processed at once")
      parallelism: Int = com.thatdot.quine.routes.IngestRoutes.defaultWriteParallelism,
      @description(
        """Send the result of the Cypher query to another standing query output (in order to provide chained
                                    |transformation and actions). The data returned by this query will be passed as the `data` object
                                    |of the new StandingQueryResult (see \"Standing Query Result Output\")""".stripMargin
          .replace('\n', ' '),
      )
      @description(
        "To prevent unintentional resource use, if the Cypher query possibly contains an all node scan, then this parameter must be true",
      )
      allowAllNodeScan: Boolean = false,
      @description(
        """Whether queries that raise a potentially-recoverable error should be retried. If set to true (the default),
                                    |such errors will be retried until they succeed. Additionally, if the query is not idempotent, the query's
                                    |effects may occur multiple times in the case of external system failure. Query idempotency
                                    |can be checked with the EXPLAIN keyword. If set to false, results and effects will not be duplicated,
                                    |but may be dropped in the case of external system failure""".stripMargin
          .replace('\n', ' '),
      )
      shouldRetry: Boolean = true,
      @description("A list of Cypher Queries to be run sequentially and then fed into this output")
      sequence: List[SequencedCypherQuery] = List.empty,
      structure: StandingQueryOutputStructure = StandingQueryOutputStructure.WithMetadata(),
    ) extends StandingQueryResultOutputUserDef

    //Separate from CypherQuery to prevent recursive structures in the API
    final case class SequencedCypherQuery(
      @description("Cypher query to execute on standing query result") query: String,
      @description("Name of the Cypher parameter holding the standing query result") parameter: String = "that",
      @description("maximum number of standing query results being processed at once")
      parallelism: Int = com.thatdot.quine.routes.IngestRoutes.defaultWriteParallelism,
      @description(
        """Send the result of the Cypher query to another standing query output (in order to provide chained
                                    |transformation and actions). The data returned by this query will be passed as the `data` object
                                    |of the new StandingQueryResult (see \"Standing Query Result Output\")""".stripMargin
          .replace('\n', ' '),
      )
      @description(
        "To prevent unintentional resource use, if the Cypher query possibly contains an all node scan, then this parameter must be true",
      )
      allowAllNodeScan: Boolean = false,
      @description(
        """Whether queries that raise a potentially-recoverable error should be retried. If set to true (the default),
                                    |such errors will be retried until they succeed. Additionally, if the query is not idempotent, the query's
                                    |effects may occur multiple times in the case of external system failure. Query idempotency
                                    |can be checked with the EXPLAIN keyword. If set to false, results and effects will not be duplicated,
                                    |but may be dropped in the case of external system failure""".stripMargin
          .replace('\n', ' '),
      )
      shouldRetry: Boolean = true,
    )

    @title("Drop")
    final case class Drop(
      @description("A list of Cypher Queries to be run sequentially and then fed into this output")
      sequence: List[SequencedCypherQuery] = List.empty,
    ) extends StandingQueryResultOutputUserDef
  }

  @title("Standing Query Pattern")
  @description("A declarative structural graph pattern.")
  sealed abstract class StandingQueryPattern
  object StandingQueryPattern {

    @title("Cypher")
    final case class Cypher(
      @description("""Cypher query describing the standing query pattern. This must take the form of
                                   |MATCH <pattern> WHERE <condition> RETURN <columns>. When the `mode` is `DistinctId`,
                                   |the `RETURN` must also be `DISTINCT`.""".stripMargin)
      query: String,
      mode: StandingQueryMode = StandingQueryMode.DistinctId,
    ) extends StandingQueryPattern

    sealed abstract class StandingQueryMode
    object StandingQueryMode {
      // DomainGraphBranch interpreter
      case object DistinctId extends StandingQueryMode
      // SQv4/Cypher interpreter
      case object MultipleValues extends StandingQueryMode

      // QuinePattern engine
      case object QuinePattern extends StandingQueryMode

      val values: Seq[StandingQueryMode] = Seq(DistinctId, MultipleValues, QuinePattern)
    }
  }

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
    @description(
      s"Statistics on progress of running the standing query, per host - see ${com.thatdot.quine.routes.StandingQueryStats.title}",
    )
    stats: Map[String, StandingQueryStats],
  )

}
