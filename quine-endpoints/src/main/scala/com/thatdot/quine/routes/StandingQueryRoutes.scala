package com.thatdot.quine.routes

import java.time.Instant
import java.util.UUID

import endpoints4s.algebra.Tag
import endpoints4s.generic.{docs, title, unnamed}

@title("Standing Query")
@docs("""
A query which gets issued once and for which results will continue to be
produced as new data gets written into the system and new matches get created.
Compared to regular queries, they are less imperative and more declarative - it
doesn't matter as much in what order parts of the pattern match, only that the
composite structure exists.
""")
final case class StandingQueryDefinition(
  pattern: StandingQueryPattern,
  outputs: Map[String, StandingQueryResultOutputUserDef],
  @docs("whether or not to include cancellations in the results of this query")
  includeCancellations: Boolean = false,
  @docs("how many standing query results to buffer before backpressuring")
  inputBufferSize: Int = 32 // should match [[StandingQuery.DefaultQueueBackpressureThreshold]]
)

@title("Registered Standing Query")
@docs("A Standing Query which has been issued.")
final case class RegisteredStandingQuery(
  name: String,
  @docs("unique identifier for the query, generated when the query is registered")
  internalId: UUID,
  @docs("query or pattern to answer in a standing fashion")
  pattern: Option[StandingQueryPattern], // TODO: remove Option once we remove DGB SQs
  @docs("output sinks into which all new standing query results should be enqueued")
  outputs: Map[String, StandingQueryResultOutputUserDef],
  includeCancellations: Boolean,
  @docs("how many standing query results to buffer on each host before backpressuring")
  inputBufferSize: Int,
  @docs("statistics on progress of running the standing query, per host")
  stats: Map[String, StandingQueryStats]
)

@title("Standing Query Pattern")
@docs("A declarative structural graph pattern.")
sealed abstract class StandingQueryPattern
object StandingQueryPattern {
  @title("Graph")
  @unnamed()
  final case class Graph(
    @docs("nodes inside the graph pattern")
    nodes: Seq[NodePattern],
    @docs("edges inside the graph pattern")
    edges: Seq[EdgePattern],
    @docs("pattern ID of the standing node (must be the ID of a node in `nodes`)")
    startingPoint: Int,
    @docs("values to extract from the graph pattern")
    toExtract: Seq[ReturnColumn],
    mode: StandingQueryMode = StandingQueryMode.DistinctId
  ) extends StandingQueryPattern

  @title("Cypher")
  @unnamed()
  final case class Cypher(
    @docs("""Cypher query describing the standing query pattern. This must take the form of
        |MATCH <pattern> WHERE <condition> RETURN <columns>. When the `mode` is `DistinctId`,
        |the `RETURN` must also be `DISTINCT`.""".stripMargin)
    query: String,
    mode: StandingQueryMode = StandingQueryMode.DistinctId
  ) extends StandingQueryPattern

  sealed abstract class StandingQueryMode
  object StandingQueryMode {
    // DomainGraphBranch interpreter
    case object DistinctId extends StandingQueryMode
    // SQv4/Cypher interpreter
    case object MultipleValues extends StandingQueryMode

    val values: Seq[StandingQueryMode] = Seq(DistinctId, MultipleValues)
  }

  @title("Node Pattern")
  @unnamed()
  final case class NodePattern(
    @docs("pattern ID for a node (must be unique in the set of pattern IDs in the pattern's `nodes`)")
    patternId: Int,
    @docs("labels that should be on the node for it to match")
    labels: Set[String],
    @docs("properties that should be on the node for it to match")
    properties: Map[String, PropertyValuePattern]
  )

  @title("Property Value Pattern")
  @unnamed()
  sealed abstract class PropertyValuePattern
  object PropertyValuePattern {

    @unnamed()
    @title("Value")
    @docs("match the property if it has exactly this value")
    final case class Value(value: ujson.Value) extends PropertyValuePattern

    @title("Any Value Except")
    @docs("match the property if it has any value except this one")
    @unnamed()
    final case class AnyValueExcept(value: ujson.Value) extends PropertyValuePattern

    @title("Any Value")
    @docs("match the property no matter what value it has (provided it has _some_ value)")
    @unnamed()
    case object AnyValue extends PropertyValuePattern

    @title("No Value")
    @docs("match the property only if the property does not exist")
    @unnamed()
    case object NoValue extends PropertyValuePattern

    @title("String Value Matching Regex")
    @docs("match the property only if the property exists and is a string matching the provided (Java) Regex pattern")
    @unnamed()
    final case class RegexMatch(pattern: String) extends PropertyValuePattern
  }

  @title("Edge Pattern")
  @unnamed()
  final case class EdgePattern(
    @docs("pattern ID for the start vertex of the edge (must be the ID of a node in the pattern's `nodes`)")
    from: Int,
    @docs("pattern ID for the end vertex of the edge (must be the ID of a node in the pattern's `nodes`)")
    to: Int,
    @docs("whether the edge is directed")
    isDirected: Boolean,
    @docs("label on the edge")
    label: String
  )

  @title("Return Column")
  @unnamed()
  sealed abstract class ReturnColumn
  object ReturnColumn {

    @title("Id")
    @unnamed()
    final case class Id(
      @docs("pattern ID of the node whose actual ID is extracted")
      nodePatternId: Int,
      @docs("whether the ID should be formatted as with `strId` or with `id`")
      formatAsString: Boolean,
      @docs("name with which to associated the extracted ID")
      aliasedAs: String
    ) extends ReturnColumn

    @title("Property")
    @unnamed()
    final case class Property(
      @docs("pattern ID of the node on which to extract a property value")
      nodePatternId: Int,
      @docs("property key whose value is extracted")
      propertyKey: String,
      @docs("name with which to associated the extracted property value")
      aliasedAs: String
    ) extends ReturnColumn
  }
}

@title("Statistics About a Running Standing Query")
final case class StandingQueryStats(
  @docs("results per second over different time periods")
  rates: RatesSummary,
  @docs("time (in ISO-8601 UTC time) when the standing query was started")
  startTime: Instant,
  @docs("time (in milliseconds) that that the standing query has been running")
  totalRuntime: Long,
  @docs("how many standing query results are buffered and waiting to be emitted")
  bufferSize: Int
)

/** Confirmation of a standing query being registered
  *
  * @param name name of the registered standing query
  * @param output where will results be written
  */
final case class StandingQueryRegistered(
  name: String,
  output: StandingQueryResultOutputUserDef
)

/** Confirmation of a standing query being cancelled
  *
  * @param name name of the standing query that was cancelled
  * @param output where the results were being written
  */
final case class StandingQueryCancelled(
  name: String,
  output: StandingQueryResultOutputUserDef
)

/** Output sink for processing standing query results */
@title("Standing Query Result Output")
@docs(
  """A destination to which StandingQueryResults should be routed.
    |
    |A StandingQueryResult is an object with 2 sub-objects: `meta` and `data`. The `meta` object consists of:
    | - a UUID `resultId`
    | - a boolean `isPositiveMatch`
    |
    |On a positive match, the `data` object consists of the data returned by the Standing Query.
    |
    |For example, a StandingQueryResult may look like the following:
    |
    |```
    |{"meta": {"resultId": "b3c35fa4-2515-442c-8a6a-35a3cb0caf6b", "isPositiveMatch": true}, "data": {"strId(n)": "a0f93a88-ecc8-4bd5-b9ba-faa6e9c5f95d"}}
    |```
    |
    |While a cancellation of that result might look like the following:
    |
    |```
    |{"meta": {"resultId": "b3c35fa4-2515-442c-8a6a-35a3cb0caf6b", "isPositiveMatch": false}, "data": {}}
    |```
    |""".stripMargin
)
sealed abstract class StandingQueryResultOutputUserDef

object StandingQueryResultOutputUserDef {

  @unnamed
  @title("POST to HTTP[S] Webhook")
  @docs(
    "Makes an HTTP[S] POST for each result. For the format of the result, see \"Standing Query Result Output\"."
  )
  final case class PostToEndpoint(
    url: String,
    parallelism: Int = 8,
    onlyPositiveMatchData: Boolean = false
  ) extends StandingQueryResultOutputUserDef

  @unnamed
  @title("Publish to Kafka Topic")
  @docs(
    "Publishes a JSON record for each result to the provided Apache Kafka topic. For the format of the result record, see \"Standing Query Result Output\"."
  )
  final case class WriteToKafka(
    topic: String,
    bootstrapServers: String,
    format: OutputFormat = OutputFormat.JSON
  ) extends StandingQueryResultOutputUserDef

  @unnamed
  @title("Publish to Kinesis Stream")
  @docs(
    "Publishes a JSON record for each result to the provided Kinesis stream. For the format of the result record, see \"StandingQueryResult\"."
  )
  final case class WriteToKinesis(
    credentials: Option[AwsCredentials],
    streamName: String,
    format: OutputFormat = OutputFormat.JSON,
    kinesisParallelism: Option[Int],
    kinesisMaxBatchSize: Option[Int],
    kinesisMaxRecordsPerSecond: Option[Int],
    kinesisMaxBytesPerSecond: Option[Int]
  ) extends StandingQueryResultOutputUserDef

  @unnamed
  @title("Publish to SNS Topic")
  @docs(
    text = """|Publishes an AWS SNS record to the provided topic containing JSON for each result.
              |For the format of the result, see "Standing Query Result Output".
              |
              |**Double check your credentials and topic ARN.** If writing to SNS fails, the write will
              |be retried indefinitely. If the error is unfixable (eg, the topic or credentials
              |cannot be found), the outputs will never be emitted and the Standing Query this output
              |is attached to may stop running.""".stripMargin
  )
  final case class WriteToSNS(
    credentials: Option[AwsCredentials],
    @docs("ARN of the topic to publish to") topic: String
  ) extends StandingQueryResultOutputUserDef

  @unnamed
  @title("Log JSON to Console")
  @docs("Prints each result as a single-line JSON object to stdout on the Connect server.")
  final case class PrintToStandardOut(
    logLevel: PrintToStandardOut.LogLevel = PrintToStandardOut.LogLevel.Info,
    logMode: PrintToStandardOut.LogMode = PrintToStandardOut.LogMode.Complete
  ) extends StandingQueryResultOutputUserDef
  object PrintToStandardOut {

    /** @see [[StandingQuerySchemas.logModeSchema]]
      */
    @unnamed
    sealed abstract class LogMode

    object LogMode {
      case object Complete extends LogMode
      case object FastSampling extends LogMode

      val modes: Seq[LogMode] = Vector(Complete, FastSampling)
    }

    @unnamed
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

  @unnamed
  @title("Log JSON to File")
  @docs(
    "Writes each result as a single-line JSON record. For the format of the result, see \"Standing Query Result Output\"."
  )
  final case class WriteToFile(
    path: String
  ) extends StandingQueryResultOutputUserDef

  @unnamed
  @title("Publish to Slack Webhook")
  @docs(
    "Sends a message to Slack via a configured webhook URL. See <https://api.slack.com/messaging/webhooks>."
  )
  final case class PostToSlack(
    hookUrl: String,
    onlyPositiveMatchData: Boolean = false,
    @docs("number of seconds to wait between messages; minimum 1") intervalSeconds: Int = 20
  ) extends StandingQueryResultOutputUserDef

  /** Each result is passed into a Cypher query as a parameter
    *
    * @param query what to execute for every standing query result
    * @param parameter name of the parameter associated with SQ results
    * @param parallelism how many queries to run at once
    * @param andThen send the result of the Cypher query to another standing query output (in order to provide chained transformation and actions)
    * @param allowAllNodeScan to prevent unintentional resource use, if the Cypher query possibly contains an all node scan, then this parameter must be true
    *
    * TODO: consider what it would take to run the query on the node that matched
    */
  @unnamed
  @title("Run Cypher Query")
  @docs(
    "For each result, assigns the result as `parameter` and runs `query`, running at most `parallelism` queries simultaneously."
  )
  final case class CypherQuery(
    @docs("Cypher query to execute on standing query result") query: String,
    @docs("name of the Cypher parameter holding the standing query result") parameter: String = "that",
    @docs(
      "maximum number of standing query results being processed at once"
    )
    parallelism: Int = IngestRoutes.defaultWriteParallelism,
    @docs(
      """send the result of the Cypher query to another standing query output (in order to provide chained
        |transformation and actions). The data returned by this query will be passed as the `data` object
        |of the new StandingQueryResult (see \"Standing Query Result Output\")""".stripMargin.replace('\n', ' ')
    )
    andThen: Option[StandingQueryResultOutputUserDef],
    @docs(
      "to prevent unintentional resource use, if the Cypher query possibly contains an all node scan, then this parameter must be true"
    )
    allowAllNodeScan: Boolean = false
  ) extends StandingQueryResultOutputUserDef
}

@title("Standing Query Result Output Format")
sealed abstract class OutputFormat

object OutputFormat {
  @unnamed
  @title("JSON")
  case object JSON extends OutputFormat
  @unnamed
  @title("Protobuf")
  final case class Protobuf(
    @docs(
      "URL (or local filename) of the Protobuf .desc file to load that contains the desired typeName to serialize to"
    ) schemaUrl: String,
    @docs(
      "message type name to use (from the given .desc file) as the message type"
    ) typeName: String
  ) extends OutputFormat
}

trait StandingQuerySchemas
    extends endpoints4s.generic.JsonSchemas
    with exts.AnySchema
    with AwsCredentialsSchemas
    with MetricsSummarySchemas {

  import StandingQueryPattern._
  import StandingQueryResultOutputUserDef._

  implicit lazy val logModeSchema: JsonSchema[StandingQueryResultOutputUserDef.PrintToStandardOut.LogMode] =
    stringEnumeration[StandingQueryResultOutputUserDef.PrintToStandardOut.LogMode](
      StandingQueryResultOutputUserDef.PrintToStandardOut.LogMode.modes
    )(_.toString).withDescription(
      """Mode used to log Standing Query results. `Complete` is the
        |default and logs all matches found, slowing down result processing
        |so that every result can be logged. `FastSampling` may skip logging some
        |matches when there are too many to keep up with, but never slows down
        |the stream of results. Use `FastSampling` if you don't need every result
        |to be logged. Note that neither option changes the behavior of other
        |StandingQueryResultOutputs registered on the same standing query.""".stripMargin
    )

  implicit lazy val logLevelSchema: JsonSchema[StandingQueryResultOutputUserDef.PrintToStandardOut.LogLevel] =
    stringEnumeration[StandingQueryResultOutputUserDef.PrintToStandardOut.LogLevel](
      StandingQueryResultOutputUserDef.PrintToStandardOut.LogLevel.levels
    )(_.toString)

  implicit lazy val standingQueryModeSchema: JsonSchema[StandingQueryMode] =
    stringEnumeration[StandingQueryMode](StandingQueryMode.values)(_.toString)
      .withDescription(
        """Mode used to execute Standing Query. `DistinctId` is the default and
          |recommended value. `MultipleValues` can be used for more
          |expressive query capabilities, but requires more computation and
          |uses more memory.""".stripMargin
      )

  implicit lazy val outputFormatSchema: JsonSchema[OutputFormat] =
    lazyTagged("OutputFormat")(
      genericTagged[OutputFormat]
    ).withExample(OutputFormat.JSON)

  implicit lazy val standingQueryResultOutputSchema: JsonSchema[StandingQueryResultOutputUserDef] =
    lazyTagged("StandingQueryResultOutput")(
      genericTagged[StandingQueryResultOutputUserDef]
    ).withExample(
      StandingQueryResultOutputUserDef.CypherQuery(
        query = "MATCH (n) WHERE id(n) = $that.data.id SET n.flagged = true",
        andThen = None
      )
    )

  val sqExample: StandingQueryDefinition =
    StandingQueryDefinition(
      pattern = StandingQueryPattern.Cypher(
        "MATCH (n)-[:has_father]->(m) WHERE exists(n.name) AND exists(m.name) RETURN DISTINCT strId(n) AS kidWithDad"
      ),
      outputs = Map(
        "file-of-results" -> StandingQueryResultOutputUserDef.WriteToFile("kidsWithDads.jsonl")
      )
    )

  val runningSqExample: RegisteredStandingQuery = RegisteredStandingQuery(
    "example-sq",
    UUID.randomUUID(),
    Some(sqExample.pattern),
    sqExample.outputs,
    includeCancellations = false,
    inputBufferSize = 32,
    stats = Map(
      "localhost:67543" -> StandingQueryStats(
        rates = RatesSummary(
          count = 123L,
          oneMinute = 14.2,
          fiveMinute = 14.2,
          fifteenMinute = 14.2,
          overall = 14.2
        ),
        startTime = Instant.parse("2020-06-05T18:02:42.907Z"),
        totalRuntime = 60000L,
        bufferSize = 20
      )
    )
  )

  val additionalSqOutput: PrintToStandardOut = StandingQueryResultOutputUserDef.PrintToStandardOut(logMode =
    StandingQueryResultOutputUserDef.PrintToStandardOut.LogMode.FastSampling
  )

  implicit lazy val propertyValuePatternSchema: JsonSchema[PropertyValuePattern] = {
    implicit val anyJson: JsonSchema[ujson.Value] = anySchema(None)
    genericJsonSchema[PropertyValuePattern]
  }
  implicit lazy val nodePatternSchema: JsonSchema[NodePattern] =
    genericJsonSchema[NodePattern]
  implicit lazy val edgePatternSchema: JsonSchema[EdgePattern] =
    genericJsonSchema[EdgePattern]
  implicit lazy val returnColumnSchema: JsonSchema[ReturnColumn] =
    genericJsonSchema[ReturnColumn]

  implicit lazy val standingQueryPatternSchema: JsonSchema[StandingQueryPattern] =
    genericJsonSchema[StandingQueryPattern].withExample(sqExample.pattern)

  implicit lazy val standingQueryStatsSchema: JsonSchema[StandingQueryStats] =
    genericJsonSchema[StandingQueryStats]

  implicit lazy val standingQueryRegisteredSchema: JsonSchema[StandingQueryRegistered] =
    genericJsonSchema[StandingQueryRegistered]
  implicit lazy val standingQueryCancelledSchema: JsonSchema[StandingQueryCancelled] =
    genericJsonSchema[StandingQueryCancelled]

  implicit lazy val standingQuerySchema: JsonSchema[StandingQueryDefinition] =
    genericJsonSchema[StandingQueryDefinition].withExample(sqExample)
  implicit lazy val runningStandingQuerySchema: JsonSchema[RegisteredStandingQuery] =
    genericJsonSchema[RegisteredStandingQuery].withExample(runningSqExample)

}

trait StandingQueryRoutes
    extends StandingQuerySchemas
    with endpoints4s.algebra.Endpoints
    with endpoints4s.algebra.JsonEntitiesFromSchemas
    with endpoints4s.generic.JsonSchemas
    with exts.QuineEndpoints {

  private val api = path / "api" / "v1"
  protected val standing: Path[Unit] = api / "query" / "standing"

  private[this] val standingTag: Tag = Tag("Standing Queries")
    .withDescription(
      Some(
        """Live queries that automatically propagate through streaming data and instantly
          |return results.
          |""".stripMargin
      )
    )

  val standingName: Path[String] =
    segment[String]("standing-query-name", docs = Some("unique name for a standing query"))
  val standingOutputName: Path[String] = segment[String](
    "standing-query-output-name",
    docs = Some("unique name for a standing query output")
  )

  val standingIssue: Endpoint[(String, StandingQueryDefinition), Either[ClientErrors, Unit]] =
    endpoint(
      request = post(
        url = standing / standingName,
        entity = jsonRequest[StandingQueryDefinition]
      ),
      response = badRequest(docs = Some("Standing query exists already"))
        .orElse(created()),
      docs = EndpointDocs()
        .withSummary(Some("create a new standing query"))
        .withTags(List(standingTag))
    )

  val standingAddOut: Endpoint[(String, String, StandingQueryResultOutputUserDef), Option[Either[ClientErrors, Unit]]] =
    endpoint(
      request = post(
        url = standing / standingName / "output" / standingOutputName,
        entity = jsonRequestWithExample[StandingQueryResultOutputUserDef](additionalSqOutput)
      ),
      response = wheneverFound(badRequest() orElse created()),
      docs = EndpointDocs()
        .withSummary(Some("register an additional output to a running standing query"))
        .withTags(List(standingTag))
    )

  val standingRemoveOut: Endpoint[(String, String), Option[StandingQueryResultOutputUserDef]] =
    endpoint(
      request = delete(standing / standingName / "output" / standingOutputName),
      response = wheneverFound(ok(jsonResponse[StandingQueryResultOutputUserDef])),
      docs = EndpointDocs()
        .withSummary(Some("remove an output from a running standing query"))
        .withTags(List(standingTag))
    )

  val standingCancel: Endpoint[String, Option[RegisteredStandingQuery]] =
    endpoint(
      request = delete(
        url = standing / standingName
      ),
      response = wheneverFound(ok(jsonResponse[RegisteredStandingQuery])),
      docs = EndpointDocs()
        .withSummary(Some("cancel a standing query"))
        .withTags(List(standingTag))
    )

  val standingGet: Endpoint[String, Option[RegisteredStandingQuery]] =
    endpoint(
      request = get(
        url = standing / standingName
      ),
      response = wheneverFound(ok(jsonResponse[RegisteredStandingQuery])),
      docs = EndpointDocs()
        .withSummary(Some("get a standing query"))
        .withTags(List(standingTag))
    )

  val standingList: Endpoint[Unit, List[RegisteredStandingQuery]] =
    endpoint(
      request = get(
        url = standing
      ),
      response = ok(jsonResponse[List[RegisteredStandingQuery]]),
      docs = EndpointDocs()
        .withSummary(Some("list all standing queries"))
        .withTags(List(standingTag))
    )

  val standingPropagate: Endpoint[(Boolean, Int), Unit] = {
    val sleepingToo = qs[Option[Boolean]](
      "include-sleeping",
      docs = Some(
        """Propagate to all sleeping nodes. Setting to `true` can be costly if there is lot of
          |data. Default is false.
          |""".stripMargin
      )
    ).xmap(_.getOrElse(false))(Some(_))
    val wakeUpParallelism = qs[Option[Int]](
      "wake-up-parallelism",
      docs = Some(
        """In the case of `include-sleeping = true`, this controls the parallelism for how many
          |nodes to propagate to at once. Default is 4.
          |""".stripMargin
      )
    ).xmap(_.getOrElse(4))(Some(_))
    endpoint(
      request = post(
        url = standing / "control" / "propagate" /? (sleepingToo & wakeUpParallelism),
        entity = emptyRequest
      ),
      response = ok(emptyResponse),
      docs = EndpointDocs()
        .withSummary(Some("ensure standing queries are propagated to existing nodes"))
        .withTags(List(standingTag))
        .withDescription(
          Some(
            """When a new standing query is registered in the system, it gets automatically
              |registered on new nodes (or old nodes that are loaded back into the cache). This
              |behaviour is the default because pro-actively setting the standing query on all
              |existing data might be quite costly depending on how much historical data there is.
              |
              |However, sometimes there is a legitimate use-case for eagerly propogating standing
              |queries across the graph, for instance:
              |
              |  * when interactively constructing a standing query for already-ingested data
              |  * when creating a new standing query that needs to be applied to recent data
              |""".stripMargin
          )
        )
    )
  }
}
