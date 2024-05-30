package com.thatdot.quine.routes

import java.time.Instant
import java.util.UUID
import java.util.concurrent.atomic.AtomicReference

import endpoints4s.algebra.Tag
import endpoints4s.generic.{docs, title, unnamed}

import com.thatdot.quine.routes.exts.{EndpointsWithCustomErrorText, NamespaceParameter}

@title("Standing Query")
@docs("Standing Query")
final case class StandingQueryDefinition(
  pattern: StandingQueryPattern,
  @docs(s"A map of named standing query outs - see the ${StandingQueryResultOutputUserDef.title} schema for the values")
  outputs: Map[String, StandingQueryResultOutputUserDef],
  @docs("Whether or not to include cancellations in the results of this query")
  includeCancellations: Boolean = false,
  @docs("How many standing query results to buffer before backpressuring")
  inputBufferSize: Int = 32, // should match [[StandingQuery.DefaultQueueBackpressureThreshold]]
  @docs("For debug and test only")
  shouldCalculateResultHashCode: Boolean = false
)

@title("Registered Standing Query")
@docs("Registered Standing Query.")
final case class RegisteredStandingQuery(
  name: String,
  @docs("Unique identifier for the query, generated when the query is registered")
  internalId: UUID,
  @docs("Query or pattern to answer in a standing fashion")
  pattern: Option[StandingQueryPattern], // TODO: remove Option once we remove DGB SQs
  @docs(
    s"output sinks into which all new standing query results should be enqueued - see ${StandingQueryResultOutputUserDef.title}"
  )
  outputs: Map[String, StandingQueryResultOutputUserDef],
  includeCancellations: Boolean,
  @docs("How many standing query results to buffer on each host before backpressuring")
  inputBufferSize: Int,
  @docs(s"Statistics on progress of running the standing query, per host - see ${StandingQueryStats.title}")
  stats: Map[String, StandingQueryStats]
)

@unnamed
@title("Standing Query Pattern")
@docs("A declarative structural graph pattern.")
sealed abstract class StandingQueryPattern
object StandingQueryPattern {

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

    // QuinePattern engine
    case object QuinePattern extends StandingQueryMode

    val values: Seq[StandingQueryMode] = Seq(DistinctId, MultipleValues, QuinePattern)
  }
}

@unnamed
@title(StandingQueryStats.title)
final case class StandingQueryStats(
  @docs("Results per second over different time periods")
  rates: RatesSummary,
  @docs("Time (in ISO-8601 UTC time) when the standing query was started")
  startTime: Instant,
  @docs("Time (in milliseconds) that that the standing query has been running")
  totalRuntime: Long,
  @docs("How many standing query results are buffered and waiting to be emitted")
  bufferSize: Int,
  @docs("Accumulated output hash code")
  outputHashCode: Long
)

object StandingQueryStats {
  val title: String = "Statistics About a Running Standing Query"
}

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
@title(StandingQueryResultOutputUserDef.title)
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
sealed abstract class StandingQueryResultOutputUserDef {
  def slug: String
}

object StandingQueryResultOutputUserDef {
  val title = "Standing Query Result Output"

  @unnamed
  @title("POST to HTTP[S] Webhook")
  @docs(
    "Makes an HTTP[S] POST for each result. For the format of the result, see \"Standing Query Result Output\"."
  )
  final case class PostToEndpoint(
    url: String,
    parallelism: Int = 8,
    onlyPositiveMatchData: Boolean = false
  ) extends StandingQueryResultOutputUserDef {
    override def slug = "http"
  }

  @unnamed
  @title("Publish to Kafka Topic")
  @docs(
    "Publishes a JSON record for each result to the provided Apache Kafka topic. For the format of the result record, see \"Standing Query Result Output\"."
  )
  final case class WriteToKafka(
    topic: String,
    bootstrapServers: String,
    format: OutputFormat = OutputFormat.JSON,
    @docs(
      "Map of Kafka producer properties. See <https://docs.confluent.io/platform/current/installation/configuration/producer-configs.html>"
    )
    kafkaProperties: Map[String, String] = Map.empty[String, String]
  ) extends StandingQueryResultOutputUserDef {
    override def slug: String = "kafka"
  }

  @unnamed
  @title("Publish to Kinesis Data Stream")
  @docs(
    "Publishes a JSON record for each result to the provided Kinesis stream. For the format of the result record, see \"StandingQueryResult\"."
  )
  final case class WriteToKinesis(
    credentials: Option[AwsCredentials],
    region: Option[AwsRegion],
    streamName: String,
    format: OutputFormat = OutputFormat.JSON,
    kinesisParallelism: Option[Int],
    kinesisMaxBatchSize: Option[Int],
    kinesisMaxRecordsPerSecond: Option[Int],
    kinesisMaxBytesPerSecond: Option[Int]
  ) extends StandingQueryResultOutputUserDef {
    override def slug: String = "kinesis"
  }

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
    region: Option[AwsRegion],
    @docs("ARN of the topic to publish to") topic: String
  ) extends StandingQueryResultOutputUserDef {
    override def slug: String = "sns"
  }

  @unnamed
  @title("Log JSON to Console")
  @docs("Prints each result as a single-line JSON object to stdout on the Quine server.")
  final case class PrintToStandardOut(
    logLevel: PrintToStandardOut.LogLevel = PrintToStandardOut.LogLevel.Info,
    logMode: PrintToStandardOut.LogMode = PrintToStandardOut.LogMode.Complete
  ) extends StandingQueryResultOutputUserDef {
    override def slug: String = "stdout"
  }

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
  ) extends StandingQueryResultOutputUserDef {
    override def slug: String = "file"
  }

  @unnamed
  @title("Publish to Slack Webhook")
  @docs(
    "Sends a message to Slack via a configured webhook URL. See <https://api.slack.com/messaging/webhooks>."
  )
  final case class PostToSlack(
    hookUrl: String,
    onlyPositiveMatchData: Boolean = false,
    @docs("Number of seconds to wait between messages; minimum 1") intervalSeconds: Int = 20
  ) extends StandingQueryResultOutputUserDef {
    override def slug: String = "slack"
  }

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
    @docs("Name of the Cypher parameter holding the standing query result") parameter: String = "that",
    @docs("maximum number of standing query results being processed at once")
    parallelism: Int = IngestRoutes.defaultWriteParallelism,
    @docs(
      """Send the result of the Cypher query to another standing query output (in order to provide chained
                                    |transformation and actions). The data returned by this query will be passed as the `data` object
                                    |of the new StandingQueryResult (see \"Standing Query Result Output\")""".stripMargin
        .replace('\n', ' ')
    )
    andThen: Option[StandingQueryResultOutputUserDef],
    @docs(
      "To prevent unintentional resource use, if the Cypher query possibly contains an all node scan, then this parameter must be true"
    )
    allowAllNodeScan: Boolean = false,
    @docs(
      """Whether queries that raise a potentially-recoverable error should be retried. If set to true (the default),
                                    |such errors will be retried until they succeed. Additionally, if the query is not idempotent, the query's
                                    |effects may occur multiple times in the case of external system failure. Query idempotency
                                    |can be checked with the EXPLAIN keyword. If set to false, results and effects will not be duplicated,
                                    |but may be dropped in the case of external system failure""".stripMargin
        .replace('\n', ' ')
    )
    shouldRetry: Boolean = true
  ) extends StandingQueryResultOutputUserDef {
    override def slug: String = "cypher"
  }

  @unnamed
  @title("Drop")
  final case object Drop extends StandingQueryResultOutputUserDef {
    override def slug: String = "drop"
  }

  /** Queue for collecting standing query results to be used programmatically.
    * Meant for internal use in Quine for testing.
    *
    * To use this, instantiate a `scala.collection.mutable.Queue[StandingQueryResult]` elsewhere (in tests)
    * and pass it to the constructor. E.g.:
    * ```
    *   val sqResultsQueue = new mutable.Queue[StandingQueryResult]()
    *   val sqOutput = StandingQueryResultOutputUserDef.InternalQueue(sqResultsQueue)
    * ```
    *
    * Ideally, the queue would be a concurrent queue, but since this is meant for testing, there is companion
    * code in `StandingQueryResultOutput.resultHandlingFlow` which uses a simple `.map` to only enqueue items singly.
    *
    * Note that `StandingQueryResult` is not accessible in this place, and so the existential types below are
    * a hack to work around the type checker.
    */
  @unnamed
  @title("Internal Queue") // TODO: Keep this unpublished (or ideally, unavailable) in OpenAPI docs/REST API.
  final case class InternalQueue() extends StandingQueryResultOutputUserDef {
    var results: AtomicReference[_] = _

    override def slug: String = "internalQueue"
  }
  case object InternalQueue {
    def apply(resultsRef: AtomicReference[_]): InternalQueue = {
      val q = InternalQueue()
      q.results = resultsRef
      q
    }
  }
}

@unnamed
@title("Standing Query Result Output Format")
sealed abstract class OutputFormat

@unnamed
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
    with AwsConfigurationSchemas
    with MetricsSummarySchemas {

  import StandingQueryPattern._
  import StandingQueryResultOutputUserDef._

  implicit lazy val logModeSchema: Enum[StandingQueryResultOutputUserDef.PrintToStandardOut.LogMode] =
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

  implicit lazy val logLevelSchema: Enum[StandingQueryResultOutputUserDef.PrintToStandardOut.LogLevel] =
    stringEnumeration[StandingQueryResultOutputUserDef.PrintToStandardOut.LogLevel](
      StandingQueryResultOutputUserDef.PrintToStandardOut.LogLevel.levels
    )(_.toString)

  implicit lazy val standingQueryModeSchema: Enum[StandingQueryMode] =
    stringEnumeration[StandingQueryMode](StandingQueryMode.values)(_.toString)
      .withDescription(
        """Mode used to execute Standing Query. `DistinctId` is the default and
          |recommended value. `MultipleValues` can be used for more
          |expressive query capabilities, but requires more computation and
          |uses more memory.""".stripMargin
      )

  implicit lazy val outputFormatSchema: Tagged[OutputFormat] =
    genericTagged[OutputFormat].withExample(OutputFormat.JSON)

  implicit lazy val standingQueryResultOutputSchema: Tagged[StandingQueryResultOutputUserDef] =
    lazyTagged(StandingQueryResultOutputUserDef.title)(
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
        "MATCH (n)-[:has_father]->(m) WHERE n.name IS NOT NULL AND m.name IS NOT NULL RETURN DISTINCT strId(n) AS kidWithDad"
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
        bufferSize = 20,
        outputHashCode = 14344L
      )
    )
  )

  val additionalSqOutput: PrintToStandardOut = StandingQueryResultOutputUserDef.PrintToStandardOut(logMode =
    StandingQueryResultOutputUserDef.PrintToStandardOut.LogMode.FastSampling
  )

  implicit lazy val standingQueryPatternSchema: Tagged[StandingQueryPattern] =
    genericTagged[StandingQueryPattern].withExample(sqExample.pattern)

  implicit lazy val standingQueryStatsSchema: Record[StandingQueryStats] =
    genericRecord[StandingQueryStats]

  implicit lazy val standingQueryRegisteredSchema: Record[StandingQueryRegistered] =
    genericRecord[StandingQueryRegistered]
  implicit lazy val standingQueryCancelledSchema: Record[StandingQueryCancelled] =
    genericRecord[StandingQueryCancelled]

  implicit lazy val standingQuerySchema: Record[StandingQueryDefinition] =
    genericRecord[StandingQueryDefinition].withExample(sqExample)
  implicit lazy val runningStandingQuerySchema: Record[RegisteredStandingQuery] =
    genericRecord[RegisteredStandingQuery].withExample(runningSqExample)

}

trait StandingQueryRoutes
    extends StandingQuerySchemas
    with EndpointsWithCustomErrorText
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
    segment[String]("standing-query-name", docs = Some("Unique name for a standing query"))
  val standingOutputName: Path[String] = segment[String](
    "standing-query-output-name",
    docs = Some("Unique name for a standing query output")
  )

  val standingIssue
    : Endpoint[(String, NamespaceParameter, StandingQueryDefinition), Either[ClientErrors, Option[Unit]]] = {
    val sq: StandingQueryDefinition = StandingQueryDefinition(
      StandingQueryPattern.Cypher(
        "MATCH (n)-[:has_father]->(m) WHERE exists(n.name) AND exists(m.name) RETURN DISTINCT strId(n) AS kidWithDad"
      ),
      Map(
        "endpoint" -> StandingQueryResultOutputUserDef.PostToEndpoint("http://myendpoint"),
        "stdout" -> StandingQueryResultOutputUserDef.PrintToStandardOut()
      ),
      includeCancellations = true,
      32,
      shouldCalculateResultHashCode = true
    )
    endpoint(
      request = post(
        url = standing / standingName /? namespace,
        entity = jsonOrYamlRequestWithExample[StandingQueryDefinition](sq)
      ),
      response = customBadRequest("Standing query exists already")
        .orElse(wheneverFound(created())),
      docs = EndpointDocs()
        .withSummary(Some("Create Standing Query"))
        .withDescription(
          Some(
            """|Individual standing queries are issued into the graph one time;
               |result outputs are produced as new data is written into Quine and matches are found.
               |
               |Compared to traditional queries, standing queries are less imperative
               |and more declarative - it doesn't matter what order parts of the pattern match,
               |only that the composite structure exists.
               |
               |Learn more about writing
               |[standing queries](https://docs.quine.io/components/writing-standing-queries.html)
               |in the docs.""".stripMargin
          )
        )
        .withTags(List(standingTag))
    )
  }

  val standingAddOut: Endpoint[(String, String, NamespaceParameter, StandingQueryResultOutputUserDef), Option[
    Either[ClientErrors, Unit]
  ]] =
    endpoint(
      request = post(
        url = standing / standingName / "output" / standingOutputName /? namespace,
        entity = jsonOrYamlRequestWithExample[StandingQueryResultOutputUserDef](additionalSqOutput)
      ),
      response = wheneverFound(badRequest() orElse created()),
      docs = EndpointDocs()
        .withSummary(Some("Create Standing Query Output"))
        .withDescription(
          Some(
            "Each standing query can have any number of destinations to which `StandingQueryResults` will be routed."
          )
        )
        .withTags(List(standingTag))
    )

  val standingRemoveOut: Endpoint[(String, String, NamespaceParameter), Option[StandingQueryResultOutputUserDef]] =
    endpoint(
      request = delete(standing / standingName / "output" / standingOutputName /? namespace),
      response = wheneverFound(ok(jsonResponse[StandingQueryResultOutputUserDef])),
      docs = EndpointDocs()
        .withSummary(Some("Delete Standing Query Output"))
        .withDescription(
          Some(
            "Remove an output from a standing query."
          )
        )
        .withTags(List(standingTag))
    )

  val standingCancel: Endpoint[(String, NamespaceParameter), Option[RegisteredStandingQuery]] =
    endpoint(
      request = delete(
        url = standing / standingName /? namespace
      ),
      response = wheneverFound(ok(jsonResponse[RegisteredStandingQuery])),
      docs = EndpointDocs()
        .withSummary(Some("Delete Standing Query"))
        .withDescription(
          Some(
            "Immediately halt and remove the named standing query from Quine."
          )
        )
        .withTags(List(standingTag))
    )

  val standingGet: Endpoint[(String, NamespaceParameter), Option[RegisteredStandingQuery]] =
    endpoint(
      request = get(
        url = standing / standingName /? namespace
      ),
      response = wheneverFound(ok(jsonResponse[RegisteredStandingQuery])),
      docs = EndpointDocs()
        .withSummary(Some("Standing Query Status"))
        .withDescription(
          Some("Return the status information for a configured standing query by name.")
        )
        .withTags(List(standingTag))
    )

  val standingList: Endpoint[NamespaceParameter, List[RegisteredStandingQuery]] =
    endpoint(
      request = get(
        url = standing /? namespace
      ),
      response = ok(jsonResponse[List[RegisteredStandingQuery]]),
      docs = EndpointDocs()
        .withSummary(Some("List Standing Queries"))
        .withDescription(
          Some(
            """|Return a JSON array containing the configured
               |[standing queries](https://docs.quine.io/components/writing-standing-queries.html)
               |and their associated metrics keyed by standing query name. """.stripMargin
          )
        )
        .withTags(List(standingTag))
    )

  val standingPropagate: Endpoint[(Boolean, Int, NamespaceParameter), Option[Unit]] = {
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
        url = standing / "control" / "propagate" /? (sleepingToo & wakeUpParallelism & namespace),
        entity = emptyRequest
      ),
      response = accepted(emptyResponse).orNotFound(notFoundDocs = Some("Namespace not found")),
      docs = EndpointDocs()
        .withSummary(Some("Propagate Standing Queries"))
        .withTags(List(standingTag))
        .withDescription(
          Some(
            """When a new standing query is registered in the system, it gets automatically
              |registered on new nodes (or old nodes that are loaded back into the cache). This
              |behavior is the default because pro-actively setting the standing query on all
              |existing data might be quite costly depending on how much historical data there is.
              |
              |However, sometimes there is a legitimate use-case for eagerly propagating standing
              |queries across the graph, for instance:
              |
              |  * When interactively constructing a standing query for already-ingested data
              |  * When creating a new standing query that needs to be applied to recent data
              |""".stripMargin
          )
        )
    )
  }
}

object StandingQueryRoutes {
  sealed trait StandingPropagateResult
  object StandingPropagateResult {
    object Success extends StandingPropagateResult
    object NotFound extends StandingPropagateResult
  }
}
