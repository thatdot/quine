package com.thatdot.quine.routes

import java.time.Instant

import endpoints4s.algebra.Tag
import endpoints4s.generic.{docs, title, unnamed}

sealed abstract class IngestStreamStatus(val isTerminal: Boolean)

object IngestStreamStatus {
  @docs("The stream is currently actively running, and possibly waiting for new records to become available upstream.")
  case object Running extends IngestStreamStatus(isTerminal = false)

  @docs("The stream has been paused by a user.")
  case object Paused extends IngestStreamStatus(isTerminal = false)

  @docs("The stream has processed all records, and the upstream data source will not make more records available.")
  case object Completed extends IngestStreamStatus(isTerminal = true)

  @docs("The stream has been stopped by a user.")
  case object Terminated extends IngestStreamStatus(isTerminal = true)

  @docs(
    "The stream has been restored from a saved state, but is not yet running: For example, after restarting Quine."
  )
  case object Restored extends IngestStreamStatus(isTerminal = false)

  @docs("The stream has been stopped by a failure during processing.")
  case object Failed extends IngestStreamStatus(isTerminal = true)

  val states: Seq[IngestStreamStatus] = Seq(Running, Paused, Completed, Terminated, Restored, Failed)
}

/** Information kept at runtime about an active ingest stream
  *
  * @param name user-given name for the stream
  * @param settings ingest configuration
  * @param stats ingest progress stats
  */
@title("Named Ingest Stream")
@docs("An active stream of data being ingested paired with a name for the stream.")
final case class IngestStreamInfoWithName(
  @docs("unique name to identify the ingest stream") name: String,
  @docs(
    "indicator of whether the ingest is still running, completed, etc."
  ) status: IngestStreamStatus,
  @docs("error message about the ingest, if any") message: Option[String],
  @docs("configuration of the ingest stream") settings: IngestStreamConfiguration,
  @docs("statistics on progress of running ingest stream") stats: IngestStreamStats
)

@title("Ingest Stream")
@docs("An active stream of data being ingested.")
final case class IngestStreamInfo(
  @docs(
    "indicator of whether the ingest is still running, completed, etc."
  ) status: IngestStreamStatus,
  @docs("error message about the ingest, if any") message: Option[String],
  @docs("configuration of the ingest stream") settings: IngestStreamConfiguration,
  @docs("statistics on progress of running ingest stream") stats: IngestStreamStats
) {
  def withName(name: String): IngestStreamInfoWithName = IngestStreamInfoWithName(
    name = name,
    status = status,
    message = message,
    settings = settings,
    stats = stats
  )
}

@title("Statistics About a Running Ingest Stream")
final case class IngestStreamStats(
  // NB this is duplicated by rates.count -- maybe remove one?
  @docs("number of source records (or lines) ingested so far") ingestedCount: Long,
  @docs("records per second over different time periods") rates: RatesSummary,
  @docs("bytes per second over different time periods") byteRates: RatesSummary,
  @docs("time (in ISO-8601 UTC time) when the ingestion was started") startTime: Instant,
  @docs("time (in milliseconds) that that the ingest has been running") totalRuntime: Long
)
object IngestStreamStats {
  val example: IngestStreamStats = IngestStreamStats(
    ingestedCount = 123L,
    rates = RatesSummary(
      123L,
      14.1,
      14.5,
      14.15,
      14.0
    ),
    byteRates = RatesSummary(
      8664000L,
      142030.1,
      145299.6,
      144287.6,
      144400.0
    ),
    startTime = Instant.parse("2020-06-05T18:02:42.907Z"),
    60000L
  )
}

@title("Rates Summary")
@docs("Summary statistics about a metered rate (ie, count per second).")
final case class RatesSummary(
  @docs("number of items metered") count: Long,
  @docs("approximate rate per second in the last minute") oneMinute: Double,
  @docs("approximate rate per second in the last five minutes") fiveMinute: Double,
  @docs("approximate rate per second in the last fifteen minutes") fifteenMinute: Double,
  @docs("approximate rate per second since the meter was started") overall: Double
)
trait MetricsSummarySchemas extends endpoints4s.generic.JsonSchemas {
  implicit lazy val ratesSummarySchema: JsonSchema[RatesSummary] =
    genericJsonSchema[RatesSummary]
}

@title("AWS Credentials")
@docs(
  "Explicit AWS access key and secret to use. If not provided, defaults to environmental credentials according to the default AWS credential chain. See <https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html#credentials-default>."
)
final case class AwsCredentials(region: String, accessKeyId: String, secretAccessKey: String)

trait AwsCredentialsSchemas extends endpoints4s.generic.JsonSchemas {
  implicit lazy val awsCredentialsSchema: JsonSchema[AwsCredentials] =
    genericJsonSchema[AwsCredentials]
}

@title("Kafka Auto Offset Reset")
@docs(
  "See [`auto.offset.reset` in the Kafka documentation](https://docs.confluent.io/current/installation/configuration/consumer-configs.html#auto.offset.reset)."
)
sealed abstract class KafkaAutoOffsetReset(val name: String)
object KafkaAutoOffsetReset {
  case object Latest extends KafkaAutoOffsetReset("latest")
  case object Earliest extends KafkaAutoOffsetReset("earliest")
  case object None extends KafkaAutoOffsetReset("none")
  val values: Seq[KafkaAutoOffsetReset] = Seq(Latest, Earliest, None)
}

@title("Kafka Security Protocol")
@docs(
  "See [`security.protocol` in the Kafka documentation](https://kafka.apache.org/24/javadoc/org/apache/kafka/common/security/auth/SecurityProtocol.html)."
)
sealed abstract class KafkaSecurityProtocol(val name: String)
object KafkaSecurityProtocol {
  case object PlainText extends KafkaSecurityProtocol("PLAINTEXT")
  case object Ssl extends KafkaSecurityProtocol("SSL")
  val values: Seq[KafkaSecurityProtocol] = Seq(PlainText, Ssl)
}

@title("Kafka offset tracking mechanism")
@docs(
  "How to keep track of current offset when consuming from Kafka, if at all."
)
sealed abstract class KafkaOffsetCommitting
object KafkaOffsetCommitting {
  @title("Auto Commit")
  @docs("Set Kafka's enable.auto.commit and auto.commit.interval.ms settings")
  final case class AutoCommit(commitIntervalMillis: Int = 5000) extends KafkaOffsetCommitting
  @title("Explicit Commit")
  @docs("Don't auto commit, only explicitly commit to consumer group on successful execution of ingest query to graph")
  final case class ExplicitCommit(
    maxBatch: Long = 1000,
    maxIntervalMillis: Int = 10000,
    parallelism: Int = 100,
    waitForCommitConfirmation: Boolean = true
  ) extends KafkaOffsetCommitting
}

@title("Ingest Stream Configuration")
@docs("A specification of a data source and rules for consuming data from that source.")
sealed abstract class IngestStreamConfiguration

object KafkaIngest {

  // Takes a set of topic names
  type Topics = Set[String]
  // Takes a set of partition numbers for each topic name.
  type PartitionAssignments = Map[String, Set[Int]]
}

/** Kafka ingest stream configuration
  *
  * @param format how the Kafka records are encoded
  * @param topics from which topics to read data
  * @param parallelism maximum number of records to process at once
  * @param bootstrapServers comma-separated list of host/port pairs
  * @param groupId consumer group this consumer belongs to
  */
@title("Kafka Ingest Stream")
@unnamed
@docs("A stream of data being ingested from Kafka.")
final case class KafkaIngest(
  @docs("format used to decode each Kafka record")
  format: StreamedRecordFormat = IngestRoutes.defaultStreamedRecordFormat,
  @docs(
    """Kafka topics from which to ingest: Either an array of topic names, or an object whose keys are topic names and
      |whose values are partition indices""".stripMargin
      .replace('\n', ' ')
  ) topics: Either[KafkaIngest.Topics, KafkaIngest.PartitionAssignments],
  @docs("maximum number of records being processed at once")
  parallelism: Int = IngestRoutes.defaultWriteParallelism,
  @docs("comma-separated list of Kafka broker servers") bootstrapServers: String,
  @docs(
    "ID of the consumer group this ingest stream should report that it belongs to; defaults to the name of the ingest"
  ) groupId: Option[String],
  securityProtocol: KafkaSecurityProtocol = KafkaSecurityProtocol.PlainText,
  offsetCommitting: Option[KafkaOffsetCommitting],
  autoOffsetReset: KafkaAutoOffsetReset = KafkaAutoOffsetReset.Latest,
  @docs(
    "offset at which this stream should complete; offsets are sequential integers starting at 0"
  ) endingOffset: Option[Long],
  @docs("maximum records to process per second") maximumPerSecond: Option[Int]
) extends IngestStreamConfiguration

object KinesisIngest {

  @title("Kinesis Shard Iterator Type")
  @docs("See <https://docs.aws.amazon.com/kinesis/latest/APIReference/API_StartingPosition.html>.")
  sealed abstract class IteratorType

  object IteratorType {

    @unnamed
    sealed abstract class Unparameterized extends IteratorType

    @unnamed
    sealed abstract class Parameterized extends IteratorType

    @title("Latest")
    @docs("All records added to the shard since subscribing.")
    @unnamed
    case object Latest extends Unparameterized

    @title("TrimHorizon")
    @docs("All records in the shard.")
    @unnamed
    case object TrimHorizon extends Unparameterized

    @title("AtSequenceNumber")
    @docs("All records starting from the provided sequence number.")
    @unnamed
    final case class AtSequenceNumber(sequenceNumber: String) extends Parameterized

    @title("AfterSequenceNumber")
    @docs("All records starting after the provided sequence number.")
    @unnamed
    final case class AfterSequenceNumber(sequenceNumber: String) extends Parameterized

    // JS-safe long gives ms until the year 287396-ish
    @title("AtTimestamp")
    @docs("All records starting from the provided unix millisecond timestamp.")
    @unnamed
    final case class AtTimestamp(millisSinceEpoch: Long) extends Parameterized
  }
}

@title("Kinesis Ingest Stream")
@unnamed
@docs("A stream of data being ingested from Kinesis.")
final case class KinesisIngest(
  @docs("format used to decode each Kinesis record")
  format: StreamedRecordFormat = IngestRoutes.defaultStreamedRecordFormat,
  @docs("name of the Kinesis stream from which to ingest") streamName: String,
  @docs(
    "IDs of the shards within the named kinesis stream from which to ingest; if empty or excluded, all shards on the stream will be used"
  )
  shardIds: Option[Set[String]],
  @docs("maximum number of records to write simultaneously")
  parallelism: Int = IngestRoutes.defaultWriteParallelism,
  credentials: Option[AwsCredentials],
  @docs("shard iterator type") iteratorType: KinesisIngest.IteratorType = KinesisIngest.IteratorType.Latest,
  @docs("number of retries to attempt on Kineses error") numRetries: Int = 3,
  @docs("maximum records to process per second") maximumPerSecond: Option[Int]
) extends IngestStreamConfiguration

@title("Server Sent Events Stream")
@unnamed
@docs(
  "A server-issued event stream, as might be handled by the EventSource JavaScript API. Only consumes the `data` portion of an event."
)
final case class ServerSentEventsIngest(
  @docs("format used to decode each event's `data`")
  format: StreamedRecordFormat = IngestRoutes.defaultStreamedRecordFormat,
  @docs("URL of the server sent event stream") url: String,
  @docs("maximum number of records to ingest simultaneously")
  parallelism: Int = IngestRoutes.defaultWriteParallelism,
  @docs("maximum records to process per second") maximumPerSecond: Option[Int]
) extends IngestStreamConfiguration

@title("Simple Queue Service Queue")
@unnamed
@docs("An active stream of data being ingested from AWS SQS.")
final case class SQSIngest(
  @docs("format used to decode each queued record")
  format: StreamedRecordFormat = IngestRoutes.defaultStreamedRecordFormat,
  @docs("URL of the queue from which to ingest") queueUrl: String,
  @docs("maximum number of records to read from the queue simultaneously") readParallelism: Int = 1,
  @docs("maximum number of records to ingest simultaneously")
  writeParallelism: Int = IngestRoutes.defaultWriteParallelism,
  credentials: Option[AwsCredentials],
  @docs("whether the queue consumer should acknowledge receipt of in-flight messages")
  deleteReadMessages: Boolean = true,
  @docs("maximum records to process per second") maximumPerSecond: Option[Int]
) extends IngestStreamConfiguration

@title("Streamed Record Format")
@docs("Format by which streamed records are decoded.")
sealed abstract class StreamedRecordFormat
object StreamedRecordFormat {

  @title("JSON via Cypher")
  @unnamed
  @docs("""Records should be JSON values. For every record received, the
  |given Cypher query will be re-executed with the parameter in the query set
  |equal to the new JSON value.
  """.stripMargin)
  final case class CypherJson(
    @docs("Cypher query to execute on each record") query: String,
    @docs("name of the Cypher parameter to populate with the JSON value") parameter: String = "that"
  ) extends StreamedRecordFormat

  @title("Raw Bytes via Cypher")
  @unnamed
  @docs("""Records may have any format. For every record received, the
          |given Cypher query will be re-executed with the parameter in the query set
          |equal to the new value as a Cypher byte array.
  """.stripMargin)
  final case class CypherRaw(
    @docs("Cypher query to execute on each record") query: String,
    @docs("name of the Cypher parameter to populate with the byte array") parameter: String = "that"
  ) extends StreamedRecordFormat

  @title("Protobuf via Cypher")
  @unnamed
  @docs(
    "Records are serialized instances of typeName as described in the schema (a `.desc` descriptor file) at " +
    "schemaUrl. For every record received, the given Cypher query will be re-executed with the parameter " +
    "in the query set equal to the new (deserialized) Protobuf message."
  )
  final case class CypherProtobuf(
    @docs("Cypher query to execute on each record") query: String,
    @docs("name of the Cypher parameter to populate with the Protobuf message") parameter: String = "that",
    @docs(
      "URL (or local filename) of the Protobuf .desc file to load to parse the typeName"
    ) schemaUrl: String,
    @docs(
      "message type name to use from the given .desc file as the incoming message type"
    ) typeName: String
  ) extends StreamedRecordFormat

  @title("Drop")
  @unnamed
  @docs("Ignore the data without further processing.")
  case object Drop extends StreamedRecordFormat
}

/** Local file ingest stream configuration
  *
  * TODO: streaming upload of file
  *
  * @param format how the file should be split into elements
  * @param path path on disk of the file
  * @param parallelism maximum number of records to process at once
  */
@title("File Ingest Stream")
@unnamed
@docs("An active stream of data being ingested from a file on this Quine host.")
final case class FileIngest(
  format: FileIngestFormat = IngestRoutes.defaultFileRecordFormat,
  @docs("path to the file")
  path: String,
  @docs(s"""text encoding used to read the file. Only UTF-8, US-ASCII and ISO-8859-1 are directly
  |supported -- other encodings will transcoded to UTF-8 on the fly (and ingest may be slower).
  |""".stripMargin)
  encoding: String = "UTF-8",
  @docs("maximum number of records being processed as once")
  parallelism: Int = IngestRoutes.defaultWriteParallelism,
  @docs("maximum size (in bytes) of any line in the file")
  maximumLineSize: Int = IngestRoutes.defaultMaximumLineSize,
  @docs(s"""start at the record with the given index. Useful for skipping some number of lines (e.g. CSV headers) or
           |resuming ingest from a partially consumed file""".stripMargin)
  startAtOffset: Long = 0L,
  @docs(s"optionally limit how many records are ingested from this file.")
  ingestLimit: Option[Long],
  @docs("maximum records to process per second")
  maximumPerSecond: Option[Int],
  @docs(
    "enables behaviors required for ingesting from a non-regular file type; default is to auto-detect if file is named pipe"
  ) fileIngestMode: Option[FileIngestMode]
) extends IngestStreamConfiguration

@title("File Ingest Format")
@docs("Format by which a file will be interpreted as a stream of elements for ingest.")
sealed abstract class FileIngestFormat
object FileIngestFormat {

  /** Create using a cypher query, passing each line in as a string */
  @unnamed()
  @docs("""For every line in the file, the  received, given Cypher query will be
  |re-executed with the parameter in the query set equal to a string matching
  |the new line value. The newline is not included in this string.
  """.stripMargin)
  final case class CypherLine(
    @docs("Cypher query to execute on each line") query: String,
    @docs("name of the Cypher parameter holding the string line value") parameter: String = "that"
  ) extends FileIngestFormat

  /** Create using a cypher query, expecting each line to be a JSON record */
  @unnamed()
  @docs("""Lines in the file should be JSON values. For every value received, the
  |given Cypher query will be re-executed with the parameter in the query set
  |equal to the new JSON value.
  """.stripMargin)
  final case class CypherJson(
    @docs("Cypher query to execute on each record") query: String,
    @docs("name of the Cypher parameter holding the JSON value") parameter: String = "that"
  ) extends FileIngestFormat

  /** Create using a cypher query, expecting each line to be a single row CSV record */
  @unnamed
  @docs("""For every row in a CSV file, the given Cypher query will be re-executed with the parameter in the query set
          |to the parsed row. Rows are parsed into either a Cypher List of strings or a Map, depending on whether
          |`headers` are available.""".stripMargin)
  final case class CypherCsv(
    @docs("Cypher query to execute on each record") query: String,
    @docs("name of the Cypher parameter holding the parsed CSV value as a list or map, depending on `headers`")
    parameter: String = "that",
    @docs("""Read the CSV file with headers read from the first row of the file (`true`) or with no headers (`false`).
            |Alternatively, an array of column headers can be passed in. If headers are not supplied, the resulting
            |type available to the Cypher query will be a List of strings with values accessible by index. If headers
            |are available (supplied or read from the file), the resulting type available to the Cypher query will be
            |a Map[String, String], with values accessible by using the corresponding header string. CSV rows longer
            |than the `headers` will have later items discarded which don't match up with a header column. CSV rows
            |with fewer columns than the `headers` will have `null` values for the missing headers. Defaults to
            |`false`.""".stripMargin)
    headers: Either[Boolean, List[String]] = Left(false),
    @docs("character used to delimit values on a single CSV row")
    delimiter: CsvCharacter = CsvCharacter.Comma,
    @docs("""character used to quote values in a field. Special characters (like new lines) inside of a quoted
            |section will be a part of the CSV value""".stripMargin)
    quoteChar: CsvCharacter = CsvCharacter.DoubleQuote,
    @docs("character used to escape other special characters")
    escapeChar: CsvCharacter = CsvCharacter.Backslash
  ) extends FileIngestFormat {
    require(delimiter != quoteChar, "Different characters must be used for `delimiter` and `quoteChar`.")
    require(delimiter != escapeChar, "Different characters must be used for `delimiter` and `escapeChar`.")
    require(quoteChar != escapeChar, "Different characters must be used for `quoteChar` and `escapeChar`.")
  }
}

@title("File Ingest Mode")
@docs("Determines behaviors required for ingesting from a non-regular file type")
sealed abstract class FileIngestMode
object FileIngestMode {
  @docs("ordinary file to be open and read once")
  case object Regular extends FileIngestMode
  @docs("named pipe to be regularly reopened and polled for more data")
  case object NamedPipe extends FileIngestMode

  val values: Seq[FileIngestMode] = Seq(Regular, NamedPipe)
}

sealed trait CsvCharacter { def char: Byte }
object CsvCharacter {
  case object Backslash extends CsvCharacter { def char: Byte = '\\' }
  case object Comma extends CsvCharacter { def char: Byte = ',' }
  case object Semicolon extends CsvCharacter { def char: Byte = ';' }
  case object Colon extends CsvCharacter { def char: Byte = ':' }
  case object Tab extends CsvCharacter { def char: Byte = '\t' }
  case object Pipe extends CsvCharacter { def char: Byte = '|' }
  case object DoubleQuote extends CsvCharacter { def char: Byte = '"' }
  val values: Seq[CsvCharacter] = Seq(Backslash, Comma, Semicolon, Colon, Tab, Pipe, DoubleQuote)
}

trait IngestSchemas extends endpoints4s.generic.JsonSchemas with AwsCredentialsSchemas with MetricsSummarySchemas {

  implicit lazy val csvHeaderOptionFormatSchema: JsonSchema[Either[Boolean, List[String]]] =
    orFallbackToJsonSchema[Boolean, List[String]](implicitly, implicitly)

  implicit lazy val csvCharacterFormatSchema: JsonSchema[CsvCharacter] =
    stringEnumeration(CsvCharacter.values)(_.toString)

  implicit lazy val entityFormatSchema: JsonSchema[StreamedRecordFormat] =
    genericJsonSchema[StreamedRecordFormat]
      .withExample(IngestRoutes.defaultStreamedRecordFormat)

  implicit lazy val fileIngestFormatSchema: JsonSchema[FileIngestFormat] =
    genericJsonSchema[FileIngestFormat]
      .withExample(IngestRoutes.defaultFileRecordFormat)

  implicit val ingestStatusSchema: JsonSchema[IngestStreamStatus] =
    stringEnumeration(IngestStreamStatus.states)(_.toString)
      .withExample(IngestStreamStatus.Running)

  implicit lazy val iteratorTypeSchema: JsonSchema[KinesisIngest.IteratorType] = {
    import KinesisIngest.IteratorType
    val unparameterizedKinesisIteratorSchema: JsonSchema[IteratorType.Unparameterized] =
      stringEnumeration[IteratorType.Unparameterized](
        Seq(IteratorType.TrimHorizon, IteratorType.Latest)
      )(_.toString)

    val parameterizedKinesisIteratorSchema: JsonSchema[IteratorType.Parameterized] =
      genericJsonSchema[IteratorType.Parameterized]

    // Try the string enumeration first, then try the parameterized versions.
    orFallbackToJsonSchema(unparameterizedKinesisIteratorSchema, parameterizedKinesisIteratorSchema)
      .xmap(_.merge) {
        case unparameterized: IteratorType.Unparameterized => Left(unparameterized)
        case parameterized: IteratorType.Parameterized => Right(parameterized)
      }
  }

  val exampleIngestStreamInfo: IngestStreamInfo = IngestStreamInfo(
    status = IngestStreamStatus.Running,
    message = None,
    settings = KafkaIngest(
      topics = Left(Set("e1-source")),
      bootstrapServers = "localhost:9092",
      groupId = Some("quine-e1-ingester"),
      offsetCommitting = None,
      endingOffset = None,
      maximumPerSecond = None
    ),
    stats = IngestStreamStats.example
  )
  val exampleIngestStreamInfoWithName: IngestStreamInfoWithName =
    exampleIngestStreamInfo.withName("log1-entity-ingest-source")

  implicit lazy val kafkaSubscriptionSchema: JsonSchema[Either[KafkaIngest.Topics, KafkaIngest.PartitionAssignments]] =
    orFallbackToJsonSchema[KafkaIngest.Topics, KafkaIngest.PartitionAssignments](implicitly, implicitly)
  implicit lazy val kafkaSecurityProtocolSchema: JsonSchema[KafkaSecurityProtocol] =
    stringEnumeration(KafkaSecurityProtocol.values)(_.name)
  implicit lazy val kafkaAutoOffsetResetSchema: JsonSchema[KafkaAutoOffsetReset] =
    stringEnumeration(KafkaAutoOffsetReset.values)(_.name)
  implicit lazy val kafkaOffsetCommittingSchema: JsonSchema[KafkaOffsetCommitting] =
    genericJsonSchema[KafkaOffsetCommitting]
  implicit lazy val ingestStreamConfigurationSchema: JsonSchema[IngestStreamConfiguration] =
    genericJsonSchema[IngestStreamConfiguration].withExample(exampleIngestStreamInfo.settings)
  implicit lazy val ingestStreamStatsSchema: JsonSchema[IngestStreamStats] =
    genericJsonSchema[IngestStreamStats].withExample(exampleIngestStreamInfo.stats)
  implicit lazy val ingestStreamInfoSchema: JsonSchema[IngestStreamInfo] =
    genericJsonSchema[IngestStreamInfo].withExample(exampleIngestStreamInfo)
  implicit lazy val ingestStreamInfoWithNameSchema: JsonSchema[IngestStreamInfoWithName] =
    genericJsonSchema[IngestStreamInfoWithName].withExample(exampleIngestStreamInfoWithName)
  implicit lazy val fileIngestModeSchema: JsonSchema[FileIngestMode] =
    stringEnumeration(FileIngestMode.values)(_.toString)
}

object IngestRoutes {
  val defaultWriteParallelism: Int = 16
  val defaultMaximumLineSize: Int = 128 * 1024 * 1024 // 128MB
  val defaultStreamedRecordFormat: StreamedRecordFormat.CypherJson = StreamedRecordFormat.CypherJson("CREATE ($that)")
  val defaultFileRecordFormat: FileIngestFormat.CypherJson = FileIngestFormat.CypherJson("CREATE ($that)", "that")
}
trait IngestRoutes
    extends endpoints4s.algebra.Endpoints
    with endpoints4s.algebra.JsonEntitiesFromSchemas
    with IngestSchemas
    with exts.QuineEndpoints {

  private val ingest: Path[Unit] = path / "api" / "v1" / "ingest"

  private[this] val ingestStreamTag: Tag = Tag("Ingest Streams")
    .withDescription(Some("Sources of streaming data ingested into the graph interpreter."))

  val ingestStreamName: Path[String] =
    segment[String]("name", docs = Some("unique name for an ingest stream"))

  val ingestStreamStart: Endpoint[(String, IngestStreamConfiguration), Either[ClientErrors, Unit]] =
    endpoint(
      request = post(
        url = ingest / ingestStreamName,
        entity = jsonRequest[IngestStreamConfiguration]
      ),
      response = badRequest(docs = Some("Ingest stream exists already"))
        .orElse(ok(emptyResponse)),
      docs = EndpointDocs()
        .withSummary(Some("create a new ingest stream"))
        .withDescription(
          Some(
            """Start up a new ingest stream, built up based on the settings passed in. The name
              |given to the stream just needs to be a unique identifier that can be then used to
              |identify the ingest stream (for example, to cancel it).""".stripMargin
          )
        )
        .withTags(List(ingestStreamTag))
    )

  val ingestStreamStop: Endpoint[String, Option[IngestStreamInfoWithName]] =
    endpoint(
      request = delete(
        url = ingest / ingestStreamName
      ),
      response = wheneverFound(ok(jsonResponse[IngestStreamInfoWithName])),
      docs = EndpointDocs()
        .withSummary(Some("cancel a running ingest stream"))
        .withDescription(
          Some(
            """Cancel an already running ingest stream, where the name is the one that was given when
            |the stream was started. If this successfully returns, it means that the stream is no
            |longer ingesting (in other words: the request will stay pending as the ingest stream
            |shuts down).
            |
            |The information returned describes the ingest stream that was just cancelled.
            |""".stripMargin
          )
        )
        .withTags(List(ingestStreamTag))
    )

  val ingestStreamLookup: Endpoint[String, Option[IngestStreamInfoWithName]] =
    endpoint(
      request = get(
        url = ingest / ingestStreamName
      ),
      response = wheneverFound(ok(jsonResponse[IngestStreamInfoWithName])),
      docs = EndpointDocs()
        .withSummary(Some("look up a running ingest stream"))
        .withDescription(
          Some(
            """|Look up a running ingest stream, using the name that was given when the ingest stream
               |was initially started.""".stripMargin
          )
        )
        .withTags(List(ingestStreamTag))
    )

  val ingestStreamPause: Endpoint[String, Option[IngestStreamInfoWithName]] =
    endpoint(
      request = put(
        url = ingest / ingestStreamName / "pause",
        entity = emptyRequest
      ),
      response = wheneverFound(ok(jsonResponse[IngestStreamInfoWithName])),
      docs = EndpointDocs()
        .withSummary(Some("pause a running ingest stream"))
        .withTags(List(ingestStreamTag))
    )

  val ingestStreamUnpause: Endpoint[String, Option[IngestStreamInfoWithName]] =
    endpoint(
      request = put(
        url = ingest / ingestStreamName / "start",
        entity = emptyRequest
      ),
      response = wheneverFound(ok(jsonResponse[IngestStreamInfoWithName])),
      docs = EndpointDocs()
        .withSummary(Some("unpause a paused ingest stream"))
        .withTags(List(ingestStreamTag))
    )

  val ingestStreamList: Endpoint[Unit, Map[String, IngestStreamInfo]] =
    endpoint(
      request = get(
        url = ingest
      ),
      response = ok(
        jsonResponseWithExample[Map[String, IngestStreamInfo]](
          Map(exampleIngestStreamInfoWithName.name -> exampleIngestStreamInfo)
        )
      ),
      docs = EndpointDocs()
        .withSummary(Some("list all running ingest streams"))
        .withDescription(
          Some(
            """|List all currently running ingest streams, keyed by the names under which they were
               |registered.""".stripMargin
          )
        )
        .withTags(List(ingestStreamTag))
    )
}
