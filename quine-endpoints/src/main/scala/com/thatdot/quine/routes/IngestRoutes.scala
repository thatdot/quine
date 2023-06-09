package com.thatdot.quine.routes

import java.time.Instant

import scala.util.control.NoStackTrace

import cats.data.NonEmptyList
import endpoints4s.algebra.Tag
import endpoints4s.generic.{docs, title, unnamed}

import com.thatdot.quine.routes.exts.EndpointsWithCustomErrorText

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
@unnamed
@docs("An active stream of data being ingested paired with a name for the stream.")
final case class IngestStreamInfoWithName(
  @docs("Unique name identifying the ingest stream") name: String,
  @docs(
    "Indicator of whether the ingest is still running, completed, etc."
  ) status: IngestStreamStatus,
  @docs("Error message about the ingest, if any") message: Option[String],
  @docs("Configuration of the ingest stream") settings: IngestStreamConfiguration,
  @docs("Statistics on progress of running ingest stream") stats: IngestStreamStats
)

@title("Ingest Stream Info")
@docs("An active stream of data being ingested.")
final case class IngestStreamInfo(
  @docs(
    "Indicator of whether the ingest is still running, completed, etc."
  ) status: IngestStreamStatus,
  @docs("Error message about the ingest, if any") message: Option[String],
  @docs("Configuration of the ingest stream") settings: IngestStreamConfiguration,
  @docs("Statistics on progress of running ingest stream") stats: IngestStreamStats
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
@unnamed
final case class IngestStreamStats(
  // NB this is duplicated by rates.count -- maybe remove one?
  @docs("Number of source records (or lines) ingested so far") ingestedCount: Long,
  @docs("Records/second over different time periods") rates: RatesSummary,
  @docs("Bytes/second over different time periods") byteRates: RatesSummary,
  @docs("Time (in ISO-8601 UTC time) when the ingestion was started") startTime: Instant,
  @docs("Time (in milliseconds) that that the ingest has been running") totalRuntime: Long
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

@unnamed
@title("Rates Summary")
@docs("Summary statistics about a metered rate (ie, count per second).")
final case class RatesSummary(
  @docs("Number of items metered") count: Long,
  @docs("Approximate rate per second in the last minute") oneMinute: Double,
  @docs("Approximate rate per second in the last five minutes") fiveMinute: Double,
  @docs("Approximate rate per second in the last fifteen minutes") fifteenMinute: Double,
  @docs("Approximate rate per second since the meter was started") overall: Double
)

trait MetricsSummarySchemas extends endpoints4s.generic.JsonSchemas {
  implicit lazy val ratesSummarySchema: Record[RatesSummary] =
    genericRecord[RatesSummary]
}

@unnamed
@title("AWS Credentials")
@docs(
  "Explicit AWS access key and secret to use. If not provided, defaults to environmental credentials according to the default AWS credential chain. See: <https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html#credentials-default>."
)
final case class AwsCredentials(accessKeyId: String, secretAccessKey: String)

@unnamed
@title("AWS Region")
@docs(
  "AWS region code. e.g. `us-west-2`. If not provided, defaults according to the default AWS region provider chain. See: <https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/region-selection.html#automatically-determine-the-aws-region-from-the-environment>."
)
final case class AwsRegion(region: String)

trait AwsConfigurationSchemas extends endpoints4s.generic.JsonSchemas {
  implicit lazy val awsCredentialsSchema: Record[AwsCredentials] = genericRecord[AwsCredentials]
  implicit val awsRegionSchema: Record[AwsRegion] = genericRecord[AwsRegion]
}

@unnamed
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

@unnamed
@title("Kafka Security Protocol")
@docs(
  "See [`security.protocol` in the Kafka documentation](https://kafka.apache.org/24/javadoc/org/apache/kafka/common/security/auth/SecurityProtocol.html)."
)
sealed abstract class KafkaSecurityProtocol(val name: String)
object KafkaSecurityProtocol {
  case object PlainText extends KafkaSecurityProtocol("PLAINTEXT")
  case object Ssl extends KafkaSecurityProtocol("SSL")
  case object Sasl_Ssl extends KafkaSecurityProtocol("SASL_SSL")
  case object Sasl_Plaintext extends KafkaSecurityProtocol("SASL_PLAINTEXT")
  val values: Seq[KafkaSecurityProtocol] = Seq(PlainText, Ssl, Sasl_Ssl, Sasl_Plaintext)
}

@unnamed
@title("Kafka offset tracking mechanism")
@docs(
  "How to keep track of current offset when consuming from Kafka, if at all. " +
  """You could alternatively set "enable.auto.commit": "true" in kafkaProperties  for this ingest, """ +
  "but in that case messages will be lost if the ingest is stopped while processing messages"
)
sealed abstract class KafkaOffsetCommitting
object KafkaOffsetCommitting {
  @unnamed
  @title("Explicit Commit")
  @docs(
    "Commit offsets to the specified Kafka consumer group on successful execution of the ingest query for that record."
  )
  final case class ExplicitCommit(
    @docs("Maximum number of messages in a single commit batch.")
    maxBatch: Long = 1000,
    @docs("Maximum interval between commits in milliseconds.")
    maxIntervalMillis: Int = 10000,
    @docs("Parallelism for async committing.")
    parallelism: Int = 100,
    @docs("Wait for a confirmation from Kafka on ack.")
    waitForCommitConfirmation: Boolean = true
  ) extends KafkaOffsetCommitting
}
@title("Scheduler Checkpoint Settings")
@docs("Settings for batch configuration for Kinesis stream checkpointing.")
@unnamed
final case class KinesisCheckpointSettings(
  @docs("Maximum checkpoint batch size.")
  maxBatchSize: Int,
  @docs("Maximum checkpoint batch wait time in ms.")
  maxBatchWait: Long
)
@title("Ingest Stream Configuration")
@docs("A specification of a data source and rules for consuming data from that source.")
sealed abstract class IngestStreamConfiguration
object IngestStreamConfiguration {
  case class InvalidStreamConfiguration(errors: NonEmptyList[String])
      extends Exception(s"Encountered errors in provided ingest configuration: ${errors.toList.mkString("; ")}")
      with NoStackTrace
}

/** Type used to persist ingest stream configurations alongside their status for later restoration.
  *
  * @param config Ingest stream configuration
  * @param status Status of the ingest stream
  */
final case class IngestStreamWithStatus(
  config: IngestStreamConfiguration,
  status: Option[IngestStreamStatus]
)

object KafkaIngest {
  // Takes a set of topic names
  type Topics = Set[String]
  // Takes a set of partition numbers for each topic name.
  type PartitionAssignments = Map[String, Set[Int]]
  // Takes a map of kafka properties
  type KafkaProperties = Map[String, String]
}
@title("Record encoding")
@docs("Record encoding format")
sealed abstract class RecordDecodingType
object RecordDecodingType {
  @docs("Zlib compression")
  case object Zlib extends RecordDecodingType
  @docs("Gzip compression")
  case object Gzip extends RecordDecodingType
  @docs("Base64 encoding")
  case object Base64 extends RecordDecodingType

  val values: Seq[RecordDecodingType] = Seq(Zlib, Gzip, Base64)

}

/** Kafka ingest stream configuration
  *
  * @param format how the Kafka records are encoded
  * @param topics from which topics to read data
  * @param parallelism maximum number of records to process at once
  * @param bootstrapServers comma-separated list of host/port pairs
  * @param groupId consumer group this consumer belongs to
  * @param kafkaProperties kafka client properties
  */

@unnamed
@title("Kafka Ingest Stream")
@docs("A stream of data being ingested from Kafka.")
final case class KafkaIngest(
  @docs("The format used to decode each Kafka record.")
  format: StreamedRecordFormat = IngestRoutes.defaultStreamedRecordFormat,
  @docs(
    """Kafka topics from which to ingest: Either an array of topic names, or an object whose keys are topic names and
      |whose values are partition indices.""".stripMargin
      .replace('\n', ' ')
  )
  topics: Either[KafkaIngest.Topics, KafkaIngest.PartitionAssignments],
  @docs("Maximum number of records to process at once.")
  parallelism: Int = IngestRoutes.defaultWriteParallelism,
  @docs("A comma-separated list of Kafka broker servers.")
  bootstrapServers: String,
  @docs(
    "Consumer group ID that this ingest stream should report belonging to; defaults to the name of the ingest stream."
  )
  groupId: Option[String],
  securityProtocol: KafkaSecurityProtocol = KafkaSecurityProtocol.PlainText,
  offsetCommitting: Option[KafkaOffsetCommitting],
  autoOffsetReset: KafkaAutoOffsetReset = KafkaAutoOffsetReset.Latest,
  @docs(
    "Map of Kafka client properties. See <https://docs.confluent.io/platform/current/installation/configuration/consumer-configs.html#ak-consumer-configurations-for-cp>"
  )
  kafkaProperties: KafkaIngest.KafkaProperties = Map.empty[String, String],
  @docs(
    "The offset at which this stream should complete; offsets are sequential integers starting at 0."
  ) endingOffset: Option[Long],
  @docs("Maximum records to process per second.")
  maximumPerSecond: Option[Int],
  @docs("List of decodings to be applied to each input. The specified decodings are applied in declared array order.")
  @unnamed
  recordDecoders: Seq[RecordDecodingType] = Seq.empty
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

@title("Pulsar Ingest Stream")
@unnamed
@docs("A stream of data being ingested from Pulsar.")
final case class PulsarIngest(
  @docs("The format used to decode each Pulsar record.")
  format: StreamedRecordFormat = IngestRoutes.defaultStreamedRecordFormat,
  @docs("Name of the Pulsar topic to ingest.") topics: Seq[String],
  @docs("Pulsar service url.") pulsarUrl: String,
  @docs("Pulsar subscription key.") subscriptionName: String,
  @docs("Pulsar subscription type.") subscriptionType: PulsarSubscriptionType,
  @docs("Maximum number of records to write simultaneously.")
  parallelism: Int = IngestRoutes.defaultWriteParallelism,
  @docs("Maximum records to process per second.") maximumPerSecond: Option[Int],
  @docs("List of decodings to be applied to each input, where specified decodings are applied in declared array order.")
  recordDecoders: Seq[RecordDecodingType] = Seq.empty
) extends IngestStreamConfiguration

@title("Kinesis Data Stream")
@unnamed
@docs("A stream of data being ingested from Kinesis.")
final case class KinesisIngest(
  @docs("The format used to decode each Kinesis record.")
  format: StreamedRecordFormat = IngestRoutes.defaultStreamedRecordFormat,
  @docs("Name of the Kinesis stream to ingest.") streamName: String,
  @docs(
    "Shards IDs within the named kinesis stream to ingest; if empty or excluded, all shards on the stream are processed."
  )
  shardIds: Option[Set[String]],
  @docs("Maximum number of records to write simultaneously.")
  parallelism: Int = IngestRoutes.defaultWriteParallelism,
  credentials: Option[AwsCredentials],
  region: Option[AwsRegion],
  @docs("Shard iterator type.") iteratorType: KinesisIngest.IteratorType = KinesisIngest.IteratorType.Latest,
  @docs("Number of retries to attempt on Kineses error.") numRetries: Int = 3,
  @docs("Maximum records to process per second.") maximumPerSecond: Option[Int],
  @docs("List of decodings to be applied to each input, where specified decodings are applied in declared array order.")
  recordDecoders: Seq[RecordDecodingType] = Seq.empty,
  @docs(
    "Optional stream checkpoint settings. If present, checkpointing will manage `iteratorType` and `shardIds`, ignoring those fields in the API request."
  )
  checkpointSettings: Option[KinesisCheckpointSettings]
) extends IngestStreamConfiguration

@title("Server Sent Events Stream")
@unnamed
@docs(
  "A server-issued event stream, as might be handled by the EventSource JavaScript API. Only consumes the `data` portion of an event."
)
final case class ServerSentEventsIngest(
  @docs("Format used to decode each event's `data`.")
  format: StreamedRecordFormat = IngestRoutes.defaultStreamedRecordFormat,
  @docs("URL of the server sent event stream.") url: String,
  @docs("Maximum number of records to ingest simultaneously.")
  parallelism: Int = IngestRoutes.defaultWriteParallelism,
  @docs("Maximum records to process per second.") maximumPerSecond: Option[Int],
  @docs(
    "List of encodings that have been applied to each input. Decoding of each type is applied in order."
  ) recordDecoders: Seq[RecordDecodingType] = Seq.empty
) extends IngestStreamConfiguration

@title("Simple Queue Service Queue")
@unnamed
@docs("An active stream of data being ingested from AWS SQS.")
final case class SQSIngest(
  @docs("Format used to decode each queued record.")
  format: StreamedRecordFormat = IngestRoutes.defaultStreamedRecordFormat,
  @docs("URL of the queue to ingest.") queueUrl: String,
  @docs("Maximum number of records to read from the queue simultaneously.") readParallelism: Int = 1,
  @docs("Maximum number of records to ingest simultaneously.")
  writeParallelism: Int = IngestRoutes.defaultWriteParallelism,
  credentials: Option[AwsCredentials],
  region: Option[AwsRegion],
  @docs("Whether the queue consumer should acknowledge receipt of in-flight messages.")
  deleteReadMessages: Boolean = true,
  @docs("Maximum records to process per second.") maximumPerSecond: Option[Int],
  @docs("List of decodings to be applied to each input, where specified decodings are applied in declared array order.")
  recordDecoders: Seq[RecordDecodingType] = Seq.empty
) extends IngestStreamConfiguration

object WebsocketSimpleStartupIngest {
  @unnamed
  @title("Websockets Keepalive Protocol")
  sealed abstract class KeepaliveProtocol
  @unnamed
  @title("Ping/Pong on interval")
  @docs("Send empty websocket messages at the specified interval (in milliseconds).")
  final case class PingPongInterval(intervalMillis: Int = 5000) extends KeepaliveProtocol
  @unnamed
  @title("Text Keepalive Message on Interval")
  @docs("Send the same text-based Websocket message at the specified interval (in milliseconds).")
  final case class SendMessageInterval(message: String, intervalMillis: Int = 5000) extends KeepaliveProtocol
  @unnamed
  @title("No Keepalive")
  @docs("Only send data messages, no keepalives.")
  final case object NoKeepalive extends KeepaliveProtocol
}
@title("Websockets Ingest Stream (Simple Startup)")
@unnamed
@docs("A websocket stream started after a sequence of text messages.")
final case class WebsocketSimpleStartupIngest(
  @docs("Format used to decode each incoming message.")
  format: StreamedRecordFormat = IngestRoutes.defaultStreamedRecordFormat,
  @docs("Websocket (ws: or wss:) url to connect to.")
  url: String,
  @docs("Initial messages to send to the server on connecting.")
  initMessages: Seq[String] = Seq.empty,
  @docs("Strategy to use for sending keepalive messages, if any.")
  keepAlive: WebsocketSimpleStartupIngest.KeepaliveProtocol = WebsocketSimpleStartupIngest.PingPongInterval(),
  @docs("Maximum number of records to ingest simultaneously.")
  parallelism: Int = IngestRoutes.defaultWriteParallelism,
  @docs(s"""Text encoding used to read text messages in the stream. Only UTF-8, US-ASCII and ISO-8859-1 are directly
           |supported -- other encodings will transcoded to UTF-8 on the fly (and ingest may be slower).
           |""".stripMargin)
  encoding: String = "UTF-8"
) extends IngestStreamConfiguration

@title("Streamed Record Format")
@unnamed
@docs("Format by which streamed records are decoded.")
sealed abstract class StreamedRecordFormat
object StreamedRecordFormat {

  @title("JSON via Cypher")
  @unnamed
  @docs("""Records are JSON values. For every record received, the
  |given Cypher query will be re-executed with the parameter in the query set
  |equal to the new JSON value.
  """.stripMargin)
  final case class CypherJson(
    @docs("Cypher query to execute on each record.") query: String,
    @docs("Name of the Cypher parameter to populate with the JSON value.") parameter: String = "that"
  ) extends StreamedRecordFormat

  @title("Raw Bytes via Cypher")
  @unnamed
  @docs("""Records may have any format. For every record received, the
          |given Cypher query will be re-executed with the parameter in the query set
          |equal to the new value as a Cypher byte array.
  """.stripMargin)
  final case class CypherRaw(
    @docs("Cypher query to execute on each record.") query: String,
    @docs("Name of the Cypher parameter to populate with the byte array.") parameter: String = "that"
  ) extends StreamedRecordFormat

  @title("Protobuf via Cypher")
  @unnamed
  @docs(
    "Records are serialized instances of `typeName` as described in the schema (a `.desc` descriptor file) at " +
    "`schemaUrl`. For every record received, the given Cypher query will be re-executed with the parameter " +
    "in the query set equal to the new (deserialized) Protobuf message."
  )
  final case class CypherProtobuf(
    @docs("Cypher query to execute on each record.") query: String,
    @docs("Name of the Cypher parameter to populate with the Protobuf message.") parameter: String = "that",
    @docs(
      "URL (or local filename) of the Protobuf `.desc` file to load to parse the `typeName`."
    ) schemaUrl: String,
    @docs(
      "Message type name to use from the given `.desc` file as the incoming message type."
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
  @docs("Local file path.")
  path: String,
  @docs(
    "The text encoding scheme for the file. UTF-8, US-ASCII and ISO-8859-1 are " +
    "supported -- other encodings will transcoded to UTF-8 on the fly (and ingest may be slower)."
  )
  encoding: String = "UTF-8",
  @docs("Maximum number of records to process at once.")
  parallelism: Int = IngestRoutes.defaultWriteParallelism,
  @docs("Maximum size (in bytes) of any line in the file.")
  maximumLineSize: Int = IngestRoutes.defaultMaximumLineSize,
  @docs(
    s"""Begin processing at the record with the given index. Useful for skipping some number of lines (e.g. CSV headers) or
           |resuming ingest from a partially consumed file.""".stripMargin
  )
  startAtOffset: Long = 0L,
  @docs(s"Optionally limit how many records are ingested from this file.")
  ingestLimit: Option[Long],
  @docs("Maximum number of records to process per second.")
  maximumPerSecond: Option[Int],
  @docs(
    "Ingest mode for reading from a non-regular file type; default is to auto-detect if file is named pipe."
  ) fileIngestMode: Option[FileIngestMode]
) extends IngestStreamConfiguration

/** Standard input ingest stream configuration */
@title("Standard Input Ingest Stream")
@unnamed
@docs("An active stream of data being ingested from standard input to this Quine process.")
final case class StandardInputIngest(
  format: FileIngestFormat = IngestRoutes.defaultFileRecordFormat,
  @docs(
    "Text encoding used to read data. Only UTF-8, US-ASCII and ISO-8859-1 are directly supported " +
    "-- other encodings will be transcoded to UTF-8 on the fly (and ingest may be slower)."
  )
  encoding: String = "UTF-8",
  @docs("Maximum number of records process at once.")
  parallelism: Int = IngestRoutes.defaultWriteParallelism,
  @docs("Maximum size (in bytes) of any line.")
  maximumLineSize: Int = IngestRoutes.defaultMaximumLineSize,
  @docs("Maximum records to process per second.")
  maximumPerSecond: Option[Int]
) extends IngestStreamConfiguration

/** Number iterator ingest source for easy testing */
@unnamed
@title("Number Iterator Ingest")
@docs(
  "An infinite ingest stream which requires no data source and just produces new sequential numbers" +
  " every time the stream is (re)started. The numbers are Java `Long`s` and will wrap at their max value."
)
case class NumberIteratorIngest(
  format: FileIngestFormat = IngestRoutes.defaultNumberFormat,
  @docs("Begin the stream with this number.")
  startAt: Long = 0L,
  @docs("Optionally end the stream after consuming this many items.")
  ingestLimit: Option[Long],
  @docs(
    "Limit the maximum rate of production to this many records per second. Note that this may be slowed by " +
    "backpressure elsewhere in the system."
  )
  throttlePerSecond: Option[Int],
  @docs("Maximum number of records to process at once.")
  parallelism: Int = IngestRoutes.defaultWriteParallelism
) extends IngestStreamConfiguration

@unnamed
@title("File Ingest Format")
@docs("Format by which a file will be interpreted as a stream of elements for ingest.")
sealed abstract class FileIngestFormat
object FileIngestFormat {

  /** Create using a cypher query, passing each line in as a string */
  @title("CypherLine")
  @unnamed()
  @docs("""For every line (LF/CRLF delimited) in the source, the given Cypher query will be
  |re-executed with the parameter in the query set equal to a string matching
  |the new line value. The newline is not included in this string.
  """.stripMargin.replace('\n', ' '))
  final case class CypherLine(
    @docs("Cypher query to execute on each line") query: String,
    @docs("name of the Cypher parameter holding the string line value") parameter: String = "that"
  ) extends FileIngestFormat

  /** Create using a cypher query, expecting each line to be a JSON record */
  @title("CypherJson")
  @unnamed()
  @docs("""Lines in the file should be JSON values. For every value received, the
  |given Cypher query will be re-executed with the parameter in the query set
  |equal to the new JSON value.
  """.stripMargin.replace('\n', ' '))
  final case class CypherJson(
    @docs("Cypher query to execute on each record") query: String,
    @docs("name of the Cypher parameter holding the JSON value") parameter: String = "that"
  ) extends FileIngestFormat

  /** Create using a cypher query, expecting each line to be a single row CSV record */
  @title("CypherCSV")
  @unnamed()
  @docs("""For every row in a CSV file, the given Cypher query will be re-executed with the parameter in the query set
          |to the parsed row. Rows are parsed into either a Cypher List of strings or a Map, depending on whether a
          |`headers` row is available.""".stripMargin.replace('\n', ' '))
  final case class CypherCsv(
    @docs("Cypher query to execute on each record.") query: String,
    @docs("Name of the Cypher parameter holding the parsed CSV row.")
    parameter: String = "that",
    @docs("""Read a CSV file containing headers in the file's first row (`true`) or with no headers (`false`).
            |Alternatively, an array of column headers can be passed in. If headers are not supplied, the resulting
            |type available to the Cypher query will be a List of strings with values accessible by index. When
            |headers are available (supplied or read from the file), the resulting type available to the Cypher
            |query will be a Map[String, String], with values accessible using the corresponding header string.
            |CSV rows containing more records than the `headers` will have items that don't match a header column
            |discarded. CSV rows with fewer columns than the `headers` will have `null` values for the missing headers.
            |Default: `false`.""".stripMargin)
    headers: Either[Boolean, List[String]] = Left(false),
    @docs("CSV row delimiter character.")
    delimiter: CsvCharacter = CsvCharacter.Comma,
    @docs("""Character used to quote values in a field. Special characters (like new lines) inside of a quoted
            |section will be a part of the CSV value.""".stripMargin)
    quoteChar: CsvCharacter = CsvCharacter.DoubleQuote,
    @docs("Character used to escape special characters.")
    escapeChar: CsvCharacter = CsvCharacter.Backslash
  ) extends FileIngestFormat {
    require(delimiter != quoteChar, "Different characters must be used for `delimiter` and `quoteChar`.")
    require(delimiter != escapeChar, "Different characters must be used for `delimiter` and `escapeChar`.")
    require(quoteChar != escapeChar, "Different characters must be used for `quoteChar` and `escapeChar`.")
  }
}

@title("File Ingest Mode")
@docs("Determines behavior when ingesting from a non-regular file type.")
sealed abstract class FileIngestMode
object FileIngestMode {
  @docs("Ordinary file to be open and read once")
  case object Regular extends FileIngestMode
  @docs("Named pipe to be regularly reopened and polled for more data")
  case object NamedPipe extends FileIngestMode

  val values: Seq[FileIngestMode] = Seq(Regular, NamedPipe)
}

sealed trait PulsarSubscriptionType {}
object PulsarSubscriptionType {
  case object Exclusive extends PulsarSubscriptionType
  case object Shared extends PulsarSubscriptionType
  case object Failover extends PulsarSubscriptionType
  case object KeyShared extends PulsarSubscriptionType
  val values: Seq[PulsarSubscriptionType] = Seq(Exclusive, Shared, Failover, KeyShared)
}

sealed trait CsvCharacter { def byte: Byte }
object CsvCharacter {
  case object Backslash extends CsvCharacter { def byte: Byte = '\\' }
  case object Comma extends CsvCharacter { def byte: Byte = ',' }
  case object Semicolon extends CsvCharacter { def byte: Byte = ';' }
  case object Colon extends CsvCharacter { def byte: Byte = ':' }
  case object Tab extends CsvCharacter { def byte: Byte = '\t' }
  case object Pipe extends CsvCharacter { def byte: Byte = '|' }
  case object DoubleQuote extends CsvCharacter { def byte: Byte = '"' }
  val values: Seq[CsvCharacter] = Seq(Backslash, Comma, Semicolon, Colon, Tab, Pipe, DoubleQuote)
}

trait IngestSchemas extends endpoints4s.generic.JsonSchemas with AwsConfigurationSchemas with MetricsSummarySchemas {

  implicit lazy val recordEncodingTypeFormatSchema: Enum[RecordDecodingType] =
    stringEnumeration(RecordDecodingType.values)(_.toString)

  implicit lazy val csvHeaderOptionFormatSchema: JsonSchema[Either[Boolean, List[String]]] =
    orFallbackToJsonSchema[Boolean, List[String]](implicitly, implicitly)

  implicit lazy val csvCharacterFormatSchema: Enum[CsvCharacter] =
    stringEnumeration(CsvCharacter.values)(_.toString)

  implicit lazy val entityFormatSchema: Tagged[StreamedRecordFormat] =
    genericTagged[StreamedRecordFormat]
      .withExample(IngestRoutes.defaultStreamedRecordFormat)

  implicit lazy val fileIngestFormatSchema: Tagged[FileIngestFormat] =
    genericTagged[FileIngestFormat]
      .withExample(IngestRoutes.defaultFileRecordFormat)

  implicit val ingestStatusSchema: Enum[IngestStreamStatus] =
    stringEnumeration(IngestStreamStatus.states)(_.toString)
      .withExample(IngestStreamStatus.Running)

  implicit lazy val iteratorTypeSchema: JsonSchema[KinesisIngest.IteratorType] = {
    import KinesisIngest.IteratorType
    val unparameterizedKinesisIteratorSchema: Enum[IteratorType.Unparameterized] =
      stringEnumeration[IteratorType.Unparameterized](
        Seq(IteratorType.TrimHorizon, IteratorType.Latest)
      )(_.toString)

    val parameterizedKinesisIteratorSchema: Tagged[IteratorType.Parameterized] =
      genericTagged[IteratorType.Parameterized]

    // Try the string enumeration first, then try the parameterized versions.
    orFallbackToJsonSchema(unparameterizedKinesisIteratorSchema, parameterizedKinesisIteratorSchema)
      .xmap(_.merge) {
        case unparameterized: IteratorType.Unparameterized => Left(unparameterized)
        case parameterized: IteratorType.Parameterized => Right(parameterized)
      }
  }

  implicit val kinesisCheckpointSettingsSchema: Record[KinesisCheckpointSettings] =
    genericRecord[KinesisCheckpointSettings].withExample(KinesisCheckpointSettings(100, 1000))

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

  // TODO review which of these are necessary
  implicit lazy val pulsarSubscriptTypeSchema: Enum[PulsarSubscriptionType] =
    stringEnumeration(PulsarSubscriptionType.values)(_.toString)
  implicit lazy val kafkaSubscriptionSchema: JsonSchema[Either[KafkaIngest.Topics, KafkaIngest.PartitionAssignments]] =
    orFallbackToJsonSchema[KafkaIngest.Topics, KafkaIngest.PartitionAssignments](implicitly, implicitly)
  implicit lazy val kafkaSecurityProtocolSchema: Enum[KafkaSecurityProtocol] =
    stringEnumeration(KafkaSecurityProtocol.values)(_.name)
  implicit lazy val kafkaAutoOffsetResetSchema: Enum[KafkaAutoOffsetReset] =
    stringEnumeration(KafkaAutoOffsetReset.values)(_.name)
  implicit lazy val kafkaOffsetCommittingSchema: Tagged[KafkaOffsetCommitting] =
    genericTagged[KafkaOffsetCommitting]
  implicit lazy val wsKeepaliveSchema: Tagged[WebsocketSimpleStartupIngest.KeepaliveProtocol] =
    genericTagged[WebsocketSimpleStartupIngest.KeepaliveProtocol]
  implicit lazy val ingestStreamConfigurationSchema: Tagged[IngestStreamConfiguration] =
    genericTagged[IngestStreamConfiguration].withExample(exampleIngestStreamInfo.settings)
  implicit lazy val ingestStreamStatsSchema: Record[IngestStreamStats] =
    genericRecord[IngestStreamStats].withExample(exampleIngestStreamInfo.stats)
  implicit lazy val ingestStreamInfoSchema: Record[IngestStreamInfo] =
    genericRecord[IngestStreamInfo].withExample(exampleIngestStreamInfo)
  implicit lazy val ingestStreamInfoWithNameSchema: Record[IngestStreamInfoWithName] =
    genericRecord[IngestStreamInfoWithName].withExample(exampleIngestStreamInfoWithName)
  implicit lazy val fileIngestModeSchema: Enum[FileIngestMode] =
    stringEnumeration(FileIngestMode.values)(_.toString)

  implicit lazy val ingestStreamWithStatus: Record[IngestStreamWithStatus] =
    genericRecord[IngestStreamWithStatus]

}

object IngestRoutes {
  val defaultWriteParallelism: Int = 16
  val defaultMaximumLineSize: Int = 128 * 1024 * 1024 // 128MB
  val defaultStreamedRecordFormat: StreamedRecordFormat.CypherJson = StreamedRecordFormat.CypherJson("CREATE ($that)")
  val defaultFileRecordFormat: FileIngestFormat.CypherJson = FileIngestFormat.CypherJson("CREATE ($that)")
  val defaultNumberFormat: FileIngestFormat.CypherLine = FileIngestFormat.CypherLine(
    "MATCH (x) WHERE id(x) = idFrom(toInteger($that)) SET x.i = toInteger($that)"
  )
}
trait IngestRoutes
    extends EndpointsWithCustomErrorText
    with endpoints4s.algebra.JsonEntitiesFromSchemas
    with IngestSchemas
    with exts.QuineEndpoints {

  private val ingest: Path[Unit] = path / "api" / "v1" / "ingest"

  private[this] val ingestStreamTag: Tag = Tag("Ingest Streams")
    .withDescription(Some("Sources of streaming data ingested into the graph interpreter."))

  val ingestStreamName: Path[String] =
    segment[String]("name", docs = Some("Ingest stream name"))

  val ingestStreamStart: Endpoint[(String, IngestStreamConfiguration), Either[ClientErrors, Unit]] =
    endpoint(
      request = post(
        url = ingest / segment[String]("name", Some("Unique name for the ingest stream")),
        entity = jsonOrYamlRequest[IngestStreamConfiguration]
      ),
      response = customBadRequest("Ingest stream exists already")
        .orElse(ok(emptyResponse)),
      docs = EndpointDocs()
        .withSummary(Some("Create Ingest Stream"))
        .withDescription(
          Some(
            """Create an [ingest stream](https://docs.quine.io/components/ingest-sources/ingest-sources.html) 
              |that connects a streaming event source to Quine and loads data into the graph.
              |
              |An ingest stream is defined by selecting a source `type`, then an appropriate data `format`, 
              |and must be created with a unique name. Many ingest stream types allow a Cypher query to operate 
              |on the event stream data to create nodes and relationships in the graph.""".stripMargin
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
        .withSummary(Some("Delete Ingest Stream"))
        .withDescription(
          Some(
            """Immediately halt and remove the named ingest stream from Quine.
              | 
              |The ingest stream will complete any pending operations and return stream information 
              |once the operation is complete.""".stripMargin
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
        .withSummary(Some("Ingest Stream Status"))
        .withDescription(
          Some("Return the ingest stream status information for a configured ingest stream by name.")
        )
        .withTags(List(ingestStreamTag))
    )

  val ingestStreamPause: Endpoint[String, Either[ClientErrors, Option[IngestStreamInfoWithName]]] =
    endpoint(
      request = put(
        url = ingest / ingestStreamName / "pause",
        entity = emptyRequest
      ),
      response = customBadRequest("Cannot pause failed ingest").orElse(
        wheneverFound(ok(jsonResponse[IngestStreamInfoWithName]))
      ),
      docs = EndpointDocs()
        .withSummary(Some("Pause Ingest Stream"))
        .withDescription(Some("Temporarily pause processing new events by the named ingest stream."))
        .withTags(List(ingestStreamTag))
    )

  val ingestStreamUnpause: Endpoint[String, Either[ClientErrors, Option[IngestStreamInfoWithName]]] =
    endpoint(
      request = put(
        url = ingest / ingestStreamName / "start",
        entity = emptyRequest
      ),
      response = customBadRequest("Cannot resume failed ingest").orElse(
        wheneverFound(ok(jsonResponse[IngestStreamInfoWithName]))
      ),
      docs = EndpointDocs()
        .withSummary(Some("Unpause Ingest Stream"))
        .withDescription(Some("Resume processing new events by the named ingest stream."))
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
        .withSummary(Some("List Ingest Streams"))
        .withDescription(
          Some(
            """Return a JSON object containing the configured [ingest streams](https://docs.quine.io/components/ingest-sources/ingest-sources.html)
              |and their associated stream metrics keyed by the stream name. """.stripMargin
          )
        )
        .withTags(List(ingestStreamTag))
    )
}
