package com.thatdot.quine.app.v2api.definitions

import java.nio.charset.Charset
import java.time.Instant

import com.typesafe.scalalogging.LazyLogging
import sttp.tapir.Schema.annotations.{default, description, title}

import com.thatdot.quine.{routes => V1}

object ApiIngest {

  sealed abstract class ValvePosition(position: String)

  object ValvePosition {

    case object Open extends ValvePosition("Open")
    case object Closed extends ValvePosition("Closed")

  }

  /** Type used to persist ingest stream configurations alongside their status for later restoration.
    *
    * @param config Ingest stream configuration
    * @param status Status of the ingest stream
    */
  final case class IngestStreamWithStatus(
    config: IngestSource,
    status: Option[IngestStreamStatus],
  )

  @title("Rates Summary")
  @description("Summary statistics about a metered rate (ie, count per second).")
  final case class RatesSummary(
    @description("Number of items metered") count: Long,
    @description("Approximate rate per second in the last minute") oneMinute: Double,
    @description("Approximate rate per second in the last five minutes") fiveMinute: Double,
    @description("Approximate rate per second in the last fifteen minutes") fifteenMinute: Double,
    @description("Approximate rate per second since the meter was started") overall: Double,
  )

  @title("Statistics About a Running Ingest Stream")
  final case class IngestStreamStats(
    // NB this is duplicated by rates.count -- maybe remove one?
    @description("Number of source records (or lines) ingested so far") ingestedCount: Long,
    @description("Records/second over different time periods") rates: RatesSummary,
    @description("Bytes/second over different time periods") byteRates: RatesSummary,
    @description("Time (in ISO-8601 UTC time) when the ingestion was started") startTime: Instant,
    @description("Time (in milliseconds) that that the ingest has been running") totalRuntime: Long,
  )

  @title("Ingest Stream Info")
  @description("An active stream of data being ingested.")
  final case class IngestStreamInfo(
    @description(
      "Indicator of whether the ingest is still running, completed, etc.",
    ) status: IngestStreamStatus,
    @description("Error message about the ingest, if any") message: Option[String],
    @description("Configuration of the ingest stream") settings: IngestSource,
    @description("Statistics on progress of running ingest stream") stats: IngestStreamStats,
  ) {
    def withName(name: String): IngestStreamInfoWithName = IngestStreamInfoWithName(
      name = name,
      status = status,
      message = message,
      settings = settings,
      stats = stats,
    )
  }

  @title("Named Ingest Stream")
  @description("An active stream of data being ingested paired with a name for the stream.")
  final case class IngestStreamInfoWithName(
    @description("Unique name identifying the ingest stream") name: String,
    @description(
      "Indicator of whether the ingest is still running, completed, etc.",
    ) status: IngestStreamStatus,
    @description("Error message about the ingest, if any") message: Option[String],
    @description("Configuration of the ingest stream") settings: IngestSource,
    @description("Statistics on progress of running ingest stream") stats: IngestStreamStats,
  )

  sealed abstract class IngestStreamStatus(val isTerminal: Boolean, val position: ValvePosition)

  object IngestStreamStatus {
    def decideRestoredStatus(
      statusAtShutdown: IngestStreamStatus,
      shouldResumeRestoredIngests: Boolean,
    ): IngestStreamStatus =
      statusAtShutdown match {
        case status: TerminalStatus =>
          // A terminated ingest should stay terminated, even if the system restarts
          status
        case Paused =>
          // An ingest that was explicitly paused by the user before restart should come back in a paused state
          Paused
        case Running | Restored =>
          // An ingest that is poised to be started should defer to the user's preference for whether
          // to start or stay in a soft-paused state
          if (shouldResumeRestoredIngests) Running else Restored
      }

    sealed abstract class TerminalStatus(
      @default(true)
      override val isTerminal: Boolean = true,
      @default("Closed")
      override val position: ValvePosition = ValvePosition.Closed,
    ) extends IngestStreamStatus(isTerminal, position)

    @description(
      "The stream is currently actively running, and possibly waiting for new records to become available upstream.",
    )
    case object Running extends IngestStreamStatus(isTerminal = false, position = ValvePosition.Open)

    @description("The stream has been paused by a user.")
    case object Paused extends IngestStreamStatus(isTerminal = false, position = ValvePosition.Closed)

    @description(
      "The stream has been restored from a saved state, but is not yet running: For example, after restarting the application.",
    )
    case object Restored extends IngestStreamStatus(isTerminal = false, position = ValvePosition.Closed)

    @description(
      "The stream has processed all records, and the upstream data source will not make more records available.",
    )
    case object Completed extends TerminalStatus

    @description("The stream has been stopped by a user.")
    case object Terminated extends TerminalStatus

    @description("The stream has been stopped by a failure during processing.")
    case object Failed extends TerminalStatus
  }

  @title("Streamed Record Format")
  @description("Format by which streamed records are decoded.")
  sealed abstract class StreamedRecordFormat
  object StreamedRecordFormat {

    @title("JSON via Cypher")
    @description("""Records are JSON values. For every record received, the
          |given Cypher query will be re-executed with the parameter in the query set
          |equal to the new JSON value.
  """.stripMargin)
    final case class CypherJson(
      @description("Cypher query to execute on each record.") query: String,
      @default("that")
      @description("Name of the Cypher parameter to populate with the JSON value.") parameter: String = "that",
    ) extends StreamedRecordFormat

    @title("Raw Bytes via Cypher")
    @description("""Records may have any format. For every record received, the
          |given Cypher query will be re-executed with the parameter in the query set
          |equal to the new value as a Cypher byte array.
  """.stripMargin)
    final case class CypherRaw(
      @description("Cypher query to execute on each record.") query: String,
      @default("that")
      @description("Name of the Cypher parameter to populate with the byte array.") parameter: String = "that",
    ) extends StreamedRecordFormat

    @title("Protobuf via Cypher")
    @description(
      "Records are serialized instances of `typeName` as described in the schema (a `.desc` descriptor file) at " +
      "`schemaUrl`. For every record received, the given Cypher query will be re-executed with the parameter " +
      "in the query set equal to the new (deserialized) Protobuf message.",
    )
    final case class CypherProtobuf(
      @description("Cypher query to execute on each record.") query: String,
      @description("Name of the Cypher parameter to populate with the Protobuf message.")
      @default("that")
      parameter: String = "that",
      @description(
        "URL (or local filename) of the Protobuf `.desc` file to load to parse the `typeName`.",
      ) schemaUrl: String,
      @description(
        "Message type name to use from the given `.desc` file as the incoming message type.",
      ) typeName: String,
    ) extends StreamedRecordFormat

    @title("Drop")
    @description("Ignore the data without further processing.")
    case object Drop extends StreamedRecordFormat
  }

  sealed trait CsvCharacter
  object CsvCharacter {
    case object Backslash extends CsvCharacter
    case object Comma extends CsvCharacter
    case object Semicolon extends CsvCharacter
    case object Colon extends CsvCharacter
    case object Tab extends CsvCharacter
    case object Pipe extends CsvCharacter
    case object DoubleQuote extends CsvCharacter
    val values: Seq[CsvCharacter] = Seq(Backslash, Comma, Semicolon, Colon, Tab, Pipe)
  }

  @title("Kafka Auto Offset Reset")
  @description(
    "See [`auto.offset.reset` in the Kafka documentation](https://docs.confluent.io/current/installation/configuration/consumer-configs.html#auto.offset.reset).",
  )
  sealed abstract class KafkaAutoOffsetReset(val name: String)
  object KafkaAutoOffsetReset {
    case object Latest extends KafkaAutoOffsetReset("latest")
    case object Earliest extends KafkaAutoOffsetReset("earliest")
    case object None extends KafkaAutoOffsetReset("none")
    @default(Seq(Latest, Earliest, None))
    val values: Seq[KafkaAutoOffsetReset] = Seq(Latest, Earliest, None)
  }

  @title("Kafka offset tracking mechanism")
  @description(
    "How to keep track of current offset when consuming from Kafka, if at all. " +
    """You could alternatively set "enable.auto.commit": "true" in kafkaProperties  for this ingest, """ +
    "but in that case messages will be lost if the ingest is stopped while processing messages",
  )
  sealed abstract class KafkaOffsetCommitting
  object KafkaOffsetCommitting {
    @title("Explicit Commit")
    @description(
      "Commit offsets to the specified Kafka consumer group on successful execution of the ingest query for that record.",
    )
    final case class ExplicitCommit(
      @description("Maximum number of messages in a single commit batch.")
      @default(1000)
      maxBatch: Long = 1000,
      @description("Maximum interval between commits in milliseconds.")
      @default(10000)
      maxIntervalMillis: Int = 10000,
      @description("Parallelism for async committing.")
      @default(100)
      parallelism: Int = 100,
      @description("Wait for a confirmation from Kafka on ack.")
      @default(true)
      waitForCommitConfirmation: Boolean = true,
    ) extends KafkaOffsetCommitting
  }

  @title("AWS Region")
  @description(
    "AWS region code. e.g. `us-west-2`. If not provided, defaults according to the default AWS region provider chain. See: <https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/region-selection.html#automatically-determine-the-aws-region-from-the-environment>.",
  )
  final case class AwsRegion(region: String)

  sealed abstract class KafkaSecurityProtocol(val name: String)
  object KafkaSecurityProtocol {
    case object PlainText extends KafkaSecurityProtocol("PLAINTEXT")
    case object Ssl extends KafkaSecurityProtocol("SSL")
    case object Sasl_Ssl extends KafkaSecurityProtocol("SASL_SSL")
    case object Sasl_Plaintext extends KafkaSecurityProtocol("SASL_PLAINTEXT")
  }

  object WebsocketSimpleStartupIngest {
    @title("Websockets Keepalive Protocol")
    sealed trait KeepaliveProtocol
    @title("Ping/Pong on interval")
    @description("Send empty websocket messages at the specified interval (in milliseconds).")
    final case class PingPongInterval(@default(5000) intervalMillis: Int = 5000) extends KeepaliveProtocol
    @title("Text Keepalive Message on Interval")
    @description("Send the same text-based Websocket message at the specified interval (in milliseconds).")
    final case class SendMessageInterval(message: String, @default(5000) intervalMillis: Int = 5000)
        extends KeepaliveProtocol
    @title("No Keepalive")
    @description("Only send data messages, no keepalives.")
    final case object NoKeepalive extends KeepaliveProtocol
  }

  @title("AWS Credentials")
  @description(
    "Explicit AWS access key and secret to use. If not provided, defaults to environmental credentials according to the default AWS credential chain. See: <https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html#credentials-default>.",
  )
  final case class AwsCredentials(accessKeyId: String, secretAccessKey: String)

  sealed abstract class RecordDecodingType
  object RecordDecodingType {
    @description("Zlib compression")
    case object Zlib extends RecordDecodingType
    @description("Gzip compression")
    case object Gzip extends RecordDecodingType
    @description("Base64 encoding")
    case object Base64 extends RecordDecodingType
    @default(Seq(Zlib, Gzip, Base64))
    val values: Seq[RecordDecodingType] = Seq(Zlib, Gzip, Base64)

  }

  sealed abstract class FileIngestMode
  object FileIngestMode {
    @description("Ordinary file to be open and read once")
    case object Regular extends FileIngestMode
    @description("Named pipe to be regularly reopened and polled for more data")
    case object NamedPipe extends FileIngestMode
    @default(Seq(Regular, NamedPipe))
    val values: Seq[FileIngestMode] = Seq(Regular, NamedPipe)
  }

  object KinesisIngest {

    @title("Kinesis Shard Iterator Type")
    @description("See <https://docs.aws.amazon.com/kinesis/latest/APIReference/API_StartingPosition.html>.")
    sealed abstract class IteratorType

    object IteratorType {

      sealed abstract class Unparameterized extends IteratorType

      sealed abstract class Parameterized extends IteratorType

      @title("Latest")
      @description("All records added to the shard since subscribing.")
      case object Latest extends Unparameterized

      @title("TrimHorizon")
      @description("All records in the shard.")
      case object TrimHorizon extends Unparameterized

      @title("AtSequenceNumber")
      @description("All records starting from the provided sequence number.")
      final case class AtSequenceNumber(sequenceNumber: String) extends Parameterized

      @title("AfterSequenceNumber")
      @description("All records starting after the provided sequence number.")
      final case class AfterSequenceNumber(sequenceNumber: String) extends Parameterized

      // JS-safe long gives ms until the year 287396-ish
      @title("AtTimestamp")
      @description("All records starting from the provided unix millisecond timestamp.")
      final case class AtTimestamp(millisSinceEpoch: Long) extends Parameterized
    }
  }

  object KafkaIngest {
    // Takes a set of topic names
    type Topics = Set[String]
    // Takes a set of partition numbers for each topic name.
    type PartitionAssignments = Map[String, Set[Int]]
    // Takes a map of kafka properties
    type KafkaProperties = Map[String, String]
  }

  object Oss {
    case class QuineIngestConfiguration(
      source: IngestSource,
      @description("Cypher query to execute on each record.")
      query: String,
      @description("Name of the Cypher parameter to populate with the JSON value.")
      @default("that")
      parameter: String = "that",
      @description("Maximum number of records to process at once.")
      @default(16)
      parallelism: Int = V1.IngestRoutes.defaultWriteParallelism,
      @description("Maximum number of records to process per second.")
      maxPerSecond: Option[Int] = None,
      @description("Action to take on a single failed record")
      @default(LogRecordErrorHandler)
      onRecordError: OnRecordErrorHandler = LogRecordErrorHandler,
      @description("Action to take on a failure of the input stream")
      @default(LogStreamError)
      onStreamError: OnStreamErrorHandler = LogStreamError,
    )
  }

  @title("Ingest source")
  sealed trait IngestSource
  @title("File Ingest")
  case class FileIngest(
    @description("format used to decode each incoming line from a file")
    format: FileFormat,
    @description("Local file path.")
    path: String,
    fileIngestMode: Option[FileIngestMode],
    @description("Maximum size (in bytes) of any line in the file.")
    maximumLineSize: Option[Int] = None,
    @description(
      s"""Begin processing at the record with the given index. Useful for skipping some number of lines (e.g. CSV headers) or
         |resuming ingest from a partially consumed file.""".stripMargin,
    )
    startOffset: Long,
    @description(s"Optionally limit how many records are ingested from this file.")
    limit: Option[Long],
    @description(
      "The text encoding scheme for the file. UTF-8, US-ASCII and ISO-8859-1 are " +
      "supported -- other encodings will transcoded to UTF-8 on the fly (and ingest may be slower).",
    )
    characterEncoding: Charset,
    @description(
      "List of decodings to be applied to each input. The specified decodings are applied in declared array order.",
    )
    @default(Seq())
    recordDecoders: Seq[RecordDecodingType] = Seq(),
  ) extends IngestSource

  @title("S3 Ingest")
  case class S3Ingest(
    @description("format used to decode each incoming line from a file in S3")
    format: FileFormat,
    bucket: String,
    @description("S3 file name")
    key: String,
    @description("AWS credentials to apply to this request")
    credentials: Option[AwsCredentials],
    @description("Maximum size (in bytes) of any line in the file.")
    maximumLineSize: Option[Int] = None,
    @description(
      s"""Begin processing at the record with the given index. Useful for skipping some number of lines (e.g. CSV headers) or
                                                    |resuming ingest from a partially consumed file.""".stripMargin,
    )
    startOffset: Long,
    @description(s"Optionally limit how many records are ingested from this file.")
    limit: Option[Long],
    @description(
      "text encoding used to read the file. Only UTF-8, US-ASCII and ISO-8859-1 are directly " +
      "supported -- other encodings will transcoded to UTF-8 on the fly (and ingest may be slower).",
    )
    characterEncoding: Charset,
    @description(
      "List of decodings to be applied to each input. The specified decodings are applied in declared array order.",
    )
    @default(Seq())
    recordDecoders: Seq[RecordDecodingType] = Seq(),
  ) extends IngestSource

  @title("Standard Input Ingest")
  case class StdInputIngest(
    @description("format used to decode each incoming line from stdIn")
    format: FileFormat,
    @description("Maximum size (in bytes) of any line in the file.")
    maximumLineSize: Option[Int] = None,
    @description(
      "text encoding used to read the file. Only UTF-8, US-ASCII and ISO-8859-1 are directly " +
      "supported -- other encodings will transcoded to UTF-8 on the fly (and ingest may be slower).",
    )
    characterEncoding: Charset,
  ) extends IngestSource

  @title("Number Iterator Ingest")
  @description(
    "An infinite ingest stream which requires no data source and just produces new sequential numbers" +
    " every time the stream is (re)started. The numbers are Java `Long`s` and will wrap at their max value.",
  )
  case class NumberIteratorIngest(
    @description("Begin the stream with this number.")
    @default(0)
    startOffset: Long = 0L,
    @description("Optionally end the stream after consuming this many items.")
    limit: Option[Long],
  ) extends IngestSource

  @title("Websockets Ingest Stream (Simple Startup)")
  @description("A websocket stream started after a sequence of text messages.")
  case class WebsocketIngest(
    @description("Format used to decode each incoming message.")
    format: StreamingFormat,
    @description("Websocket (ws: or wss:) url to connect to.")
    url: String,
    @description("Initial messages to send to the server on connecting.")
    initMessages: Seq[String],
    @description("Strategy to use for sending keepalive messages, if any.")
    @default(WebsocketSimpleStartupIngest.PingPongInterval())
    keepAlive: WebsocketSimpleStartupIngest.KeepaliveProtocol = WebsocketSimpleStartupIngest.PingPongInterval(),
    characterEncoding: Charset,
  ) extends IngestSource

  @title("Kinesis Data Stream")
  @description("A stream of data being ingested from Kinesis.")
  case class KinesisIngest(
    @description("The format used to decode each Kinesis record.")
    format: StreamingFormat,
    @description("Name of the Kinesis stream to ingest.")
    streamName: String,
    @description(
      "Shards IDs within the named kinesis stream to ingest; if empty or excluded, all shards on the stream are processed.",
    )
    shardIds: Option[Set[String]],
    @description(
      "AWS credentials for this Kinesis stream. If not provided the default credentials provider chain is used.",
    )
    credentials: Option[AwsCredentials],
    @description("AWS region for this Kinesis stream")
    region: Option[AwsRegion],
    @description("Shard iterator type.")
    @default(KinesisIngest.IteratorType.Latest)
    iteratorType: KinesisIngest.IteratorType = KinesisIngest.IteratorType.Latest,
    @description("Number of retries to attempt on Kineses error.")
    @default(3)
    numRetries: Int = 3,
    @description(
      "List of decodings to be applied to each input, where specified decodings are applied in declared array order.",
    )
    @default(Seq())
    recordDecoders: Seq[RecordDecodingType] = Seq(),
  ) extends IngestSource

  sealed trait InitialPosition

  @title("Latest")
  @description("All records added to the shard since subscribing.")
  case object Latest extends InitialPosition

  @title("TrimHorizon")
  @description("All records in the shard.")
  case object TrimHorizon extends InitialPosition

  @title("AtTimestamp")
  @description("All records starting from the provided unix millisecond timestamp.")
  final case class AtTimestamp(year: Int, month: Int, date: Int, hourOfDay: Int, minute: Int, second: Int)
      extends InitialPosition

  @title("Kinesis Data Stream Using Kcl lib")
  @description("A stream of data being ingested from Kinesis")
  case class KinesisKclIngest(
    @description("The unique, human-facing name of the ingest stream")
    name: String,
    @description(
      """Name of the application (irrelevant unless using KCL, where `applicationName` also becomes the default DynamoDB
        |lease table name. Defaults to 'Quine' if needed and not provided).""".stripMargin,
    )
    applicationName: Option[String],
    @description("Name of the Kinesis stream to ingest.")
    streamName: String,
    @description("The format used to decode each Kinesis record.")
    format: StreamingFormat,
    @description(
      "AWS credentials for this Kinesis stream. If not provided the default credentials provider chain is used.",
    )
    credentials: Option[AwsCredentials],
    @description("AWS region for this Kinesis stream. If none is provided uses aws default.")
    regionOpt: Option[AwsRegion],
    @description("Where to start in the kinesis stream")
    @default(Latest)
    initialPosition: InitialPosition = Latest,
    @description("Number of retries to attempt when communicating with aws services")
    @default(3)
    numRetries: Int = 3,
    @description(
      "Sets the KinesisSchedulerSourceSettings buffer size. Buffer size must be greater than 0; use size 1 to disable stage buffering.",
    )
    @default(1000)
    bufferSize: Int = 1000,
    @description(
      "Sets the KinesisSchedulerSourceSettings backpressureTimeout in milliseconds",
    )
    @default(6000)
    backpressureTimeoutMillis: Long = 60000,
    @description(
      "List of decodings to be applied to each input, where specified decodings are applied in declared array order.",
    )
    @default(Seq())
    recordDecoders: Seq[RecordDecodingType] = Seq(),
    @description("When should checkpoints occur for a stream")
    checkpointSettings: Option[V1.KinesisIngest.KinesisCheckpointSettings], // TODO V2 type
  ) extends IngestSource

  @title("Server Sent Events Stream")
  @description(
    "A server-issued event stream, as might be handled by the EventSource JavaScript API. Only consumes the `data` portion of an event.",
  )
  case class ServerSentEventIngest(
    @description("Format used to decode each event's `data`.")
    format: StreamingFormat,
    @description("URL of the server sent event stream.")
    url: String,
    @description(
      "List of decodings to be applied to each input, where specified decodings are applied in declared array order.",
    )
    @default(Seq())
    recordDecoders: Seq[RecordDecodingType] = Seq(),
  ) extends IngestSource

  @title("Simple Queue Service Queue")
  @description("An active stream of data being ingested from AWS SQS.")
  case class SQSIngest(
    format: StreamingFormat,
    @description("URL of the queue to ingest.") queueUrl: String,
    @description("Maximum number of records to read from the queue simultaneously.")
    @default(1)
    readParallelism: Int = 1,
    credentials: Option[AwsCredentials],
    region: Option[AwsRegion],
    @description("Whether the queue consumer should acknowledge receipt of in-flight messages.")
    @default(true)
    deleteReadMessages: Boolean = true,
    @description(
      "List of decodings to be applied to each input, where specified decodings are applied in declared array order.",
    )
    @default(Seq())
    recordDecoders: Seq[RecordDecodingType] = Seq(),
  ) extends IngestSource

  @title("Kafka Ingest Stream")
  @description("A stream of data being ingested from Kafka.")
  case class KafkaIngest(
    format: StreamingFormat,
    @description(
      """Kafka topics from which to ingest: Either an array of topic names, or an object whose keys are topic names and
                              |whose values are partition indices.""".stripMargin
        .replace('\n', ' '),
    )
    topics: Either[KafkaIngest.Topics, KafkaIngest.PartitionAssignments],
    @description("A comma-separated list of Kafka broker servers.")
    bootstrapServers: String,
    @description(
      "Consumer group ID that this ingest stream should report belonging to; defaults to the name of the ingest stream.",
    )
    groupId: Option[String],
    @default(KafkaSecurityProtocol.PlainText)
    securityProtocol: KafkaSecurityProtocol = KafkaSecurityProtocol.PlainText,
    offsetCommitting: Option[KafkaOffsetCommitting],
    @default(KafkaAutoOffsetReset.Latest)
    autoOffsetReset: KafkaAutoOffsetReset = KafkaAutoOffsetReset.Latest,
    @description(
      "Map of Kafka client properties. See <https://docs.confluent.io/platform/current/installation/configuration/consumer-configs.html#ak-consumer-configurations-for-cp>",
    )
    kafkaProperties: KafkaIngest.KafkaProperties = Map.empty[String, String],
    @description(
      "The offset at which this stream should complete; offsets are sequential integers starting at 0.",
    ) endingOffset: Option[Long],
    @description(
      "List of decodings to be applied to each input, where specified decodings are applied in declared array order.",
    )
    @default(Seq())
    recordDecoders: Seq[RecordDecodingType] = Seq(),
  ) extends IngestSource

  @title("Reactive Stream Ingest")
  @description("A stream of data being ingested from a reactive stream.")
  case class ReactiveStream(
    format: StreamingFormat,
    url: String,
    port: Int,
  ) extends IngestSource

  sealed trait IngestFormat

  @title("File Ingest Format")
  @description("Format by which a file will be interpreted as a stream of elements for ingest.")
  sealed trait FileFormat extends IngestFormat
  object FileFormat {

    /** Create using a cypher query, passing each line in as a string */
    @title("Line")
    @description("""For every line (LF/CRLF delimited) in the source, the given Cypher query will be
        |re-executed with the parameter in the query set equal to a string matching
        |the new line value. The newline is not included in this string.
  """.stripMargin.replace('\n', ' '))
    case object LineFormat extends FileFormat

    /** Create using a cypher query, expecting each line to be a JSON record */
    @title("Json")
    @description("""Lines in the file should be JSON values. For every value received, the
        |given Cypher query will be re-executed with the parameter in the query set
        |equal to the new JSON value.
  """.stripMargin.replace('\n', ' '))
    case object JsonFormat extends FileFormat

    /** Create using a cypher query, expecting each line to be a single row CSV record */
    @title("CSV")
    @description(
      """For every row in a CSV file, the given Cypher query will be re-executed with the parameter in the query set
        |to the parsed row. Rows are parsed into either a Cypher List of strings or a Map, depending on whether a
        |`headers` row is available.""".stripMargin.replace('\n', ' '),
    )
    case class CsvFormat(
      @description("""Read a CSV file containing headers in the file's first row (`true`) or with no headers (`false`).
                              |Alternatively, an array of column headers can be passed in. If headers are not supplied, the resulting
                              |type available to the Cypher query will be a List of strings with values accessible by index. When
                              |headers are available (supplied or read from the file), the resulting type available to the Cypher
                              |query will be a Map[String, String], with values accessible using the corresponding header string.
                              |CSV rows containing more records than the `headers` will have items that don't match a header column
                              |discarded. CSV rows with fewer columns than the `headers` will have `null` values for the missing headers.
                              |Default: `false`.""".stripMargin)
      @default(Left(false))
      headers: Either[Boolean, List[String]] = Left(false),
      @description("CSV row delimiter character.")
      @default(CsvCharacter.Comma)
      delimiter: CsvCharacter = CsvCharacter.Comma,
      @description("""Character used to quote values in a field. Special characters (like new lines) inside of a quoted
                              |section will be a part of the CSV value.""".stripMargin)
      @default(CsvCharacter.DoubleQuote)
      quoteChar: CsvCharacter = CsvCharacter.DoubleQuote,
      @description("Character used to escape special characters.")
      @default(CsvCharacter.Backslash)
      escapeChar: CsvCharacter = CsvCharacter.Backslash,
    ) extends FileFormat
  }

  @title("Streamed Record Format")
  @description("Format by which streamed records are decoded.")
  sealed trait StreamingFormat extends IngestFormat

  object StreamingFormat {

    @title("Json")
    @description("""Records are JSON values. For every record received, the
        |given Cypher query will be re-executed with the parameter in the query set
        |equal to the new JSON value.
  """.stripMargin)
    case object JsonFormat extends StreamingFormat

    @title("Raw Bytes")
    @description("""Records may have any format. For every record received, the
        |given Cypher query will be re-executed with the parameter in the query set
        |equal to the new value as a Cypher byte array.
  """.stripMargin)
    case object RawFormat extends StreamingFormat

    @title("Protobuf via Cypher")
    @description(
      "Records are serialized instances of `typeName` as described in the schema (a `.desc` descriptor file) at " +
      "`schemaUrl`. For every record received, the given Cypher query will be re-executed with the parameter " +
      "in the query set equal to the new (deserialized) Protobuf message.",
    )
    final case class ProtobufFormat(
      @description(
        "URL (or local filename) of the Protobuf `.desc` file to load to parse the `typeName`.",
      ) schemaUrl: String,
      @description(
        "Message type name to use from the given `.desc` file as the incoming message type.",
      ) typeName: String,
    ) extends StreamingFormat

    @title("Avro format")
    case class AvroFormat(
      @description(
        "URL (or local filename) of the file to load to parse the avro schema.",
      )
      schemaUrl: String,
    ) extends StreamingFormat

    @title("Drop")
    @description("Ignore the data without further processing.")
    case object DropFormat extends StreamingFormat
  }
  sealed trait OnStreamErrorHandler

  @title("Retry Stream Error Handler")
  @description("Retry the stream on failure")
  case class RetryStreamError(retryCount: Int) extends OnStreamErrorHandler

  @title("Log Stream Error Handler")
  @description("If the stream fails log a message but do not retry.")
  case object LogStreamError extends OnStreamErrorHandler

  // --------------------
  // Stream Error Handler
  // --------------------
  /** Error handler defined for errors that affect only a single record. This is intended to handle errors in
    * a configurable way distinct from stream-level errors, where the entire stream fails - e.g. handling
    * a single corrupt record rather than a failure in the stream communication.
    */
  sealed trait OnRecordErrorHandler

  @title("Log Record Error Handler")
  @description("Log a message for each message that encounters an error in processing")
  case object LogRecordErrorHandler extends OnRecordErrorHandler with LazyLogging

  @title("Dead-letter Record Error Handler")
  @description(
    "Preserve records that encounter an error in processing by forwarding them to a specified dead-letter destination (TBD)",
  )
  case object DeadLetterErrorHandler extends OnRecordErrorHandler

}
