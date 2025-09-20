package com.thatdot.quine.app.v2api.definitions.ingest2

import java.nio.charset.{Charset, StandardCharsets}
import java.time.Instant

import sttp.tapir.Schema.annotations.{default, description, encodedExample, title}

import com.thatdot.api.v2.{AwsCredentials, AwsRegion, RatesSummary}
import com.thatdot.quine.{routes => V1}

object ApiIngest {
  import com.thatdot.quine.app.util.StringOps.syntax._

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

  @title("Statistics About a Running Ingest Stream")
  final case class IngestStreamStats(
    // NB this is duplicated by rates.count -- maybe remove one?
    @description("Number of source records (or lines) ingested so far.") ingestedCount: Long,
    @description("Records/second over different time periods.") rates: RatesSummary,
    @description("Bytes/second over different time periods.") byteRates: RatesSummary,
    @description("Time (in ISO-8601 UTC time) when the ingestion was started.") startTime: Instant,
    @description("Time (in milliseconds) that that the ingest has been running.") totalRuntime: Long,
  )

  @title("Ingest Stream Info")
  @description("An active stream of data being ingested.")
  final case class IngestStreamInfo(
    @description("Indicator of whether the ingest is still running, completed, etc.") status: IngestStreamStatus,
    @description("Error message about the ingest, if any.") message: Option[String],
    // Add a warnings output string
    @description("Configuration of the ingest stream.") settings: IngestSource,
    @description("Statistics on progress of running ingest stream.") stats: IngestStreamStats,
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
    @description("Unique name identifying the ingest stream.") name: String,
    @description("Indicator of whether the ingest is still running, completed, etc.") status: IngestStreamStatus,
    @description("Error message about the ingest, if any.") message: Option[String],
    @description("Configuration of the ingest stream.") settings: IngestSource,
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
    """How to keep track of current offset when consuming from Kafka, if at all.
      |You could alternatively set "enable.auto.commit": "true" in kafkaProperties for this ingest,
      |but in that case messages will be lost if the ingest is stopped while processing messages.""".asOneLine,
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
    @description("Ordinary file to be open and read once.")
    case object Regular extends FileIngestMode

    @description("Named pipe to be regularly reopened and polled for more data.")
    case object NamedPipe extends FileIngestMode

    @default(Seq(Regular, NamedPipe))
    val values: Seq[FileIngestMode] = Seq(Regular, NamedPipe)
  }

  sealed trait Transformation
  object Transformation {
    case class JavaScript(
      @description("JavaScript source code of the function. Must be callable.")
      @encodedExample("that => that")
      function: String,
    ) extends Transformation
  }

  object Oss {
    case class QuineIngestConfiguration(
      @description("Unique name identifying the ingest stream.")
      name: String,
      source: IngestSource,
      @description("Cypher query to execute on each record.")
      query: String,
      @description("Name of the Cypher parameter to populate with the JSON value.")
      @default("that")
      parameter: String = "that",
      @description("A function to be run before the cypher query is executed. Used to pre-process input.")
      transformation: Option[Transformation] = None,
      @description("Maximum number of records to process at once.")
      @default(16)
      parallelism: Int = V1.IngestRoutes.defaultWriteParallelism,
      @description("Maximum number of records to process per second.")
      maxPerSecond: Option[Int] = None,
      @description("Action to take on a single failed record.")
      @default(OnRecordErrorHandler())
      onRecordError: OnRecordErrorHandler = OnRecordErrorHandler(),
      @description("Action to take on a failure of the input stream.")
      @default(LogStreamError)
      onStreamError: OnStreamErrorHandler = LogStreamError,
    )
  }

  @title("Ingest source")
  sealed trait IngestSource

  object IngestSource {

    @title("Server Sent Events Stream")
    @description(
      """A server-issued event stream, as might be handled by the EventSource JavaScript API.
        |Only consumes the `data` portion of an event.""".asOneLine,
    )
    case class ServerSentEvent(
      @description("Format used to decode each event's `data`.")
      format: IngestFormat.StreamingFormat,
      @description("URL of the server sent event stream.")
      url: String,
      @description(
        "List of decodings to be applied to each input, where specified decodings are applied in declared array order.",
      )
      @default("[]")
      recordDecoders: Seq[RecordDecodingType] = Seq(),
    ) extends IngestSource

    @title("Simple Queue Service Queue")
    @description("An active stream of data being ingested from AWS SQS.")
    case class SQS(
      format: IngestFormat.StreamingFormat,
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
      @default("[]")
      recordDecoders: Seq[RecordDecodingType] = Seq(),
    ) extends IngestSource

    @title("Kafka Ingest Stream")
    @description("A stream of data being ingested from Kafka.")
    case class Kafka(
      format: IngestFormat.StreamingFormat,
      @description(
        """Kafka topics from which to ingest:
          |Either an array of topic names, or an object whose keys are topic names and whose values
          |are partition indices.""".asOneLine,
      )
      topics: Either[Kafka.Topics, Kafka.PartitionAssignments],
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
      kafkaProperties: Kafka.KafkaProperties = Map.empty[String, String],
      @description(
        "The offset at which this stream should complete; offsets are sequential integers starting at 0.",
      )
      endingOffset: Option[Long],
      @description(
        "List of decodings to be applied to each input, where specified decodings are applied in declared array order.",
      )
      @default("[]")
      recordDecoders: Seq[RecordDecodingType] = Seq(),
    ) extends IngestSource

    object Kafka {
      // Takes a set of topic names
      type Topics = Set[String]
      // Takes a set of partition numbers for each topic name.
      type PartitionAssignments = Map[String, Set[Int]]
      // Takes a map of kafka properties
      type KafkaProperties = Map[String, String]
    }

    @title("Reactive Stream Ingest")
    @description("A stream of data being ingested from a reactive stream.")
    case class ReactiveStream(
      format: IngestFormat.StreamingFormat,
      url: String,
      port: Int,
    ) extends IngestSource

    @title("File Ingest")
    case class File(
      @description("format used to decode each incoming line from a file")
      format: IngestFormat.FileFormat,
      @description("Local file path.")
      path: String,
      fileIngestMode: Option[FileIngestMode],
      @description("Maximum size (in bytes) of any line in the file.")
      maximumLineSize: Option[Int] = None,
      @description(
        s"""Begin processing at the record with the given index. Useful for skipping some number of lines (e.g. CSV headers) or
           |resuming ingest from a partially consumed file.""".asOneLine,
      )
      @default(0)
      startOffset: Long = 0,
      @description(s"Optionally limit how many records are ingested from this file.")
      limit: Option[Long],
      @description(
        """The text encoding scheme for the file. UTF-8, US-ASCII and ISO-8859-1 are supported — other
          |encodings will transcoded to UTF-8 on the fly (and ingest may be slower).""".asOneLine,
      )
      @default(StandardCharsets.UTF_8)
      characterEncoding: Charset = StandardCharsets.UTF_8,
      @description(
        "List of decodings to be applied to each input. The specified decodings are applied in declared array order.",
      )
      @default("[]")
      recordDecoders: Seq[RecordDecodingType] = Seq(),
    ) extends IngestSource

    @title("S3 Ingest")
    case class S3(
      @description("format used to decode each incoming line from a file in S3")
      format: IngestFormat.FileFormat,
      bucket: String,
      @description("S3 file name")
      key: String,
      @description("AWS credentials to apply to this request")
      credentials: Option[AwsCredentials],
      @description("Maximum size (in bytes) of any line in the file.")
      maximumLineSize: Option[Int] = None,
      @description(
        s"""Begin processing at the record with the given index. Useful for skipping some number of lines
           |(e.g. CSV headers) or resuming ingest from a partially consumed file.""".asOneLine,
      )
      startOffset: Long,
      @description(s"Optionally limit how many records are ingested from this file.")
      limit: Option[Long],
      @description(
        """Text encoding used to read the file. Only UTF-8, US-ASCII and ISO-8859-1 are directly supported —
          |other encodings will transcoded to UTF-8 on the fly (and ingest may be slower).""".asOneLine,
      )
      characterEncoding: Charset,
      @description(
        "List of decodings to be applied to each input. The specified decodings are applied in declared array order.",
      )
      @default("[]")
      recordDecoders: Seq[RecordDecodingType] = Seq(),
    ) extends IngestSource

    @title("Standard Input Ingest")
    case class StdInput(
      @description("format used to decode each incoming line from stdIn")
      format: IngestFormat.FileFormat,
      @description("Maximum size (in bytes) of any line in the file.")
      maximumLineSize: Option[Int] = None,
      @description(
        """Text encoding used to read the file. Only UTF-8, US-ASCII and ISO-8859-1 are directly supported —
          |other encodings will transcoded to UTF-8 on the fly (and ingest may be slower).""".asOneLine,
      )
      characterEncoding: Charset,
    ) extends IngestSource

    @title("Number Iterator Ingest")
    @description(
      "An infinite ingest stream which requires no data source and just produces new sequential numbers" +
      " every time the stream is (re)started. The numbers are Java `Long`s` and will wrap at their max value.",
    )
    case class NumberIterator(
      @description("Begin the stream with this number.")
      @default(0)
      startOffset: Long = 0L,
      @description("Optionally end the stream after consuming this many items.")
      limit: Option[Long],
    ) extends IngestSource

    @title("Websockets Ingest Stream (Simple Startup)")
    @description("A websocket stream started after a sequence of text messages.")
    case class Websocket(
      @description("Format used to decode each incoming message.")
      format: IngestFormat.StreamingFormat,
      @description("Websocket (ws: or wss:) url to connect to.")
      url: String,
      @description("Initial messages to send to the server on connecting.")
      initMessages: Seq[String],
      @description("Strategy to use for sending keepalive messages, if any.")
      @default(WebsocketSimpleStartupIngest.PingPongInterval())
      keepAlive: WebsocketSimpleStartupIngest.KeepaliveProtocol = WebsocketSimpleStartupIngest.PingPongInterval(),
      @description(
        """Text encoding used to read the file. Only UTF-8, US-ASCII and ISO-8859-1 are directly supported —
          |other encodings will transcoded to UTF-8 on the fly (and ingest may be slower).""".asOneLine,
      )
      characterEncoding: Charset,
    ) extends IngestSource

    @title("Kinesis Data Stream")
    @description("A stream of data being ingested from Kinesis.")
    case class Kinesis(
      @description("The format used to decode each Kinesis record.")
      format: IngestFormat.StreamingFormat,
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
      @default(Kinesis.IteratorType.Latest)
      iteratorType: Kinesis.IteratorType = Kinesis.IteratorType.Latest,
      @description("Number of retries to attempt on Kineses error.")
      @default(3)
      numRetries: Int = 3,
      @description(
        "List of decodings to be applied to each input, where specified decodings are applied in declared array order.",
      )
      @default("[]")
      recordDecoders: Seq[RecordDecodingType] = Seq(),
    ) extends IngestSource

    object Kinesis {

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

    @title("Kinesis Data Stream Using Kcl lib")
    @description("A stream of data being ingested from Kinesis")
    case class KinesisKCL(
      @description("The name of the stream that this application processes records from.")
      kinesisStreamName: String,
      @description(
        "Overrides the table name used for the Amazon DynamoDB lease table, the default CloudWatch namespace, and EFO consumer name.",
      )
      applicationName: String,
      @description("The format used to decode each Kinesis record.")
      format: IngestFormat.StreamingFormat,
      @description(
        "AWS credentials for this Kinesis stream. If not provided the default credentials provider chain is used.",
      )
      credentials: Option[AwsCredentials],
      @description("AWS region for this Kinesis stream. If none is provided uses aws default.")
      regionOpt: Option[AwsRegion],
      @description("Where to start in the kinesis stream")
      @default(InitialPosition.Latest)
      initialPosition: InitialPosition = InitialPosition.Latest,
      @description("Number of retries to attempt when communicating with aws services")
      @default(3)
      numRetries: Int = 3,
      @description(
        "Sets the KinesisSchedulerSourceSettings buffer size. Buffer size must be greater than 0; use size 1 to disable stage buffering.",
      )
      @default("[]")
      recordDecoders: Seq[RecordDecodingType] = Seq(),
      @description("Additional settings for the Kinesis Scheduler.")
      schedulerSourceSettings: Option[KinesisSchedulerSourceSettings],
      @description(
        """Optional stream checkpoint settings. If present, checkpointing will manage `iteratorType` and `shardIds`,
          |ignoring those fields in the API request.""".asOneLine,
      )
      checkpointSettings: Option[KinesisCheckpointSettings],
      @description(
        """Optional advanced configuration, derived from the KCL 3.x documented configuration
          |table (https://docs.aws.amazon.com/streams/latest/dev/kcl-configuration.html), but without fields that are
          |available elsewhere in this API object schema.""".asOneLine,
      )
      advancedSettings: Option[KCLConfiguration],
    ) extends IngestSource
  }

  @title("Scheduler Checkpoint Settings")
  final case class KinesisCheckpointSettings(
    @description("Whether to disable checkpointing, which is enabled by default.")
    @default(false)
    disableCheckpointing: Boolean = false,
    @description("Maximum checkpoint batch size.")
    @default(None)
    maxBatchSize: Option[Int] = None,
    @description("Maximum checkpoint batch wait time in ms.")
    @default(None)
    maxBatchWaitMillis: Option[Long] = None,
  )

  case class KinesisSchedulerSourceSettings(
    @description(
      """Sets the KinesisSchedulerSourceSettings buffer size. Buffer size must be greater than 0; use size 1 to disable
        |stage buffering.""".asOneLine,
    )
    bufferSize: Option[Int] = None,
    @description("Sets the KinesisSchedulerSourceSettings backpressureTimeout in milliseconds")
    backpressureTimeoutMillis: Option[Long] = None,
  )

  @title("KCLConfiguration")
  @description(
    "A complex object comprising abbreviated configuration objects used by the Kinesis Client Library (KCL).",
  )
  case class KCLConfiguration(
    configsBuilder: Option[ConfigsBuilder] = None,
    leaseManagementConfig: Option[LeaseManagementConfig] = None,
    retrievalSpecificConfig: Option[RetrievalSpecificConfig] = None,
    processorConfig: Option[ProcessorConfig] = None,
    coordinatorConfig: Option[CoordinatorConfig] = None,
    lifecycleConfig: Option[LifecycleConfig] = None,
    retrievalConfig: Option[RetrievalConfig] = None,
    metricsConfig: Option[MetricsConfig] = None,
  )

  @title("ConfigsBuilder")
  @description("Abbreviated configuration for the KCL configurations builder.")
  case class ConfigsBuilder(
    @description("Overrides the table name used only for the Amazon DynamoDB lease table")
    tableName: Option[String],
    @description(
      """A unique identifier that represents this instantiation of the application processor. This must be unique.
        |Default will be `hostname:<UUID.randomUUID`""".asOneLine,
    )
    workerIdentifier: Option[String],
  )

  sealed trait BillingMode {
    def value: String
  }

  object BillingMode {
    @title("Provisioned")
    @description("Provisioned billing.")
    case object PROVISIONED extends BillingMode {
      val value = "PROVISIONED"
    }

    @title("Pay-Per-Request")
    @description("Pay-per-request billing.")
    case object PAY_PER_REQUEST extends BillingMode {
      val value = "PAY_PER_REQUEST"
    }

    @title("Unknown")
    @description("The billing mode is not one of these provided options.")
    case object UNKNOWN_TO_SDK_VERSION extends BillingMode {
      val value = "UNKNOWN_TO_SDK_VERSION"
    }
  }

  sealed trait InitialPosition

  object InitialPosition {

    @title("Latest")
    @description("All records added to the shard since subscribing.")
    case object Latest extends InitialPosition

    @title("TrimHorizon")
    @description("All records in the shard.")
    case object TrimHorizon extends InitialPosition

    @title("AtTimestamp")
    @description("All records starting from the provided data time.")
    final case class AtTimestamp(year: Int, month: Int, date: Int, hourOfDay: Int, minute: Int, second: Int)
        extends InitialPosition
  }

  case class LeaseManagementConfig(
    @description(
      """The number of milliseconds that must pass before you can consider a lease owner to have failed.
        |For applications that have a large number of shards, this may be set to a higher number to reduce the number
        |of DynamoDB IOPS required for tracking leases.""".asOneLine,
    )
    failoverTimeMillis: Option[Long],
    @description("The time between shard sync calls.")
    shardSyncIntervalMillis: Option[Long],
    @description("When set, leases are removed as soon as the child leases have started processing.")
    cleanupLeasesUponShardCompletion: Option[Boolean],
    @description("When set, child shards that have an open shard are ignored. This is primarily for DynamoDB Streams.")
    ignoreUnexpectedChildShards: Option[Boolean],
    @description(
      """The maximum number of leases a single worker should accept. Setting it too low may cause data loss if workers can't
        |process all shards, and lead to a suboptimal lease assignment among workers. Consider total shard count, number
        |of workers, and worker processing capacity when configuring it.""".asOneLine,
    )
    maxLeasesForWorker: Option[Int],
    @description(
      """Controls the size of the lease renewer thread pool. The more leases that your application could take, the larger
        |this pool should be.""".asOneLine,
    )
    maxLeaseRenewalThreads: Option[Int],
    @description(
      """Determines the capacity mode of the lease table created in DynamoDB. There are two options: on-demand mode
        |(PAY_PER_REQUEST) and provisioned mode. We recommend using the default setting of on-demand mode because it
        |automatically scales to accommodate your workload without the need for capacity planning.""".asOneLine,
    )
    billingMode: Option[BillingMode],
    @description(
      """The DynamoDB read capacity that is used if the Kinesis Client Library needs to create a new DynamoDB lease table
        |with provisioned capacity mode. You can ignore this configuration if you are using the default on-demand capacity
        |mode in `billingMode` configuration.""".asOneLine,
    )
    initialLeaseTableReadCapacity: Option[Int],
    @description(
      """The DynamoDB read capacity that is used if the Kinesis Client Library needs to create a new DynamoDB lease table.
        |You can ignore this configuration if you are using the default on-demand capacity mode in `billingMode`
        |configuration.""".asOneLine,
    )
    initialLeaseTableWriteCapacity: Option[Int],
    @description(
      """A percentage value that determines when the load balancing algorithm should consider reassigning shards among
        |workers.
        |This is a new configuration introduced in KCL 3.x.""".stripMargin,
    )
    reBalanceThresholdPercentage: Option[Int],
    @description(
      """A percentage value that is used to dampen the amount of load that will be moved from the overloaded worker in a
        |single rebalance operation.
        |This is a new configuration introduced in KCL 3.x.""".stripMargin,
    )
    dampeningPercentage: Option[Int],
    @description(
      """Determines whether additional lease still needs to be taken from the overloaded worker even if it causes total
        |amount of lease throughput taken to exceed the desired throughput amount.
        |This is a new configuration introduced in KCL 3.x.""".stripMargin,
    )
    allowThroughputOvershoot: Option[Boolean],
    @description(
      """Determines if KCL should ignore resource metrics from workers (such as CPU utilization) when reassigning leases
        |and load balancing. Set this to TRUE if you want to prevent KCL from load balancing based on CPU utilization.
        |This is a new configuration introduced in KCL 3.x.""".stripMargin,
    )
    disableWorkerMetrics: Option[Boolean],
    @description(
      """Amount of the maximum throughput to assign to a worker during the lease assignment.
        |This is a new configuration introduced in KCL 3.x.""".stripMargin,
    )
    maxThroughputPerHostKBps: Option[Double],
    @description(
      """Controls the behavior of lease handoff between workers. When set to true, KCL will attempt to gracefully transfer
        |leases by allowing the shard's RecordProcessor sufficient time to complete processing before handing off the
        |lease to another worker. This can help ensure data integrity and smooth transitions but may increase handoff time.
        |When set to false, the lease will be handed off immediately without waiting for the RecordProcessor to shut down
        |gracefully. This can lead to faster handoffs but may risk incomplete processing.
        |
        |Note: Checkpointing must be implemented inside the shutdownRequested() method of the RecordProcessor to get
        |benefited from the graceful lease handoff feature.
        |This is a new configuration introduced in KCL 3.x.""".stripMargin,
    )
    isGracefulLeaseHandoffEnabled: Option[Boolean],
    @description(
      """Specifies the minimum time (in milliseconds) to wait for the current shard's RecordProcessor to gracefully
        |shut down before forcefully transferring the lease to the next owner.
        |If your processRecords method typically runs longer than the default value, consider increasing this setting.
        |This ensures the RecordProcessor has sufficient time to complete its processing before the lease transfer occurs.
        |This is a new configuration introduced in KCL 3.x.""".stripMargin,
    )
    gracefulLeaseHandoffTimeoutMillis: Option[Long],
  )

  sealed trait RetrievalSpecificConfig

  object RetrievalSpecificConfig {
    case class FanOutConfig(
      @description(
        "The ARN of an already created consumer, if this is set no automatic consumer creation will be attempted.",
      )
      consumerArn: Option[String],
      @description("The name of the consumer to create. If this isn't set the `applicationName` will be used.")
      consumerName: Option[String],
      @description(
        """The maximum number of retries for calling DescribeStreamSummary.
          |Once exhausted the consumer creation/retrieval will fail.""".asOneLine,
      )
      maxDescribeStreamSummaryRetries: Option[Int],
      @description(
        """The maximum number of retries for calling DescribeStreamConsumer.
          |Once exhausted the consumer creation/retrieval will fail.""".asOneLine,
      )
      maxDescribeStreamConsumerRetries: Option[Int],
      @description(
        """The maximum number of retries for calling RegisterStreamConsumer.
          |Once exhausted the consumer creation/retrieval will fail.""".asOneLine,
      )
      registerStreamConsumerRetries: Option[Int],
      @description("The maximum amount of time that will be made between failed calls.")
      retryBackoffMillis: Option[Long],
    ) extends RetrievalSpecificConfig

    case class PollingConfig(
      @description("Allows setting the maximum number of records that Kinesis returns.")
      maxRecords: Option[Int],
      @description("Configures the delay between GetRecords attempts for failures.")
      retryGetRecordsInSeconds: Option[Int],
      @description("The thread pool size used for GetRecords.")
      maxGetRecordsThreadPool: Option[Int],
      @description(
        """Determines how long KCL waits between GetRecords calls to poll the data from data streams.
          |The unit is milliseconds.""".asOneLine,
      )
      idleTimeBetweenReadsInMillis: Option[Long],
    ) extends RetrievalSpecificConfig
  }

  case class ProcessorConfig(
    @description("When set, the record processor is called even when no records were provided from Kinesis.")
    callProcessRecordsEvenForEmptyRecordList: Option[Boolean],
  )

  sealed trait ShardPrioritization

  object ShardPrioritization {
    case object NoOpShardPrioritization extends ShardPrioritization

    @description("Processes shard parents first, limited by a 'max depth' argument.")
    case class ParentsFirstShardPrioritization(maxDepth: Int) extends ShardPrioritization
  }

  sealed trait ClientVersionConfig

  object ClientVersionConfig {
    case object CLIENT_VERSION_CONFIG_COMPATIBLE_WITH_2X extends ClientVersionConfig

    case object CLIENT_VERSION_CONFIG_3X extends ClientVersionConfig
  }

  case class CoordinatorConfig(
    @description(
      """How often a record processor should poll to see if the parent shard has been completed.
        |The unit is milliseconds.""".asOneLine,
    )
    parentShardPollIntervalMillis: Option[Long],
    @description("Disable synchronizing shard data if the lease table contains existing leases.")
    skipShardSyncAtWorkerInitializationIfLeasesExist: Option[Boolean],
    @description("Which shard prioritization to use.")
    shardPrioritization: Option[ShardPrioritization],
    @description(
      """Determines which KCL version compatibility mode the application will run in. This configuration is only for the
        |migration from previous KCL versions. When migrating to 3.x, you need to set this configuration to
        |`CLIENT_VERSION_CONFIG_COMPATIBLE_WITH_2X`.
        |You can remove this configuration when you complete the migration.""".asOneLine,
    )
    clientVersionConfig: Option[ClientVersionConfig],
  )

  case class LifecycleConfig(
    @description("The time to wait to retry failed KCL tasks. The unit is milliseconds.")
    taskBackoffTimeMillis: Option[Long],
    @description("How long to wait before a warning is logged if a task hasn't completed.")
    logWarningForTaskAfterMillis: Option[Long],
  )

  case class RetrievalConfig(
    @description(
      "The number of milliseconds to wait between calls to `ListShards` when failures occur. The unit is milliseconds.",
    )
    listShardsBackoffTimeInMillis: Option[Long],
    @description("The maximum number of times that `ListShards` retries before giving up.")
    maxListShardsRetryAttempts: Option[Int],
  )

  sealed trait MetricsLevel

  object MetricsLevel {
    case object NONE extends MetricsLevel

    /** SUMMARY metrics level can be used to emit only the most significant metrics. */
    case object SUMMARY extends MetricsLevel

    /** DETAILED metrics level can be used to emit all metrics. */
    case object DETAILED extends MetricsLevel
  }

  @title("Dimensions that may be attached to CloudWatch metrics.")
  @description("See: https://docs.aws.amazon.com/streams/latest/dev/monitoring-with-kcl.html#metric-levels")
  sealed trait MetricsDimension {
    def value: String
  }

  object MetricsDimension {
    case object OPERATION_DIMENSION_NAME extends MetricsDimension {
      val value = "Operation"
    }

    case object SHARD_ID_DIMENSION_NAME extends MetricsDimension {
      val value = "ShardId"
    }

    case object STREAM_IDENTIFIER extends MetricsDimension {
      val value = "StreamId"
    }

    case object WORKER_IDENTIFIER extends MetricsDimension {
      val value = "WorkerIdentifier"
    }
  }

  case class MetricsConfig(
    @description(
      "Specifies the maximum duration (in milliseconds) to buffer metrics before publishing them to CloudWatch.",
    )
    metricsBufferTimeMillis: Option[Long],
    @description("Specifies the maximum number of metrics to buffer before publishing to CloudWatch.")
    metricsMaxQueueSize: Option[Int],
    @description("Specifies the granularity level of CloudWatch metrics to be enabled and published.")
    metricsLevel: Option[MetricsLevel],
    @description("Controls allowed dimensions for CloudWatch Metrics.")
    metricsEnabledDimensions: Option[Set[MetricsDimension]],
  )

  sealed trait IngestFormat

  object IngestFormat {

    @title("File Ingest Format")
    @description("Format by which a file will be interpreted as a stream of elements for ingest.")
    sealed trait FileFormat extends IngestFormat

    object FileFormat {

      /** Create using a cypher query, passing each line in as a string */
      @title("Line")
      @description(
        """For every line (LF/CRLF delimited) in the source, the given Cypher query will be
          |re-executed with the parameter in the query set equal to a string matching
          |the new line value. The newline is not included in this string.""".asOneLine,
      )
      case object Line extends FileFormat

      /** Create using a cypher query, expecting each line to be a JSON record */
      @title("Json")
      @description(
        """Lines in the file should be JSON values. For every value received, the
          |given Cypher query will be re-executed with the parameter in the query set
          |equal to the new JSON value.""".asOneLine,
      )
      case object Json extends FileFormat

      /** Create using a cypher query, expecting each line to be a single row CSV record */
      @title("CSV")
      @description(
        """For every row in a CSV file, the given Cypher query will be re-executed with the parameter in the query set
          |to the parsed row. Rows are parsed into either a Cypher List of strings or a Map, depending on whether a
          |`headers` row is available.""".asOneLine,
      )
      case class Csv(
        @description(
          """Read a CSV file containing headers in the file's first row (`true`) or with no headers (`false`).
            |Alternatively, an array of column headers can be passed in. If headers are not supplied, the resulting
            |type available to the Cypher query will be a List of strings with values accessible by index. When
            |headers are available (supplied or read from the file), the resulting type available to the Cypher
            |query will be a Map[String, String], with values accessible using the corresponding header string.
            |CSV rows containing more records than the `headers` will have items that don't match a header column
            |discarded. CSV rows with fewer columns than the `headers` will have `null` values for the missing headers.
            |Default: `false`.""".asOneLine,
        )
        @default(Left(false))
        headers: Either[Boolean, List[String]] = Left(false),
        @description("CSV row delimiter character.")
        @default(CsvCharacter.Comma)
        delimiter: CsvCharacter = CsvCharacter.Comma,
        @description(
          """Character used to quote values in a field. Special characters (like new lines) inside of a quoted
            |section will be a part of the CSV value.""".asOneLine,
        )
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
      @description(
        """Records are JSON values. For every record received, the
          |given Cypher query will be re-executed with the parameter in the query set
          |equal to the new JSON value.""".asOneLine,
      )
      case object Json extends StreamingFormat

      @title("Raw Bytes")
      @description(
        """Records may have any format. For every record received, the
          |given Cypher query will be re-executed with the parameter in the query set
          |equal to the new value as a Cypher byte array.""".asOneLine,
      )
      case object Raw extends StreamingFormat

      @title("Protobuf via Cypher")
      @description(
        """Records are serialized instances of `typeName` as described in the schema (a `.desc` descriptor file)
          |at `schemaUrl`. For every record received, the given Cypher query will be re-executed with the parameter
          |in the query set equal to the new (deserialized) Protobuf message.""".asOneLine,
      )
      final case class Protobuf(
        @description(
          "URL (or local filename) of the Protobuf `.desc` file to load to parse the `typeName`.",
        )
        schemaUrl: String,
        @description(
          "Message type name to use from the given `.desc` file as the incoming message type.",
        )
        typeName: String,
      ) extends StreamingFormat

      @title("Avro format")
      case class Avro(
        @description(
          "URL (or local filename) of the file to load to parse the avro schema.",
        )
        schemaUrl: String,
      ) extends StreamingFormat

      @title("Drop")
      @description("Ignore the data without further processing.")
      case object Drop extends StreamingFormat
    }
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

  case class RecordRetrySettings(
    @default(200)
    @description("Minimum duration to backoff between issuing retries, in milliseconds.")
    minBackoff: Int = 2000,
    @description("Maximum duration to backoff between issuing retries, in seconds.")
    @default(20)
    maxBackoff: Int = 20,
    @description("Adds jitter to the retry delay. Use 0 for no jitter.")
    @default(0.2)
    randomFactor: Double = 0.2,
    @description("Total number of allowed retries, when reached the last result will be emitted even if unsuccessful")
    @default(6)
    maxRetries: Int = 6,
  )

  /** Error handler defined for errors that affect only a single record. This is intended to handle errors in
    * a configurable way distinct from stream-level errors, where the entire stream fails - e.g. handling
    * a single corrupt record rather than a failure in the stream communication.
    */
  @title("On Record Error Handler")
  @description(
    """Settings for retrying failed record processing along with options for logging or
      |forwarding failed records to dead letter queues.""".asOneLine,
  )
  case class OnRecordErrorHandler(
    @description("Should record errors be retried. Useful when targeting a decode schema that can change.")
    retrySettings: Option[RecordRetrySettings] = None,
    @description("Should records be logged in case of failure.")
    @default(true)
    logRecord: Boolean = true,
    @description("Send failed records to a collection of dead letter queue destinations.")
    @default(DeadLetterQueueSettings())
    deadLetterQueueSettings: DeadLetterQueueSettings = DeadLetterQueueSettings(),
  )
}
