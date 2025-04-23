package com.thatdot.quine.routes

import java.time.Instant

import scala.util.control.NoStackTrace

import cats.data.NonEmptyList
import endpoints4s.algebra.Tag
import endpoints4s.generic.{docs, title, unnamed}
import sttp.tapir.Schema.annotations.{description, title => ttitle}

import com.thatdot.quine.routes.exts.{EndpointsWithCustomErrorText, NamespaceParameter}

sealed abstract class ValvePosition(position: String)

object ValvePosition {

  case object Open extends ValvePosition("Open")
  case object Closed extends ValvePosition("Closed")

}

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
  sealed abstract class TerminalStatus extends IngestStreamStatus(isTerminal = true, position = ValvePosition.Closed)

  @docs("The stream is currently actively running, and possibly waiting for new records to become available upstream.")
  @description(
    "The stream is currently actively running, and possibly waiting for new records to become available upstream.",
  )
  case object Running extends IngestStreamStatus(isTerminal = false, position = ValvePosition.Open)

  @docs("The stream has been paused by a user.")
  @description("The stream has been paused by a user.")
  case object Paused extends IngestStreamStatus(isTerminal = false, position = ValvePosition.Closed)

  @docs(
    """The stream has been restored from a saved state, but is not yet running: For example, after restarting the
      |application.""".stripMargin,
  )
  @description(
    """The stream has been restored from a saved state, but is not yet running: For example, after restarting the
      |application.""".stripMargin,
  )
  case object Restored extends IngestStreamStatus(isTerminal = false, position = ValvePosition.Closed)

  @docs("The stream has processed all records, and the upstream data source will not make more records available.")
  @description(
    "The stream has processed all records, and the upstream data source will not make more records available.",
  )
  case object Completed extends TerminalStatus

  @docs("The stream has been stopped by a user.")
  @description("The stream has been stopped by a user.")
  case object Terminated extends TerminalStatus

  @docs("The stream has been stopped by a failure during processing.")
  @description("The stream has been stopped by a failure during processing.")
  case object Failed extends TerminalStatus

  val states: Seq[IngestStreamStatus] = Seq(Running, Paused, Restored, Completed, Terminated, Failed)
}

/** Formats that have an embedded query member */
trait IngestQuery {
  val query: String
  val parameter: String
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
    "Indicator of whether the ingest is still running, completed, etc.",
  ) status: IngestStreamStatus,
  @docs("Error message about the ingest, if any") message: Option[String],
  @docs("Configuration of the ingest stream") settings: IngestStreamConfiguration,
  @docs("Statistics on progress of running ingest stream") stats: IngestStreamStats,
)

@title("Ingest Stream Info")
@docs("An active stream of data being ingested.")
final case class IngestStreamInfo(
  @docs(
    "Indicator of whether the ingest is still running, completed, etc.",
  ) status: IngestStreamStatus,
  @docs("Error message about the ingest, if any") message: Option[String],
  @docs("Configuration of the ingest stream") settings: IngestStreamConfiguration,
  @docs("Statistics on progress of running ingest stream") stats: IngestStreamStats,
) {
  def withName(name: String): IngestStreamInfoWithName = IngestStreamInfoWithName(
    name = name,
    status = status,
    message = message,
    settings = settings,
    stats = stats,
  )
}

@title("Statistics About a Running Ingest Stream")
@unnamed
final case class IngestStreamStats(
  // NB this is duplicated by rates.count -- maybe remove one?
  @docs("Number of source records (or lines) ingested so far")
  @description("Number of source records (or lines) ingested so far") ingestedCount: Long,
  @docs("Records/second over different time periods")
  @description("Records/second over different time periods") rates: RatesSummary,
  @docs("Bytes/second over different time periods")
  @description("Bytes/second over different time periods") byteRates: RatesSummary,
  @docs("Time (in ISO-8601 UTC time) when the ingestion was started")
  @description("Time (in ISO-8601 UTC time) when the ingestion was started") startTime: Instant,
  @docs("Time (in milliseconds) that that the ingest has been running")
  @description("Time (in milliseconds) that that the ingest has been running") totalRuntime: Long,
)
object IngestStreamStats {
  val example: IngestStreamStats = IngestStreamStats(
    ingestedCount = 123L,
    rates = RatesSummary(
      123L,
      14.1,
      14.5,
      14.15,
      14.0,
    ),
    byteRates = RatesSummary(
      8664000L,
      142030.1,
      145299.6,
      144287.6,
      144400.0,
    ),
    startTime = Instant.parse("2020-06-05T18:02:42.907Z"),
    60000L,
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
  @docs("Approximate rate per second since the meter was started") overall: Double,
)

trait MetricsSummarySchemas extends endpoints4s.generic.JsonSchemas {
  implicit lazy val ratesSummarySchema: Record[RatesSummary] =
    genericRecord[RatesSummary]
}

@unnamed
@title("AWS Credentials")
@ttitle("AWS Credentials")
@docs(
  """Explicit AWS access key and secret to use. If not provided, defaults to environmental credentials according to the
    |default AWS credential chain.
    |See: <https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html#credentials-default>.""".stripMargin,
)
@description(
  """Explicit AWS access key and secret to use. If not provided, defaults to environmental credentials according to the
    |default AWS credential chain.
    |See: <https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html#credentials-default>.""".stripMargin,
)
final case class AwsCredentials(accessKeyId: String, secretAccessKey: String)

@unnamed
@title("AWS Region")
@ttitle("AWS Region")
@docs(
  """AWS region code. e.g. `us-west-2`. If not provided, defaults according to the default AWS region provider chain.
    |See: <https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/region-selection.html#automatically-determine-the-aws-region-from-the-environment>.""".stripMargin,
)
@description(
  """AWS region code. e.g. `us-west-2`. If not provided, defaults according to the default AWS region provider chain.
    |See: <https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/region-selection.html#automatically-determine-the-aws-region-from-the-environment>.""".stripMargin,
)
final case class AwsRegion(region: String)

trait AwsConfigurationSchemas extends endpoints4s.generic.JsonSchemas {
  implicit lazy val awsCredentialsSchema: Record[AwsCredentials] = genericRecord[AwsCredentials]
  implicit val awsRegionSchema: Record[AwsRegion] = genericRecord[AwsRegion]
}

@unnamed
@title("Kafka Auto Offset Reset")
@ttitle("Kafka Auto Offset Reset")
@docs(
  """See [`auto.offset.reset` in the Kafka documentation](https://docs.confluent.io/current/installation/configuration/consumer-configs.html#auto.offset.reset).""",
)
@description(
  "See [`auto.offset.reset` in the Kafka documentation](https://docs.confluent.io/current/installation/configuration/consumer-configs.html#auto.offset.reset).",
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
@ttitle("Kafka Security Protocol")
@docs(
  "See [`security.protocol` in the Kafka documentation](https://kafka.apache.org/24/javadoc/org/apache/kafka/common/security/auth/SecurityProtocol.html).",
)
@description(
  "See [`security.protocol` in the Kafka documentation](https://kafka.apache.org/24/javadoc/org/apache/kafka/common/security/auth/SecurityProtocol.html).",
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
@ttitle("Kafka offset tracking mechanism")
@docs(
  "How to keep track of current offset when consuming from Kafka, if at all. " +
  """You could alternatively set "enable.auto.commit": "true" in kafkaProperties  for this ingest, """ +
  "but in that case messages will be lost if the ingest is stopped while processing messages",
)
@description(
  "How to keep track of current offset when consuming from Kafka, if at all. " +
  """You could alternatively set "enable.auto.commit": "true" in kafkaProperties  for this ingest, """ +
  "but in that case messages will be lost if the ingest is stopped while processing messages",
)
sealed abstract class KafkaOffsetCommitting
object KafkaOffsetCommitting {
  @unnamed
  @title("Explicit Commit")
  @ttitle("Explicit Commit")
  @docs(
    "Commit offsets to the specified Kafka consumer group on successful execution of the ingest query for that record.",
  )
  @description(
    "Commit offsets to the specified Kafka consumer group on successful execution of the ingest query for that record.",
  )
  final case class ExplicitCommit(
    @docs("Maximum number of messages in a single commit batch.")
    @description("Maximum number of messages in a single commit batch.")
    maxBatch: Long = 1000,
    @docs("Maximum interval between commits in milliseconds.")
    @description("Maximum interval between commits in milliseconds.")
    maxIntervalMillis: Int = 10000,
    @docs("Parallelism for async committing.")
    @description("Parallelism for async committing.")
    parallelism: Int = 100,
    @docs("Wait for a confirmation from Kafka on ack.")
    @description("Wait for a confirmation from Kafka on ack.")
    waitForCommitConfirmation: Boolean = true,
  ) extends KafkaOffsetCommitting
}

@title("Ingest Stream Configuration")
@docs("A specification of a data source and rules for consuming data from that source.")
sealed abstract class IngestStreamConfiguration {
  def slug: String
  def maximumPerSecond: Option[Int]
}
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
  status: Option[IngestStreamStatus],
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
@ttitle("Record encoding")
@docs("Record encoding format")
@description("Record encoding format")
sealed abstract class RecordDecodingType
object RecordDecodingType {
  @docs("Zlib compression")
  @description("Zlib compression")
  case object Zlib extends RecordDecodingType
  @docs("Gzip compression")
  @description("Gzip compression")
  case object Gzip extends RecordDecodingType
  @docs("Base64 encoding")
  @description("Base64 encoding")
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
      .replace('\n', ' '),
  )
  topics: Either[KafkaIngest.Topics, KafkaIngest.PartitionAssignments],
  @docs("Maximum number of records to process at once.")
  parallelism: Int = IngestRoutes.defaultWriteParallelism,
  @docs("A comma-separated list of Kafka broker servers.")
  bootstrapServers: String,
  @docs(
    "Consumer group ID that this ingest stream should report belonging to; defaults to the name of the ingest stream.",
  )
  groupId: Option[String],
  securityProtocol: KafkaSecurityProtocol = KafkaSecurityProtocol.PlainText,
  offsetCommitting: Option[KafkaOffsetCommitting],
  autoOffsetReset: KafkaAutoOffsetReset = KafkaAutoOffsetReset.Latest,
  @docs(
    """Map of Kafka client properties.
      |See <https://docs.confluent.io/platform/current/installation/configuration/consumer-configs.html#ak-consumer-configurations-for-cp>""".stripMargin,
  )
  kafkaProperties: KafkaIngest.KafkaProperties = Map.empty[String, String],
  @docs(
    "The offset at which this stream should complete; offsets are sequential integers starting at 0.",
  ) endingOffset: Option[Long],
  @docs("Maximum records to process per second.")
  maximumPerSecond: Option[Int],
  @docs("List of decodings to be applied to each input. The specified decodings are applied in declared array order.")
  @unnamed
  recordDecoders: Seq[RecordDecodingType] = Seq.empty,
) extends IngestStreamConfiguration {
  def getQuery: Option[String] = StreamedRecordFormat.getQuery(format)
  override def slug: String = "kafka"
}

object KinesisIngest {

  /** ⚠️ [[IteratorType]] and [[InitialPosition]] are different!
    *
    * Provides all supported iterator types that are available for use by the non-KCL implementation of Kinesis ingests.
    */
  @title("Kinesis Shard Iterator Type")
  @ttitle("Kinesis Shard Iterator Type")
  @docs(
    "See <https://docs.aws.amazon.com/kinesis/latest/APIReference/API_GetShardIterator.html#Streams-GetShardIterator-request-ShardIteratorType>.",
  )
  @description(
    "See <https://docs.aws.amazon.com/kinesis/latest/APIReference/API_Shttps://docs.aws.amazon.com/kinesis/latest/APIReference/API_StartingPosition.html>.",
  )
  sealed abstract class IteratorType

  object IteratorType {

    @unnamed
    sealed abstract class Unparameterized extends IteratorType

    @unnamed
    sealed abstract class Parameterized extends IteratorType

    @title("Latest")
    @docs("All records added to the shard since subscribing.")
    @ttitle("Latest")
    @description("All records added to the shard since subscribing.")
    @unnamed
    case object Latest extends Unparameterized

    @title("TrimHorizon")
    @docs("All records in the shard.")
    @ttitle("TrimHorizon")
    @description("All records in the shard.")
    @unnamed
    case object TrimHorizon extends Unparameterized

    @title("AtSequenceNumber")
    @docs("All records starting from the provided sequence number.")
    @ttitle("AtSequenceNumber")
    @description("All records starting from the provided sequence number.")
    @unnamed
    final case class AtSequenceNumber(sequenceNumber: String) extends Parameterized

    @title("AfterSequenceNumber")
    @docs("All records starting after the provided sequence number.")
    @ttitle("AfterSequenceNumber")
    @description("All records starting after the provided sequence number.")
    @unnamed
    final case class AfterSequenceNumber(sequenceNumber: String) extends Parameterized

    // JS-safe long gives ms until the year 287396-ish
    @title("AtTimestamp")
    @docs("All records starting from the provided unix millisecond timestamp.")
    @ttitle("AtTimestamp")
    @description("All records starting from the provided unix millisecond timestamp.")
    @unnamed
    final case class AtTimestamp(millisSinceEpoch: Long) extends Parameterized
  }

  /** ⚠️ [[InitialPosition]] and [[IteratorType]] are different!
    *
    * Provides all supported iterator types that are available for use by the non-KCL implementation of Kinesis ingests.
    */
  @title("Kinesis Initial Position Type")
  @description(
    "See <https://docs.aws.amazon.com/kinesis/latest/APIReference/API_StartingPosition.html>.",
  )
  sealed abstract class InitialPosition

  object InitialPosition {

    @unnamed
    sealed abstract class Unparameterized extends InitialPosition

    @unnamed
    sealed abstract class Parameterized extends InitialPosition

    @title("Latest")
    @description("All records added to the shard since subscribing.")
    @unnamed
    case object Latest extends Unparameterized

    @title("TrimHorizon")
    @description("All records in the shard.")
    @unnamed
    case object TrimHorizon extends Unparameterized

    @title("AtTimestamp")
    @description("All records starting from the provided date-time. Month and day-of-month are indexed starting at 1.")
    final case class AtTimestamp(year: Int, month: Int, dayOfMonth: Int, hourOfDay: Int, minute: Int, second: Int)
        extends Parameterized
  }

  case class KinesisSchedulerSourceSettings(
    @docs(
      """Sets the KinesisSchedulerSourceSettings buffer size. Buffer size must be greater than 0; use size 1 to disable
        |stage buffering.""".stripMargin,
    )
    bufferSize: Option[Int],
    @docs(
      "Sets the KinesisSchedulerSourceSettings backpressureTimeout in milliseconds",
    )
    backpressureTimeoutMillis: Option[Long],
  )

  @title("Scheduler Checkpoint Settings")
  @ttitle("Scheduler Checkpoint Settings")
  @docs("Settings for batch configuration for Kinesis stream checkpointing.")
  @description("Settings for batch configuration for Kinesis stream checkpointing.")
  @unnamed
  final case class KinesisCheckpointSettings(
    @docs("Whether to disable checkpointing, which is enabled by default.")
    @description("Whether to disable checkpointing, which is enabled by default.")
    disableCheckpointing: Boolean = false,
    @docs("Maximum checkpoint batch size. Appropriate only when checkpointing is not disabled.")
    @description("Maximum checkpoint batch size. Appropriate only when checkpointing is not disabled.")
    maxBatchSize: Option[Int],
    @docs("Maximum checkpoint batch wait time in ms. Appropriate only when checkpointing is not disabled.")
    @description("Maximum checkpoint batch wait time in ms. Appropriate only when checkpointing is not disabled.")
    maxBatchWaitMillis: Option[Long],
  )

  @title("KCLConfiguration")
  @docs("A complex object comprising abbreviated configuration objects used by the Kinesis Client Library (KCL).")
  case class KCLConfiguration(
    configsBuilder: Option[ConfigsBuilder],
    leaseManagementConfig: Option[LeaseManagementConfig],
    retrievalSpecificConfig: Option[RetrievalSpecificConfig],
    processorConfig: Option[ProcessorConfig],
    coordinatorConfig: Option[CoordinatorConfig],
    lifecycleConfig: Option[LifecycleConfig],
    retrievalConfig: Option[RetrievalConfig],
    metricsConfig: Option[MetricsConfig],
  )

  @title("ConfigsBuilder")
  @docs("Abbreviated configuration for the KCL configurations builder.")
  case class ConfigsBuilder(
    @docs("""If the default, provided by `applicationName`, is unsuitable,
            |this will be the table name used for the Amazon DynamoDB lease table.""".stripMargin)
    tableName: Option[String],
    @docs("A unique identifier that represents this instantiation of the application processor. This must be unique.")
    workerIdentifier: Option[String],
  )

  sealed abstract class BillingMode extends Product with Serializable
  object BillingMode {
    @title("Provisioned")
    @docs("Provisioned billing.")
    case object PROVISIONED extends BillingMode
    @title("Pay-Per-Request")
    @docs("Pay-per-request billing.")
    case object PAY_PER_REQUEST extends BillingMode
    @title("Unknown")
    @docs("The billing mode is not one of these provided options.")
    case object UNKNOWN_TO_SDK_VERSION extends BillingMode
  }

  case class LeaseManagementConfig(
    @docs(
      """The number of milliseconds that must pass before you can consider a lease owner to have failed. For applications that have a large number of shards, this may be set to a higher number to reduce the number of DynamoDB IOPS required for tracking leases.""".stripMargin,
    )
    failoverTimeMillis: Option[Long],
    @docs("The time between shard sync calls.")
    shardSyncIntervalMillis: Option[Long],
    @docs("When set, leases are removed as soon as the child leases have started processing.")
    cleanupLeasesUponShardCompletion: Option[Boolean],
    @docs("When set, child shards that have an open shard are ignored. This is primarily for DynamoDB Streams.")
    ignoreUnexpectedChildShards: Option[Boolean],
    @docs(
      """The maximum number of leases a single worker should accept. Setting it too low may cause data loss if workers can't
        |process all shards, and lead to a suboptimal lease assignment among workers. Consider total shard count, number
        |of workers, and worker processing capacity when configuring it.""".stripMargin,
    )
    maxLeasesForWorker: Option[Int],
    @docs(
      """Controls the size of the lease renewer thread pool. The more leases that your application could take, the larger
        |this pool should be.""".stripMargin,
    )
    maxLeaseRenewalThreads: Option[Int],
    @docs(
      """Determines the capacity mode of the lease table created in DynamoDB. There are two options: on-demand mode
        |(PAY_PER_REQUEST) and provisioned mode. We recommend using the default setting of on-demand mode because it
        |automatically scales to accommodate your workload without the need for capacity planning.""".stripMargin,
    )
    billingMode: Option[BillingMode],
    @docs(
      """The DynamoDB read capacity that is used if the Kinesis Client Library needs to create a new DynamoDB lease table
        |with provisioned capacity mode. You can ignore this configuration if you are using the default on-demand capacity
        |mode in `billingMode` configuration.""".stripMargin,
    )
    initialLeaseTableReadCapacity: Option[Int],
    @docs(
      """The DynamoDB read capacity that is used if the Kinesis Client Library needs to create a new DynamoDB lease table.
        |You can ignore this configuration if you are using the default on-demand capacity mode in `billingMode`
        |configuration.""".stripMargin,
    )
    initialLeaseTableWriteCapacity: Option[Int],
    @docs(
      """A percentage value that determines when the load balancing algorithm should consider reassigning shards among
        |workers.
        |This is a new configuration introduced in KCL 3.x.""".stripMargin,
    )
    reBalanceThresholdPercentage: Option[Int],
    @docs(
      """A percentage value that is used to dampen the amount of load that will be moved from the overloaded worker in a
        |single rebalance operation.
        |This is a new configuration introduced in KCL 3.x.""".stripMargin,
    )
    dampeningPercentage: Option[Int],
    @docs(
      """Determines whether additional lease still needs to be taken from the overloaded worker even if it causes total
        |amount of lease throughput taken to exceed the desired throughput amount.
        |This is a new configuration introduced in KCL 3.x.""".stripMargin,
    )
    allowThroughputOvershoot: Option[Boolean],
    @docs(
      """Determines if KCL should ignore resource metrics from workers (such as CPU utilization) when reassigning leases
        |and load balancing. Set this to TRUE if you want to prevent KCL from load balancing based on CPU utilization.
        |This is a new configuration introduced in KCL 3.x.""".stripMargin,
    )
    disableWorkerMetrics: Option[Boolean],
    @docs("""Amount of the maximum throughput to assign to a worker during the lease assignment.
            |This is a new configuration introduced in KCL 3.x.""".stripMargin)
    maxThroughputPerHostKBps: Option[Double],
    @docs(
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
    @docs("""Specifies the minimum time (in milliseconds) to wait for the current shard's RecordProcessor to gracefully
            |shut down before forcefully transferring the lease to the next owner.
            |If your processRecords method typically runs longer than the default value, consider increasing this setting.
            |This ensures the RecordProcessor has sufficient time to complete its processing before the lease transfer occurs.
            |This is a new configuration introduced in KCL 3.x.""".stripMargin)
    gracefulLeaseHandoffTimeoutMillis: Option[Long],
  )

  sealed abstract class RetrievalSpecificConfig

  object RetrievalSpecificConfig {

    case class FanOutConfig(
      @docs("The ARN of an already created consumer, if this is set no automatic consumer creation will be attempted.")
      consumerArn: Option[String],
      @docs("The name of the consumer to create. If this isn't set the `applicationName` will be used.")
      consumerName: Option[String],
      @docs(
        """The maximum number of retries for calling DescribeStreamSummary.
          |Once exhausted the consumer creation/retrieval will fail.""".stripMargin,
      )
      maxDescribeStreamSummaryRetries: Option[Int],
      @docs(
        """The maximum number of retries for calling DescribeStreamConsumer.
          |Once exhausted the consumer creation/retrieval will fail.""".stripMargin,
      )
      maxDescribeStreamConsumerRetries: Option[Int],
      @docs(
        """The maximum number of retries for calling RegisterStreamConsumer.
          |Once exhausted the consumer creation/retrieval will fail.""".stripMargin,
      )
      registerStreamConsumerRetries: Option[Int],
      @docs("The maximum amount of time that will be made between failed calls.")
      retryBackoffMillis: Option[Long],
    ) extends RetrievalSpecificConfig

    case class PollingConfig(
      @docs("Allows setting the maximum number of records that Kinesis returns.")
      maxRecords: Option[Int],
      @docs("Configures the delay between GetRecords attempts for failures.")
      retryGetRecordsInSeconds: Option[Int],
      @docs("The thread pool size used for GetRecords.")
      maxGetRecordsThreadPool: Option[Int],
      @docs("""Determines how long KCL waits between GetRecords calls to poll the data from data streams.
          |The unit is milliseconds.""".stripMargin)
      idleTimeBetweenReadsInMillis: Option[Long],
    ) extends RetrievalSpecificConfig
  }

  case class ProcessorConfig(
    @docs("When set, the record processor is called even when no records were provided from Kinesis.")
    callProcessRecordsEvenForEmptyRecordList: Option[Boolean],
  )

  sealed trait ShardPrioritization extends Product with Serializable
  object ShardPrioritization {
    @unnamed
    sealed abstract class Unparameterized extends ShardPrioritization

    @unnamed
    sealed abstract class Parameterized extends ShardPrioritization

    case object NoOpShardPrioritization extends Unparameterized

    @docs("Processes shard parents first, limited by a 'max depth' argument.")
    case class ParentsFirstShardPrioritization(maxDepth: Int) extends Parameterized
  }

  sealed trait ClientVersionConfig extends Product with Serializable
  object ClientVersionConfig {
    case object CLIENT_VERSION_CONFIG_COMPATIBLE_WITH_2X extends ClientVersionConfig
    case object CLIENT_VERSION_CONFIG_3X extends ClientVersionConfig
  }

  case class CoordinatorConfig(
    @docs(
      """How often a record processor should poll to see if the parent shard has been completed.
        |The unit is milliseconds.""".stripMargin,
    )
    parentShardPollIntervalMillis: Option[Long],
    @docs("Disable synchronizing shard data if the lease table contains existing leases.")
    skipShardSyncAtWorkerInitializationIfLeasesExist: Option[Boolean],
    @docs(
      """Which shard prioritization to use.
        |
        |Options: NoOpShardPrioritization, ParentsFirstShardPrioritization(maxDepth: Int)""".stripMargin,
    )
    shardPrioritization: Option[ShardPrioritization],
    @docs(
      """Determines which KCL version compatibility mode the application will run in. This configuration is only for the
        |migration from previous KCL versions. When migrating to 3.x, you need to set this configuration to `CLIENT_VERSION_CONFIG_COMPATIBLE_WITH_2X`. You can remove this configuration when you complete the migration.""".stripMargin,
    )
    clientVersionConfig: Option[ClientVersionConfig],
  )
  case class LifecycleConfig(
    @docs("The time to wait to retry failed KCL tasks. The unit is milliseconds.")
    taskBackoffTimeMillis: Option[Long],
    @docs("How long to wait before a warning is logged if a task hasn't completed.")
    logWarningForTaskAfterMillis: Option[Long],
  )
  case class RetrievalConfig(
    @docs(
      "The number of milliseconds to wait between calls to `ListShards` when failures occur. The unit is milliseconds.",
    )
    listShardsBackoffTimeInMillis: Option[Long],
    @docs("The maximum number of times that `ListShards` retries before giving up.")
    maxListShardsRetryAttempts: Option[Int],
  )
  sealed trait MetricsLevel extends Product with Serializable
  object MetricsLevel {
    case object NONE extends MetricsLevel

    /** SUMMARY metrics level can be used to emit only the most significant metrics. */
    case object SUMMARY extends MetricsLevel

    /** DETAILED metrics level can be used to emit all metrics. */
    case object DETAILED extends MetricsLevel
  }

  sealed abstract class MetricsDimension(val value: String) extends Product with Serializable
  object MetricsDimension {
    case object OPERATION_DIMENSION_NAME extends MetricsDimension("Operation")
    case object SHARD_ID_DIMENSION_NAME extends MetricsDimension("ShardId")
    case object STREAM_IDENTIFIER extends MetricsDimension("StreamId")
    case object WORKER_IDENTIFIER extends MetricsDimension("WorkerIdentifier")
  }

  case class MetricsConfig(
    @docs("Specifies the maximum duration (in milliseconds) to buffer metrics before publishing them to CloudWatch.")
    metricsBufferTimeMillis: Option[Long],
    @docs("Specifies the maximum number of metrics to buffer before publishing to CloudWatch.")
    metricsMaxQueueSize: Option[Int],
    @docs("Specifies the granularity level of CloudWatch metrics to be enabled and published.")
    metricsLevel: Option[MetricsLevel],
    @docs("Controls allowed dimensions for CloudWatch Metrics.")
    metricsEnabledDimensions: Option[Set[MetricsDimension]],
  )

}

@title("Kinesis Data Stream")
@unnamed
@docs("A stream of data being ingested from Kinesis.")
final case class KinesisIngest(
  @docs("The format used to decode each Kinesis record.")
  format: StreamedRecordFormat = IngestRoutes.defaultStreamedRecordFormat,
  @docs("Name of the Kinesis stream to ingest.")
  streamName: String,
  @docs(
    "Shards IDs within the named kinesis stream to ingest; if empty or excluded, all shards on the stream are processed.",
  )
  shardIds: Option[Set[String]],
  @docs("Maximum number of records to write simultaneously.")
  parallelism: Int = IngestRoutes.defaultWriteParallelism,
  credentials: Option[AwsCredentials],
  region: Option[AwsRegion],
  @docs(
    """Shard iterator type.
      |
      |Options: Latest, TrimHorizon, AtSequenceNumber(sequenceNumber: String), AfterSequenceNumber(sequenceNumber: String), AtTimestamp(millisSinceEpoch: Long)
      |Default: Latest""".stripMargin,
  )
  iteratorType: KinesisIngest.IteratorType = KinesisIngest.IteratorType.Latest,
  @docs("Number of retries to attempt on Kineses error.")
  numRetries: Int = 3,
  @docs("Maximum records to process per second.")
  maximumPerSecond: Option[Int],
  @docs("List of decodings to be applied to each input, where specified decodings are applied in declared array order.")
  recordDecoders: Seq[RecordDecodingType] = Seq.empty,
) extends IngestStreamConfiguration {
  override def slug: String = "kinesis"
}

@title("Kinesis Data Stream via Kinesis Client Library (KCL)")
@unnamed
@docs("A stream of data being ingested from Kinesis using KCL.")
final case class KinesisKCLIngest(
  @docs("The format used to decode each Kinesis record.")
  format: StreamedRecordFormat = IngestRoutes.defaultStreamedRecordFormat,
  @docs("Name of the application (also used as the default DynamoDB lease table name unless overridden).")
  applicationName: String,
  @docs("Name of the Kinesis stream to ingest.")
  kinesisStreamName: String,
  @docs("Maximum number of records to write simultaneously.")
  parallelism: Int = IngestRoutes.defaultWriteParallelism,
  credentials: Option[AwsCredentials],
  region: Option[AwsRegion],
  @docs(
    """The initial position value for lease configuration.
      |
      |Options: Latest, TrimHorizon, AtSequenceNumber(sequenceNumber: String)
      |Default: Latest""".stripMargin,
  )
  initialPosition: KinesisIngest.InitialPosition = KinesisIngest.InitialPosition.Latest,
  @docs("Number of retries to attempt on Kineses error.")
  numRetries: Int = 3,
  @docs("Maximum records to process per second.")
  maximumPerSecond: Option[Int],
  @docs("List of decodings to be applied to each input, where specified decodings are applied in declared array order.")
  recordDecoders: Seq[RecordDecodingType] = Seq.empty,
  @docs("Optional additional settings for the KCL Scheduler.")
  schedulerSourceSettings: Option[KinesisIngest.KinesisSchedulerSourceSettings],
  @docs("Stream checkpoint settings.")
  checkpointSettings: Option[KinesisIngest.KinesisCheckpointSettings],
  @docs(
    """Optional advanced configuration, derived from the KCL 3.x documented configuration table
      |(https://docs.aws.amazon.com/streams/latest/dev/kcl-configuration.html), but without fields that are available
      |elsewhere in this API object schema.""".stripMargin,
  )
  advancedSettings: Option[KinesisIngest.KCLConfiguration],
) extends IngestStreamConfiguration {
  override def slug: String = "kinesisKCL"
}

@title("Server Sent Events Stream")
@unnamed
@docs(
  """A server-issued event stream, as might be handled by the EventSource JavaScript API. Only consumes the `data`
    |portion of an event.""".stripMargin,
)
final case class ServerSentEventsIngest(
  @docs("Format used to decode each event's `data`.")
  format: StreamedRecordFormat = IngestRoutes.defaultStreamedRecordFormat,
  @docs("URL of the server sent event stream.") url: String,
  @docs("Maximum number of records to ingest simultaneously.")
  parallelism: Int = IngestRoutes.defaultWriteParallelism,
  @docs("Maximum records to process per second.") maximumPerSecond: Option[Int],
  @docs(
    "List of encodings that have been applied to each input. Decoding of each type is applied in order.",
  ) recordDecoders: Seq[RecordDecodingType] = Seq.empty,
) extends IngestStreamConfiguration {
  override def slug: String = "sse"
}

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
  recordDecoders: Seq[RecordDecodingType] = Seq.empty,
) extends IngestStreamConfiguration {
  override def slug: String = "sqs"
}

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
  encoding: String = "UTF-8",
) extends IngestStreamConfiguration {
  override def slug: String = "websocket"
  override val maximumPerSecond = None
}

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
    @docs("Name of the Cypher parameter to populate with the JSON value.") parameter: String = "that",
  ) extends StreamedRecordFormat
      with IngestQuery

  final case class QuinePatternJson(
    @docs("Cypher query to execute on each record.") query: String,
    @docs("Name of the Cypher parameter to populate with the JSON value.") parameter: String = "that",
  ) extends StreamedRecordFormat
      with IngestQuery

  @title("Raw Bytes via Cypher")
  @unnamed
  @docs("""Records may have any format. For every record received, the
          |given Cypher query will be re-executed with the parameter in the query set
          |equal to the new value as a Cypher byte array.
  """.stripMargin)
  final case class CypherRaw(
    @docs("Cypher query to execute on each record.") query: String,
    @docs("Name of the Cypher parameter to populate with the byte array.") parameter: String = "that",
  ) extends StreamedRecordFormat
      with IngestQuery

  @title("Protobuf via Cypher")
  @unnamed
  @docs(
    "Records are serialized instances of `typeName` as described in the schema (a `.desc` descriptor file) at " +
    "`schemaUrl`. For every record received, the given Cypher query will be re-executed with the parameter " +
    "in the query set equal to the new (deserialized) Protobuf message.",
  )
  final case class CypherProtobuf(
    @docs("Cypher query to execute on each record.") query: String,
    @docs("Name of the Cypher parameter to populate with the Protobuf message.") parameter: String = "that",
    @docs(
      "URL (or local filename) of the Protobuf `.desc` file to load to parse the `typeName`.",
    ) schemaUrl: String,
    @docs(
      "Message type name to use from the given `.desc` file as the incoming message type.",
    ) typeName: String,
  ) extends StreamedRecordFormat
      with IngestQuery

  @title("Drop")
  @unnamed
  @docs("Ignore the data without further processing.")
  case object Drop extends StreamedRecordFormat
  def getQuery(f: StreamedRecordFormat): Option[String] = f match {
    case Drop => None
    case c: IngestQuery => Some(c.query)
  }
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
    """The text encoding scheme for the file. UTF-8, US-ASCII and ISO-8859-1 are supported -- other encodings will be
      |transcoded to UTF-8 on the fly (and ingest may be slower).""".stripMargin,
  )
  encoding: String = "UTF-8",
  @docs("Maximum number of records to process at once.")
  parallelism: Int = IngestRoutes.defaultWriteParallelism,
  @docs("Maximum size (in bytes) of any line in the file.")
  maximumLineSize: Int = IngestRoutes.defaultMaximumLineSize,
  @docs(
    s"""Begin processing at the record with the given index. Useful for skipping some number of lines (e.g. CSV headers)
       |or resuming ingest from a partially consumed file.""".stripMargin,
  )
  startAtOffset: Long = 0L,
  @docs(s"Optionally limit how many records are ingested from this file.")
  ingestLimit: Option[Long],
  @docs("Maximum number of records to process per second.")
  maximumPerSecond: Option[Int],
  @docs(
    "Ingest mode for reading from a non-regular file type; default is to auto-detect if file is named pipe.",
  ) fileIngestMode: Option[FileIngestMode],
) extends IngestStreamConfiguration {
  override def slug: String = "file"
}

@title("S3 File ingest (Experimental)")
@unnamed
@docs(
  """An ingest stream from a file in S3, newline delimited. This ingest source is
    |experimental and is subject to change without warning. In particular, there are
    |known issues with durability when the stream is inactive for at least 1 minute.""".stripMargin
    .replace('\n', ' '),
)
final case class S3Ingest(
  @docs("format used to decode each incoming line from a file in S3")
  format: FileIngestFormat = IngestRoutes.defaultTextFileFormat,
  @docs("S3 bucket name")
  bucket: String,
  @docs("S3 file name")
  key: String,
  @docs(
    "text encoding used to read the file. Only UTF-8, US-ASCII and ISO-8859-1 are directly " +
    "supported -- other encodings will transcoded to UTF-8 on the fly (and ingest may be slower).",
  )
  encoding: String = "UTF-8",
  @docs("maximum number of records being processed at once")
  parallelism: Int = IngestRoutes.defaultWriteParallelism,
  credentials: Option[AwsCredentials],
  @docs("maximum size (in bytes) of any line in the file")
  maximumLineSize: Int = IngestRoutes.defaultMaximumLineSize,
  @docs(s"""start at the record with the given index. Useful for skipping some number of lines (e.g. CSV headers) or
           |resuming ingest from a partially consumed file""".stripMargin)
  startAtOffset: Long = 0L,
  @docs(s"optionally limit how many records are ingested from this file.")
  ingestLimit: Option[Long],
  @docs("maximum records to process per second")
  maximumPerSecond: Option[Int],
) extends IngestStreamConfiguration {
  override def slug: String = "s3"
}

/** Standard input ingest stream configuration */
@title("Standard Input Ingest Stream")
@unnamed
@docs("An active stream of data being ingested from standard input to this Quine process.")
final case class StandardInputIngest(
  format: FileIngestFormat = IngestRoutes.defaultFileRecordFormat,
  @docs(
    "Text encoding used to read data. Only UTF-8, US-ASCII and ISO-8859-1 are directly supported " +
    "-- other encodings will be transcoded to UTF-8 on the fly (and ingest may be slower).",
  )
  encoding: String = "UTF-8",
  @docs("Maximum number of records process at once.")
  parallelism: Int = IngestRoutes.defaultWriteParallelism,
  @docs("Maximum size (in bytes) of any line.")
  maximumLineSize: Int = IngestRoutes.defaultMaximumLineSize,
  @docs("Maximum records to process per second.")
  maximumPerSecond: Option[Int],
) extends IngestStreamConfiguration {
  override def slug: String = "stdin"
}

/** Number iterator ingest source for easy testing */
@unnamed
@title("Number Iterator Ingest")
@docs(
  "An infinite ingest stream which requires no data source and just produces new sequential numbers" +
  " every time the stream is (re)started. The numbers are Java `Long`s` and will wrap at their max value.",
)
case class NumberIteratorIngest(
  format: FileIngestFormat = IngestRoutes.defaultNumberFormat,
  @docs("Begin the stream with this number.")
  startAtOffset: Long = 0L,
  @docs("Optionally end the stream after consuming this many items.")
  ingestLimit: Option[Long],
  @docs(
    """Limit the maximum rate of production to this many records per second.
      |Note that this may be slowed by backpressure elsewhere in the system.""".stripMargin,
  )
  maximumPerSecond: Option[Int],
  @docs("Maximum number of records to process at once.")
  parallelism: Int = IngestRoutes.defaultWriteParallelism,
) extends IngestStreamConfiguration {
  override def slug: String = "numberIterator"
}

@unnamed
@title("File Ingest Format")
@docs("Format by which a file will be interpreted as a stream of elements for ingest.")
sealed abstract class FileIngestFormat extends IngestQuery {
  val query: String
  val parameter: String
}
object FileIngestFormat {

  /** Create using a cypher query, passing each line in as a string */
  @title("CypherLine")
  @unnamed()
  @docs("""For every line (LF/CRLF delimited) in the source, the given Cypher query will be
          |re-executed with the parameter in the query set equal to a string matching
          |the new line value. The newline is not included in this string.""".stripMargin.replace('\n', ' '))
  final case class CypherLine(
    @docs("Cypher query to execute on each line") query: String,
    @docs("name of the Cypher parameter holding the string line value") parameter: String = "that",
  ) extends FileIngestFormat

  @title("QuinePatternLine")
  @unnamed()
  @docs("""TODO add some docs here
      |
      |""".stripMargin.replace('\n', ' '))
  final case class QuinePatternLine(
    @docs("QuinePattern query to execute on each line") query: String,
    @docs("name of the QuinePattern parameter holding the string line value") parameter: String = "that",
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
    @docs("name of the Cypher parameter holding the JSON value") parameter: String = "that",
  ) extends FileIngestFormat

  @title("CypherJson")
  @unnamed()
  @docs("""|TODO Add some docs here
  """.stripMargin.replace('\n', ' '))
  final case class QuinePatternJson(
    @docs("QuinePAttern query to execute on each record") query: String,
    @docs("name of the QuinePattern parameter holding the JSON value") parameter: String = "that",
  ) extends FileIngestFormat

  @title("QuinePatternCSV")
  @unnamed()
  @docs("""blah blah blah""")
  final case class QuinePatternCsv(
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
    escapeChar: CsvCharacter = CsvCharacter.Backslash,
  ) extends FileIngestFormat {
    require(delimiter != quoteChar, "Different characters must be used for `delimiter` and `quoteChar`.")
    require(delimiter != escapeChar, "Different characters must be used for `delimiter` and `escapeChar`.")
    require(quoteChar != escapeChar, "Different characters must be used for `quoteChar` and `escapeChar`.")
  }

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
    escapeChar: CsvCharacter = CsvCharacter.Backslash,
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

  // FIXME Right now, this doesn't seem to help. `iteratorType` in OpenAPI shows as "one of: string"
  implicit lazy val iteratorTypeSchema: JsonSchema[KinesisIngest.IteratorType] = {
    import KinesisIngest.IteratorType
    val unparameterizedKinesisIteratorSchema: Enum[IteratorType.Unparameterized] =
      stringEnumeration[IteratorType.Unparameterized](
        Seq(IteratorType.TrimHorizon, IteratorType.Latest),
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

  implicit lazy val initialPositionSchema: JsonSchema[KinesisIngest.InitialPosition] = {
    import KinesisIngest.InitialPosition
    val unparameterizedKinesisIteratorSchema: Enum[InitialPosition.Unparameterized] =
      stringEnumeration[InitialPosition.Unparameterized](
        Seq(InitialPosition.TrimHorizon, InitialPosition.Latest),
      )(_.toString)

    val parameterizedKinesisIteratorSchema: Tagged[InitialPosition.Parameterized] =
      genericTagged[InitialPosition.Parameterized]

    // Try the string enumeration first, then try the parameterized versions.
    orFallbackToJsonSchema(unparameterizedKinesisIteratorSchema, parameterizedKinesisIteratorSchema)
      .xmap(_.merge) {
        case unparameterized: InitialPosition.Unparameterized => Left(unparameterized)
        case parameterized: InitialPosition.Parameterized => Right(parameterized)
      }
  }

  private val exampleCheckpointSettings: KinesisIngest.KinesisCheckpointSettings =
    KinesisIngest.KinesisCheckpointSettings(
      maxBatchSize = Some(1000),
      maxBatchWaitMillis = Some(10000),
    )
  implicit val kinesisCheckpointSettingsSchema: Record[KinesisIngest.KinesisCheckpointSettings] =
    genericRecord[KinesisIngest.KinesisCheckpointSettings].withExample(exampleCheckpointSettings)

  private val examplePollingConfig: KinesisIngest.RetrievalSpecificConfig.PollingConfig =
    KinesisIngest.RetrievalSpecificConfig.PollingConfig(
      maxRecords = Some(1),
      retryGetRecordsInSeconds = Some(1),
      maxGetRecordsThreadPool = Some(1),
      idleTimeBetweenReadsInMillis = Some(2222),
    )

  implicit val retrievalSpecificConfigSchema: Tagged[KinesisIngest.RetrievalSpecificConfig] =
    genericTagged[KinesisIngest.RetrievalSpecificConfig].withExample(examplePollingConfig)

  private val exampleProcessorConfig: KinesisIngest.ProcessorConfig = KinesisIngest.ProcessorConfig(
    callProcessRecordsEvenForEmptyRecordList = Some(true),
  )
  implicit val exampleProcessorConfigSchema: Record[KinesisIngest.ProcessorConfig] =
    genericRecord[KinesisIngest.ProcessorConfig].withExample(exampleProcessorConfig)

  // FIXME Right now, this doesn't seem to help. `shardPrioritization` in OpenAPI shows as "one of: string"
  implicit val shardPrioritizationSchema: JsonSchema[KinesisIngest.ShardPrioritization] = {
    val unparameterizedShardPrioritizationSchema: Enum[KinesisIngest.ShardPrioritization.Unparameterized] =
      stringEnumeration[KinesisIngest.ShardPrioritization.Unparameterized](
        Seq(KinesisIngest.ShardPrioritization.NoOpShardPrioritization),
      )(_.toString)

    val parameterizedShardPrioritizationSchema: Tagged[KinesisIngest.ShardPrioritization.Parameterized] =
      genericTagged[KinesisIngest.ShardPrioritization.Parameterized]

    // Try the string enumeration first, then try the parameterized versions.
    orFallbackToJsonSchema(unparameterizedShardPrioritizationSchema, parameterizedShardPrioritizationSchema)
      .xmap(_.merge) {
        case unparameterized: KinesisIngest.ShardPrioritization.Unparameterized => Left(unparameterized)
        case parameterized: KinesisIngest.ShardPrioritization.Parameterized => Right(parameterized)
      }
  }

  implicit val clientVersionConfigSchema: Enum[KinesisIngest.ClientVersionConfig] =
    stringEnumeration(
      Seq(
        KinesisIngest.ClientVersionConfig.CLIENT_VERSION_CONFIG_3X,
        KinesisIngest.ClientVersionConfig.CLIENT_VERSION_CONFIG_COMPATIBLE_WITH_2X,
      ),
    )(_.toString).withExample(KinesisIngest.ClientVersionConfig.CLIENT_VERSION_CONFIG_3X)

  private val exampleCoordinatorConfig: KinesisIngest.CoordinatorConfig = KinesisIngest.CoordinatorConfig(
    parentShardPollIntervalMillis = Some(2222),
    skipShardSyncAtWorkerInitializationIfLeasesExist = Some(true),
    shardPrioritization = Some(KinesisIngest.ShardPrioritization.NoOpShardPrioritization),
    clientVersionConfig = Some(KinesisIngest.ClientVersionConfig.CLIENT_VERSION_CONFIG_3X),
  )
  implicit val exampleCoordinatorConfigSchema: Record[KinesisIngest.CoordinatorConfig] =
    genericRecord[KinesisIngest.CoordinatorConfig].withExample(exampleCoordinatorConfig)

  private val exampleLifecycleConfig: KinesisIngest.LifecycleConfig = KinesisIngest.LifecycleConfig(
    taskBackoffTimeMillis = Some(2222),
    logWarningForTaskAfterMillis = Some(2222),
  )
  implicit val exampleLifecycleConfigSchema: Record[KinesisIngest.LifecycleConfig] =
    genericRecord[KinesisIngest.LifecycleConfig].withExample(exampleLifecycleConfig)

  private val exampleRetrievalConfig: KinesisIngest.RetrievalConfig = KinesisIngest.RetrievalConfig(
    listShardsBackoffTimeInMillis = Some(2222),
    maxListShardsRetryAttempts = Some(1),
  )
  implicit val exampleRetrievalConfigSchema: Record[KinesisIngest.RetrievalConfig] =
    genericRecord[KinesisIngest.RetrievalConfig].withExample(exampleRetrievalConfig)

  implicit val metricsLevelSchema: Enum[KinesisIngest.MetricsLevel] =
    stringEnumeration(
      Seq(KinesisIngest.MetricsLevel.DETAILED, KinesisIngest.MetricsLevel.SUMMARY, KinesisIngest.MetricsLevel.NONE),
    )(_.toString)
      .withExample(KinesisIngest.MetricsLevel.SUMMARY)

  implicit val metricsDimensionSchema: Enum[KinesisIngest.MetricsDimension] =
    stringEnumeration(
      Seq(
        KinesisIngest.MetricsDimension.SHARD_ID_DIMENSION_NAME,
        KinesisIngest.MetricsDimension.OPERATION_DIMENSION_NAME,
        KinesisIngest.MetricsDimension.STREAM_IDENTIFIER,
        KinesisIngest.MetricsDimension.WORKER_IDENTIFIER,
      ),
    )(_.toString)
      .withExample(KinesisIngest.MetricsDimension.STREAM_IDENTIFIER)

  private val exampleMetricsConfig: KinesisIngest.MetricsConfig = KinesisIngest.MetricsConfig(
    metricsBufferTimeMillis = Some(2222),
    metricsMaxQueueSize = Some(1),
    metricsLevel = Some(KinesisIngest.MetricsLevel.DETAILED),
    metricsEnabledDimensions = Some(
      Set(
        KinesisIngest.MetricsDimension.SHARD_ID_DIMENSION_NAME,
        KinesisIngest.MetricsDimension.OPERATION_DIMENSION_NAME,
      ),
    ),
  )
  implicit val exampleMetricsConfigSchema: Record[KinesisIngest.MetricsConfig] =
    genericRecord[KinesisIngest.MetricsConfig].withExample(exampleMetricsConfig)

  implicit val billingModeSchema: Enum[KinesisIngest.BillingMode] =
    stringEnumeration(
      Seq(
        KinesisIngest.BillingMode.PROVISIONED,
        KinesisIngest.BillingMode.PAY_PER_REQUEST,
        KinesisIngest.BillingMode.UNKNOWN_TO_SDK_VERSION,
      ),
    )(
      _.toString,
    ).withExample(KinesisIngest.BillingMode.PAY_PER_REQUEST)

  private val exampleLeaseManagementConfig: KinesisIngest.LeaseManagementConfig = KinesisIngest.LeaseManagementConfig(
    failoverTimeMillis = Some(2222),
    shardSyncIntervalMillis = Some(2222),
    cleanupLeasesUponShardCompletion = Some(true),
    ignoreUnexpectedChildShards = Some(true),
    maxLeasesForWorker = Some(1),
    maxLeaseRenewalThreads = Some(1),
    billingMode = Some(KinesisIngest.BillingMode.PROVISIONED),
    initialLeaseTableReadCapacity = Some(1),
    initialLeaseTableWriteCapacity = Some(1),
    reBalanceThresholdPercentage = Some(1),
    dampeningPercentage = Some(1),
    allowThroughputOvershoot = Some(true),
    disableWorkerMetrics = Some(true),
    maxThroughputPerHostKBps = Some(32.0),
    isGracefulLeaseHandoffEnabled = Some(true),
    gracefulLeaseHandoffTimeoutMillis = Some(2222),
  )
  implicit val exampleLeaseManagementConfigSchema: Record[KinesisIngest.LeaseManagementConfig] =
    genericRecord[KinesisIngest.LeaseManagementConfig].withExample(exampleLeaseManagementConfig)

  private val exampleConfigsBuilder: KinesisIngest.ConfigsBuilder = KinesisIngest.ConfigsBuilder(
    tableName = Some("my-table"),
    workerIdentifier = Some("worker-id-1"),
  )
  implicit val exampleConfigsBuilderSchema: Record[KinesisIngest.ConfigsBuilder] =
    genericRecord[KinesisIngest.ConfigsBuilder].withExample(exampleConfigsBuilder)

  private val exampleKinesisSchedulerSourceSettings: KinesisIngest.KinesisSchedulerSourceSettings =
    KinesisIngest.KinesisSchedulerSourceSettings(
      bufferSize = Some(1),
      backpressureTimeoutMillis = Some(2222),
    )
  implicit val kinesisSchedulerSourceSettingsSchema: Record[KinesisIngest.KinesisSchedulerSourceSettings] =
    genericRecord[KinesisIngest.KinesisSchedulerSourceSettings].withExample(exampleKinesisSchedulerSourceSettings)

  private val exampleKclConfiguration: KinesisIngest.KCLConfiguration = KinesisIngest.KCLConfiguration(
    configsBuilder = Some(exampleConfigsBuilder),
    leaseManagementConfig = Some(exampleLeaseManagementConfig),
    retrievalSpecificConfig = Some(examplePollingConfig),
    processorConfig = Some(exampleProcessorConfig),
    coordinatorConfig = Some(exampleCoordinatorConfig),
    lifecycleConfig = Some(exampleLifecycleConfig),
    retrievalConfig = Some(exampleRetrievalConfig),
    metricsConfig = Some(exampleMetricsConfig),
  )
  implicit val kclConfigurationSchema: Record[KinesisIngest.KCLConfiguration] =
    genericRecord[KinesisIngest.KCLConfiguration].withExample(exampleKclConfiguration)

  val exampleIngestStreamInfo: IngestStreamInfo = IngestStreamInfo(
    status = IngestStreamStatus.Running,
    message = None,
    settings = KafkaIngest(
      topics = Left(Set("e1-source")),
      bootstrapServers = "localhost:9092",
      groupId = Some("quine-e1-ingester"),
      offsetCommitting = None,
      endingOffset = None,
      maximumPerSecond = None,
    ),
    stats = IngestStreamStats.example,
  )
  val exampleIngestStreamInfoWithName: IngestStreamInfoWithName =
    exampleIngestStreamInfo.withName("log1-entity-ingest-source")

  // TODO review which of these are necessary
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
    "MATCH (x) WHERE id(x) = idFrom(toInteger($that)) SET x.i = toInteger($that)",
  )
  val defaultTextFileFormat: FileIngestFormat.CypherLine = FileIngestFormat.CypherLine(
    "MATCH (x) WHERE id(x) = idFrom($that) SET x.content = $that",
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

  /** The use of `Either[ClientErrors, Option[Unit]]` was chosen to correspond to different HTTP codes. The outer Either
    * uses the Left for 400 errors, and the Right for everything else. Within the Right, the Option is used to represent
    * 404 with None and Some[Unit] to represent a success (200).
    * When adding the Option to allow returning 404, these implementations were considered:
    * - (Chosen) Adding the Option to the endpoint definition, adding a response case of wheneverFound, and making the
    *   implementations of the route plumb through Either to represent that scenario.
    * - Skipping the Option, using a NamespaceNotFoundException to avoid threading the wrappers around and adding a
    *   top level handler to translate into different response codes. This could be less code, but makes it less
    *   obvious what's happening, would make us need to change all of the intermediate layer exception handling to not
    *   swallow ones we want to reach the top, and prevents it from showing up in anything using the endpoint (e.g.
    *   the UI). It is also awkward project-wise since the endpoint definitions and quine-core graph implementation
    *   would both need to reference the exception type, and they don't currently share a common ancestor project.
    * - Making an abstraction-violating codec that examines app state to look for the existence of a namespace, then
    *   using an exception handler like the previous strategy. This would make it automagically available anywhere we
    *   used the namespace type as a parameter, but would be confusing code to maintain, depend on app state loading
    *   order and also not show up in the endpoint definition.
    */
  val ingestStreamStart
    : Endpoint[(String, NamespaceParameter, IngestStreamConfiguration), Either[ClientErrors, Option[Unit]]] =
    endpoint(
      request = post(
        url = ingest / segment[String]("name", Some("Unique name for the ingest stream")) /? namespace,
        entity = jsonOrYamlRequest[IngestStreamConfiguration],
      ),
      response = customBadRequest("Ingest stream exists already")
        .orElse(wheneverFound(ok(emptyResponse))),
      docs = EndpointDocs()
        .withSummary(Some("Create Ingest Stream"))
        .withDescription(
          Some(
            """Create an [ingest stream](https://docs.quine.io/components/ingest-sources/ingest-sources.html)
              |that connects a streaming event source to Quine and loads data into the graph.
              |
              |An ingest stream is defined by selecting a source `type`, then an appropriate data `format`,
              |and must be created with a unique name. Many ingest stream types allow a Cypher query to operate
              |on the event stream data to create nodes and relationships in the graph.""".stripMargin,
          ),
        )
        .withTags(List(ingestStreamTag)),
    )

  val ingestStreamStop: Endpoint[(String, NamespaceParameter), Option[IngestStreamInfoWithName]] =
    endpoint(
      request = delete(
        url = ingest / ingestStreamName /? namespace,
      ),
      response = wheneverFound(ok(jsonResponse[IngestStreamInfoWithName])),
      docs = EndpointDocs()
        .withSummary(Some("Delete Ingest Stream"))
        .withDescription(
          Some(
            """Immediately halt and remove the named ingest stream from Quine.
              |
              |The ingest stream will complete any pending operations and return stream information
              |once the operation is complete.""".stripMargin,
          ),
        )
        .withTags(List(ingestStreamTag)),
    )

  // Inner Option is for representing namespace not found
  val ingestStreamLookup: Endpoint[(String, NamespaceParameter), Option[IngestStreamInfoWithName]] =
    endpoint(
      request = get(
        url = ingest / ingestStreamName /? namespace,
      ),
      response = wheneverFound(ok(jsonResponse[IngestStreamInfoWithName])),
      docs = EndpointDocs()
        .withSummary(Some("Ingest Stream Status"))
        .withDescription(
          Some("Return the ingest stream status information for a configured ingest stream by name."),
        )
        .withTags(List(ingestStreamTag)),
    )

  // Inner Option is for representing namespace not found
  val ingestStreamPause
    : Endpoint[(String, NamespaceParameter), Either[ClientErrors, Option[IngestStreamInfoWithName]]] =
    endpoint(
      request = put(
        url = ingest / ingestStreamName / "pause" /? namespace,
        entity = emptyRequest,
      ),
      response = customBadRequest("Cannot pause failed ingest").orElse(
        wheneverFound(ok(jsonResponse[IngestStreamInfoWithName])),
      ),
      docs = EndpointDocs()
        .withSummary(Some("Pause Ingest Stream"))
        .withDescription(Some("Temporarily pause processing new events by the named ingest stream."))
        .withTags(List(ingestStreamTag)),
    )

  // Inner Option is for representing namespace not found
  val ingestStreamUnpause
    : Endpoint[(String, NamespaceParameter), Either[ClientErrors, Option[IngestStreamInfoWithName]]] =
    endpoint(
      request = put(
        url = ingest / ingestStreamName / "start" /? namespace,
        entity = emptyRequest,
      ),
      response = customBadRequest("Cannot resume failed ingest").orElse(
        wheneverFound(ok(jsonResponse[IngestStreamInfoWithName])),
      ),
      docs = EndpointDocs()
        .withSummary(Some("Unpause Ingest Stream"))
        .withDescription(Some("Resume processing new events by the named ingest stream."))
        .withTags(List(ingestStreamTag)),
    )

  val ingestStreamList: Endpoint[NamespaceParameter, Map[String, IngestStreamInfo]] =
    endpoint(
      request = get(
        url = ingest /? namespace,
      ),
      response = ok(
        jsonResponseWithExample[Map[String, IngestStreamInfo]](
          Map(exampleIngestStreamInfoWithName.name -> exampleIngestStreamInfo),
        ),
      ),
      docs = EndpointDocs()
        .withSummary(Some("List Ingest Streams"))
        .withDescription(
          Some(
            """Return a JSON object containing the configured
              |[ingest streams](https://docs.quine.io/components/ingest-sources/ingest-sources.html)
              |and their associated stream metrics keyed by the stream name. """.stripMargin,
          ),
        )
        .withTags(List(ingestStreamTag)),
    )
}
