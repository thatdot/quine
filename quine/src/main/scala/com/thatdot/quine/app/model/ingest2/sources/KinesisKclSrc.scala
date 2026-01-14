package com.thatdot.quine.app.model.ingest2.sources

import java.net.InetAddress
import java.nio.ByteBuffer
import java.util.{Calendar, Optional, UUID}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.jdk.DurationConverters.ScalaDurationOps

import org.apache.pekko.stream.connectors.kinesis.scaladsl.KinesisSchedulerSource
import org.apache.pekko.stream.connectors.kinesis.{
  CommittableRecord,
  KinesisSchedulerCheckpointSettings,
  KinesisSchedulerSourceSettings => PekkoKinesisSchedulerSourceSettings,
}
import org.apache.pekko.stream.scaladsl.{Flow, Source}
import org.apache.pekko.{Done, NotUsed}

import cats.data.Validated.Valid
import cats.data.ValidatedNel
import com.typesafe.scalalogging.LazyLogging
import software.amazon.awssdk.awscore.retry.AwsRetryStrategy
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration
import software.amazon.awssdk.http.async.SdkAsyncHttpClient
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient
import software.amazon.awssdk.retries.StandardRetryStrategy
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.{BillingMode => AwsBillingMode}
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.awssdk.services.kinesis.model.EncryptionType
import software.amazon.kinesis.common.{ConfigsBuilder, InitialPositionInStream, InitialPositionInStreamExtended}
import software.amazon.kinesis.coordinator.CoordinatorConfig.{ClientVersionConfig => AwsClientVersionConfig}
import software.amazon.kinesis.coordinator.Scheduler
import software.amazon.kinesis.leases.{NoOpShardPrioritization, ParentsFirstShardPrioritization}
import software.amazon.kinesis.metrics.{MetricsLevel => AwsMetricsLevel}
import software.amazon.kinesis.processor.{ShardRecordProcessorFactory, SingleStreamTracker}
import software.amazon.kinesis.retrieval.fanout.FanOutConfig
import software.amazon.kinesis.retrieval.polling.PollingConfig

import com.thatdot.data.{DataFoldableFrom, DataFolderTo}
import com.thatdot.quine.app.model.ingest.serialization.ContentDecoder
import com.thatdot.quine.app.model.ingest.util.AwsOps
import com.thatdot.quine.app.model.ingest.util.AwsOps.AwsBuilderOps
import com.thatdot.quine.app.model.ingest2._
import com.thatdot.quine.app.model.ingest2.source.FramedSource
import com.thatdot.quine.app.routes.IngestMeter
import com.thatdot.quine.util.BaseError
import com.thatdot.quine.{routes => V1}

/** The definition of a source stream from Amazon Kinesis using KCL,
  * now translated to expose a framedSource.
  *
  * @param kinesisStreamName  The name of the kinesis stream to start ingesting from
  * @param applicationName    The name of the dynamo db table and cloud watch metrics, unless overridden
  * @param meter              An instance of [[IngestMeter]] for metering the ingest flow
  * @param credentialsOpt     The AWS credentials to access the stream (optional)
  * @param regionOpt          The AWS region in which Kinesis resides (optional)
  * @param initialPosition    The KCL initial position in stream describing where to begin reading records
  * @param numRetries         The maximum number of retry attempts for AWS client calls
  * @param decoders           A sequence of [[ContentDecoder]] for handling inbound Kinesis records
  * @param schedulerSettings  Pekko Connectors scheduler settings
  * @param checkpointSettings Pekko Connectors checkpointing configuration
  * @param advancedSettings   All additional configuration settings for KCL
  */
final case class KinesisKclSrc(
  kinesisStreamName: String,
  applicationName: String,
  meter: IngestMeter,
  credentialsOpt: Option[V1.AwsCredentials],
  regionOpt: Option[V1.AwsRegion],
  initialPosition: InitialPosition,
  numRetries: Int,
  decoders: Seq[ContentDecoder],
  schedulerSettings: KinesisSchedulerSourceSettings,
  checkpointSettings: KinesisCheckpointSettings,
  advancedSettings: KCLConfiguration,
)(implicit val ec: ExecutionContext)
    extends FramedSourceProvider
    with LazyLogging {

  import KinesisKclSrc._

  /** Builds and returns a `FramedSource`, wrapped in a `ValidatedNel` for error handling.
    * This method instantiates Kinesis, DynamoDB, and CloudWatch async clients,
    * configures a KCL scheduler, and returns a framed Akka Stream source that
    * emits byte representation of [[CommittableRecord]] instances.
    *
    * @return A [[ValidatedNel]] of [[BaseError]] or a [[FramedSource]].
    */
  override def framedSource: ValidatedNel[BaseError, FramedSource] = {
    val httpClient = buildAsyncHttpClient
    val kinesisClient = buildAsyncClient(httpClient, credentialsOpt, regionOpt, numRetries)
    val dynamoClient: DynamoDbAsyncClient = DynamoDbAsyncClient.builder
      .credentials(credentialsOpt)
      .httpClient(httpClient)
      .region(regionOpt)
      .build
    val cloudWatchClient: CloudWatchAsyncClient = CloudWatchAsyncClient.builder
      .credentials(credentialsOpt)
      .httpClient(httpClient)
      .region(regionOpt)
      .build

    val schedulerSourceSettings: PekkoKinesisSchedulerSourceSettings = {
      val base = PekkoKinesisSchedulerSourceSettings.defaults
      val withSize = schedulerSettings.bufferSize.fold(base)(base.withBufferSize)
      val withSizeAndTimeout = schedulerSettings.backpressureTimeoutMillis.fold(withSize) { t =>
        withSize.withBackpressureTimeout(java.time.Duration.ofMillis(t))
      }
      withSizeAndTimeout
    }

    val builder: ShardRecordProcessorFactory => Scheduler = { recordProcessorFactory =>

      val initialPositionInStream: InitialPositionInStreamExtended = initialPosition match {
        case InitialPosition.Latest =>
          InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.LATEST)
        case InitialPosition.TrimHorizon =>
          InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.TRIM_HORIZON)
        case InitialPosition.AtTimestamp(year, month, date, hourOfDay, minute, second) =>
          val time = Calendar.getInstance()
          // Minus one because Calendar Month is 0 indexed
          time.set(year, month - 1, date, hourOfDay, minute, second)
          InitialPositionInStreamExtended.newInitialPositionAtTimestamp(time.getTime)
      }

      val streamTracker = new SingleStreamTracker(kinesisStreamName, initialPositionInStream)
      val workerId = advancedSettings.configsBuilder.workerIdentifier
        .getOrElse(s"${InetAddress.getLocalHost.getHostName}:${UUID.randomUUID()}")
      val configsBuilder = new ConfigsBuilder(
        streamTracker,
        applicationName,
        kinesisClient,
        dynamoClient,
        cloudWatchClient,
        workerId,
        recordProcessorFactory,
      )

      advancedSettings.configsBuilder.tableName.foreach(configsBuilder.tableName)

      val leaseManagementConfig = configsBuilder.leaseManagementConfig
        // This should be covered by `streamTracker`, but this is to be safe since we're
        // not providing an override in the abbreviated `LeaseManagementConfig` API schema
        .initialPositionInStream(initialPositionInStream)
      val processorConfig = configsBuilder.processorConfig
      val coordinatorConfig = configsBuilder.coordinatorConfig
      val lifecycleConfig = configsBuilder.lifecycleConfig
      val retrievalConfig = configsBuilder.retrievalConfig
      val metricsConfig = configsBuilder.metricsConfig

      advancedSettings.leaseManagementConfig.failoverTimeMillis.foreach(leaseManagementConfig.failoverTimeMillis)
      advancedSettings.leaseManagementConfig.shardSyncIntervalMillis.foreach(
        leaseManagementConfig.shardSyncIntervalMillis,
      )
      advancedSettings.leaseManagementConfig.cleanupLeasesUponShardCompletion.foreach(
        leaseManagementConfig.cleanupLeasesUponShardCompletion,
      )
      advancedSettings.leaseManagementConfig.ignoreUnexpectedChildShards.foreach(
        leaseManagementConfig.ignoreUnexpectedChildShards,
      )
      advancedSettings.leaseManagementConfig.maxLeasesForWorker.foreach(leaseManagementConfig.maxLeasesForWorker)
      advancedSettings.leaseManagementConfig.maxLeaseRenewalThreads.foreach(value =>
        leaseManagementConfig.maxLeaseRenewalThreads(value),
      )
      advancedSettings.leaseManagementConfig.billingMode.foreach {
        case BillingMode.PROVISIONED =>
          leaseManagementConfig.billingMode(AwsBillingMode.PROVISIONED)
        case BillingMode.PAY_PER_REQUEST =>
          leaseManagementConfig.billingMode(AwsBillingMode.PAY_PER_REQUEST)
        case BillingMode.UNKNOWN_TO_SDK_VERSION =>
          leaseManagementConfig.billingMode(AwsBillingMode.UNKNOWN_TO_SDK_VERSION)
      }
      advancedSettings.leaseManagementConfig.initialLeaseTableReadCapacity.foreach(
        leaseManagementConfig.initialLeaseTableReadCapacity,
      )
      advancedSettings.leaseManagementConfig.initialLeaseTableWriteCapacity.foreach(
        leaseManagementConfig.initialLeaseTableWriteCapacity,
      )
      // Begin setting workerUtilizationAwareAssignmentConfig
      val workerUtilizationAwareAssignmentConfig = leaseManagementConfig.workerUtilizationAwareAssignmentConfig()
      advancedSettings.leaseManagementConfig.reBalanceThresholdPercentage.foreach(
        workerUtilizationAwareAssignmentConfig.reBalanceThresholdPercentage,
      )
      advancedSettings.leaseManagementConfig.dampeningPercentage.foreach(
        workerUtilizationAwareAssignmentConfig.dampeningPercentage,
      )
      advancedSettings.leaseManagementConfig.allowThroughputOvershoot.foreach(
        workerUtilizationAwareAssignmentConfig.allowThroughputOvershoot,
      )
      advancedSettings.leaseManagementConfig.disableWorkerMetrics.foreach(
        workerUtilizationAwareAssignmentConfig.disableWorkerMetrics,
      )
      advancedSettings.leaseManagementConfig.maxThroughputPerHostKBps.foreach(
        workerUtilizationAwareAssignmentConfig.maxThroughputPerHostKBps,
      )
      // Finalize setting workerUtilizationAwareAssignmentConfig by updating its value in the leaseManagementConfig
      leaseManagementConfig.workerUtilizationAwareAssignmentConfig(workerUtilizationAwareAssignmentConfig)

      val gracefulLeaseHandoffConfig = leaseManagementConfig.gracefulLeaseHandoffConfig()
      advancedSettings.leaseManagementConfig.isGracefulLeaseHandoffEnabled.foreach(
        gracefulLeaseHandoffConfig.isGracefulLeaseHandoffEnabled,
      )
      advancedSettings.leaseManagementConfig.gracefulLeaseHandoffTimeoutMillis.foreach(
        gracefulLeaseHandoffConfig.gracefulLeaseHandoffTimeoutMillis,
      )
      leaseManagementConfig.gracefulLeaseHandoffConfig(gracefulLeaseHandoffConfig)

      advancedSettings.retrievalSpecificConfig
        .map {
          case RetrievalSpecificConfig.FanOutConfig(
                consumerArn,
                consumerName,
                maxDescribeStreamSummaryRetries,
                maxDescribeStreamConsumerRetries,
                registerStreamConsumerRetries,
                retryBackoffMillis,
              ) =>
            val fanOutConfig = new FanOutConfig(kinesisClient)
            fanOutConfig.streamName(kinesisStreamName)
            consumerArn.foreach(fanOutConfig.consumerArn)
            consumerName.foreach(fanOutConfig.consumerName)
            maxDescribeStreamSummaryRetries.foreach(fanOutConfig.maxDescribeStreamSummaryRetries)
            maxDescribeStreamConsumerRetries.foreach(fanOutConfig.maxDescribeStreamConsumerRetries)
            registerStreamConsumerRetries.foreach(fanOutConfig.registerStreamConsumerRetries)
            retryBackoffMillis.foreach(fanOutConfig.retryBackoffMillis)
            fanOutConfig

          case RetrievalSpecificConfig.PollingConfig(
                maxRecords,
                retryGetRecordsInSeconds,
                maxGetRecordsThreadPool,
                idleTimeBetweenReadsInMillis,
              ) =>
            val pollingConfig = new PollingConfig(kinesisStreamName, kinesisClient)
            maxRecords.foreach(pollingConfig.maxRecords)
            // It's tempting to always set the config value for Optional types, using RichOption or some such,
            // but we really only want to set something other than the library default if one is provided via the API
            maxGetRecordsThreadPool.foreach(value => pollingConfig.maxGetRecordsThreadPool(Optional.of(value)))
            retryGetRecordsInSeconds.foreach(value => pollingConfig.retryGetRecordsInSeconds(Optional.of(value)))
            idleTimeBetweenReadsInMillis.foreach(pollingConfig.idleTimeBetweenReadsInMillis)
            pollingConfig
        }
        .foreach(retrievalConfig.retrievalSpecificConfig)

      advancedSettings.processorConfig.callProcessRecordsEvenForEmptyRecordList.foreach(
        processorConfig.callProcessRecordsEvenForEmptyRecordList,
      )

      advancedSettings.coordinatorConfig.parentShardPollIntervalMillis.foreach(
        coordinatorConfig.parentShardPollIntervalMillis,
      )
      advancedSettings.coordinatorConfig.skipShardSyncAtWorkerInitializationIfLeasesExist.foreach(
        coordinatorConfig.skipShardSyncAtWorkerInitializationIfLeasesExist,
      )
      advancedSettings.coordinatorConfig.shardPrioritization.foreach {
        case ShardPrioritization.ParentsFirstShardPrioritization(maxDepth) =>
          coordinatorConfig.shardPrioritization(new ParentsFirstShardPrioritization(maxDepth))
        case ShardPrioritization.NoOpShardPrioritization =>
          coordinatorConfig.shardPrioritization(new NoOpShardPrioritization())
      }
      advancedSettings.coordinatorConfig.clientVersionConfig.foreach {
        case ClientVersionConfig.CLIENT_VERSION_CONFIG_COMPATIBLE_WITH_2X =>
          coordinatorConfig.clientVersionConfig(AwsClientVersionConfig.CLIENT_VERSION_CONFIG_COMPATIBLE_WITH_2X)
        case ClientVersionConfig.CLIENT_VERSION_CONFIG_3X =>
          coordinatorConfig.clientVersionConfig(AwsClientVersionConfig.CLIENT_VERSION_CONFIG_3X)
      }

      advancedSettings.lifecycleConfig.taskBackoffTimeMillis.foreach(lifecycleConfig.taskBackoffTimeMillis)

      // It's tempting to always set the config value for Optional types, using RichOption or some such,
      // but we really only want to set something other than the library default if one is provided via the API
      advancedSettings.lifecycleConfig.logWarningForTaskAfterMillis.foreach(value =>
        lifecycleConfig.logWarningForTaskAfterMillis(Optional.of(value)),
      )

      advancedSettings.retrievalConfig.listShardsBackoffTimeInMillis.foreach(
        retrievalConfig.listShardsBackoffTimeInMillis,
      )
      advancedSettings.retrievalConfig.maxListShardsRetryAttempts.foreach(retrievalConfig.maxListShardsRetryAttempts)

      advancedSettings.metricsConfig.metricsBufferTimeMillis.foreach(metricsConfig.metricsBufferTimeMillis)
      advancedSettings.metricsConfig.metricsMaxQueueSize.foreach(metricsConfig.metricsMaxQueueSize)
      advancedSettings.metricsConfig.metricsLevel.foreach {
        case MetricsLevel.NONE => metricsConfig.metricsLevel(AwsMetricsLevel.NONE)
        case MetricsLevel.SUMMARY => metricsConfig.metricsLevel(AwsMetricsLevel.SUMMARY)
        case MetricsLevel.DETAILED => metricsConfig.metricsLevel(AwsMetricsLevel.DETAILED)
      }
      advancedSettings.metricsConfig.metricsEnabledDimensions.foreach(values =>
        metricsConfig.metricsEnabledDimensions(new java.util.HashSet(values.map(_.value).asJava)),
      )

      // Note: Currently, this config is the only one built within the configs builder
      // that is not affected by the `advancedSettings` traversal above. That makes
      // sense because we also have `checkpointSettings` at the same level, but the
      // reasons that we don't build a `checkpointConfig` from that parameter are:
      //   1. Those settings are used for `KinesisSchedulerCheckpointSettings` in the
      //      `ack` flow, and that purpose is distinct from this checkpoint config's
      //      purpose, so we probably don't want to re-use those values for discrete
      //      things.
      //   2. At a glance, the only way to build a checkpoint config other than the
      //      parameterless default one built within the configs builder at this
      //      accessor is to build a `DynamoDBCheckpointer` via its factory, and that
      //      is no small task.
      val checkpointConfig = configsBuilder.checkpointConfig

      new Scheduler(
        checkpointConfig,
        coordinatorConfig,
        leaseManagementConfig,
        lifecycleConfig,
        metricsConfig,
        processorConfig,
        retrievalConfig,
      )
    }

    val source: Source[CommittableRecord, NotUsed] =
      KinesisSchedulerSource(builder, schedulerSourceSettings)
        .mapMaterializedValue(_ => NotUsed)
        .via(metered[CommittableRecord](meter, r => recordBufferToArray(r.record.data()).length))

    val framed = FramedSource[CommittableRecord](
      withKillSwitches(source),
      meter,
      record => ContentDecoder.decode(decoders, recordBufferToArray(record.record.data())),
      committableRecordFolder,
      terminationHook = () => {
        Seq(kinesisClient, dynamoClient, cloudWatchClient).foreach { client =>
          client.close()
        }
      },
      // Performs Checkpointing logic, defined below
      ackFlow = ack,
    )
    Valid(framed)
  }

  val ack: Flow[CommittableRecord, Done, NotUsed] = {
    if (checkpointSettings.disableCheckpointing) {
      Flow.fromFunction[CommittableRecord, Done](_ => Done)
    } else {
      val settings: KinesisSchedulerCheckpointSettings = {
        val base = KinesisSchedulerCheckpointSettings.defaults
        val withBatchSize = checkpointSettings.maxBatchSize.fold(base)(base.withMaxBatchSize)
        val withBatchAndWait = checkpointSettings.maxBatchWaitMillis.fold(withBatchSize) { wait =>
          withBatchSize.withMaxBatchWait(wait.millis.toJava)
        }
        withBatchAndWait
      }
      KinesisSchedulerSource
        .checkpointRecordsFlow(settings)
        .map(_ => Done)
    }
  }
}

object KinesisKclSrc {

  /** Converts the supplied [[ByteBuffer]] to an `Array[Byte]`.
    * A new byte array is allocated and populated by reading from a duplication of the buffer.
    *
    * @param data The [[ByteBuffer]] to convert
    * @return A corresponding array of bytes
    */
  private def recordBufferToArray(data: ByteBuffer): Array[Byte] = {
    // Duplicate in case something else was using the position information
    val duplicateBuffer = data.duplicate()
    val bytes = new Array[Byte](duplicateBuffer.remaining())
    duplicateBuffer.get(bytes)
    bytes
  }

  def buildAsyncHttpClient: SdkAsyncHttpClient =
    NettyNioAsyncHttpClient.builder.maxConcurrency(AwsOps.httpConcurrencyPerClient).build()

  def buildAsyncClient(
    httpClient: SdkAsyncHttpClient,
    credentialsOpt: Option[V1.AwsCredentials],
    regionOpt: Option[V1.AwsRegion],
    numRetries: Int,
  ): KinesisAsyncClient = {
    val retryStrategy: StandardRetryStrategy = AwsRetryStrategy
      .standardRetryStrategy()
      .toBuilder
      .maxAttempts(numRetries)
      .build()
    KinesisAsyncClient
      .builder()
      .credentials(credentialsOpt)
      .region(regionOpt)
      .httpClient(httpClient)
      .overrideConfiguration(
        ClientOverrideConfiguration
          .builder()
          .retryStrategy(retryStrategy)
          .build(),
      )
      .build
  }

  protected val committableRecordFolder: DataFoldableFrom[CommittableRecord] = new DataFoldableFrom[CommittableRecord] {
    def fold[B](value: CommittableRecord, folder: DataFolderTo[B]): B = {
      val builder = folder.mapBuilder()
      builder.add("data", folder.bytes(recordBufferToArray(value.record.data())))
      builder.add("sequenceNumber", folder.string(value.record.sequenceNumber()))
      builder.add("approximateArrivalTimestamp", folder.string(value.record.approximateArrivalTimestamp().toString))
      builder.add("partitionKey", folder.string(value.record.partitionKey()))
      builder.add(
        "encryptionType",
        value.record.encryptionType() match {
          case EncryptionType.NONE => folder.string(EncryptionType.NONE.toString)
          case EncryptionType.KMS => folder.string(EncryptionType.KMS.toString)
          case EncryptionType.UNKNOWN_TO_SDK_VERSION => folder.nullValue
        },
      )
      builder.add("subSequenceNumber", folder.integer(value.record.subSequenceNumber()))
      builder.add("explicitHashKey", folder.string(value.record.explicitHashKey()))
      builder.add(
        "aggregated",
        value.record.aggregated() match {
          case true => folder.trueValue
          case false => folder.falseValue
        },
      )

      val schemaBuilder = folder.mapBuilder()
      schemaBuilder.add("schemaName", folder.string(value.record.schema().getSchemaName))
      schemaBuilder.add("schemaDefinition", folder.string(value.record.schema().getSchemaDefinition))
      schemaBuilder.add("dataFormat", folder.string(value.record.schema().getDataFormat))

      builder.add("schema", schemaBuilder.finish())

      builder.finish()
    }
  }

}
