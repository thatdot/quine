package com.thatdot.quine.app.ingest

import java.net.InetAddress
import java.nio.ByteBuffer
import java.util.{Calendar, Optional, UUID}

import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.util.Try

import org.apache.pekko.stream.connectors.kinesis.scaladsl.KinesisSchedulerSource
import org.apache.pekko.stream.connectors.kinesis.{
  CommittableRecord,
  KinesisSchedulerCheckpointSettings,
  KinesisSchedulerSourceSettings,
}
import org.apache.pekko.stream.scaladsl.{Flow, Source}
import org.apache.pekko.{Done, NotUsed}

import software.amazon.awssdk.awscore.retry.AwsRetryStrategy
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration
import software.amazon.awssdk.http.async.SdkAsyncHttpClient
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient
import software.amazon.awssdk.retries.StandardRetryStrategy
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.BillingMode
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.kinesis.common.{ConfigsBuilder, InitialPositionInStream, InitialPositionInStreamExtended}
import software.amazon.kinesis.coordinator.CoordinatorConfig.ClientVersionConfig
import software.amazon.kinesis.coordinator.Scheduler
import software.amazon.kinesis.leases.{NoOpShardPrioritization, ParentsFirstShardPrioritization}
import software.amazon.kinesis.metrics.MetricsLevel
import software.amazon.kinesis.processor.{ShardRecordProcessorFactory, SingleStreamTracker}
import software.amazon.kinesis.retrieval.polling.PollingConfig

import com.thatdot.common.logging.Log.LogConfig
import com.thatdot.quine.app.ingest.serialization.{ContentDecoder, ImportFormat}
import com.thatdot.quine.app.ingest.util.AwsOps
import com.thatdot.quine.app.ingest.util.AwsOps.AwsBuilderOps
import com.thatdot.quine.graph.MasterStream.IngestSrcExecToken
import com.thatdot.quine.graph.cypher.Value
import com.thatdot.quine.graph.{CypherOpsGraph, NamespaceId}
import com.thatdot.quine.routes.{AwsCredentials, AwsRegion, KinesisIngest}
import com.thatdot.quine.util.SwitchMode

/** The definition of a source stream from Amazon Kinesis using the Kinesis Client Library (KCL).
  *
  * @param name              The unique, human-facing name of the ingest stream
  * @param intoNamespace     The namespace (database) into which the data is ingested
  * @param applicationName   The name of the application as seen by KCL and its accompanying DynamoDB instance
  * @param streamName        The Kinesis stream name
  * @param format            The [[ImportFormat]] describing how to parse bytes read from Kinesis
  * @param initialSwitchMode The initial mode that controls whether ingestion is active or paused
  * @param parallelism       How many concurrent writes should be performed on the database
  * @param credentialsOpt    The AWS credentials to access the stream (if None, default credentials are used)
  * @param regionOpt         The AWS region in which the stream resides (if None, default region is used)
  * @param initialPosition   The initial position from which KCL will consume from a Kinesis stream (e.g., LATEST, TRIM_HORIZON)
  * @param numRetries        How many times to retry on ingest failures
  * @param maxPerSecond      Optional rate limit (records per second). If None, no explicit rate limit is applied
  * @param decoders          A sequence of [[ContentDecoder]] instances for transforming the ingested data
  * @param checkpointSettings Settings controlling how checkpoints are managed for this stream
  */
final case class KinesisKclSrcDef(
  override val name: String,
  override val intoNamespace: NamespaceId,
  applicationName: String,
  streamName: String,
  format: ImportFormat,
  initialSwitchMode: SwitchMode,
  parallelism: Int = 2,
  credentialsOpt: Option[AwsCredentials],
  regionOpt: Option[AwsRegion],
  initialPosition: KinesisIngest.InitialPosition,
  numRetries: Int,
  maxPerSecond: Option[Int],
  decoders: Seq[ContentDecoder],
  schedulerSettings: Option[KinesisIngest.KCLSchedulerSourceSettings],
  checkpointSettings: Option[KinesisIngest.KinesisCheckpointSettings],
  advancedKCLConfig: Option[KinesisIngest.KCLConfiguration],
)(implicit val graph: CypherOpsGraph, protected val logConfig: LogConfig)
    extends RawValuesIngestSrcDef(
      format,
      initialSwitchMode,
      parallelism,
      maxPerSecond,
      decoders,
      s"$name (Kinesis ingest)",
      intoNamespace,
    ) {
  import KinesisKclSrcDef._

  type InputType = CommittableRecord

  override val ingestToken: IngestSrcExecToken = IngestSrcExecToken(format.label)

  def rawBytes(record: CommittableRecord): Array[Byte] = recordBufferToArray(record.record.data())

  def source(): Source[CommittableRecord, NotUsed] = {
    val httpClient = buildAsyncHttpClient
    val kinesisClient = buildAsyncClient(buildAsyncHttpClient, credentialsOpt, regionOpt, numRetries)
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

    Seq(kinesisClient, dynamoClient, cloudWatchClient).foreach { client =>
      graph.system.registerOnTermination(client.close())
    }

    val schedulerSourceSettings: KinesisSchedulerSourceSettings = schedulerSettings
      .map { apiKinesisSchedulerSourceSettings =>
        val base = KinesisSchedulerSourceSettings.defaults
        val withSize = apiKinesisSchedulerSourceSettings.bufferSize.fold(base)(base.withBufferSize)
        val withSizeAndTimeout = apiKinesisSchedulerSourceSettings.backpressureTimeoutMillis.fold(withSize) { t =>
          withSize.withBackpressureTimeout(java.time.Duration.ofMillis(t))
        }
        withSizeAndTimeout
      }
      .getOrElse(KinesisSchedulerSourceSettings.defaults)

    val builder: ShardRecordProcessorFactory => Scheduler = recordProcessorFactory => {

      // Configuration settings point to set the initial stream position used below in the Scheduler
      val initialPositionInStream: InitialPositionInStreamExtended = initialPosition match {
        case KinesisIngest.InitialPosition.Latest =>
          InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.LATEST)
        case KinesisIngest.InitialPosition.TrimHorizon =>
          InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.TRIM_HORIZON)
        case KinesisIngest.InitialPosition.AtTimestamp(year, month, dayOfMonth, hour, minute, second) =>
          val cal = Calendar.getInstance()
          cal.set(year, month - 1, dayOfMonth, hour, minute, second)
          InitialPositionInStreamExtended.newInitialPositionAtTimestamp(cal.getTime)
        case _ =>
          throw new IllegalArgumentException(
            s"Only Latest, TrimHorizon, and AtTimestamp are valid Iterator Types when using the KCL version of Kinesis",
          ) // will be caught as an "Invalid" (400) below
      }

      val streamTracker = new SingleStreamTracker(streamName, initialPositionInStream)
      val workerId = advancedKCLConfig
        .flatMap(_.configsBuilder.flatMap(_.workerIdentifier))
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

      // `ConfigsBuilder#tableName` may only be set after construction, but we
      // need to do it before the rest of the `advancedKCLConfig` traversal
      advancedKCLConfig.foreach(_.configsBuilder.foreach(_.tableName.foreach(configsBuilder.tableName)))

      // Used below to set the retrievalSpecificConfig that PollingConfig implements
      val pollingConfig = new PollingConfig(streamName, kinesisClient)

      val leaseManagementConfig = configsBuilder.leaseManagementConfig
        // This should be covered by `streamTracker`, but this is to be safe since we're
        // not providing an override in the abbreviated `LeaseManagementConfig` API schema
        .initialPositionInStream(initialPositionInStream)
      val processorConfig = configsBuilder.processorConfig
      val coordinatorConfig = configsBuilder.coordinatorConfig
      val lifecycleConfig = configsBuilder.lifecycleConfig
      val retrievalConfig = configsBuilder.retrievalConfig
      val metricsConfig = configsBuilder.metricsConfig

      advancedKCLConfig.foreach { apiKclConfig =>
        apiKclConfig.leaseManagementConfig.foreach { apiLeaseConfig =>
          apiLeaseConfig.failoverTimeMillis.foreach(leaseManagementConfig.failoverTimeMillis)
          apiLeaseConfig.shardSyncIntervalMillis.foreach(leaseManagementConfig.shardSyncIntervalMillis)
          apiLeaseConfig.cleanupLeasesUponShardCompletion.foreach(
            leaseManagementConfig.cleanupLeasesUponShardCompletion,
          )
          apiLeaseConfig.ignoreUnexpectedChildShards.foreach(leaseManagementConfig.ignoreUnexpectedChildShards)
          apiLeaseConfig.maxLeasesForWorker.foreach(leaseManagementConfig.maxLeasesForWorker)
          apiLeaseConfig.maxLeaseRenewalThreads.foreach(value => leaseManagementConfig.maxLeaseRenewalThreads(value))
          apiLeaseConfig.billingMode.foreach {
            case KinesisIngest.BillingMode.PROVISIONED =>
              leaseManagementConfig.billingMode(BillingMode.PROVISIONED)
            case KinesisIngest.BillingMode.PAY_PER_REQUEST =>
              leaseManagementConfig.billingMode(BillingMode.PAY_PER_REQUEST)
            case KinesisIngest.BillingMode.UNKNOWN_TO_SDK_VERSION =>
              leaseManagementConfig.billingMode(BillingMode.UNKNOWN_TO_SDK_VERSION)
          }
          apiLeaseConfig.initialLeaseTableReadCapacity.foreach(leaseManagementConfig.initialLeaseTableReadCapacity)
          apiLeaseConfig.initialLeaseTableWriteCapacity.foreach(leaseManagementConfig.initialLeaseTableWriteCapacity)
          // Begin setting workerUtilizationAwareAssignmentConfig
          val workerUtilizationAwareAssignmentConfig = leaseManagementConfig.workerUtilizationAwareAssignmentConfig()
          apiLeaseConfig.reBalanceThresholdPercentage.foreach(
            workerUtilizationAwareAssignmentConfig.reBalanceThresholdPercentage,
          )
          apiLeaseConfig.dampeningPercentage.foreach(workerUtilizationAwareAssignmentConfig.dampeningPercentage)
          apiLeaseConfig.allowThroughputOvershoot.foreach(
            workerUtilizationAwareAssignmentConfig.allowThroughputOvershoot,
          )
          apiLeaseConfig.disableWorkerMetrics.foreach(workerUtilizationAwareAssignmentConfig.disableWorkerMetrics)
          apiLeaseConfig.maxThroughputPerHostKBps.foreach(
            workerUtilizationAwareAssignmentConfig.maxThroughputPerHostKBps,
          )
          // Finalize setting workerUtilizationAwareAssignmentConfig by updating its value in the leaseManagementConfig
          leaseManagementConfig.workerUtilizationAwareAssignmentConfig(workerUtilizationAwareAssignmentConfig)

          val gracefulLeaseHandoffConfig = leaseManagementConfig.gracefulLeaseHandoffConfig()
          apiLeaseConfig.isGracefulLeaseHandoffEnabled.foreach(
            gracefulLeaseHandoffConfig.isGracefulLeaseHandoffEnabled,
          )
          apiLeaseConfig.gracefulLeaseHandoffTimeoutMillis.foreach(
            gracefulLeaseHandoffConfig.gracefulLeaseHandoffTimeoutMillis,
          )
          leaseManagementConfig.gracefulLeaseHandoffConfig(gracefulLeaseHandoffConfig)
        }

        apiKclConfig.pollingConfig.foreach { apiPollingConfig =>
          apiPollingConfig.maxRecords.foreach(pollingConfig.maxRecords)
          // It's tempting to always set the config value for Optional types, using RichOption or some such,
          // but we really only want to set something other than the library default if one is provided via the API
          apiPollingConfig.maxGetRecordsThreadPool.foreach(value =>
            pollingConfig.maxGetRecordsThreadPool(Optional.of(value)),
          )
          apiPollingConfig.retryGetRecordsInSeconds.foreach(value =>
            pollingConfig.retryGetRecordsInSeconds(Optional.of(value)),
          )
          apiPollingConfig.idleTimeBetweenReadsInMillis.foreach(pollingConfig.idleTimeBetweenReadsInMillis)
        }
        // Setting polling config defined above with stream name and kinesisClient
        retrievalConfig.retrievalSpecificConfig(pollingConfig)

        apiKclConfig.processorConfig.foreach { apiProcessorConfig =>
          apiProcessorConfig.callProcessRecordsEvenForEmptyRecordList.foreach(
            processorConfig.callProcessRecordsEvenForEmptyRecordList,
          )
        }

        apiKclConfig.coordinatorConfig.foreach { apiCoordinatorConfig =>
          apiCoordinatorConfig.parentShardPollIntervalMillis.foreach(coordinatorConfig.parentShardPollIntervalMillis)
          apiCoordinatorConfig.skipShardSyncAtWorkerInitializationIfLeasesExist.foreach(
            coordinatorConfig.skipShardSyncAtWorkerInitializationIfLeasesExist,
          )
          apiCoordinatorConfig.shardPrioritization.foreach {
            case KinesisIngest.ShardPrioritization.ParentsFirstShardPrioritization(maxDepth) =>
              coordinatorConfig.shardPrioritization(new ParentsFirstShardPrioritization(maxDepth))
            case KinesisIngest.ShardPrioritization.NoOpShardPrioritization =>
              coordinatorConfig.shardPrioritization(new NoOpShardPrioritization())
          }
          apiCoordinatorConfig.clientVersionConfig.foreach {
            case KinesisIngest.ClientVersionConfig.CLIENT_VERSION_CONFIG_COMPATIBLE_WITH_2X =>
              coordinatorConfig.clientVersionConfig(ClientVersionConfig.CLIENT_VERSION_CONFIG_COMPATIBLE_WITH_2X)
            case KinesisIngest.ClientVersionConfig.CLIENT_VERSION_CONFIG_3X =>
              coordinatorConfig.clientVersionConfig(ClientVersionConfig.CLIENT_VERSION_CONFIG_3X)
          }
        }

        apiKclConfig.lifecycleConfig.foreach { apiLifecycleConfig =>
          apiLifecycleConfig.taskBackoffTimeMillis.foreach(lifecycleConfig.taskBackoffTimeMillis)
          // It's tempting to always set the config value for Optional types, using RichOption or some such,
          // but we really only want to set something other than the library default if one is provided via the API
          apiLifecycleConfig.logWarningForTaskAfterMillis.foreach(value =>
            lifecycleConfig.logWarningForTaskAfterMillis(Optional.of(value)),
          )
        }

        apiKclConfig.retrievalConfig.foreach { apiRetrievalConfig =>
          apiRetrievalConfig.listShardsBackoffTimeInMillis.foreach(retrievalConfig.listShardsBackoffTimeInMillis)
          apiRetrievalConfig.maxListShardsRetryAttempts.foreach(retrievalConfig.maxListShardsRetryAttempts)
        }

        apiKclConfig.metricsConfig.foreach { apiMetricsConfig =>
          apiMetricsConfig.metricsBufferTimeMillis.foreach(metricsConfig.metricsBufferTimeMillis)
          apiMetricsConfig.metricsMaxQueueSize.foreach(metricsConfig.metricsMaxQueueSize)
          apiMetricsConfig.metricsLevel.foreach {
            case KinesisIngest.MetricsLevel.NONE => metricsConfig.metricsLevel(MetricsLevel.NONE)
            case KinesisIngest.MetricsLevel.SUMMARY => metricsConfig.metricsLevel(MetricsLevel.SUMMARY)
            case KinesisIngest.MetricsLevel.DETAILED => metricsConfig.metricsLevel(MetricsLevel.DETAILED)
          }
          apiMetricsConfig.metricsEnabledDimensions.foreach(values =>
            metricsConfig.metricsEnabledDimensions(new java.util.HashSet(values.map(_.value).asJava)),
          )
        }
      }

      // Note: Currently, this config is the only one built within the configs builder
      // that is not affected by the `advancedKCLConfig` traversal above. That makes
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

    val source = KinesisSchedulerSource(builder, schedulerSourceSettings)
    source.mapMaterializedValue(_ => NotUsed)
  }

  override val ack: Flow[(Try[Value], CommittableRecord), Done, NotUsed] = {
    val defaultSettings: KinesisSchedulerCheckpointSettings = KinesisSchedulerCheckpointSettings.defaults
    checkpointSettings
      .map {
        case apiSettings if !apiSettings.disableCheckpointing =>
          KinesisSchedulerCheckpointSettings
            .apply(
              apiSettings.maxBatchSize.getOrElse(defaultSettings.maxBatchSize),
              apiSettings.maxBatchWaitMillis.map(Duration(_, MILLISECONDS)).getOrElse(defaultSettings.maxBatchWait),
            )
        case _ =>
          defaultSettings
      }
      .map(
        KinesisSchedulerSource
          .checkpointRecordsFlow(_)
          .contramap[(Try[Value], CommittableRecord)]({ case (_, cr) => cr })
          .map(_ => Done),
      )
      .getOrElse(Flow[(Try[Value], CommittableRecord)].map(_ => Done))
  }
}

object KinesisKclSrcDef {

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
    credentialsOpt: Option[AwsCredentials],
    regionOpt: Option[AwsRegion],
    numRetries: Int,
  ): KinesisAsyncClient = {
    val retryStrategy: StandardRetryStrategy = AwsRetryStrategy
      .standardRetryStrategy()
      .toBuilder
      .maxAttempts(numRetries)
      .build()
    val builder = KinesisAsyncClient
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
    builder.build
  }
}
