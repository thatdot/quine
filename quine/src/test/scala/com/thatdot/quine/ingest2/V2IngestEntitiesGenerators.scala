package com.thatdot.quine.ingest2

import org.scalacheck.{Arbitrary, Gen}

import com.thatdot.quine.ScalaPrimitiveGenerators
import com.thatdot.quine.app.model.ingest2.V2IngestEntities._

object V2IngestEntitiesGenerators {

  import ScalaPrimitiveGenerators.Gens._

  object Gens {

    val billingMode: Gen[BillingMode] = Gen.oneOf(
      BillingMode.PROVISIONED,
      BillingMode.PAY_PER_REQUEST,
      BillingMode.UNKNOWN_TO_SDK_VERSION,
    )

    val metricsLevel: Gen[MetricsLevel] = Gen.oneOf(
      MetricsLevel.NONE,
      MetricsLevel.SUMMARY,
      MetricsLevel.DETAILED,
    )

    val metricsDimension: Gen[MetricsDimension] = Gen.oneOf(
      MetricsDimension.OPERATION_DIMENSION_NAME,
      MetricsDimension.SHARD_ID_DIMENSION_NAME,
      MetricsDimension.STREAM_IDENTIFIER,
      MetricsDimension.WORKER_IDENTIFIER,
    )

    val clientVersionConfig: Gen[ClientVersionConfig] = Gen.oneOf(
      ClientVersionConfig.CLIENT_VERSION_CONFIG_COMPATIBLE_WITH_2X,
      ClientVersionConfig.CLIENT_VERSION_CONFIG_3X,
    )

    val shardPrioritization: Gen[ShardPrioritization] = Gen.oneOf(
      Gen.const(ShardPrioritization.NoOpShardPrioritization),
      smallPosNum.map(ShardPrioritization.ParentsFirstShardPrioritization(_)),
    )

    val fanOutConfig: Gen[RetrievalSpecificConfig.FanOutConfig] = for {
      consumerArn <- optNonEmptyAlphaNumStr
      consumerName <- optNonEmptyAlphaNumStr
      maxDescribeStreamSummaryRetries <- Gen.option(smallPosNum)
      maxDescribeStreamConsumerRetries <- Gen.option(smallPosNum)
      registerStreamConsumerRetries <- Gen.option(smallPosNum)
      retryBackoffMillis <- Gen.option(smallPosLong)
    } yield RetrievalSpecificConfig.FanOutConfig(
      consumerArn,
      consumerName,
      maxDescribeStreamSummaryRetries,
      maxDescribeStreamConsumerRetries,
      registerStreamConsumerRetries,
      retryBackoffMillis,
    )

    val pollingConfig: Gen[RetrievalSpecificConfig.PollingConfig] = for {
      maxRecords <- Gen.option(smallPosNum)
      retryGetRecordsInSeconds <- Gen.option(smallPosNum)
      maxGetRecordsThreadPool <- Gen.option(smallPosNum)
      idleTimeBetweenReadsInMillis <- Gen.option(smallPosLong)
    } yield RetrievalSpecificConfig.PollingConfig(
      maxRecords,
      retryGetRecordsInSeconds,
      maxGetRecordsThreadPool,
      idleTimeBetweenReadsInMillis,
    )

    val retrievalSpecificConfig: Gen[RetrievalSpecificConfig] = Gen.oneOf(fanOutConfig, pollingConfig)

    val kinesisCheckpointSettings: Gen[KinesisCheckpointSettings] = for {
      disableCheckpointing <- bool
      maxBatchSize <- Gen.option(smallPosNum)
      maxBatchWaitMillis <- Gen.option(smallPosLong)
    } yield KinesisCheckpointSettings(disableCheckpointing, maxBatchSize, maxBatchWaitMillis)

    val kinesisSchedulerSourceSettings: Gen[KinesisSchedulerSourceSettings] = for {
      bufferSize <- Gen.option(smallPosNum)
      backpressureTimeoutMillis <- Gen.option(smallPosLong)
    } yield KinesisSchedulerSourceSettings(bufferSize, backpressureTimeoutMillis)

    val configsBuilder: Gen[ConfigsBuilder] = for {
      tableName <- optNonEmptyAlphaNumStr
      workerIdentifier <- optNonEmptyAlphaNumStr
    } yield ConfigsBuilder(tableName, workerIdentifier)

    val lifecycleConfig: Gen[LifecycleConfig] = for {
      taskBackoffTimeMillis <- Gen.option(smallPosLong)
      logWarningForTaskAfterMillis <- Gen.option(smallPosLong)
    } yield LifecycleConfig(taskBackoffTimeMillis, logWarningForTaskAfterMillis)

    val retrievalConfig: Gen[RetrievalConfig] = for {
      listShardsBackoffTimeInMillis <- Gen.option(smallPosLong)
      maxListShardsRetryAttempts <- Gen.option(smallPosNum)
    } yield RetrievalConfig(listShardsBackoffTimeInMillis, maxListShardsRetryAttempts)

    val processorConfig: Gen[ProcessorConfig] = for {
      callProcessRecordsEvenForEmptyRecordList <- Gen.option(bool)
    } yield ProcessorConfig(callProcessRecordsEvenForEmptyRecordList)

    val leaseManagementConfig: Gen[LeaseManagementConfig] = for {
      failoverTimeMillis <- Gen.option(smallPosLong)
      shardSyncIntervalMillis <- Gen.option(smallPosLong)
      cleanupLeasesUponShardCompletion <- Gen.option(bool)
      ignoreUnexpectedChildShards <- Gen.option(bool)
      maxLeasesForWorker <- Gen.option(smallPosNum)
      maxLeaseRenewalThreads <- Gen.option(smallPosNum)
      bm <- Gen.option(billingMode)
      initialLeaseTableReadCapacity <- Gen.option(smallPosNum)
      initialLeaseTableWriteCapacity <- Gen.option(smallPosNum)
      reBalanceThresholdPercentage <- Gen.option(smallPosNum)
      dampeningPercentage <- Gen.option(smallPosNum)
      allowThroughputOvershoot <- Gen.option(bool)
      disableWorkerMetrics <- Gen.option(bool)
      maxThroughputPerHostKBps <- Gen.option(mediumPosDouble)
      isGracefulLeaseHandoffEnabled <- Gen.option(bool)
      gracefulLeaseHandoffTimeoutMillis <- Gen.option(smallPosLong)
    } yield LeaseManagementConfig(
      failoverTimeMillis,
      shardSyncIntervalMillis,
      cleanupLeasesUponShardCompletion,
      ignoreUnexpectedChildShards,
      maxLeasesForWorker,
      maxLeaseRenewalThreads,
      bm,
      initialLeaseTableReadCapacity,
      initialLeaseTableWriteCapacity,
      reBalanceThresholdPercentage,
      dampeningPercentage,
      allowThroughputOvershoot,
      disableWorkerMetrics,
      maxThroughputPerHostKBps,
      isGracefulLeaseHandoffEnabled,
      gracefulLeaseHandoffTimeoutMillis,
    )

    val coordinatorConfig: Gen[CoordinatorConfig] = for {
      parentShardPollIntervalMillis <- Gen.option(smallPosLong)
      skipShardSyncAtWorkerInitializationIfLeasesExist <- Gen.option(bool)
      sp <- Gen.option(shardPrioritization)
      cvc <- Gen.option(clientVersionConfig)
    } yield CoordinatorConfig(parentShardPollIntervalMillis, skipShardSyncAtWorkerInitializationIfLeasesExist, sp, cvc)

    val metricsConfig: Gen[MetricsConfig] = for {
      metricsBufferTimeMillis <- Gen.option(smallPosLong)
      metricsMaxQueueSize <- Gen.option(smallPosNum)
      ml <- Gen.option(metricsLevel)
      dimensions <- Gen.option(Gen.containerOf[Set, MetricsDimension](metricsDimension))
    } yield MetricsConfig(metricsBufferTimeMillis, metricsMaxQueueSize, ml, dimensions)

    val kclConfiguration: Gen[KCLConfiguration] = for {
      cb <- configsBuilder
      lmc <- leaseManagementConfig
      rsc <- Gen.option(retrievalSpecificConfig)
      pc <- processorConfig
      cc <- coordinatorConfig
      lc <- lifecycleConfig
      rc <- retrievalConfig
      mc <- metricsConfig
    } yield KCLConfiguration(cb, lmc, rsc, pc, cc, lc, rc, mc)
  }

  object Arbs {
    implicit val billingMode: Arbitrary[BillingMode] = Arbitrary(Gens.billingMode)
    implicit val metricsLevel: Arbitrary[MetricsLevel] = Arbitrary(Gens.metricsLevel)
    implicit val metricsDimension: Arbitrary[MetricsDimension] = Arbitrary(Gens.metricsDimension)
    implicit val clientVersionConfig: Arbitrary[ClientVersionConfig] = Arbitrary(Gens.clientVersionConfig)
    implicit val shardPrioritization: Arbitrary[ShardPrioritization] = Arbitrary(Gens.shardPrioritization)
    implicit val fanOutConfig: Arbitrary[RetrievalSpecificConfig.FanOutConfig] = Arbitrary(Gens.fanOutConfig)
    implicit val pollingConfig: Arbitrary[RetrievalSpecificConfig.PollingConfig] = Arbitrary(Gens.pollingConfig)
    implicit val retrievalSpecificConfig: Arbitrary[RetrievalSpecificConfig] = Arbitrary(Gens.retrievalSpecificConfig)
    implicit val kinesisCheckpointSettings: Arbitrary[KinesisCheckpointSettings] =
      Arbitrary(Gens.kinesisCheckpointSettings)
    implicit val kinesisSchedulerSourceSettings: Arbitrary[KinesisSchedulerSourceSettings] =
      Arbitrary(Gens.kinesisSchedulerSourceSettings)
    implicit val configsBuilder: Arbitrary[ConfigsBuilder] = Arbitrary(Gens.configsBuilder)
    implicit val lifecycleConfig: Arbitrary[LifecycleConfig] = Arbitrary(Gens.lifecycleConfig)
    implicit val retrievalConfig: Arbitrary[RetrievalConfig] = Arbitrary(Gens.retrievalConfig)
    implicit val processorConfig: Arbitrary[ProcessorConfig] = Arbitrary(Gens.processorConfig)
    implicit val leaseManagementConfig: Arbitrary[LeaseManagementConfig] = Arbitrary(Gens.leaseManagementConfig)
    implicit val coordinatorConfig: Arbitrary[CoordinatorConfig] = Arbitrary(Gens.coordinatorConfig)
    implicit val metricsConfig: Arbitrary[MetricsConfig] = Arbitrary(Gens.metricsConfig)
    implicit val kclConfiguration: Arbitrary[KCLConfiguration] = Arbitrary(Gens.kclConfiguration)
  }
}
