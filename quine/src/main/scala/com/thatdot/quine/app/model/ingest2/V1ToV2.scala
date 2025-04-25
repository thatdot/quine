package com.thatdot.quine.app.model.ingest2

import com.thatdot.quine.app.model.ingest2.{V2IngestEntities => V2}
import com.thatdot.quine.{routes => V1}

object V1ToV2 {

  def apply(
    schedulerSourceSettings: V1.KinesisIngest.KinesisSchedulerSourceSettings,
  ): V2.KinesisSchedulerSourceSettings =
    V2.KinesisSchedulerSourceSettings(
      bufferSize = schedulerSourceSettings.bufferSize,
      backpressureTimeoutMillis = schedulerSourceSettings.backpressureTimeoutMillis,
    )

  def apply(
    maybeSchedulerSourceSettings: Option[V1.KinesisIngest.KinesisSchedulerSourceSettings],
  ): V2.KinesisSchedulerSourceSettings = maybeSchedulerSourceSettings.fold(V2.KinesisSchedulerSourceSettings())(apply)

  def apply(checkpointSettings: V1.KinesisIngest.KinesisCheckpointSettings): V2.KinesisCheckpointSettings =
    V2.KinesisCheckpointSettings(
      disableCheckpointing = checkpointSettings.disableCheckpointing,
      maxBatchSize = checkpointSettings.maxBatchSize,
      maxBatchWaitMillis = checkpointSettings.maxBatchWaitMillis,
    )

  def apply(maybeCheckpointSettings: Option[V1.KinesisIngest.KinesisCheckpointSettings]): V2.KinesisCheckpointSettings =
    maybeCheckpointSettings.fold(V2.KinesisCheckpointSettings())(apply)

  def apply(configsBuilder: V1.KinesisIngest.ConfigsBuilder): V2.ConfigsBuilder = V2.ConfigsBuilder(
    tableName = configsBuilder.tableName,
    workerIdentifier = configsBuilder.workerIdentifier,
  )

  def apply(maybeConfigsBuilder: Option[V1.KinesisIngest.ConfigsBuilder]): V2.ConfigsBuilder =
    maybeConfigsBuilder.fold(V2.ConfigsBuilder())(apply)

  def apply(billingMode: V1.KinesisIngest.BillingMode): V2IngestEntities.BillingMode = billingMode match {
    case V1.KinesisIngest.BillingMode.PROVISIONED => V2.BillingMode.PROVISIONED
    case V1.KinesisIngest.BillingMode.PAY_PER_REQUEST => V2.BillingMode.PAY_PER_REQUEST
    case V1.KinesisIngest.BillingMode.UNKNOWN_TO_SDK_VERSION => V2.BillingMode.UNKNOWN_TO_SDK_VERSION
  }

  def apply(leaseManagementConfig: V1.KinesisIngest.LeaseManagementConfig): V2.LeaseManagementConfig =
    V2.LeaseManagementConfig(
      failoverTimeMillis = leaseManagementConfig.failoverTimeMillis,
      shardSyncIntervalMillis = leaseManagementConfig.shardSyncIntervalMillis,
      cleanupLeasesUponShardCompletion = leaseManagementConfig.cleanupLeasesUponShardCompletion,
      ignoreUnexpectedChildShards = leaseManagementConfig.ignoreUnexpectedChildShards,
      maxLeasesForWorker = leaseManagementConfig.maxLeasesForWorker,
      maxLeaseRenewalThreads = leaseManagementConfig.maxLeaseRenewalThreads,
      billingMode = leaseManagementConfig.billingMode.map(apply),
      initialLeaseTableReadCapacity = leaseManagementConfig.initialLeaseTableReadCapacity,
      initialLeaseTableWriteCapacity = leaseManagementConfig.initialLeaseTableWriteCapacity,
      reBalanceThresholdPercentage = leaseManagementConfig.reBalanceThresholdPercentage,
      dampeningPercentage = leaseManagementConfig.dampeningPercentage,
      allowThroughputOvershoot = leaseManagementConfig.allowThroughputOvershoot,
      disableWorkerMetrics = leaseManagementConfig.disableWorkerMetrics,
      maxThroughputPerHostKBps = leaseManagementConfig.maxThroughputPerHostKBps,
      isGracefulLeaseHandoffEnabled = leaseManagementConfig.isGracefulLeaseHandoffEnabled,
      gracefulLeaseHandoffTimeoutMillis = leaseManagementConfig.gracefulLeaseHandoffTimeoutMillis,
    )

  def apply(maybeLeaseManagementConfig: Option[V1.KinesisIngest.LeaseManagementConfig]): V2.LeaseManagementConfig =
    maybeLeaseManagementConfig.fold(V2.LeaseManagementConfig())(apply)

  def apply(
    retrievalSpecificConfig: V1.KinesisIngest.RetrievalSpecificConfig,
  ): V2.RetrievalSpecificConfig = retrievalSpecificConfig match {
    case fanOutConfig: V1.KinesisIngest.RetrievalSpecificConfig.FanOutConfig => apply(fanOutConfig)
    case pollingConfig: V1.KinesisIngest.RetrievalSpecificConfig.PollingConfig => apply(pollingConfig)
  }

  def apply(
    maybeRetrievalSpecificConfig: Option[V1.KinesisIngest.RetrievalSpecificConfig],
  ): Option[V2.RetrievalSpecificConfig] = maybeRetrievalSpecificConfig.map(apply)

  def apply(
    fanOutConfig: V1.KinesisIngest.RetrievalSpecificConfig.FanOutConfig,
  ): V2.RetrievalSpecificConfig.FanOutConfig = V2.RetrievalSpecificConfig.FanOutConfig(
    consumerArn = fanOutConfig.consumerArn,
    consumerName = fanOutConfig.consumerName,
    maxDescribeStreamSummaryRetries = fanOutConfig.maxDescribeStreamSummaryRetries,
    maxDescribeStreamConsumerRetries = fanOutConfig.maxDescribeStreamConsumerRetries,
    registerStreamConsumerRetries = fanOutConfig.registerStreamConsumerRetries,
    retryBackoffMillis = fanOutConfig.retryBackoffMillis,
  )

  def apply(
    pollingConfig: V1.KinesisIngest.RetrievalSpecificConfig.PollingConfig,
  ): V2.RetrievalSpecificConfig.PollingConfig = V2.RetrievalSpecificConfig.PollingConfig(
    maxRecords = pollingConfig.maxRecords,
    retryGetRecordsInSeconds = pollingConfig.retryGetRecordsInSeconds,
    maxGetRecordsThreadPool = pollingConfig.maxGetRecordsThreadPool,
    idleTimeBetweenReadsInMillis = pollingConfig.idleTimeBetweenReadsInMillis,
  )

  def apply(processorConfig: V1.KinesisIngest.ProcessorConfig): V2.ProcessorConfig = V2.ProcessorConfig(
    callProcessRecordsEvenForEmptyRecordList = processorConfig.callProcessRecordsEvenForEmptyRecordList,
  )

  def apply(maybeProcessorConfig: Option[V1.KinesisIngest.ProcessorConfig]): V2.ProcessorConfig =
    maybeProcessorConfig.fold(V2.ProcessorConfig())(apply)

  def apply(shardPrioritization: V1.KinesisIngest.ShardPrioritization): V2IngestEntities.ShardPrioritization =
    shardPrioritization match {
      case V1.KinesisIngest.ShardPrioritization.NoOpShardPrioritization =>
        V2.ShardPrioritization.NoOpShardPrioritization
      case V1.KinesisIngest.ShardPrioritization.ParentsFirstShardPrioritization(maxDepth) =>
        V2.ShardPrioritization.ParentsFirstShardPrioritization(maxDepth)
    }

  def apply(clientVersionConfig: V1.KinesisIngest.ClientVersionConfig): V2IngestEntities.ClientVersionConfig =
    clientVersionConfig match {
      case V1.KinesisIngest.ClientVersionConfig.CLIENT_VERSION_CONFIG_COMPATIBLE_WITH_2X =>
        V2.ClientVersionConfig.CLIENT_VERSION_CONFIG_COMPATIBLE_WITH_2X
      case V1.KinesisIngest.ClientVersionConfig.CLIENT_VERSION_CONFIG_3X =>
        V2.ClientVersionConfig.CLIENT_VERSION_CONFIG_3X
    }

  def apply(coordinatorConfig: V1.KinesisIngest.CoordinatorConfig): V2.CoordinatorConfig = V2.CoordinatorConfig(
    parentShardPollIntervalMillis = coordinatorConfig.parentShardPollIntervalMillis,
    skipShardSyncAtWorkerInitializationIfLeasesExist =
      coordinatorConfig.skipShardSyncAtWorkerInitializationIfLeasesExist,
    shardPrioritization = coordinatorConfig.shardPrioritization.map(apply),
    clientVersionConfig = coordinatorConfig.clientVersionConfig.map(apply),
  )

  def apply(maybeCoordinatorConfig: Option[V1.KinesisIngest.CoordinatorConfig]): V2.CoordinatorConfig =
    maybeCoordinatorConfig.fold(V2.CoordinatorConfig())(apply)

  def apply(lifecycleConfig: V1.KinesisIngest.LifecycleConfig): V2.LifecycleConfig = V2.LifecycleConfig(
    taskBackoffTimeMillis = lifecycleConfig.taskBackoffTimeMillis,
    logWarningForTaskAfterMillis = lifecycleConfig.logWarningForTaskAfterMillis,
  )

  def apply(maybeLifecycleConfig: Option[V1.KinesisIngest.LifecycleConfig]): V2.LifecycleConfig =
    maybeLifecycleConfig.fold(V2.LifecycleConfig())(apply)

  def apply(retrievalConfig: V1.KinesisIngest.RetrievalConfig): V2.RetrievalConfig = V2.RetrievalConfig(
    listShardsBackoffTimeInMillis = retrievalConfig.listShardsBackoffTimeInMillis,
    maxListShardsRetryAttempts = retrievalConfig.maxListShardsRetryAttempts,
  )

  def apply(maybeRetrievalConfig: Option[V1.KinesisIngest.RetrievalConfig]): V2.RetrievalConfig =
    maybeRetrievalConfig.fold(V2.RetrievalConfig())(apply)

  def apply(metricsLevel: V1.KinesisIngest.MetricsLevel): V2IngestEntities.MetricsLevel = metricsLevel match {
    case V1.KinesisIngest.MetricsLevel.NONE => V2.MetricsLevel.NONE
    case V1.KinesisIngest.MetricsLevel.SUMMARY => V2.MetricsLevel.SUMMARY
    case V1.KinesisIngest.MetricsLevel.DETAILED => V2.MetricsLevel.DETAILED
  }

  def apply(metricsDimension: V1.KinesisIngest.MetricsDimension): V2IngestEntities.MetricsDimension =
    metricsDimension match {
      case V1.KinesisIngest.MetricsDimension.OPERATION_DIMENSION_NAME =>
        V2.MetricsDimension.OPERATION_DIMENSION_NAME
      case V1.KinesisIngest.MetricsDimension.SHARD_ID_DIMENSION_NAME =>
        V2.MetricsDimension.SHARD_ID_DIMENSION_NAME
      case V1.KinesisIngest.MetricsDimension.STREAM_IDENTIFIER =>
        V2.MetricsDimension.STREAM_IDENTIFIER
      case V1.KinesisIngest.MetricsDimension.WORKER_IDENTIFIER =>
        V2.MetricsDimension.WORKER_IDENTIFIER
    }

  def apply(metricsConfig: V1.KinesisIngest.MetricsConfig): V2.MetricsConfig = V2.MetricsConfig(
    metricsBufferTimeMillis = metricsConfig.metricsBufferTimeMillis,
    metricsMaxQueueSize = metricsConfig.metricsMaxQueueSize,
    metricsLevel = metricsConfig.metricsLevel.map(apply),
    metricsEnabledDimensions = metricsConfig.metricsEnabledDimensions.map(_.map(apply)),
  )

  def apply(maybeMetricsConfig: Option[V1.KinesisIngest.MetricsConfig]): V2.MetricsConfig =
    maybeMetricsConfig.fold(V2.MetricsConfig())(apply)

  def apply(advancedSettings: V1.KinesisIngest.KCLConfiguration): V2.KCLConfiguration = V2.KCLConfiguration(
    configsBuilder = V1ToV2(advancedSettings.configsBuilder),
    leaseManagementConfig = V1ToV2(advancedSettings.leaseManagementConfig),
    retrievalSpecificConfig = V1ToV2(advancedSettings.retrievalSpecificConfig),
    processorConfig = V1ToV2(advancedSettings.processorConfig),
    coordinatorConfig = V1ToV2(advancedSettings.coordinatorConfig),
    lifecycleConfig = V1ToV2(advancedSettings.lifecycleConfig),
    retrievalConfig = V1ToV2(advancedSettings.retrievalConfig),
    metricsConfig = V1ToV2(advancedSettings.metricsConfig),
  )

  def apply(advancedSettings: Option[V1.KinesisIngest.KCLConfiguration]): V2.KCLConfiguration =
    advancedSettings.fold(V2.KCLConfiguration())(apply)

  def apply(initialPosition: V1.KinesisIngest.InitialPosition): V2.InitialPosition = initialPosition match {
    case V1.KinesisIngest.InitialPosition.TrimHorizon => V2.InitialPosition.TrimHorizon
    case V1.KinesisIngest.InitialPosition.Latest => V2.InitialPosition.Latest
    case V1.KinesisIngest.InitialPosition.AtTimestamp(year, month, day, hour, minute, second) =>
      V2.InitialPosition.AtTimestamp(year, month, day, hour, minute, second)
  }

}
