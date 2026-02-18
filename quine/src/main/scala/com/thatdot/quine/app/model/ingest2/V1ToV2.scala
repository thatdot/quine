package com.thatdot.quine.app.model.ingest2

import com.thatdot.api.{v2 => api}
import com.thatdot.quine.app.model.ingest2.{V2IngestEntities => V2}
import com.thatdot.quine.{routes => V1}

/** Converts V1 API types to V2 API types. */
object V1ToV2 {

  def apply(config: V1.SaslJaasConfig): api.SaslJaasConfig = config match {
    case V1.SaslJaasConfig.PlainLogin(username, password) =>
      api.PlainLogin(username, password)
    case V1.SaslJaasConfig.ScramLogin(username, password) =>
      api.ScramLogin(username, password)
    case V1.SaslJaasConfig.OAuthBearerLogin(clientId, clientSecret, scope, tokenEndpointUrl) =>
      api.OAuthBearerLogin(clientId, clientSecret, scope, tokenEndpointUrl)
  }

  def apply(
    schedulerSourceSettings: V1.KinesisIngest.KinesisSchedulerSourceSettings,
  ): KinesisSchedulerSourceSettings =
    KinesisSchedulerSourceSettings(
      bufferSize = schedulerSourceSettings.bufferSize,
      backpressureTimeoutMillis = schedulerSourceSettings.backpressureTimeoutMillis,
    )

  def apply(
    maybeSchedulerSourceSettings: Option[V1.KinesisIngest.KinesisSchedulerSourceSettings],
  ): KinesisSchedulerSourceSettings = maybeSchedulerSourceSettings.fold(KinesisSchedulerSourceSettings())(apply)

  def apply(checkpointSettings: V1.KinesisIngest.KinesisCheckpointSettings): KinesisCheckpointSettings =
    KinesisCheckpointSettings(
      disableCheckpointing = checkpointSettings.disableCheckpointing,
      maxBatchSize = checkpointSettings.maxBatchSize,
      maxBatchWaitMillis = checkpointSettings.maxBatchWaitMillis,
    )

  def apply(maybeCheckpointSettings: Option[V1.KinesisIngest.KinesisCheckpointSettings]): KinesisCheckpointSettings =
    maybeCheckpointSettings.fold(KinesisCheckpointSettings())(apply)

  def apply(configsBuilder: V1.KinesisIngest.ConfigsBuilder): ConfigsBuilder = ConfigsBuilder(
    tableName = configsBuilder.tableName,
    workerIdentifier = configsBuilder.workerIdentifier,
  )

  def apply(maybeConfigsBuilder: Option[V1.KinesisIngest.ConfigsBuilder]): ConfigsBuilder =
    maybeConfigsBuilder.fold(ConfigsBuilder())(apply)

  def apply(billingMode: V1.KinesisIngest.BillingMode): BillingMode = billingMode match {
    case V1.KinesisIngest.BillingMode.PROVISIONED => BillingMode.PROVISIONED
    case V1.KinesisIngest.BillingMode.PAY_PER_REQUEST => BillingMode.PAY_PER_REQUEST
    case V1.KinesisIngest.BillingMode.UNKNOWN_TO_SDK_VERSION => BillingMode.UNKNOWN_TO_SDK_VERSION
  }

  def apply(leaseManagementConfig: V1.KinesisIngest.LeaseManagementConfig): LeaseManagementConfig =
    LeaseManagementConfig(
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

  def apply(maybeLeaseManagementConfig: Option[V1.KinesisIngest.LeaseManagementConfig]): LeaseManagementConfig =
    maybeLeaseManagementConfig.fold(LeaseManagementConfig())(apply)

  def apply(
    retrievalSpecificConfig: V1.KinesisIngest.RetrievalSpecificConfig,
  ): RetrievalSpecificConfig = retrievalSpecificConfig match {
    case fanOutConfig: V1.KinesisIngest.RetrievalSpecificConfig.FanOutConfig => apply(fanOutConfig)
    case pollingConfig: V1.KinesisIngest.RetrievalSpecificConfig.PollingConfig => apply(pollingConfig)
  }

  def apply(
    maybeRetrievalSpecificConfig: Option[V1.KinesisIngest.RetrievalSpecificConfig],
  ): Option[RetrievalSpecificConfig] = maybeRetrievalSpecificConfig.map(apply)

  def apply(
    fanOutConfig: V1.KinesisIngest.RetrievalSpecificConfig.FanOutConfig,
  ): RetrievalSpecificConfig.FanOutConfig = RetrievalSpecificConfig.FanOutConfig(
    consumerArn = fanOutConfig.consumerArn,
    consumerName = fanOutConfig.consumerName,
    maxDescribeStreamSummaryRetries = fanOutConfig.maxDescribeStreamSummaryRetries,
    maxDescribeStreamConsumerRetries = fanOutConfig.maxDescribeStreamConsumerRetries,
    registerStreamConsumerRetries = fanOutConfig.registerStreamConsumerRetries,
    retryBackoffMillis = fanOutConfig.retryBackoffMillis,
  )

  def apply(
    pollingConfig: V1.KinesisIngest.RetrievalSpecificConfig.PollingConfig,
  ): RetrievalSpecificConfig.PollingConfig = RetrievalSpecificConfig.PollingConfig(
    maxRecords = pollingConfig.maxRecords,
    retryGetRecordsInSeconds = pollingConfig.retryGetRecordsInSeconds,
    maxGetRecordsThreadPool = pollingConfig.maxGetRecordsThreadPool,
    idleTimeBetweenReadsInMillis = pollingConfig.idleTimeBetweenReadsInMillis,
  )

  def apply(processorConfig: V1.KinesisIngest.ProcessorConfig): ProcessorConfig = ProcessorConfig(
    callProcessRecordsEvenForEmptyRecordList = processorConfig.callProcessRecordsEvenForEmptyRecordList,
  )

  def apply(maybeProcessorConfig: Option[V1.KinesisIngest.ProcessorConfig]): ProcessorConfig =
    maybeProcessorConfig.fold(ProcessorConfig())(apply)

  def apply(shardPrioritization: V1.KinesisIngest.ShardPrioritization): ShardPrioritization =
    shardPrioritization match {
      case V1.KinesisIngest.ShardPrioritization.NoOpShardPrioritization =>
        ShardPrioritization.NoOpShardPrioritization
      case V1.KinesisIngest.ShardPrioritization.ParentsFirstShardPrioritization(maxDepth) =>
        ShardPrioritization.ParentsFirstShardPrioritization(maxDepth)
    }

  def apply(clientVersionConfig: V1.KinesisIngest.ClientVersionConfig): ClientVersionConfig =
    clientVersionConfig match {
      case V1.KinesisIngest.ClientVersionConfig.CLIENT_VERSION_CONFIG_COMPATIBLE_WITH_2X =>
        ClientVersionConfig.CLIENT_VERSION_CONFIG_COMPATIBLE_WITH_2X
      case V1.KinesisIngest.ClientVersionConfig.CLIENT_VERSION_CONFIG_3X =>
        ClientVersionConfig.CLIENT_VERSION_CONFIG_3X
    }

  def apply(coordinatorConfig: V1.KinesisIngest.CoordinatorConfig): CoordinatorConfig = CoordinatorConfig(
    parentShardPollIntervalMillis = coordinatorConfig.parentShardPollIntervalMillis,
    skipShardSyncAtWorkerInitializationIfLeasesExist =
      coordinatorConfig.skipShardSyncAtWorkerInitializationIfLeasesExist,
    shardPrioritization = coordinatorConfig.shardPrioritization.map(apply),
    clientVersionConfig = coordinatorConfig.clientVersionConfig.map(apply),
  )

  def apply(maybeCoordinatorConfig: Option[V1.KinesisIngest.CoordinatorConfig]): CoordinatorConfig =
    maybeCoordinatorConfig.fold(CoordinatorConfig())(apply)

  def apply(lifecycleConfig: V1.KinesisIngest.LifecycleConfig): LifecycleConfig = LifecycleConfig(
    taskBackoffTimeMillis = lifecycleConfig.taskBackoffTimeMillis,
    logWarningForTaskAfterMillis = lifecycleConfig.logWarningForTaskAfterMillis,
  )

  def apply(maybeLifecycleConfig: Option[V1.KinesisIngest.LifecycleConfig]): LifecycleConfig =
    maybeLifecycleConfig.fold(LifecycleConfig())(apply)

  def apply(retrievalConfig: V1.KinesisIngest.RetrievalConfig): RetrievalConfig = RetrievalConfig(
    listShardsBackoffTimeInMillis = retrievalConfig.listShardsBackoffTimeInMillis,
    maxListShardsRetryAttempts = retrievalConfig.maxListShardsRetryAttempts,
  )

  def apply(maybeRetrievalConfig: Option[V1.KinesisIngest.RetrievalConfig]): RetrievalConfig =
    maybeRetrievalConfig.fold(RetrievalConfig())(apply)

  def apply(metricsLevel: V1.KinesisIngest.MetricsLevel): MetricsLevel = metricsLevel match {
    case V1.KinesisIngest.MetricsLevel.NONE => MetricsLevel.NONE
    case V1.KinesisIngest.MetricsLevel.SUMMARY => MetricsLevel.SUMMARY
    case V1.KinesisIngest.MetricsLevel.DETAILED => MetricsLevel.DETAILED
  }

  def apply(metricsDimension: V1.KinesisIngest.MetricsDimension): MetricsDimension =
    metricsDimension match {
      case V1.KinesisIngest.MetricsDimension.OPERATION_DIMENSION_NAME =>
        MetricsDimension.OPERATION_DIMENSION_NAME
      case V1.KinesisIngest.MetricsDimension.SHARD_ID_DIMENSION_NAME =>
        MetricsDimension.SHARD_ID_DIMENSION_NAME
      case V1.KinesisIngest.MetricsDimension.STREAM_IDENTIFIER =>
        MetricsDimension.STREAM_IDENTIFIER
      case V1.KinesisIngest.MetricsDimension.WORKER_IDENTIFIER =>
        MetricsDimension.WORKER_IDENTIFIER
    }

  def apply(metricsConfig: V1.KinesisIngest.MetricsConfig): MetricsConfig = MetricsConfig(
    metricsBufferTimeMillis = metricsConfig.metricsBufferTimeMillis,
    metricsMaxQueueSize = metricsConfig.metricsMaxQueueSize,
    metricsLevel = metricsConfig.metricsLevel.map(apply),
    metricsEnabledDimensions = metricsConfig.metricsEnabledDimensions.map(_.map(apply)),
  )

  def apply(maybeMetricsConfig: Option[V1.KinesisIngest.MetricsConfig]): MetricsConfig =
    maybeMetricsConfig.fold(MetricsConfig())(apply)

  def apply(advancedSettings: V1.KinesisIngest.KCLConfiguration): KCLConfiguration = KCLConfiguration(
    configsBuilder = V1ToV2(advancedSettings.configsBuilder),
    leaseManagementConfig = V1ToV2(advancedSettings.leaseManagementConfig),
    retrievalSpecificConfig = V1ToV2(advancedSettings.retrievalSpecificConfig),
    processorConfig = V1ToV2(advancedSettings.processorConfig),
    coordinatorConfig = V1ToV2(advancedSettings.coordinatorConfig),
    lifecycleConfig = V1ToV2(advancedSettings.lifecycleConfig),
    retrievalConfig = V1ToV2(advancedSettings.retrievalConfig),
    metricsConfig = V1ToV2(advancedSettings.metricsConfig),
  )

  def apply(advancedSettings: Option[V1.KinesisIngest.KCLConfiguration]): KCLConfiguration =
    advancedSettings.fold(KCLConfiguration())(apply)

  def apply(initialPosition: V1.KinesisIngest.InitialPosition): InitialPosition = initialPosition match {
    case V1.KinesisIngest.InitialPosition.TrimHorizon => InitialPosition.TrimHorizon
    case V1.KinesisIngest.InitialPosition.Latest => InitialPosition.Latest
    case V1.KinesisIngest.InitialPosition.AtTimestamp(year, month, day, hour, minute, second) =>
      InitialPosition.AtTimestamp(year, month, day, hour, minute, second)
  }

  def apply(stats: V1.IngestStreamStats): V2.IngestStreamStats = V2.IngestStreamStats(
    ingestedCount = stats.ingestedCount,
    rates = apply(stats.rates),
    byteRates = apply(stats.byteRates),
    startTime = stats.startTime,
    totalRuntime = stats.totalRuntime,
  )

  def apply(summary: V1.RatesSummary): V2.RatesSummary = V2.RatesSummary(
    count = summary.count,
    oneMinute = summary.oneMinute,
    fiveMinute = summary.fiveMinute,
    fifteenMinute = summary.fifteenMinute,
    overall = summary.overall,
  )

  def apply(status: V1.IngestStreamStatus): V2.IngestStreamStatus = status match {
    case V1.IngestStreamStatus.Running => V2.IngestStreamStatus.Running
    case V1.IngestStreamStatus.Paused => V2.IngestStreamStatus.Paused
    case V1.IngestStreamStatus.Restored => V2.IngestStreamStatus.Restored
    case V1.IngestStreamStatus.Completed => V2.IngestStreamStatus.Completed
    case V1.IngestStreamStatus.Terminated => V2.IngestStreamStatus.Terminated
    case V1.IngestStreamStatus.Failed => V2.IngestStreamStatus.Failed
  }
}
