package com.thatdot.quine.app.v2api.converters

import com.thatdot.quine.app.model.ingest2._
import com.thatdot.quine.app.model.ingest2.{V2IngestEntities => Ingest}
import com.thatdot.quine.app.v2api.definitions.ingest2.{ApiIngest => Api}
import com.thatdot.quine.{routes => V1}
import com.thatdot.{convert => ConvertCore}

object ApiToIngest {

  //For conversions from API methods that we may not be able to define in quine OSS, but we also
  // don't want to bake into a non-sealed base type
  trait OfApiMethod[A, B] {
    def apply(b: B): A
  }

  object OssConversions {

    implicit val quineIngestConfigurationOfApi
      : OfApiMethod[Ingest.QuineIngestConfiguration, Api.Oss.QuineIngestConfiguration] =
      (b: Api.Oss.QuineIngestConfiguration) => apply(b)
  }

  def apply(stats: Api.IngestStreamStats): V1.IngestStreamStats =
    V1.IngestStreamStats(
      stats.ingestedCount,
      ConvertCore.Api2ToModel1.apply(stats.rates),
      ConvertCore.Api2ToModel1.apply(stats.byteRates),
      stats.startTime,
      stats.totalRuntime,
    )

  def apply(c: Api.CsvCharacter): V1.CsvCharacter = c match {
    case Api.CsvCharacter.Backslash => V1.CsvCharacter.Backslash
    case Api.CsvCharacter.Comma => V1.CsvCharacter.Comma
    case Api.CsvCharacter.Semicolon => V1.CsvCharacter.Semicolon
    case Api.CsvCharacter.Colon => V1.CsvCharacter.Colon
    case Api.CsvCharacter.Tab => V1.CsvCharacter.Tab
    case Api.CsvCharacter.Pipe => V1.CsvCharacter.Pipe
    case Api.CsvCharacter.DoubleQuote => V1.CsvCharacter.DoubleQuote
  }

  def apply(format: Api.IngestFormat.FileFormat): FileFormat = format match {
    case Api.IngestFormat.FileFormat.Line => FileFormat.LineFormat
    case Api.IngestFormat.FileFormat.JsonL => FileFormat.JsonLinesFormat
    case Api.IngestFormat.FileFormat.Json => FileFormat.JsonFormat
    case Api.IngestFormat.FileFormat.CSV(headers, delimiter, quoteChar, escapeChar) =>
      FileFormat.CsvFormat(headers, apply(delimiter), apply(quoteChar), apply(escapeChar))
  }
  def apply(format: Api.IngestFormat.StreamingFormat): StreamingFormat = format match {
    case Api.IngestFormat.StreamingFormat.Json => StreamingFormat.JsonFormat
    case Api.IngestFormat.StreamingFormat.Raw => StreamingFormat.RawFormat
    case Api.IngestFormat.StreamingFormat.Protobuf(schemaUrl, typeName) =>
      StreamingFormat.ProtobufFormat(schemaUrl, typeName)
    case Api.IngestFormat.StreamingFormat.Avro(schemaUrl) => StreamingFormat.AvroFormat(schemaUrl)
    case Api.IngestFormat.StreamingFormat.Drop => StreamingFormat.DropFormat
  }
  def apply(mode: Api.FileIngestMode): V1.FileIngestMode = mode match {
    case Api.FileIngestMode.Regular => V1.FileIngestMode.Regular
    case Api.FileIngestMode.NamedPipe => V1.FileIngestMode.NamedPipe
  }
  def apply(mode: Api.RecordDecodingType): V1.RecordDecodingType = mode match {
    case Api.RecordDecodingType.Zlib => V1.RecordDecodingType.Zlib
    case Api.RecordDecodingType.Gzip => V1.RecordDecodingType.Gzip
    case Api.RecordDecodingType.Base64 => V1.RecordDecodingType.Base64
  }
  def apply(
    ingest: Api.WebSocketClient.KeepaliveProtocol,
  ): V1.WebsocketSimpleStartupIngest.KeepaliveProtocol = ingest match {
    case Api.WebSocketClient.PingPongInterval(intervalMillis) =>
      V1.WebsocketSimpleStartupIngest.PingPongInterval(intervalMillis)
    case Api.WebSocketClient.SendMessageInterval(message, intervalMillis) =>
      V1.WebsocketSimpleStartupIngest.SendMessageInterval(message, intervalMillis)
    case Api.WebSocketClient.NoKeepalive => V1.WebsocketSimpleStartupIngest.NoKeepalive
  }
  def apply(ingest: Api.IngestSource.Kinesis.IteratorType): V1.KinesisIngest.IteratorType = ingest match {
    case Api.IngestSource.Kinesis.IteratorType.Latest => V1.KinesisIngest.IteratorType.Latest
    case Api.IngestSource.Kinesis.IteratorType.TrimHorizon => V1.KinesisIngest.IteratorType.TrimHorizon
    case Api.IngestSource.Kinesis.IteratorType.AtSequenceNumber(sequenceNumber) =>
      V1.KinesisIngest.IteratorType.AtSequenceNumber(sequenceNumber)
    case Api.IngestSource.Kinesis.IteratorType.AfterSequenceNumber(sequenceNumber) =>
      V1.KinesisIngest.IteratorType.AtSequenceNumber(sequenceNumber)
    case Api.IngestSource.Kinesis.IteratorType.AtTimestamp(millisSinceEpoch) =>
      V1.KinesisIngest.IteratorType.AtTimestamp(millisSinceEpoch)
  }
  def apply(proto: Api.KafkaSecurityProtocol): V1.KafkaSecurityProtocol = proto match {
    case Api.KafkaSecurityProtocol.PlainText => V1.KafkaSecurityProtocol.PlainText
    case Api.KafkaSecurityProtocol.Ssl => V1.KafkaSecurityProtocol.Ssl
    case Api.KafkaSecurityProtocol.Sasl_Ssl => V1.KafkaSecurityProtocol.Sasl_Ssl
    case Api.KafkaSecurityProtocol.Sasl_Plaintext => V1.KafkaSecurityProtocol.Sasl_Plaintext
  }

  def apply(reset: Api.KafkaAutoOffsetReset): V1.KafkaAutoOffsetReset = reset match {
    case Api.KafkaAutoOffsetReset.Latest => V1.KafkaAutoOffsetReset.Latest
    case Api.KafkaAutoOffsetReset.Earliest => V1.KafkaAutoOffsetReset.Earliest
    case Api.KafkaAutoOffsetReset.None => V1.KafkaAutoOffsetReset.None
  }
  def apply(offset: Api.KafkaOffsetCommitting): V1.KafkaOffsetCommitting = offset match {
    case offset: Api.KafkaOffsetCommitting.ExplicitCommit =>
      V1.KafkaOffsetCommitting.ExplicitCommit(
        offset.maxBatch,
        offset.maxIntervalMillis,
        offset.parallelism,
        offset.waitForCommitConfirmation,
      )
  }

  def apply(bm: Api.BillingMode): BillingMode = bm match {
    case Api.BillingMode.PROVISIONED => BillingMode.PROVISIONED
    case Api.BillingMode.PAY_PER_REQUEST => BillingMode.PAY_PER_REQUEST
    case Api.BillingMode.UNKNOWN_TO_SDK_VERSION => BillingMode.UNKNOWN_TO_SDK_VERSION
  }

  def apply(ip: Api.InitialPosition): InitialPosition = ip match {
    case Api.InitialPosition.Latest => InitialPosition.Latest
    case Api.InitialPosition.TrimHorizon => InitialPosition.TrimHorizon
    case Api.InitialPosition.AtTimestamp(y, m, d, h, mm, s) =>
      InitialPosition.AtTimestamp(y, m, d, h, mm, s)
  }

  def apply(sp: Api.ShardPrioritization): ShardPrioritization = sp match {
    case Api.ShardPrioritization.NoOpShardPrioritization => ShardPrioritization.NoOpShardPrioritization
    case Api.ShardPrioritization.ParentsFirstShardPrioritization(d) =>
      ShardPrioritization.ParentsFirstShardPrioritization(d)
  }

  def apply(cvc: Api.ClientVersionConfig): ClientVersionConfig = cvc match {
    case Api.ClientVersionConfig.CLIENT_VERSION_CONFIG_COMPATIBLE_WITH_2X =>
      ClientVersionConfig.CLIENT_VERSION_CONFIG_COMPATIBLE_WITH_2X
    case Api.ClientVersionConfig.CLIENT_VERSION_CONFIG_3X => ClientVersionConfig.CLIENT_VERSION_CONFIG_3X
  }

  def apply(ml: Api.MetricsLevel): MetricsLevel = ml match {
    case Api.MetricsLevel.NONE => MetricsLevel.NONE
    case Api.MetricsLevel.SUMMARY => MetricsLevel.SUMMARY
    case Api.MetricsLevel.DETAILED => MetricsLevel.DETAILED
  }

  def apply(md: Api.MetricsDimension): MetricsDimension = md match {
    case Api.MetricsDimension.OPERATION_DIMENSION_NAME => MetricsDimension.OPERATION_DIMENSION_NAME
    case Api.MetricsDimension.SHARD_ID_DIMENSION_NAME => MetricsDimension.SHARD_ID_DIMENSION_NAME
    case Api.MetricsDimension.STREAM_IDENTIFIER => MetricsDimension.STREAM_IDENTIFIER
    case Api.MetricsDimension.WORKER_IDENTIFIER => MetricsDimension.STREAM_IDENTIFIER // best fallback
  }

  def apply(kcs: Api.KinesisCheckpointSettings): KinesisCheckpointSettings =
    KinesisCheckpointSettings(kcs.disableCheckpointing, kcs.maxBatchSize, kcs.maxBatchWaitMillis)

  def apply(ksss: Api.KinesisSchedulerSourceSettings): KinesisSchedulerSourceSettings =
    KinesisSchedulerSourceSettings(ksss.bufferSize, ksss.backpressureTimeoutMillis)

  def apply(lmc: Api.LeaseManagementConfig): LeaseManagementConfig =
    LeaseManagementConfig(
      lmc.failoverTimeMillis,
      lmc.shardSyncIntervalMillis,
      lmc.cleanupLeasesUponShardCompletion,
      lmc.ignoreUnexpectedChildShards,
      lmc.maxLeasesForWorker,
      lmc.maxLeaseRenewalThreads,
      lmc.billingMode.map(apply),
      lmc.initialLeaseTableReadCapacity,
      lmc.initialLeaseTableWriteCapacity,
      lmc.reBalanceThresholdPercentage,
      lmc.dampeningPercentage,
      lmc.allowThroughputOvershoot,
      lmc.disableWorkerMetrics,
      lmc.maxThroughputPerHostKBps,
      lmc.isGracefulLeaseHandoffEnabled,
      lmc.gracefulLeaseHandoffTimeoutMillis,
    )

  def apply(rsc: Api.RetrievalSpecificConfig): RetrievalSpecificConfig = rsc match {
    case foc: Api.RetrievalSpecificConfig.FanOutConfig => apply(foc)
    case pc: Api.RetrievalSpecificConfig.PollingConfig => apply(pc)
  }

  def apply(foc: Api.RetrievalSpecificConfig.FanOutConfig): RetrievalSpecificConfig.FanOutConfig =
    RetrievalSpecificConfig.FanOutConfig(
      consumerArn = foc.consumerArn,
      consumerName = foc.consumerName,
      maxDescribeStreamSummaryRetries = foc.maxDescribeStreamSummaryRetries,
      maxDescribeStreamConsumerRetries = foc.maxDescribeStreamConsumerRetries,
      registerStreamConsumerRetries = foc.registerStreamConsumerRetries,
      retryBackoffMillis = foc.retryBackoffMillis,
    )

  def apply(pc: Api.RetrievalSpecificConfig.PollingConfig): RetrievalSpecificConfig.PollingConfig =
    RetrievalSpecificConfig.PollingConfig(
      pc.maxRecords,
      pc.retryGetRecordsInSeconds,
      pc.maxGetRecordsThreadPool,
      pc.idleTimeBetweenReadsInMillis,
    )

  def apply(prc: Api.ProcessorConfig): ProcessorConfig =
    ProcessorConfig(prc.callProcessRecordsEvenForEmptyRecordList)

  def apply(cc: Api.CoordinatorConfig): CoordinatorConfig =
    CoordinatorConfig(
      cc.parentShardPollIntervalMillis,
      cc.skipShardSyncAtWorkerInitializationIfLeasesExist,
      cc.shardPrioritization.map(apply),
      cc.clientVersionConfig.map(apply),
    )

  def apply(lc: Api.LifecycleConfig): LifecycleConfig =
    LifecycleConfig(lc.taskBackoffTimeMillis, lc.logWarningForTaskAfterMillis)

  def apply(rc: Api.RetrievalConfig): RetrievalConfig =
    RetrievalConfig(rc.listShardsBackoffTimeInMillis, rc.maxListShardsRetryAttempts)

  def apply(mc: Api.MetricsConfig): MetricsConfig =
    MetricsConfig(
      mc.metricsBufferTimeMillis,
      mc.metricsMaxQueueSize,
      mc.metricsLevel.map(apply),
      mc.metricsEnabledDimensions.map(_.map(apply)),
    )

  def apply(kcl: Api.KCLConfiguration): KCLConfiguration =
    KCLConfiguration(
      kcl.configsBuilder.map(apply).getOrElse(ConfigsBuilder()),
      kcl.leaseManagementConfig.map(apply).getOrElse(LeaseManagementConfig()),
      kcl.retrievalSpecificConfig.map(apply),
      kcl.processorConfig.map(apply).getOrElse(ProcessorConfig()),
      kcl.coordinatorConfig.map(apply).getOrElse(CoordinatorConfig()),
      kcl.lifecycleConfig.map(apply).getOrElse(LifecycleConfig()),
      kcl.retrievalConfig.map(apply).getOrElse(RetrievalConfig()),
      kcl.metricsConfig.map(apply).getOrElse(MetricsConfig()),
    )

  def apply(cb: Api.ConfigsBuilder): ConfigsBuilder =
    ConfigsBuilder(cb.tableName, cb.workerIdentifier)

  def apply(src: Api.IngestSource): IngestSource = src match {
    case src: Api.IngestSource.File =>
      FileIngest(
        apply(src.format),
        src.path,
        src.fileIngestMode.map(apply),
        src.maximumLineSize,
        src.startOffset,
        src.limit,
        src.characterEncoding,
        src.recordDecoders.map(apply),
      )
    case src: Api.IngestSource.StdInput =>
      StdInputIngest(
        apply(src.format),
        src.maximumLineSize,
        src.characterEncoding,
      )
    case src: Api.IngestSource.NumberIterator =>
      NumberIteratorIngest(
        StreamingFormat.RawFormat,
        src.startOffset,
        src.limit,
      )
    case src: Api.IngestSource.WebsocketClient =>
      WebsocketIngest(
        apply(src.format),
        src.url,
        src.initMessages,
        apply(src.keepAlive),
        src.characterEncoding,
      )
    case src: Api.IngestSource.Kinesis =>
      KinesisIngest(
        apply(src.format),
        src.streamName,
        src.shardIds,
        src.credentials.map(ConvertCore.Api2ToModel1.apply),
        src.region.map(ConvertCore.Api2ToModel1.apply),
        apply(src.iteratorType),
        src.numRetries,
        src.recordDecoders.map(apply),
      )
    case Api.IngestSource.KinesisKCL(
          kinesisStreamName,
          applicationName,
          format,
          credentialsOpt,
          regionOpt,
          initialPosition,
          numRetries,
          recordDecoders,
          schedulerSourceSettings,
          checkpointSettings,
          advancedSettings,
        ) =>
      KinesisKclIngest(
        kinesisStreamName = kinesisStreamName,
        applicationName = applicationName,
        format = apply(format),
        credentialsOpt = credentialsOpt.map(ConvertCore.Api2ToModel1.apply),
        regionOpt = regionOpt.map(ConvertCore.Api2ToModel1.apply),
        initialPosition = apply(initialPosition),
        numRetries = numRetries,
        recordDecoders = recordDecoders.map(apply),
        schedulerSourceSettings = schedulerSourceSettings
          .map(apply)
          .getOrElse(KinesisSchedulerSourceSettings()),
        checkpointSettings = checkpointSettings.map(apply).getOrElse(KinesisCheckpointSettings()),
        advancedSettings = advancedSettings.map(apply).getOrElse(KCLConfiguration()),
      )
    case src: Api.IngestSource.ServerSentEvent =>
      ServerSentEventIngest(
        apply(src.format),
        src.url,
        src.recordDecoders.map(apply),
      )
    case src: Api.IngestSource.SQS =>
      SQSIngest(
        apply(src.format),
        src.queueUrl,
        src.readParallelism,
        src.credentials.map(ConvertCore.Api2ToModel1.apply),
        src.region.map(ConvertCore.Api2ToModel1.apply),
        src.deleteReadMessages,
        src.recordDecoders.map(apply),
      )
    case src: Api.IngestSource.Kafka =>
      KafkaIngest(
        apply(src.format),
        src.topics,
        src.bootstrapServers,
        src.groupId,
        apply(src.securityProtocol),
        src.offsetCommitting.map(apply),
        apply(src.autoOffsetReset),
        src.kafkaProperties,
        src.endingOffset,
        src.recordDecoders.map(apply),
      )
    case src: Api.IngestSource.S3 =>
      S3Ingest(
        apply(src.format),
        src.bucket,
        src.key,
        src.credentials.map(ConvertCore.Api2ToModel1.apply),
        src.maximumLineSize,
        src.startOffset,
        src.limit,
        src.characterEncoding,
        src.recordDecoders.map(apply),
      )
    case Api.IngestSource.ReactiveStream(url, port, format) =>
      ReactiveStreamIngest(apply(url), port, format)
    case Api.WebSocketFileUpload(format) =>
      WebSocketFileUpload(apply(format))
  }

  def apply(handler: Api.OnStreamErrorHandler): Ingest.OnStreamErrorHandler = handler match {
    case Api.RetryStreamError(retryCount) => Ingest.RetryStreamError(retryCount)
    case Api.LogStreamError => Ingest.LogStreamError
  }

  def apply(transformation: Api.Transformation): Ingest.Transformation = transformation match {
    case Api.Transformation.JavaScript(function) => Ingest.Transformation.JavaScript(function)
  }

  def apply(conf: Api.Oss.QuineIngestConfiguration): Ingest.QuineIngestConfiguration =
    Ingest.QuineIngestConfiguration(
      conf.name,
      apply(conf.source),
      conf.query,
      conf.parameter,
      conf.transformation.map(apply),
      conf.parallelism,
      conf.maxPerSecond,
      conf.onRecordError,
      apply(conf.onStreamError),
    )

  def apply(status: Api.IngestStreamStatus): V1.IngestStreamStatus = status match {
    case Api.IngestStreamStatus.Completed => V1.IngestStreamStatus.Completed
    case Api.IngestStreamStatus.Terminated => V1.IngestStreamStatus.Terminated
    case Api.IngestStreamStatus.Failed => V1.IngestStreamStatus.Failed
    case Api.IngestStreamStatus.Running => V1.IngestStreamStatus.Running
    case Api.IngestStreamStatus.Paused => V1.IngestStreamStatus.Paused
    case Api.IngestStreamStatus.Restored => V1.IngestStreamStatus.Restored
  }

}
