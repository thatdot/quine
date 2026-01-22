package com.thatdot.quine.app.v2api.converters

import com.thatdot.quine.app.model.ingest2._
import com.thatdot.quine.app.model.ingest2.{V2IngestEntities => Ingest}
import com.thatdot.quine.app.v2api.definitions.ingest2.{ApiIngest => Api}
import com.thatdot.quine.{routes => V1}
import com.thatdot.{convert => ConvertCore}

object IngestToApi {

  //For conversions to API methods that we may not be able to define in quine OSS, but we also
  // don't want inside a non-sealed base type
  trait ToApiMethod[A, B] {
    def apply(a: A): B
  }

  object OssConversions {

    implicit val quineIngestConfigurationToApi
      : ToApiMethod[Ingest.QuineIngestConfiguration, Api.Oss.QuineIngestConfiguration] =
      (a: Ingest.QuineIngestConfiguration) => apply(a)
  }
  def apply(status: V1.IngestStreamStatus): Api.IngestStreamStatus = status match {
    case V1.IngestStreamStatus.Completed => Api.IngestStreamStatus.Completed
    case V1.IngestStreamStatus.Terminated => Api.IngestStreamStatus.Terminated
    case V1.IngestStreamStatus.Failed => Api.IngestStreamStatus.Failed
    case V1.IngestStreamStatus.Running => Api.IngestStreamStatus.Running
    case V1.IngestStreamStatus.Paused => Api.IngestStreamStatus.Paused
    case V1.IngestStreamStatus.Restored => Api.IngestStreamStatus.Restored
  }

  def apply(stats: V1.IngestStreamStats): Api.IngestStreamStats =
    Api.IngestStreamStats(
      ingestedCount = stats.ingestedCount,
      rates = ConvertCore.Model1ToApi2.apply(stats.rates),
      byteRates = ConvertCore.Model1ToApi2.apply(stats.byteRates),
      startTime = stats.startTime,
      totalRuntime = stats.totalRuntime,
    )

  def apply(c: V1.CsvCharacter): Api.CsvCharacter = c match {
    case V1.CsvCharacter.Backslash => Api.CsvCharacter.Backslash
    case V1.CsvCharacter.Comma => Api.CsvCharacter.Comma
    case V1.CsvCharacter.Semicolon => Api.CsvCharacter.Semicolon
    case V1.CsvCharacter.Colon => Api.CsvCharacter.Colon
    case V1.CsvCharacter.Tab => Api.CsvCharacter.Tab
    case V1.CsvCharacter.Pipe => Api.CsvCharacter.Pipe
    case V1.CsvCharacter.DoubleQuote => Api.CsvCharacter.DoubleQuote
  }
  def apply(format: FileFormat): Api.IngestFormat.FileFormat = format match {
    case FileFormat.LineFormat => Api.IngestFormat.FileFormat.Line
    case FileFormat.JsonLinesFormat => Api.IngestFormat.FileFormat.JsonL
    case FileFormat.JsonFormat => Api.IngestFormat.FileFormat.Json
    case FileFormat.CsvFormat(headers, delimiter, quoteChar, escapeChar) =>
      Api.IngestFormat.FileFormat.CSV(
        headers = headers,
        delimiter = apply(delimiter),
        quoteChar = apply(quoteChar),
        escapeChar = apply(escapeChar),
      )
  }

  def apply(format: StreamingFormat): Api.IngestFormat.StreamingFormat = format match {
    case StreamingFormat.JsonFormat => Api.IngestFormat.StreamingFormat.Json
    case StreamingFormat.RawFormat => Api.IngestFormat.StreamingFormat.Raw
    case StreamingFormat.ProtobufFormat(schemaUrl, typeName) =>
      Api.IngestFormat.StreamingFormat.Protobuf(schemaUrl, typeName)
    case StreamingFormat.AvroFormat(schemaUrl) => Api.IngestFormat.StreamingFormat.Avro(schemaUrl)
    case StreamingFormat.DropFormat => Api.IngestFormat.StreamingFormat.Drop
  }
  def apply(
    proto: V1.WebsocketSimpleStartupIngest.KeepaliveProtocol,
  ): Api.WebSocketClient.KeepaliveProtocol = proto match {
    case V1.WebsocketSimpleStartupIngest.PingPongInterval(intervalMillis) =>
      Api.WebSocketClient.PingPongInterval(intervalMillis)
    case V1.WebsocketSimpleStartupIngest.SendMessageInterval(message, intervalMillis) =>
      Api.WebSocketClient.SendMessageInterval(message, intervalMillis)
    case V1.WebsocketSimpleStartupIngest.NoKeepalive => Api.WebSocketClient.NoKeepalive
  }
  def apply(it: V1.KinesisIngest.IteratorType): Api.IngestSource.Kinesis.IteratorType = it match {
    case V1.KinesisIngest.IteratorType.Latest => Api.IngestSource.Kinesis.IteratorType.Latest
    case V1.KinesisIngest.IteratorType.TrimHorizon => Api.IngestSource.Kinesis.IteratorType.TrimHorizon
    case V1.KinesisIngest.IteratorType.AtSequenceNumber(sequenceNumber) =>
      Api.IngestSource.Kinesis.IteratorType.AtSequenceNumber(sequenceNumber)
    case V1.KinesisIngest.IteratorType.AfterSequenceNumber(sequenceNumber) =>
      Api.IngestSource.Kinesis.IteratorType.AfterSequenceNumber(sequenceNumber)
    case V1.KinesisIngest.IteratorType.AtTimestamp(millisSinceEpoch) =>
      Api.IngestSource.Kinesis.IteratorType.AtTimestamp(millisSinceEpoch)
  }
  def apply(proto: V1.KafkaSecurityProtocol): Api.KafkaSecurityProtocol = proto match {
    case V1.KafkaSecurityProtocol.PlainText => Api.KafkaSecurityProtocol.PlainText
    case V1.KafkaSecurityProtocol.Ssl => Api.KafkaSecurityProtocol.Ssl
    case V1.KafkaSecurityProtocol.Sasl_Ssl => Api.KafkaSecurityProtocol.Sasl_Ssl
    case V1.KafkaSecurityProtocol.Sasl_Plaintext => Api.KafkaSecurityProtocol.Sasl_Plaintext
  }
  def apply(reset: V1.KafkaAutoOffsetReset): Api.KafkaAutoOffsetReset = reset match {
    case V1.KafkaAutoOffsetReset.Latest => Api.KafkaAutoOffsetReset.Latest
    case V1.KafkaAutoOffsetReset.Earliest => Api.KafkaAutoOffsetReset.Earliest
    case V1.KafkaAutoOffsetReset.None => Api.KafkaAutoOffsetReset.None
  }

  def apply(mode: V1.FileIngestMode): Api.FileIngestMode = mode match {
    case V1.FileIngestMode.Regular => Api.FileIngestMode.Regular
    case V1.FileIngestMode.NamedPipe => Api.FileIngestMode.NamedPipe
  }
  def apply(ty: V1.RecordDecodingType): Api.RecordDecodingType = ty match {
    case V1.RecordDecodingType.Zlib => Api.RecordDecodingType.Zlib
    case V1.RecordDecodingType.Gzip => Api.RecordDecodingType.Gzip
    case V1.RecordDecodingType.Base64 => Api.RecordDecodingType.Base64
  }

  def apply(c: V1.KafkaOffsetCommitting): Api.KafkaOffsetCommitting = c match {
    case V1.KafkaOffsetCommitting.ExplicitCommit(maxBatch, maxIntervalMillis, parallelism, waitForCommitConfirmation) =>
      Api.KafkaOffsetCommitting.ExplicitCommit(maxBatch, maxIntervalMillis, parallelism, waitForCommitConfirmation)
  }

  /* ---------- enums / sealed‑traits ---------- */

  def apply(bm: BillingMode): Api.BillingMode = bm match {
    case BillingMode.PROVISIONED => Api.BillingMode.PROVISIONED
    case BillingMode.PAY_PER_REQUEST => Api.BillingMode.PAY_PER_REQUEST
    case BillingMode.UNKNOWN_TO_SDK_VERSION => Api.BillingMode.UNKNOWN_TO_SDK_VERSION
  }

  def apply(ip: InitialPosition): Api.InitialPosition = ip match {
    case InitialPosition.Latest => Api.InitialPosition.Latest
    case InitialPosition.TrimHorizon => Api.InitialPosition.TrimHorizon
    case InitialPosition.AtTimestamp(y, m, d, h, mm, s) =>
      Api.InitialPosition.AtTimestamp(y, m, d, h, mm, s)
  }

  def apply(sp: ShardPrioritization): Api.ShardPrioritization = sp match {
    case ShardPrioritization.NoOpShardPrioritization => Api.ShardPrioritization.NoOpShardPrioritization
    case ShardPrioritization.ParentsFirstShardPrioritization(d) =>
      Api.ShardPrioritization.ParentsFirstShardPrioritization(d)
  }

  def apply(cvc: ClientVersionConfig): Api.ClientVersionConfig = cvc match {
    case ClientVersionConfig.CLIENT_VERSION_CONFIG_COMPATIBLE_WITH_2X =>
      Api.ClientVersionConfig.CLIENT_VERSION_CONFIG_COMPATIBLE_WITH_2X
    case ClientVersionConfig.CLIENT_VERSION_CONFIG_3X => Api.ClientVersionConfig.CLIENT_VERSION_CONFIG_3X
  }

  def apply(ml: MetricsLevel): Api.MetricsLevel = ml match {
    case MetricsLevel.NONE => Api.MetricsLevel.NONE
    case MetricsLevel.SUMMARY => Api.MetricsLevel.SUMMARY
    case MetricsLevel.DETAILED => Api.MetricsLevel.DETAILED
  }

  def apply(md: MetricsDimension): Api.MetricsDimension = md match {
    case MetricsDimension.OPERATION_DIMENSION_NAME => Api.MetricsDimension.OPERATION_DIMENSION_NAME
    case MetricsDimension.SHARD_ID_DIMENSION_NAME => Api.MetricsDimension.SHARD_ID_DIMENSION_NAME
    case MetricsDimension.STREAM_IDENTIFIER => Api.MetricsDimension.STREAM_IDENTIFIER
    case MetricsDimension.WORKER_IDENTIFIER => Api.MetricsDimension.WORKER_IDENTIFIER
  }

  def apply(kcs: KinesisCheckpointSettings): Api.KinesisCheckpointSettings =
    Api.KinesisCheckpointSettings(kcs.disableCheckpointing, kcs.maxBatchSize, kcs.maxBatchWaitMillis)

  def apply(ksss: KinesisSchedulerSourceSettings): Api.KinesisSchedulerSourceSettings =
    Api.KinesisSchedulerSourceSettings(ksss.bufferSize, ksss.backpressureTimeoutMillis)

  def apply(lmc: LeaseManagementConfig): Api.LeaseManagementConfig =
    Api.LeaseManagementConfig(
      failoverTimeMillis = lmc.failoverTimeMillis,
      shardSyncIntervalMillis = lmc.shardSyncIntervalMillis,
      cleanupLeasesUponShardCompletion = lmc.cleanupLeasesUponShardCompletion,
      ignoreUnexpectedChildShards = lmc.ignoreUnexpectedChildShards,
      maxLeasesForWorker = lmc.maxLeasesForWorker,
      maxLeaseRenewalThreads = lmc.maxLeaseRenewalThreads,
      billingMode = lmc.billingMode.map(apply),
      initialLeaseTableReadCapacity = lmc.initialLeaseTableReadCapacity,
      initialLeaseTableWriteCapacity = lmc.initialLeaseTableWriteCapacity,
      reBalanceThresholdPercentage = lmc.reBalanceThresholdPercentage,
      dampeningPercentage = lmc.dampeningPercentage,
      allowThroughputOvershoot = lmc.allowThroughputOvershoot,
      disableWorkerMetrics = lmc.disableWorkerMetrics,
      maxThroughputPerHostKBps = lmc.maxThroughputPerHostKBps,
      isGracefulLeaseHandoffEnabled = lmc.isGracefulLeaseHandoffEnabled,
      gracefulLeaseHandoffTimeoutMillis = lmc.gracefulLeaseHandoffTimeoutMillis,
    )

  def apply(rsc: RetrievalSpecificConfig): Api.RetrievalSpecificConfig = rsc match {
    case foc: RetrievalSpecificConfig.FanOutConfig => apply(foc)
    case pc: RetrievalSpecificConfig.PollingConfig => apply(pc)
  }

  def apply(foc: RetrievalSpecificConfig.FanOutConfig): Api.RetrievalSpecificConfig.FanOutConfig =
    Api.RetrievalSpecificConfig.FanOutConfig(
      consumerArn = foc.consumerArn,
      consumerName = foc.consumerName,
      maxDescribeStreamSummaryRetries = foc.maxDescribeStreamSummaryRetries,
      maxDescribeStreamConsumerRetries = foc.maxDescribeStreamConsumerRetries,
      registerStreamConsumerRetries = foc.registerStreamConsumerRetries,
      retryBackoffMillis = foc.retryBackoffMillis,
    )

  def apply(pc: RetrievalSpecificConfig.PollingConfig): Api.RetrievalSpecificConfig.PollingConfig =
    Api.RetrievalSpecificConfig.PollingConfig(
      maxRecords = pc.maxRecords,
      retryGetRecordsInSeconds = pc.retryGetRecordsInSeconds,
      maxGetRecordsThreadPool = pc.maxGetRecordsThreadPool,
      idleTimeBetweenReadsInMillis = pc.idleTimeBetweenReadsInMillis,
    )

  def apply(prc: ProcessorConfig): Api.ProcessorConfig =
    Api.ProcessorConfig(prc.callProcessRecordsEvenForEmptyRecordList)

  def apply(cc: CoordinatorConfig): Api.CoordinatorConfig =
    Api.CoordinatorConfig(
      parentShardPollIntervalMillis = cc.parentShardPollIntervalMillis,
      skipShardSyncAtWorkerInitializationIfLeasesExist = cc.skipShardSyncAtWorkerInitializationIfLeasesExist,
      shardPrioritization = cc.shardPrioritization.map(apply),
      clientVersionConfig = cc.clientVersionConfig.map(apply),
    )

  def apply(lc: LifecycleConfig): Api.LifecycleConfig =
    Api.LifecycleConfig(lc.taskBackoffTimeMillis, lc.logWarningForTaskAfterMillis)

  def apply(rc: RetrievalConfig): Api.RetrievalConfig =
    Api.RetrievalConfig(rc.listShardsBackoffTimeInMillis, rc.maxListShardsRetryAttempts)

  def apply(mc: MetricsConfig): Api.MetricsConfig =
    Api.MetricsConfig(
      metricsBufferTimeMillis = mc.metricsBufferTimeMillis,
      metricsMaxQueueSize = mc.metricsMaxQueueSize,
      metricsLevel = mc.metricsLevel.map(apply),
      metricsEnabledDimensions = mc.metricsEnabledDimensions.map(_.map(apply)),
    )

  def apply(kcl: KCLConfiguration): Api.KCLConfiguration =
    Api.KCLConfiguration(
      configsBuilder = Some(apply(kcl.configsBuilder)),
      leaseManagementConfig = Some(apply(kcl.leaseManagementConfig)),
      retrievalSpecificConfig = kcl.retrievalSpecificConfig.map(apply),
      processorConfig = Some(apply(kcl.processorConfig)),
      coordinatorConfig = Some(apply(kcl.coordinatorConfig)),
      lifecycleConfig = Some(apply(kcl.lifecycleConfig)),
      retrievalConfig = Some(apply(kcl.retrievalConfig)),
      metricsConfig = Some(apply(kcl.metricsConfig)),
    )

  def apply(cb: ConfigsBuilder): Api.ConfigsBuilder =
    Api.ConfigsBuilder(cb.tableName, cb.workerIdentifier)

  def apply(info: Ingest.IngestStreamInfo): Api.IngestStreamInfo =
    Api.IngestStreamInfo(
      status = apply(info.status),
      message = info.message,
      settings = apply(info.settings),
      stats = apply(info.stats),
    )

  def apply(stats: Ingest.IngestStreamStats): Api.IngestStreamStats = Api.IngestStreamStats(
    ingestedCount = stats.ingestedCount,
    rates = apply(stats.rates),
    byteRates = apply(stats.byteRates),
    startTime = stats.startTime,
    totalRuntime = stats.totalRuntime,
  )

  def apply(ratesSummary: Ingest.RatesSummary): com.thatdot.api.v2.RatesSummary =
    com.thatdot.api.v2.RatesSummary(
      count = ratesSummary.count,
      oneMinute = ratesSummary.oneMinute,
      fiveMinute = ratesSummary.fiveMinute,
      fifteenMinute = ratesSummary.fifteenMinute,
      overall = ratesSummary.overall,
    )

  def apply(source: IngestSource): Api.IngestSource = source match {
    case FileIngest(
          format,
          path,
          ingestMode,
          maximumLineSize,
          startOffset,
          limit,
          characterEncoding,
          recordDecoders,
        ) =>
      Api.IngestSource.File(
        format = apply(format),
        path = path,
        fileIngestMode = ingestMode.map(apply),
        maximumLineSize = maximumLineSize,
        startOffset = startOffset,
        limit = limit,
        characterEncoding = characterEncoding,
        recordDecoders = recordDecoders.map(apply),
      )
    case S3Ingest(
          format,
          bucket,
          key,
          credentials,
          maximumLineSize,
          startOffset,
          limit,
          characterEncoding,
          recordDecoders,
        ) =>
      Api.IngestSource.S3(
        format = apply(format),
        bucket = bucket,
        key = key,
        credentials = credentials.map(ConvertCore.Model1ToApi2.apply),
        maximumLineSize = maximumLineSize,
        startOffset = startOffset,
        limit = limit,
        characterEncoding = characterEncoding,
        recordDecoders = recordDecoders.map(apply),
      )
    case StdInputIngest(format, maximumLineSize, characterEncoding) =>
      Api.IngestSource.StdInput(
        format = apply(format),
        maximumLineSize = maximumLineSize,
        characterEncoding = characterEncoding,
      )
    case NumberIteratorIngest(_, startOffset, limit) =>
      Api.IngestSource.NumberIterator(startOffset, limit)
    case WebsocketIngest(format, url, initMessages, keepAlive, characterEncoding) =>
      Api.IngestSource.WebsocketClient(
        format = apply(format),
        url = url,
        initMessages = initMessages,
        keepAlive = apply(keepAlive),
        characterEncoding = characterEncoding,
      )
    case KinesisIngest(
          format,
          streamName,
          shardIds,
          credentials,
          region,
          iteratorType,
          numRetries,
          recordDecoders,
        ) =>
      Api.IngestSource.Kinesis(
        format = apply(format),
        streamName = streamName,
        shardIds = shardIds,
        credentials = credentials.map(ConvertCore.Model1ToApi2.apply),
        region = region.map(ConvertCore.Model1ToApi2.apply),
        iteratorType = apply(iteratorType),
        numRetries = numRetries,
        recordDecoders = recordDecoders.map(apply),
      )

    case KinesisKclIngest(
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
      Api.IngestSource.KinesisKCL(
        kinesisStreamName = kinesisStreamName,
        applicationName = applicationName,
        format = apply(format),
        credentials = credentialsOpt.map(ConvertCore.Model1ToApi2.apply),
        region = regionOpt.map(ConvertCore.Model1ToApi2.apply),
        initialPosition = apply(initialPosition),
        numRetries = numRetries,
        recordDecoders = recordDecoders.map(apply),
        schedulerSourceSettings = Some(apply(schedulerSourceSettings)),
        checkpointSettings = Some(apply(checkpointSettings)),
        advancedSettings = Some(apply(advancedSettings)),
      )
    case ServerSentEventIngest(format, url, recordDecoders) =>
      Api.IngestSource.ServerSentEvent(
        format = apply(format),
        url = url,
        recordDecoders = recordDecoders.map(apply),
      )
    case SQSIngest(format, queueUrl, readParallelism, credentials, region, deleteReadMessages, recordDecoders) =>
      Api.IngestSource.SQS(
        format = apply(format),
        queueUrl = queueUrl,
        readParallelism = readParallelism,
        credentials = credentials.map(ConvertCore.Model1ToApi2.apply),
        region = region.map(ConvertCore.Model1ToApi2.apply),
        deleteReadMessages = deleteReadMessages,
        recordDecoders = recordDecoders.map(apply),
      )
    case KafkaIngest(
          format,
          topics,
          bootstrapServers,
          groupId,
          protocol,
          offsetCommitting,
          autoOffsetReset,
          kafkaProperties,
          endingOffset,
          recordDecoders,
        ) =>
      Api.IngestSource.Kafka(
        format = apply(format),
        topics = topics,
        bootstrapServers = bootstrapServers,
        groupId = groupId,
        securityProtocol = apply(protocol),
        offsetCommitting = offsetCommitting.map(apply),
        autoOffsetReset = apply(autoOffsetReset),
        kafkaProperties = kafkaProperties,
        endingOffset = endingOffset,
        recordDecoders = recordDecoders.map(apply),
      )
    case ReactiveStreamIngest(format, url, port) =>
      Api.IngestSource.ReactiveStream(apply(format), url, port)
    case WebSocketFileUpload(format) =>
      Api.WebSocketFileUpload(apply(format))
  }

  def apply(handler: Ingest.OnStreamErrorHandler): Api.OnStreamErrorHandler = handler match {
    case Ingest.RetryStreamError(retryCount) => Api.RetryStreamError(retryCount)
    case Ingest.LogStreamError => Api.LogStreamError
  }

  def apply(transformation: Ingest.Transformation): Api.Transformation = transformation match {
    case Ingest.Transformation.JavaScript(function) => Api.Transformation.JavaScript(function)
  }

  def apply(conf: Ingest.QuineIngestConfiguration): Api.Oss.QuineIngestConfiguration =
    Api.Oss.QuineIngestConfiguration(
      name = conf.name,
      source = apply(conf.source),
      query = conf.query,
      parameter = conf.parameter,
      transformation = conf.transformation.map(apply),
      parallelism = conf.parallelism,
      maxPerSecond = conf.maxPerSecond,
      onRecordError = conf.onRecordError,
      onStreamError = apply(conf.onStreamError),
    )

  def apply(status: Ingest.IngestStreamStatus): Api.IngestStreamStatus = status match {
    case Ingest.IngestStreamStatus.Completed => Api.IngestStreamStatus.Completed
    case Ingest.IngestStreamStatus.Terminated => Api.IngestStreamStatus.Terminated
    case Ingest.IngestStreamStatus.Failed => Api.IngestStreamStatus.Failed
    case Ingest.IngestStreamStatus.Running => Api.IngestStreamStatus.Running
    case Ingest.IngestStreamStatus.Paused => Api.IngestStreamStatus.Paused
    case Ingest.IngestStreamStatus.Restored => Api.IngestStreamStatus.Restored
  }

  def apply(info: Ingest.IngestStreamInfoWithName): Api.IngestStreamInfoWithName =
    Api.IngestStreamInfoWithName(
      name = info.name,
      status = apply(info.status),
      message = info.message,
      settings = apply(info.settings),
      stats = apply(info.stats),
    )
}
