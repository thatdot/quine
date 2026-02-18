package com.thatdot.quine.ingest2

import java.nio.charset.Charset
import java.time.{Duration, Instant, ZoneOffset}

import scala.jdk.CollectionConverters._

import org.scalacheck.{Arbitrary, Gen}

import com.thatdot.api.v2.SaslJaasConfigGenerators
import com.thatdot.common.security.Secret
import com.thatdot.quine.app.model.ingest2.V2IngestEntities._
import com.thatdot.quine.app.model.ingest2._
import com.thatdot.quine.{ScalaPrimitiveGenerators, TimeGenerators, routes => V1}

object V2IngestEntitiesGenerators {

  import ScalaPrimitiveGenerators.Gens._
  import SaslJaasConfigGenerators.Gens.{optSecret, optSaslJaasConfig}

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
      retryBackoffMillis <- Gen.option(mediumPosLong)
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
      idleTimeBetweenReadsInMillis <- Gen.option(mediumPosLong)
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
      maxBatchWaitMillis <- Gen.option(mediumPosLong)
    } yield KinesisCheckpointSettings(disableCheckpointing, maxBatchSize, maxBatchWaitMillis)

    val kinesisSchedulerSourceSettings: Gen[KinesisSchedulerSourceSettings] = for {
      bufferSize <- Gen.option(smallPosNum)
      backpressureTimeoutMillis <- Gen.option(mediumPosLong)
    } yield KinesisSchedulerSourceSettings(bufferSize, backpressureTimeoutMillis)

    val configsBuilder: Gen[ConfigsBuilder] = for {
      tableName <- optNonEmptyAlphaNumStr
      workerIdentifier <- optNonEmptyAlphaNumStr
    } yield ConfigsBuilder(tableName, workerIdentifier)

    val lifecycleConfig: Gen[LifecycleConfig] = for {
      taskBackoffTimeMillis <- Gen.option(mediumPosLong)
      logWarningForTaskAfterMillis <- Gen.option(mediumPosLong)
    } yield LifecycleConfig(taskBackoffTimeMillis, logWarningForTaskAfterMillis)

    val retrievalConfig: Gen[RetrievalConfig] = for {
      listShardsBackoffTimeInMillis <- Gen.option(mediumPosLong)
      maxListShardsRetryAttempts <- Gen.option(smallPosNum)
    } yield RetrievalConfig(listShardsBackoffTimeInMillis, maxListShardsRetryAttempts)

    val processorConfig: Gen[ProcessorConfig] = for {
      callProcessRecordsEvenForEmptyRecordList <- Gen.option(bool)
    } yield ProcessorConfig(callProcessRecordsEvenForEmptyRecordList)

    val leaseManagementConfig: Gen[LeaseManagementConfig] = for {
      failoverTimeMillis <- Gen.option(mediumPosLong)
      shardSyncIntervalMillis <- Gen.option(mediumPosLong)
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
      maxThroughputPerHostKBps <- Gen.option(mediumNonNegDouble)
      isGracefulLeaseHandoffEnabled <- Gen.option(bool)
      gracefulLeaseHandoffTimeoutMillis <- Gen.option(mediumPosLong)
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
      parentShardPollIntervalMillis <- Gen.option(mediumPosLong)
      skipShardSyncAtWorkerInitializationIfLeasesExist <- Gen.option(bool)
      sp <- Gen.option(shardPrioritization)
      cvc <- Gen.option(clientVersionConfig)
    } yield CoordinatorConfig(parentShardPollIntervalMillis, skipShardSyncAtWorkerInitializationIfLeasesExist, sp, cvc)

    val metricsConfig: Gen[MetricsConfig] = for {
      metricsBufferTimeMillis <- Gen.option(mediumPosLong)
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

    val awsCredentials: Gen[V1.AwsCredentials] = for {
      accessKeyId <- nonEmptyAlphaNumStr
      secretAccessKey <- nonEmptyAlphaNumStr
    } yield V1.AwsCredentials(Secret(accessKeyId), Secret(secretAccessKey))

    val awsRegion: Gen[V1.AwsRegion] =
      Gen.oneOf("us-east-1", "us-west-2", "eu-west-1", "ap-northeast-1").map(V1.AwsRegion.apply)

    val kinesisIteratorType: Gen[V1.KinesisIngest.IteratorType] = Gen.oneOf(
      Gen.const(V1.KinesisIngest.IteratorType.TrimHorizon),
      Gen.const(V1.KinesisIngest.IteratorType.Latest),
      nonEmptyAlphaNumStr.map(V1.KinesisIngest.IteratorType.AtSequenceNumber(_)),
      nonEmptyAlphaNumStr.map(V1.KinesisIngest.IteratorType.AfterSequenceNumber(_)),
      mediumPosLong.map(V1.KinesisIngest.IteratorType.AtTimestamp(_)),
    )

    val transformationJavaScript: Gen[Transformation.JavaScript] =
      nonEmptyAlphaNumStr.map(Transformation.JavaScript(_))

    val transformation: Gen[Transformation] = transformationJavaScript

    val charset: Gen[Charset] =
      Gen.oneOf[String](Charset.availableCharsets().keySet().asScala.take(10)).map(Charset.forName)

    val fileIngestMode: Gen[V1.FileIngestMode] = Gen.oneOf(V1.FileIngestMode.Regular, V1.FileIngestMode.NamedPipe)

    val recordDecodingType: Gen[V1.RecordDecodingType] =
      Gen.oneOf(V1.RecordDecodingType.Gzip, V1.RecordDecodingType.Base64, V1.RecordDecodingType.Zlib)

    val recordDecodingSeq: Gen[Seq[V1.RecordDecodingType]] =
      Gen.containerOf[Seq, V1.RecordDecodingType](recordDecodingType)

    val csvCharacter: Gen[V1.CsvCharacter] = Gen.oneOf(
      V1.CsvCharacter.Comma,
      V1.CsvCharacter.Tab,
      V1.CsvCharacter.Semicolon,
      V1.CsvCharacter.Colon,
      V1.CsvCharacter.Backslash,
      V1.CsvCharacter.DoubleQuote,
    )

    val csvFormat: Gen[FileFormat.CsvFormat] = for {
      headers <- Gen.oneOf(
        Gen.const(Left(false)),
        Gen.const(Left(true)),
        Gen.nonEmptyListOf(nonEmptyAlphaNumStr).map(l => Right(l)),
      )
      delimiter <- csvCharacter
      quoteChar <- csvCharacter.suchThat(_ != delimiter)
      escapeChar <- csvCharacter.suchThat(c => c != delimiter && c != quoteChar)
    } yield FileFormat.CsvFormat(headers, delimiter, quoteChar, escapeChar)

    val fileFormat: Gen[FileFormat] = Gen.oneOf(
      Gen.const(FileFormat.LineFormat),
      Gen.const(FileFormat.JsonLinesFormat),
      Gen.const(FileFormat.JsonFormat),
      csvFormat,
    )

    val protobufFormat: Gen[StreamingFormat.ProtobufFormat] = for {
      schemaUrl <- nonEmptyAlphaNumStr
      typeName <- nonEmptyAlphaNumStr
    } yield StreamingFormat.ProtobufFormat(schemaUrl, typeName)

    val avroFormat: Gen[StreamingFormat.AvroFormat] =
      nonEmptyAlphaNumStr.map(StreamingFormat.AvroFormat(_))

    val streamingFormat: Gen[StreamingFormat] = Gen.oneOf(
      Gen.const(StreamingFormat.JsonFormat),
      Gen.const(StreamingFormat.RawFormat),
      Gen.const(StreamingFormat.DropFormat),
      protobufFormat,
      avroFormat,
    )

    val kafkaSecurityProtocol: Gen[V1.KafkaSecurityProtocol] = Gen.oneOf(
      V1.KafkaSecurityProtocol.PlainText,
      V1.KafkaSecurityProtocol.Ssl,
      V1.KafkaSecurityProtocol.Sasl_Ssl,
      V1.KafkaSecurityProtocol.Sasl_Plaintext,
    )

    val kafkaAutoOffsetReset: Gen[V1.KafkaAutoOffsetReset] = Gen.oneOf(
      V1.KafkaAutoOffsetReset.Latest,
      V1.KafkaAutoOffsetReset.Earliest,
      V1.KafkaAutoOffsetReset.None,
    )

    val kafkaOffsetCommitting: Gen[V1.KafkaOffsetCommitting] = for {
      maxBatch <- smallPosNum
      maxInterval <- mediumPosLong
      parallelism <- smallPosNum
      waitForCommitConfirmation <- bool
    } yield V1.KafkaOffsetCommitting.ExplicitCommit(
      maxBatch.toLong,
      maxInterval.toInt,
      parallelism,
      waitForCommitConfirmation,
    )

    val keepaliveProtocol: Gen[V1.WebsocketSimpleStartupIngest.KeepaliveProtocol] = Gen.oneOf(
      Gen.const(V1.WebsocketSimpleStartupIngest.NoKeepalive),
      smallPosNum.map(V1.WebsocketSimpleStartupIngest.PingPongInterval(_)),
      Gen.zip(nonEmptyAlphaNumStr, smallPosNum).map { case (msg, interval) =>
        V1.WebsocketSimpleStartupIngest.SendMessageInterval(msg, interval)
      },
    )

    val fileIngest: Gen[FileIngest] = for {
      format <- fileFormat
      path <- nonEmptyAlphaNumStr
      fileIngestMode <- Gen.option(fileIngestMode)
      maximumLineSize <- Gen.option(smallPosNum)
      startOffset <- mediumPosLong
      limit <- Gen.option(mediumPosLong)
      characterEncoding <- charset
      recordDecoders <- recordDecodingSeq
    } yield FileIngest(
      format,
      path,
      fileIngestMode,
      maximumLineSize,
      startOffset,
      limit,
      characterEncoding,
      recordDecoders,
    )

    val s3Ingest: Gen[S3Ingest] = for {
      format <- fileFormat
      bucket <- nonEmptyAlphaNumStr
      key <- nonEmptyAlphaNumStr
      credentials <- Gen.option(awsCredentials)
      maximumLineSize <- Gen.option(smallPosNum)
      startOffset <- mediumPosLong
      limit <- Gen.option(mediumPosLong)
      characterEncoding <- charset
      recordDecoders <- recordDecodingSeq
    } yield S3Ingest(
      format,
      bucket,
      key,
      credentials,
      maximumLineSize,
      startOffset,
      limit,
      characterEncoding,
      recordDecoders,
    )

    val reactiveStreamIngest: Gen[ReactiveStreamIngest] = for {
      format <- streamingFormat
      url <- nonEmptyAlphaNumStr
      port <- smallPosNum
    } yield ReactiveStreamIngest(format, url, port)

    val webSocketFileUpload: Gen[WebSocketFileUpload] = fileFormat.map(WebSocketFileUpload(_))

    val stdInputIngest: Gen[StdInputIngest] = for {
      format <- fileFormat
      maximumLineSize <- Gen.option(smallPosNum)
      characterEncoding <- charset
    } yield StdInputIngest(format, maximumLineSize, characterEncoding)

    val numberIteratorIngest: Gen[NumberIteratorIngest] = for {
      format <- streamingFormat
      startOffset <- mediumPosLong
      limit <- Gen.option(mediumPosLong)
    } yield NumberIteratorIngest(format, startOffset, limit)

    val websocketIngest: Gen[WebsocketIngest] = for {
      format <- streamingFormat
      url <- nonEmptyAlphaNumStr
      initMessages <- Gen.listOf(nonEmptyAlphaNumStr)
      keepAlive <- keepaliveProtocol
      characterEncoding <- charset
    } yield WebsocketIngest(format, url, initMessages, keepAlive, characterEncoding)

    val kinesisIngest: Gen[KinesisIngest] = for {
      format <- streamingFormat
      streamName <- nonEmptyAlphaNumStr
      shardIds <- Gen.option(Gen.containerOf[Set, String](nonEmptyAlphaNumStr))
      credentials <- Gen.option(awsCredentials)
      region <- Gen.option(awsRegion)
      iteratorType <- kinesisIteratorType
      numRetries <- smallPosNum
      recordDecoders <- recordDecodingSeq
    } yield KinesisIngest(format, streamName, shardIds, credentials, region, iteratorType, numRetries, recordDecoders)

    val kinesisKclIngest: Gen[KinesisKclIngest] = for {
      kinesisStreamName <- nonEmptyAlphaNumStr
      applicationName <- nonEmptyAlphaNumStr
      format <- streamingFormat
      credentialsOpt <- Gen.option(awsCredentials)
      regionOpt <- Gen.option(awsRegion)
      initialPosition <- Gen.oneOf(
        Gen.const(InitialPosition.Latest),
        Gen.const(InitialPosition.TrimHorizon), {
          val now = Instant.now()
          val fourYearsAgo = now.minus(Duration.ofDays(1460))
          TimeGenerators.Gens.instantWithinRange(from = Some(fourYearsAgo), to = Some(now)).map { instant =>
            val zdt = instant.atZone(ZoneOffset.UTC)
            InitialPosition.AtTimestamp(
              zdt.getYear,
              zdt.getMonthValue,
              zdt.getDayOfMonth,
              zdt.getHour,
              zdt.getMinute,
              zdt.getSecond,
            )
          }
        },
      )
      numRetries <- smallPosNum
      recordDecoders <- recordDecodingSeq
      schedulerSourceSettings <- kinesisSchedulerSourceSettings
      checkpointSettings <- kinesisCheckpointSettings
      advancedSettings <- kclConfiguration
    } yield KinesisKclIngest(
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
    )

    val serverSentEventIngest: Gen[ServerSentEventIngest] = for {
      format <- streamingFormat
      url <- nonEmptyAlphaNumStr
      recordDecoders <- recordDecodingSeq
    } yield ServerSentEventIngest(format, url, recordDecoders)

    val sqsIngest: Gen[SQSIngest] = for {
      format <- streamingFormat
      queueUrl <- nonEmptyAlphaNumStr
      readParallelism <- smallPosNum
      credentials <- Gen.option(awsCredentials)
      region <- Gen.option(awsRegion)
      deleteReadMessages <- bool
      recordDecoders <- recordDecodingSeq
    } yield SQSIngest(format, queueUrl, readParallelism, credentials, region, deleteReadMessages, recordDecoders)

    val kafkaTopics: Gen[Either[V1.KafkaIngest.Topics, V1.KafkaIngest.PartitionAssignments]] =
      Gen.nonEmptyListOf(nonEmptyAlphaNumStr).map(topics => Left(topics.toSet))

    /** Quasi-realistic Kafka Properties map generator. */
    val kafkaProperties: Gen[Map[String, String]] = for {
      maybeServers <- Gen.option(nonEmptyAlphaNumStr.map("bootstrap.servers" -> _))
      maybeFoo <- Gen.option(nonEmptyAlphaNumStr.map("foo" -> _))
      maybeGroupId <- Gen.option(smallPosNum.map(_.toString).map("group.id" -> _))
      maybeAutoCommit <- Gen.option(bool.map(_.toString).map("enable.auto.commit" -> _))
    } yield Seq(maybeServers, maybeFoo, maybeGroupId, maybeAutoCommit).flatten.toMap

    val kafkaIngest: Gen[KafkaIngest] = for {
      format <- streamingFormat
      topics <- kafkaTopics
      bootstrapServers <- nonEmptyAlphaNumStr
      groupId <- Gen.option(nonEmptyAlphaNumStr)
      securityProtocol <- kafkaSecurityProtocol
      offsetCommitting <- Gen.option(kafkaOffsetCommitting)
      autoOffsetReset <- kafkaAutoOffsetReset
      sslKeystorePassword <- optSecret
      sslTruststorePassword <- optSecret
      sslKeyPassword <- optSecret
      saslJaasConfig <- optSaslJaasConfig
      kafkaProperties <- kafkaProperties
      endingOffset <- Gen.option(mediumPosLong)
      recordDecoders <- recordDecodingSeq
    } yield KafkaIngest(
      format,
      topics,
      bootstrapServers,
      groupId,
      securityProtocol,
      offsetCommitting,
      autoOffsetReset,
      sslKeystorePassword,
      sslTruststorePassword,
      sslKeyPassword,
      saslJaasConfig,
      kafkaProperties,
      endingOffset,
      recordDecoders,
    )

    val ingestSource: Gen[IngestSource] = Gen.oneOf(
      fileIngest,
      s3Ingest,
      reactiveStreamIngest,
      webSocketFileUpload,
      stdInputIngest,
      numberIteratorIngest,
      websocketIngest,
      kinesisIngest,
      kinesisKclIngest,
      serverSentEventIngest,
      sqsIngest,
      kafkaIngest,
    )

    val v1IngestStreamStatus: Gen[V1.IngestStreamStatus] = Gen.oneOf(
      V1.IngestStreamStatus.Running,
      V1.IngestStreamStatus.Paused,
      V1.IngestStreamStatus.Restored,
      V1.IngestStreamStatus.Completed,
      V1.IngestStreamStatus.Terminated,
      V1.IngestStreamStatus.Failed,
    )

    val v2IngestStreamStatus: Gen[IngestStreamStatus] = Gen.oneOf(
      IngestStreamStatus.Running,
      IngestStreamStatus.Paused,
      IngestStreamStatus.Restored,
      IngestStreamStatus.Completed,
      IngestStreamStatus.Terminated,
      IngestStreamStatus.Failed,
    )

    val ratesSummary: Gen[RatesSummary] = for {
      count <- Arbitrary.arbitrary[Long]
      oneMinute <- Gen.posNum[Double]
      fiveMinute <- Gen.posNum[Double]
      fifteenMinute <- Gen.posNum[Double]
      overall <- Gen.posNum[Double]
    } yield RatesSummary(count, oneMinute, fiveMinute, fifteenMinute, overall)

    val ingestStreamStats: Gen[IngestStreamStats] = for {
      ingestedCount <- Arbitrary.arbitrary[Long]
      rates <- ratesSummary
      byteRates <- ratesSummary
      startTime <- TimeGenerators.Gens.instant
      totalRuntime <- Arbitrary.arbitrary[Long]
    } yield IngestStreamStats(ingestedCount, rates, byteRates, startTime, totalRuntime)

    val ingestStreamInfo: Gen[IngestStreamInfo] = for {
      status <- v2IngestStreamStatus
      message <- Gen.option(nonEmptyAlphaNumStr)
      settings <- ingestSource
      stats <- ingestStreamStats
    } yield IngestStreamInfo(status, message, settings, stats)

    val ingestStreamInfoWithName: Gen[IngestStreamInfoWithName] = for {
      name <- nonEmptyAlphaNumStr
      status <- v2IngestStreamStatus
      message <- Gen.option(nonEmptyAlphaNumStr)
      settings <- ingestSource
      stats <- ingestStreamStats
    } yield IngestStreamInfoWithName(name, status, message, settings, stats)

    val ingestFormat: Gen[IngestFormat] = Gen.oneOf(fileFormat, streamingFormat)

    val quineIngestConfiguration: Gen[QuineIngestConfiguration] = for {
      name <- nonEmptyAlphaNumStr
      source <- ingestSource
      query <- nonEmptyAlphaNumStr
      parameter <- Gen.alphaNumStr
      transformation <- Gen.option(transformation)
      parallelism <- smallPosNum
      maxPerSecond <- Gen.option(smallPosNum)
    } yield QuineIngestConfiguration(
      name = name,
      source = source,
      query = query,
      parameter = if (parameter.isEmpty) "that" else parameter,
      transformation = transformation,
      parallelism = parallelism,
      maxPerSecond = maxPerSecond,
    )

    val quineIngestStreamWithStatus: Gen[QuineIngestStreamWithStatus] = for {
      config <- quineIngestConfiguration
      status <- Gen.option(v1IngestStreamStatus)
    } yield QuineIngestStreamWithStatus(config, status)
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

    implicit val awsCredentials: Arbitrary[V1.AwsCredentials] = Arbitrary(Gens.awsCredentials)
    implicit val awsRegion: Arbitrary[V1.AwsRegion] = Arbitrary(Gens.awsRegion)
    implicit val kinesisIteratorType: Arbitrary[V1.KinesisIngest.IteratorType] = Arbitrary(Gens.kinesisIteratorType)

    implicit val transformation: Arbitrary[Transformation] = Arbitrary(Gens.transformation)

    implicit val fileIngest: Arbitrary[FileIngest] = Arbitrary(Gens.fileIngest)
    implicit val s3Ingest: Arbitrary[S3Ingest] = Arbitrary(Gens.s3Ingest)
    implicit val reactiveStreamIngest: Arbitrary[ReactiveStreamIngest] = Arbitrary(Gens.reactiveStreamIngest)
    implicit val webSocketFileUpload: Arbitrary[WebSocketFileUpload] = Arbitrary(Gens.webSocketFileUpload)
    implicit val stdInputIngest: Arbitrary[StdInputIngest] = Arbitrary(Gens.stdInputIngest)
    implicit val numberIteratorIngest: Arbitrary[NumberIteratorIngest] = Arbitrary(Gens.numberIteratorIngest)
    implicit val websocketIngest: Arbitrary[WebsocketIngest] = Arbitrary(Gens.websocketIngest)
    implicit val kinesisIngest: Arbitrary[KinesisIngest] = Arbitrary(Gens.kinesisIngest)
    implicit val kinesisKclIngest: Arbitrary[KinesisKclIngest] = Arbitrary(Gens.kinesisKclIngest)
    implicit val serverSentEventIngest: Arbitrary[ServerSentEventIngest] = Arbitrary(Gens.serverSentEventIngest)
    implicit val sqsIngest: Arbitrary[SQSIngest] = Arbitrary(Gens.sqsIngest)
    implicit val kafkaIngest: Arbitrary[KafkaIngest] = Arbitrary(Gens.kafkaIngest)
    implicit val ingestSource: Arbitrary[IngestSource] = Arbitrary(Gens.ingestSource)

    implicit val v1IngestStreamStatus: Arbitrary[V1.IngestStreamStatus] = Arbitrary(Gens.v1IngestStreamStatus)
    implicit val v2IngestStreamStatus: Arbitrary[IngestStreamStatus] = Arbitrary(Gens.v2IngestStreamStatus)
    implicit val ratesSummary: Arbitrary[RatesSummary] = Arbitrary(Gens.ratesSummary)
    implicit val ingestStreamStats: Arbitrary[IngestStreamStats] = Arbitrary(Gens.ingestStreamStats)
    implicit val ingestStreamInfo: Arbitrary[IngestStreamInfo] = Arbitrary(Gens.ingestStreamInfo)
    implicit val ingestStreamInfoWithName: Arbitrary[IngestStreamInfoWithName] =
      Arbitrary(Gens.ingestStreamInfoWithName)
    implicit val ingestFormat: Arbitrary[IngestFormat] = Arbitrary(Gens.ingestFormat)
    implicit val quineIngestConfiguration: Arbitrary[QuineIngestConfiguration] = Arbitrary(
      Gens.quineIngestConfiguration,
    )
    implicit val quineIngestStreamWithStatus: Arbitrary[QuineIngestStreamWithStatus] =
      Arbitrary(Gens.quineIngestStreamWithStatus)
  }
}
