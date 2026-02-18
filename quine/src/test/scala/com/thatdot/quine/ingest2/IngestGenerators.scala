package com.thatdot.quine.ingest2

import java.nio.charset.Charset

import scala.jdk.CollectionConverters._

import org.scalacheck.{Arbitrary, Gen}

import com.thatdot.api.v2.{AwsGenerators, SaslJaasConfigGenerators}
import com.thatdot.common.logging.Log.LogConfig
import com.thatdot.quine.ScalaPrimitiveGenerators
import com.thatdot.quine.ScalaPrimitiveGenerators.Gens.nonEmptyAlphaNumStr
import com.thatdot.quine.app.v2api.definitions.ingest2.ApiIngest.FileIngestMode.{NamedPipe, Regular}
import com.thatdot.quine.app.v2api.definitions.ingest2.ApiIngest.KafkaOffsetCommitting.ExplicitCommit
import com.thatdot.quine.app.v2api.definitions.ingest2.ApiIngest.WebSocketClient.KeepaliveProtocol
import com.thatdot.quine.app.v2api.definitions.ingest2.ApiIngest._
import com.thatdot.quine.app.v2api.definitions.ingest2.{DeadLetterQueueOutput, DeadLetterQueueSettings, OutputFormat}

object IngestGenerators {

  import AwsGenerators.Gens.{optAwsCredentials, optAwsRegion}
  import SaslJaasConfigGenerators.Gens.{optSaslJaasConfig, optSecret}
  import ScalaPrimitiveGenerators.Gens.{bool, unitInterval}

  object Gens {

    val charset: Gen[Charset] =
      Gen.oneOf[String](Charset.availableCharsets().keySet().asScala).map(Charset.forName)

    val keepAliveProtocol: Gen[KeepaliveProtocol] =
      Gen.oneOf[KeepaliveProtocol](
        Gen.posNum[Int].map(WebSocketClient.PingPongInterval(_)),
        Gen.zip(Gen.asciiPrintableStr, Gen.posNum[Int]).map { case (message, intervalMillis) =>
          WebSocketClient.SendMessageInterval(message, intervalMillis)
        },
        Gen.const(WebSocketClient.NoKeepalive),
      )

    val decoderSeq: Gen[Seq[RecordDecodingType]] =
      Gen.someOf(RecordDecodingType.Zlib, RecordDecodingType.Gzip, RecordDecodingType.Base64)

    val fileIngestMode: Gen[FileIngestMode] = Gen.oneOf(Regular, NamedPipe)

    val recordDecodingTypes: Gen[Seq[RecordDecodingType]] =
      Gen.containerOf[Seq, RecordDecodingType](
        Gen.oneOf(RecordDecodingType.Gzip, RecordDecodingType.Base64, RecordDecodingType.Zlib),
      )

    val optionSetStrings: Gen[Option[Set[String]]] = Gen.option(Gen.containerOfN[Set, String](3, Gen.asciiStr))
    val optionPosInt: Gen[Option[Int]] = Gen.option(Gen.posNum[Int])

    val kinesisIteratorType: Gen[IngestSource.Kinesis.IteratorType] =
      Gen.oneOf(
        Gen.const(IngestSource.Kinesis.IteratorType.Latest),
        Gen.const(IngestSource.Kinesis.IteratorType.TrimHorizon),
        Gen.numStr.map(IngestSource.Kinesis.IteratorType.AtSequenceNumber(_)),
        Gen.numStr.map(IngestSource.Kinesis.IteratorType.AfterSequenceNumber(_)),
        Gen.posNum[Long].map(IngestSource.Kinesis.IteratorType.AtTimestamp(_)),
      )

    val kafkaOffsetCommitting: Gen[KafkaOffsetCommitting] = for {
      maxBatch <- Gen.posNum[Long]
      maxIntervalMillis <- Gen.posNum[Int]
      parallelism <- Gen.posNum[Int]
      waitForCommitConfirmation <- bool
    } yield ExplicitCommit(maxBatch, maxIntervalMillis, parallelism, waitForCommitConfirmation)

    val kafkaSecurityProtocol: Gen[KafkaSecurityProtocol] = Gen.oneOf(
      KafkaSecurityProtocol.PlainText,
      KafkaSecurityProtocol.Ssl,
      KafkaSecurityProtocol.Sasl_Ssl,
      KafkaSecurityProtocol.Sasl_Plaintext,
    )

    val kafkaAutoOffsetReset: Gen[KafkaAutoOffsetReset] = Gen.oneOf(KafkaAutoOffsetReset.values)

    val fileFormat: Gen[IngestFormat.FileFormat] = Gen.oneOf(
      IngestFormat.FileFormat.JsonL,
      IngestFormat.FileFormat.Line,
      IngestFormat.FileFormat.CSV(),
      IngestFormat.FileFormat.CSV(Left(true)),
      IngestFormat.FileFormat.CSV(Right(List("A", "N", "C"))),
    )

    val streamingFormat: Gen[IngestFormat.StreamingFormat] = Gen.oneOf(
      IngestFormat.StreamingFormat.Json,
      IngestFormat.StreamingFormat.Raw,
      IngestFormat.StreamingFormat.Drop,
      IngestFormat.StreamingFormat.Protobuf("url", "typeName"),
      IngestFormat.StreamingFormat.Avro("url"),
    )

    val recordRetrySettings: Gen[RecordRetrySettings] = for {
      minBackoff <- Gen.posNum[Int]
      maxBackoff <- Gen.posNum[Int]
      randomFactor <- unitInterval
      maxRetries <- Gen.posNum[Int]
    } yield RecordRetrySettings(minBackoff, maxBackoff, randomFactor, maxRetries)

    val kinesisCheckpointSettings: Gen[KinesisCheckpointSettings] = for {
      disableCheckpointing <- bool
      maxBatchSize <- Gen.option(Gen.posNum[Int])
      maxBatchWaitMillis <- Gen.option(Gen.posNum[Long])
    } yield KinesisCheckpointSettings(disableCheckpointing, maxBatchSize, maxBatchWaitMillis)

    val kinesisSchedulerSourceSettings: Gen[KinesisSchedulerSourceSettings] = for {
      bufferSize <- Gen.option(Gen.posNum[Int])
      backpressureTimeoutMillis <- Gen.option(Gen.posNum[Long])
    } yield KinesisSchedulerSourceSettings(bufferSize, backpressureTimeoutMillis)

    val configsBuilder: Gen[ConfigsBuilder] = Gen.const(ConfigsBuilder(None, None))

    val leaseManagementConfig: Gen[LeaseManagementConfig] =
      Gen.const(
        LeaseManagementConfig(
          failoverTimeMillis = None,
          shardSyncIntervalMillis = None,
          cleanupLeasesUponShardCompletion = None,
          ignoreUnexpectedChildShards = None,
          maxLeasesForWorker = None,
          maxLeaseRenewalThreads = None,
          billingMode = None,
          initialLeaseTableReadCapacity = None,
          initialLeaseTableWriteCapacity = None,
          reBalanceThresholdPercentage = None,
          dampeningPercentage = None,
          allowThroughputOvershoot = None,
          disableWorkerMetrics = None,
          maxThroughputPerHostKBps = None,
          isGracefulLeaseHandoffEnabled = None,
          gracefulLeaseHandoffTimeoutMillis = None,
        ),
      )

    val retrievalSpecificConfig: Gen[RetrievalSpecificConfig] =
      Gen.const(RetrievalSpecificConfig.PollingConfig(None, None, None, None))

    val processorConfig: Gen[ProcessorConfig] = Gen.const(ProcessorConfig(None))

    val coordinatorConfig: Gen[CoordinatorConfig] =
      Gen.const(CoordinatorConfig(None, None, None, None))

    val lifecycleConfig: Gen[LifecycleConfig] = Gen.const(LifecycleConfig(None, None))

    val retrievalConfig: Gen[RetrievalConfig] = Gen.const(RetrievalConfig(None, None))

    val metricsConfig: Gen[MetricsConfig] = Gen.const(MetricsConfig(None, None, None, None))

    val kclConfiguration: Gen[KCLConfiguration] = for {
      configsBuilder <- Gen.option(Gens.configsBuilder)
      leaseManagementConfig <- Gen.option(Gens.leaseManagementConfig)
      retrievalSpecificConfig <- Gen.option(Gens.retrievalSpecificConfig)
      processorConfig <- Gen.option(Gens.processorConfig)
      coordinatorConfig <- Gen.option(Gens.coordinatorConfig)
      lifecycleConfig <- Gen.option(Gens.lifecycleConfig)
      retrievalConfig <- Gen.option(Gens.retrievalConfig)
      metricsConfig <- Gen.option(Gens.metricsConfig)
    } yield KCLConfiguration(
      configsBuilder,
      leaseManagementConfig,
      retrievalSpecificConfig,
      processorConfig,
      coordinatorConfig,
      lifecycleConfig,
      retrievalConfig,
      metricsConfig,
    )

    val onRecordErrorHandler: Gen[OnRecordErrorHandler] = Gen.const(OnRecordErrorHandler())

    val onStreamErrorHandler: Gen[OnStreamErrorHandler] = Gen.oneOf(LogStreamError, RetryStreamError(1))

    val deadLetterQueueOutputFile: Gen[DeadLetterQueueOutput.File] =
      nonEmptyAlphaNumStr.map(DeadLetterQueueOutput.File(_))

    val outputFormatJSON: Gen[OutputFormat.JSON] = for {
      withInfoEnvelope <- bool
    } yield OutputFormat.JSON(withInfoEnvelope)

    val deadLetterQueueOutputKafka: Gen[DeadLetterQueueOutput.Kafka] = for {
      topic <- nonEmptyAlphaNumStr
      bootstrapServers <- nonEmptyAlphaNumStr.map(s => s"localhost:9092,$s:9092")
      sslKeystorePassword <- optSecret
      sslTruststorePassword <- optSecret
      sslKeyPassword <- optSecret
      saslJaasConfig <- optSaslJaasConfig
      outputFormat <- outputFormatJSON
    } yield DeadLetterQueueOutput.Kafka(
      topic = topic,
      bootstrapServers = bootstrapServers,
      sslKeystorePassword = sslKeystorePassword,
      sslTruststorePassword = sslTruststorePassword,
      sslKeyPassword = sslKeyPassword,
      saslJaasConfig = saslJaasConfig,
      outputFormat = outputFormat,
    )

    val deadLetterQueueOutput: Gen[DeadLetterQueueOutput] =
      Gen.oneOf(deadLetterQueueOutputFile, deadLetterQueueOutputKafka)

    val deadLetterQueueSettings: Gen[DeadLetterQueueSettings] = for {
      destinations <- Gen.listOf(deadLetterQueueOutput)
    } yield DeadLetterQueueSettings(destinations)

    val file: Gen[IngestSource.File] = for {
      format <- fileFormat
      path <- Gen.asciiPrintableStr
      fileIngestMode <- Gen.option(Gens.fileIngestMode)
      maximumLineSize <- Gen.option(Gen.posNum[Int])
      startOffset <- Gen.posNum[Long]
      limit <- Gen.option(Gen.posNum[Long])
      characterEncoding <- charset
      recordDecoders <- recordDecodingTypes
    } yield IngestSource.File(
      format,
      path,
      fileIngestMode,
      maximumLineSize,
      startOffset,
      limit,
      characterEncoding,
      recordDecoders,
    )

    val s3: Gen[IngestSource.S3] = for {
      format <- fileFormat
      bucket <- Gen.asciiPrintableStr
      key <- Gen.asciiPrintableStr
      credentials <- optAwsCredentials
      maximumLineSize <- Gen.option(Gen.posNum[Int])
      startOffset <- Gen.posNum[Long]
      limit <- Gen.option(Gen.posNum[Long])
      characterEncoding <- charset
      recordDecoders <- recordDecodingTypes
    } yield IngestSource.S3(
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

    val stdInput: Gen[IngestSource.StdInput] = for {
      format <- fileFormat
      maximumLineSize <- Gen.option(Gen.posNum[Int])
      characterEncoding <- charset
    } yield IngestSource.StdInput(format, maximumLineSize, characterEncoding)

    val numberIterator: Gen[IngestSource.NumberIterator] = for {
      startOffset <- Gen.posNum[Long]
      limit <- Gen.option(Gen.posNum[Long])
    } yield IngestSource.NumberIterator(startOffset, limit)

    val websocketClient: Gen[IngestSource.WebsocketClient] = for {
      format <- streamingFormat
      url <- Gen.asciiPrintableStr
      initMessages <- Gen.listOf(Gen.asciiPrintableStr)
      keepAlive <- keepAliveProtocol
      characterEncoding <- charset
    } yield IngestSource.WebsocketClient(format, url, initMessages, keepAlive, characterEncoding)

    val serverSentEvent: Gen[IngestSource.ServerSentEvent] = for {
      format <- streamingFormat
      url <- Gen.asciiPrintableStr
      recordDecoders <- recordDecodingTypes
    } yield IngestSource.ServerSentEvent(format, url, recordDecoders)

    val sqs: Gen[IngestSource.SQS] = for {
      format <- streamingFormat
      queueUrl <- Gen.asciiPrintableStr
      readParallelism <- Gen.posNum[Int]
      credentials <- optAwsCredentials
      region <- optAwsRegion
      deleteReadMessages <- bool
      recordDecoders <- recordDecodingTypes
    } yield IngestSource.SQS(format, queueUrl, readParallelism, credentials, region, deleteReadMessages, recordDecoders)

    val kinesis: Gen[IngestSource.Kinesis] = for {
      format <- streamingFormat
      streamName <- Gen.asciiPrintableStr
      shardIds <- optionSetStrings
      credentials <- optAwsCredentials
      region <- optAwsRegion
      iteratorType <- kinesisIteratorType
      numRetries <- Gen.posNum[Int]
      recordDecoders <- recordDecodingTypes
    } yield IngestSource.Kinesis(
      format,
      streamName,
      shardIds,
      credentials,
      region,
      iteratorType,
      numRetries,
      recordDecoders,
    )

    val kafka: Gen[IngestSource.Kafka] = for {
      format <- streamingFormat
      topics <- Gen.either(
        Gen.containerOfN[Set, String](2, Gen.asciiPrintableStr),
        Gen.const(Map.empty[String, Set[Int]]),
      )
      bootstrapServers <- Gen.asciiPrintableStr
      groupId <- Gen.option(Gen.asciiPrintableStr)
      securityProtocol <- kafkaSecurityProtocol
      offsetCommitting <- Gen.option(kafkaOffsetCommitting)
      autoOffsetReset <- kafkaAutoOffsetReset
      sslKeystorePassword <- optSecret
      sslTruststorePassword <- optSecret
      sslKeyPassword <- optSecret
      saslJaasConfig <- optSaslJaasConfig
      kafkaProperties <- Gen.const(Map.empty[String, String])
      endingOffset <- Gen.option(Gen.posNum[Long])
      recordDecoders <- recordDecodingTypes
    } yield IngestSource.Kafka(
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

    val ingestSource: Gen[IngestSource] =
      Gen.oneOf(file, s3, stdInput, numberIterator, websocketClient, serverSentEvent, sqs, kinesis, kafka)

    val quineIngestConfiguration: Gen[Oss.QuineIngestConfiguration] = for {
      initChar <- Gen.alphaChar
      nameTail <- Gen.alphaNumStr
      name = s"$initChar$nameTail"
      source <- ingestSource
    } yield Oss.QuineIngestConfiguration(name, source, "CREATE ($that)")

    val ingestStreamStatus: Gen[IngestStreamStatus] = Gen.oneOf(
      Gen.const(IngestStreamStatus.Running),
      Gen.const(IngestStreamStatus.Paused),
      Gen.const(IngestStreamStatus.Restored),
      Gen.const(IngestStreamStatus.Completed),
      Gen.const(IngestStreamStatus.Terminated),
      Gen.const(IngestStreamStatus.Failed),
    )
  }

  object Arbs {
    implicit val arbCharset: Arbitrary[Charset] = Arbitrary(Gens.charset)
    implicit val arbKeepAliveProtocol: Arbitrary[WebSocketClient.KeepaliveProtocol] = Arbitrary(Gens.keepAliveProtocol)
    implicit val arbFileIngestMode: Arbitrary[FileIngestMode] = Arbitrary(Gens.fileIngestMode)
    implicit val arbRecordDecodingTypes: Arbitrary[Seq[RecordDecodingType]] = Arbitrary(Gens.recordDecodingTypes)
    implicit val arbKinesisIteratorType: Arbitrary[IngestSource.Kinesis.IteratorType] =
      Arbitrary(Gens.kinesisIteratorType)
    implicit val arbKafkaOffsetCommitting: Arbitrary[KafkaOffsetCommitting] = Arbitrary(Gens.kafkaOffsetCommitting)
    implicit val arbKafkaSecurityProtocol: Arbitrary[KafkaSecurityProtocol] = Arbitrary(Gens.kafkaSecurityProtocol)
    implicit val arbKafkaAutoOffsetReset: Arbitrary[KafkaAutoOffsetReset] = Arbitrary(Gens.kafkaAutoOffsetReset)
    implicit val arbFileFormat: Arbitrary[IngestFormat.FileFormat] = Arbitrary(Gens.fileFormat)
    implicit val arbStreamingFormat: Arbitrary[IngestFormat.StreamingFormat] = Arbitrary(Gens.streamingFormat)
    implicit val arbRecordRetrySettings: Arbitrary[RecordRetrySettings] = Arbitrary(Gens.recordRetrySettings)
    implicit val arbKinesisCheckpointSettings: Arbitrary[KinesisCheckpointSettings] =
      Arbitrary(Gens.kinesisCheckpointSettings)
    implicit val arbKinesisSchedulerSourceSettings: Arbitrary[KinesisSchedulerSourceSettings] =
      Arbitrary(Gens.kinesisSchedulerSourceSettings)
    implicit val arbConfigsBuilder: Arbitrary[ConfigsBuilder] = Arbitrary(Gens.configsBuilder)
    implicit val arbLeaseManagementConfig: Arbitrary[LeaseManagementConfig] = Arbitrary(Gens.leaseManagementConfig)
    implicit val arbRetrievalSpecificConfig: Arbitrary[RetrievalSpecificConfig] =
      Arbitrary(Gens.retrievalSpecificConfig)
    implicit val arbProcessorConfig: Arbitrary[ProcessorConfig] = Arbitrary(Gens.processorConfig)
    implicit val arbCoordinatorConfig: Arbitrary[CoordinatorConfig] = Arbitrary(Gens.coordinatorConfig)
    implicit val arbLifecycleConfig: Arbitrary[LifecycleConfig] = Arbitrary(Gens.lifecycleConfig)
    implicit val arbRetrievalConfig: Arbitrary[RetrievalConfig] = Arbitrary(Gens.retrievalConfig)
    implicit val arbMetricsConfig: Arbitrary[MetricsConfig] = Arbitrary(Gens.metricsConfig)
    implicit val arbKCLConfiguration: Arbitrary[KCLConfiguration] = Arbitrary(Gens.kclConfiguration)
    implicit val arbOnRecordErrorHandler: Arbitrary[OnRecordErrorHandler] = Arbitrary(Gens.onRecordErrorHandler)
    implicit val arbOnStreamErrorHandler: Arbitrary[OnStreamErrorHandler] = Arbitrary(Gens.onStreamErrorHandler)
    implicit val arbDeadLetterQueueOutput: Arbitrary[DeadLetterQueueOutput] = Arbitrary(Gens.deadLetterQueueOutput)
    implicit val arbDeadLetterQueueSettings: Arbitrary[DeadLetterQueueSettings] =
      Arbitrary(Gens.deadLetterQueueSettings)
    implicit val arbOutputFormatJSON: Arbitrary[OutputFormat.JSON] = Arbitrary(Gens.outputFormatJSON)
    implicit val arbFile: Arbitrary[IngestSource.File] = Arbitrary(Gens.file)
    implicit val arbS3: Arbitrary[IngestSource.S3] = Arbitrary(Gens.s3)
    implicit val arbStdInput: Arbitrary[IngestSource.StdInput] = Arbitrary(Gens.stdInput)
    implicit val arbNumberIterator: Arbitrary[IngestSource.NumberIterator] = Arbitrary(Gens.numberIterator)
    implicit val arbWebsocketClient: Arbitrary[IngestSource.WebsocketClient] = Arbitrary(Gens.websocketClient)
    implicit val arbServerSentEvent: Arbitrary[IngestSource.ServerSentEvent] = Arbitrary(Gens.serverSentEvent)
    implicit val arbSQS: Arbitrary[IngestSource.SQS] = Arbitrary(Gens.sqs)
    implicit val arbKinesis: Arbitrary[IngestSource.Kinesis] = Arbitrary(Gens.kinesis)
    implicit val arbKafka: Arbitrary[IngestSource.Kafka] = Arbitrary(Gens.kafka)
    implicit val arbIngestSource: Arbitrary[IngestSource] = Arbitrary(Gens.ingestSource)
    implicit val arbQuineIngestConfiguration: Arbitrary[Oss.QuineIngestConfiguration] =
      Arbitrary(Gens.quineIngestConfiguration)
    implicit val arbIngestStreamStatus: Arbitrary[IngestStreamStatus] = Arbitrary(Gens.ingestStreamStatus)

    implicit def logConfig: LogConfig = LogConfig.permissive
  }
}
