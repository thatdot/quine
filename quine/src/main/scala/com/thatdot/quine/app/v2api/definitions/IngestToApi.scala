package com.thatdot.quine.app.v2api.definitions

import com.thatdot.quine.app.ingest2.{V2IngestEntities => Ingest}
import com.thatdot.quine.app.v2api.definitions.{ApiIngest => Api}
import com.thatdot.quine.{routes => V1}

object IngestToApi {

  //For conversions to API methods that we may not be able to define in quine OSS, but we also
  // don't want inside a non-sealed base type
  trait ToApiMethod[A, B] {
    def apply(a: A): B
  }

  object OssConversions {

    implicit val quineIngestConfigurationToApi
      : ToApiMethod[Ingest.QuineIngestConfiguration, Api.Oss.QuineIngestConfiguration] =
      new ToApiMethod[Ingest.QuineIngestConfiguration, Api.Oss.QuineIngestConfiguration] {
        override def apply(a: Ingest.QuineIngestConfiguration): Api.Oss.QuineIngestConfiguration =
          IngestToApi.apply(a)
      }
  }
  def apply(status: V1.IngestStreamStatus): Api.IngestStreamStatus = status match {
    case V1.IngestStreamStatus.Completed => Api.IngestStreamStatus.Completed
    case V1.IngestStreamStatus.Terminated => Api.IngestStreamStatus.Terminated
    case V1.IngestStreamStatus.Failed => Api.IngestStreamStatus.Failed
    case V1.IngestStreamStatus.Running => Api.IngestStreamStatus.Running
    case V1.IngestStreamStatus.Paused => Api.IngestStreamStatus.Paused
    case V1.IngestStreamStatus.Restored => Api.IngestStreamStatus.Restored
  }

  def apply(rates: V1.RatesSummary): Api.RatesSummary =
    Api.RatesSummary(
      rates.count,
      rates.oneMinute,
      rates.fiveMinute,
      rates.fifteenMinute,
      rates.overall,
    )
  def apply(stats: V1.IngestStreamStats): Api.IngestStreamStats =
    Api.IngestStreamStats(
      stats.ingestedCount,
      IngestToApi(stats.rates),
      IngestToApi(stats.byteRates),
      stats.startTime,
      stats.totalRuntime,
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
  def apply(format: Ingest.FileFormat): Api.FileFormat = format match {
    case Ingest.FileFormat.LineFormat => Api.FileFormat.LineFormat
    case Ingest.FileFormat.JsonFormat => Api.FileFormat.JsonFormat
    case Ingest.FileFormat.CsvFormat(headers, delimiter, quoteChar, escapeChar) =>
      Api.FileFormat.CsvFormat(headers, IngestToApi(delimiter), IngestToApi(quoteChar), IngestToApi(escapeChar))
  }

  def apply(format: Ingest.StreamingFormat): Api.StreamingFormat = format match {
    case Ingest.StreamingFormat.JsonFormat => Api.StreamingFormat.JsonFormat
    case Ingest.StreamingFormat.RawFormat => Api.StreamingFormat.RawFormat
    case Ingest.StreamingFormat.ProtobufFormat(schemaUrl, typeName) =>
      Api.StreamingFormat.ProtobufFormat(schemaUrl, typeName)
    case Ingest.StreamingFormat.AvroFormat(schemaUrl) => Api.StreamingFormat.AvroFormat(schemaUrl)
    case Ingest.StreamingFormat.DropFormat => Api.StreamingFormat.DropFormat
  }
  def apply(
    proto: V1.WebsocketSimpleStartupIngest.KeepaliveProtocol,
  ): Api.WebsocketSimpleStartupIngest.KeepaliveProtocol = proto match {
    case V1.WebsocketSimpleStartupIngest.PingPongInterval(intervalMillis) =>
      Api.WebsocketSimpleStartupIngest.PingPongInterval(intervalMillis)
    case V1.WebsocketSimpleStartupIngest.SendMessageInterval(message, intervalMillis) =>
      Api.WebsocketSimpleStartupIngest.SendMessageInterval(message, intervalMillis)
    case V1.WebsocketSimpleStartupIngest.NoKeepalive => Api.WebsocketSimpleStartupIngest.NoKeepalive
  }
  def apply(ingest: Ingest.InitialPosition): Api.InitialPosition = ingest match {
    case Ingest.Latest => Api.Latest
    case Ingest.TrimHorizon => Api.TrimHorizon
    case Ingest.AtTimestamp(year, month, date, hourOfDay, minute, second) =>
      Api.AtTimestamp(year, month, date, hourOfDay, minute, second)
  }
  def apply(it: V1.KinesisIngest.IteratorType): Api.KinesisIngest.IteratorType = it match {
    case V1.KinesisIngest.IteratorType.Latest => Api.KinesisIngest.IteratorType.Latest
    case V1.KinesisIngest.IteratorType.TrimHorizon => Api.KinesisIngest.IteratorType.TrimHorizon
    case V1.KinesisIngest.IteratorType.AtSequenceNumber(sequenceNumber) =>
      Api.KinesisIngest.IteratorType.AtSequenceNumber(sequenceNumber)
    case V1.KinesisIngest.IteratorType.AfterSequenceNumber(sequenceNumber) =>
      Api.KinesisIngest.IteratorType.AfterSequenceNumber(sequenceNumber)
    case V1.KinesisIngest.IteratorType.AtTimestamp(millisSinceEpoch) =>
      Api.KinesisIngest.IteratorType.AtTimestamp(millisSinceEpoch)
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
  def apply(c: V1.AwsCredentials): Api.AwsCredentials =
    Api.AwsCredentials(c.accessKeyId, c.secretAccessKey)

  def apply(r: V1.AwsRegion): Api.AwsRegion =
    Api.AwsRegion(r.region)

  def apply(c: V1.KafkaOffsetCommitting): Api.KafkaOffsetCommitting = c match {
    case V1.KafkaOffsetCommitting.ExplicitCommit(maxBatch, maxIntervalMillis, parallelism, waitForCommitConfirmation) =>
      Api.KafkaOffsetCommitting.ExplicitCommit(maxBatch, maxIntervalMillis, parallelism, waitForCommitConfirmation)
  }
  def apply(source: Ingest.IngestSource): Api.IngestSource = source match {
    case Ingest.FileIngest(
          format,
          path,
          ingestMode,
          maximumLineSize,
          startOffset,
          limit,
          characterEncoding,
          recordDecoders,
        ) =>
      Api.FileIngest(
        IngestToApi(format),
        path,
        ingestMode.map(IngestToApi.apply),
        maximumLineSize,
        startOffset,
        limit,
        characterEncoding,
        recordDecoders.map(IngestToApi.apply),
      )
    case Ingest.S3Ingest(
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
      Api.S3Ingest(
        IngestToApi(format),
        bucket,
        key,
        credentials.map(IngestToApi.apply),
        maximumLineSize,
        startOffset,
        limit,
        characterEncoding,
        recordDecoders.map(IngestToApi.apply),
      )
    case Ingest.StdInputIngest(format, maximumLineSize, characterEncoding) =>
      Api.StdInputIngest(
        IngestToApi(format),
        maximumLineSize,
        characterEncoding,
      )
    case Ingest.NumberIteratorIngest(_, startOffset, limit) =>
      Api.NumberIteratorIngest(
        startOffset,
        limit,
      )
    case Ingest.WebsocketIngest(format, url, initMessages, keepAlive, characterEncoding) =>
      Api.WebsocketIngest(
        IngestToApi(format),
        url,
        initMessages,
        IngestToApi(keepAlive),
        characterEncoding,
      )
    case Ingest.KinesisIngest(
          format,
          streamName,
          shardIds,
          credentials,
          region,
          iteratorType,
          numRetries,
          recordDecoders,
        ) =>
      Api.KinesisIngest(
        IngestToApi(format),
        streamName,
        shardIds,
        credentials.map(IngestToApi.apply),
        region.map(IngestToApi.apply),
        IngestToApi(iteratorType),
        numRetries,
        recordDecoders.map(IngestToApi.apply),
      )

    case Ingest.KinesisKclIngest(
          name,
          applicationName,
          streamName,
          format,
          initialPosition,
          credentialsOpt,
          regionOpt,
          numRetries,
          maxBatchSize,
          backpressureTimeoutMillis,
          recordDecoders,
          checkpointSettings,
        ) =>
      Api.KinesisKclIngest(
        name,
        applicationName,
        streamName,
        IngestToApi(format),
        credentialsOpt.map(IngestToApi.apply),
        regionOpt.map(IngestToApi.apply),
        IngestToApi(initialPosition),
        numRetries,
        maxBatchSize,
        backpressureTimeoutMillis,
        recordDecoders.map(IngestToApi.apply),
        checkpointSettings,
      )
    case Ingest.ServerSentEventIngest(format, url, recordDecoders) =>
      Api.ServerSentEventIngest(
        IngestToApi(format),
        url,
        recordDecoders.map(IngestToApi.apply),
      )
    case Ingest.SQSIngest(format, queueUrl, readParallelism, credentials, region, deleteReadMessages, recordDecoders) =>
      Api.SQSIngest(
        IngestToApi(format),
        queueUrl,
        readParallelism,
        credentials.map(IngestToApi.apply),
        region.map(IngestToApi.apply),
        deleteReadMessages,
        recordDecoders.map(IngestToApi.apply),
      )
    case Ingest.KafkaIngest(
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
      Api.KafkaIngest(
        IngestToApi(format),
        topics,
        bootstrapServers,
        groupId,
        IngestToApi(protocol),
        offsetCommitting.map(IngestToApi.apply),
        IngestToApi(autoOffsetReset),
        kafkaProperties,
        endingOffset,
        recordDecoders.map(IngestToApi.apply),
      )
    case Ingest.ReactiveStreamIngest(format, url, port) => Api.ReactiveStream(IngestToApi(format), url, port)
  }

  def apply(handler: Ingest.OnStreamErrorHandler): Api.OnStreamErrorHandler = handler match {
    case Ingest.RetryStreamError(retryCount) => Api.RetryStreamError(retryCount)
    case Ingest.LogStreamError => Api.LogStreamError
  }

  def apply(handler: Ingest.OnRecordErrorHandler): Api.OnRecordErrorHandler = handler match {
    case Ingest.LogRecordErrorHandler => Api.LogRecordErrorHandler
    case Ingest.DeadLetterErrorHandler => Api.DeadLetterErrorHandler
  }
  def apply(conf: Ingest.QuineIngestConfiguration): Api.Oss.QuineIngestConfiguration =
    Api.Oss.QuineIngestConfiguration(
      IngestToApi(conf.source),
      conf.query,
      conf.parameter,
      conf.parallelism,
      conf.maxPerSecond,
      IngestToApi(conf.onRecordError),
      IngestToApi(conf.onStreamError),
    )
}
