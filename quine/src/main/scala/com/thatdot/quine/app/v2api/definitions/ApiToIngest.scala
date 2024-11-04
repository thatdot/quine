package com.thatdot.quine.app.v2api.definitions

import com.thatdot.quine.app.ingest2.{V2IngestEntities, V2IngestEntities => Ingest}
import com.thatdot.quine.app.v2api.definitions.{ApiIngest => Api}
import com.thatdot.quine.{routes => V1}

object ApiToIngest {

  //For conversions from API methods that we may not be able to define in quine OSS, but we also
  // don't want to bake into a non-sealed base type
  trait OfApiMethod[A, B] {
    def apply(b: B): A
  }

  object OssConversions {

    implicit val quineIngestConfigurationOfApi
      : OfApiMethod[Ingest.QuineIngestConfiguration, Api.Oss.QuineIngestConfiguration] =
      new OfApiMethod[Ingest.QuineIngestConfiguration, Api.Oss.QuineIngestConfiguration] {
        override def apply(b: Api.Oss.QuineIngestConfiguration): V2IngestEntities.QuineIngestConfiguration =
          ApiToIngest.apply(b)
      }
  }

  def apply(rates: Api.RatesSummary): V1.RatesSummary =
    V1.RatesSummary(rates.count, rates.oneMinute, rates.fiveMinute, rates.fifteenMinute, rates.overall)

  def apply(stats: Api.IngestStreamStats): V1.IngestStreamStats =
    V1.IngestStreamStats(
      stats.ingestedCount,
      ApiToIngest(stats.rates),
      ApiToIngest(stats.byteRates),
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

  def apply(format: Api.FileFormat): Ingest.FileFormat = format match {
    case Api.FileFormat.LineFormat => Ingest.FileFormat.LineFormat
    case Api.FileFormat.JsonFormat => Ingest.FileFormat.JsonFormat
    case Api.FileFormat.CsvFormat(headers, delimiter, quoteChar, escapeChar) =>
      Ingest.FileFormat.CsvFormat(headers, ApiToIngest(delimiter), ApiToIngest(quoteChar), ApiToIngest(escapeChar))
  }
  def apply(format: Api.StreamingFormat): Ingest.StreamingFormat = format match {
    case Api.StreamingFormat.JsonFormat => Ingest.StreamingFormat.JsonFormat
    case Api.StreamingFormat.RawFormat => Ingest.StreamingFormat.RawFormat
    case Api.StreamingFormat.ProtobufFormat(schemaUrl, typeName) =>
      Ingest.StreamingFormat.ProtobufFormat(schemaUrl, typeName)
    case Api.StreamingFormat.AvroFormat(schemaUrl) => Ingest.StreamingFormat.AvroFormat(schemaUrl)
    case Api.StreamingFormat.DropFormat => Ingest.StreamingFormat.DropFormat
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
    ingest: Api.WebsocketSimpleStartupIngest.KeepaliveProtocol,
  ): V1.WebsocketSimpleStartupIngest.KeepaliveProtocol = ingest match {
    case Api.WebsocketSimpleStartupIngest.PingPongInterval(intervalMillis) =>
      V1.WebsocketSimpleStartupIngest.PingPongInterval(intervalMillis)
    case Api.WebsocketSimpleStartupIngest.SendMessageInterval(message, intervalMillis) =>
      V1.WebsocketSimpleStartupIngest.SendMessageInterval(message, intervalMillis)
    case Api.WebsocketSimpleStartupIngest.NoKeepalive => V1.WebsocketSimpleStartupIngest.NoKeepalive
  }
  def apply(cred: Api.AwsCredentials): V1.AwsCredentials = V1.AwsCredentials(cred.accessKeyId, cred.secretAccessKey)
  def apply(region: Api.AwsRegion): V1.AwsRegion = V1.AwsRegion(region.region)
  def apply(ingest: Api.KinesisIngest.IteratorType): V1.KinesisIngest.IteratorType = ingest match {
    case Api.KinesisIngest.IteratorType.Latest => V1.KinesisIngest.IteratorType.Latest
    case Api.KinesisIngest.IteratorType.TrimHorizon => V1.KinesisIngest.IteratorType.TrimHorizon
    case Api.KinesisIngest.IteratorType.AtSequenceNumber(sequenceNumber) =>
      V1.KinesisIngest.IteratorType.AtSequenceNumber(sequenceNumber)
    case Api.KinesisIngest.IteratorType.AfterSequenceNumber(sequenceNumber) =>
      V1.KinesisIngest.IteratorType.AtSequenceNumber(sequenceNumber)
    case Api.KinesisIngest.IteratorType.AtTimestamp(millisSinceEpoch) =>
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
  def apply(src: Api.IngestSource): Ingest.IngestSource = src match {
    case src: ApiIngest.FileIngest =>
      Ingest.FileIngest(
        ApiToIngest(src.format),
        src.path,
        src.fileIngestMode.map(ApiToIngest.apply),
        src.maximumLineSize,
        src.startOffset,
        src.limit,
        src.characterEncoding,
        src.recordDecoders.map(ApiToIngest.apply),
      )
    case src: ApiIngest.StdInputIngest =>
      Ingest.StdInputIngest(
        ApiToIngest(src.format),
        src.maximumLineSize,
        src.characterEncoding,
      )
    case src: ApiIngest.NumberIteratorIngest =>
      Ingest.NumberIteratorIngest(
        Ingest.StreamingFormat.RawFormat,
        src.startOffset,
        src.limit,
      )
    case src: ApiIngest.WebsocketIngest =>
      Ingest.WebsocketIngest(
        ApiToIngest(src.format),
        src.url,
        src.initMessages,
        ApiToIngest(src.keepAlive),
        src.characterEncoding,
      )
    case src: ApiIngest.KinesisIngest =>
      Ingest.KinesisIngest(
        ApiToIngest(src.format),
        src.streamName,
        src.shardIds,
        src.credentials.map(ApiToIngest.apply),
        src.region.map(ApiToIngest.apply),
        ApiToIngest(src.iteratorType),
        src.numRetries,
        src.recordDecoders.map(ApiToIngest.apply),
      )
    case src: ApiIngest.ServerSentEventIngest =>
      Ingest.ServerSentEventIngest(
        ApiToIngest(src.format),
        src.url,
        src.recordDecoders.map(ApiToIngest.apply),
      )
    case src: ApiIngest.SQSIngest =>
      Ingest.SQSIngest(
        ApiToIngest(src.format),
        src.queueUrl,
        src.readParallelism,
        src.credentials.map(ApiToIngest.apply),
        src.region.map(ApiToIngest.apply),
        src.deleteReadMessages,
        src.recordDecoders.map(ApiToIngest.apply),
      )
    case src: ApiIngest.KafkaIngest =>
      Ingest.KafkaIngest(
        ApiToIngest(src.format),
        src.topics,
        src.bootstrapServers,
        src.groupId,
        ApiToIngest(src.securityProtocol),
        src.offsetCommitting.map(ApiToIngest.apply),
        ApiToIngest(src.autoOffsetReset),
        src.kafkaProperties,
        src.endingOffset,
        src.recordDecoders.map(ApiToIngest.apply),
      )
    case src: Api.S3Ingest =>
      Ingest.S3Ingest(
        ApiToIngest(src.format),
        src.bucket,
        src.key,
        src.credentials.map(ApiToIngest.apply),
        src.maximumLineSize,
        src.startOffset,
        src.limit,
        src.characterEncoding,
        src.recordDecoders.map(ApiToIngest.apply),
      )
  }
  def apply(handler: Api.OnRecordErrorHandler): Ingest.OnRecordErrorHandler = handler match {
    case ApiIngest.LogRecordErrorHandler => Ingest.LogRecordErrorHandler
    case ApiIngest.DeadLetterErrorHandler => Ingest.DeadLetterErrorHandler
  }
  def apply(handler: Api.OnStreamErrorHandler): Ingest.OnStreamErrorHandler = handler match {
    case ApiIngest.RetryStreamError(retryCount) => Ingest.RetryStreamError(retryCount)
    case ApiIngest.LogStreamError => Ingest.LogStreamError
  }
  def apply(conf: Api.Oss.QuineIngestConfiguration): Ingest.QuineIngestConfiguration =
    Ingest.QuineIngestConfiguration(
      ApiToIngest(conf.source),
      conf.query,
      conf.parameter,
      conf.parallelism,
      conf.maxPerSecond,
      ApiToIngest(conf.onRecordError),
      ApiToIngest(conf.onStreamError),
    )

  def apply(status: V1.IngestStreamStatus): Api.IngestStreamStatus = status match {
    case V1.IngestStreamStatus.Completed => Api.IngestStreamStatus.Completed
    case V1.IngestStreamStatus.Terminated => Api.IngestStreamStatus.Terminated
    case V1.IngestStreamStatus.Failed => Api.IngestStreamStatus.Failed
    case V1.IngestStreamStatus.Running => Api.IngestStreamStatus.Running
    case V1.IngestStreamStatus.Paused => Api.IngestStreamStatus.Paused
    case V1.IngestStreamStatus.Restored => Api.IngestStreamStatus.Restored
  }

}
