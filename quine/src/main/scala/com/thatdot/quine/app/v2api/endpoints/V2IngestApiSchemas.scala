package com.thatdot.quine.app.v2api.endpoints

import java.nio.charset.Charset

import scala.util.{Failure, Success}

import io.circe.{Decoder, Encoder}
import sttp.tapir.CodecFormat.TextPlain
import sttp.tapir.{Codec, DecodeResult, Schema}

import com.thatdot.api.v2.schema.V2ApiSchemas
import com.thatdot.api.v2.{AwsCredentials, AwsRegion}
import com.thatdot.quine.app.v2api.definitions.ingest2.ApiIngest._

trait V2IngestApiSchemas extends V2ApiSchemas {
  implicit val config: Configuration = typeDiscriminatorConfig

  implicit val recordDecodingTypeSchema: Schema[RecordDecodingType] = Schema.derived[RecordDecodingType]

  implicit val charsetCodec: Codec[String, Charset, TextPlain] = Codec.string.mapDecode(s =>
    scala.util.Try(Charset.forName(s)) match {
      case Success(charset) => DecodeResult.Value(charset)
      case Failure(e) => DecodeResult.Error(s"Invalid charset: $s", e)
    },
  )(_.toString)

  implicit val charsetSchema: Schema[Charset] = charsetCodec.schema
  implicit val charsetEncoder: Encoder[Charset] = Encoder.encodeString.contramap(_.name)
  implicit val charsetDecoder: Decoder[Charset] = Decoder.decodeString.map(s => Charset.forName(s))
  implicit lazy val recordDecoderSeqSchema: Schema[Seq[RecordDecodingType]] =
    Schema.schemaForArray(recordDecodingTypeSchema).map(a => Some(a.toSeq))(s => s.toArray)

  implicit lazy val csvCharacterSchema: Schema[CsvCharacter] = Schema.derived[CsvCharacter]
  implicit lazy val kafkaSecurityProtocolSchema: Schema[KafkaSecurityProtocol] = Schema.derived

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // Explicit Schema definitions to avoid repeated automatic derivation (QU-2417)
  // (This may not be the ideal long-term solution, but it's low-hanging fruit for faster compilation).

  // IngestFormat dependencies
  implicit lazy val fileFormatSchema: Schema[IngestFormat.FileFormat] = Schema.derived
  implicit lazy val streamingFormatSchema: Schema[IngestFormat.StreamingFormat] = Schema.derived
  implicit lazy val ingestFormatSchema: Schema[IngestFormat] = Schema.derived

  // IngestSource dependencies
  implicit lazy val awsCredentialsSchema: Schema[AwsCredentials] = Schema.derived
  implicit lazy val awsRegionSchema: Schema[AwsRegion] = Schema.derived
  implicit lazy val kafkaAutoOffsetResetSchema: Schema[KafkaAutoOffsetReset] = Schema.derived
  implicit lazy val kafkaOffsetCommittingSchema: Schema[KafkaOffsetCommitting] = Schema.derived
  implicit lazy val keepaliveProtocolSchema: Schema[WebSocketClient.KeepaliveProtocol] = Schema.derived
  implicit lazy val fileIngestModeSchema: Schema[FileIngestMode] = Schema.derived
  implicit lazy val kinesisIteratorTypeSchema: Schema[IngestSource.Kinesis.IteratorType] = Schema.derived
  implicit lazy val initialPositionSchema: Schema[InitialPosition] = Schema.derived

  // KCL-related schemas
  implicit lazy val kinesisSchedulerSourceSettingsSchema: Schema[KinesisSchedulerSourceSettings] = Schema.derived
  implicit lazy val kinesisCheckpointSettingsSchema: Schema[KinesisCheckpointSettings] = Schema.derived
  implicit lazy val billingModeSchema: Schema[BillingMode] = Schema.derived
  implicit lazy val leaseManagementConfigSchema: Schema[LeaseManagementConfig] = Schema.derived
  implicit lazy val retrievalSpecificConfigSchema: Schema[RetrievalSpecificConfig] = Schema.derived
  implicit lazy val processorConfigSchema: Schema[ProcessorConfig] = Schema.derived
  implicit lazy val shardPrioritizationSchema: Schema[ShardPrioritization] = Schema.derived
  implicit lazy val clientVersionConfigSchema: Schema[ClientVersionConfig] = Schema.derived
  implicit lazy val coordinatorConfigSchema: Schema[CoordinatorConfig] = Schema.derived
  implicit lazy val lifecycleConfigSchema: Schema[LifecycleConfig] = Schema.derived
  implicit lazy val retrievalConfigSchema: Schema[RetrievalConfig] = Schema.derived
  implicit lazy val metricsLevelSchema: Schema[MetricsLevel] = Schema.derived
  implicit lazy val metricsDimensionSchema: Schema[MetricsDimension] = Schema.derived
  implicit lazy val metricsConfigSchema: Schema[MetricsConfig] = Schema.derived
  implicit lazy val configsBuilderSchema: Schema[ConfigsBuilder] = Schema.derived
  implicit lazy val kclConfigurationSchema: Schema[KCLConfiguration] = Schema.derived

  // IngestStreamInfo dependencies
  implicit lazy val ingestStreamStatsSchema: Schema[IngestStreamStats] = Schema.derived
  implicit lazy val ingestStreamStatusSchema: Schema[IngestStreamStatus] = Schema.derived
  implicit lazy val ingestSourceSchema: Schema[IngestSource] = Schema.derived

  // Top-level (-ish) types, dependent on above schemas
  implicit lazy val ingestStreamInfoSchema: Schema[IngestStreamInfo] = Schema.derived
  implicit lazy val ingestStreamInfoWithNameSchema: Schema[IngestStreamInfoWithName] = Schema.derived
}
