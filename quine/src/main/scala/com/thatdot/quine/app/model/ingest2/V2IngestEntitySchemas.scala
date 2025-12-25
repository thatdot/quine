package com.thatdot.quine.app.model.ingest2

import java.nio.charset.Charset

import scala.util.{Failure, Success}

import cats.implicits.catsSyntaxEitherId
import io.circe.Encoder.encodeString
import io.circe.generic.extras.auto._
import io.circe.generic.extras.semiauto.{
  deriveConfiguredDecoder,
  deriveConfiguredEncoder,
  deriveEnumerationDecoder,
  deriveEnumerationEncoder,
}
import io.circe.syntax.EncoderOps
import io.circe.{Decoder, Encoder}
import sttp.tapir.CodecFormat.TextPlain
import sttp.tapir.{Codec, DecodeResult, Schema}

import com.thatdot.api.v2.schema.V2ApiConfiguration
import com.thatdot.quine.app.model.ingest2.V2IngestEntities.FileFormat.CsvFormat
import com.thatdot.quine.app.model.ingest2.V2IngestEntities.StreamingFormat.ProtobufFormat
import com.thatdot.quine.app.model.ingest2.V2IngestEntities._
import com.thatdot.quine.routes.AwsCredentials
import com.thatdot.quine.routes.CsvCharacter.{Backslash, Comma, DoubleQuote}
import com.thatdot.quine.serialization.EncoderDecoder
import com.thatdot.quine.{routes => V1}

object V2IngestEntityEncoderDecoders extends V2IngestEntitySchemas {

  // Importing V2IngestEncoderDecoders.implicits._ imports all of the EncoderDecoders without
  // importing anything related to Tapir Schemas
  // This allows selectively importing the EncoderDecoders for V2 ingests without importing the entire
  // V2IngestSchemas, which is necessary for working with EncoderDecoders in an environment where
  // the implicits are already defined using Endpoints4s for V1 ingests
  object implicits {

    implicit def quineIngestStreamWithStatusSchema: EncoderDecoder[QuineIngestStreamWithStatus] =
      EncoderDecoder.ofEncodeDecode
    implicit val quineIngestConfigurationSchema: EncoderDecoder[QuineIngestConfiguration] =
      EncoderDecoder.ofEncodeDecode
  }
}

trait V2IngestEntitySchemas extends V2ApiConfiguration {

  implicit lazy val config: Configuration = typeDiscriminatorConfig

  implicit val csvCharacterSchema: Schema[V1.CsvCharacter] = Schema.derived[V1.CsvCharacter]
  implicit val recordDecodingTypeSchema: Schema[V1.RecordDecodingType] =
    Schema.derived[V1.RecordDecodingType]

  implicit val onStreamErrorHandlerSchema: Schema[OnStreamErrorHandler] =
    Schema.derived[OnStreamErrorHandler].description("Action to take on stream error")

  implicit val ingestFormatTypeSchema: Schema[IngestFormat] =
    Schema.derived
      .description("Ingest format")
      .encodedExample(CsvFormat(Right(List("header1", "header2")), Comma, DoubleQuote, Backslash).asJson)

  implicit val charsetCodec: Codec[String, Charset, TextPlain] = Codec.string.mapDecode(s =>
    scala.util.Try(Charset.forName(s)) match {
      case Success(charset) => DecodeResult.Value(charset)
      case Failure(e) => DecodeResult.Error(s"Invalid charset: $s", e)
    },
  )(_.toString)

  implicit val charsetSchema: Schema[Charset] = charsetCodec.schema

  implicit val fileIngestModeSchema: Schema[V1.FileIngestMode] =
    Schema.derived

  implicit lazy val kafkaSecurityProtocolSchema: Schema[V1.KafkaSecurityProtocol] = Schema.derived
  implicit lazy val kafkaAutoOffsetResetSchema: Schema[V1.KafkaAutoOffsetReset] = Schema.derived
  implicit lazy val kafkaOffsetCommittingSchema: Schema[V1.KafkaOffsetCommitting] = Schema.derived
  implicit lazy val awsCredentialsSchema: Schema[AwsCredentials] = Schema.derived
  implicit lazy val initialPositionSchema: Schema[InitialPosition] = Schema.derived
  implicit lazy val awsRegionSchema: Schema[V1.AwsRegion] = Schema.derived
  implicit lazy val keepaliveProtocolSchema: Schema[V1.WebsocketSimpleStartupIngest.KeepaliveProtocol] = Schema.derived
  implicit lazy val csvIngestFormatSchema: Schema[CsvFormat] = Schema.derived
  implicit lazy val protobufIngestFormatSchema: Schema[ProtobufFormat] = Schema.derived
  implicit lazy val recordDecoderSeqSchema: Schema[Seq[V1.RecordDecodingType]] =
    Schema.schemaForArray(recordDecodingTypeSchema).map(a => Some(a.toSeq))(s => s.toArray)
  implicit lazy val fileFormatTypeSchema: Schema[FileFormat] = Schema.derived
  implicit lazy val streamingFormatTypeSchema: Schema[StreamingFormat] = Schema.derived
  implicit lazy val kinesisSchedulerSourceSettingsSchema: Schema[KinesisSchedulerSourceSettings] = Schema.derived

  // ---- Schemas for the KCLConfiguration ----

  implicit lazy val metricsDimensionSchema: Schema[MetricsDimension] = Schema.derived
  implicit lazy val clientVersionConfigSchema: Schema[ClientVersionConfig] = Schema.derived

  implicit lazy val billingModeSchema: Schema[BillingMode] = Schema.derived

  implicit lazy val shardPrioritizationSchema: Schema[ShardPrioritization] = Schema.derived
  implicit lazy val metricsLevelSchema: Schema[MetricsLevel] = Schema.derived
  implicit lazy val metricsConfigSchema: Schema[MetricsConfig] = Schema.derived
  implicit lazy val configsBuilderSchema: Schema[ConfigsBuilder] = Schema.derived
  implicit lazy val leaseManagementConfigSchema: Schema[LeaseManagementConfig] = Schema.derived
  implicit lazy val retrievalSpecificConfigSchema: Schema[RetrievalSpecificConfig] = Schema.derived
  implicit lazy val processorConfigSchema: Schema[ProcessorConfig] = Schema.derived
  implicit lazy val coordinatorConfigSchema: Schema[CoordinatorConfig] = Schema.derived
  implicit lazy val lifecycleConfigSchema: Schema[LifecycleConfig] = Schema.derived
  implicit lazy val retrievalConfigSchema: Schema[RetrievalConfig] = Schema.derived
  implicit lazy val kinesisIteratorSchema: Schema[V1.KinesisIngest.IteratorType] = Schema.derived
  implicit lazy val kinesisCheckpointSettingsSchema: Schema[KinesisCheckpointSettings] = Schema.derived
  implicit lazy val kclConfigurationSchema: Schema[KCLConfiguration] = Schema.derived

  implicit lazy val javaScriptScheme: Schema[Transformation.JavaScript] = Schema.derived
  implicit lazy val transformationScheme: Schema[Transformation] = Schema.derived

  implicit lazy val ingestSourceTypeSchema: Schema[IngestSource] = Schema.derived
  //implicit lazy val ingestSchema: Schema[QuineIngestConfiguration] = Schema.derived[QuineIngestConfiguration]

  implicit val charsetEncoder: Encoder[Charset] = Encoder.encodeString.contramap(_.name)
  implicit val charsetDecoder: Decoder[Charset] = Decoder.decodeString.map(s => Charset.forName(s))

  implicit lazy val FileIngestModeEncoder: Encoder[V1.FileIngestMode] = deriveEnumerationEncoder[V1.FileIngestMode]
  implicit lazy val FileIngestModeDecoder: Decoder[V1.FileIngestMode] = deriveEnumerationDecoder[V1.FileIngestMode]

  implicit lazy val recordDecoderEncoder: Encoder[V1.RecordDecodingType] =
    deriveEnumerationEncoder[V1.RecordDecodingType]
  implicit lazy val recordDecoderDecoder: Decoder[V1.RecordDecodingType] =
    deriveEnumerationDecoder[V1.RecordDecodingType]

  implicit lazy val kafkaOffsetResetEncoder: Encoder[V1.KafkaAutoOffsetReset] =
    deriveEnumerationEncoder[V1.KafkaAutoOffsetReset]
  implicit lazy val kafkaOffsetResetDecoder: Decoder[V1.KafkaAutoOffsetReset] =
    deriveEnumerationDecoder[V1.KafkaAutoOffsetReset]

  implicit lazy val csvCharacterEncoder: Encoder[V1.CsvCharacter] = deriveEnumerationEncoder[V1.CsvCharacter]
  implicit lazy val csvCharacterDecoder: Decoder[V1.CsvCharacter] = deriveEnumerationDecoder[V1.CsvCharacter]

  implicit lazy val (
    kafkaOffsetCommittingEncoder: Encoder[V1.KafkaOffsetCommitting],
    kafkaOffsetCommittingDecoder: Decoder[V1.KafkaOffsetCommitting],
  ) =
    (deriveConfiguredEncoder[V1.KafkaOffsetCommitting], deriveConfiguredDecoder[V1.KafkaOffsetCommitting])

  implicit val (
    encodeKafkaSecurityProtocol: Encoder[V1.KafkaSecurityProtocol],
    decodeKafkaSecurityProtocol: Decoder[V1.KafkaSecurityProtocol],
  ) = {
    val encoder: Encoder[V1.KafkaSecurityProtocol] = encodeString.contramap(_.name)
    val decoder: Decoder[V1.KafkaSecurityProtocol] = Decoder.decodeString.emap {
      case s if s == V1.KafkaSecurityProtocol.PlainText.name => V1.KafkaSecurityProtocol.PlainText.asRight
      case s if s == V1.KafkaSecurityProtocol.Ssl.name => V1.KafkaSecurityProtocol.Ssl.asRight
      case s if s == V1.KafkaSecurityProtocol.Sasl_Ssl.name => V1.KafkaSecurityProtocol.Sasl_Ssl.asRight
      case s if s == V1.KafkaSecurityProtocol.Sasl_Plaintext.name => V1.KafkaSecurityProtocol.Sasl_Plaintext.asRight
      case s => Left(s"$s is not a valid KafkaSecurityProtocol")
    }
    (encoder, decoder)
  }

  implicit lazy val (
    encodeKeepaliveProtocol: Encoder[V1.WebsocketSimpleStartupIngest.KeepaliveProtocol],
    decodeKeepaliveProtocol: Decoder[V1.WebsocketSimpleStartupIngest.KeepaliveProtocol],
  ) =
    (
      deriveConfiguredEncoder[V1.WebsocketSimpleStartupIngest.KeepaliveProtocol],
      deriveConfiguredDecoder[V1.WebsocketSimpleStartupIngest.KeepaliveProtocol],
    )

  implicit lazy val (
    encodeInitialPosition: Encoder[InitialPosition],
    decodeInitialPosition: Decoder[InitialPosition],
  ) =
    (deriveConfiguredEncoder[InitialPosition], deriveConfiguredDecoder[InitialPosition])

  implicit lazy val FileFormatEncoder: Encoder[FileFormat] =
    deriveConfiguredEncoder[FileFormat]

  implicit lazy val FileFormatDecoder: Decoder[FileFormat] =
    deriveConfiguredDecoder[FileFormat]
  implicit lazy val StreamingFormatEncoder: Encoder[StreamingFormat] =
    deriveConfiguredEncoder[StreamingFormat]

  implicit lazy val StreamingFormatDecoder: Decoder[StreamingFormat] =
    deriveConfiguredDecoder[StreamingFormat]

  implicit lazy val OnStreamErrorHandlerEncoder: Encoder[OnStreamErrorHandler] =
    deriveConfiguredEncoder[OnStreamErrorHandler]
  implicit lazy val OnStreamErrorHandlerDecoder: Decoder[OnStreamErrorHandler] =
    deriveConfiguredDecoder[OnStreamErrorHandler]

  implicit lazy val billingModeEncoder: Encoder[BillingMode] = deriveConfiguredEncoder[BillingMode]
  implicit lazy val billingModeDecoder: Decoder[BillingMode] = deriveConfiguredDecoder[BillingMode]

  implicit lazy val metricsLevelEncoder: Encoder[MetricsLevel] = deriveEnumerationEncoder[MetricsLevel]
  implicit lazy val metricsLevelDecoder: Decoder[MetricsLevel] = deriveEnumerationDecoder[MetricsLevel]

  implicit lazy val metricsDimensionEncoder: Encoder[MetricsDimension] = deriveConfiguredEncoder[MetricsDimension]
  implicit lazy val metricsDimensionDecoder: Decoder[MetricsDimension] = deriveConfiguredDecoder[MetricsDimension]

  implicit lazy val clientVersionConfigEncoder: Encoder[ClientVersionConfig] =
    deriveEnumerationEncoder[ClientVersionConfig]
  implicit lazy val clientVersionConfigDecoder: Decoder[ClientVersionConfig] =
    deriveEnumerationDecoder[ClientVersionConfig]

  implicit lazy val shardPrioritizationEncoder: Encoder[ShardPrioritization] =
    deriveConfiguredEncoder[ShardPrioritization]
  implicit lazy val shardPrioritizationDecoder: Decoder[ShardPrioritization] =
    deriveConfiguredDecoder[ShardPrioritization]

  implicit lazy val kinesisCheckpointSettingsEncoder: Encoder[KinesisCheckpointSettings] =
    deriveConfiguredEncoder[KinesisCheckpointSettings]
  implicit lazy val kinesisCheckpointSettingsDecoder: Decoder[KinesisCheckpointSettings] =
    deriveConfiguredDecoder[KinesisCheckpointSettings]

  implicit lazy val kinesisSchedulerSourceSettingsEncoder: Encoder[KinesisSchedulerSourceSettings] =
    deriveConfiguredEncoder[KinesisSchedulerSourceSettings]
  implicit lazy val kinesisSchedulerSourceSettingsDecoder: Decoder[KinesisSchedulerSourceSettings] =
    deriveConfiguredDecoder[KinesisSchedulerSourceSettings]

  implicit lazy val configsBuilderEncoder: Encoder[ConfigsBuilder] = deriveConfiguredEncoder[ConfigsBuilder]
  implicit lazy val configsBuilderDecoder: Decoder[ConfigsBuilder] = deriveConfiguredDecoder[ConfigsBuilder]

  implicit lazy val lifecycleConfigEncoder: Encoder[LifecycleConfig] = deriveConfiguredEncoder[LifecycleConfig]
  implicit lazy val lifecycleConfigDecoder: Decoder[LifecycleConfig] = deriveConfiguredDecoder[LifecycleConfig]

  implicit lazy val retrievalConfigEncoder: Encoder[RetrievalConfig] = deriveConfiguredEncoder[RetrievalConfig]
  implicit lazy val retrievalConfigDecoder: Decoder[RetrievalConfig] = deriveConfiguredDecoder[RetrievalConfig]

  implicit lazy val processorConfigEncoder: Encoder[ProcessorConfig] = deriveConfiguredEncoder[ProcessorConfig]
  implicit lazy val processorConfigDecoder: Decoder[ProcessorConfig] = deriveConfiguredDecoder[ProcessorConfig]

  implicit lazy val retrievalSpecificConfigEncoder: Encoder[RetrievalSpecificConfig] =
    deriveConfiguredEncoder[RetrievalSpecificConfig]
  implicit lazy val retrievalSpecificConfigDecoder: Decoder[RetrievalSpecificConfig] =
    deriveConfiguredDecoder[RetrievalSpecificConfig]

  implicit lazy val leaseManagementConfigEncoder: Encoder[LeaseManagementConfig] =
    deriveConfiguredEncoder[LeaseManagementConfig]
  implicit lazy val leaseManagementConfigDecoder: Decoder[LeaseManagementConfig] =
    deriveConfiguredDecoder[LeaseManagementConfig]

  implicit lazy val coordinatorConfigEncoder: Encoder[CoordinatorConfig] =
    deriveConfiguredEncoder[CoordinatorConfig]
  implicit lazy val coordinatorConfigDecoder: Decoder[CoordinatorConfig] =
    deriveConfiguredDecoder[CoordinatorConfig]

  implicit lazy val metricsConfigEncoder: Encoder[MetricsConfig] = deriveConfiguredEncoder[MetricsConfig]
  implicit lazy val metricsConfigDecoder: Decoder[MetricsConfig] = deriveConfiguredDecoder[MetricsConfig]

  implicit lazy val kclConfigurationEncoder: Encoder[KCLConfiguration] = deriveConfiguredEncoder[KCLConfiguration]
  implicit lazy val kclConfigurationDecoder: Decoder[KCLConfiguration] = deriveConfiguredDecoder[KCLConfiguration]

  implicit lazy val awsCredentialsEncoder: Encoder[V1.AwsCredentials] = deriveConfiguredEncoder[V1.AwsCredentials]
  implicit lazy val awsCredentialsDecoder: Decoder[V1.AwsCredentials] = deriveConfiguredDecoder[V1.AwsCredentials]

  implicit lazy val awsRegionEncoder: Encoder[V1.AwsRegion] = deriveConfiguredEncoder[V1.AwsRegion]
  implicit lazy val awsRegionDecoder: Decoder[V1.AwsRegion] = deriveConfiguredDecoder[V1.AwsRegion]

  implicit lazy val kinesisIteratorTypeEncoder: Encoder[V1.KinesisIngest.IteratorType] =
    deriveConfiguredEncoder[V1.KinesisIngest.IteratorType]
  implicit lazy val kinesisIteratorTypeDecoder: Decoder[V1.KinesisIngest.IteratorType] =
    deriveConfiguredDecoder[V1.KinesisIngest.IteratorType]

  implicit lazy val transformationEncoder: Encoder[Transformation] = deriveConfiguredEncoder[Transformation]
  implicit lazy val transformationDecoder: Decoder[Transformation] = deriveConfiguredDecoder[Transformation]

  implicit lazy val ingestSourceEncoder: Encoder[IngestSource] = deriveConfiguredEncoder[IngestSource]
  implicit lazy val ingestSourceDecoder: Decoder[IngestSource] = deriveConfiguredDecoder[IngestSource]
}
