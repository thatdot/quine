package com.thatdot.quine.app.ingest2

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
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.syntax.EncoderOps
import io.circe.{Decoder, Encoder}
import sttp.tapir.CodecFormat.TextPlain
import sttp.tapir.{Codec, DecodeResult, Schema}

import com.thatdot.quine.app.ingest2.V2IngestEntities.FileFormat.CsvFormat
import com.thatdot.quine.app.ingest2.V2IngestEntities.StreamingFormat.ProtobufFormat
import com.thatdot.quine.app.ingest2.V2IngestEntities._
import com.thatdot.quine.app.serialization.EncoderDecoder
import com.thatdot.quine.app.v2api.endpoints.V2ApiConfiguration
import com.thatdot.quine.routes.CsvCharacter.{Backslash, Comma, DoubleQuote}
import com.thatdot.quine.routes.{KinesisIngest => V1KinesisIngest, _}

object V2IngestEntityEncoderDecoders extends V2IngestEntitySchemas {

  // Importing V2IngestEncoderDecoders.implicits._ imports all of the  EncoderDecoders without
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

  implicit lazy val config: Configuration = ingestSourceTypeConfig

  implicit val csvCharacterSchema: Schema[CsvCharacter] = Schema.derived[CsvCharacter]
  implicit val recordDecodingTypeSchema: Schema[RecordDecodingType] =
    Schema.derived[RecordDecodingType]
  implicit val onRecordErrorHandlerSchema: Schema[OnRecordErrorHandler] =
    Schema.derived[OnRecordErrorHandler].description("Action to take on record error")

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

  implicit val fileIngestModeSchema: Schema[FileIngestMode] =
    Schema.derived

  implicit lazy val kafkaSecurityProtocolSchema: Schema[KafkaSecurityProtocol] = Schema.derived
  implicit lazy val kafkaAutoOffsetResetSchema: Schema[KafkaAutoOffsetReset] = Schema.derived
  implicit lazy val kafkaOffsetCommittingSchema: Schema[KafkaOffsetCommitting] = Schema.derived
  implicit lazy val awsCredentialsSchema: Schema[AwsCredentials] = Schema.derived
  implicit lazy val kinesisIteratorSchema: Schema[V1KinesisIngest.IteratorType] = Schema.derived
  implicit lazy val kinesisKCLIteratorSchema: Schema[KCLIteratorType] = Schema.derived
  implicit lazy val awsRegionSchema: Schema[AwsRegion] = Schema.derived
  implicit lazy val keepaliveProtocolSchema: Schema[WebsocketSimpleStartupIngest.KeepaliveProtocol] = Schema.derived
  implicit lazy val csvIngestFormatSchema: Schema[CsvFormat] = Schema.derived
  implicit lazy val protobufIngestFormatSchema: Schema[ProtobufFormat] = Schema.derived
  implicit lazy val recordDecoderSeqSchema: Schema[Seq[RecordDecodingType]] =
    Schema.schemaForArray(recordDecodingTypeSchema).map(a => Some(a.toSeq))(s => s.toArray)
  implicit lazy val fileFormatTypeSchema: Schema[FileFormat] = Schema.derived
  implicit lazy val streamingFormatTypeSchema: Schema[StreamingFormat] = Schema.derived
  implicit lazy val checkpointSettingsSchema: Schema[KinesisCheckpointSettings] =
    Schema.derived[KinesisCheckpointSettings]
  implicit lazy val ingestSourceTypeSchema: Schema[IngestSource] = Schema.derived[IngestSource]

  implicit lazy val ingestSchema: Schema[QuineIngestConfiguration] = Schema.derived[QuineIngestConfiguration]

  implicit val charsetEncoder: Encoder[Charset] = Encoder.encodeString.contramap(_.name)
  implicit val charsetDecoder: Decoder[Charset] = Decoder.decodeString.map(s => Charset.forName(s))

  implicit lazy val FileIngestModeEncoder: Encoder[FileIngestMode] = deriveEnumerationEncoder[FileIngestMode]
  implicit lazy val FileIngestModeDecoder: Decoder[FileIngestMode] = deriveEnumerationDecoder[FileIngestMode]

  implicit lazy val recordDecoderEncoder: Encoder[RecordDecodingType] = deriveEnumerationEncoder[RecordDecodingType]
  implicit lazy val recordDecoderDecoder: Decoder[RecordDecodingType] = deriveEnumerationDecoder[RecordDecodingType]

  implicit lazy val kafkaOffsetResetEncoder: Encoder[KafkaAutoOffsetReset] =
    deriveEnumerationEncoder[KafkaAutoOffsetReset]
  implicit lazy val kafkaOffsetResetDecoder: Decoder[KafkaAutoOffsetReset] =
    deriveEnumerationDecoder[KafkaAutoOffsetReset]

  implicit lazy val csvCharacterEncoder: Encoder[CsvCharacter] = deriveEnumerationEncoder[CsvCharacter]
  implicit lazy val csvCharacterDecoder: Decoder[CsvCharacter] = deriveEnumerationDecoder[CsvCharacter]

  implicit lazy val (
    kafkaOffsetCommittingEncoder: Encoder[KafkaOffsetCommitting],
    kafkaOffsetCommittingDecoder: Decoder[KafkaOffsetCommitting],
  ) =
    (deriveConfiguredEncoder[KafkaOffsetCommitting], deriveConfiguredDecoder[KafkaOffsetCommitting])

  implicit val (
    encodeKafkaSecurityProtocol: Encoder[KafkaSecurityProtocol],
    decodeKafkaSecurityProtocol: Decoder[KafkaSecurityProtocol],
  ) = {
    val encoder: Encoder[KafkaSecurityProtocol] = encodeString.contramap(_.name)
    val decoder: Decoder[KafkaSecurityProtocol] = Decoder.decodeString.emap {
      case s if s == KafkaSecurityProtocol.PlainText.name => KafkaSecurityProtocol.PlainText.asRight
      case s if s == KafkaSecurityProtocol.Ssl.name => KafkaSecurityProtocol.Ssl.asRight
      case s if s == KafkaSecurityProtocol.Sasl_Ssl.name => KafkaSecurityProtocol.Sasl_Ssl.asRight
      case s if s == KafkaSecurityProtocol.Sasl_Plaintext.name => KafkaSecurityProtocol.Sasl_Plaintext.asRight
      case s => Left(s"$s is not a valid KafkaSecurityProtocol")
    }
    (encoder, decoder)
  }

  implicit lazy val (
    encodeKeepaliveProtocol: Encoder[WebsocketSimpleStartupIngest.KeepaliveProtocol],
    decodeKeepaliveProtocol: Decoder[WebsocketSimpleStartupIngest.KeepaliveProtocol],
  ) =
    (
      deriveConfiguredEncoder[WebsocketSimpleStartupIngest.KeepaliveProtocol],
      deriveConfiguredDecoder[WebsocketSimpleStartupIngest.KeepaliveProtocol],
    )

  implicit lazy val (
    encodeKCLIteratorType: Encoder[KCLIteratorType],
    decodeKCLIteratorType: Decoder[KCLIteratorType],
  ) =
    (deriveConfiguredEncoder[KCLIteratorType], deriveConfiguredDecoder[KCLIteratorType])

  implicit lazy val (
    encodeIteratorType: Encoder[V1KinesisIngest.IteratorType],
    decodeIteratorType: Decoder[V1KinesisIngest.IteratorType],
  ) =
    (deriveConfiguredEncoder[V1KinesisIngest.IteratorType], deriveConfiguredDecoder[V1KinesisIngest.IteratorType])

  implicit lazy val FileFormatEncoder: Encoder[FileFormat] =
    deriveConfiguredEncoder[FileFormat]

  implicit lazy val FileFormatDecoder: Decoder[FileFormat] =
    deriveConfiguredDecoder[FileFormat]
  implicit lazy val StreamingFormatEncoder: Encoder[StreamingFormat] =
    deriveConfiguredEncoder[StreamingFormat]

  implicit lazy val StreamingFormatDecoder: Decoder[StreamingFormat] =
    deriveConfiguredDecoder[StreamingFormat]

  implicit lazy val onRecordErrorHandlerEncoder: Encoder[OnRecordErrorHandler] =
    deriveConfiguredEncoder[OnRecordErrorHandler]
  implicit lazy val onRecordErrorHandlerDecoder: Decoder[OnRecordErrorHandler] =
    deriveConfiguredDecoder[OnRecordErrorHandler]
  implicit lazy val OnStreamErrorHandlerEncoder: Encoder[OnStreamErrorHandler] =
    deriveConfiguredEncoder[OnStreamErrorHandler]
  implicit lazy val OnStreamErrorHandlerDecoder: Decoder[OnStreamErrorHandler] =
    deriveConfiguredDecoder[OnStreamErrorHandler]

  implicit val encoder: Encoder.AsObject[QuineIngestConfiguration] = deriveEncoder[QuineIngestConfiguration]
  implicit val decoder: Decoder[QuineIngestConfiguration] = deriveDecoder[QuineIngestConfiguration]
}
