package com.thatdot.quine.app.ingest2

import java.nio.charset.Charset

import scala.util.{Failure, Success}

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

  implicit lazy val config: Configuration = ingestSourceTypeConfig

  implicit val csvCharacterSchema: Schema[V1.CsvCharacter] = Schema.derived[V1.CsvCharacter]
  implicit val recordDecodingTypeSchema: Schema[V1.RecordDecodingType] =
    Schema.derived[V1.RecordDecodingType]
  implicit val onRecordErrorHandlerSchema: Schema[OnRecordErrorHandler] =
    Schema.derived[OnRecordErrorHandler].description("Action to take on record error")

  implicit val onStreamErrorHandlerSchema: Schema[OnStreamErrorHandler] =
    Schema.derived[OnStreamErrorHandler].description("Action to take on stream error")
  implicit val ingestFormatTypeSchema: Schema[IngestFormat] =
    Schema.derived
      .description("Ingest format")
      .encodedExample(
        CsvFormat(
          Right(List("header1", "header2")),
          V1.CsvCharacter.Comma,
          V1.CsvCharacter.DoubleQuote,
          V1.CsvCharacter.Backslash,
        ).asJson,
      )

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
  implicit lazy val awsCredentialsSchema: Schema[V1.AwsCredentials] = Schema.derived
  implicit lazy val kinesisIteratorSchema: Schema[V1.KinesisIngest.IteratorType] = Schema.derived
  implicit lazy val kinesisKCLIteratorSchema: Schema[InitialPosition] = Schema.derived
  implicit lazy val awsRegionSchema: Schema[V1.AwsRegion] = Schema.derived
  implicit lazy val keepaliveProtocolSchema: Schema[V1.WebsocketSimpleStartupIngest.KeepaliveProtocol] = Schema.derived
  implicit lazy val csvIngestFormatSchema: Schema[CsvFormat] = Schema.derived
  implicit lazy val protobufIngestFormatSchema: Schema[ProtobufFormat] = Schema.derived
  implicit lazy val recordDecoderSeqSchema: Schema[Seq[V1.RecordDecodingType]] =
    Schema.schemaForArray(recordDecodingTypeSchema).map(a => Some(a.toSeq))(s => s.toArray)
  implicit lazy val fileFormatTypeSchema: Schema[FileFormat] = Schema.derived
  implicit lazy val streamingFormatTypeSchema: Schema[StreamingFormat] = Schema.derived
  implicit lazy val checkpointSettingsSchema: Schema[V1.KinesisIngest.KinesisCheckpointSettings] = Schema.derived
  implicit lazy val ingestSourceTypeSchema: Schema[IngestSource] = Schema.derived
  implicit lazy val ingestSchema: Schema[QuineIngestConfiguration] = Schema.derived[QuineIngestConfiguration]

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
    import cats.implicits.catsSyntaxEitherId
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
    encodeKCLIteratorType: Encoder[InitialPosition],
    decodeKCLIteratorType: Decoder[InitialPosition],
  ) =
    (deriveConfiguredEncoder[InitialPosition], deriveConfiguredDecoder[InitialPosition])

  implicit lazy val (
    encodeIteratorType: Encoder[V1.KinesisIngest.IteratorType],
    decodeIteratorType: Decoder[V1.KinesisIngest.IteratorType],
  ) =
    (deriveConfiguredEncoder[V1.KinesisIngest.IteratorType], deriveConfiguredDecoder[V1.KinesisIngest.IteratorType])

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
