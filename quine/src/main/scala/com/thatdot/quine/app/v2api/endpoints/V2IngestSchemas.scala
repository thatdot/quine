package com.thatdot.quine.app.v2api.endpoints

import java.nio.charset.Charset

import scala.util.{Failure, Success}

import io.circe.generic.auto._
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.semiauto.{
  deriveConfiguredDecoder,
  deriveConfiguredEncoder,
  deriveEnumerationDecoder,
  deriveEnumerationEncoder,
}
import io.circe.syntax.EncoderOps
import io.circe.{Decoder, Encoder}
import sttp.tapir.CodecFormat.TextPlain
import sttp.tapir.json.circe.TapirJsonCirce
import sttp.tapir.{Codec, DecodeResult, Schema}

import com.thatdot.quine.app.v2api.endpoints.V2IngestEntities.FileFormat.CsvFormat
import com.thatdot.quine.app.v2api.endpoints.V2IngestEntities.StreamingFormat.ProtobufFormat
import com.thatdot.quine.app.v2api.endpoints.V2IngestEntities._
import com.thatdot.quine.routes.CsvCharacter.{Backslash, Comma, DoubleQuote}
import com.thatdot.quine.routes.{KinesisIngest => V1KinesisIngest, _}
trait V2IngestSchemas extends TapirJsonCirce {
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
    Schema.derived //TODO this is a V1 object and only has endpoints4s docs

  implicit lazy val kafkaSecurityProtocolSchema: Schema[KafkaSecurityProtocol] = Schema.derived
  implicit lazy val kafkaAutoOffsetResetSchema: Schema[KafkaAutoOffsetReset] = Schema.derived
  implicit lazy val kafkaOffsetCommittingSchema: Schema[KafkaOffsetCommitting] = Schema.derived
  implicit lazy val awsCredentialsSchema: Schema[AwsCredentials] = Schema.derived
  implicit lazy val kinesisIteratorSchema: Schema[V1KinesisIngest.IteratorType] = Schema.derived
  implicit lazy val awsRegionSchema: Schema[AwsRegion] = Schema.derived
  implicit lazy val keepaliveProtocolSchema: Schema[WebsocketSimpleStartupIngest.KeepaliveProtocol] = Schema.derived
  implicit lazy val csvIngestFormatSchema: Schema[CsvFormat] = Schema.derived
  implicit lazy val protobufIngestFormatSchema: Schema[ProtobufFormat] = Schema.derived
  implicit lazy val recordDecoderSeqSchema: Schema[Seq[RecordDecodingType]] =
    Schema.schemaForArray(recordDecodingTypeSchema).map(a => Some(a.toSeq))(s => s.toArray)
  implicit lazy val fileFormatTypeSchema: Schema[FileFormat] = Schema.derived
  implicit lazy val streamingFormatTypeSchema: Schema[StreamingFormat] = Schema.derived
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

  val ingestSourceTypeConfig: Configuration = Configuration.default.withDiscriminator("type")

  implicit lazy val ingestSourceTypeEncoder: Encoder[IngestSource] = {
    implicit val config = ingestSourceTypeConfig
    deriveConfiguredEncoder[IngestSource]
  }
  implicit lazy val ingestSourceTypeDecoder: Decoder[IngestSource] = {
    implicit val config = ingestSourceTypeConfig
    deriveConfiguredDecoder[IngestSource]
  }
  implicit lazy val FileFormatEncoder: Encoder[FileFormat] = {
    implicit val config = ingestSourceTypeConfig
    deriveConfiguredEncoder[FileFormat]
  }

  implicit lazy val FileFormatDecoder: Decoder[FileFormat] = {
    implicit val config = ingestSourceTypeConfig
    deriveConfiguredDecoder[FileFormat]
  }
  implicit lazy val StreamingFormatEncoder: Encoder[StreamingFormat] = {
    implicit val config = ingestSourceTypeConfig
    deriveConfiguredEncoder[StreamingFormat]
  }

  implicit lazy val StreamingFormatDecoder: Decoder[StreamingFormat] = {
    implicit val config = ingestSourceTypeConfig
    deriveConfiguredDecoder[StreamingFormat]
  }

  implicit lazy val onRecordErrorHandlerEncoder: Encoder[OnRecordErrorHandler] = {
    implicit val config = ingestSourceTypeConfig
    deriveConfiguredEncoder[OnRecordErrorHandler]
  }
  implicit lazy val onRecordErrorHandlerDecoder: Decoder[OnRecordErrorHandler] = {
    implicit val config = ingestSourceTypeConfig
    deriveConfiguredDecoder[OnRecordErrorHandler]
  }
  implicit lazy val OnStreamErrorHandlerEncoder: Encoder[OnStreamErrorHandler] = {
    implicit val config = ingestSourceTypeConfig
    deriveConfiguredEncoder[OnStreamErrorHandler]
  }
  implicit lazy val OnStreamErrorHandlerDecoder: Decoder[OnStreamErrorHandler] = {
    implicit val config = ingestSourceTypeConfig
    deriveConfiguredDecoder[OnStreamErrorHandler]
  }
  implicit val ingestStreamStatusEncoder: Encoder[IngestStreamStatus] = Encoder.encodeString.contramap(_.toString)
}
