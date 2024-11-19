package com.thatdot.quine.app.v2api.endpoints

import java.nio.charset.Charset

import scala.util.{Failure, Success}

import cats.implicits.catsSyntaxEitherId
import io.circe.Encoder.encodeString
import io.circe.generic.auto._
import io.circe.generic.extras.semiauto.{
  deriveConfiguredDecoder,
  deriveConfiguredEncoder,
  deriveEnumerationDecoder,
  deriveEnumerationEncoder,
}
import io.circe.generic.extras.{Configuration => CirceConfig}
import io.circe.syntax.EncoderOps
import io.circe.{Decoder, Encoder, Json}
import sttp.tapir.CodecFormat.TextPlain
import sttp.tapir.generic.{Configuration => TapirConfig}
import sttp.tapir.json.circe.TapirJsonCirce
import sttp.tapir.{Codec, DecodeResult, Schema}

import com.thatdot.quine.app.ingest2.V2IngestEntities.StreamingFormat.ProtobufFormat
import com.thatdot.quine.app.v2api.definitions.ApiIngest.CsvCharacter.{Backslash, Comma, DoubleQuote}
import com.thatdot.quine.app.v2api.definitions.ApiIngest.FileFormat.CsvFormat
import com.thatdot.quine.app.v2api.definitions.ApiIngest._

trait IngestApiSchemas extends TapirJsonCirce {

  //For some reason the Schema derivation uses Circe configuration while encoder/decoder derivation uses
  //Tapir configuration, so this exists to unify the two for options we need
  case class Configuration(discriminator: Option[String]) {
    def asTapir: TapirConfig =
      discriminator.fold(TapirConfig.default)(d => TapirConfig.default.withDiscriminator(d))
    def asCircle: CirceConfig =
      discriminator.fold(CirceConfig.default)(d => CirceConfig.default.withDiscriminator(d))
    def withDiscriminator(d: String): Configuration =
      copy(discriminator = Some(d))
  }
  object Configuration {
    val default: Configuration = Configuration(None)
  }
  implicit def circeConfig(implicit config: Configuration): CirceConfig = config.asCircle
  implicit def tapirConfig(implicit config: Configuration): TapirConfig = config.asTapir

  val ingestSourceTypeConfig: Configuration = Configuration.default.withDiscriminator("type")

  trait JsonDisjoint[A, B]
  trait JsonPrim[A]
  trait JsonListLike[A]
  trait JsonObjLike[A]

  implicit val jsonPrimInt: JsonPrim[Int] = new JsonPrim[Int] {}
  implicit val jsonPrimString: JsonPrim[String] = new JsonPrim[String] {}
  implicit val jsonPrimBoolean: JsonPrim[Boolean] = new JsonPrim[Boolean] {}

  implicit def jsonObjMap[K, V]: JsonObjLike[Map[K, V]] = new JsonObjLike[Map[K, V]] {}

  implicit def jsonListList[A]: JsonListLike[List[A]] = new JsonListLike[List[A]] {}
  implicit def jsonListSet[A]: JsonListLike[Set[A]] = new JsonListLike[Set[A]] {}

  implicit def jsonDisjointPrimObj[A: JsonPrim, B: JsonObjLike]: JsonDisjoint[A, B] = new JsonDisjoint[A, B] {}
  implicit def jsonDisjointObjPrim[A: JsonObjLike, B: JsonPrim]: JsonDisjoint[A, B] = new JsonDisjoint[A, B] {}
  implicit def jsonDisjointPrimList[A: JsonPrim, B: JsonListLike]: JsonDisjoint[A, B] = new JsonDisjoint[A, B] {}
  implicit def jsonDisjointListPrim[A: JsonListLike, B: JsonPrim]: JsonDisjoint[A, B] = new JsonDisjoint[A, B] {}
  implicit def jsonDisjointListObj[A: JsonListLike, B: JsonObjLike]: JsonDisjoint[A, B] = new JsonDisjoint[A, B] {}
  implicit def jsonDisjointObjList[A: JsonObjLike, B: JsonListLike]: JsonDisjoint[A, B] = new JsonDisjoint[A, B] {}

  implicit def disjointEitherEncoder[A, B](implicit
    disjoint: JsonDisjoint[A, B],
    encodeA: Encoder[A],
    encodeB: Encoder[B],
  ): Encoder[Either[A, B]] = new Encoder[Either[A, B]] {
    override def apply(a: Either[A, B]): Json = a match {
      case Left(value) => encodeA(value)
      case Right(value) => encodeB(value)
    }
  }
  implicit def disjointEitherDecoder[A, B](implicit
    disjoint: JsonDisjoint[A, B],
    decodeA: Decoder[A],
    decodeB: Decoder[B],
  ): Decoder[Either[A, B]] =
    decodeA.map(Left(_)).or(decodeB.map(Right(_)))

  implicit val recordDecodingTypeSchema: Schema[RecordDecodingType] =
    Schema.derived[RecordDecodingType]
  implicit val onRecordErrorHandlerSchema: Schema[OnRecordErrorHandler] =
    Schema.derived[OnRecordErrorHandler].description("Action to take on record error")

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

  implicit lazy val awsCredentialsSchema: Schema[AwsCredentials] = Schema.derived
  implicit lazy val awsRegionSchema: Schema[AwsRegion] = Schema.derived
  implicit lazy val csvIngestFormatSchema: Schema[CsvFormat] = Schema.derived
  implicit lazy val protobufIngestFormatSchema: Schema[ProtobufFormat] = Schema.derived

  implicit val charsetSchema: Schema[Charset] = charsetCodec.schema
  implicit val charsetEncoder: Encoder[Charset] = Encoder.encodeString.contramap(_.name)
  implicit val charsetDecoder: Decoder[Charset] = Decoder.decodeString.map(s => Charset.forName(s))

  implicit val fileIngestModeSchema: Schema[FileIngestMode] = Schema.derived
  implicit lazy val FileIngestModeEncoder: Encoder[FileIngestMode] = deriveEnumerationEncoder[FileIngestMode]
  implicit lazy val FileIngestModeDecoder: Decoder[FileIngestMode] = deriveEnumerationDecoder[FileIngestMode]

  implicit lazy val recordDecoderSeqSchema: Schema[Seq[RecordDecodingType]] =
    Schema.schemaForArray(recordDecodingTypeSchema).map(a => Some(a.toSeq))(s => s.toArray)
  implicit lazy val recordDecoderEncoder: Encoder[RecordDecodingType] = deriveEnumerationEncoder[RecordDecodingType]
  implicit lazy val recordDecoderDecoder: Decoder[RecordDecodingType] = deriveEnumerationDecoder[RecordDecodingType]

  implicit lazy val kafkaAutoOffsetResetSchema: Schema[KafkaAutoOffsetReset] = Schema.derived
  implicit lazy val kafkaOffsetResetEncoder: Encoder[KafkaAutoOffsetReset] =
    deriveEnumerationEncoder[KafkaAutoOffsetReset]
  implicit lazy val kafkaOffsetResetDecoder: Decoder[KafkaAutoOffsetReset] =
    deriveEnumerationDecoder[KafkaAutoOffsetReset]

  implicit lazy val csvCharacterSchema: Schema[CsvCharacter] = Schema.derived[CsvCharacter]
  implicit lazy val csvCharacterEncoder: Encoder[CsvCharacter] = deriveEnumerationEncoder[CsvCharacter]
  implicit lazy val csvCharacterDecoder: Decoder[CsvCharacter] = deriveEnumerationDecoder[CsvCharacter]

  implicit lazy val (
    kafkaOffsetCommittingSchema: Schema[KafkaOffsetCommitting],
    kafkaOffsetCommittingEncoder: Encoder[KafkaOffsetCommitting],
    kafkaOffsetCommittingDecoder: Decoder[KafkaOffsetCommitting],
  ) = {
    implicit val config = ingestSourceTypeConfig
    (
      Schema.derived[KafkaOffsetCommitting],
      deriveConfiguredEncoder[KafkaOffsetCommitting],
      deriveConfiguredDecoder[KafkaOffsetCommitting],
    )
  }

  implicit val (
    kafkaSecurityProtocolSchema: Schema[KafkaSecurityProtocol],
    encodeKafkaSecurityProtocol: Encoder[KafkaSecurityProtocol],
    decodeKafkaSecurityProtocol: Decoder[KafkaSecurityProtocol],
  ) = {
    val schema: Schema[KafkaSecurityProtocol] = Schema.derived
    val encoder: Encoder[KafkaSecurityProtocol] = encodeString.contramap(_.name)
    val decoder: Decoder[KafkaSecurityProtocol] = Decoder.decodeString.emap {
      case s if s == KafkaSecurityProtocol.PlainText.name => KafkaSecurityProtocol.PlainText.asRight
      case s if s == KafkaSecurityProtocol.Ssl.name => KafkaSecurityProtocol.Ssl.asRight
      case s if s == KafkaSecurityProtocol.Sasl_Ssl.name => KafkaSecurityProtocol.Sasl_Ssl.asRight
      case s if s == KafkaSecurityProtocol.Sasl_Plaintext.name => KafkaSecurityProtocol.Sasl_Plaintext.asRight
      case s => Left(s"$s is not a valid KafkaSecurityProtocol")
    }
    (schema, encoder, decoder)
  }

  implicit lazy val (
    ingestSourceTypeSchema: Schema[IngestSource],
    ingestSourceTypeEncoder: Encoder[IngestSource],
    ingestSourceTypeDecoder: Decoder[IngestSource],
  ) = {
    implicit val config = ingestSourceTypeConfig
    (
      Schema.derived[IngestSource],
      deriveConfiguredEncoder[IngestSource],
      deriveConfiguredDecoder[IngestSource],
    )
  }

  implicit lazy val (
    keepaliveProtocolSchema: Schema[WebsocketSimpleStartupIngest.KeepaliveProtocol],
    keepaliveProtocolEncoder: Encoder[WebsocketSimpleStartupIngest.KeepaliveProtocol],
    keepaliveProtocolDecoder: Decoder[WebsocketSimpleStartupIngest.KeepaliveProtocol],
  ) = {
    implicit val config = ingestSourceTypeConfig
    (
      Schema.derived[WebsocketSimpleStartupIngest.KeepaliveProtocol],
      deriveConfiguredEncoder[WebsocketSimpleStartupIngest.KeepaliveProtocol],
      deriveConfiguredDecoder[WebsocketSimpleStartupIngest.KeepaliveProtocol],
    )
  }

  implicit lazy val (
    iteratorTypeSchema: Schema[KinesisIngest.IteratorType],
    iteratorTypeEncoder: Encoder[KinesisIngest.IteratorType],
    iteratorTypeDecoder: Decoder[KinesisIngest.IteratorType],
  ) = {
    implicit val config = ingestSourceTypeConfig
    (
      Schema.derived[KinesisIngest.IteratorType],
      deriveConfiguredEncoder[KinesisIngest.IteratorType],
      deriveConfiguredDecoder[KinesisIngest.IteratorType],
    )
  }

  implicit lazy val (
    fileFormatSchema: Schema[FileFormat],
    fileFormatEncoder: Encoder[FileFormat],
    fileFormatDecoder: Decoder[FileFormat],
  ) = {
    implicit val config = ingestSourceTypeConfig
    (
      Schema.derived[FileFormat],
      deriveConfiguredEncoder[FileFormat],
      deriveConfiguredDecoder[FileFormat],
    )
  }

  implicit lazy val (
    streamingFormatTypeSchema: Schema[StreamingFormat],
    streamingFormatEncoder: Encoder[StreamingFormat],
    streamingFormatDecoder: Decoder[StreamingFormat],
  ) = {
    implicit val config = ingestSourceTypeConfig
    (
      Schema.derived[StreamingFormat],
      deriveConfiguredEncoder[StreamingFormat],
      deriveConfiguredDecoder[StreamingFormat],
    )
  }

  implicit lazy val (
    onRecordErrorHandlerEncoder: Encoder[OnRecordErrorHandler],
    onRecordErrorHandlerDecoder: Decoder[OnRecordErrorHandler],
  ) = {
    implicit val config = ingestSourceTypeConfig
    (
      deriveConfiguredEncoder[OnRecordErrorHandler],
      deriveConfiguredDecoder[OnRecordErrorHandler],
    )
  }

  implicit lazy val (
    onStreamErrorHandlerSchema: Schema[OnStreamErrorHandler],
    onStreamErrorHandlerEncoder: Encoder[OnStreamErrorHandler],
    onStreamErrorHandlerDecoder: Decoder[OnStreamErrorHandler],
  ) = {
    implicit val config = ingestSourceTypeConfig
    (
      Schema.derived[OnStreamErrorHandler].description("Action to take on stream error"),
      deriveConfiguredEncoder[OnStreamErrorHandler],
      deriveConfiguredDecoder[OnStreamErrorHandler],
    )
  }

  implicit lazy val (
    quineIngestConfigurationSchema: Schema[Oss.QuineIngestConfiguration],
    quineIngestConfigurationEncoder: Encoder[Oss.QuineIngestConfiguration],
    quineIngestConfigurationDecoder: Decoder[Oss.QuineIngestConfiguration],
  ) = {
    implicit val config = ingestSourceTypeConfig
    (
      Schema.derived[Oss.QuineIngestConfiguration],
      deriveConfiguredEncoder[Oss.QuineIngestConfiguration],
      deriveConfiguredDecoder[Oss.QuineIngestConfiguration],
    )
  }

}
