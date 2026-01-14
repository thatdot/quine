package com.thatdot.quine.app.model.ingest2

import cats.implicits.catsSyntaxEitherId
import io.circe.Encoder.encodeString
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.semiauto.{
  deriveConfiguredDecoder,
  deriveConfiguredEncoder,
  deriveEnumerationDecoder,
  deriveEnumerationEncoder,
}
import io.circe.{Decoder, Encoder}

import com.thatdot.api.v2.schema.V2ApiConfiguration.typeDiscriminatorConfig
import com.thatdot.quine.{routes => V1}

/** Circe encoders and decoders for V1 routes types used by V2 ingest.
  *
  * These types are defined in quine-endpoints which doesn't have Circe dependencies,
  * so codecs are provided here.
  *
  * For Tapir schemas, see [[V1IngestSchemas]].
  *
  * Usage:
  * {{{
  * import com.thatdot.quine.app.model.ingest2.V1IngestCodecs._
  * }}}
  */
object V1IngestCodecs {
  implicit private val config: Configuration = typeDiscriminatorConfig.asCirce

  implicit val csvCharacterEncoder: Encoder[V1.CsvCharacter] = deriveEnumerationEncoder[V1.CsvCharacter]
  implicit val csvCharacterDecoder: Decoder[V1.CsvCharacter] = deriveEnumerationDecoder[V1.CsvCharacter]

  implicit val recordDecodingTypeEncoder: Encoder[V1.RecordDecodingType] =
    deriveEnumerationEncoder[V1.RecordDecodingType]
  implicit val recordDecodingTypeDecoder: Decoder[V1.RecordDecodingType] =
    deriveEnumerationDecoder[V1.RecordDecodingType]

  implicit val fileIngestModeEncoder: Encoder[V1.FileIngestMode] = deriveEnumerationEncoder[V1.FileIngestMode]
  implicit val fileIngestModeDecoder: Decoder[V1.FileIngestMode] = deriveEnumerationDecoder[V1.FileIngestMode]

  implicit val kafkaAutoOffsetResetEncoder: Encoder[V1.KafkaAutoOffsetReset] =
    deriveEnumerationEncoder[V1.KafkaAutoOffsetReset]
  implicit val kafkaAutoOffsetResetDecoder: Decoder[V1.KafkaAutoOffsetReset] =
    deriveEnumerationDecoder[V1.KafkaAutoOffsetReset]

  implicit val ingestStreamStatusEncoder: Encoder[V1.IngestStreamStatus] =
    deriveEnumerationEncoder[V1.IngestStreamStatus]
  implicit val ingestStreamStatusDecoder: Decoder[V1.IngestStreamStatus] =
    deriveEnumerationDecoder[V1.IngestStreamStatus]

  // KafkaSecurityProtocol uses custom codec for name mapping
  implicit val kafkaSecurityProtocolEncoder: Encoder[V1.KafkaSecurityProtocol] =
    encodeString.contramap[V1.KafkaSecurityProtocol](_.name)
  implicit val kafkaSecurityProtocolDecoder: Decoder[V1.KafkaSecurityProtocol] = Decoder.decodeString.emap {
    case s if s == V1.KafkaSecurityProtocol.PlainText.name => V1.KafkaSecurityProtocol.PlainText.asRight
    case s if s == V1.KafkaSecurityProtocol.Ssl.name => V1.KafkaSecurityProtocol.Ssl.asRight
    case s if s == V1.KafkaSecurityProtocol.Sasl_Ssl.name => V1.KafkaSecurityProtocol.Sasl_Ssl.asRight
    case s if s == V1.KafkaSecurityProtocol.Sasl_Plaintext.name => V1.KafkaSecurityProtocol.Sasl_Plaintext.asRight
    case s => Left(s"$s is not a valid KafkaSecurityProtocol")
  }

  implicit val kafkaOffsetCommittingEncoder: Encoder[V1.KafkaOffsetCommitting] =
    deriveConfiguredEncoder[V1.KafkaOffsetCommitting]
  implicit val kafkaOffsetCommittingDecoder: Decoder[V1.KafkaOffsetCommitting] =
    deriveConfiguredDecoder[V1.KafkaOffsetCommitting]

  implicit val awsCredentialsEncoder: Encoder[V1.AwsCredentials] = deriveConfiguredEncoder[V1.AwsCredentials]
  implicit val awsCredentialsDecoder: Decoder[V1.AwsCredentials] = deriveConfiguredDecoder[V1.AwsCredentials]

  implicit val awsRegionEncoder: Encoder[V1.AwsRegion] = deriveConfiguredEncoder[V1.AwsRegion]
  implicit val awsRegionDecoder: Decoder[V1.AwsRegion] = deriveConfiguredDecoder[V1.AwsRegion]

  implicit val keepaliveProtocolEncoder: Encoder[V1.WebsocketSimpleStartupIngest.KeepaliveProtocol] =
    deriveConfiguredEncoder[V1.WebsocketSimpleStartupIngest.KeepaliveProtocol]
  implicit val keepaliveProtocolDecoder: Decoder[V1.WebsocketSimpleStartupIngest.KeepaliveProtocol] =
    deriveConfiguredDecoder[V1.WebsocketSimpleStartupIngest.KeepaliveProtocol]

  implicit val kinesisIteratorTypeEncoder: Encoder[V1.KinesisIngest.IteratorType] =
    deriveConfiguredEncoder[V1.KinesisIngest.IteratorType]
  implicit val kinesisIteratorTypeDecoder: Decoder[V1.KinesisIngest.IteratorType] =
    deriveConfiguredDecoder[V1.KinesisIngest.IteratorType]
}
