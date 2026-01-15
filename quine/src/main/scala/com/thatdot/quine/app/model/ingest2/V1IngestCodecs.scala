package com.thatdot.quine.app.model.ingest2

import cats.implicits.catsSyntaxEitherId
import io.circe.Encoder.encodeString
import io.circe.generic.extras.semiauto.{
  deriveConfiguredDecoder,
  deriveConfiguredEncoder,
  deriveEnumerationDecoder,
  deriveEnumerationEncoder,
}
import io.circe.{Decoder, Encoder}

import com.thatdot.api.v2.TypeDiscriminatorConfig.instances.circeConfig
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

  implicit val csvCharacterEncoder: Encoder[V1.CsvCharacter] = deriveEnumerationEncoder
  implicit val csvCharacterDecoder: Decoder[V1.CsvCharacter] = deriveEnumerationDecoder

  implicit val recordDecodingTypeEncoder: Encoder[V1.RecordDecodingType] = deriveEnumerationEncoder
  implicit val recordDecodingTypeDecoder: Decoder[V1.RecordDecodingType] = deriveEnumerationDecoder

  implicit val fileIngestModeEncoder: Encoder[V1.FileIngestMode] = deriveEnumerationEncoder
  implicit val fileIngestModeDecoder: Decoder[V1.FileIngestMode] = deriveEnumerationDecoder

  implicit val kafkaAutoOffsetResetEncoder: Encoder[V1.KafkaAutoOffsetReset] = deriveEnumerationEncoder
  implicit val kafkaAutoOffsetResetDecoder: Decoder[V1.KafkaAutoOffsetReset] = deriveEnumerationDecoder

  implicit val ingestStreamStatusEncoder: Encoder[V1.IngestStreamStatus] = deriveEnumerationEncoder
  implicit val ingestStreamStatusDecoder: Decoder[V1.IngestStreamStatus] = deriveEnumerationDecoder

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

  implicit val kafkaOffsetCommittingEncoder: Encoder[V1.KafkaOffsetCommitting] = deriveConfiguredEncoder
  implicit val kafkaOffsetCommittingDecoder: Decoder[V1.KafkaOffsetCommitting] = deriveConfiguredDecoder

  implicit val awsCredentialsEncoder: Encoder[V1.AwsCredentials] = deriveConfiguredEncoder
  implicit val awsCredentialsDecoder: Decoder[V1.AwsCredentials] = deriveConfiguredDecoder

  implicit val awsRegionEncoder: Encoder[V1.AwsRegion] = deriveConfiguredEncoder
  implicit val awsRegionDecoder: Decoder[V1.AwsRegion] = deriveConfiguredDecoder

  implicit val keepaliveProtocolEncoder: Encoder[V1.WebsocketSimpleStartupIngest.KeepaliveProtocol] =
    deriveConfiguredEncoder
  implicit val keepaliveProtocolDecoder: Decoder[V1.WebsocketSimpleStartupIngest.KeepaliveProtocol] =
    deriveConfiguredDecoder

  implicit val kinesisIteratorTypeEncoder: Encoder[V1.KinesisIngest.IteratorType] = deriveConfiguredEncoder
  implicit val kinesisIteratorTypeDecoder: Decoder[V1.KinesisIngest.IteratorType] = deriveConfiguredDecoder
}
