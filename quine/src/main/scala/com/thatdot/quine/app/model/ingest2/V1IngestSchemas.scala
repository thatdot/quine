package com.thatdot.quine.app.model.ingest2

import sttp.tapir.Schema

import com.thatdot.quine.{routes => V1}

/** Tapir schemas for V1 routes types used by V2 ingest.
  *
  * These types are defined in quine-endpoints which doesn't have Tapir dependencies,
  * so schemas are provided here.
  *
  * For Circe codecs, see [[V1IngestCodecs]].
  *
  * Usage:
  * {{{
  * import com.thatdot.quine.app.model.ingest2.V1IngestSchemas._
  * }}}
  */
object V1IngestSchemas {
  implicit val csvCharacterSchema: Schema[V1.CsvCharacter] = Schema.derived[V1.CsvCharacter]
  implicit val recordDecodingTypeSchema: Schema[V1.RecordDecodingType] = Schema.derived[V1.RecordDecodingType]
  implicit val fileIngestModeSchema: Schema[V1.FileIngestMode] = Schema.derived[V1.FileIngestMode]
  implicit val kafkaSecurityProtocolSchema: Schema[V1.KafkaSecurityProtocol] = Schema.derived[V1.KafkaSecurityProtocol]
  implicit val kafkaAutoOffsetResetSchema: Schema[V1.KafkaAutoOffsetReset] = Schema.derived[V1.KafkaAutoOffsetReset]
  implicit val kafkaOffsetCommittingSchema: Schema[V1.KafkaOffsetCommitting] = Schema.derived[V1.KafkaOffsetCommitting]
  implicit val awsCredentialsSchema: Schema[V1.AwsCredentials] = Schema.derived[V1.AwsCredentials]
  implicit val awsRegionSchema: Schema[V1.AwsRegion] = Schema.derived[V1.AwsRegion]
  implicit val keepaliveProtocolSchema: Schema[V1.WebsocketSimpleStartupIngest.KeepaliveProtocol] =
    Schema.derived[V1.WebsocketSimpleStartupIngest.KeepaliveProtocol]
  implicit val kinesisIteratorTypeSchema: Schema[V1.KinesisIngest.IteratorType] =
    Schema.derived[V1.KinesisIngest.IteratorType]

  implicit val recordDecoderSeqSchema: Schema[Seq[V1.RecordDecodingType]] =
    Schema.schemaForArray(recordDecodingTypeSchema).map(a => Some(a.toSeq))(s => s.toArray)
}
