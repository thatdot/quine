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
  implicit lazy val csvCharacterSchema: Schema[V1.CsvCharacter] = Schema.derived
  implicit lazy val recordDecodingTypeSchema: Schema[V1.RecordDecodingType] = Schema.derived
  implicit lazy val fileIngestModeSchema: Schema[V1.FileIngestMode] = Schema.derived
  implicit lazy val kafkaSecurityProtocolSchema: Schema[V1.KafkaSecurityProtocol] = Schema.derived
  implicit lazy val kafkaAutoOffsetResetSchema: Schema[V1.KafkaAutoOffsetReset] = Schema.derived
  implicit lazy val kafkaOffsetCommittingSchema: Schema[V1.KafkaOffsetCommitting] = Schema.derived
  implicit lazy val awsCredentialsSchema: Schema[V1.AwsCredentials] = Schema.derived
  implicit lazy val awsRegionSchema: Schema[V1.AwsRegion] = Schema.derived
  implicit lazy val keepaliveProtocolSchema: Schema[V1.WebsocketSimpleStartupIngest.KeepaliveProtocol] = Schema.derived
  implicit lazy val kinesisIteratorTypeSchema: Schema[V1.KinesisIngest.IteratorType] = Schema.derived

  implicit lazy val recordDecoderSeqSchema: Schema[Seq[V1.RecordDecodingType]] =
    Schema.schemaForArray(recordDecodingTypeSchema).map(a => Some(a.toSeq))(s => s.toArray)
}
