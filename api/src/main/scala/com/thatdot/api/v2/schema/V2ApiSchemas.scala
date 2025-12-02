package com.thatdot.api.v2.schema

import io.circe.syntax.EncoderOps
import io.circe.{Decoder, Encoder}
import sttp.tapir.Schema

import com.thatdot.api.v2.outputs.DestinationSteps.KafkaPropertyValue
import com.thatdot.api.v2.{RatesSummary, SuccessEnvelope}

trait V2ApiSchemas extends V2ApiConfiguration {

  // Kafka Property Value codec, hand-rolled for like-a-String representation instead of as-an-Object with a field
  implicit lazy val kafkaPropertyValueEncoder: Encoder[KafkaPropertyValue] = Encoder.encodeString.contramap(_.s)
  implicit lazy val kafkaPropertyValueDecoder: Decoder[KafkaPropertyValue] =
    Decoder.decodeString.map(KafkaPropertyValue.apply)
  implicit lazy val kafkaPropertyValueSchema: Schema[KafkaPropertyValue] = Schema.derived

  // Attempting schema for type alias to string without overriding all Map_String
  implicit class StringKafkaPropertyValue(s: String) {
    def toKafkaPropertyValue: KafkaPropertyValue = KafkaPropertyValue(s)
  }
  private val exampleKafkaProperties: Map[String, KafkaPropertyValue] = Map(
    "security.protocol" -> "SSL",
    "ssl.keystore.type" -> "PEM",
    "ssl.keystore.certificate.chain" -> "/path/to/file/containing/certificate/chain",
    "ssl.key.password" -> "private_key_password",
    "ssl.truststore.type" -> "PEM",
    "ssl.truststore.certificates" -> "/path/to/truststore/certificate",
  ).view.mapValues(_.toKafkaPropertyValue).toMap
  implicit lazy val stringKafkaPropertyMapSchema: Schema[Map[String, KafkaPropertyValue]] =
    Schema
      .schemaForMap[KafkaPropertyValue]
      // Cannot get `.default` to work here
      .encodedExample(exampleKafkaProperties.asJson)

  implicit lazy val ratesSummarySchema: Schema[RatesSummary] = Schema.derived

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // Explicit Schema definitions for SuccessEnvelope wrappers to avoid repeated automatic derivation (QU-2417)
  // Notably, for this to be effective, this file MUST NOT import `sttp.tapir.generic.auto._`.
  // (This may not be the ideal long-term organization, but it's low-hanging fruit for faster compilation).
  implicit def okSchema[A](implicit inner: Schema[A]): Schema[SuccessEnvelope.Ok[A]] = Schema.derived
  implicit def createdSchema[A](implicit inner: Schema[A]): Schema[SuccessEnvelope.Created[A]] = Schema.derived
  implicit lazy val acceptedSchema: Schema[SuccessEnvelope.Accepted] = Schema.derived
  implicit lazy val noContentSchema: Schema[SuccessEnvelope.NoContent.type] = Schema.derived

}

object V2ApiSchemas extends V2ApiSchemas
