package com.thatdot.api.v2.schema

import io.circe.syntax.EncoderOps
import io.circe.{Decoder, Encoder}
import org.polyvariant.sttp.oauth2.Secret
import sttp.tapir.Schema
import sttp.tapir.generic.auto._

import com.thatdot.api.v2.outputs.DestinationSteps.KafkaPropertyValue

trait V2ApiSchemas extends V2ApiConfiguration {

  // Kafka Property Value codec, hand-rolled for like-a-String representation instead of as-an-Object with a field
  implicit val kafkaPropertyValueEncoder: Encoder[KafkaPropertyValue] = Encoder.encodeString.contramap(_.s)
  implicit val kafkaPropertyValueDecoder: Decoder[KafkaPropertyValue] =
    Decoder.decodeString.map(KafkaPropertyValue.apply)

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

  implicit val secretStringEncoder: Encoder[Secret[String]] = Encoder.encodeString.contramap(_.toString)
  implicit val secretStringDecoder: Decoder[Secret[String]] = Decoder.decodeString.map(Secret(_))
  implicit val secretStringSchema: Schema[Secret[String]] = Schema.string[Secret[String]]
}

object V2ApiSchemas extends V2ApiSchemas
