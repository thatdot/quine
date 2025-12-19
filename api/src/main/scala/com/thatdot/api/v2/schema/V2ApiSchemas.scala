package com.thatdot.api.v2.schema

import io.circe.syntax.EncoderOps
import sttp.tapir.Schema

import com.thatdot.api.v2.outputs.DestinationSteps.KafkaPropertyValue

trait V2ApiSchemas extends V2ApiConfiguration {

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
      // Cannot get `.default` to work here
      .schemaForMap[KafkaPropertyValue](KafkaPropertyValue.schema)
      .encodedExample(exampleKafkaProperties.asJson)
}

object V2ApiSchemas extends V2ApiSchemas
