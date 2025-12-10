package com.thatdot.api.v2.outputs

import io.circe.generic.extras.Configuration
import io.circe.generic.extras.semiauto.{deriveConfiguredDecoder, deriveConfiguredEncoder}
import io.circe.{Decoder, Encoder}
import sttp.tapir.Schema.annotations.{description, encodedExample, title}

import com.thatdot.api.v2.schema.V2ApiConfiguration._

@title("Result Output Format")
sealed trait OutputFormat

object OutputFormat {
  implicit val circeConfig: Configuration = typeDiscriminatorConfig.asCirce

  @title("JSON")
  @encodedExample("JSON")
  case object JSON extends OutputFormat

  @title("Protobuf")
  @encodedExample("""{
      |  "type": "Protobuf",
      |  "schemaUrl": "conf/protobuf-schemas/example_schema.desc",
      |  "typeName": "ExampleType"
      |}""".stripMargin)
  final case class Protobuf(
    @description(
      "URL (or local filename) of the Protobuf .desc file to load that contains the desired typeName to serialize to",
    )
    @encodedExample("conf/protobuf-schemas/example_schema.desc")
    schemaUrl: String,
    @description("message type name to use (from the given .desc file) as the message type")
    @encodedExample("ExampleType")
    typeName: String,
  ) extends OutputFormat

  implicit val encoder: Encoder[OutputFormat] = deriveConfiguredEncoder
  implicit val decoder: Decoder[OutputFormat] = deriveConfiguredDecoder
}
