package com.thatdot.api.v2.outputs

import sttp.tapir.Schema.annotations.{description, encodedExample, title}

@title("Result Output Format")
sealed trait OutputFormat

object OutputFormat {
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
}
