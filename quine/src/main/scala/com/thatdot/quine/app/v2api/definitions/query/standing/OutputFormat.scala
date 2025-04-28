package com.thatdot.quine.app.v2api.definitions.query.standing

import sttp.tapir.Schema.annotations.{description, title}

@title("Standing Query Result Output Format")
sealed trait OutputFormat

object OutputFormat {
  @title("JSON")
  case object JSON extends OutputFormat

  @title("Protobuf")
  final case class Protobuf(
    @description(
      "URL (or local filename) of the Protobuf .desc file to load that contains the desired typeName to serialize to",
    )
    schemaUrl: String,
    @description("message type name to use (from the given .desc file) as the message type")
    typeName: String,
  ) extends OutputFormat
}
