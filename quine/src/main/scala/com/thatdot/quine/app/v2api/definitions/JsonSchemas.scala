package com.thatdot.quine.app.v2api.definitions

import io.circe.Json
import sttp.tapir.Schema

/** Tapir schemas for Circe JSON-related types, for reusability. */
trait JsonSchemas {
  implicit val jsonSchema: Schema[Json] = Schema.any[Json]
  implicit lazy val mapStringJsonSchema: Schema[Map[String, Json]] = Schema.schemaForMap[String, Json](identity)
  implicit val seqSeqJsonSchema: Schema[Seq[Seq[Json]]] = jsonSchema.asIterable[Seq].asIterable[Seq]
}
