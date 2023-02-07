package com.thatdot.quine.routes.exts

import io.circe.{Decoder, Encoder, Json}
import ujson.circe.CirceJson

/** Add a schema for untyped JSON */
trait AnySchema extends endpoints4s.algebra.JsonSchemas {

  /** Schema for any JSON value. Use this to duck under `endpoints` :) */
  def anySchema(format: Option[String]): JsonSchema[Json]

  /** Schema for an optional value
    *
    * @note schemas like this cannot be derived because some cases, such as a nested `Option`,
    * won't roundtrip between serialization and deserialization.
    */
  def optionalSchema[A](implicit schema: JsonSchema[A]): JsonSchema[Option[A]]
}
trait CirceJsonAnySchema extends AnySchema with endpoints4s.circe.JsonSchemas {
  def anySchema(format: Option[String]): JsonSchema[Json] = JsonSchema(
    Encoder.instance(identity),
    Decoder.instance(c => Right(c.value))
  )

  def optionalSchema[A](implicit schema: JsonSchema[A]): JsonSchema[Option[A]] = JsonSchema(
    _.fold(Json.Null)(schema.encoder.apply),
    json => if (json.value.isNull) Right(None) else schema.decoder(json).map(Some(_))
  )
}

/** Implementation of [[AnySchema]] for OpenAPI schemas */
trait OpenApiAnySchema extends AnySchema with endpoints4s.openapi.JsonSchemas {

  def anySchema(format: Option[String]): JsonSchema[Json] = {

    val docs = DocumentedJsonSchema.Primitive(
      name = "", // TODO: really we want to just omit this, but =.=
      format,
      example = None
    )

    val schema = new ujsonSchemas.JsonSchema[Json] {
      val encoder = CirceJson.transform(_, ujson.Value)
      val decoder = (value: ujson.Value) => endpoints4s.Valid(value.transform(CirceJson))
    }

    new JsonSchema(schema, docs)
  }

  def optionalSchema[A](implicit schema: JsonSchema[A]): JsonSchema[Option[A]] = {

    val optSchema = new ujsonSchemas.JsonSchema[Option[A]] {
      val encoder = _.fold[ujson.Value](ujson.Null)(schema.ujsonSchema.encoder.encode)
      val decoder = {
        case ujson.Null => endpoints4s.Valid(None)
        case other => schema.ujsonSchema.decoder.decode(other).map(Some(_))
      }
    }

    new JsonSchema(optSchema, schema.docs)
  }
}
