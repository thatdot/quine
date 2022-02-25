package com.thatdot.quine.routes.exts

import ujson.Value

/** Add a schema for untyped JSON */
trait AnySchema extends endpoints4s.algebra.JsonSchemas {

  /** Schema for any JSON value. Use this to duck under `endpoints` :) */
  def anySchema(format: Option[String]): JsonSchema[ujson.Value]

  /** Schema for an optional value
    *
    * @note schemas like this cannot be derived because some cases, such as a nested `Option`,
    * won't roundtrip between serialization and deserialization.
    */
  def optionalSchema[A](implicit schema: JsonSchema[A]): JsonSchema[Option[A]]
}

/** Implementation of [[AnySchema]] for `ujson`-backed schemas */
trait UjsonAnySchema extends AnySchema with endpoints4s.ujson.JsonSchemas {

  def anySchema(format: Option[String]): JsonSchema[Value] = new JsonSchema[ujson.Value] {
    val encoder = (value: ujson.Value) => value
    val decoder = (value: ujson.Value) => endpoints4s.Valid(value)
  }

  def optionalSchema[A](implicit schema: JsonSchema[A]): JsonSchema[Option[A]] = new JsonSchema[Option[A]] {
    val encoder = _.fold[ujson.Value](ujson.Null)(schema.encoder.encode)
    val decoder = {
      case ujson.Null => endpoints4s.Valid(None)
      case other => schema.decoder.decode(other).map(Some(_))
    }
  }
}

/** Implementation of [[AnySchema]] for OpenAPI schemas */
trait OpenApiAnySchema extends AnySchema with endpoints4s.openapi.JsonSchemas {

  def anySchema(format: Option[String]): JsonSchema[Value] = {

    val docs = DocumentedJsonSchema.Primitive(
      name = "", // TODO: really we want to just omit this, but =.=
      format,
      example = None
    )

    val schema = new ujsonSchemas.JsonSchema[ujson.Value] {
      val encoder = (value: ujson.Value) => value
      val decoder = (value: ujson.Value) => endpoints4s.Valid(value)
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
