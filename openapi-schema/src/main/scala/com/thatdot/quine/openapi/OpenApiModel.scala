package com.thatdot.quine.openapi

import io.circe.Json

/** Minimal subset of OpenAPI 3.0 needed to drive spec-driven forms.
  * Not a full OpenAPI parser — just the pieces relevant to rendering
  * forms from JSON Schema definitions and making API calls.
  */

/** A node in a JSON Schema tree. Mirrors the subset of JSON Schema
  * used in OpenAPI 3.0 component schemas.
  */
final case class SchemaNode(
  typ: Option[String] = None,
  title: Option[String] = None,
  description: Option[String] = None,
  default: Option[Json] = None,
  `enum`: Option[List[Json]] = None,
  format: Option[String] = None,
  properties: Option[Map[String, SchemaNode]] = None,
  required: Option[Set[String]] = None,
  additionalProperties: Option[Either[Boolean, SchemaNode]] = None,
  items: Option[SchemaNode] = None,
  oneOf: Option[List[SchemaNode]] = None,
  allOf: Option[List[SchemaNode]] = None,
  anyOf: Option[List[SchemaNode]] = None,
  discriminator: Option[Discriminator] = None,
  ref: Option[String] = None,
  nullable: Boolean = false,
  // Name under `components.schemas` when the node originated from the top-level
  // schema table. Populated by the parser, preserved through `resolveRef` so
  // downstream code (UI hints, renderers) can identify which named schema is
  // being rendered even after a ref has been followed.
  schemaName: Option[String] = None,
)

final case class Discriminator(
  propertyName: String,
  mapping: Option[Map[String, String]] = None,
)

/** A single API endpoint extracted from the OpenAPI spec paths. */
final case class ApiEndpoint(
  method: String,
  path: String,
  summary: Option[String] = None,
  operationId: Option[String] = None,
  requestBodySchema: Option[SchemaNode] = None,
  responseSchema: Option[SchemaNode] = None,
  pathParams: List[String] = Nil,
  queryParams: List[QueryParam] = Nil,
)

final case class QueryParam(
  name: String,
  required: Boolean = false,
  schema: Option[SchemaNode] = None,
)

/** The result of parsing an OpenAPI spec — the schemas and endpoints we care about.
  *
  * `hints` is an overlay of UI rendering preferences (field order, promote-to-primary,
  * hide) that the renderer consults. Defaults to [[UiHintsSource.empty]]; callers
  * attach a loaded overlay via `copy(hints = ...)` after parsing.
  */
final case class ParsedSpec(
  schemas: Map[String, SchemaNode],
  endpoints: List[ApiEndpoint],
  hints: UiHintsSource = UiHintsSource.empty,
)
