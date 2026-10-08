package com.thatdot.quine.webapp.components.streams

import io.circe.Json

import com.thatdot.quine.openapi.{QueryParam, SchemaNode}

/** Folds an endpoint's HTTP query parameters into a schema-driven create form.
  *
  * The create endpoints carry request-shaping options as query parameters rather
  * than request-body fields — e.g. a standing query's `propagateTo` and
  * `wakeUpParallelism`. Rather than render a separate section, these are merged
  * into the request schema as optional properties so they appear inside the
  * form's existing "More Options" collapsible alongside the other optional body
  * fields. On submit they are split back out of the assembled form state: their
  * values ride as URL query parameters and are stripped from the request body.
  *
  * Ingest create has no such parameters, so every operation here is a no-op for
  * it — the wiring is present for parity with the standing-query form.
  */
object QueryParamFields {

  /** Query parameters that can drive a form field (those carrying a schema). */
  private def renderable(params: List[QueryParam]): List[(String, SchemaNode)] =
    params.flatMap(p => p.schema.map(p.name -> _))

  /** Merge the query parameters into `schema` as optional properties, so the
    * form renderer places them in its "More Options" section. They are left out
    * of `required`, so they render as optional and their schema defaults seed
    * form state.
    */
  def mergeInto(schema: SchemaNode, params: List[QueryParam]): SchemaNode = {
    val extraProps = renderable(params).toMap
    if (extraProps.isEmpty) schema
    else schema.copy(properties = Some(schema.properties.getOrElse(Map.empty) ++ extraProps))
  }

  /** Pull the query-parameter values out of assembled form state into URL
    * query-string parameters. Null entries are dropped (unset); strings pass
    * through verbatim while other primitives (numbers, booleans) serialize
    * without JSON quoting.
    */
  def extract(state: Json, params: List[QueryParam]): Map[String, String] = {
    val names = renderable(params).map(_._1).toSet
    state.asObject
      .map(_.toMap.collect {
        case (key, value) if names.contains(key) && !value.isNull =>
          key -> value.asString.getOrElse(value.noSpaces)
      })
      .getOrElse(Map.empty)
  }

  /** Remove the query-parameter keys from the request body — they travel as query
    * string, not body fields.
    */
  def stripFrom(body: Json, params: List[QueryParam]): Json = {
    val names = renderable(params).map(_._1)
    body.asObject.map(obj => Json.fromJsonObject(names.foldLeft(obj)(_.remove(_)))).getOrElse(body)
  }
}
