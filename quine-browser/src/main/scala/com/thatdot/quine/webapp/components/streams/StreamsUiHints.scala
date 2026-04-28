package com.thatdot.quine.webapp.components.streams

import com.thatdot.quine.openapi.UiHintsSource

/** UI rendering overlay for the Streams forms. Keys are schema names
  * from the V2 OpenAPI document's `components.schemas`; values are per-schema
  * [[com.thatdot.quine.openapi.UiHints]] written as JSON so the shape
  * parallels the spec and is easy to extend from documentation.
  *
  * Supported keys per schema (all optional, all additive):
  *   - `order`   : explicit field order; unlisted fields follow in spec order,
  *                 so new schema properties render automatically.
  *   - `promote` : field names forced into the "primary" (non-collapsed) bucket
  *                 even when the schema would consider them optional.
  *   - `hide`    : field names to omit from the rendered form.
  *
  * Drift between these hints and the current spec is reported as a browser
  * console warning at startup (see [[UiHintsSource.checkDrift]]).
  */
object StreamsUiHints {

  private val rawJson: String =
    """{
      |  "StandingQueryDefinition": {
      |    "order": ["name", "pattern", "outputs", "includeCancellations", "inputBufferSize"],
      |    "promote": ["outputs"]
      |  },
      |  "Cypher": {
      |    "order": ["query", "mode"],
      |    "promote": ["mode"]
      |  },
      |  "StandingQueryResultWorkflow": {
      |    "order": ["name", "resultEnrichment", "destinations", "filter", "preEnrichmentTransformation"],
      |    "promote": ["destinations", "resultEnrichment"],
      |    "labels": {
      |      "resultEnrichment": "Enrichment Query"
      |    }
      |  }
      |}""".stripMargin

  val source: UiHintsSource = UiHintsSource
    .parse(rawJson)
    .getOrElse(throw new RuntimeException("Invalid built-in Streams UI hints"))
}
