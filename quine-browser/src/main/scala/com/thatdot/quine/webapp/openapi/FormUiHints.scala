package com.thatdot.quine.webapp.openapi

import com.thatdot.quine.openapi.UiHintsSource

/** UI rendering overlay for every schema-generated form in the app. Keys are schema names
  * from the V2 OpenAPI document's `components.schemas`; values are per-schema
  * [[com.thatdot.quine.openapi.UiHints]] written as JSON so the shape
  * parallels the spec and is easy to extend from documentation.
  *
  * Lives beside [[ApiSpecCache]] rather than with any one page because the surfaces that render
  * these schemas are spread across the app — the Streams page's create forms and the query bar's
  * run-in-background dialog — and they load the spec through that one shared cache. A hint added
  * for one of them is attached to the document all of them see.
  *
  * Supported keys per schema (all optional, all additive):
  *   - `order`   : explicit field order; unlisted fields follow in spec order,
  *                 so new schema properties render automatically.
  *   - `promote` : field names forced into the "primary" (non-collapsed) bucket
  *                 even when the schema would consider them optional.
  *   - `hide`    : field names to omit from the rendered form. A hidden field the server still
  *                 needs must be supplied by the form that owns it — `BackgroundQuery.namespace`
  *                 is hidden here and filled from the Streams page's graph selector by
  *                 `CreateJobForm`.
  *   - `labels`       : per-field human-facing label, overriding the schema's title.
  *   - `descriptions` : per-field help text, overriding the schema's `description`. Use this to
  *                      keep the form terse while the API reference (the schema) stays thorough.
  *   - `placeholders` : per-field greyed placeholder text (e.g. `168h` on a duration field).
  *
  * Drift between these hints and the current spec is reported as a browser
  * console warning at startup (see [[UiHintsSource.checkDrift]]).
  */
object FormUiHints {

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
      |  },
      |  "CreateJobRequest": {
      |    "order": ["name", "schedule", "action", "updateIfExists"],
      |    "promote": ["schedule", "action", "updateIfExists"],
      |    "labels": {
      |      "updateIfExists": "Replace a job that already has this name"
      |    }
      |  },
      |  "Interval": {
      |    "order": ["every", "startAt"],
      |    "promote": ["every", "startAt"]
      |  },
      |  "BackgroundQueryDef": {
      |    "order": ["query", "destinations", "name", "statusExpiry", "parameters"],
      |    "promote": ["destinations"],
      |    "labels": {
      |      "statusExpiry": "Keep status for"
      |    },
      |    "descriptions": {
      |      "name": "Optional human-readable name",
      |      "statusExpiry": "How long the status record is kept after the query ends."
      |    },
      |    "placeholders": {
      |      "statusExpiry": "168h"
      |    }
      |  },
      |  "BackgroundQuery": {
      |    "order": ["query", "destinations", "name", "parameters", "statusExpiry"],
      |    "promote": ["query", "destinations"],
      |    "hide": ["namespace"],
      |    "labels": {
      |      "statusExpiry": "Keep status for"
      |    },
      |    "descriptions": {
      |      "name": "Optional human-readable name",
      |      "statusExpiry": "How long the status record is kept after the query ends."
      |    },
      |    "placeholders": {
      |      "statusExpiry": "168h"
      |    }
      |  }
      |}""".stripMargin

  val source: UiHintsSource = UiHintsSource
    .parse(rawJson)
    .getOrElse(throw new RuntimeException("Invalid built-in form UI hints"))
}
