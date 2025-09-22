package com.thatdot.quine.app.v2api.definitions.query.standing

import cats.data.NonEmptyList
import sttp.tapir.Schema.annotations.{description, title}

import com.thatdot.quine.app.v2api.definitions.outputs.QuineDestinationSteps
import com.thatdot.quine.app.v2api.definitions.outputs.QuineDestinationSteps.CypherQuery

@title(StandingQueryResultWorkflow.apiTitle)
@description(StandingQueryResultWorkflow.apiDescription)
case class StandingQueryResultWorkflow(
  @description("Name of this output Workflow, unique within the Standing Query.")
  name: String,
  @description("A `StandingQueryResult` filter (one of any built-in options), which runs before any enrichment query.")
  filter: Option[Predicate] = None,
  @description("A transformation function to apply to each result.")
  preEnrichmentTransformation: Option[StandingQueryResultTransformation] = None,
  @description("A `CypherQuery` that returns data.")
  resultEnrichment: Option[CypherQuery] = None,
  @description("The destinations to which the latest data passed through the workflow steps shall be delivered.")
  destinations: NonEmptyList[QuineDestinationSteps],
)

object StandingQueryResultWorkflow {
  import com.thatdot.quine.app.util.StringOps.syntax._

  val apiTitle: String = "Standing Query Result Workflow"
  val apiDescription: String =
    """A workflow comprising steps toward sending data derived from `StandingQueryResults` to destinations.
      |
      |The workflow's steps are processed in order. When a Standing Query emits a `StandingQueryResult`, the steps are:
      | 1. The optional `filter` step.
      | 2. The optional `preEnrichmentTransformation` step, which may transform `StandingQueryResults` to desired shapes and values.
      | 3. The optional `resultEnrichment` step, which may be a CypherQuery that "enriches" the data provided by the previous steps. This CypherQuery must return data.
      | 4. The `destinations` step, which passes the result of the previous steps to every `DestinationSteps` object in the list.
      |
      |In full, while any of steps 1-3 may be skipped, the workflow can be diagrammed like this:
      |<pre>
      |                 Standing Query Result
      |                           │
      |                       ┌───▼──┐
      |         1)            │filter│
      |                       └───┬──┘
      |             ┌─────────────▼─────────────┐
      |         2)  │preEnrichmentTransformation│
      |             └─────────────┬─────────────┘
      |                   ┌───────▼────────┐
      |         3)        │resultEnrichment│
      |                   └───────┬────────┘
      |         4) ┌──────────────┴┬─────────┐
      |            ▼               ▼         ▼
      |      DestinationSteps-1   ...    DestinationSteps-N
      |</pre>
      |A `StandingQueryResult` is an object with 2 sub-objects: `meta` and `data`. The `meta` object consists of:
      | - a boolean `isPositiveMatch`
      |
      |On a positive match, the `data` object consists of the data returned by the Standing Query.
      |
      |For example, a `StandingQueryResult` may look like the following:
      |```
      |{"meta": {"isPositiveMatch": true}, "data": {"strId(n)": "a0f93a88-ecc8-4bd5-b9ba-faa6e9c5f95d"}}
      |```
      |
      |While a cancellation of that result might look like the following:
      |```
      |{"meta": {"isPositiveMatch": false}, "data": {}}
      |```
      |
      |""".stripMargin +
    """You may choose to use zero or more of the optional steps that precede the `destinations` step, each of which uses
      |the data output of the latest preceding step (or, if none, the original `StandingQueryResult`).
      |Transformation and enrichment may affect the shape of the data sent to subsequent steps, as well as the
      |DestinationSteps objects.""".asOneLine
}
