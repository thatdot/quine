package com.thatdot.quine.app.v2api.definitions.query.standing

import cats.data.NonEmptyList
import sttp.tapir.Schema.annotations.{description, title}

import com.thatdot.quine.app.v2api.definitions.outputs.QuineDestinationSteps.CypherQuery
import com.thatdot.quine.app.v2api.definitions.query.standing.StandingQueryResultTransform

sealed trait QuineSupportedDestinationSteps

object QuineSupportedDestinationSteps {
  import com.thatdot.api.v2.outputs.{DestinationSteps => Core}
  import com.thatdot.quine.app.v2api.definitions.outputs.{QuineDestinationSteps => Quine}

  case class CoreDestinationSteps(steps: Core) extends QuineSupportedDestinationSteps
  case class QuineAdditionalDestinationSteps(steps: Quine) extends QuineSupportedDestinationSteps
}

@title(StandingQueryResultWorkflow.title)
@description(
  """A workflow comprising steps toward sending data derived from StandingQueryResults to destinations.
    |
    |The workflow's steps are processed in order. When a Standing Query emits a StandingQueryResult, the steps are:
    | 1.
    |StandingQueryResultWorkflow will first execute the optional `filter` step, w `resultEnrichment` step, which may be a
    |CypherQuery that "enriches" the data provided by the StandingQueryResult. This CypherQuery must return
    |data. Second, the workflow will send the enriched results to all provided destinations.
    |
    |A StandingQueryResult is an object with 2 sub-objects: `meta` and `data`. The `meta` object consists of:
    | - a boolean `isPositiveMatch`
    |
    |On a positive match, the `data` object consists of the data returned by the Standing Query.
    |
    |For example, a StandingQueryResult may look like the following:
    |
    |```
    |{"meta": {"isPositiveMatch": true}, "data": {"strId(n)": "a0f93a88-ecc8-4bd5-b9ba-faa6e9c5f95d"}}
    |```
    |
    |While a cancellation of that result might look like the following:
    |
    |```
    |{"meta": {"isPositiveMatch": false}, "data": {}}
    |```
    |""".stripMargin,
)
case class StandingQueryResultWorkflow(
  @description("A StandingQueryResult filter (one of any built-in options), which runs before any enrichment query.")
  filter: Option[Predicate] = None,
  @description("Transformation function to apply to each result")
  preEnrichmentTransform: Option[StandingQueryResultTransform] = None,
  @description("A CypherQuery that returns data.")
  resultEnrichment: Option[CypherQuery] = None,
  @description("The destinations to which the latest data passed through the workflow steps shall be delivered.")
  destinations: NonEmptyList[QuineSupportedDestinationSteps],
)

object StandingQueryResultWorkflow {
  val title = "Standing Query Result Workflow"
}
