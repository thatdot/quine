package com.thatdot.quine.app.v2api.converters

import scala.annotation.unused

import com.thatdot.quine.app.model.outputs2.query.standing
import com.thatdot.quine.app.v2api.definitions.query.{standing => Api}
import com.thatdot.{convert => ConvertCore}

/** Conversions from internal models in [[com.thatdot.quine.app.model.outputs2.query.standing]]
  * to API models in [[com.thatdot.quine.app.v2api.definitions.query.standing]].
  */
@unused
object StandingToApi {
  @unused
  private def apply(mode: standing.StandingQueryPattern.StandingQueryMode): Api.StandingQueryPattern.StandingQueryMode =
    mode match {
      case standing.StandingQueryPattern.StandingQueryMode.DistinctId =>
        Api.StandingQueryPattern.StandingQueryMode.DistinctId
      case standing.StandingQueryPattern.StandingQueryMode.MultipleValues =>
        Api.StandingQueryPattern.StandingQueryMode.MultipleValues
      case standing.StandingQueryPattern.StandingQueryMode.QuinePattern =>
        Api.StandingQueryPattern.StandingQueryMode.QuinePattern
    }

  @unused
  private def apply(pattern: standing.StandingQueryPattern): Api.StandingQueryPattern =
    pattern match {
      case standing.StandingQueryPattern.Cypher(query, mode) =>
        Api.StandingQueryPattern.Cypher(query, apply(mode))
    }

  @unused
  private def apply(stats: standing.StandingQueryStats): Api.StandingQueryStats =
    Api.StandingQueryStats(
      ConvertCore.Model2ToApi2(stats.rates),
      stats.startTime,
      stats.totalRuntime,
      stats.bufferSize,
      stats.outputHashCode,
    )

  @unused
  private def apply(t: standing.StandingQueryResultTransform): Api.StandingQueryResultTransform = t match {
    case standing.StandingQueryResultTransform.InlineData() => Api.StandingQueryResultTransform.InlineData
  }

  def apply(workflow: standing.StandingQueryResultWorkflow): Api.StandingQueryResultWorkflow =
    Api.StandingQueryResultWorkflow(
      filter = workflow.workflow.filter.map(Outputs2ToApi2.apply),
      preEnrichmentTransform = workflow.workflow.preEnrichmentTransform.map(apply),
      resultEnrichment = workflow.workflow.enrichmentQuery.map(Outputs2ToApi2.fromCypherQuery),
      destinations = workflow.destinationStepsList.map {
        case standing.QuineSupportedDestinationSteps.CoreDestinationSteps(underlying) =>
          Api.QuineSupportedDestinationSteps.CoreDestinationSteps(ConvertCore.Outputs2ToApi2(underlying))
        case standing.QuineSupportedDestinationSteps.QuineAdditionalFoldableDataResultDestinations(underlying) =>
          Api.QuineSupportedDestinationSteps.QuineAdditionalDestinationSteps(Outputs2ToApi2(underlying))
      },
    )

  def apply(
    registeredQuery: standing.StandingQuery.RegisteredStandingQuery,
  ): Api.StandingQuery.RegisteredStandingQuery = {
    val q = registeredQuery
    Api.StandingQuery.RegisteredStandingQuery(
      name = q.name,
      internalId = q.internalId,
      pattern = q.pattern.map(apply),
      outputs = q.outputs.view.mapValues(apply).toMap,
      includeCancellations = q.includeCancellations,
      inputBufferSize = q.inputBufferSize,
      stats = q.stats.view.mapValues(apply).toMap,
    )
  }

  def apply(
    standingQueryDefinition: standing.StandingQuery.StandingQueryDefinition,
  ): Api.StandingQuery.StandingQueryDefinition = {
    val q = standingQueryDefinition
    Api.StandingQuery.StandingQueryDefinition(
      pattern = apply(q.pattern),
      outputs = q.outputs.view.mapValues(apply).toMap,
      includeCancellations = q.includeCancellations,
      inputBufferSize = q.inputBufferSize,
    )
  }

}
