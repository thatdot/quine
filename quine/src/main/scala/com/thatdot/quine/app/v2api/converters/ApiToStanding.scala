package com.thatdot.quine.app.v2api.converters

import scala.concurrent.{ExecutionContext, Future}

import org.apache.pekko.dispatch.MessageDispatcher

import com.thatdot.quine.app.model.outputs2.query.standing
import com.thatdot.quine.app.v2api.definitions.query.{standing => Api}
import com.thatdot.quine.graph.{CypherOpsGraph, NamespaceId}
import com.thatdot.quine.model.QuineIdProvider
import com.thatdot.quine.serialization.ProtobufSchemaCache
import com.thatdot.{convert => ConvertCore}

/** Conversions from API models in [[com.thatdot.quine.app.v2api.definitions.query.standing]]
  * to internal models in [[com.thatdot.quine.app.model.outputs2.query.standing]].
  */
object ApiToStanding {

  private def apply(mode: Api.StandingQueryPattern.StandingQueryMode): standing.StandingQueryPattern.StandingQueryMode =
    mode match {
      case Api.StandingQueryPattern.StandingQueryMode.DistinctId =>
        standing.StandingQueryPattern.StandingQueryMode.DistinctId
      case Api.StandingQueryPattern.StandingQueryMode.MultipleValues =>
        standing.StandingQueryPattern.StandingQueryMode.MultipleValues
      case Api.StandingQueryPattern.StandingQueryMode.QuinePattern =>
        standing.StandingQueryPattern.StandingQueryMode.QuinePattern
    }

  private def apply(pattern: Api.StandingQueryPattern): standing.StandingQueryPattern = pattern match {
    case Api.StandingQueryPattern.Cypher(query, mode) =>
      standing.StandingQueryPattern.Cypher(query, apply(mode))
  }

  private def apply(
    t: Api.StandingQueryResultTransform,
  )(implicit idProvider: QuineIdProvider): standing.StandingQueryResultTransform = t match {
    case Api.StandingQueryResultTransform.InlineData => standing.StandingQueryResultTransform.InlineData()
  }

  def apply(
    workflow: Api.StandingQueryResultWorkflow,
    outputName: String,
    namespaceId: NamespaceId,
  )(implicit
    graph: CypherOpsGraph,
    protobufSchemaCache: ProtobufSchemaCache,
  ): Future[standing.StandingQueryResultWorkflow] = {
    import cats.instances.future.catsStdInstancesForFuture
    implicit val ec: MessageDispatcher = graph.nodeDispatcherEC
    implicit val idProvider: QuineIdProvider = graph.idProvider

    workflow.destinations
      .traverse {
        case Api.QuineSupportedDestinationSteps.CoreDestinationSteps(steps) =>
          ConvertCore.Api2ToOutputs2(steps).map(standing.QuineSupportedDestinationSteps.CoreDestinationSteps)
        case Api.QuineSupportedDestinationSteps.QuineAdditionalDestinationSteps(steps) =>
          Api2ToOutputs2(steps).map(
            standing.QuineSupportedDestinationSteps.QuineAdditionalFoldableDataResultDestinations,
          )
      }
      .map(dests =>
        standing.StandingQueryResultWorkflow(
          outputName = outputName,
          namespaceId = namespaceId,
          workflow = standing.Workflow(
            filter = workflow.filter.map(Api2ToOutputs2.apply),
            preEnrichmentTransform = workflow.preEnrichmentTransform.map(apply),
            enrichmentQuery = workflow.resultEnrichment.map(Api2ToOutputs2.toEnrichmentQuery),
          ),
          destinationStepsList = dests,
        ),
      )
  }

  def apply(standingQueryDefinition: Api.StandingQuery.StandingQueryDefinition, namespace: NamespaceId)(implicit
    ec: ExecutionContext,
    graph: CypherOpsGraph,
    protobufSchemaCache: ProtobufSchemaCache,
  ): Future[standing.StandingQuery.StandingQueryDefinition] = {
    val q = standingQueryDefinition
    val pattern = apply(q.pattern)
    val outputsFut = Future.traverse(q.outputs.toVector) { case (outputName, workflow) =>
      apply(workflow = workflow, outputName = outputName, namespaceId = namespace).map { internalWorkflow =>
        outputName -> internalWorkflow
      }
    }

    outputsFut.map(outputs =>
      standing.StandingQuery.StandingQueryDefinition(
        pattern = pattern,
        outputs = outputs.toMap,
        includeCancellations = q.includeCancellations,
        inputBufferSize = q.inputBufferSize,
        shouldCalculateResultHashCode = q.includeCancellations,
      ),
    )
  }

  def apply(
    registeredSQ: Api.StandingQuery.RegisteredStandingQuery,
    namespace: NamespaceId,
  )(implicit
    graph: CypherOpsGraph,
    protobufSchemaCache: ProtobufSchemaCache,
  ): Future[standing.StandingQuery.RegisteredStandingQuery] = {
    val q = registeredSQ
    implicit val ec: ExecutionContext = graph.nodeDispatcherEC
    Future
      .traverse(q.outputs.toVector) { case (outputName, apiWorkflow) =>
        apply(apiWorkflow, outputName, namespace).map(internalWorkflow => outputName -> internalWorkflow)
      }
      .map { internalWorkflowsByName =>
        standing.StandingQuery.RegisteredStandingQuery(
          name = q.name,
          internalId = q.internalId,
          pattern = q.pattern.map(apply),
          outputs = internalWorkflowsByName.toMap,
          includeCancellations = q.includeCancellations,
          inputBufferSize = q.inputBufferSize,
          stats = q.stats.view.mapValues(apply).toMap,
        )
      }
  }

  def apply(
    stats: Api.StandingQueryStats,
  ): standing.StandingQueryStats =
    standing.StandingQueryStats(
      rates = stats.rates,
      startTime = stats.startTime,
      totalRuntime = stats.totalRuntime,
      bufferSize = stats.bufferSize,
      outputHashCode = stats.outputHashCode,
    )

}
