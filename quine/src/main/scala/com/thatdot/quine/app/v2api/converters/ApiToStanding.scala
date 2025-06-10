package com.thatdot.quine.app.v2api.converters

import scala.annotation.unused
import scala.concurrent.Future

import org.apache.pekko.dispatch.MessageDispatcher

import com.thatdot.convert.{Api2ToModel1, Api2ToOutput2}
import com.thatdot.quine.app.model.outputs2.query.{standing => Standing}
import com.thatdot.quine.app.v2api.definitions.query.{standing => Api}
import com.thatdot.quine.graph.{BaseGraph, NamespaceId}
import com.thatdot.quine.serialization.ProtobufSchemaCache

/** Conversions from API models in [[com.thatdot.quine.app.v2api.definitions.query.standing]]
  * to internal models in [[com.thatdot.quine.app.model.outputs2.query.standing]].
  */
@unused
object ApiToStanding {

  @unused
  private def apply(mode: Api.StandingQueryPattern.StandingQueryMode): Standing.StandingQueryPattern.StandingQueryMode =
    mode match {
      case Api.StandingQueryPattern.StandingQueryMode.DistinctId =>
        Standing.StandingQueryPattern.StandingQueryMode.DistinctId
      case Api.StandingQueryPattern.StandingQueryMode.MultipleValues =>
        Standing.StandingQueryPattern.StandingQueryMode.MultipleValues
      case Api.StandingQueryPattern.StandingQueryMode.QuinePattern =>
        Standing.StandingQueryPattern.StandingQueryMode.QuinePattern
    }

  @unused
  private def apply(pattern: Api.StandingQueryPattern): Standing.StandingQueryPattern = pattern match {
    case Api.StandingQueryPattern.Cypher(query, mode) =>
      Standing.StandingQueryPattern.Cypher(query, apply(mode))
  }

  @unused
  def apply(
    workflow: Api.StandingQueryResultWorkflow,
    outputName: String,
    namespaceId: NamespaceId,
  )(implicit
    graph: BaseGraph,
    protobufSchemaCache: ProtobufSchemaCache,
  ): Future[Standing.StandingQueryResultWorkflow] = {
    import cats.instances.future.catsStdInstancesForFuture
    implicit val ec: MessageDispatcher = graph.nodeDispatcherEC

    workflow.destinations
      .traverse(Api2ToOutput2.apply)
      .map(dests =>
        Standing.StandingQueryResultWorkflow(
          outputName = outputName,
          namespaceId = namespaceId,
          workflow = Standing.Workflow(
            enrichmentQuery = workflow.resultEnrichment.map(ApiToQuery.apply),
          ),
          destinationStepsList = dests,
        ),
      )
  }

}

// Shall be deleted when Outputs V2 is used in API V2
object V2ApiToV1Standing {
  import com.thatdot.quine.{routes => V1Standing}

  private def apply(
    mode: Api.StandingQueryPattern.StandingQueryMode,
  ): V1Standing.StandingQueryPattern.StandingQueryMode =
    mode match {
      case Api.StandingQueryPattern.StandingQueryMode.DistinctId =>
        V1Standing.StandingQueryPattern.StandingQueryMode.DistinctId
      case Api.StandingQueryPattern.StandingQueryMode.MultipleValues =>
        V1Standing.StandingQueryPattern.StandingQueryMode.MultipleValues
      case Api.StandingQueryPattern.StandingQueryMode.QuinePattern =>
        V1Standing.StandingQueryPattern.StandingQueryMode.QuinePattern
    }

  private def apply(
    level: Api.StandingQueryResultOutputUserDef.PrintToStandardOut.LogLevel,
  ): V1Standing.StandingQueryResultOutputUserDef.PrintToStandardOut.LogLevel = level match {
    case Api.StandingQueryResultOutputUserDef.PrintToStandardOut.LogLevel.Trace =>
      V1Standing.StandingQueryResultOutputUserDef.PrintToStandardOut.LogLevel.Trace
    case Api.StandingQueryResultOutputUserDef.PrintToStandardOut.LogLevel.Debug =>
      V1Standing.StandingQueryResultOutputUserDef.PrintToStandardOut.LogLevel.Debug
    case Api.StandingQueryResultOutputUserDef.PrintToStandardOut.LogLevel.Info =>
      V1Standing.StandingQueryResultOutputUserDef.PrintToStandardOut.LogLevel.Info
    case Api.StandingQueryResultOutputUserDef.PrintToStandardOut.LogLevel.Warn =>
      V1Standing.StandingQueryResultOutputUserDef.PrintToStandardOut.LogLevel.Warn
    case Api.StandingQueryResultOutputUserDef.PrintToStandardOut.LogLevel.Error =>
      V1Standing.StandingQueryResultOutputUserDef.PrintToStandardOut.LogLevel.Error
  }

  private def apply(
    mode: Api.StandingQueryResultOutputUserDef.PrintToStandardOut.LogMode,
  ): V1Standing.StandingQueryResultOutputUserDef.PrintToStandardOut.LogMode = mode match {
    case Api.StandingQueryResultOutputUserDef.PrintToStandardOut.LogMode.Complete =>
      V1Standing.StandingQueryResultOutputUserDef.PrintToStandardOut.LogMode.Complete
    case Api.StandingQueryResultOutputUserDef.PrintToStandardOut.LogMode.FastSampling =>
      V1Standing.StandingQueryResultOutputUserDef.PrintToStandardOut.LogMode.FastSampling
  }

  private def apply(pattern: Api.StandingQueryPattern): V1Standing.StandingQueryPattern = pattern match {
    case Api.StandingQueryPattern.Cypher(query, mode) =>
      V1Standing.StandingQueryPattern.Cypher(query, V2ApiToV1Standing(mode))
  }

  private def apply(structure: Api.StandingQueryOutputStructure): V1Standing.StandingQueryOutputStructure =
    structure match {
      case Api.StandingQueryOutputStructure.WithMetadata() => V1Standing.StandingQueryOutputStructure.WithMetadata()
      case Api.StandingQueryOutputStructure.Bare() => V1Standing.StandingQueryOutputStructure.Bare()
    }

  def apply(sq: Api.StandingQueryResultOutputUserDef): V1Standing.StandingQueryResultOutputUserDef = {
    val result = sq match {
      case Api.StandingQueryResultOutputUserDef.PostToEndpoint(url, parallelism, onlyPositiveMatchData, _, structure) =>
        V1Standing.StandingQueryResultOutputUserDef.PostToEndpoint(
          url,
          parallelism,
          onlyPositiveMatchData,
          V2ApiToV1Standing(structure),
        )
      case Api.StandingQueryResultOutputUserDef.WriteToKafka(
            topic,
            bootstrapServers,
            format,
            kafkaProperties,
            _,
            structure,
          ) =>
        V1Standing.StandingQueryResultOutputUserDef.WriteToKafka(
          topic,
          bootstrapServers,
          format,
          kafkaProperties,
          V2ApiToV1Standing(structure),
        )
      case Api.StandingQueryResultOutputUserDef.WriteToKinesis(
            credentials,
            region,
            streamName,
            format,
            kinesisParallelism,
            kinesisMaxBatchSize,
            kinesisMaxRecordsPerSecond,
            kinesisMaxBytesPerSecond,
            _,
            structure,
          ) =>
        V1Standing.StandingQueryResultOutputUserDef.WriteToKinesis(
          credentials.map(Api2ToModel1.apply),
          region.map(Api2ToModel1.apply),
          streamName,
          format,
          kinesisParallelism,
          kinesisMaxBatchSize,
          kinesisMaxRecordsPerSecond,
          kinesisMaxBytesPerSecond,
          V2ApiToV1Standing(structure),
        )
      case Api.StandingQueryResultOutputUserDef.WriteToSNS(credentials, region, topic, _, structure) =>
        V1Standing.StandingQueryResultOutputUserDef.WriteToSNS(
          credentials.map(Api2ToModel1.apply),
          region.map(Api2ToModel1.apply),
          topic,
          V2ApiToV1Standing(structure),
        )
      case Api.StandingQueryResultOutputUserDef.PrintToStandardOut(logLevel, logMode, _, structure) =>
        V1Standing.StandingQueryResultOutputUserDef.PrintToStandardOut(
          V2ApiToV1Standing(logLevel),
          V2ApiToV1Standing(logMode),
          V2ApiToV1Standing(structure),
        )
      case Api.StandingQueryResultOutputUserDef.WriteToFile(path, _, structure) =>
        V1Standing.StandingQueryResultOutputUserDef.WriteToFile(path, V2ApiToV1Standing(structure))
      case Api.StandingQueryResultOutputUserDef.PostToSlack(hookUrl, onlyPositiveMatchData, intervalSeconds, _) =>
        V1Standing.StandingQueryResultOutputUserDef.PostToSlack(
          hookUrl,
          onlyPositiveMatchData,
          intervalSeconds,
        )
      case Api.StandingQueryResultOutputUserDef.Drop(_) =>
        V1Standing.StandingQueryResultOutputUserDef.Drop
      case Api.StandingQueryResultOutputUserDef.CypherQuery(
            query,
            parameter,
            parallelism,
            allowAllNodeScan,
            shouldRetry,
            _,
            structure,
          ) =>
        V1Standing.StandingQueryResultOutputUserDef.CypherQuery(
          query,
          parameter,
          parallelism,
          None,
          allowAllNodeScan,
          shouldRetry,
          V2ApiToV1Standing(structure),
        )
      case Api.StandingQueryResultOutputUserDef.ReactiveStream(
            address,
            port,
            _,
          ) =>
        V1Standing.StandingQueryResultOutputUserDef.ReactiveStream(address, port, V1Standing.OutputFormat.JSON)
    }
    sq.sequence.foldRight(result) { case (cypher, sq) =>
      V1Standing.StandingQueryResultOutputUserDef.CypherQuery(
        cypher.query,
        cypher.parameter,
        cypher.parallelism,
        Some(sq),
        cypher.allowAllNodeScan,
        cypher.shouldRetry,
      )
    }
  }

  def apply(
    sq: Api.StandingQuery.StandingQueryDefinition,
    shouldCalculateResultHashCode: Boolean,
  ): V1Standing.StandingQueryDefinition =
    V1Standing.StandingQueryDefinition(
      V2ApiToV1Standing(sq.pattern),
      sq.outputs.view.mapValues(V2ApiToV1Standing.apply).toMap,
      sq.includeCancellations,
      sq.inputBufferSize,
      shouldCalculateResultHashCode,
    )

}
