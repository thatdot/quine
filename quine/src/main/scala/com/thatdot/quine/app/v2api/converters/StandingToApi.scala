package com.thatdot.quine.app.v2api.converters

import scala.annotation.unused

import com.thatdot.quine.app.model.outputs2.query.{standing => Standing}
import com.thatdot.quine.app.v2api.definitions.query.{standing => Api}
import com.thatdot.quine.{routes => V1}

/** Conversions from internal models in [[com.thatdot.quine.app.model.outputs2.query.standing]]
  * to API models in [[com.thatdot.quine.app.v2api.definitions.query.standing]].
  */
@unused
object StandingToApi {
  @unused
  private def apply(mode: Standing.StandingQueryPattern.StandingQueryMode): Api.StandingQueryPattern.StandingQueryMode =
    mode match {
      case Standing.StandingQueryPattern.StandingQueryMode.DistinctId =>
        Api.StandingQueryPattern.StandingQueryMode.DistinctId
      case Standing.StandingQueryPattern.StandingQueryMode.MultipleValues =>
        Api.StandingQueryPattern.StandingQueryMode.MultipleValues
      case Standing.StandingQueryPattern.StandingQueryMode.QuinePattern =>
        Api.StandingQueryPattern.StandingQueryMode.QuinePattern
    }

  @unused
  private def apply(pattern: Standing.StandingQueryPattern): Api.StandingQueryPattern =
    pattern match {
      case Standing.StandingQueryPattern.Cypher(query, mode) =>
        Api.StandingQueryPattern.Cypher(query, apply(mode))
    }

  @unused
  private def apply(stats: Standing.StandingQueryStats): Api.StandingQueryStats =
    Api.StandingQueryStats(
      InternalToApi(stats.rates),
      stats.startTime,
      stats.totalRuntime,
      stats.bufferSize,
      stats.outputHashCode,
    )

  @unused
  def apply(workflow: Standing.StandingQueryResultWorkflow): Api.StandingQueryResultWorkflow =
    Api.StandingQueryResultWorkflow(
      resultEnrichment = workflow.workflow.enrichmentQuery.map(QueryToApi.apply),
      destinations = workflow.destinationStepsList.map(OutputToApi.apply),
    )

}

// Shall be deleted when Outputs V2 is used in API V2
object V1StandingToV2Api {

  private def apply(mode: V1.StandingQueryPattern.StandingQueryMode): Api.StandingQueryPattern.StandingQueryMode =
    mode match {
      case V1.StandingQueryPattern.StandingQueryMode.DistinctId =>
        Api.StandingQueryPattern.StandingQueryMode.DistinctId
      case V1.StandingQueryPattern.StandingQueryMode.MultipleValues =>
        Api.StandingQueryPattern.StandingQueryMode.MultipleValues
      case V1.StandingQueryPattern.StandingQueryMode.QuinePattern =>
        Api.StandingQueryPattern.StandingQueryMode.QuinePattern
    }

  private def apply(
    level: V1.StandingQueryResultOutputUserDef.PrintToStandardOut.LogLevel,
  ): Api.StandingQueryResultOutputUserDef.PrintToStandardOut.LogLevel = level match {
    case V1.StandingQueryResultOutputUserDef.PrintToStandardOut.LogLevel.Trace =>
      Api.StandingQueryResultOutputUserDef.PrintToStandardOut.LogLevel.Trace
    case V1.StandingQueryResultOutputUserDef.PrintToStandardOut.LogLevel.Debug =>
      Api.StandingQueryResultOutputUserDef.PrintToStandardOut.LogLevel.Debug
    case V1.StandingQueryResultOutputUserDef.PrintToStandardOut.LogLevel.Info =>
      Api.StandingQueryResultOutputUserDef.PrintToStandardOut.LogLevel.Info
    case V1.StandingQueryResultOutputUserDef.PrintToStandardOut.LogLevel.Warn =>
      Api.StandingQueryResultOutputUserDef.PrintToStandardOut.LogLevel.Warn
    case V1.StandingQueryResultOutputUserDef.PrintToStandardOut.LogLevel.Error =>
      Api.StandingQueryResultOutputUserDef.PrintToStandardOut.LogLevel.Error
  }
  private def apply(
    level: V1.StandingQueryResultOutputUserDef.PrintToStandardOut.LogMode,
  ): Api.StandingQueryResultOutputUserDef.PrintToStandardOut.LogMode = level match {
    case V1.StandingQueryResultOutputUserDef.PrintToStandardOut.LogMode.Complete =>
      Api.StandingQueryResultOutputUserDef.PrintToStandardOut.LogMode.Complete
    case V1.StandingQueryResultOutputUserDef.PrintToStandardOut.LogMode.FastSampling =>
      Api.StandingQueryResultOutputUserDef.PrintToStandardOut.LogMode.FastSampling
  }

  private def prependToOutputDef(
    cypher: Api.StandingQueryResultOutputUserDef.SequencedCypherQuery,
    outputDef: Api.StandingQueryResultOutputUserDef,
  ): Api.StandingQueryResultOutputUserDef = outputDef match {
    case out: Api.StandingQueryResultOutputUserDef.PostToEndpoint => out.copy(sequence = cypher +: out.sequence)
    case out: Api.StandingQueryResultOutputUserDef.WriteToKafka => out.copy(sequence = cypher +: out.sequence)
    case out: Api.StandingQueryResultOutputUserDef.WriteToKinesis => out.copy(sequence = cypher +: out.sequence)
    case out: Api.StandingQueryResultOutputUserDef.WriteToSNS => out.copy(sequence = cypher +: out.sequence)
    case out: Api.StandingQueryResultOutputUserDef.PrintToStandardOut => out.copy(sequence = cypher +: out.sequence)
    case out: Api.StandingQueryResultOutputUserDef.WriteToFile => out.copy(sequence = cypher +: out.sequence)
    case out: Api.StandingQueryResultOutputUserDef.PostToSlack => out.copy(sequence = cypher +: out.sequence)
    case out: Api.StandingQueryResultOutputUserDef.CypherQuery => out.copy(sequence = cypher +: out.sequence)
    case out: Api.StandingQueryResultOutputUserDef.Drop => out.copy(sequence = cypher +: out.sequence)
    case out: Api.StandingQueryResultOutputUserDef.ReactiveStream => out.copy(sequence = cypher +: out.sequence)
  }

  def apply(structure: V1.StandingQueryOutputStructure): Api.StandingQueryOutputStructure = structure match {
    case V1.StandingQueryOutputStructure.WithMetadata() => Api.StandingQueryOutputStructure.WithMetadata()
    case V1.StandingQueryOutputStructure.Bare() => Api.StandingQueryOutputStructure.Bare()
  }

  def apply(sq: V1.StandingQueryResultOutputUserDef): Api.StandingQueryResultOutputUserDef = sq match {
    case V1.StandingQueryResultOutputUserDef.PostToEndpoint(url, parallelism, onlyPositiveMatchData, structure) =>
      Api.StandingQueryResultOutputUserDef.PostToEndpoint(
        url,
        parallelism,
        onlyPositiveMatchData,
        List.empty,
        V1StandingToV2Api(structure),
      )
    case V1.StandingQueryResultOutputUserDef.WriteToKafka(
          topic,
          bootstrapServers,
          format,
          kafkaProperties,
          structure,
        ) =>
      Api.StandingQueryResultOutputUserDef.WriteToKafka(
        topic,
        bootstrapServers,
        format,
        kafkaProperties,
        List.empty,
        V1StandingToV2Api(structure),
      )
    case V1.StandingQueryResultOutputUserDef.WriteToKinesis(
          credentials,
          region,
          streamName,
          format,
          kinesisParallelism,
          kinesisMaxBatchSize,
          kinesisMaxRecordsPerSecond,
          kinesisMaxBytesPerSecond,
          structure,
        ) =>
      Api.StandingQueryResultOutputUserDef.WriteToKinesis(
        credentials.map(InternalToApi.fromV1),
        region.map(InternalToApi.fromV1),
        streamName,
        format,
        kinesisParallelism,
        kinesisMaxBatchSize,
        kinesisMaxRecordsPerSecond,
        kinesisMaxBytesPerSecond,
        List.empty,
        V1StandingToV2Api(structure),
      )
    case V1.StandingQueryResultOutputUserDef.WriteToSNS(credentials, region, topic, structure) =>
      Api.StandingQueryResultOutputUserDef.WriteToSNS(
        credentials.map(InternalToApi.fromV1),
        region.map(InternalToApi.fromV1),
        topic,
        List.empty,
        V1StandingToV2Api(structure),
      )
    case V1.StandingQueryResultOutputUserDef.PrintToStandardOut(logLevel, logMode, structure) =>
      Api.StandingQueryResultOutputUserDef.PrintToStandardOut(
        V1StandingToV2Api(logLevel),
        V1StandingToV2Api(logMode),
        List.empty,
        V1StandingToV2Api(structure),
      )
    case V1.StandingQueryResultOutputUserDef.WriteToFile(path, structure) =>
      Api.StandingQueryResultOutputUserDef.WriteToFile(path, List.empty, V1StandingToV2Api(structure))
    case V1.StandingQueryResultOutputUserDef.PostToSlack(hookUrl, onlyPositiveMatchData, intervalSeconds) =>
      Api.StandingQueryResultOutputUserDef.PostToSlack(hookUrl, onlyPositiveMatchData, intervalSeconds, List.empty)
    case V1.StandingQueryResultOutputUserDef.Drop =>
      Api.StandingQueryResultOutputUserDef.Drop(List.empty)
    case V1.StandingQueryResultOutputUserDef.InternalQueue(_) =>
      // InternalQueue is for internal use, so it should never be exposed to the API
      Api.StandingQueryResultOutputUserDef.Drop(List.empty)
    case V1.StandingQueryResultOutputUserDef.CypherQuery(
          query,
          parameter,
          parallelism,
          None,
          allowAllNodeScan,
          shouldRetry,
          structure,
        ) =>
      Api.StandingQueryResultOutputUserDef.CypherQuery(
        query,
        parameter,
        parallelism,
        allowAllNodeScan,
        shouldRetry,
        List.empty,
        V1StandingToV2Api(structure),
      )
    case V1.StandingQueryResultOutputUserDef.ReactiveStream(address, port, _) =>
      Api.StandingQueryResultOutputUserDef.ReactiveStream(address, port, List.empty)
    case V1.StandingQueryResultOutputUserDef.CypherQuery(
          query,
          parameter,
          parallelism,
          Some(andThen),
          allowAllNodeScan,
          shouldRetry,
          _,
        ) =>
      val cypherQuery = Api.StandingQueryResultOutputUserDef.SequencedCypherQuery(
        query,
        parameter,
        parallelism,
        allowAllNodeScan,
        shouldRetry,
      )
      prependToOutputDef(cypherQuery, V1StandingToV2Api(andThen))
    case other => sys.error(s"API v2 doesn't support StandingQueryResultOutputUserDef: $other")
  }
  private def apply(pattern: V1.StandingQueryPattern): Api.StandingQueryPattern =
    pattern match {
      case V1.StandingQueryPattern.Cypher(query, mode) =>
        Api.StandingQueryPattern.Cypher(query, V1StandingToV2Api(mode))
    }

  private def apply(stats: V1.StandingQueryStats): Api.StandingQueryStats =
    Api.StandingQueryStats(
      InternalToApi.fromV1(stats.rates),
      stats.startTime,
      stats.totalRuntime,
      stats.bufferSize,
      stats.outputHashCode,
    )

  def apply(query: V1.RegisteredStandingQuery): Api.StandingQuery.RegisteredStandingQuery =
    Api.StandingQuery.RegisteredStandingQuery(
      query.name,
      query.internalId,
      query.pattern.map(V1StandingToV2Api.apply),
      query.outputs.view.mapValues(V1StandingToV2Api.apply).toMap,
      query.includeCancellations,
      query.inputBufferSize,
      query.stats.view.mapValues(V1StandingToV2Api.apply).toMap,
    )

}
