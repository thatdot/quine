package com.thatdot.quine.app.v2api.definitions
import com.thatdot.quine.app.v2api.definitions.{ApiStandingQueries => Api}
import com.thatdot.quine.{routes => Standing}
object StandingToApi {

  private def apply(mode: Standing.StandingQueryPattern.StandingQueryMode): Api.StandingQueryPattern.StandingQueryMode =
    mode match {
      case Standing.StandingQueryPattern.StandingQueryMode.DistinctId =>
        Api.StandingQueryPattern.StandingQueryMode.DistinctId
      case Standing.StandingQueryPattern.StandingQueryMode.MultipleValues =>
        Api.StandingQueryPattern.StandingQueryMode.MultipleValues
      case Standing.StandingQueryPattern.StandingQueryMode.QuinePattern =>
        Api.StandingQueryPattern.StandingQueryMode.QuinePattern
    }

  private def apply(format: Standing.OutputFormat): Api.OutputFormat = format match {
    case Standing.OutputFormat.JSON => Api.OutputFormat.JSON
    case Standing.OutputFormat.Protobuf(schemaUrl, typeName) =>
      Api.OutputFormat.Protobuf(schemaUrl, typeName)
  }
  private def apply(
    level: Standing.StandingQueryResultOutputUserDef.PrintToStandardOut.LogLevel,
  ): Api.StandingQueryResultOutputUserDef.PrintToStandardOut.LogLevel = level match {
    case Standing.StandingQueryResultOutputUserDef.PrintToStandardOut.LogLevel.Trace =>
      Api.StandingQueryResultOutputUserDef.PrintToStandardOut.LogLevel.Trace
    case Standing.StandingQueryResultOutputUserDef.PrintToStandardOut.LogLevel.Debug =>
      Api.StandingQueryResultOutputUserDef.PrintToStandardOut.LogLevel.Debug
    case Standing.StandingQueryResultOutputUserDef.PrintToStandardOut.LogLevel.Info =>
      Api.StandingQueryResultOutputUserDef.PrintToStandardOut.LogLevel.Info
    case Standing.StandingQueryResultOutputUserDef.PrintToStandardOut.LogLevel.Warn =>
      Api.StandingQueryResultOutputUserDef.PrintToStandardOut.LogLevel.Warn
    case Standing.StandingQueryResultOutputUserDef.PrintToStandardOut.LogLevel.Error =>
      Api.StandingQueryResultOutputUserDef.PrintToStandardOut.LogLevel.Error
  }
  private def apply(
    level: Standing.StandingQueryResultOutputUserDef.PrintToStandardOut.LogMode,
  ): Api.StandingQueryResultOutputUserDef.PrintToStandardOut.LogMode = level match {
    case Standing.StandingQueryResultOutputUserDef.PrintToStandardOut.LogMode.Complete =>
      Api.StandingQueryResultOutputUserDef.PrintToStandardOut.LogMode.Complete
    case Standing.StandingQueryResultOutputUserDef.PrintToStandardOut.LogMode.FastSampling =>
      Api.StandingQueryResultOutputUserDef.PrintToStandardOut.LogMode.FastSampling
  }

  def prependToOutputDef(
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
  }

  def apply(sq: Standing.StandingQueryResultOutputUserDef): Api.StandingQueryResultOutputUserDef = sq match {
    case Standing.StandingQueryResultOutputUserDef.PostToEndpoint(url, parallelism, onlyPositiveMatchData) =>
      Api.StandingQueryResultOutputUserDef.PostToEndpoint(
        url,
        parallelism,
        onlyPositiveMatchData,
        List.empty,
      )
    case Standing.StandingQueryResultOutputUserDef.WriteToKafka(topic, bootstrapServers, format, kafkaProperties) =>
      Api.StandingQueryResultOutputUserDef.WriteToKafka(
        topic,
        bootstrapServers,
        StandingToApi(format),
        kafkaProperties,
        List.empty,
      )
    case Standing.StandingQueryResultOutputUserDef.WriteToKinesis(
          credentials,
          region,
          streamName,
          format,
          kinesisParallelism,
          kinesisMaxBatchSize,
          kinesisMaxRecordsPerSecond,
          kinesisMaxBytesPerSecond,
        ) =>
      Api.StandingQueryResultOutputUserDef.WriteToKinesis(
        credentials.map(IngestToApi.apply),
        region.map(IngestToApi.apply),
        streamName,
        StandingToApi(format),
        kinesisParallelism,
        kinesisMaxBatchSize,
        kinesisMaxRecordsPerSecond,
        kinesisMaxBytesPerSecond,
        List.empty,
      )
    case Standing.StandingQueryResultOutputUserDef.WriteToSNS(credentials, region, topic) =>
      Api.StandingQueryResultOutputUserDef.WriteToSNS(
        credentials.map(IngestToApi.apply),
        region.map(IngestToApi.apply),
        topic,
        List.empty,
      )
    case Standing.StandingQueryResultOutputUserDef.PrintToStandardOut(logLevel, logMode) =>
      Api.StandingQueryResultOutputUserDef.PrintToStandardOut(
        StandingToApi(logLevel),
        StandingToApi(logMode),
        List.empty,
      )
    case Standing.StandingQueryResultOutputUserDef.WriteToFile(path) =>
      Api.StandingQueryResultOutputUserDef.WriteToFile(path, List.empty)
    case Standing.StandingQueryResultOutputUserDef.PostToSlack(hookUrl, onlyPositiveMatchData, intervalSeconds) =>
      Api.StandingQueryResultOutputUserDef.PostToSlack(hookUrl, onlyPositiveMatchData, intervalSeconds, List.empty)
    case Standing.StandingQueryResultOutputUserDef.Drop =>
      Api.StandingQueryResultOutputUserDef.Drop(List.empty)
    case Standing.StandingQueryResultOutputUserDef.InternalQueue() =>
      // InternalQueue is for internal use, so it should never be exposed to the API
      Api.StandingQueryResultOutputUserDef.Drop(List.empty)
    case Standing.StandingQueryResultOutputUserDef.CypherQuery(
          query,
          parameter,
          parallelism,
          None,
          allowAllNodeScan,
          shouldRetry,
        ) =>
      Api.StandingQueryResultOutputUserDef.CypherQuery(
        query,
        parameter,
        parallelism,
        allowAllNodeScan,
        shouldRetry,
        List.empty,
      )
    case Standing.StandingQueryResultOutputUserDef.CypherQuery(
          query,
          parameter,
          parallelism,
          Some(andThen),
          allowAllNodeScan,
          shouldRetry,
        ) =>
      val cypherQuery = Api.StandingQueryResultOutputUserDef.SequencedCypherQuery(
        query,
        parameter,
        parallelism,
        allowAllNodeScan,
        shouldRetry,
      )
      prependToOutputDef(cypherQuery, StandingToApi(andThen))
  }
  private def apply(pattern: Standing.StandingQueryPattern): Api.StandingQueryPattern =
    pattern match {
      case Standing.StandingQueryPattern.Cypher(query, mode) =>
        Api.StandingQueryPattern.Cypher(query, StandingToApi(mode))
    }

  private def apply(stats: Standing.StandingQueryStats): Api.StandingQueryStats =
    Api.StandingQueryStats(
      IngestToApi(stats.rates),
      stats.startTime,
      stats.totalRuntime,
      stats.bufferSize,
      stats.outputHashCode,
    )

  def apply(query: Standing.RegisteredStandingQuery): Api.RegisteredStandingQuery =
    Api.RegisteredStandingQuery(
      query.name,
      query.internalId,
      query.pattern.map(StandingToApi.apply),
      query.outputs.view.mapValues(StandingToApi.apply).toMap,
      query.includeCancellations,
      query.inputBufferSize,
      query.stats.view.mapValues(StandingToApi.apply).toMap,
    )

}
