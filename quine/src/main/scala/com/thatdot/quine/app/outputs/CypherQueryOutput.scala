package com.thatdot.quine.app.outputs

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Flow

import com.thatdot.common.logging.Log.{LazySafeLogging, LogConfig, Safe, SafeLoggableInterpolator}
import com.thatdot.quine.app.serialization.ProtobufSchemaCache
import com.thatdot.quine.app.util.AtLeastOnceCypherQuery
import com.thatdot.quine.compiler
import com.thatdot.quine.graph.MasterStream.SqResultsExecToken
import com.thatdot.quine.graph.cypher.QueryContext
import com.thatdot.quine.graph.{CypherOpsGraph, MasterStream, NamespaceId, StandingQueryResult, cypher}
import com.thatdot.quine.model.QuineValue
import com.thatdot.quine.routes.StandingQueryResultOutputUserDef
import com.thatdot.quine.routes.StandingQueryResultOutputUserDef.CypherQuery
import com.thatdot.quine.util.Log.implicits._
import com.thatdot.quine.util.PekkoStreams.wireTapFirst
class CypherQueryOutput(
  val config: CypherQuery,
  val createRecursiveOutput: (
    String,
    NamespaceId,
    StandingQueryResultOutputUserDef,
    CypherOpsGraph,
    ProtobufSchemaCache,
    LogConfig,
  ) => Flow[StandingQueryResult, SqResultsExecToken, NotUsed],
)(implicit
  private val logConfig: LogConfig,
  private val protobufSchemaCache: ProtobufSchemaCache,
) extends OutputRuntime
    with LazySafeLogging {

  def flow(
    name: String,
    inNamespace: NamespaceId,
    output: StandingQueryResultOutputUserDef,
    graph: CypherOpsGraph,
  ): Flow[StandingQueryResult, MasterStream.SqResultsExecToken, NotUsed] = {
    val token = execToken(name, inNamespace)
    val CypherQuery(query, parameter, parallelism, andThen, allowAllNodeScan, shouldRetry, structure) = config

    val compiledQuery @ cypher.CompiledQuery(_, queryAst, _, _, _) = compiler.cypher.compile(
      query,
      unfixedParameters = Seq(parameter),
    )

    // TODO: When in the initial set of SQ outputs, these should be tested before the SQ is registered!
    if (queryAst.canContainAllNodeScan && !allowAllNodeScan) {
      throw new RuntimeException(
        "Cypher query may contain full node scan; re-write without possible full node scan, or pass allowAllNodeScan true. " +
        s"The provided query was: $query",
      )
    }
    if (!queryAst.isIdempotent && shouldRetry) {
      logger.warn(
        safe"""Could not verify that the provided Cypher query is idempotent. If timeouts or external system errors
              |occur, query execution may be retried and duplicate data may be created. To avoid this
              |set shouldRetry = false in the Standing Query output""".cleanLines,
      )
    }

    val andThenFlow: Flow[(StandingQueryResult.Meta, cypher.QueryContext), SqResultsExecToken, NotUsed] =
      (andThen match {
        case None =>
          wireTapFirst[(StandingQueryResult.Meta, cypher.QueryContext)](tup =>
            logger.warn(
              safe"""Unused Cypher Standing Query output for Standing Query output:
                    |${Safe(name)} with: ${Safe(tup._2.environment.size)} columns.
                    |Did you mean to specify `andThen`?""".cleanLines,
            ),
          ).map(_ => token)

        case Some(thenOutput) =>
          Flow[(StandingQueryResult.Meta, cypher.QueryContext)]
            .map { case (meta: StandingQueryResult.Meta, qc: cypher.QueryContext) =>
              val newData = qc.environment.map { case (keySym, cypherVal) =>
                keySym.name -> cypher.Expr.toQuineValue(cypherVal).getOrElse {
                  logger.warn(
                    log"""Cypher Value: ${cypherVal} could not be represented as a Quine value in Standing
                         |Query output: ${Safe(name)}. Using `null` instead.""".cleanLines,
                  )
                  QuineValue.Null
                }
              }
              StandingQueryResult(meta, newData)
            }
            .via(createRecursiveOutput(name, inNamespace, thenOutput, graph, protobufSchemaCache, logConfig))
      }).named(s"sq-output-andthen-for-$name")

    lazy val atLeastOnceCypherQuery =
      AtLeastOnceCypherQuery(compiledQuery, parameter, s"sq-output-action-query-for-$name")

    Flow[StandingQueryResult]
      .flatMapMerge(
        breadth = parallelism,
        result => {
          val value: cypher.Value = cypher.Expr.fromQuineValue(result.toQuineValueMap(structure))

          val cypherResultRows =
            if (shouldRetry) atLeastOnceCypherQuery.stream(value, inNamespace)(graph)
            else
              graph.cypherOps
                .query(
                  compiledQuery,
                  namespace = inNamespace,
                  atTime = None,
                  parameters = Map(parameter -> value),
                )
                .results

          cypherResultRows
            .map { resultRow =>
              QueryContext(compiledQuery.columns.zip(resultRow).toMap)
            }
            .map(data => (result.meta, data))
        },
      )
      .via(andThenFlow)
  }
}
