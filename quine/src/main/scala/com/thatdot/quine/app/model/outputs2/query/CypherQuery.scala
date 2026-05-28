package com.thatdot.quine.app.model.outputs2.query

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Flow

import com.thatdot.common.logging.Log.{LazySafeLogging, LogConfig, SafeLoggableInterpolator}
import com.thatdot.quine.app.util.AtLeastOnceCypherQuery
import com.thatdot.quine.compiler
import com.thatdot.quine.graph.cypher.{CompiledQuery, Location, QueryContext, Value}
import com.thatdot.quine.graph.{CypherOpsGraph, NamespaceId}

class AllNodeScanException(queryText: String)
    extends RuntimeException(
      "Cypher query may contain full node scan; re-write without possible full node scan, or pass allowAllNodeScan true. " +
      s"The provided query was: $queryText",
    )

object CypherQuery {

  /** Compile and validate a cypher query, throwing [[CypherException]] on syntax/semantic errors
    * or [[AllNodeScanException]] if the query may scan all nodes without explicit opt-in.
    */
  def validateAndCompile(
    queryText: String,
    parameter: String,
    allowAllNodeScan: Boolean,
  ): CompiledQuery[Location.Anywhere] = {
    val compiled = compiler.cypher.compile(queryText, Seq(parameter))
    if (compiled.canContainAllNodeScan && !allowAllNodeScan) {
      throw new AllNodeScanException(queryText)
    }
    compiled
  }
}

case class CypherQuery(
  queryText: String,
  parameter: String = "that",
  parallelism: Int,
  allowAllNodeScan: Boolean,
  shouldRetry: Boolean,
) extends LazySafeLogging {

  def flow(name: String, inNamespace: NamespaceId)(implicit
    graph: CypherOpsGraph,
    logConfig: LogConfig,
  ): Flow[Value, QueryContext, NotUsed] = {
    val compiledQuery = CypherQuery.validateAndCompile(queryText, parameter, allowAllNodeScan)
    val queryAst = compiledQuery.query
    if (!queryAst.isIdempotent && shouldRetry) {
      logger.warn(
        safe"""Could not verify that the provided Cypher query is idempotent. If timeouts or external system errors
              |occur, query execution may be retried and duplicate data may be created. To avoid this
              |set shouldRetry = false in the Standing Query output""".cleanLines,
      )
    }

    lazy val atLeastOnceCypherQuery =
      AtLeastOnceCypherQuery(compiledQuery, parameter, s"cypher-query-for--$name")

    Flow[Value]
      .flatMapMerge(
        breadth = parallelism,
        value => {
          val cypherResultRows =
            if (shouldRetry) atLeastOnceCypherQuery.stream(value, inNamespace)(graph)
            else
              graph.cypherOps
                .query(
                  query = compiledQuery,
                  namespace = inNamespace,
                  // `atTime` is `None` because we only want current time here—this is where we would
                  // pass in `atTime` for historically aware output queries (if we chose to do that)
                  atTime = None,
                  parameters = Map(parameter -> value),
                )
                .results

          cypherResultRows
            .map { resultRow =>
              QueryContext(compiledQuery.columns.zip(resultRow).toMap)
            }
        },
      )
  }
}
