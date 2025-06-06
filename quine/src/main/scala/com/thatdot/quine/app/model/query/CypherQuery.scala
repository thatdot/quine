package com.thatdot.quine.app.model.query

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Flow

import com.thatdot.common.logging.Log.LazySafeLogging
import com.thatdot.quine.compiler
import com.thatdot.quine.graph.cypher.{QueryContext, Value}
import com.thatdot.quine.graph.{CypherOpsGraph, NamespaceId}

case class CypherQuery(
  queryText: String,
  parameter: String = "that",
) extends LazySafeLogging {

  def flow(name: String, inNamespace: NamespaceId)(implicit
    graph: CypherOpsGraph,
  ): Flow[Value, QueryContext, NotUsed] = {
    val compiledQuery = compiler.cypher.compile(queryText, Seq(parameter))

    Flow[Value]
      .flatMap { value =>
        graph.cypherOps
          .query(
            query = compiledQuery,
            namespace = inNamespace,
            atTime = None,
            parameters = Map(parameter -> value),
          )
          .results
          .map { resultRow =>
            QueryContext(compiledQuery.columns.zip(resultRow).toMap)
          }
      }
  }
}
