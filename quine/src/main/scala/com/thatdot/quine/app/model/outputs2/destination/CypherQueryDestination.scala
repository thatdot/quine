package com.thatdot.quine.app.model.outputs2.destination

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.{Flow, Sink}

import com.thatdot.common.logging.Log
import com.thatdot.common.logging.Log.LazySafeLogging
import com.thatdot.data.DataFoldableFrom
import com.thatdot.quine.app.data.QuineDataFoldersTo
import com.thatdot.quine.app.model.outputs2
import com.thatdot.quine.app.model.outputs2.QuineResultDestination
import com.thatdot.quine.graph.{CypherOpsGraph, NamespaceId, cypher}

case class CypherQueryDestination(
  queryText: String,
  parameter: String = "that",
  parallelism: Int,
  allowAllNodeScan: Boolean,
  shouldRetry: Boolean,
)(implicit graph: CypherOpsGraph)
    extends QuineResultDestination.FoldableData.CypherQuery
    with LazySafeLogging {

  override def slug: String = "cypher"

  private val underlyingCypherQuery =
    outputs2.query.CypherQuery(queryText, parameter, parallelism, allowAllNodeScan, shouldRetry)

  override def sink[A: DataFoldableFrom](name: String, inNamespace: NamespaceId)(implicit
    logConfig: Log.LogConfig,
  ): Sink[A, NotUsed] = {
    import QuineDataFoldersTo.cypherValueFolder

    val toCypherValue = DataFoldableFrom[A].to[cypher.Value]

    Flow[A]
      .map(toCypherValue)
      .via(underlyingCypherQuery.flow(name, inNamespace))
      .to(Sink.ignore)
      .named(sinkName(name))
  }
}
