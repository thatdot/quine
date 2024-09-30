package com.thatdot.quine.app.outputs

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Flow

import com.thatdot.quine.graph.MasterStream.SqResultsExecToken
import com.thatdot.quine.graph.{CypherOpsGraph, NamespaceId, StandingQueryResult, namespaceToString}
import com.thatdot.quine.routes.StandingQueryResultOutputUserDef

trait OutputRuntime {
  final def execToken(name: String, namespaceId: NamespaceId): SqResultsExecToken = SqResultsExecToken(
    s"SQ: $name in: ${namespaceToString(namespaceId)}",
  )
  def flow(
    name: String,
    inNamespace: NamespaceId,
    output: StandingQueryResultOutputUserDef,
    graph: CypherOpsGraph,
  ): Flow[StandingQueryResult, SqResultsExecToken, NotUsed]
}
