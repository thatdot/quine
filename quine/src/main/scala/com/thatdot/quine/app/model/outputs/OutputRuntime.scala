package com.thatdot.quine.app.model.outputs

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Flow

import com.thatdot.quine.graph.MasterStream.SqResultsExecToken
import com.thatdot.quine.graph.{
  CypherOpsGraph,
  NamespaceId,
  StandingQueryResult,
  StandingQueryResultStructure,
  namespaceToString,
}
import com.thatdot.quine.routes.{StandingQueryOutputStructure, StandingQueryResultOutputUserDef}

trait OutputRuntime {

  import scala.language.implicitConversions
  implicit def sqResultOutputStructureConversion(
    structure: StandingQueryOutputStructure,
  ): StandingQueryResultStructure =
    structure match {
      case StandingQueryOutputStructure.WithMetadata() => StandingQueryResultStructure.WithMetaData()
      case StandingQueryOutputStructure.Bare() => StandingQueryResultStructure.Bare()
    }

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
