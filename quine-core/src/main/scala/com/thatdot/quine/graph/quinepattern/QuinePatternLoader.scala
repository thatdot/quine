package com.thatdot.quine.graph.quinepattern

import org.apache.pekko.actor.{Actor, Props}

import com.thatdot.quine.graph.behavior.QuinePatternCommand
import com.thatdot.quine.graph.cypher.quinepattern.{OutputTarget, QueryPlan, RuntimeMode}
import com.thatdot.quine.graph.{NamespaceId, StandingQueryId, StandingQueryOpsGraph}
import com.thatdot.quine.language.{ast => Pattern}

case class LoadQuery(
  standingQueryId: StandingQueryId,
  queryPlan: QueryPlan,
  mode: RuntimeMode,
  params: Map[Symbol, Pattern.Value],
  namespace: NamespaceId,
  output: OutputTarget,
  returnColumns: Option[Set[Symbol]] = None, // Columns to include in output (from RETURN clause)
  outputNameMapping: Map[Symbol, Symbol] = Map.empty, // Maps internal binding IDs to human-readable output names
  queryName: Option[String] = None, // For metrics filtering (e.g., "INGEST-1")
)

/** QuinePattern query loader - handles loading query plans.
  *
  * Creates NonNodeActor to host root state, then dispatches via anchors.
  */
class QuinePatternLoader(graph: QuinePatternOpsGraph with StandingQueryOpsGraph) extends Actor {
  override def receive: Receive = {
    case LoadQuery(sqid, queryPlan, runtimeMode, params, namespace, output, returnColumns, outputNameMapping, _) =>
      val ephemeralActor = graph.system.actorOf(Props(classOf[NonNodeActor], graph, namespace))
      ephemeralActor ! QuinePatternCommand.LoadQueryPlan(
        sqid,
        queryPlan,
        runtimeMode,
        params,
        namespace,
        output,
        returnColumns = returnColumns,
        outputNameMapping = outputNameMapping,
      )
  }
}
