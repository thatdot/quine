package com.thatdot.quine.graph.quinepattern

import org.apache.pekko.actor.{Actor, Props}

import com.thatdot.quine.graph.behavior.QuinePatternCommand
import com.thatdot.quine.graph.cypher.quinepattern.{OutputTarget, QueryPlan, RuntimeMode}
import com.thatdot.quine.graph.{NamespaceId, StandingQueryId, StandingQueryOpsGraph}
import com.thatdot.quine.language.ast.BindingId
import com.thatdot.quine.language.{ast => Pattern}
import com.thatdot.quine.model.Milliseconds

/** Message to load and execute a QuinePattern query plan.
  *
  * @param standingQueryId unique identifier for this query execution
  * @param queryPlan the compiled query plan to execute
  * @param mode execution mode: Eager for one-shot queries, Lazy for standing queries
  * @param params query parameters (e.g., from Cypher `$param` references)
  * @param namespace the graph namespace to query
  * @param output where to send query results
  * @param returnColumns columns to include in output (from RETURN clause)
  * @param outputNameMapping maps internal binding IDs to human-readable output names
  * @param queryName optional name for metrics filtering (e.g., "INGEST-1")
  * @param atTime historical timestamp for time-travel queries; None queries current state
  */
case class LoadQuery(
  standingQueryId: StandingQueryId,
  queryPlan: QueryPlan,
  mode: RuntimeMode,
  params: Map[Symbol, Pattern.Value],
  namespace: NamespaceId,
  output: OutputTarget,
  returnColumns: Option[Set[BindingId]] = None,
  outputNameMapping: Map[BindingId, Symbol] = Map.empty,
  queryName: Option[String] = None,
  atTime: Option[Milliseconds] = None,
)

/** QuinePattern query loader - handles loading query plans.
  *
  * Creates NonNodeActor to host root state, then dispatches via anchors.
  */
class QuinePatternLoader(graph: QuinePatternOpsGraph with StandingQueryOpsGraph) extends Actor {
  override def receive: Receive = {
    case LoadQuery(
          sqid,
          queryPlan,
          runtimeMode,
          params,
          namespace,
          output,
          returnColumns,
          outputNameMapping,
          _,
          atTime,
        ) =>
      val ephemeralActor = graph.system.actorOf(Props(classOf[NonNodeActor], graph, namespace))
      // We may want to consider warning or rejecting on non-read commands with `atTime` defined.
      ephemeralActor ! QuinePatternCommand.LoadQueryPlan(
        sqid = sqid,
        plan = queryPlan,
        mode = runtimeMode,
        params = params,
        namespace = namespace,
        output = output,
        returnColumns = returnColumns,
        outputNameMapping = outputNameMapping,
        atTime = atTime,
      )
  }
}
