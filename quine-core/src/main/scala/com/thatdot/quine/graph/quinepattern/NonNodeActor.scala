package com.thatdot.quine.graph.quinepattern

import org.apache.pekko.actor.Actor

import com.thatdot.quine.graph.behavior.QuinePatternCommand
import com.thatdot.quine.graph.cypher.quinepattern.{
  DefaultStateInstantiator,
  NodeContext,
  QueryStateBuilder,
  QueryStateHost,
}
import com.thatdot.quine.graph.{NamespaceId, StandingQueryOpsGraph}
import com.thatdot.quine.model.HalfEdge

/** Actor that hosts query state without being tied to a specific node.
  *
  * Used as the root of a query plan before it dispatches to actual nodes
  * via Anchor operations. Handles plan loading and state routing for
  * queries initiated from outside the node graph.
  */
class NonNodeActor(graph: QuinePatternOpsGraph with StandingQueryOpsGraph, namespace: NamespaceId)
    extends Actor
    with QueryStateHost {
  implicit val idProvider: com.thatdot.quine.model.QuineIdProvider = graph.idProvider

  override def receive: Receive = {
    case QuinePatternCommand
          .LoadQueryPlan(sqid, plan, mode, params, ns, output, injectedContext, returnColumns, outputNameMapping) =>
      try {
        com.thatdot.quine.graph.cypher.quinepattern.QPTrace.log(
          s"NonNodeActor LoadQueryPlan sqid=$sqid params=[${params.keys.map(_.name).mkString(",")}]",
        )
        params.get(Symbol("that")).foreach { thatVal =>
          com.thatdot.quine.graph.cypher.quinepattern.QPTrace.log(
            s"NonNodeActor LoadQueryPlan that value=$thatVal",
          )
        }
        // Build the state graph from the plan
        val stateGraph =
          QueryStateBuilder.build(plan, mode, params, ns, output, injectedContext, returnColumns, outputNameMapping)

        // Create empty node context (no node ID, no properties, no edges)
        // This is a "virtual" context for the root of the query
        val nodeContext = NodeContext(
          quineId = None,
          properties = Map.empty,
          edges = Set.empty[HalfEdge],
          labels = Set.empty,
          graph = Some(graph),
          namespace = Some(ns),
        )

        // Install the state graph and kickstart it
        val _ = installStateGraph(stateGraph, DefaultStateInstantiator, nodeContext)
      } catch {
        case e: Exception =>
          System.err.println(s"[QP ERROR] NonNodeActor failed to load query plan: ${e.getMessage}")
          e.printStackTrace()
          // Try to complete the promise with failure to avoid deadlock
          // Use tryFailure since the promise might already be completed
          import com.thatdot.quine.graph.cypher.quinepattern.OutputTarget
          output match {
            case OutputTarget.EagerCollector(promise) =>
              val _ = promise.tryFailure(e)
            case _ => ()
          }
      }

    case QuinePatternCommand.QueryUpdate(stateToUpdate, from, delta) =>
      routeNotification(stateToUpdate, from, delta)

    case QuinePatternCommand.UnregisterState(queryId) =>
      hostedStates.remove(queryId)
      ()

    case QuinePatternCommand.NodeWake(anchorStateId, nodeId, _, context) =>
      // Route node wake event to the appropriate Anchor state (thread-safe dispatch)
      hostedStates.get(anchorStateId) match {
        case Some(anchor: com.thatdot.quine.graph.cypher.quinepattern.AnchorState) =>
          anchor.handleNodeWake(nodeId, context, self)
        case Some(other) =>
          System.err.println(
            s"[QP WARNING] NodeWake for state $anchorStateId but found ${other.getClass.getSimpleName} instead of AnchorState",
          )
        case None =>
          // Anchor may have been unregistered - ignore
          ()
      }

    // Catch node-specific commands that shouldn't be sent to NonNodeActor
    case cmd @ QuinePatternCommand.SetProperty(_, _, _, _) =>
      System.err.println(
        s"[QP WARNING] NonNodeActor received node-specific command: $cmd. " +
        "This indicates a planner bug - effects should be inside an Anchor's onTarget so they run on actual nodes.",
      )
    case cmd @ QuinePatternCommand.SetProperties(_) =>
      System.err.println(
        s"[QP WARNING] NonNodeActor received node-specific command: $cmd. " +
        "This indicates a planner bug - effects should be inside an Anchor's onTarget so they run on actual nodes.",
      )
    case cmd @ QuinePatternCommand.SetLabels(_) =>
      System.err.println(
        s"[QP WARNING] NonNodeActor received node-specific command: $cmd. " +
        "This indicates a planner bug - effects should be inside an Anchor's onTarget so they run on actual nodes.",
      )
    case cmd @ QuinePatternCommand.CreateEdge(_, _, _) =>
      System.err.println(
        s"[QP WARNING] NonNodeActor received node-specific command: $cmd. " +
        "This indicates a planner bug - effects should be inside an Anchor's onTarget so they run on actual nodes.",
      )
  }
}
