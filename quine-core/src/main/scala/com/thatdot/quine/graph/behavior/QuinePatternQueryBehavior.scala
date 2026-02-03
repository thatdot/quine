package com.thatdot.quine.graph.behavior

import org.apache.pekko.actor.Actor

import com.thatdot.common.logging.Log.LazySafeLogging
import com.thatdot.common.quineid.QuineId
import com.thatdot.quine.graph.EdgeEvent.EdgeAdded
import com.thatdot.quine.graph.PropertyEvent.{PropertyRemoved, PropertySet}
import com.thatdot.quine.graph.cypher.quinepattern.{
  CypherAndQuineHelpers,
  DefaultStateInstantiator,
  NodeContext,
  QPMetrics,
  QueryStateBuilder,
  QueryStateHost,
  QuinePatternExpressionInterpreter,
  QuinePatternUnimplementedException,
}
import com.thatdot.quine.graph.messaging.{QuineIdOps, QuineMessage, QuineRefOps}
import com.thatdot.quine.graph.quinepattern.QuinePatternOpsGraph
import com.thatdot.quine.graph.{BaseNodeActor, NamespaceId, StandingQueryId, StandingQueryOpsGraph}
import com.thatdot.quine.language.{ast => Pattern}
import com.thatdot.quine.model.{EdgeDirection, HalfEdge}

/** `QuinePatternCommand` represents commands or instructions used within the Quine graph processing system.
  * This sealed trait defines a hierarchy of messages that are utilized for managing and interacting
  * with standing queries, node updates, property modifications, and edge creation within the system.
  *
  * It extends `QuineMessage`, enabling these commands to be relayable across the Quine graph
  * using specific mechanisms such as `relayTell` or `relayAsk`.
  *
  * The implementations of this trait include commands for:
  * - Acknowledgements and control such as stopping a pattern.
  * - Loading and managing lazy standing query plans.
  * - Updating lazy query relationships.
  * - Local node operations like loading nodes, modifying properties, setting labels, or creating edges.
  * - Performing atomic operations such as property increments.
  */
sealed trait QuinePatternCommand extends QuineMessage

object QuinePatternCommand {
  case object QuinePatternAck extends QuinePatternCommand

  case object QuinePatternStop extends QuinePatternCommand

  // Local execution instructions
  case class SetProperty(
    property: Symbol,
    valueExpr: Pattern.Expression,
    context: com.thatdot.quine.graph.cypher.quinepattern.QueryContext, // Uses Pattern.Value directly
    params: Map[Symbol, Pattern.Value],
  ) extends QuinePatternCommand
  case class SetProperties(props: Map[Symbol, Pattern.Value]) extends QuinePatternCommand
  case class SetLabels(labels: Set[Symbol]) extends QuinePatternCommand
  case class CreateEdge(
    to: QuineId,
    direction: EdgeDirection,
    label: Symbol,
  ) extends QuinePatternCommand

  case class LoadQueryPlan(
    sqid: StandingQueryId,
    plan: com.thatdot.quine.graph.cypher.quinepattern.QueryPlan,
    mode: com.thatdot.quine.graph.cypher.quinepattern.RuntimeMode,
    params: Map[Symbol, Pattern.Value],
    namespace: NamespaceId,
    output: com.thatdot.quine.graph.cypher.quinepattern.OutputTarget,
    injectedContext: Map[Symbol, Pattern.Value] = Map.empty, // Query context bindings to seed into state graph
    returnColumns: Option[Set[Symbol]] = None, // Columns to include in output (from RETURN clause)
    outputNameMapping: Map[Symbol, Symbol] = Map.empty, // Maps internal binding IDs to human-readable output names
  ) extends QuinePatternCommand

  case class QueryUpdate(
    stateToUpdate: StandingQueryId,
    from: StandingQueryId,
    delta: Map[com.thatdot.quine.graph.cypher.quinepattern.QueryContext, Int],
  ) extends QuinePatternCommand

  case class UnregisterState(queryId: StandingQueryId) extends QuinePatternCommand

  // Sent when a node wakes up - triggers Anchor dispatch (used for thread-safe node wake handling)
  case class NodeWake(
    anchorStateId: StandingQueryId,
    nodeId: com.thatdot.common.quineid.QuineId,
    namespace: NamespaceId,
    context: Map[Symbol, Pattern.Value] = Map.empty,
  ) extends QuinePatternCommand
}

/** A trait that defines the behavior for Quine's pattern-based query system. This trait implements
  * functionality for managing pattern queries, handling various query commands, and maintaining state.
  * It builds upon foundational classes and traits such as `Actor`, `BaseNodeActor`, `QuineIdOps`,
  * `QuineRefOps`, and `StandingQueryBehavior`.
  *
  * The trait provides mechanisms for:
  * - Creating and managing pattern query states.
  * - Loading and executing lazy queries.
  * - Responding to various commands related to pattern queries, such as stopping queries, updating properties,
  * setting labels, creating edges, and publishing state updates.
  *
  * It is meant to facilitate interactions with Quine's node graph and integrates with its underlying
  * subsystems for handling sophisticated graph operations and query behaviors.
  */
trait QuinePatternQueryBehavior
    extends Actor
    with BaseNodeActor
    with QuineIdOps
    with QuineRefOps
    with StandingQueryBehavior
    with QueryStateHost
    with LazySafeLogging {

  protected def graph: QuinePatternOpsGraph with StandingQueryOpsGraph

  def quinePatternQueryBehavior(command: QuinePatternCommand): Unit = command match {
    case QuinePatternCommand.LoadQueryPlan(
          sqid,
          plan,
          mode,
          params,
          namespace,
          output,
          injectedContext,
          returnColumns,
          outputNameMapping,
        ) =>
      loadQueryPlan(sqid, plan, mode, params, namespace, output, injectedContext, returnColumns, outputNameMapping)
    case QuinePatternCommand.QueryUpdate(stateToUpdate, from, delta) =>
      routeNotification(stateToUpdate, from, delta)
    case QuinePatternCommand.UnregisterState(queryId) =>
      // SOFT UNREGISTER: We intentionally only remove the state locally without calling
      // state.cleanup() to propagate UnregisterState to child states on remote nodes.
      //
      // Why soft unregister:
      // - Calling cleanup() would send UnregisterState to all target nodes
      // - This could wake large subgraphs just to clean up orphaned states
      // - Orphaned states are harmless: their updates are dropped by routeNotification
      //   when the parent state no longer exists (see "State not found" case)
      //
      // Known issues with this approach:
      // 1. MEMORY LEAK: AnchorState.cleanup() would unregister NodeWakeHooks, but since
      //    we don't call it, hooks accumulate in QuinePatternOpsGraph.nodeHooks
      // 2. ORPHANED STATES: Child states on remote nodes continue to exist until their
      //    host node sleeps (states are not persisted, so sleep clears them)
      // 3. WASTED WORK: Orphaned states may continue processing events and sending
      //    updates that get dropped
      //
      // Future work (see QueryStateHost trait docs for full vision):
      // - Persist states so they survive node sleep/wake
      // - Implement lazy cleanup: nodes validate state relevance on wake
      // - Use epoch/generation tracking so stale states self-terminate
      // - Implement proper partial and total standing query unregistration
      hostedStates.remove(queryId).foreach { state =>
        QPMetrics.stateUninstalled(state.mode)
      }
    case QuinePatternCommand.QuinePatternStop =>
      hostedStates.clear()
    case QuinePatternCommand.QuinePatternAck => ()
    case QuinePatternCommand.SetLabels(labels) =>
      // Labels are stored in a special property; state notifications happen inside applyPropertyEffect
      setLabels(labels)
      ()
    case QuinePatternCommand.SetProperties(props) =>
      val events = props.flatMap { case (k, v) =>
        v match {
          case Pattern.Value.Null =>
            properties.get(k) match {
              case Some(oldValue) => List(PropertyRemoved(k, oldValue))
              case None => Nil
            }
          case value => List(PropertySet(k, CypherAndQuineHelpers.patternValueToPropertyValue(value).get))
        }
      }.toList
      // State notifications happen inside applyPropertyEffect (called by processPropertyEvents)
      processPropertyEvents(events)
      ()
    case QuinePatternCommand.CreateEdge(to, direction, label) =>
      val halfEdge = HalfEdge(label, direction, to)
      val event = EdgeAdded(halfEdge)
      processEdgeEvent(event)
      ()
    case QuinePatternCommand.SetProperty(property, valueExpr, context, params) =>
      // Evaluate the expression and set the property - context uses Pattern.Value directly
      val env = QuinePatternExpressionInterpreter.EvalEnvironment(context, params)
      QuinePatternExpressionInterpreter.eval(valueExpr)(graph.idProvider).run(env) match {
        case Right(value) =>
          val events = value match {
            case Pattern.Value.Null =>
              properties.get(property) match {
                case Some(oldValue) => List(PropertyRemoved(property, oldValue))
                case None => Nil
              }
            case v => List(PropertySet(property, CypherAndQuineHelpers.patternValueToPropertyValue(v).get))
          }
          processPropertyEvents(events)
        case Left(err) =>
          System.err.println(s"[QuinePattern WARNING] Failed to evaluate SetProperty expression: ${err.getMessage}")
      }
      ()
    case _ => throw new QuinePatternUnimplementedException(s"Not yet implemented: $command")
  }

  /** Load a query plan on this node.
    *
    * This builds the state graph from the plan and installs it,
    * then kickstarts all leaf states with the current node context.
    */
  private def loadQueryPlan(
    sqid: StandingQueryId,
    plan: com.thatdot.quine.graph.cypher.quinepattern.QueryPlan,
    mode: com.thatdot.quine.graph.cypher.quinepattern.RuntimeMode,
    params: Map[Symbol, Pattern.Value],
    namespace: NamespaceId,
    output: com.thatdot.quine.graph.cypher.quinepattern.OutputTarget,
    injectedContext: Map[Symbol, Pattern.Value],
    returnColumns: Option[Set[Symbol]],
    outputNameMapping: Map[Symbol, Symbol],
  ): Unit =
    try {
      // Build the state graph from the plan
      val stateGraph =
        QueryStateBuilder.build(
          plan,
          mode,
          params,
          namespace,
          output,
          injectedContext,
          returnColumns,
          outputNameMapping,
        )

      // Create node context with current node state
      val nodeContext = NodeContext(
        quineId = Some(qid),
        properties = properties.toMap,
        edges = edges.toSet,
        labels = getLabels().getOrElse(Set.empty),
        graph = Some(graph),
        namespace = Some(namespace),
      )

      // Install the state graph
      val _ = installStateGraph(stateGraph, DefaultStateInstantiator, nodeContext)
    } catch {
      case e: Exception =>
        System.err.println(s"[QP ERROR] Target node ${qid} failed to load query plan: ${e.getMessage}")
        e.printStackTrace()
        // Send empty delta back to origin to avoid deadlock
        import com.thatdot.quine.graph.cypher.quinepattern.{Delta, OutputTarget}
        import com.thatdot.quine.graph.messaging.SpaceTimeQuineId
        output match {
          case OutputTarget.RemoteState(originNode, stateId, ns, dispatchId) =>
            val stqid = SpaceTimeQuineId(originNode, ns, None)
            graph.relayTell(stqid, QuinePatternCommand.QueryUpdate(stateId, dispatchId, Delta.empty))
          case OutputTarget.HostedState(hostActorRef, stateId, dispatchId) =>
            hostActorRef ! QuinePatternCommand.QueryUpdate(stateId, dispatchId, Delta.empty)
          case OutputTarget.EagerCollector(promise) =>
            val _ = promise.tryFailure(e)
          case _ => ()
        }
    }
}
