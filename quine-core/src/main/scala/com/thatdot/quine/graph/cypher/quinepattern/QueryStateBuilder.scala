package com.thatdot.quine.graph.cypher.quinepattern

import com.thatdot.language.ast.Value
import com.thatdot.quine.graph.{NamespaceId, StandingQueryId}

/** Result of building a query state graph from a QueryPlan.
  *
  * This is a pure data structure representing the built state machine,
  * with no Actor dependencies. It can be inspected, tested, and then
  * installed into an Actor separately.
  *
  * @param rootId The output state that receives final results
  * @param states All states indexed by their ID
  * @param leaves States that need initial kickstart (no upstream dependencies)
  * @param edges Parent-child relationships (child -> parent, for notification flow)
  */
case class StateGraph(
  rootId: StandingQueryId,
  states: Map[StandingQueryId, StateDescriptor],
  leaves: Set[StandingQueryId],
  edges: Map[StandingQueryId, StandingQueryId], // child -> parent
  params: Map[Symbol, Value], // Query parameters (e.g., $that)
  injectedContext: Map[Symbol, Value], // Context bindings injected from parent (e.g., from Anchor dispatch)
  returnColumns: Option[Set[Symbol]], // Columns from outermost RETURN/Project clause for output filtering
  outputNameMapping: Map[Symbol, Symbol] = Map.empty, // Maps internal binding IDs to human-readable output names
)

/** Describes a state to be instantiated.
  *
  * This is a pure description - the actual QuinePatternQueryState instance
  * is created later when installing into an Actor. This separation allows
  * the building logic to be pure and testable.
  */
sealed trait StateDescriptor {
  def id: StandingQueryId
  def parentId: StandingQueryId
  def mode: RuntimeMode
  def plan: QueryPlan
}

object StateDescriptor {

  /** Output state - the root that collects final results */
  case class Output(
    id: StandingQueryId,
    mode: RuntimeMode,
    outputTarget: OutputTarget,
  ) extends StateDescriptor {
    def parentId: StandingQueryId = id // Root has no parent
    def plan: QueryPlan = QueryPlan.Unit // Placeholder
  }

  /** State for LocalId operator.
    *
    * Binds the node's ID and labels to the given symbol. Properties are NOT included -
    * use WatchAllProperties for all properties, or WatchProperty for individual properties.
    */
  case class WatchId(
    id: StandingQueryId,
    parentId: StandingQueryId,
    mode: RuntimeMode,
    plan: QueryPlan.LocalId,
    binding: Symbol,
  ) extends StateDescriptor

  /** State for LocalProperty operator */
  case class WatchProperty(
    id: StandingQueryId,
    parentId: StandingQueryId,
    mode: RuntimeMode,
    plan: QueryPlan.LocalProperty,
    property: Symbol,
    aliasAs: Option[Symbol],
    constraint: PropertyConstraint,
  ) extends StateDescriptor

  /** State for LocalAllProperties operator */
  case class WatchAllProperties(
    id: StandingQueryId,
    parentId: StandingQueryId,
    mode: RuntimeMode,
    plan: QueryPlan.LocalAllProperties,
    binding: Symbol,
  ) extends StateDescriptor

  /** State for LocalLabels operator */
  case class WatchLabels(
    id: StandingQueryId,
    parentId: StandingQueryId,
    mode: RuntimeMode,
    plan: QueryPlan.LocalLabels,
    aliasAs: Option[Symbol],
    constraint: LabelConstraint,
  ) extends StateDescriptor

  /** State for LocalNode operator.
    *
    * Emits a complete Value.Node with id, labels, and properties.
    * The labelsProperty is filtered from properties since labels are provided separately.
    */
  case class WatchNode(
    id: StandingQueryId,
    parentId: StandingQueryId,
    mode: RuntimeMode,
    plan: QueryPlan.LocalNode,
    binding: Symbol,
  ) extends StateDescriptor

  /** State for Unit operator */
  case class Unit(
    id: StandingQueryId,
    parentId: StandingQueryId,
    mode: RuntimeMode,
    plan: QueryPlan.Unit.type,
  ) extends StateDescriptor

  /** State for CrossProduct operator */
  case class Product(
    id: StandingQueryId,
    parentId: StandingQueryId,
    mode: RuntimeMode,
    plan: QueryPlan.CrossProduct,
    childIds: List[StandingQueryId],
    childEntryPoints: Set[StandingQueryId] = Set.empty, // Children that need context injection
  ) extends StateDescriptor

  /** State for Sequence operator */
  case class Sequence(
    id: StandingQueryId,
    parentId: StandingQueryId,
    mode: RuntimeMode,
    plan: QueryPlan.Sequence,
    firstId: StandingQueryId,
    andThenId: StandingQueryId,
    contextBridgeId: Option[StandingQueryId], // Bridge that feeds first's context into andThen
    contextFlow: ContextFlow,
  ) extends StateDescriptor

  /** Bridge state that receives context from Sequence and forwards to andThen's entry point.
    * This enables Sequence to properly inject first's context into andThen before andThen evaluates.
    */
  case class ContextBridge(
    id: StandingQueryId,
    parentId: StandingQueryId, // The state in andThen that receives context
    mode: RuntimeMode,
    sequenceId: StandingQueryId, // The Sequence state that sends context
  ) extends StateDescriptor {
    def plan: QueryPlan = QueryPlan.Unit // Placeholder
  }

  /** State for Expand operator */
  case class Expand(
    id: StandingQueryId,
    parentId: StandingQueryId,
    mode: RuntimeMode,
    plan: QueryPlan.Expand,
    onNeighborPlan: QueryPlan, // Plan to instantiate on neighbors
  ) extends StateDescriptor

  /** State for Anchor operator */
  case class Anchor(
    id: StandingQueryId,
    parentId: StandingQueryId,
    mode: RuntimeMode,
    plan: QueryPlan.Anchor,
    target: AnchorTarget,
    onTargetPlan: QueryPlan, // Plan to instantiate on targets
    fallbackOutput: Option[OutputTarget], // Used when hosted on NonNodeActor (no QuineId to route back to)
  ) extends StateDescriptor

  /** State for Filter operator */
  case class Filter(
    id: StandingQueryId,
    parentId: StandingQueryId,
    mode: RuntimeMode,
    plan: QueryPlan.Filter,
    inputId: StandingQueryId,
  ) extends StateDescriptor

  /** State for Project operator */
  case class Project(
    id: StandingQueryId,
    parentId: StandingQueryId,
    mode: RuntimeMode,
    plan: QueryPlan.Project,
    inputId: StandingQueryId,
  ) extends StateDescriptor

  /** State for Distinct operator */
  case class Distinct(
    id: StandingQueryId,
    parentId: StandingQueryId,
    mode: RuntimeMode,
    plan: QueryPlan.Distinct,
    inputId: StandingQueryId,
  ) extends StateDescriptor

  /** State for Unwind operator */
  case class Unwind(
    id: StandingQueryId,
    parentId: StandingQueryId,
    mode: RuntimeMode,
    plan: QueryPlan.Unwind,
    subqueryId: StandingQueryId,
    contextBridgeId: Option[StandingQueryId], // Bridge that feeds Unwind's bindings into subquery
  ) extends StateDescriptor

  /** State for Procedure call operator.
    *
    * Like Unwind, executes a subquery for each result row yielded by the procedure.
    * The procedure is executed when context is injected (for standing queries) or
    * at kickstart (for eager queries).
    */
  case class Procedure(
    id: StandingQueryId,
    parentId: StandingQueryId,
    mode: RuntimeMode,
    plan: QueryPlan.Procedure,
    subqueryId: StandingQueryId,
    contextBridgeId: Option[StandingQueryId], // Bridge that feeds procedure result bindings into subquery
  ) extends StateDescriptor

  /** State for LocalEffect operator */
  case class Effect(
    id: StandingQueryId,
    parentId: StandingQueryId,
    mode: RuntimeMode,
    plan: QueryPlan.LocalEffect,
    inputId: StandingQueryId,
  ) extends StateDescriptor

  /** State for Aggregate operator */
  case class Aggregate(
    id: StandingQueryId,
    parentId: StandingQueryId,
    mode: RuntimeMode,
    plan: QueryPlan.Aggregate,
    inputId: StandingQueryId,
  ) extends StateDescriptor

  /** State for Sort operator */
  case class Sort(
    id: StandingQueryId,
    parentId: StandingQueryId,
    mode: RuntimeMode,
    plan: QueryPlan.Sort,
    inputId: StandingQueryId,
  ) extends StateDescriptor

  /** State for Limit operator */
  case class Limit(
    id: StandingQueryId,
    parentId: StandingQueryId,
    mode: RuntimeMode,
    plan: QueryPlan.Limit,
    inputId: StandingQueryId,
  ) extends StateDescriptor

  /** State for SubscribeToQueryPart operator */
  case class SubscribeToQueryPart(
    id: StandingQueryId,
    parentId: StandingQueryId,
    mode: RuntimeMode,
    plan: QueryPlan.SubscribeToQueryPart,
    queryPartId: QueryPartId,
  ) extends StateDescriptor
}

/** Where final query results should be delivered */
sealed trait OutputTarget

object OutputTarget {

  /** Deliver to a standing query result queue */
  case class StandingQuerySink(sqId: StandingQueryId, namespace: NamespaceId) extends OutputTarget

  /** Deliver to eager query result collector */
  case class EagerCollector(promise: scala.concurrent.Promise[Seq[QueryContext]]) extends OutputTarget

  /** Deliver to lazy query result collector (for testing incremental behavior).
    *
    * Unlike EagerCollector, this does not auto-complete on first notification.
    * Instead, it accumulates all deltas (including retractions) for verification.
    *
    * @param collector The collector that accumulates deltas
    */
  case class LazyCollector(collector: LazyResultCollector) extends OutputTarget

  /** Collector for lazy mode results that tracks incremental updates.
    *
    * Thread-safe accumulator for testing lazy/standing query behavior.
    * Tracks both positive (match) and negative (retraction) deltas.
    */
  class LazyResultCollector {
    private val deltas = new java.util.concurrent.ConcurrentLinkedQueue[Delta.T]()
    private val latch = new java.util.concurrent.CountDownLatch(1)

    /** Record an incoming delta */
    def addDelta(delta: Delta.T): Unit = {
      deltas.add(delta)
      latch.countDown() // Signal that at least one delta arrived
    }

    /** Get all accumulated deltas */
    def allDeltas: Seq[Delta.T] = {
      import scala.jdk.CollectionConverters._
      deltas.asScala.toSeq
    }

    /** Wait for at least one delta to arrive */
    def awaitFirstDelta(timeout: scala.concurrent.duration.Duration): Boolean =
      latch.await(timeout.toMillis, java.util.concurrent.TimeUnit.MILLISECONDS)

    /** Compute the net result (all deltas combined) */
    def netResult: Delta.T =
      allDeltas.foldLeft(Delta.empty)(Delta.add)

    /** Count total positive emissions */
    def positiveCount: Int =
      netResult.values.filter(_ > 0).sum

    /** Count total negative emissions (retractions) */
    def negativeCount: Int =
      netResult.values.filter(_ < 0).map(_.abs).sum

    /** Check if any retractions have occurred */
    def hasRetractions: Boolean =
      allDeltas.exists(_.values.exists(_ < 0))

    /** Clear all accumulated deltas */
    def clear(): Unit = deltas.clear()
  }

  /** Deliver to a state on another node (for cross-node subscriptions).
    *
    * Used by Expand and Anchor to receive results from plans dispatched to other nodes.
    *
    * @param originNode The node that dispatched the plan and wants results
    * @param stateId The state on originNode that should receive the results
    * @param namespace The namespace for message routing
    * @param dispatchId The sqid used when dispatching, used as 'from' so the state can identify this as expected results
    */
  case class RemoteState(
    originNode: com.thatdot.common.quineid.QuineId,
    stateId: StandingQueryId,
    namespace: com.thatdot.quine.graph.NamespaceId,
    dispatchId: StandingQueryId,
  ) extends OutputTarget

  /** Deliver to a state on the hosting actor (for NonNodeActor subscriptions).
    *
    * Used by Anchors on NonNodeActor to receive results from plans dispatched to nodes.
    * Since NonNodeActor doesn't have a QuineId, we route via ActorRef instead.
    *
    * @param hostActorRef The actor hosting the state that wants results
    * @param stateId The state on the host actor that should receive the results
    * @param dispatchId The sqid used when dispatching, used as 'from' so the Anchor can identify this as target results
    */
  case class HostedState(
    hostActorRef: org.apache.pekko.actor.ActorRef,
    stateId: StandingQueryId,
    dispatchId: StandingQueryId,
  ) extends OutputTarget
}

/** Immutable builder context accumulated during graph construction */
private[quinepattern] case class BuildContext(
  states: Map[StandingQueryId, StateDescriptor],
  edges: Map[StandingQueryId, StandingQueryId], // child -> parent
  leaves: Set[StandingQueryId],
) {
  def addState(desc: StateDescriptor, isLeaf: Boolean): BuildContext =
    copy(
      states = states + (desc.id -> desc),
      edges = if (desc.id != desc.parentId) edges + (desc.id -> desc.parentId) else edges,
      leaves = if (isLeaf) leaves + desc.id else leaves,
    )

  def markNotLeaf(id: StandingQueryId): BuildContext =
    copy(leaves = leaves - id)
}

private[quinepattern] object BuildContext {
  val empty: BuildContext = BuildContext(Map.empty, Map.empty, Set.empty)
}

/** Builds a StateGraph from a QueryPlan.
  *
  * This is pure - no Actor dependencies, no side effects.
  * The resulting StateGraph can be tested and inspected before installation.
  */
object QueryStateBuilder {

  /** Build a state graph from a query plan.
    *
    * @param plan            The query plan to build from
    * @param mode            Eager or Lazy execution mode
    * @param params            Query parameters
    * @param namespace         The namespace for this query
    * @param output            Where to deliver results
    * @param injectedContext   Context bindings from parent (e.g., Anchor dispatch), seeded into Unit states
    * @param returnColumns     Columns to include in output (from RETURN clause), extracted before pushIntoAnchors
    * @param outputNameMapping Maps internal binding IDs to human-readable output names
    * @return A StateGraph ready for installation
    */
  def build(
    plan: QueryPlan,
    mode: RuntimeMode,
    params: Map[Symbol, Value],
    namespace: NamespaceId,
    output: OutputTarget,
    injectedContext: Map[Symbol, Value] = Map.empty,
    returnColumns: Option[Set[Symbol]] = None,
    outputNameMapping: Map[Symbol, Symbol] = Map.empty,
  ): StateGraph = {
    val rootId = StandingQueryId.fresh()
    val outputDesc = StateDescriptor.Output(rootId, mode, output)

    val initialContext = BuildContext.empty
      .addState(outputDesc, isLeaf = false)

    val (finalContext, _) = buildPlan(plan, rootId, mode, initialContext, Some(output))

    StateGraph(
      rootId = rootId,
      states = finalContext.states,
      leaves = finalContext.leaves,
      edges = finalContext.edges,
      params = params,
      injectedContext = injectedContext,
      returnColumns = returnColumns,
      outputNameMapping = outputNameMapping,
    )
  }

  /** Recursively build states for a plan subtree.
    *
    * @param plan           The plan node to process
    * @param parentId       The parent state that will receive notifications
    * @param mode           Eager or Lazy execution mode
    * @param ctx            Accumulated build context
    * @param fallbackOutput For root Anchors, the output target to use when hosted on NonNodeActor
    * @return Tuple of (updated context with new states added, this plan's root state ID)
    */
  private def buildPlan(
    plan: QueryPlan,
    parentId: StandingQueryId,
    mode: RuntimeMode,
    ctx: BuildContext,
    fallbackOutput: Option[OutputTarget],
  ): (BuildContext, StandingQueryId) = {
    plan match {

      // === LEAF OPERATORS ===

      case p @ QueryPlan.LocalId(binding) =>
        val id = StandingQueryId.fresh()
        val desc = StateDescriptor.WatchId(id, parentId, mode, p, binding)
        (ctx.addState(desc, isLeaf = true), id)

      case p @ QueryPlan.LocalProperty(property, aliasAs, constraint) =>
        val id = StandingQueryId.fresh()
        val desc = StateDescriptor.WatchProperty(id, parentId, mode, p, property, aliasAs, constraint)
        (ctx.addState(desc, isLeaf = true), id)

      case p @ QueryPlan.LocalAllProperties(binding) =>
        val id = StandingQueryId.fresh()
        val desc = StateDescriptor.WatchAllProperties(id, parentId, mode, p, binding)
        (ctx.addState(desc, isLeaf = true), id)

      case p @ QueryPlan.LocalLabels(aliasAs, constraint) =>
        val id = StandingQueryId.fresh()
        val desc = StateDescriptor.WatchLabels(id, parentId, mode, p, aliasAs, constraint)
        (ctx.addState(desc, isLeaf = true), id)

      case p @ QueryPlan.LocalNode(binding) =>
        val id = StandingQueryId.fresh()
        val desc = StateDescriptor.WatchNode(id, parentId, mode, p, binding)
        (ctx.addState(desc, isLeaf = true), id)

      case QueryPlan.Unit =>
        val id = StandingQueryId.fresh()
        val desc = StateDescriptor.Unit(id, parentId, mode, QueryPlan.Unit)
        (ctx.addState(desc, isLeaf = true), id)

      case p @ QueryPlan.SubscribeToQueryPart(queryPartId, _) =>
        val id = StandingQueryId.fresh()
        val desc = StateDescriptor.SubscribeToQueryPart(id, parentId, mode, p, queryPartId)
        (ctx.addState(desc, isLeaf = true), id)

      // === COMBINING OPERATORS ===

      case p @ QueryPlan.CrossProduct(queries, _) =>
        val id = StandingQueryId.fresh()
        // First add this state, then build children
        val ctxWithProduct = ctx.addState(
          StateDescriptor.Product(id, parentId, mode, p, Nil), // childIds filled below
          isLeaf = false,
        )
        // Build each child with this product as parent
        val (finalCtx, childIds) = queries.foldLeft((ctxWithProduct, List.empty[StandingQueryId])) {
          case ((accCtx, accIds), childPlan) =>
            val (childCtx, childId) = buildPlan(childPlan, id, mode, accCtx, fallbackOutput)
            (childCtx, accIds :+ childId)
        }
        // Update the product state with actual child IDs
        val updatedDesc = StateDescriptor.Product(id, parentId, mode, p, childIds)
        (finalCtx.copy(states = finalCtx.states + (id -> updatedDesc)), id)

      case p @ QueryPlan.Sequence(first, andThen, contextFlow) =>
        val id = StandingQueryId.fresh()
        // Build first child
        val (ctxAfterFirst, firstId) = buildPlan(first, id, mode, ctx, fallbackOutput)

        // Create a ContextBridge to inject first's context into andThen
        val bridgeId = StandingQueryId.fresh()

        // Find the entry point of andThen (Unit leaf or first state that needs context)
        // and build andThen with the bridge as the parent for its entry point
        val (ctxAfterAndThen, andThenId, maybeEntryPointId) =
          buildPlanWithContextInjection(andThen, id, mode, ctxAfterFirst, fallbackOutput, bridgeId)

        // Add the context bridge state
        val ctxWithBridge = maybeEntryPointId match {
          case Some(entryPointId) =>
            // Bridge receives from Sequence and forwards to andThen's entry point
            val bridgeDesc = StateDescriptor.ContextBridge(bridgeId, entryPointId, mode, id)
            ctxAfterAndThen
              .addState(bridgeDesc, isLeaf = false) // Bridge is not a leaf - it waits for Sequence
              .markNotLeaf(entryPointId) // Entry point is no longer a leaf - it receives from bridge
          case None =>
            // No entry point found (andThen doesn't need context injection)
            ctxAfterAndThen
        }

        // Add sequence state with bridge ID
        val desc = StateDescriptor.Sequence(
          id,
          parentId,
          mode,
          p,
          firstId,
          andThenId,
          maybeEntryPointId.map(_ => bridgeId), // Only set bridgeId if we have an entry point
          contextFlow,
        )
        (ctxWithBridge.addState(desc, isLeaf = false), id)

      // === DISPATCH OPERATORS ===

      case p @ QueryPlan.Expand(_, _, onNeighbor) =>
        val id = StandingQueryId.fresh()
        // Expand doesn't build the onNeighbor plan here - it's instantiated
        // at runtime when edges are discovered. We just store the plan.
        val desc = StateDescriptor.Expand(id, parentId, mode, p, onNeighbor)
        (ctx.addState(desc, isLeaf = true), id) // Leaf in terms of static structure

      case p @ QueryPlan.Anchor(target, onTarget) =>
        val id = StandingQueryId.fresh()
        // Anchor doesn't build the onTarget plan here - it's instantiated
        // at runtime on target nodes. We just store the plan.
        // For root Anchors (directly under Output), we pass the fallbackOutput so they can route
        // results correctly when hosted on NonNodeActor (which has no QuineId for RemoteState).
        val desc = StateDescriptor.Anchor(id, parentId, mode, p, target, onTarget, fallbackOutput)
        (ctx.addState(desc, isLeaf = true), id) // Leaf in terms of static structure

      // === TRANSFORM OPERATORS ===

      case p @ QueryPlan.Filter(_, input) =>
        val id = StandingQueryId.fresh()
        val (ctxAfterInput, inputId) = buildPlan(input, id, mode, ctx, fallbackOutput)
        val desc = StateDescriptor.Filter(id, parentId, mode, p, inputId)
        (ctxAfterInput.addState(desc, isLeaf = false), id)

      case p @ QueryPlan.Project(_, _, input) =>
        val id = StandingQueryId.fresh()
        val (ctxAfterInput, inputId) = buildPlan(input, id, mode, ctx, fallbackOutput)
        val desc = StateDescriptor.Project(id, parentId, mode, p, inputId)
        (ctxAfterInput.addState(desc, isLeaf = false), id)

      case p @ QueryPlan.Distinct(input) =>
        val id = StandingQueryId.fresh()
        val (ctxAfterInput, inputId) = buildPlan(input, id, mode, ctx, fallbackOutput)
        val desc = StateDescriptor.Distinct(id, parentId, mode, p, inputId)
        (ctxAfterInput.addState(desc, isLeaf = false), id)

      // === UNWIND ===

      case p @ QueryPlan.Unwind(_, _, subquery) =>
        val id = StandingQueryId.fresh()
        // Create a bridge ID for forwarding bindings to subquery
        val bridgeId = StandingQueryId.fresh()
        // Use buildPlanWithContextInjection to find subquery's entry point
        val (ctxAfterSubquery, subqueryId, maybeEntryPointId) =
          buildPlanWithContextInjection(subquery, id, mode, ctx, fallbackOutput, bridgeId)

        // Create context bridge if subquery has an entry point
        val ctxWithBridge = maybeEntryPointId match {
          case Some(entryPointId) =>
            // Bridge receives from Unwind and forwards to subquery's entry point
            val bridgeDesc = StateDescriptor.ContextBridge(bridgeId, entryPointId, mode, id)
            ctxAfterSubquery
              .addState(bridgeDesc, isLeaf = false) // Bridge is not a leaf - it waits for Unwind
              .markNotLeaf(entryPointId) // Entry point is no longer a leaf - it receives from bridge
          case None =>
            ctxAfterSubquery
        }

        val desc = StateDescriptor.Unwind(id, parentId, mode, p, subqueryId, maybeEntryPointId.map(_ => bridgeId))
        // Unwind IS a leaf - it generates initial bindings from the list expression
        (ctxWithBridge.addState(desc, isLeaf = true), id)

      // === PROCEDURE CALL ===

      case p @ QueryPlan.Procedure(_, _, _, subquery) =>
        val id = StandingQueryId.fresh()
        // Create a bridge ID for forwarding procedure result bindings to subquery
        val bridgeId = StandingQueryId.fresh()
        // Use buildPlanWithContextInjection to find subquery's entry point
        val (ctxAfterSubquery, subqueryId, maybeEntryPointId) =
          buildPlanWithContextInjection(subquery, id, mode, ctx, fallbackOutput, bridgeId)

        // Create context bridge if subquery has an entry point
        val ctxWithBridge = maybeEntryPointId match {
          case Some(entryPointId) =>
            // Bridge receives from Procedure and forwards to subquery's entry point
            val bridgeDesc = StateDescriptor.ContextBridge(bridgeId, entryPointId, mode, id)
            ctxAfterSubquery
              .addState(bridgeDesc, isLeaf = false) // Bridge is not a leaf - it waits for Procedure
              .markNotLeaf(entryPointId) // Entry point is no longer a leaf - it receives from bridge
          case None =>
            ctxAfterSubquery
        }

        val desc = StateDescriptor.Procedure(id, parentId, mode, p, subqueryId, maybeEntryPointId.map(_ => bridgeId))
        // Procedure IS a leaf - it generates initial bindings from procedure results
        (ctxWithBridge.addState(desc, isLeaf = true), id)

      // === EFFECT OPERATORS ===

      case p @ QueryPlan.LocalEffect(_, input) =>
        val id = StandingQueryId.fresh()
        val (ctxAfterInput, inputId) = buildPlan(input, id, mode, ctx, fallbackOutput)
        val desc = StateDescriptor.Effect(id, parentId, mode, p, inputId)
        (ctxAfterInput.addState(desc, isLeaf = false), id)

      // === MATERIALIZING OPERATORS ===

      case p @ QueryPlan.Aggregate(_, _, input) =>
        val id = StandingQueryId.fresh()
        val (ctxAfterInput, inputId) = buildPlan(input, id, mode, ctx, fallbackOutput)
        val desc = StateDescriptor.Aggregate(id, parentId, mode, p, inputId)
        (ctxAfterInput.addState(desc, isLeaf = false), id)

      case p @ QueryPlan.Sort(_, input) =>
        val id = StandingQueryId.fresh()
        val (ctxAfterInput, inputId) = buildPlan(input, id, mode, ctx, fallbackOutput)
        val desc = StateDescriptor.Sort(id, parentId, mode, p, inputId)
        (ctxAfterInput.addState(desc, isLeaf = false), id)

      case p @ QueryPlan.Limit(_, input) =>
        val id = StandingQueryId.fresh()
        val (ctxAfterInput, inputId) = buildPlan(input, id, mode, ctx, fallbackOutput)
        val desc = StateDescriptor.Limit(id, parentId, mode, p, inputId)
        (ctxAfterInput.addState(desc, isLeaf = false), id)
    }
  }

  /** Build a plan for Sequence's andThen, identifying the entry point that needs context injection.
    *
    * The entry point is the state that should receive context from first (via ContextBridge).
    * This is typically:
    * - Unit leaf (for plans like Project(columns, Unit))
    * - The root itself (for dispatch operators like Anchor that need context for target evaluation)
    *
    * @return (updated context, andThen root ID, optional entry point ID)
    */
  private def buildPlanWithContextInjection(
    plan: QueryPlan,
    parentId: StandingQueryId,
    mode: RuntimeMode,
    ctx: BuildContext,
    fallbackOutput: Option[OutputTarget],
    bridgeId: StandingQueryId, // The bridge that will provide context
  ): (BuildContext, StandingQueryId, Option[StandingQueryId]) =
    plan match {
      // For Unit, the entry point IS the Unit - it will receive from bridge instead of kickstarting
      case QueryPlan.Unit =>
        val id = StandingQueryId.fresh()
        // Create Unit state but with bridge as its "source" - it will receive context, not generate it
        val desc = StateDescriptor.Unit(id, parentId, mode, QueryPlan.Unit)
        (ctx.addState(desc, isLeaf = true), id, Some(id))

      // For dispatch operators (Anchor, Expand), they ARE the entry point
      // They need context to evaluate their target/edges
      case p @ QueryPlan.Anchor(target, onTarget) =>
        val id = StandingQueryId.fresh()
        val desc = StateDescriptor.Anchor(id, parentId, mode, p, target, onTarget, fallbackOutput)
        (ctx.addState(desc, isLeaf = true), id, Some(id))

      case p @ QueryPlan.Expand(_, _, onNeighbor) =>
        val id = StandingQueryId.fresh()
        val desc = StateDescriptor.Expand(id, parentId, mode, p, onNeighbor)
        (ctx.addState(desc, isLeaf = true), id, Some(id))

      // For transform operators (Project, Filter, etc.), recurse to find the entry point in their input
      case p @ QueryPlan.Project(_, _, input) =>
        val id = StandingQueryId.fresh()
        val (ctxAfterInput, inputId, maybeEntryPoint) =
          buildPlanWithContextInjection(input, id, mode, ctx, fallbackOutput, bridgeId)
        val desc = StateDescriptor.Project(id, parentId, mode, p, inputId)
        (ctxAfterInput.addState(desc, isLeaf = false), id, maybeEntryPoint)

      case p @ QueryPlan.Filter(_, input) =>
        val id = StandingQueryId.fresh()
        val (ctxAfterInput, inputId, maybeEntryPoint) =
          buildPlanWithContextInjection(input, id, mode, ctx, fallbackOutput, bridgeId)
        val desc = StateDescriptor.Filter(id, parentId, mode, p, inputId)
        (ctxAfterInput.addState(desc, isLeaf = false), id, maybeEntryPoint)

      case p @ QueryPlan.Distinct(input) =>
        val id = StandingQueryId.fresh()
        val (ctxAfterInput, inputId, maybeEntryPoint) =
          buildPlanWithContextInjection(input, id, mode, ctx, fallbackOutput, bridgeId)
        val desc = StateDescriptor.Distinct(id, parentId, mode, p, inputId)
        (ctxAfterInput.addState(desc, isLeaf = false), id, maybeEntryPoint)

      case p @ QueryPlan.LocalEffect(_, input) =>
        val id = StandingQueryId.fresh()
        val (ctxAfterInput, inputId, maybeEntryPoint) =
          buildPlanWithContextInjection(input, id, mode, ctx, fallbackOutput, bridgeId)
        val desc = StateDescriptor.Effect(id, parentId, mode, p, inputId)
        (ctxAfterInput.addState(desc, isLeaf = false), id, maybeEntryPoint)

      case p @ QueryPlan.Aggregate(_, _, input) =>
        val id = StandingQueryId.fresh()
        val (ctxAfterInput, inputId, maybeEntryPoint) =
          buildPlanWithContextInjection(input, id, mode, ctx, fallbackOutput, bridgeId)
        val desc = StateDescriptor.Aggregate(id, parentId, mode, p, inputId)
        (ctxAfterInput.addState(desc, isLeaf = false), id, maybeEntryPoint)

      case p @ QueryPlan.Sort(_, input) =>
        val id = StandingQueryId.fresh()
        val (ctxAfterInput, inputId, maybeEntryPoint) =
          buildPlanWithContextInjection(input, id, mode, ctx, fallbackOutput, bridgeId)
        val desc = StateDescriptor.Sort(id, parentId, mode, p, inputId)
        (ctxAfterInput.addState(desc, isLeaf = false), id, maybeEntryPoint)

      case p @ QueryPlan.Limit(_, input) =>
        val id = StandingQueryId.fresh()
        val (ctxAfterInput, inputId, maybeEntryPoint) =
          buildPlanWithContextInjection(input, id, mode, ctx, fallbackOutput, bridgeId)
        val desc = StateDescriptor.Limit(id, parentId, mode, p, inputId)
        (ctxAfterInput.addState(desc, isLeaf = false), id, maybeEntryPoint)

      // For nested Sequence, the entry point is in first's subtree
      // Context flows: outer bridge -> first's entry point -> first -> (inner bridge) -> andThen's entry point
      case p @ QueryPlan.Sequence(first, andThen, contextFlow) =>
        val id = StandingQueryId.fresh()
        // Recurse into first to find its entry point (using outer bridgeId)
        val (ctxAfterFirst, firstId, maybeEntryInFirst) =
          buildPlanWithContextInjection(first, id, mode, ctx, fallbackOutput, bridgeId)

        // This inner Sequence ALSO needs its own bridge for first->andThen context flow!
        // Create a NEW bridge for this Sequence's internal context forwarding
        val innerBridgeId = StandingQueryId.fresh()

        // Build andThen with context injection using this Sequence's inner bridge
        val (ctxAfterAndThen, andThenId, maybeEntryInAndThen) =
          buildPlanWithContextInjection(andThen, id, mode, ctxAfterFirst, fallbackOutput, innerBridgeId)

        // Add the inner context bridge if andThen has an entry point
        val ctxWithBridge = maybeEntryInAndThen match {
          case Some(entryPointId) =>
            // Inner bridge receives from this Sequence and forwards to andThen's entry point
            val bridgeDesc = StateDescriptor.ContextBridge(innerBridgeId, entryPointId, mode, id)
            ctxAfterAndThen
              .addState(bridgeDesc, isLeaf = false)
              .markNotLeaf(entryPointId)
          case None =>
            ctxAfterAndThen
        }

        val desc = StateDescriptor.Sequence(
          id,
          parentId,
          mode,
          p,
          firstId,
          andThenId,
          maybeEntryInAndThen.map(_ => innerBridgeId), // Use inner bridge for first->andThen flow
          contextFlow,
        )
        (ctxWithBridge.addState(desc, isLeaf = false), id, maybeEntryInFirst)

      // For CrossProduct, collect ALL child entry points (not just the first)
      // If multiple children need context, CrossProduct itself becomes the entry point
      // and forwards context to all child entry points
      case p @ QueryPlan.CrossProduct(queries, _) =>
        val id = StandingQueryId.fresh()
        var currentCtx = ctx
        var allChildEntryPoints = Set.empty[StandingQueryId]
        val childIds = queries.map { childPlan =>
          val (childCtx, childId, maybeChildEntry) =
            buildPlanWithContextInjection(childPlan, id, mode, currentCtx, fallbackOutput, bridgeId)
          currentCtx = childCtx
          // Collect ALL child entry points
          maybeChildEntry.foreach(ep => allChildEntryPoints += ep)
          childId
        }

        // If any children need context, CrossProduct becomes the entry point
        // and will forward context to all child entry points
        val (finalCtx, entryPoint) = if (allChildEntryPoints.nonEmpty) {
          // Mark all child entry points as not-leaves (they receive from CrossProduct)
          val ctxWithMarkedChildren = allChildEntryPoints.foldLeft(currentCtx)(_.markNotLeaf(_))
          val desc = StateDescriptor.Product(id, parentId, mode, p, childIds, allChildEntryPoints)
          (ctxWithMarkedChildren.addState(desc, isLeaf = false), Some(id))
        } else {
          val desc = StateDescriptor.Product(id, parentId, mode, p, childIds)
          (currentCtx.addState(desc, isLeaf = false), None)
        }
        (finalCtx, id, entryPoint)

      // For Unwind, create bridge to subquery's entry point
      // Unwind itself becomes an entry point (it needs context from parent to combine with unwound values)
      case p @ QueryPlan.Unwind(_, _, subquery) =>
        val id = StandingQueryId.fresh()
        // Create Unwind's own context bridge to its subquery
        val unwindBridgeId = StandingQueryId.fresh()
        // Find subquery's entry point using buildPlanWithContextInjection
        val (ctxAfterSubquery, subqueryId, maybeSubqueryEntry) =
          buildPlanWithContextInjection(subquery, id, mode, ctx, fallbackOutput, unwindBridgeId)

        val ctxWithBridge = maybeSubqueryEntry match {
          case Some(entryPointId) =>
            val bridgeDesc = StateDescriptor.ContextBridge(unwindBridgeId, entryPointId, mode, id)
            ctxAfterSubquery
              .addState(bridgeDesc, isLeaf = false)
              .markNotLeaf(entryPointId)
          case None =>
            ctxAfterSubquery
        }

        val desc =
          StateDescriptor.Unwind(id, parentId, mode, p, subqueryId, maybeSubqueryEntry.map(_ => unwindBridgeId))
        // Unwind IS the entry point - it receives context from parent and combines with unwound values
        (ctxWithBridge.addState(desc, isLeaf = true), id, Some(id))

      // For Procedure, create bridge to subquery's entry point
      // Procedure itself becomes an entry point (it needs context from parent to evaluate arguments)
      case p @ QueryPlan.Procedure(_, _, _, subquery) =>
        val id = StandingQueryId.fresh()
        // Create Procedure's own context bridge to its subquery
        val procBridgeId = StandingQueryId.fresh()
        // Find subquery's entry point using buildPlanWithContextInjection
        val (ctxAfterSubquery, subqueryId, maybeSubqueryEntry) =
          buildPlanWithContextInjection(subquery, id, mode, ctx, fallbackOutput, procBridgeId)

        val ctxWithBridge = maybeSubqueryEntry match {
          case Some(entryPointId) =>
            val bridgeDesc = StateDescriptor.ContextBridge(procBridgeId, entryPointId, mode, id)
            ctxAfterSubquery
              .addState(bridgeDesc, isLeaf = false)
              .markNotLeaf(entryPointId)
          case None =>
            ctxAfterSubquery
        }

        val desc =
          StateDescriptor.Procedure(id, parentId, mode, p, subqueryId, maybeSubqueryEntry.map(_ => procBridgeId))
        // Procedure IS the entry point - it receives context from parent and combines with procedure results
        (ctxWithBridge.addState(desc, isLeaf = true), id, Some(id))

      // For other leaf operators that generate their own context (LocalId, LocalProperty, etc.),
      // there's no entry point to inject - they don't need context from first
      case _ =>
        val (ctxAfter, rootId) = buildPlan(plan, parentId, mode, ctx, fallbackOutput)
        (ctxAfter, rootId, None)
    }
}

/** Query execution context - bindings from symbols to values.
  *
  * This is the unit of data that flows through the state graph.
  * In lazy mode, results include multiplicity (+1 for assertion, -1 for retraction).
  */
case class QueryContext(bindings: Map[Symbol, Value]) {
  def get(symbol: Symbol): Option[Value] = bindings.get(symbol)
  def +(kv: (Symbol, Value)): QueryContext = QueryContext(bindings + kv)
  def ++(other: QueryContext): QueryContext = QueryContext(bindings ++ other.bindings)
  def ++(other: Map[Symbol, Value]): QueryContext = QueryContext(bindings ++ other)
}

object QueryContext {
  val empty: QueryContext = QueryContext(Map.empty)
}

/** Runtime execution mode */
sealed trait RuntimeMode

object RuntimeMode {

  /** Execute once, collect all results */
  case object Eager extends RuntimeMode

  /** Standing query - maintain state and emit deltas */
  case object Lazy extends RuntimeMode
}
