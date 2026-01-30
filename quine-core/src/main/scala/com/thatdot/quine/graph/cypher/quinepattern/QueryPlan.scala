package com.thatdot.quine.graph.cypher.quinepattern

import com.thatdot.language.ast.{Expression, Value}
import com.thatdot.quine.model.EdgeDirection

/** Query plan algebra for QuinePattern.
  *
  * This is a tree-structured algebra where each operator's children form a proper tree.
  * The tree property ensures that when a leaf changes, only its ancestors need to update,
  * enabling O(depth) update propagation rather than O(total matches).
  *
  * Key design principles:
  *   1. Tree Property: No cycles, no cross-references between siblings
  *   2. Subtree Containment: Dispatch operators contain their continuation as a subtree
  *   3. Separation of Concerns: Each operator does one thing
  *   4. Updates Flow Up: Changes propagate from leaves to root
  */
sealed trait QueryPlan {

  /** Direct children of this query plan (for traversal/analysis) */
  def children: Seq[QueryPlan]
}

object QueryPlan {

  // ============================================================
  // LEAF OPERATORS (no children, run on current node)
  // ============================================================

  /** Emit the current node's ID bound to `binding` as a Value.NodeId.
    *
    * This is a pure identity operator - it only provides the node's QuineId.
    * The emitted value is stable (node IDs don't change), so this operator
    * emits once on kickstart and never retracts.
    *
    * Note: Properties and labels are NOT included. Use:
    *   - LocalProperty for individual property access (with constraint pushdown)
    *   - LocalAllProperties to bind all properties as a Map
    *   - LocalLabels to watch/constrain node labels
    *
    * @param binding The symbol to bind the node ID to
    */
  case class LocalId(binding: Symbol) extends QueryPlan {
    def children: Seq[QueryPlan] = Seq.empty
  }

  /** Subscribe to an existing standing query's maintained state.
    *
    * Enables reuse of deployed standing queries as "indexes". A sophisticated
    * query planner can recognize when an existing standing query covers (fully
    * or partially) a needed pattern, and subscribe to its maintained state
    * instead of re-executing the pattern from scratch.
    *
    * EAGER MODE: Takes a snapshot of the standing query's current accumulated
    * state and emits all results. Since eager queries are run-once, lifecycle
    * is simple - the subscription exists only for the query's duration.
    *
    * LAZY MODE: Subscribes to the standing query's delta stream. Receives
    * initial snapshot, then continues receiving deltas as the underlying
    * data changes.
    *
    * @param queryPartId Identifies the standing query part to subscribe to
    * @param projection Maps the standing query's output bindings to this query's
    *                   bindings. E.g., if standing query outputs 'a and 'b, and
    *                   this query needs 'x and 'y, projection = Map('a -> 'x, 'b -> 'y)
    */
  case class SubscribeToQueryPart(
    queryPartId: QueryPartId,
    projection: Map[Symbol, Symbol],
  ) extends QueryPlan {
    def children: Seq[QueryPlan] = Seq.empty
  }

  /** Watch a property on the current node.
    *
    * Emits when property matches constraint. In lazy mode, re-emits on changes
    * (retraction of old value + assertion of new value).
    *
    * @param property Which property to watch
    * @param aliasAs If Some, bind the property value to this name; if None, just check constraint
    * @param constraint Predicate the property value must satisfy
    */
  case class LocalProperty(
    property: Symbol,
    aliasAs: Option[Symbol],
    constraint: PropertyConstraint = PropertyConstraint.Any,
  ) extends QueryPlan {
    def children: Seq[QueryPlan] = Seq.empty
  }

  /** Watch all properties on the current node.
    *
    * Binds all properties as a Map to the given binding.
    */
  case class LocalAllProperties(
    binding: Symbol,
  ) extends QueryPlan {
    def children: Seq[QueryPlan] = Seq.empty
  }

  /** Watch labels on the current node */
  case class LocalLabels(
    aliasAs: Option[Symbol],
    constraint: LabelConstraint,
  ) extends QueryPlan {
    def children: Seq[QueryPlan] = Seq.empty
  }

  /** Emit a complete node value (ID + labels + properties).
    *
    * This operator watches both properties and labels, emitting a full Value.Node.
    * The labelsProperty (configurable, typically __LABEL) is filtered from properties
    * since labels are provided separately.
    *
    * Use this for bare node references like `RETURN n` where the full node is needed.
    * For individual components, use:
    *   - LocalId for id(n)
    *   - LocalAllProperties for properties(n)
    *   - LocalLabels for labels(n)
    *
    * @param binding The symbol to bind the node value to
    */
  case class LocalNode(binding: Symbol) extends QueryPlan {
    def children: Seq[QueryPlan] = Seq.empty
  }

  /** Emit a single empty result (identity element for CrossProduct) */
  case object Unit extends QueryPlan {
    def children: Seq[QueryPlan] = Seq.empty
  }

  // ============================================================
  // CROSS-PRODUCT (MVSQ-style independent combination)
  // ============================================================

  /** Cross-product of independent subqueries.
    *
    * All children are evaluated concurrently on the current node.
    * Results are combined via cross-product.
    *
    * KEY PROPERTY: When one child updates, only that child's contribution
    * to the cross-product changes. Other children's contributions are cached.
    *
    * This is the core MVSQ combining operator.
    *
    * @param queries Non-empty list of independent subqueries
    * @param emitSubscriptionsLazily If true, subscribe to children left-to-right
    *                                only once previous child has some results
    */
  case class CrossProduct(
    queries: List[QueryPlan],
    emitSubscriptionsLazily: Boolean = false,
  ) extends QueryPlan {
    def children: Seq[QueryPlan] = queries
  }

  // ============================================================
  // SEQUENCE (Imperative extension for WITH clauses)
  // ============================================================

  /** Sequential composition where later steps depend on earlier steps' context.
    *
    * Handles Cypher's imperative semantics that have sequential dependencies:
    *
    * {{{
    * MATCH (a:Person)
    * WITH a.friendId AS fid    <- first produces {fid: ...}
    * MATCH (b) WHERE id(b) = fid  <- andThen uses fid from context
    * }}}
    *
    * @param first The first step, produces results with context
    * @param andThen The continuation, receives context from first
    * @param contextFlow How context flows from first to andThen
    */
  case class Sequence(
    first: QueryPlan,
    andThen: QueryPlan,
    contextFlow: ContextFlow,
  ) extends QueryPlan {
    def children: Seq[QueryPlan] = Seq(first, andThen)
  }

  // ============================================================
  // DISPATCH OPERATORS (contain subtree to run elsewhere)
  // ============================================================

  /** Expand along edges and run subquery on each neighbor.
    *
    * For each edge matching label/direction, instantiates `onNeighbor`
    * on the neighbor node. Results flow back through this operator.
    *
    * This is the standard "Expand" operation from query planning literature
    * (e.g., Neo4j's query planner). It represents relative positioning -
    * from the current node, follow edges to neighbors.
    *
    * CRITICAL: `onNeighbor` is a SUBTREE, not a dispatch target.
    * This preserves the tree property - the neighbor's query is part of
    * this node's query tree.
    *
    * @param edgeLabel If Some, only edges with this label
    * @param direction Edge direction to match
    * @param onNeighbor Subtree to instantiate on each neighbor
    */
  case class Expand(
    edgeLabel: Option[Symbol],
    direction: EdgeDirection,
    onNeighbor: QueryPlan,
  ) extends QueryPlan {
    def children: Seq[QueryPlan] = Seq(onNeighbor)
  }

  /** Anchor execution on target node(s) and run subquery there.
    *
    * Evaluates `target` to determine which node(s) to anchor on,
    * then instantiates `onTarget` on those nodes. This is absolute
    * positioning - go to specific nodes determined by the target.
    *
    * "Anchor" is the standard query planning term for the entry point
    * of a query or subquery.
    *
    * @param target How to determine target node(s)
    * @param onTarget Subtree to instantiate on target(s)
    */
  case class Anchor(
    target: AnchorTarget,
    onTarget: QueryPlan,
  ) extends QueryPlan {
    def children: Seq[QueryPlan] = Seq(onTarget)
  }

  // ============================================================
  // TRANSFORM OPERATORS (single child, modify results)
  // ============================================================

  /** Filter results by predicate.
    *
    * Only results where predicate evaluates to true are emitted.
    */
  case class Filter(
    predicate: Expression,
    input: QueryPlan,
  ) extends QueryPlan {
    def children: Seq[QueryPlan] = Seq(input)
  }

  /** Project/rename/compute columns.
    *
    * @param columns New columns to add (can reference input columns)
    * @param dropExisting If true, output only has `columns`; if false, includes input columns too
    * @param input Source of results to project
    */
  case class Project(
    columns: List[Projection],
    dropExisting: Boolean,
    input: QueryPlan,
  ) extends QueryPlan {
    def children: Seq[QueryPlan] = Seq(input)
  }

  /** Deduplicate results.
    *
    * Maintains count of how many times each distinct result has been seen
    * from upstream. Only emits changes to the deduplicated set.
    *
    * EAGER MODE:
    *   - Track seen results in Set
    *   - First occurrence: emit
    *   - Subsequent occurrences: suppress
    *
    * LAZY MODE (with retractions):
    *   - Track count per distinct result: Map[QueryContext, Int]
    *   - First occurrence (0 → 1): emit assertion (+1)
    *   - Subsequent (n → n+1 where n > 0): suppress (already emitted)
    *   - Retraction (n → n-1 where n > 1): suppress (still have copies)
    *   - Final retraction (1 → 0): emit retraction (-1)
    *
    * This ensures downstream sees each distinct result exactly once,
    * with proper retraction when the last copy disappears.
    *
    * @param input Source of results to deduplicate
    */
  case class Distinct(
    input: QueryPlan,
  ) extends QueryPlan {
    def children: Seq[QueryPlan] = Seq(input)
  }

  // ============================================================
  // UNWIND (Iterate over lists)
  // ============================================================

  /** Iterate over a list, binding each element.
    *
    * For each element in the list expression, executes subquery with
    * that element bound to `binding`. Results are concatenated.
    *
    * Used for:
    *   - `UNWIND [1,2,3] AS x`
    *   - `MATCH (b) WHERE id(b) IN [...]` (rewritten to Unwind + Anchor)
    *
    * @param list Expression evaluating to a list
    * @param binding Symbol to bind each element to
    * @param subquery Query to run for each element
    */
  case class Unwind(
    list: Expression,
    binding: Symbol,
    subquery: QueryPlan,
  ) extends QueryPlan {
    def children: Seq[QueryPlan] = Seq(subquery)
  }

  // ============================================================
  // EFFECT OPERATORS
  // ============================================================

  /** Execute LOCAL side effects on the current node, then pass through.
    *
    * Effects in this operator apply to the node where execution is happening.
    * This is important for distributed execution - effects must run on the
    * node they affect.
    *
    * For cross-node effects (like creating edges), the plan must navigate
    * to each node and apply LocalEffect there.
    *
    * @param effects Side effects to execute on THIS node
    * @param input Source of results that trigger effects
    */
  case class LocalEffect(
    effects: List[LocalQueryEffect],
    input: QueryPlan,
  ) extends QueryPlan {
    def children: Seq[QueryPlan] = Seq(input)
  }

  // ============================================================
  // MATERIALIZING OPERATORS (buffer until input complete)
  // ============================================================
  //
  // These operators need to see all input before producing output.
  //
  // EAGER MODE: Wait until all children have notified (each child notifies
  // exactly once, even if empty). Completion is implicit - when we've received
  // one notification from each child, they're done.
  //
  // LAZY MODE: These operators cannot run in lazy mode without special handling.
  // May emit on timeout, threshold, or explicit flush.

  /** Aggregate results.
    *
    * Accumulates results until input is complete, then emits aggregated result.
    *
    * @param aggregations Aggregation functions (COUNT, SUM, etc.)
    * @param groupBy Columns to group by (empty = single group)
    * @param input Source of results to aggregate
    */
  case class Aggregate(
    aggregations: List[Aggregation],
    groupBy: List[Symbol],
    input: QueryPlan,
  ) extends QueryPlan {
    def children: Seq[QueryPlan] = Seq(input)
  }

  /** Sort results (eager mode only).
    *
    * Buffers all results until input complete, sorts, then emits.
    */
  case class Sort(
    orderBy: List[SortKey],
    input: QueryPlan,
  ) extends QueryPlan {
    def children: Seq[QueryPlan] = Seq(input)
  }

  /** Limit result count.
    *
    * Emits up to `count` results, then stops.
    */
  case class Limit(
    count: Long,
    input: QueryPlan,
  ) extends QueryPlan {
    def children: Seq[QueryPlan] = Seq(input)
  }
}

// ============================================================
// ANCHOR TARGET
// ============================================================

/** Specifies how to determine target node(s) for an Anchor operation */
sealed trait AnchorTarget

object AnchorTarget {

  /** Evaluate expression with current context to get node ID */
  case class Computed(expr: Expression) extends AnchorTarget

  /** All nodes in namespace (scan in eager mode, hook in lazy mode) */
  case object AllNodes extends AnchorTarget
}

// ============================================================
// CONTEXT FLOW
// ============================================================

/** How context flows between Sequence steps */
sealed trait ContextFlow

object ContextFlow {

  /** Output replaces first's bindings with andThen's result only.
    *
    * First's bindings are passed to andThen for evaluation, but the final
    * output contains only andThen's bindings.
    * Used when first's bindings are intermediate and not needed in output.
    */
  case object Replace extends ContextFlow

  /** Output extends first's bindings with andThen's result.
    *
    * Cross-products first's context with andThen's result, so final output
    * contains bindings from both.
    * Used when both first and andThen's bindings are needed in output.
    */
  case object Extend extends ContextFlow
}

// ============================================================
// PROPERTY CONSTRAINTS
// ============================================================

/** Constraint on a property value for LocalProperty */
sealed trait PropertyConstraint {

  /** Whether this constraint is satisfied by a missing property */
  def satisfiedByNone: Boolean

  /** Test if a value satisfies this constraint */
  def apply(value: Value): Boolean
}

object PropertyConstraint {

  /** Property must exist with any value */
  case object Any extends PropertyConstraint {
    val satisfiedByNone: Boolean = false
    def apply(value: Value): Boolean = true
  }

  /** Property must equal specific value */
  case class Equal(to: Value) extends PropertyConstraint {
    val satisfiedByNone: Boolean = false
    def apply(value: Value): Boolean = value == to
  }

  /** Property must not equal specific value */
  case class NotEqual(to: Value) extends PropertyConstraint {
    val satisfiedByNone: Boolean = false
    def apply(value: Value): Boolean = value != to
  }

  /** Property must match regex (strings only) */
  case class Regex(pattern: String) extends PropertyConstraint {
    private val compiled = pattern.r
    val satisfiedByNone: Boolean = false
    def apply(value: Value): Boolean = value match {
      case Value.Text(s) => compiled.matches(s)
      case _ => false
    }
  }

  /** Emit regardless of property presence */
  case object Unconditional extends PropertyConstraint {
    val satisfiedByNone: Boolean = true
    def apply(value: Value): Boolean = true
  }
}

// ============================================================
// LABEL CONSTRAINTS
// ============================================================

/** Constraint on node labels for LocalLabels */
sealed trait LabelConstraint {

  /** Test if a set of labels satisfies this constraint */
  def apply(labels: Set[Symbol]): Boolean
}

object LabelConstraint {

  /** Node must have all specified labels */
  case class Contains(mustContain: Set[Symbol]) extends LabelConstraint {
    def apply(labels: Set[Symbol]): Boolean = mustContain.subsetOf(labels)
  }

  /** Emit regardless of labels */
  case object Unconditional extends LabelConstraint {
    def apply(labels: Set[Symbol]): Boolean = true
  }
}

// ============================================================
// LOCAL QUERY EFFECTS
// ============================================================

/** Effects that run on the current node */
sealed trait LocalQueryEffect

object LocalQueryEffect {

  /** Create a new node with labels and optional properties, binding it to a symbol.
    *
    * This creates a fresh QuineId and sets up the node with the given labels/properties.
    * The node is bound to `binding` in the context for subsequent effects.
    *
    * @param binding Symbol to bind the new node to (can be used in subsequent effects)
    * @param labels Labels to set on the new node
    * @param properties Optional properties expression to set on the new node
    */
  case class CreateNode(
    binding: Symbol,
    labels: Set[Symbol],
    properties: Option[Expression] = None,
  ) extends LocalQueryEffect

  /** Set a property on a node.
    *
    * @param target Optional node binding to set the property on. If None, uses current node.
    * @param property The property name to set
    * @param value Expression for the property value
    */
  case class SetProperty(target: Option[Symbol], property: Symbol, value: Expression) extends LocalQueryEffect

  /** Set multiple properties on a node.
    *
    * @param target Optional node binding to set properties on. If None, uses current node.
    * @param properties Expression evaluating to a map of properties
    */
  case class SetProperties(target: Option[Symbol], properties: Expression) extends LocalQueryEffect

  /** Set labels on a node.
    *
    * @param target Optional node binding to set labels on. If None, uses current node.
    * @param labels Labels to set
    */
  case class SetLabels(target: Option[Symbol], labels: Set[Symbol]) extends LocalQueryEffect

  /** Create a half-edge from one node to another.
    *
    * @param source Node binding for the source of the edge (where the half-edge lives)
    * @param label Edge label
    * @param direction Outgoing = source is tail; Incoming = source is head
    * @param other The other node's ID expression (from context)
    */
  case class CreateHalfEdge(source: Option[Symbol], label: Symbol, direction: EdgeDirection, other: Expression)
      extends LocalQueryEffect

  /** Iterate over a list and apply nested effects for each item.
    *
    * @param binding The loop variable name
    * @param list Expression evaluating to a list
    * @param effects Effects to apply for each item (binding will be set in context)
    */
  case class Foreach(binding: Symbol, list: Expression, effects: List[LocalQueryEffect]) extends LocalQueryEffect
}

// ============================================================
// PROJECTION
// ============================================================

/** A single column projection */
case class Projection(expression: Expression, as: Symbol)

// ============================================================
// AGGREGATION
// ============================================================

/** Aggregation function specification */
sealed trait Aggregation

object Aggregation {
  case class Count(distinct: Boolean) extends Aggregation
  case class Sum(expr: Expression) extends Aggregation
  case class Avg(expr: Expression) extends Aggregation
  case class Min(expr: Expression) extends Aggregation
  case class Max(expr: Expression) extends Aggregation
  case class Collect(expr: Expression, distinct: Boolean) extends Aggregation
}

// ============================================================
// SORT KEY
// ============================================================

/** Specification for ordering results */
case class SortKey(expression: Expression, ascending: Boolean)

// ============================================================
// QUERY PART ID (for SubscribeToQueryPart)
// ============================================================

/** Unique identifier for a deployed query part */
case class QueryPartId(value: String)
