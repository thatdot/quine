package com.thatdot.quine.graph.cypher

import java.util.regex.{Pattern, PatternSyntaxException}

import scala.collection.immutable.ArraySeq

import com.google.common.hash.Hashing.murmur3_128

import com.thatdot.quine.graph.MultipleValuesStandingQueryPartId
import com.thatdot.quine.model.{EdgeDirection, HalfEdge, QuineValue}
import com.thatdot.quine.util.Hashable

/** AST for a `MultipleValues` standing query */
sealed abstract class MultipleValuesStandingQuery extends Product with Serializable {

  /** Type of associated standing query states */
  type State <: MultipleValuesStandingQueryState

  /** Create a new associated standing query state
    *
    * Each created state is an independent object capable of tracking on some fixed node the
    * progress of the standing query as it attempts to match on the node. Standing query states
    * tend to be mutable since they do the book-keeping around which components of the standing
    * query have matched or not matched yet.
    */
  def createState(): State

  /** An unique identifier for this sub-query
    *
    * @note [[queryPartId]] must be injective (so `q1.queryPartId == q2.queryPartId` implies `q1 == q2` where equality
    *       is structural, not by reference). In order to maximize sharing of standing query state, it is
    *       also desirable that `q1 == q2 implies `q1.queryPartId == q2.queryPartId` whenever possible.
    */
  final val queryPartId: MultipleValuesStandingQueryPartId = MultipleValuesStandingQueryPartId(
    MultipleValuesStandingQuery.hashable.hashToUuid(murmur3_128, this)
  )

  /** Direct children of this query
    *
    * Doesn't include the receiver (`this`) or any further descendants.
    */
  def children: Seq[MultipleValuesStandingQuery]

  /** Which columns do we expect this query to return at runtime? */
  def columns: Columns
}

object MultipleValuesStandingQuery {

  /* NOTE: UnitSq currently must be a case class and not an object (despite having no parameters)
     This is because `id` is defined concretely as a val in the superclass in terms of the shapeless
     generically-defined Hashable instance, and due to the way Generic's macro does pattern-matching
     (to see which case of this sealed abstract class you passed it), the object must exist at that point when
     matching. I'm not sure how it works to define a val (the hashcode) in the superclass terms of fields that
     don't exist yet (aren't in scope) at that point. A less cursed arrangement might be to either:
     A) if using "externally defined" (i.e. the generically-derived Hashable) things, use them externally.
        I.e. call someHashableInstance.hash(foo) instead of trying to inline that into the superclass constructor
        as foo.id
     B) Leave id abstract in the superclass, and take advantage of normal OO inheritance / polymorphism /
        dynamic dispatch and have the actual impl be in the subclasses. That way you don't have values in the
        superclass that need to be implemented by pattern-matching on the `this` reference and use values that
        aren't initialized yet.
   */

  /** Produces exactly one result, as soon as initialized, with no columns */
  final case class UnitSq private () extends MultipleValuesStandingQuery {

    type State = UnitState

    override def columns: Columns = Columns.Omitted

    override def createState(): UnitState = UnitState()

    val children: Seq[MultipleValuesStandingQuery] = Seq.empty
  }
  object UnitSq {
    val instance = new UnitSq()
  }

  /** Produces a cross-product of queries, with columns that are the concatenation of the columns of
    * sub-queries in the product
    *
    * @param queries                 (non-empty) sub queries to cross together (if empty use [[UnitState]] instead)
    * @param emitSubscriptionsLazily emit subscriptions to subqueries lazily (left to right) only
    *                                once the previous query has _some_ results
    */
  final case class Cross(
    queries: ArraySeq[MultipleValuesStandingQuery],
    emitSubscriptionsLazily: Boolean,
    columns: Columns = Columns.Omitted
  ) extends MultipleValuesStandingQuery {

    type State = CrossState

    override def createState(): CrossState = CrossState(queryPartId)

    def children: Seq[MultipleValuesStandingQuery] = queries
  }

  /*
  case class Apply(
    doThis: StandingQuery,
    composedWithThis: StandingQuery,
    columns: Columns = Columns.Omitted
  ) extends StandingQuery
   */

  object LocalProperty {
    sealed abstract class ValueConstraint {

      /** Whether this constraint is satisfied by the absence of a property
        */
      def satisfiedByNone: Boolean

      /** check this constraint against the provided value
        */
      def apply(value: Value): Boolean

      /** check this constraint against the provided value
        */
      final def apply(value: QuineValue): Boolean = apply(Expr.fromQuineValue(value))
    }
    final case class Equal(equalTo: Value) extends ValueConstraint {
      val satisfiedByNone = false

      override def apply(value: Value): Boolean = equalTo == value
    }
    final case class NotEqual(notEqualTo: Value) extends ValueConstraint {
      // eg consider the query `create (m:Test) WITH m match (n:Test) where n.name <> "Foo" return n` -- this should return no rows
      val satisfiedByNone = false
      override def apply(value: Value): Boolean = notEqualTo != value
    }

    /** Emits for a property key, regardless of what that key's value is (or if that key is unset)
      * Emits any time either `Any` emits, or `None` emits
      */
    case object Unconditional extends ValueConstraint {
      val satisfiedByNone = true
      override def apply(value: Value): Boolean = true
    }
    case object Any extends ValueConstraint {
      val satisfiedByNone = false
      override def apply(value: Value): Boolean = true
    }
    case object None extends ValueConstraint {
      val satisfiedByNone = true
      override def apply(value: Value): Boolean = false
    }
    final case class Regex(pattern: String) extends ValueConstraint {
      val compiled: Pattern =
        try Pattern.compile(pattern)
        catch {
          case e: PatternSyntaxException => throw CypherException.Compile(e.getMessage(), scala.None)
        }
      val satisfiedByNone = false

      // The intention is that this matches the semantics of Cypher's `=~` with a constant regex
      override def apply(value: Value): Boolean = value match {
        case Expr.Str(testStr) =>
          try compiled.matcher(testStr).matches
          catch {
            //This shouldn't happen because compiled should already be compiled
            case e: PatternSyntaxException => throw CypherException.ConstraintViolation(e.getMessage(), scala.None)
          }
        case _ => false
      }
    }
    final case class ListContains(mustContain: Set[Value]) extends ValueConstraint {
      val satisfiedByNone = false

      override def apply(value: Value): Boolean = value match {
        case Expr.List(values) => mustContain.subsetOf(values.toSet)
        case _ => false
      }
    }
  }

  /** Watches for changes to the projection of node properties as a map
    *
    * INV: a result emitted by this state on a node `n` has the same value as the result of executing `properties(n)`
    *      on `n`'s CypherBehavior at the same time
    * @param aliasedAs
    * @param columns
    */
  final case class AllProperties(
    aliasedAs: Symbol,
    columns: Columns = Columns.Omitted
  ) extends MultipleValuesStandingQuery {
    type State = AllPropertiesState
    def createState(): AllPropertiesState = AllPropertiesState(queryPartId)
    def children: Seq[MultipleValuesStandingQuery] = Seq.empty
  }

  /** Watches for a certain local property to be set and returns a result if/when that happens
    *
    * @param propKey key of the local property to watch
    * @param propConstraint additional constraints to enforce on the local property
    * @param aliasedAs if the property should be extracted, under what name is it stored?
    */
  final case class LocalProperty(
    propKey: Symbol,
    propConstraint: LocalProperty.ValueConstraint,
    aliasedAs: Option[Symbol],
    columns: Columns = Columns.Omitted
  ) extends MultipleValuesStandingQuery {

    type State = LocalPropertyState

    override def createState(): LocalPropertyState = LocalPropertyState(queryPartId)

    val children: Seq[MultipleValuesStandingQuery] = Seq.empty
  }

  /** Produces exactly one result, as soon as initialized, with one column: the node ID
    *
    * @param aliasedAs under what name should the result go
    * @param formatAsString if `true`, return `strId(n)` otherwise `id(n)`
    */
  final case class LocalId(
    aliasedAs: Symbol,
    formatAsString: Boolean,
    columns: Columns = Columns.Omitted
  ) extends MultipleValuesStandingQuery {

    type State = LocalIdState

    override def createState(): LocalIdState = LocalIdState(queryPartId)

    val children: Seq[MultipleValuesStandingQuery] = Seq.empty
  }

  /** Watch for an edge pattern and match however many edges fit that pattern.
    *
    * @param edgeName if populated, the edges matched must have this label
    * @param edgeDirection if populated, the edges matched must have this direction
    * @param andThen once an edge matched, this subquery must match on the other side.
    *                note that this query will not actually directly subscribe to [[andThen]],
    *                but rather to an ephemeral, reciprocal state (to account for the split
    *                nature of half-edges). That reciprocal state is what actually subscribes
    *                to [[andThen]]
    */
  final case class SubscribeAcrossEdge(
    edgeName: Option[Symbol],
    edgeDirection: Option[EdgeDirection],
    andThen: MultipleValuesStandingQuery,
    columns: Columns = Columns.Omitted
  ) extends MultipleValuesStandingQuery {

    type State = SubscribeAcrossEdgeState

    override def createState(): SubscribeAcrossEdgeState = SubscribeAcrossEdgeState(queryPartId)

    val children: Seq[MultipleValuesStandingQuery] = Seq(andThen)
  }

  /** Watch for an edge reciprocal and relay the recursive standing query only if the reciprocal
    * half edge is present.
    *
    * @note do not generate SQ's with this AST node - it is used internally in the interpreter
    * @param halfEdge the edge that must be on this node for it to match
    * @param andThenId ID of the standing query to execute if the half edge is present
    */
  final case class EdgeSubscriptionReciprocal(
    halfEdge: HalfEdge,
    andThenId: MultipleValuesStandingQueryPartId,
    columns: Columns = Columns.Omitted
  ) extends MultipleValuesStandingQuery {

    type State = EdgeSubscriptionReciprocalState

    override def createState(): EdgeSubscriptionReciprocalState =
      EdgeSubscriptionReciprocalState(queryPartId, halfEdge, andThenId)

    // NB andThenId is technically the child of the [[SubscribeAcrossEdge]] query, NOT the reciprocal
    val children: Seq[MultipleValuesStandingQuery] = Seq.empty
  }

  /** Filter and map over results of another query
    *
    * @param condition if present, this must condition is a filter (uses columns from `subQuery`)
    * @param toFilter subquery whose results are being filtered and mapped
    * @param dropExisting should existing columns from the subquery be truncated?
    * @param toAdd which new columns should be added
    */
  final case class FilterMap(
    condition: Option[Expr],
    toFilter: MultipleValuesStandingQuery,
    dropExisting: Boolean,
    toAdd: List[(Symbol, Expr)],
    columns: Columns = Columns.Omitted
  ) extends MultipleValuesStandingQuery {

    type State = FilterMapState

    override def createState(): FilterMapState = FilterMapState(queryPartId)

    val children: Seq[MultipleValuesStandingQuery] = Seq(toFilter)
  }

  /** Enumerate all globally indexable subqueries in a standing query
    *
    * The only subqueries that are excluded are `EdgeSubscriptionReciprocal`,
    * since those get synthetically introduced to watch for edge reciprocals.
    *
    * @param sq query whose subqueries are being extracted
    * @param acc accumulator of subqueries
    * @return set of globally indexable subqueries
    */
  def indexableSubqueries(
    sq: MultipleValuesStandingQuery,
    acc: Set[MultipleValuesStandingQuery] = Set.empty
  ): Set[MultipleValuesStandingQuery] =
    // EdgeSubscriptionReciprocal are not useful to index -- they're ephemeral, fully owned/created/used by 1 node
    if (sq.isInstanceOf[EdgeSubscriptionReciprocal]) acc
    // Since subqueries can be duplicated, try not to traverse already-traversed subqueries
    else if (acc.contains(sq)) acc
    // otherwise, traverse
    else sq.children.foldLeft(acc + sq)((acc, child) => indexableSubqueries(child, acc))

  val hashable: Hashable[MultipleValuesStandingQuery] = Hashable[MultipleValuesStandingQuery]
}
