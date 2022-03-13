package com.thatdot.quine.graph.cypher

import java.util.regex.Pattern

import scala.collection.compat.immutable._
import scala.collection.mutable

import com.google.common.hash.Hashing.murmur3_128

import com.thatdot.quine.graph.StandingQueryPartId
import com.thatdot.quine.graph.messaging.StandingQueryMessage.ResultId
import com.thatdot.quine.model.{EdgeDirection, HalfEdge, QuineValue}
import com.thatdot.quine.util.Hashable

/** AST for a `MultipleValues` standing query */
sealed abstract class StandingQuery extends Product with Serializable {

  /** Type of associated standing query states */
  type State <: StandingQueryState

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
    * @note [[id]] must be injective (so `q1.id == q2.id` implies `q1 == q2` where equality is
    * structural, not by reference). In order to maximize sharing of standing query state, it is
    * also desirable that `q1 == q2 implies `q1.id == q2.id` whenever possible.
    */
  val id: StandingQueryPartId = StandingQueryPartId(StandingQuery.hashable.hashToUuid(murmur3_128(), this))

  /** Direct children of this query
    *
    * Doesn't include the receiver (`this`) or any further descendants.
    */
  def children: Seq[StandingQuery]

  /** Which columns do we expect this query to return at runtime? */
  def columns: Columns
}

object StandingQuery {

  /** Produces exactly one result, as soon as initialized, with no columns */
  final case class UnitSq() extends StandingQuery {

    type State = UnitState

    override def columns: Columns = Columns.Omitted

    override def createState(): UnitState =
      UnitState(
        queryPartId = id,
        resultId = None
      )

    val children: Seq[StandingQuery] = Seq.empty
  }

  /** Produces a cross-product of queries, with columns that are the concatenation of the columns of
    * sub-queries in the product
    *
    * @param queries (non-empty) sub queries to cross together (if empty use [[UnitState]] instead)
    * @param emitSubscriptionsLazily emit subscriptions to subqueries lazily (left to right) only
    *                                once the previous query has _some_ results
    */
  final case class Cross(
    queries: ArraySeq[StandingQuery],
    emitSubscriptionsLazily: Boolean,
    columns: Columns = Columns.Omitted
  ) extends StandingQuery {

    type State = CrossState

    override def createState(): CrossState =
      CrossState(
        queryPartId = id,
        subscriptionsEmitted = 0,
        accumulatedResults = ArraySeq.unsafeWrapArray( // Plan: use `ArraySeq.fill` when 2.12 support is dropped
          Array.fill(queries.length)(mutable.Map.empty[ResultId, QueryContext])
        ),
        resultDependency = mutable.Map.empty
      )

    def children: Seq[StandingQuery] = queries
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
    case object Any extends ValueConstraint {
      val satisfiedByNone = false
      override def apply(value: Value): Boolean = true
    }
    case object None extends ValueConstraint {
      val satisfiedByNone = true
      override def apply(value: Value): Boolean = false
    }
    final case class Regex(pattern: String) extends ValueConstraint {
      val compiled: Pattern = Pattern.compile(pattern)
      val satisfiedByNone = false

      // The intention is that this matches the semantics of Cypher's `=~` with a constant regex
      override def apply(value: Value): Boolean = value match {
        case Expr.Str(testStr) => compiled.matcher(testStr).matches
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
  ) extends StandingQuery {

    type State = LocalPropertyState

    override def createState(): LocalPropertyState =
      LocalPropertyState(
        queryPartId = id,
        currentResult = None
      )

    val children: Seq[StandingQuery] = Seq.empty
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
  ) extends StandingQuery {

    type State = LocalIdState

    override def createState(): LocalIdState =
      LocalIdState(
        queryPartId = id,
        resultId = None
      )

    val children: Seq[StandingQuery] = Seq.empty
  }

  /** Watch for an edge pattern and match however many edges fit that pattern.
    *
    * @param edgeName if populated, the edges matched must have this label
    * @param edgeDirection if populated, the edges matched must have this direction
    * @param andThen once an edge matched, this subquery must match on the other side
    */
  final case class SubscribeAcrossEdge(
    edgeName: Option[Symbol],
    edgeDirection: Option[EdgeDirection],
    andThen: StandingQuery,
    columns: Columns = Columns.Omitted
  ) extends StandingQuery {

    type State = SubscribeAcrossEdgeState

    override def createState(): SubscribeAcrossEdgeState =
      SubscribeAcrossEdgeState(
        queryPartId = id,
        edgesWatched = mutable.Map.empty
      )

    val children: Seq[StandingQuery] = Seq(andThen)
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
    andThenId: StandingQueryPartId,
    columns: Columns = Columns.Omitted
  ) extends StandingQuery {

    type State = EdgeSubscriptionReciprocalState

    override def createState(): EdgeSubscriptionReciprocalState =
      EdgeSubscriptionReciprocalState(
        queryPartId = id,
        halfEdge = halfEdge,
        currentlyMatching = false,
        reverseResultDependency = mutable.Map.empty,
        andThenId = andThenId
      )

    // NB andThenId is technically the child of the [[SubscribeAcrossEdge]] query, NOT the reciprocal
    val children: Seq[StandingQuery] = Seq.empty
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
    toFilter: StandingQuery,
    dropExisting: Boolean,
    toAdd: List[(Symbol, Expr)],
    columns: Columns = Columns.Omitted
  ) extends StandingQuery {

    type State = FilterMapState

    override def createState(): FilterMapState =
      FilterMapState(
        queryPartId = id,
        keptResults = mutable.Map.empty
      )

    val children: Seq[StandingQuery] = Seq(toFilter)
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
  def indexableSubqueries(sq: StandingQuery, acc: Set[StandingQuery] = Set.empty): Set[StandingQuery] =
    // EdgeSubscriptionReciprocal are not useful to index -- they're ephemeral, fully owned/created/used by 1 node
    if (sq.isInstanceOf[EdgeSubscriptionReciprocal]) acc
    // Since subqueries can be duplicated, try not to traverse already-traversed subqueries
    else if (acc.contains(sq)) acc
    // otherwise, traverse
    else sq.children.foldLeft(acc + sq)((acc, child) => indexableSubqueries(child, acc))

  val hashable: Hashable[StandingQuery] = implicitly[Hashable[StandingQuery]]
}
