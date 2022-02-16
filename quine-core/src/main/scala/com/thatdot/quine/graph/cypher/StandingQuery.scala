package com.thatdot.quine.graph.cypher

import java.util.regex.Pattern

import scala.collection.compat.immutable._
import scala.collection.mutable

import com.google.common.hash.Hashing.murmur3_128

import com.thatdot.quine.graph.StandingQueryPartId
import com.thatdot.quine.graph.messaging.StandingQueryMessage.ResultId
import com.thatdot.quine.model.{EdgeDirection, HalfEdge, QuineValue}
import com.thatdot.quine.util.Hashable

sealed abstract class StandingQuery extends Product with Serializable {
  def columns: Columns

  /** Type of associated standing query states */
  type State <: StandingQueryState

  /** An unique identifier for this sub-query
    *
    * @note [[id]] must be injective (so `q1.id == q2.id` implies `q1 == q2` where equality is
    * structural, not by reference). In order to maximize sharing of standing query state, it is
    * also desirable that `q1 == q2 implies `q1.id == q2.id` whenever possible.
    */
  val id: StandingQueryPartId = StandingQueryPartId(StandingQuery.hashable.hashToUuid(murmur3_128(), this))

  def createState(): State

  // Direct children of this query -- ie, not including the receiver, and not including any further descendants
  def children: Seq[StandingQuery]
}

object StandingQuery {
  // enumerate all subqueries including the provided query, excluding those which should not be indexed globally
  def indexableSubqueries(sq: StandingQuery, acc: Set[StandingQuery] = Set.empty): Set[StandingQuery] =
    // EdgeSubscriptionReciprocal are not useful to index -- they're ephemeral, fully owned/created/used by 1 node
    if (sq.isInstanceOf[EdgeSubscriptionReciprocal]) acc
    // Since subqueries can be duplicated, try not to traverse already-traversed subqueries
    else if (acc.contains(sq)) acc
    // otherwise, traverse
    else sq.children.foldLeft(acc + sq)((acc, child) => indexableSubqueries(child, acc))

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

  /** Watches for a certain local property to be set and returns a result if/when that happens */
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

  // Do not generate SQ's with this AST node - it is used internally in the interpreter
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

  /** Filter and map over results of another query */
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

  val hashable: Hashable[StandingQuery] = implicitly[Hashable[StandingQuery]]
}
