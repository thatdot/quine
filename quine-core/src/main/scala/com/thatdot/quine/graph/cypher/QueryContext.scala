package com.thatdot.quine.graph.cypher

import scala.collection.compat._

import com.thatdot.quine.model.QuineIdProvider

/** Container for query results
  *
  * Invariants:
  *
  *  1. `QueryContext.empty` has schema `Columns.empty`
  *
  *  2. Given `qc: QueryContext` with schema `c: Columns`, `q: (Symbol, Value)`
  *     with name `s: Symbol`, and `s` not in `c`, then `qc + q` has schema
  *     `q + s`
  *
  *  3. Given `qc1: QueryContext` with schema `c1: Columns`, `qc2: QueryContext`
  *     with schema `c2: Columns`, and `c1` and `c2` having distinct column
  *     names, then `qc1 ++ qc2` has schema `c1 ++ c2`.
  *
  * @param environment mapping of variable to value
  */
final case class QueryContext(
  environment: Map[Symbol, Value]
) extends AnyVal {

  def +(kv: (Symbol, Value)): QueryContext = QueryContext(environment + kv)
  def ++(other: QueryContext): QueryContext = QueryContext(environment ++ other.environment)
  def ++(other: Iterable[(Symbol, Value)]): QueryContext = QueryContext(environment ++ other)

  def apply(k: Symbol): Value = environment(k)
  def get(k: Symbol): Option[Value] = environment.get(k)
  def getOrElse(k: Symbol, v: => Value): Value = environment.getOrElse(k, v)

  /** Extract and re-order a subset of the context (eg. when importing into a subquery
    *
    * @param importedColumns columns to extract
    * @return subcontext containing only the specified imported columns
    */
  def subcontext(importedColumns: Seq[Symbol]): QueryContext =
    // TODO: if `QueryContext` was ordered, re-order it according to `importedColumns`
    QueryContext(
      environment.view
        .filterKeys(importedColumns.contains)
        .toMap
    )

  def pretty: String = environment
    .map { case (k, v) => s"${k.name}: ${v.pretty}" }
    .mkString("{ ", ", ", " }")

  def prettyMap: Map[String, String] = environment.map { case (k, v) => k.name -> v.pretty }.toMap
}
object QueryContext {
  val empty: QueryContext = QueryContext(Map.empty)

  /** Compare query contexts along an ordered list of criteria, each of which
    * can be be inverted (ie. descending instead of ascending)
    *
    * @param exprs ranked criteria along which to order query contexts
    * @return an ordering of query contexts
    */
  def orderingBy(
    exprs: Seq[(Expr, Boolean)]
  )(implicit idp: QuineIdProvider, p: Parameters): Ordering[QueryContext] =
    exprs.foldRight[Ordering[QueryContext]](Ordering.by(_ => ())) { case ((by, isAscending), tieBreaker) =>
      val evaluated = Ordering.by[QueryContext, Value](by.eval(_))(Value.ordering)
      val directed = if (isAscending) evaluated.reverse else evaluated

      // Use just `directed.orElse(tieBreaker)` when dropping support for 2.12
      Ordering.comparatorToOrdering(directed.thenComparing(tieBreaker))
    }
}

/** Return columns of queries */
sealed abstract class Columns {
  def +(variable: Symbol): Columns

  def ++(variables: Columns): Columns

  def rename(remapping: PartialFunction[Symbol, Symbol]): Columns
}
object Columns {
  val empty = Specified.empty

  case object Omitted extends Columns {
    def +(variable: Symbol) = Omitted
    def ++(variables: Columns) = Omitted
    def rename(remapping: PartialFunction[Symbol, Symbol]) = Omitted
  }

  final case class Specified(variables: Vector[Symbol]) extends Columns {
    def +(variable: Symbol) = {
      require(
        !variables.contains(variable),
        s"Variable $variable cannot be added to a context it is already in ($variables)"
      )
      Specified(variables :+ variable)
    }

    def ++(columns2: Columns): Columns = {
      val variables2 = columns2 match {
        case Specified(variables2) => variables2
        case Omitted => return Omitted
      }
      require(
        (variables.toSet & variables2.toSet).isEmpty,
        s"Variable context $variables and $variables2 cannot be added - they have elements in common"
      )
      Specified(variables ++ variables2)
    }

    def rename(remapping: PartialFunction[Symbol, Symbol]): Specified = Columns.Specified(
      variables.collect(remapping)
    )
  }
  object Specified {
    val empty: Specified = Specified(Vector.empty)
  }
}
