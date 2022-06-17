package com.thatdot.quine.graph.cypher

import com.thatdot.quine.graph.cypher.EntryPoint.{AllNodesScan, NodeById}
import com.thatdot.quine.model.{EdgeDirection, QuineId}

/** Subset of Cypher queries which rely on indices/persistors (i.e. are
  * non-local). These are also the query types that ignore their inputs
  */
sealed abstract class EntryPoint
object EntryPoint {

  /** Scan every node */
  case object AllNodesScan extends EntryPoint

  /** Scan nodes by ID
    *
    * @param ids nodes with these IDs will be returned
    */
  final case class NodeById(
    ids: Vector[QuineId]
  ) extends EntryPoint

  /* TODO: consider adding this back if/when we add support for scans on labels
  final case class NodeByLabel(
    label: Symbol
  ) extends EntryPoint
   */

  /* TODO: consider adding this back if/when we add support for indices
  final case class NodeIndex(
    label: Option[Symbol],
    keyValues: Map[Symbol, Option[QuineValue]]
  ) extends EntryPoint
   */
}

/** Represents a location from which a query (or sub-query) can be executed */
sealed abstract class Location
object Location {

  /** For queries that can be executed from inside or outside the graph */
  sealed trait Anywhere extends Location

  /** For queries that can only be executed from inside the graph */
  type OnNode = Location
}

/** A cypher query which can be executed starting at a location and ending at a
  * another location.
  *
  * It is important to track the starting location because not all queries can
  * be executed anywhere. Example: you can't expand along an edge if you are not
  * on a node.
  *
  * The fundamental motivation behind the [[Query]] AST is that any query should
  * have as fields all the information needed to start executing the query.
  * Furthermore, any subquery that needs to be passed along (to be executed
  * elsewhere) should also be a field.
  *
  * @tparam Start location from which the query can be executed
  */
sealed abstract class Query[+Start <: Location] extends Product with Serializable {

  /** Output columns this query should produce */
  def columns: Columns

  /** Is this query read-only?
    *
    * @note if this is `false` it does not mean the query definitely writes
    */
  def isReadOnly: Boolean

  /** Is the query idempotent? An idempotent query will produce the same
    * graph state when applied more than once to a graph. An idempotent
    * query is allowed to change graph state, however there is no cumulative
    * effect of additional evaluations.
    *
    * {{{
    * apply(GraphState_1, Query) => GraphState_2
    * apply(GraphState_2, Query) => GraphState_2
    * }}}
    *
    * An idempotent query must obey the above description only in the idealized context
    * of no interleaving queries. In other words, the graph state produced by the first
    * application of the query is the input graph state to the second application of the query.
    *
    * Implementation notes:
    * - A query is idempotent only if all its subqueries, procedures, and aggregates are idempotent
    * - A query is idempotent only if all its expressions and user defined functions are pure
    */
  def isIdempotent: Boolean

  /** Barring unbound variable or parameter exceptions, is it impossible for
    * the expression to throw exceptions when evaluated?
    */
  def cannotFail: Boolean

  /** Is it possibly for this query to touch node-local state?
    *
    * This is used for determining when it is OK to skip some thread-safety
    * protections. When in doubt, err on the side of `true`. Put another way:
    * setting this to false means that even if the query is running on a node,
    * it is OK for the query to execute off the node actor thread.
    *
    * @note this does not include indirect effects due to subqueries
    */
  def canDirectlyTouchNode: Boolean

  /** Can the query contain a full node scan?
    * Note: if this is true it does not mean the query definitely does cause a full node scan
    */
  def canContainAllNodeScan: Boolean

  /** substitute all parameters in this query and all descendants
    * @param parameters a [[Parameters]] providing parameters used by [[Expr.Parameter]]s in this query.
    * @return a copy of this query all provided parameters substituted
    * INV: If all parameters used by [[Expr.Parameter]] AST nodes are provided, the returned
    * query will have no [[Expr.Parameter]] AST nodes remaining in the tree
    */
  def substitute(parameters: Map[Expr.Parameter, Value]): Query[Start]

  /** Queries that might have to be executed in order to start execution of this query -- ie, "children" of this query.
    * These are usually called "andThen" or similar. The children should be ordered by their execution order.
    * For example, {{query1} UNION {query2}}.children == Seq({query1}, {query2})
    */
  def children: Seq[Query[_]]
}

object Query {

  /** Like [[Unit]], but plays better with type inference */
  val unit: Query[Location.Anywhere] = Unit()

  /** Like [[Apply]], but applies some peephole optimizations */
  def apply[Start <: Location](
    startWithThis: Query[Start],
    thenCrossWithThis: Query[Start]
  ): Query[Start] = startWithThis match {
    // Apply(Unit, q) ==> q
    case Unit(_) => thenCrossWithThis

    // Apply(Empty, q) ==> Empty
    case Empty(_) => Empty()

    // Apply(Unwind(list, q1), q2) ==> Unwind(list, Apply(q1, q2))
    case Unwind(list, v, q, _) => Unwind(list, v, apply(q, thenCrossWithThis))

    case _ =>
      thenCrossWithThis match {
        // Apply(q,  Unit) ==> q
        case Unit(_) => startWithThis

        case _ => Apply(startWithThis, thenCrossWithThis)
      }
  }

  /** Like [[AdjustContext]], but applies some peephole optimizations */
  def adjustContext[Start <: Location](
    dropExisting: Boolean,
    toAdd: Vector[(Symbol, Expr)],
    adjustThis: Query[Start]
  ): Query[Start] = adjustThis match {
    // Nested AdjustContext
    case AdjustContext(dropExisting2, toAdd2, inner, _) if toAdd == toAdd2 =>
      val newDrop = dropExisting || dropExisting2
      AdjustContext(newDrop, toAdd, inner)

    case _ =>
      val toAdd2 = toAdd.filter {
        case (sym, Expr.Variable(sym2)) if !dropExisting => sym != sym2
        case _ => true
      }
      if (toAdd2.isEmpty && !dropExisting)
        adjustThis // Nothing changes!
      else
        AdjustContext(dropExisting, toAdd2, adjustThis)
  }

  /** Like [[Filter]], but applies from peephole optimizations */
  def filter[Start <: Location](
    condition: Expr,
    toFilter: Query[Start]
  ): Query[Start] = condition match {
    case Expr.True => toFilter
    case Expr.And(Vector()) => toFilter
    case Expr.And(Vector(cond)) => filter(cond, toFilter)
    case _ => Filter(condition, toFilter)
  }

  /** An empty query - always returns no results */
  final case class Empty(
    columns: Columns = Columns.Omitted
  ) extends Query[Location.Anywhere] {
    def isReadOnly: Boolean = true
    def cannotFail: Boolean = true
    def canDirectlyTouchNode: Boolean = false
    def isIdempotent: Boolean = true
    def canContainAllNodeScan: Boolean = false
    def substitute(parameters: Map[Expr.Parameter, Value]): Empty = this
    def children: Seq[Query[_]] = Seq.empty
  }

  /** A unit query - returns exactly the input */
  final case class Unit(
    columns: Columns = Columns.Omitted
  ) extends Query[Location.Anywhere] {
    def isReadOnly: Boolean = true
    def cannotFail: Boolean = true
    def canDirectlyTouchNode: Boolean = false
    def isIdempotent: Boolean = true
    def canContainAllNodeScan: Boolean = false
    def substitute(parameters: Map[Expr.Parameter, Value]): Unit = this
    def children: Seq[Query[_]] = Seq.empty
  }

  /** A solid starting point for a query - usually some sort of index scan
    *
    * @param entry information for how to scan/lookup starting nodes
    * @param andThen once those nodes, what to do
    */
  final case class AnchoredEntry(
    entry: EntryPoint,
    andThen: Query[Location.OnNode],
    columns: Columns = Columns.Omitted
  ) extends Query[Location.Anywhere] {
    def isReadOnly: Boolean = andThen.isReadOnly
    def cannotFail: Boolean = andThen.cannotFail
    def canDirectlyTouchNode: Boolean = false
    def isIdempotent: Boolean = andThen.isIdempotent
    def canContainAllNodeScan: Boolean = entry match {
      case AllNodesScan => true
      case NodeById(_) => andThen.canContainAllNodeScan
    }
    def substitute(parameters: Map[Expr.Parameter, Value]): AnchoredEntry =
      copy(andThen = andThen.substitute(parameters))
    def children: Seq[Query[_]] = Seq(andThen)
  }

  /** A starting point from a node. This _can_ be an entry point from outside
    * the graph.
    *
    * @param node expression evaluating to a node
    * @param andThen once on that node, what to do
    */
  final case class ArgumentEntry(
    node: Expr,
    andThen: Query[Location.OnNode],
    columns: Columns = Columns.Omitted
  ) extends Query[Location.Anywhere] {
    def isReadOnly: Boolean = andThen.isReadOnly
    def cannotFail: Boolean = node.cannotFail && andThen.cannotFail
    def canDirectlyTouchNode: Boolean = false
    def isIdempotent: Boolean = node.isPure && andThen.isIdempotent
    def canContainAllNodeScan: Boolean = andThen.canContainAllNodeScan
    def substitute(parameters: Map[Expr.Parameter, Value]): ArgumentEntry =
      copy(node = node.substitute(parameters), andThen = andThen.substitute(parameters))
    def children: Seq[Query[_]] = Seq(andThen)
  }

  /** Get the degree of a node
    *
    * @param edgeName name constraint on which edges are counted
    * @param direction direction constraint on which edges are counted
    * @param bindName name under which to add the degree to context
    */
  final case class GetDegree(
    edgeName: Option[Symbol],
    direction: EdgeDirection,
    bindName: Symbol,
    columns: Columns = Columns.Omitted
  ) extends Query[Location.OnNode] {
    def isReadOnly: Boolean = true
    def cannotFail: Boolean = true
    def canDirectlyTouchNode: Boolean = true
    def isIdempotent: Boolean = true
    def canContainAllNodeScan: Boolean = false
    def substitute(parameters: Map[Expr.Parameter, Value]): GetDegree = this
    def children: Seq[Query[_]] = Seq.empty
  }

  /** Hop across all matching edges going from one node to another
    *
    * @param edgeName permitted edge names ([[scala.None]] means all edge names work)
    * @param toNode node to which the edge is required to go
    * @param direction direction of the edge
    * @param range Defines optional lower and upper inclusive bounds for variable length edge traversal
    * @param visited Set of nodes already visited within Expands issued recursively in service of a range
    * @param bindRelation name under which to add the edge to the context
    * @param andThen once on the other node, what to do
    */
  final case class Expand(
    edgeName: Option[Seq[Symbol]],
    toNode: Option[Expr],
    direction: EdgeDirection,
    bindRelation: Option[Symbol],
    range: Option[(Option[Long], Option[Long])] = None,
    visited: VisitedVariableEdgeMatches = VisitedVariableEdgeMatches.empty,
    andThen: Query[Location.OnNode],
    columns: Columns = Columns.Omitted
  ) extends Query[Location.OnNode] {
    def isReadOnly: Boolean = andThen.isReadOnly
    def cannotFail: Boolean = toNode.isEmpty && range.isEmpty && andThen.cannotFail
    def canDirectlyTouchNode: Boolean = true
    def isIdempotent: Boolean = toNode.forall(_.isPure)
    def canContainAllNodeScan: Boolean = andThen.canContainAllNodeScan
    def substitute(parameters: Map[Expr.Parameter, Value]): Expand =
      copy(toNode = toNode.map(_.substitute(parameters)), andThen = andThen.substitute(parameters))
    def children: Seq[Query[_]] = Seq(andThen)
  }

  /** Check that a node has certain labels and properties, and add the node to
    * the context if it does
    *
    * TODO: we should be able to statically get to `propertiesOpt: Map[String, Expr]`
    *
    * @param labelsOpt labels that should be on the node
    * @param propertiesOpt map of properties that should be on the node
    * @param bindName name under which to add the node to context
    */
  final case class LocalNode(
    labelsOpt: Option[Seq[Symbol]],
    propertiesOpt: Option[Expr],
    bindName: Option[Symbol],
    columns: Columns = Columns.Omitted
  ) extends Query[Location.OnNode] {
    def isReadOnly: Boolean = true
    def cannotFail: Boolean = propertiesOpt.isEmpty
    def canDirectlyTouchNode: Boolean = true
    def isIdempotent: Boolean = propertiesOpt.forall(_.isPure)
    def canContainAllNodeScan: Boolean = false
    def substitute(parameters: Map[Expr.Parameter, Value]): LocalNode =
      copy(propertiesOpt = propertiesOpt.map(_.substitute(parameters)))
    def children: Seq[Query[_]] = Seq.empty
  }

  /** Walk through the records in a external CSV
    *
    * @param withHeaders if defined, maps (with keys being the header values)
    *        will be added to the context instead of list
    * @param urlString path at  which the CSV file can be found
    * @param variable name under which the record will be added to the context
    * @param fieldTerminator field delimiters
    */
  final case class LoadCSV(
    withHeaders: Boolean,
    urlString: Expr,
    variable: Symbol,
    fieldTerminator: Char = ',',
    columns: Columns = Columns.Omitted
  ) extends Query[Location.Anywhere] {
    def isReadOnly: Boolean = true
    def cannotFail: Boolean = false // URL might lead nowhere
    def canDirectlyTouchNode: Boolean = false
    def isIdempotent: Boolean = urlString.isPure
    def canContainAllNodeScan: Boolean = false
    def substitute(parameters: Map[Expr.Parameter, Value]): LoadCSV =
      copy(urlString = urlString.substitute(parameters))
    def children: Seq[Query[_]] = Seq.empty
  }

  /** Execute both queries one after another and concatenate the results
    *
    * @param unionLhs first query to run
    * @param unionRhs second query to run
    */
  final case class Union[+Start <: Location](
    unionLhs: Query[Start],
    unionRhs: Query[Start],
    columns: Columns = Columns.Omitted
  ) extends Query[Start] {
    def isReadOnly: Boolean = unionLhs.isReadOnly && unionRhs.isReadOnly
    def cannotFail: Boolean = unionLhs.cannotFail && unionRhs.cannotFail
    def canDirectlyTouchNode: Boolean = false
    def isIdempotent: Boolean = unionLhs.isIdempotent && unionRhs.isIdempotent
    def canContainAllNodeScan: Boolean = unionLhs.canContainAllNodeScan || unionRhs.canContainAllNodeScan
    def substitute(parameters: Map[Expr.Parameter, Value]): Union[Start] =
      copy(unionLhs = unionLhs.substitute(parameters), unionRhs = unionRhs.substitute(parameters))
    def children: Seq[Query[_]] = Seq(unionLhs, unionRhs)
  }

  /** Execute the first query then, if it didn't return any results, execute the
    * second (ie. second query is only run if the first query returns nothing)
    *
    * @param tryFirst first query to run
    * @param trySecond fallback query if first query didn't return anything
    */
  final case class Or[+Start <: Location](
    tryFirst: Query[Start],
    trySecond: Query[Start],
    columns: Columns = Columns.Omitted
  ) extends Query[Start] {
    def isReadOnly: Boolean = tryFirst.isReadOnly && trySecond.isReadOnly
    def cannotFail: Boolean = tryFirst.cannotFail && trySecond.cannotFail
    def canDirectlyTouchNode: Boolean = false
    def isIdempotent: Boolean = tryFirst.isIdempotent && trySecond.isIdempotent
    def canContainAllNodeScan: Boolean = tryFirst.canContainAllNodeScan || trySecond.canContainAllNodeScan
    def substitute(parameters: Map[Expr.Parameter, Value]): Or[Start] =
      copy(tryFirst = tryFirst.substitute(parameters), trySecond = trySecond.substitute(parameters))
    def children: Seq[Query[_]] = Seq(tryFirst, trySecond)
  }

  /** Execute two queries and join pairs of results which had matching values
    * for the join properties
    *
    * Logically this may look symmetric, but operationally it cannot be. In
    * order to let go of a result from one side, we need to know that we've
    * already seen all results from the other side with that property.
    *
    * This suggest the following implementation: eagerly pull all results from
    * one side, building a multimap of results keyed by the join property. After
    * that, values from the other side can be streamed lazily.
    *
    * @param joinLhs one side of the join query
    * @param joinRhs other side of the join query
    * @param lhsProperty join value for LHS query
    * @param rhsProperty join value for RHS query
    */
  final case class ValueHashJoin[+Start <: Location](
    joinLhs: Query[Start],
    joinRhs: Query[Start],
    lhsProperty: Expr,
    rhsProperty: Expr,
    columns: Columns = Columns.Omitted
  ) extends Query[Start] {
    def isReadOnly: Boolean = joinLhs.isReadOnly && joinRhs.isReadOnly
    def cannotFail: Boolean =
      lhsProperty.cannotFail && rhsProperty.cannotFail && joinLhs.cannotFail && joinRhs.cannotFail
    def canDirectlyTouchNode: Boolean = false
    def isIdempotent: Boolean = joinLhs.isIdempotent && joinRhs.isIdempotent &&
      lhsProperty.isPure && rhsProperty.isPure
    def canContainAllNodeScan: Boolean = joinLhs.canContainAllNodeScan || joinRhs.canContainAllNodeScan
    def substitute(parameters: Map[Expr.Parameter, Value]): ValueHashJoin[Start] = copy(
      joinLhs = joinLhs.substitute(parameters),
      joinRhs = joinRhs.substitute(parameters),
      lhsProperty = lhsProperty.substitute(parameters),
      rhsProperty = rhsProperty.substitute(parameters)
    )
    def children: Seq[Query[_]] = Seq(joinLhs, joinRhs)
  }

  /** Filter input stream keeping only entries which produce something when run
    * against some other query
    *
    * @param acceptIfThisSucceeds test query
    * @param inverted invert the match: keep only elements for which the test
    *        query returns no results
    */
  final case class SemiApply[Start <: Location](
    acceptIfThisSucceeds: Query[Start],
    inverted: Boolean = false,
    columns: Columns = Columns.Omitted
  ) extends Query[Start] {
    def isReadOnly: Boolean = acceptIfThisSucceeds.isReadOnly
    def cannotFail: Boolean = acceptIfThisSucceeds.cannotFail
    def canDirectlyTouchNode: Boolean = false
    def isIdempotent: Boolean = acceptIfThisSucceeds.isIdempotent
    def canContainAllNodeScan: Boolean = acceptIfThisSucceeds.canContainAllNodeScan
    def substitute(parameters: Map[Expr.Parameter, Value]): SemiApply[Start] =
      copy(acceptIfThisSucceeds = acceptIfThisSucceeds.substitute(parameters))
    def children: Seq[Query[_]] = Seq(acceptIfThisSucceeds)
  }

  /** Apply one query, then apply another query to all the results of the first
    * query. This is very much like a `flatMap`.
    *
    * NB: Execution of the second query starts from the same place as the first
    * query; only the [[QueryContext]]'s passed the second query will be
    * different.
    *
    * @param startWithThis first query to run
    * @param thenCrossWithThis for each output, run this other query
    */
  final case class Apply[+Start <: Location](
    startWithThis: Query[Start],
    thenCrossWithThis: Query[Start],
    columns: Columns = Columns.Omitted
  ) extends Query[Start] {
    def isReadOnly: Boolean = startWithThis.isReadOnly && thenCrossWithThis.isReadOnly
    def cannotFail: Boolean = startWithThis.cannotFail && thenCrossWithThis.cannotFail
    def canDirectlyTouchNode: Boolean = false
    def isIdempotent: Boolean = startWithThis.isIdempotent && thenCrossWithThis.isIdempotent
    def canContainAllNodeScan: Boolean = startWithThis.canContainAllNodeScan || thenCrossWithThis.canContainAllNodeScan
    def substitute(parameters: Map[Expr.Parameter, Value]): Apply[Start] =
      copy(
        startWithThis = startWithThis.substitute(parameters),
        thenCrossWithThis = thenCrossWithThis.substitute(parameters)
      )
    def children: Seq[Query[_]] = Seq(startWithThis, thenCrossWithThis)
  }

  /** Try to apply a query. If there are results, return those as the outputs.
    * If there are no results, return the input as the only output.
    *
    * @param query the optional query to run
    */
  final case class Optional[Start <: Location](
    query: Query[Start],
    columns: Columns = Columns.Omitted
  ) extends Query[Start] {
    def isReadOnly: Boolean = query.isReadOnly
    def cannotFail: Boolean = query.cannotFail
    def canDirectlyTouchNode: Boolean = false
    def isIdempotent: Boolean = query.isIdempotent
    def canContainAllNodeScan: Boolean = query.canContainAllNodeScan
    def substitute(parameters: Map[Expr.Parameter, Value]): Optional[Start] = copy(query = query.substitute(parameters))
    def children: Seq[Query[_]] = Seq(query)
  }

  /** Given a query, filter the outputs to keep only those where a condition
    * evaluates to `true`
    *
    * @param condition the condition to test
    * @param toFilter the query whose output is filtered
    */
  final case class Filter[+Start <: Location](
    condition: Expr,
    toFilter: Query[Start],
    columns: Columns = Columns.Omitted
  ) extends Query[Start] {
    def isReadOnly: Boolean = toFilter.isReadOnly
    def cannotFail: Boolean = condition.cannotFail && toFilter.cannotFail
    def canDirectlyTouchNode: Boolean = false
    def isIdempotent: Boolean = condition.isPure && toFilter.isIdempotent
    def canContainAllNodeScan: Boolean = toFilter.canContainAllNodeScan
    def substitute(parameters: Map[Expr.Parameter, Value]): Filter[Start] = copy(
      condition = condition.substitute(parameters),
      toFilter = toFilter.substitute(parameters)
    )
    def children: Seq[Query[_]] = Seq(toFilter)
  }

  /** Given a query, drop a prefix of the results
    *
    * @param drop how many results to drop (@see [[Query.Skip.Drop]])
    * @param toSkip the query whose output is cropped
    */
  final case class Skip[+Start <: Location](
    drop: Skip.Drop,
    toSkip: Query[Start],
    columns: Columns = Columns.Omitted
  ) extends Query[Start] {
    def isReadOnly: Boolean = toSkip.isReadOnly
    def cannotFail: Boolean = false // non-number skip
    def canDirectlyTouchNode: Boolean = false
    def isIdempotent: Boolean = drop.isPure && toSkip.isIdempotent
    def canContainAllNodeScan: Boolean = toSkip.canContainAllNodeScan
    def substitute(parameters: Map[Expr.Parameter, Value]): Skip[Start] = copy(
      drop = drop.substitute(parameters),
      toSkip = toSkip.substitute(parameters)
    )
    def children: Seq[Query[_]] = Seq(toSkip)
  }
  object Skip {

    /** an Expr that should evaluate to an integer describing how many rows to skip
      * This expression will be run in a context including only results of a [[Limit.Take]] (if present) --
      * in particular, values from the SKIPed query are not accessible
      */
    type Drop = Expr
  }

  /** Given a query, keep only a prefix of the results
    *
    * @param take how many results to keep (@see [[Query.Limit.Take]])
    * @param toLimit the query whose output is cropped
    */
  final case class Limit[+Start <: Location](
    take: Limit.Take,
    toLimit: Query[Start],
    columns: Columns = Columns.Omitted
  ) extends Query[Start] {
    def isReadOnly: Boolean = toLimit.isReadOnly
    def cannotFail: Boolean = false // non-number limit
    def canDirectlyTouchNode: Boolean = false
    def isIdempotent: Boolean = take.isPure && toLimit.isIdempotent
    def canContainAllNodeScan: Boolean = toLimit.canContainAllNodeScan
    def substitute(parameters: Map[Expr.Parameter, Value]): Limit[Start] = copy(
      take = take.substitute(parameters),
      toLimit = toLimit.substitute(parameters)
    )
    def children: Seq[Query[_]] = Seq(toLimit)
  }
  object Limit {

    /** an Expr that should evaluate to an integer describing how many rows to keep
      * This expression will be run in a context where values from the LIMITed query are not accessible
      */
    type Take = Expr
  }

  /** Given a query, sort the results by a certain expression in the output
    *
    * @param by @see [[Query.Sort.SortBy]]
    * @param toSort the query whose output is sorted
    */
  final case class Sort[+Start <: Location](
    by: Sort.SortBy,
    toSort: Query[Start],
    columns: Columns = Columns.Omitted
  ) extends Query[Start] {
    def isReadOnly: Boolean = toSort.isReadOnly
    def cannotFail: Boolean = by.forall(_._1.cannotFail) && toSort.cannotFail
    def canDirectlyTouchNode: Boolean = false
    def isIdempotent: Boolean = by.forall(_._1.isPure) && toSort.isIdempotent
    def canContainAllNodeScan: Boolean = toSort.canContainAllNodeScan
    def substitute(parameters: Map[Expr.Parameter, Value]): Sort[Start] = copy(
      by = by.map { case (expr, bool) => expr.substitute(parameters) -> bool },
      toSort = toSort.substitute(parameters)
    )
    def children: Seq[Query[_]] = Seq(toSort)
  }
  object Sort {

    /** expressions under which the rows should be compared, and whether or not the sort order is ascending
      * @example (Variable('x), true) is like "ORDER BY x ASC"
      * These expressions will be run in a context including context and results of the query being ordered
      */
    type SortBy = Seq[(Expr, Boolean)]
  }

  /** Given a query, map non-aggregated results of that query according to the specified rules for
    * sorting, deduplication, and windowing/pagination
    *
    * @inv Interpretation of Return matches cypher semantics for a single RETURN clause with no aggregations: In
    * particular, interpreting a [[Return]] will produce the same results as interpreting an equivalent stack of
    * Limit(Skip(Distinct(Sort)))) (@see [[delegates.naiveStack]])
    *
    * @param toReturn the query whose output is to be mapped
    * @param orderBy either Some sequence of rules by which to order the results (@see [[Sort.SortBy]]) or None
    *                TODO: is Some(Seq.empty) meaningful? If not, maybe just use Seq
    * @param distinctBy either Some sequence of expressions among which to deduplicate (@see [[Distinct.DistinctBy]]) or None
    *                TODO: is Some(Seq.empty) meaningful? If not, maybe just use Seq
    *                TODO: is this ever different than the full set of columns? If not, maybe just Boolean like OC uses
    * @param drop either Some number of results to drop (@see [[Skip.Drop]]) or None
    * @param take either Some number of results to limit the result to (@see [[Limit.Take]]) or None
    */
  final case class Return[+Start <: Location](
    toReturn: Query[Start],
    orderBy: Option[Sort.SortBy],
    distinctBy: Option[Distinct.DistinctBy],
    drop: Option[Skip.Drop],
    take: Option[Limit.Take],
    columns: Columns = Columns.Omitted
  ) extends Query[Start] {
    private[cypher] object delegates {
      def sort[S >: Start <: Location](query: Query[S]): Option[Sort[S]] =
        orderBy.map(by => Sort(by, query))
      def distinct[S >: Start <: Location](query: Query[S]): Option[Distinct[S]] =
        distinctBy.map(by => Distinct(by, query))
      def skip[S >: Start <: Location](query: Query[S]): Option[Skip[S]] =
        drop.map(n => Skip(n, query))
      def limit[S >: Start <: Location](query: Query[S]): Option[Limit[S]] =
        take.map(n => Limit(n, query))

      private def orPassThru[S >: Start <: Location](step: Query[S] => Option[Query[S]]): Query[S] => Query[S] =
        query => step(query).getOrElse(query)

      val naiveStack: Query[Start] =
        (orPassThru(sort) andThen orPassThru(distinct) andThen orPassThru(skip) andThen orPassThru(limit))(toReturn)
    }
    def isReadOnly: Boolean = delegates.naiveStack.isReadOnly
    def cannotFail: Boolean = delegates.naiveStack.cannotFail
    def canDirectlyTouchNode: Boolean = delegates.naiveStack.canDirectlyTouchNode
    def isIdempotent: Boolean = delegates.naiveStack.isIdempotent
    def canContainAllNodeScan: Boolean = delegates.naiveStack.canContainAllNodeScan
    def substitute(parameters: Map[Expr.Parameter, Value]): Query[Start] = copy(
      toReturn = toReturn.substitute(parameters),
      orderBy = orderBy.map(_.map { case (expr, bool) => expr.substitute(parameters) -> bool }),
      distinctBy = distinctBy.map(_.map(_.substitute(parameters))),
      drop = drop.map(_.substitute(parameters)),
      take = take.map(_.substitute(parameters))
    )
    def children: Seq[Query[_]] = Seq(toReturn)
  }

  /** Given a query, deduplicate the results by a certain expression
    *
    * @param by expressions under which the output is compared (@see [[Query.Distinct.DistinctBy]])
    * @param toDedup the query whose output is deduplicated
    */
  final case class Distinct[+Start <: Location](
    by: Distinct.DistinctBy,
    toDedup: Query[Start],
    columns: Columns = Columns.Omitted
  ) extends Query[Start] {
    def isReadOnly: Boolean = toDedup.isReadOnly
    def cannotFail: Boolean = by.forall(_.cannotFail) && toDedup.cannotFail
    def canDirectlyTouchNode: Boolean = false
    def isIdempotent: Boolean = by.forall(_.isPure) && toDedup.isIdempotent
    def canContainAllNodeScan: Boolean = toDedup.canContainAllNodeScan
    def substitute(parameters: Map[Expr.Parameter, Value]): Distinct[Start] = copy(
      by = by.map(_.substitute(parameters)),
      toDedup = toDedup.substitute(parameters)
    )
    def children: Seq[Query[_]] = Seq(toDedup)
  }
  object Distinct {

    /** Expressions by which the rows are deduplicated -- each unique cross-product will be kept, with duplicates dropped
      * @example a DistinctBy of Seq(Variable('x), Variable('y)) would deduplicate the following stream like:
      * Input: (x=1, y=2), (x=1, y=1), (x=1, y=2), (x=2, y=1)
      * Output: (x=1, y=2), (x=1, y=1),             (x=2, y=1)
      * These expressions will be run in a context including context and results of the query being ordered
      */
    type DistinctBy = Seq[Expr]
  }

  /** Expand out a list in the context object
    *
    * @param listExpr expression for the list which gets unfolded
    * @param as name under which to register elements of this list in output
    *        contexts
    * @param unwindFrom the query whose output is unwound
    */
  final case class Unwind[+Start <: Location](
    listExpr: Expr,
    as: Symbol,
    unwindFrom: Query[Start],
    columns: Columns = Columns.Omitted
  ) extends Query[Start] {
    def isReadOnly: Boolean = unwindFrom.isReadOnly
    def cannotFail: Boolean = listExpr.cannotFail && unwindFrom.cannotFail
    def canDirectlyTouchNode: Boolean = false
    def isIdempotent: Boolean = listExpr.isPure && unwindFrom.isIdempotent
    def canContainAllNodeScan: Boolean = unwindFrom.canContainAllNodeScan
    def substitute(parameters: Map[Expr.Parameter, Value]): Unwind[Start] = copy(
      listExpr = listExpr.substitute(parameters),
      unwindFrom = unwindFrom.substitute(parameters)
    )
    def children: Seq[Query[_]] = Seq(unwindFrom)
  }

  /** Tweak the values stored in the output (context)
    *
    * @param dropExisting drop all keys from the context
    * @param toAdd add all of these keys to the context
    * @param adjustThis query whose output is adjusted
    */
  final case class AdjustContext[+Start <: Location](
    dropExisting: Boolean,
    toAdd: Vector[(Symbol, Expr)],
    adjustThis: Query[Start],
    columns: Columns = Columns.Omitted
  ) extends Query[Start] {
    def isReadOnly: Boolean = adjustThis.isReadOnly
    def cannotFail: Boolean = toAdd.forall(_._2.cannotFail) && adjustThis.cannotFail
    def canDirectlyTouchNode: Boolean = false
    def isIdempotent: Boolean = toAdd.forall(_._2.isPure) && adjustThis.isIdempotent
    def canContainAllNodeScan: Boolean = adjustThis.canContainAllNodeScan
    def substitute(parameters: Map[Expr.Parameter, Value]): AdjustContext[Start] = copy(
      toAdd = toAdd.map { case (sym, expr) => sym -> expr.substitute(parameters) },
      adjustThis = adjustThis.substitute(parameters)
    )
    def children: Seq[Query[_]] = Seq(adjustThis)
  }

  /** Mutate a property of a node
    *
    * @param key the key of the property
    * @param newValue the updated value ([[scala.None]] means remove the value)
    */
  final case class SetProperty(
    key: Symbol,
    newValue: Option[Expr],
    columns: Columns = Columns.Omitted
  ) extends Query[Location.OnNode] {
    def isReadOnly: Boolean = false
    def cannotFail: Boolean = false // Trying to set a non-property value
    def canDirectlyTouchNode: Boolean = true
    def isIdempotent: Boolean = newValue.forall(_.isPure)
    def canContainAllNodeScan: Boolean = false
    def substitute(parameters: Map[Expr.Parameter, Value]): SetProperty =
      copy(newValue = newValue.map(_.substitute(parameters)))
    def children: Seq[Query[_]] = Seq.empty
  }

  /** Mutate in batch properties of a node
    *
    * @param properties keys and values to set (expected to be a map)
    * @param includeExisting if false, existing properties will be cleared
    */
  final case class SetProperties(
    properties: Expr,
    includeExisting: Boolean,
    columns: Columns = Columns.Omitted
  ) extends Query[Location.OnNode] {
    def isReadOnly: Boolean = false
    def cannotFail: Boolean = false // Trying to set non-property values
    def canDirectlyTouchNode: Boolean = true
    def isIdempotent: Boolean = !includeExisting && properties.isPure
    def canContainAllNodeScan: Boolean = false
    def substitute(parameters: Map[Expr.Parameter, Value]): SetProperties =
      copy(properties = properties.substitute(parameters))
    def children: Seq[Query[_]] = Seq.empty
  }

  /** Delete a node, relationship, or path
    *
    * If the node has edges, a force (aka `DETACH`) delete will clear these
    * edges, and a regular delete will fail.
    *
    * @param toDelete delete a node, edge, or path
    * @param detach delete edges too (else, throw if there are edges)
    */
  final case class Delete(
    toDelete: Expr,
    detach: Boolean,
    columns: Columns = Columns.Omitted
  ) extends Query[Location.Anywhere] {
    def isReadOnly: Boolean = false
    def cannotFail: Boolean = false // Trying to delete non-deletable entity
    def canDirectlyTouchNode: Boolean = true
    def isIdempotent: Boolean = toDelete.isPure
    def canContainAllNodeScan: Boolean = false
    def substitute(parameters: Map[Expr.Parameter, Value]): Delete = copy(toDelete = toDelete.substitute(parameters))
    def children: Seq[Query[_]] = Seq.empty
  }

  /** Mutate an edge of a node
    *
    * TODO: support aliasing the edge?
    *
    * @param label the label of the edge
    * @param direction the direction of the edge
    * @param target the other side of the edge
    * @param add are we adding or removing the edge?
    * @param andThen once on the other side, what do we do?
    */
  final case class SetEdge(
    label: Symbol,
    direction: EdgeDirection,
    bindRelation: Option[Symbol],
    target: Expr,
    add: Boolean,
    andThen: Query[Location.OnNode],
    columns: Columns = Columns.Omitted
  ) extends Query[Location.OnNode] {
    def isReadOnly: Boolean = false
    def cannotFail: Boolean = false // Target is not node-like
    def canDirectlyTouchNode: Boolean = true
    def isIdempotent: Boolean = target.isPure && andThen.isIdempotent
    def canContainAllNodeScan: Boolean = andThen.canContainAllNodeScan
    def substitute(parameters: Map[Expr.Parameter, Value]): SetEdge = copy(
      target = target.substitute(parameters),
      andThen = andThen.substitute(parameters)
    )
    def children: Seq[Query[_]] = Seq(andThen)
  }

  /** Mutate labels of a node
    *
    * @param labels the labels to change
    * @param add are we adding or removing the labels?
    */
  final case class SetLabels(
    labels: Seq[Symbol],
    add: Boolean,
    columns: Columns = Columns.Omitted
  ) extends Query[Location.OnNode] {
    def isReadOnly: Boolean = false
    def cannotFail: Boolean = true
    def canDirectlyTouchNode: Boolean = true
    def isIdempotent: Boolean = true // NB labels are a deduplicated `Set`
    def canContainAllNodeScan: Boolean = false
    def substitute(parameters: Map[Expr.Parameter, Value]): SetLabels = this
    def children: Seq[Query[_]] = Seq.empty
  }

  /** Eager aggregation along properties
    *
    * This is eager because 'no' rows get emitted until all rows have been
    * consumed. Each incoming row is evaluated along a list of expressions.
    * For each such bucket, some aggregate expression is maintained. When
    * there are no more inputs, one row gets outputed for each bucket.
    *
    * @param aggregateAlong criteria along which to partition rows
    * @param aggregateWith how to perform aggregation on each bucket
    * @param toAggregate query whose output is aggregated
    * @param keepExisting do we start from a fresh query context or not?
    */
  final case class EagerAggregation[+Start <: Location](
    aggregateAlong: Vector[(Symbol, Expr)],
    aggregateWith: Vector[(Symbol, Aggregator)],
    toAggregate: Query[Start],
    keepExisting: Boolean,
    columns: Columns = Columns.Omitted
  ) extends Query[Start] {
    def isReadOnly: Boolean = toAggregate.isReadOnly
    def cannotFail: Boolean =
      aggregateAlong.forall(_._2.cannotFail) && toAggregate.cannotFail && aggregateWith.forall(_._2.cannotFail)
    def canDirectlyTouchNode: Boolean = false
    def isIdempotent: Boolean =
      aggregateAlong.forall(_._2.isPure) && aggregateWith.forall(_._2.isPure) && toAggregate.isIdempotent
    def canContainAllNodeScan: Boolean = toAggregate.canContainAllNodeScan
    def substitute(parameters: Map[Expr.Parameter, Value]): EagerAggregation[Start] = copy(
      aggregateAlong = aggregateAlong.map { case (sym, expr) => sym -> expr.substitute(parameters) },
      aggregateWith = aggregateWith.map { case (sym, aggregator) => sym -> aggregator.substitute(parameters) },
      toAggregate = toAggregate.substitute(parameters)
    )
    def children: Seq[Query[_]] = Seq(toAggregate)
  }

  /** Custom procedure call
    *
    * This is where users can define their own custom traversals
    *
    * @param procedure the procedure to call
    * @param arguments the arguments
    * @param returns optional remapping of the procedures output columns
    */
  final case class ProcedureCall(
    procedure: Proc,
    arguments: Seq[Expr],
    returns: Option[Map[Symbol, Symbol]],
    columns: Columns = Columns.Omitted
  ) extends Query[Location.Anywhere] {
    def isReadOnly: Boolean = !procedure.canContainUpdates
    def cannotFail: Boolean = false
    def canDirectlyTouchNode: Boolean = true
    def isIdempotent: Boolean = procedure.isIdempotent && arguments.forall(_.isPure)
    def canContainAllNodeScan: Boolean = procedure.canContainAllNodeScan
    def substitute(parameters: Map[Expr.Parameter, Value]): ProcedureCall =
      copy(arguments = arguments.map(_.substitute(parameters)))
    def children: Seq[Query[_]] = Seq.empty
  }

  /** Sub query context, which allows for running a subquery and then stitching
    * the initial input columns back to the subquery outputs.
    *
    * @param subQuery inner query
    */
  final case class SubQuery[+Start <: Location](
    subQuery: Query[Start],
    columns: Columns = Columns.Omitted
  ) extends Query[Start] {
    def isReadOnly: Boolean = subQuery.isReadOnly
    def cannotFail: Boolean = subQuery.cannotFail
    def canDirectlyTouchNode: Boolean = false
    def isIdempotent: Boolean = subQuery.isIdempotent
    def canContainAllNodeScan: Boolean = subQuery.canContainAllNodeScan
    def substitute(parameters: Map[Expr.Parameter, Value]): SubQuery[Start] =
      copy(subQuery = subQuery.substitute(parameters))
    def children: Seq[Query[_]] = Seq(subQuery)
  }
}
