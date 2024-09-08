package com.thatdot.quine.compiler.cypher

import org.opencypher.v9_0.expressions.LogicalVariable

import com.thatdot.quine.graph.cypher.Expr

/** Tracks information related to variables in scope
  *
  * @param anchoredNodes mapping from node variable to an expression that can be jumped to
  * @param columnIdx mapping from a variable to the index into the columns
  * @param columnsReversed columns in scope, in reverse order
  *
  * Invariants:
  *
  * {{{
  * // `anchoredNodes` and `columnIdx` store disjoint information
  * anchoredNodes.keySet & columnIdx.keySet == Set.empty
  *
  * // `columnIdx` and `columnsReversed` store the same information
  * columnIdx.toList.sortBy(_._2).map(_._1) == columnsReversed.reverse
  * }}}
  */
final case class QueryScopeInfo(
  private val anchoredNodes: Map[Symbol, Expr],
  private val columnIdx: Map[Symbol, Int],
  private val columnsReversed: List[Symbol],
) {

  /** Look up a variable in the columns
    *
    * TODO: when we switch variable accesses to be indexed based, we'll use the `Int`
    *
    * @param variable openCypher variable we want
    * @return IR resolved variable
    */
  def getVariable(variable: Symbol): Option[Expr.Variable] =
    columnIdx
      .get(variable)
      .map(_ => Expr.Variable(variable))

  /** Look up an expression that can be used to jump to a given node
    *
    * @param variable variable whose node we want to jumpt to
    * @return expression that evaluates to the node or its address
    */
  def getAnchor(variable: LogicalVariable): Option[Expr] = {
    val sym = logicalVariable2Symbol(variable)
    getVariable(sym) orElse anchoredNodes.get(sym)
  }

  /** Add some anchor nodes to the context
    *
    * @param anchors extra information for how to jump to some nodes
    * @return new context with extra anchors
    */
  def withNewAnchors(
    anchors: Iterable[(Symbol, Expr)],
  ): QueryScopeInfo =
    copy(anchoredNodes = anchoredNodes ++ anchors.filter { case (v, _) => !columnIdx.contains(v) })

  /** Remove all anchor nodes from the context
    *
    * @return new context without any anchors
    */
  def withoutAnchors: QueryScopeInfo = copy(anchoredNodes = Map.empty)

  /** Add a new column to the end of the context
    *
    * @param variable new variable to append to the context
    * @return context with the variable and expression for reading the variable
    */
  def addColumn(variable: Symbol): (QueryScopeInfo, Expr.Variable) = {
    require(!columnIdx.contains(variable), s"variable $variable is already in context")
    val scope = QueryScopeInfo(
      anchoredNodes = anchoredNodes - variable,
      columnIdx = columnIdx + (variable -> columnIdx.size),
      columnsReversed = Symbol(variable.name) :: columnsReversed,
    )
    (scope, Expr.Variable(variable))
  }

  /** Clear all columns from the context
    *
    * @return context without any columns
    */
  def clearColumns: QueryScopeInfo =
    QueryScopeInfo(
      anchoredNodes,
      columnIdx = Map.empty,
      columnsReversed = List.empty,
    )

  /** @return columns in scope */
  def getColumns: Vector[Symbol] = columnsReversed.reverse.toVector
}
object QueryScopeInfo {

  /** Empty scope */
  final val empty: QueryScopeInfo = QueryScopeInfo(Map.empty, Map.empty, List.empty)
}
