package com.thatdot.quine.graph.cypher

import com.thatdot.quine.graph.CypherOpsGraph
import com.thatdot.quine.model.Milliseconds

/** Packages together all the information about a query that can be run
  *
  * @param queryText the original query
  * @param query compiled query
  * @param unfixedParameters parameter names that still need to be specified
  * @param fixedParameters vector of parameters already specified
  * @param initialColumns columns that will need to initially be in scope
  */
final case class CompiledQuery(
  queryText: String,
  query: Query[Location.Anywhere],
  unfixedParameters: Seq[String],
  fixedParameters: Parameters,
  initialColumns: Seq[String]
) {

  /** Is the query read-only? */
  def isReadOnly: Boolean = query.isReadOnly

  /** Can the query contain a full node scan? */
  def canContainAllNodeScan: Boolean = query.canContainAllNodeScan

  /** Ordered variables returned by the query */
  def columns: Vector[Symbol] = query.columns match {
    case Columns.Specified(cols) => cols
    case Columns.Omitted =>
      throw new IllegalArgumentException(
        "Missing column information for query"
      )
  }

  /** Run this query on a graph
    *
    * @param parameters          constants referred to in the query
    * @param initialColumnValues variables that should be in scope for the query
    * @param atTime              moment in time to query ([[None]] represents the present)
    * @param initialInterpreter  Some(interpreter that will be used to run the [[query]]) or None to use the
    *                            default AnchoredInterpreter for the provided atTime. Note that certain queries may
    *                            cause other interpreters to be invoked as the query propagates through the graph
    * @return query and its results
    */
  def run(
    parameters: Map[String, Value] = Map.empty,
    initialColumnValues: Map[String, Value] = Map.empty,
    atTime: Option[Milliseconds] = None,
    initialInterpreter: Option[CypherInterpreter[Location.Anywhere]] = None
  )(implicit
    graph: CypherOpsGraph
  ): QueryResults = {

    /* Construct the runtime vector of parameters by combining the ones that
     * fixed at compile time to the ones specified here at runtime
     */
    val params: Parameters = if (unfixedParameters.isEmpty) {
      fixedParameters // optimal case - no user parameters
    } else {
      Parameters(
        unfixedParameters.view.map(parameters.getOrElse(_, Expr.Null)).toIndexedSeq ++
        fixedParameters.params
      )
    }

    // Construct the runtime initial scope
    val initialContext = if (initialColumns.isEmpty) {
      QueryContext.empty
    } else {
      QueryContext(
        initialColumns.view
          .map(colName => Symbol(colName) -> initialColumnValues.getOrElse(colName, Expr.Null))
          .toMap
      )
    }

    val results = graph.cypherOps.query(
      query,
      params,
      atTime,
      initialContext,
      bypassSkipOptimization = false,
      initialInterpreter = initialInterpreter
    )
    QueryResults(this, resultContexts = results)
  }
}
