package com.thatdot.quine.graph.cypher

import org.apache.pekko.NotUsed

import com.thatdot.quine.graph.namespaceToString

/** Packages together all the information about a query that can be run
  *
  * @param queryText the original query
  * @param query compiled query
  * @param unfixedParameters parameter names that still need to be specified
  * @param fixedParameters vector of parameters already specified
  * @param initialColumns columns that will need to initially be in scope
  */
final case class CompiledQuery[+Start <: Location](
  queryText: String,
  query: Query[Start],
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

  /** To start a query, use [[graph.cypherOps.query]] or [[graph.cypherOps.queryFromNode]] instead
    * Run this query on a graph
    *
    * @param parameters          constants referred to in the query
    * @param initialColumnValues variables that should be in scope for the query
    * @param initialInterpreter  Some(interpreter that will be used to run the [[query]]) or None to use the
    *                            default AnchoredInterpreter for the provided atTime. Note that certain queries may
    *                            cause other interpreters to be invoked as the query propagates through the graph
    * @return query and its results
    */
  private[graph] def run(
    parameters: Map[String, Value],
    initialColumnValues: Map[String, Value],
    initialInterpreter: CypherInterpreter[Start]
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
        initialColumns
          .map(colName => Symbol(colName) -> initialColumnValues.getOrElse(colName, Expr.Null))
          .toMap
      )
    }

    val results = initialInterpreter
      .interpret(query, initialContext)(params)
      .mapMaterializedValue(_ => NotUsed)
      .named(
        "cypher-query-namespace-" + namespaceToString(
          initialInterpreter.namespace
        ) + "-atTime-" + initialInterpreter.atTime.fold("none")(_.millis.toString)
      )

    QueryResults(this, resultContexts = results)
  }
}
