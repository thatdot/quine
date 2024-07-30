package com.thatdot.quine.graph.cypher

import com.thatdot.quine.model.QuineIdProvider
import com.thatdot.quine.util.Log._

/** Packages together all the information about a query that can be run
  *
  * @param expressionText the original expression
  * @param expr compiled expression
  * @param unfixedParameters parameter names that still need to be specified
  * @param fixedParameters vector of parameters already specified
  * @param initialColumns columns that will need to initially be in scope
  */
final case class CompiledExpr(
  expressionText: String,
  expr: Expr,
  unfixedParameters: Seq[String],
  fixedParameters: Parameters,
  initialColumns: Seq[String]
) {

  /** Evaluate this expression
    *
    * @param parameters constants referred to in the query
    * @param initialColumnValues variables that should be in scope for the query
    *
    * @return query and its results
    */
  def evaluate(
    parameters: Map[String, Value] = Map.empty,
    initialColumnValues: Map[String, Value] = Map.empty
  )(implicit
    idProvider: QuineIdProvider,
    logConfig: LogConfig
  ): Value = {

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

    expr.eval(initialContext)(idProvider, params, logConfig)
  }
}
