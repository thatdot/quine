package com.thatdot.quine.graph.cypher

import akka.NotUsed
import akka.stream.scaladsl.Source

/** Packages together all the information about a query that is running */
final case class QueryResults(
  compiled: CompiledQuery,
  private val resultContexts: Source[QueryContext, NotUsed]
) {

  /** Ordered variables returned by the query */
  def columns: Vector[Symbol] = compiled.query.columns match {
    case Columns.Specified(cols) => cols
    case Columns.Omitted =>
      throw new IllegalArgumentException(
        "Missing column information for query"
      )
  }

  /** Results, in the same order as [[columns]] */
  def results: Source[Vector[Value], NotUsed] =
    resultContexts.map { (context: QueryContext) =>
      columns.map(context.getOrElse(_, Expr.Null))
    }

  @deprecated("Use `results` instead", "soon!")
  def contexts: Source[QueryContext, NotUsed] = resultContexts
}
