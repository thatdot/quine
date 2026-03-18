package com.thatdot.quine.webapp.components

import com.raquo.laminar.api.L._
import io.circe.Json
import io.circe.Printer.{noSpaces, spaces2}

import com.thatdot.quine.routes.CypherQueryResult
import com.thatdot.quine.webapp.Styles

/** Render Cypher results in a table */
object CypherResultsTable {

  /** Laminar-compatible version of Util.renderJsonResultValue */
  private def renderJsonResultValue(value: Json): HtmlElement = {
    val indent = value.isObject ||
      value.asArray.exists(_.exists(_.isObject))
    if (indent) pre(spaces2.print(value))
    else span(noSpaces.print(value))
  }

  def apply(result: CypherQueryResult): HtmlElement = {
    val tableHead: Seq[HtmlElement] = result.columns.map(col => th(col))
    val tableBody: Seq[HtmlElement] = result.results.map { row: Seq[Json] =>
      tr(row.map(cypherValue => td(renderJsonResultValue(cypherValue))))
    }
    table(
      cls := Styles.cypherResultsTable,
      thead(tr(tableHead)),
      tbody(tableBody),
    )
  }
}
