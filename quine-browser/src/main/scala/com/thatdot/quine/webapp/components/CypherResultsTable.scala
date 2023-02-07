package com.thatdot.quine.webapp.components

import io.circe.Json
import slinky.core.FunctionalComponent
import slinky.core.annotations.react
import slinky.core.facade.ReactElement
import slinky.web.html._

import com.thatdot.quine.Util.renderJsonResultValue
import com.thatdot.quine.routes.CypherQueryResult
import com.thatdot.quine.webapp.Styles

/** Render Cypher results in a table */
@react object CypherResultsTable {
  val component: FunctionalComponent[CypherQueryResult] = FunctionalComponent[CypherQueryResult] { props =>
    val tableHead: Seq[ReactElement] = props.columns.map(col => th(col))
    val tableBody: Seq[ReactElement] = props.results.map { row: Seq[Json] =>
      tr(row.map { cypherValue =>
        td(
          renderJsonResultValue(cypherValue)
        ): ReactElement
      }: _*)
    }
    table(className := Styles.cypherResultsTable)(
      thead(tr(tableHead: _*)),
      tbody(tableBody: _*)
    )
  }
}
