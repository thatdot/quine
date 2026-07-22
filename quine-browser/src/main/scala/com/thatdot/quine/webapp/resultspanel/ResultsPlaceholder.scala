package com.thatdot.quine.webapp.resultspanel

import com.raquo.laminar.api.L._

import com.thatdot.quine.webapp.Styles

/** Centered NonIdealState-style placeholders for the empty and restored outcomes. */
object ResultsPlaceholder {

  def empty(queryEcho: String): HtmlElement =
    div(
      cls := Styles.resultsEmpty,
      div(cls := Styles.resultsEmptyIcon, "—"),
      div(cls := Styles.resultsEmptyTitle, "No rows matched this query"),
      div(
        cls := Styles.resultsEmptyDesc,
        "The query ran successfully. It just returned nothing. Try widening it.",
      ),
      span(cls := Styles.resultsQueryChip, queryEcho),
    )

  def restored(queryEcho: String, errorMessage: Option[String]): HtmlElement =
    div(
      cls := Styles.resultsEmpty,
      div(cls := Styles.resultsEmptyIcon, "↺"),
      div(cls := Styles.resultsEmptyTitle, errorMessage.getOrElse[String]("Restored from previous session")),
      div(
        cls := Styles.resultsEmptyDesc,
        "Click the query chip above to load it into the editor and re-run.",
      ),
      span(cls := Styles.resultsQueryChip, queryEcho),
    )
}
