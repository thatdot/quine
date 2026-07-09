package com.thatdot.quine.webapp.resultspanel

import com.raquo.laminar.api.L._

import com.thatdot.quine.webapp.Styles

/** Centered NonIdealState-style placeholders for the empty and error outcomes. */
object ResultsPlaceholder {

  def empty(queryEcho: String): HtmlElement =
    div(
      cls := Styles.resultsEmpty,
      div(cls := Styles.resultsEmptyIcon, "—"),
      div(cls := Styles.resultsEmptyTitle, "No rows matched this query"),
      div(
        cls := Styles.resultsEmptyDesc,
        "The query ran successfully — it just returned nothing. Try widening it.",
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

  def error(err: StructuredError, actions: Seq[ResultsAction]): HtmlElement =
    div(
      cls := Styles.resultsError,
      div(
        cls := Styles.resultsErrorHead,
        span(cls := Styles.resultsErrorBadge, "!"),
        span(cls := Styles.resultsErrorTitle, err.headline),
        err.kind.map(k => span(cls := Styles.resultsErrorKind, k)),
      ),
      err.location.map(loc => div(cls := Styles.resultsErrorLocation, locationLine(loc))),
      errorQueryBlock(err),
      actions.map(actionButton),
    )

  private def actionButton(action: ResultsAction): HtmlElement =
    button(
      tpe := "button",
      cls := Styles.resultsErrorAction,
      action.label,
      onClick --> (_ => action.run()),
    )

  private def locationLine(loc: ErrorLocation): String = {
    val base = Seq(loc.line.map(l => s"line $l"), loc.column.map(c => s"column $c")).flatten.mkString(" · ")
    loc.offset.fold(base) { o =>
      if (base.isEmpty) s"offset $o" else s"$base (offset $o)"
    }
  }

  /** The offending query in a mono block, with a caret under the error column when
    * one was parsed (best-effort; accurate for single-line queries).
    */
  private def errorQueryBlock(err: StructuredError): Option[HtmlElement] =
    Option.when(err.offendingQuery.nonEmpty) {
      val caretMods: Seq[Modifier[HtmlElement]] = err.location.flatMap(_.column) match {
        case Some(column) =>
          Seq(br(), span(cls := Styles.resultsErrorCaret, " " * (column - 1).max(0) + "^"))
        case None => Seq.empty
      }
      pre(cls := Styles.resultsErrorQuery, err.offendingQuery, caretMods)
    }
}
