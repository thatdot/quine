package com.thatdot.quine.webapp.resultspanel.cards

import com.raquo.laminar.api.L._
import io.circe.Json
import io.circe.Printer.spaces2

import com.thatdot.quine.webapp.Styles
import com.thatdot.quine.webapp.resultspanel.{
  ResultOutcome,
  ResultsContent,
  ResultsData,
  ResultsExport,
  ResultsPlaceholder,
  ResultsTable,
  ResultsView,
  RowDrawer,
  ViewerCommand,
  ViewerReads,
}

/** An adhoc card's tabular body: the existing [[ResultsTable]] / [[RowDrawer]] rendering
  * (Table/JSON toggle handled by the caller's [[com.thatdot.quine.webapp.resultspanel.ViewerControls]]
  * in the popup header — design doc §3 "same header, same … drawer behavior as every other
  * card"). Adhoc results are a complete set, shown in full — sampling controls are
  * tap-only (see [[TapCardBodies.tapTable]]).
  */
object AdhocCardBody {

  def apply(
    outcomeOpt: Option[ResultsContent],
    reads: ViewerReads,
    vd: Observer[ViewerCommand],
  ): HtmlElement =
    outcomeOpt match {
      case None =>
        div(cls := Styles.resultsBody, ResultsPlaceholder.empty(""))
      case Some(content) =>
        content.outcome match {
          case ResultOutcome.Tabular(result) =>
            div(
              cls := Styles.resultsContentArea,
              div(
                cls := Styles.resultsBody,
                child <-- Signal
                  .combine(reads.view, reads.search, reads.sortCol, reads.sortDir)
                  .map { case (view, needle, sortCol, sortDir) =>
                    val derived = ResultsData.derive(result, needle, sortCol, sortDir)
                    view match {
                      case ResultsView.Table =>
                        ResultsTable(derived, reads.sortCol, reads.sortDir, reads.colWidths, vd)
                      case ResultsView.Json =>
                        pre(cls := Styles.resultsJson, ResultsExport.toJson(derived)): HtmlElement
                    }
                  },
              ),
              RowDrawer(result.columns, reads.selectedRow, vd),
            )
          case ResultOutcome.TextResults(values) =>
            div(cls := Styles.resultsBody, pre(cls := Styles.resultsJson, textJson(values)))
          case ResultOutcome.EmptyResult(_, _) =>
            div(cls := Styles.resultsBody, ResultsPlaceholder.empty(content.queryEcho))
          case ResultOutcome.Restored(errorMessage) =>
            div(cls := Styles.resultsBody, ResultsPlaceholder.restored(content.queryEcho, errorMessage))
        }
    }

  private def textJson(values: Seq[Json]): String = spaces2.print(Json.fromValues(values))
}
