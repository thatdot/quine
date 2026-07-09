package com.thatdot.quine.webapp.resultspanel

import com.raquo.laminar.api.L._

import com.thatdot.quine.webapp.Styles

/** The result-value controls in the bar's right cluster — filter (magnifier → inline field →
  * clearable chip), the Table·JSON segmented toggle, and the export menu. Tabular results only;
  * other outcomes have nothing to view. Reads a [[ViewerReads]] projection and emits
  * [[ViewerCommand]]s.
  */
object ViewerControls {

  def apply(
    outcome: ResultOutcome,
    reads: ViewerReads,
    vd: Observer[ViewerCommand],
  ): Modifier[HtmlElement] = outcome match {
    case ResultOutcome.Tabular(_) =>
      Seq(filterAffordance(reads, vd), viewToggle(reads.view, vd), exportControl(reads, vd))
    case _ => emptyNode
  }

  /** A magnifier at rest, an inline filter field while open, or a clearable `⌕ text ✕` chip when a
    * filter is applied but collapsed.
    */
  private def filterAffordance(reads: ViewerReads, vd: Observer[ViewerCommand]): HtmlElement =
    span(
      cls := Styles.resultsFilterAffordance,
      child <-- reads.filterOpen.combineWith(reads.search).map {
        case (true, _) => filterInput(reads.search, vd)
        case (false, q) if q.nonEmpty =>
          span(
            cls := Styles.resultsFilterChip,
            span(cursor := "pointer", s"⌕ $q", onClick --> (_ => vd.onNext(ViewerCommand.OpenFilter))),
            span(cursor := "pointer", " ✕", onClick --> (_ => vd.onNext(ViewerCommand.ClearSearch))),
          )
        case _ =>
          span(
            cls := Styles.resultsFilterButton,
            cursor := "pointer",
            title := "Filter rows",
            ResultsIcons.magnifier,
            onClick --> (_ => vd.onNext(ViewerCommand.OpenFilter)),
          )
      },
    )

  private def filterInput(search: Signal[String], vd: Observer[ViewerCommand]): HtmlElement =
    div(
      cls := Styles.resultsFilterInput,
      span(cls := Styles.resultsFilterIcon, ResultsIcons.magnifier),
      input(
        tpe := "text",
        cls := Styles.resultsFilterField,
        placeholder := "Filter rows",
        controlled(value <-- search, onInput.mapToValue --> (v => vd.onNext(ViewerCommand.SetSearch(v)))),
        onMountCallback(ctx => ctx.thisNode.ref.focus()),
        onKeyDown --> (e => if (e.key == "Escape") vd.onNext(ViewerCommand.CloseFilter)),
      ),
      span(cursor := "pointer", "✕", onClick --> (_ => vd.onNext(ViewerCommand.CloseFilter))),
    )

  private def viewToggle(view: Signal[ResultsView], vd: Observer[ViewerCommand]): HtmlElement =
    div(
      cls := Styles.resultsViewToggle,
      seg("Table", ResultsView.Table, view, vd),
      seg("JSON", ResultsView.Json, view, vd),
    )

  private def seg(
    label: String,
    target: ResultsView,
    view: Signal[ResultsView],
    vd: Observer[ViewerCommand],
  ): HtmlElement =
    button(
      tpe := "button",
      cls := Styles.resultsViewSeg,
      cls(Styles.resultsViewSegActive) <-- view.map(_ == target),
      label,
      onClick --> (_ => vd.onNext(ViewerCommand.SetView(target))),
    )

  private def exportControl(reads: ViewerReads, vd: Observer[ViewerCommand]): HtmlElement =
    div(
      cls := Styles.resultsExportWrap,
      OutsideClick.dismiss(reads.exportOpen, () => vd.onNext(ViewerCommand.CloseExport)),
      button(
        tpe := "button",
        cls := Styles.resultsExportButton,
        title := "Export",
        onClick --> (_ => vd.onNext(ViewerCommand.ToggleExport)),
        ResultsIcons.download,
      ),
      child <-- reads.exportOpen.map {
        case true => exportMenu(reads.csvFlat, vd)
        case false => emptyNode
      },
    )

  private def exportMenu(csvFlat: Signal[Boolean], vd: Observer[ViewerCommand]): HtmlElement =
    div(
      cls := Styles.resultsExportMenu,
      menuItem("Copy to clipboard (JSON)", ViewerCommand.CopyJson, vd),
      menuItem("Copy as CSV", ViewerCommand.CopyCsv, vd),
      menuItem("Download CSV (.csv)", ViewerCommand.DownloadCsv, vd),
      menuItem("Download JSON (.json)", ViewerCommand.DownloadJson, vd),
      label(
        cls := Styles.resultsExportFlatten,
        input(
          tpe := "checkbox",
          checked <-- csvFlat,
          onClick --> (_ => vd.onNext(ViewerCommand.ToggleCsvFlatten)),
        ),
        span("Flatten objects to columns"),
      ),
    )

  private def menuItem(text: String, cmd: ViewerCommand, vd: Observer[ViewerCommand]): HtmlElement =
    div(cls := Styles.resultsExportItem, text, onClick --> (_ => vd.onNext(cmd)))
}
