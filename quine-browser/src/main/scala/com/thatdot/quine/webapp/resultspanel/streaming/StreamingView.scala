package com.thatdot.quine.webapp.resultspanel.streaming

import com.raquo.laminar.api.L._
import io.circe.Json
import org.scalajs.dom

import com.thatdot.quine.webapp.Styles
import com.thatdot.quine.webapp.resultspanel.{
  CellRender,
  ColumnResize,
  ResultsData,
  ResultsLayout,
  RowDrawer,
  TapEntry,
  ViewerCommand,
}

/** Renders a live tap's [[LiveStream]] buffer as a growing table. The buffer (rows + column
  * union) and the frame model live alongside in this package; this is just their view.
  */
object StreamingView {

  /** Live tap view: a table whose rows are keyed by `seq` so only newly-appended rows mount
    * (each plays the new-row highlight once); the header binds to the column set so columns
    * appearing mid-stream fill in.
    */
  /** @param maxRows client-side display cap on the buffered rows (oldest first) — the
    *   card system's sampling budget, a `Signal` so budget edits re-cap the live table
    *   without remounting it. `None` (the default, and the legacy results-panel path)
    *   renders the full buffer.
    * @param filterNeedle the card's filter text ([[ResultsData.matches]] semantics — the
    *   same case-insensitive substring-over-JSON match the adhoc card applies), narrowing
    *   the *displayed* rows after the cap: the sample buffer is what it is, the filter is
    *   a view over it. Empty (the default, and the legacy path) shows everything.
    * @param colWidths explicit per-column widths, empty meaning content auto-fit — the same
    *   two-phase model the adhoc table uses (see [[ColumnResize]]). Widths matter more here
    *   than there: under auto layout the browser re-solves every column whenever content
    *   changes, so in a live table one long value arriving shifts the whole table sideways.
    * @param selectedRow the row whose detail the drawer shows, as the adhoc table's rows do
    *   — a snapshot taken at click time, so it keeps showing that row's values even as the
    *   buffer scrolls past it or the cap drops it.
    * @param vd sink for the width edits the header's drag handles emit, and for the row
    *   selection the drawer displays.
    */
  def tapBody(
    tap: TapEntry,
    maxRows: Signal[Option[Int]] = Signal.fromValue(None),
    filterNeedle: Signal[String] = Signal.fromValue(""),
    colWidths: Signal[Vector[Double]] = Signal.fromValue(Vector.empty),
    selectedRow: Signal[Option[Seq[(String, Json)]]] = Signal.fromValue(None),
    vd: Observer[ViewerCommand] = Observer.empty,
  ): HtmlElement = {
    val columns = tap.stream.columns.signal
    val widths = colWidths.combineWith(columns).map { case (ws, cols) => ColumnResize.padToColumns(ws, cols.length) }
    val shownRows =
      tap.stream.rows.signal.combineWith(maxRows, columns, filterNeedle).map { case (rows, cap, cols, needle) =>
        val capped = cap.fold(rows)(rows.take)
        if (needle.trim.isEmpty) capped
        else capped.filter(r => ResultsData.matches(cols.map(c => r.fields.getOrElse(c, Json.Null)), needle))
      }
    // Tail-follow: while the view is at the bottom, keep new rows in view as they arrive;
    // once the user scrolls up they're left alone until they scroll back down.
    var scroller: Option[dom.Element] = None
    var stick = true
    var tableRef: Option[dom.html.Table] = None
    def atBottom(el: dom.Element): Boolean =
      el.scrollTop + el.clientHeight >= el.scrollHeight - ResultsLayout.tailFollowSlackPx
    div(
      cls := Styles.resultsContentArea,
      div(
        cls := Styles.resultsBody,
        onMountCallback(ctx => scroller = Some(ctx.thisNode.ref)),
        onScroll --> (_ => scroller.foreach(el => stick = atBottom(el))),
        tap.stream.rows.signal --> { _ =>
          if (stick) scroller.foreach { el =>
            val _ = dom.window.requestAnimationFrame(_ => el.scrollTop = el.scrollHeight.toDouble)
          }
        },
        table(
          cls := Styles.resultsGrid,
          cls(Styles.resultsGridFixed) <-- widths.map(_.nonEmpty),
          width <-- widths.map(ws => if (ws.nonEmpty) s"${ws.sum.toInt}px" else "100%"),
          onMountCallback(ctx => tableRef = Some(ctx.thisNode.ref)),
          thead(tr(children <-- columns.map(_.zipWithIndex.map { case (name, idx) =>
            headerCell(name, idx, widths, () => ColumnResize.measuredWidths(tableRef), vd)
          }))),
          tbody(children <-- shownRows.splitSeq(_.seq)(rowSig => streamRow(rowSig.now(), columns, vd))),
        ),
      ),
      RowDrawer(selectedRow, vd),
    )
  }

  /** One header cell: the column name plus a right-edge resize handle. Unlike the adhoc
    * table's header this one doesn't sort — a live buffer is append-ordered.
    */
  private def headerCell(
    name: String,
    colIdx: Int,
    widths: Signal[Vector[Double]],
    measuredWidths: () => Vector[Double],
    vd: Observer[ViewerCommand],
  ): HtmlElement =
    th(
      cls := Styles.resultsGridSortable,
      width <-- widths.map(ws => ws.lift(colIdx).map(w => s"${w.toInt}px").getOrElse("")),
      span(name),
      ColumnResize.handle(colIdx, measuredWidths, vd),
    )

  /** One live row. Cells bind to the column set (blank for columns added after this row).
    * A retraction (`!isMatch`) renders struck with a RETRACTED badge on the first cell.
    * Clicking pairs the row's fields with the column set as it stands at that moment and
    * sends them to the drawer.
    */
  private def streamRow(row: StreamRow, columns: Signal[Vector[String]], vd: Observer[ViewerCommand]): HtmlElement =
    tr(
      cls := Styles.streamRow,
      cls(Styles.streamRowRetraction) := !row.isMatch,
      onClick.compose(_.sample(columns)) --> { cols =>
        vd.onNext(ViewerCommand.SelectRow(cols.map(col => col -> row.fields.getOrElse(col, Json.Null))))
      },
      children <-- columns.map { cols =>
        cols.zipWithIndex.map { case (col, idx) =>
          val value = CellRender.value(row.fields.getOrElse(col, Json.Null))
          if (idx == 0 && !row.isMatch) td(span(cls := Styles.retractedBadge, "RETRACTED"), value)
          else td(value)
        }
      },
    )
}
