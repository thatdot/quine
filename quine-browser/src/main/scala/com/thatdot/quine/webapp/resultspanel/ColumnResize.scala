package com.thatdot.quine.webapp.resultspanel

import com.raquo.laminar.api.L._
import org.scalajs.dom

import com.thatdot.quine.webapp.Styles

/** Column-width dragging, shared by the adhoc [[ResultsTable]] and the live tap table
  * ([[com.thatdot.quine.webapp.resultspanel.streaming.StreamingView]]).
  *
  * Both use the same two-phase model: with no stored widths the table lays out `auto` at
  * `width: 100%`, so columns size to their content; the first drag reads those
  * content-derived widths back off the DOM, emits them as the explicit set, and the table
  * switches to `is-fixed`.
  */
object ColumnResize {

  /** The rendered header widths — the browser's own auto-layout solution. */
  def measuredWidths(table: Option[dom.html.Table]): Vector[Double] =
    table.toVector.flatMap { t =>
      val ths = t.querySelectorAll("thead th")
      (0 until ths.length).map(i => ths(i).asInstanceOf[dom.html.Element].offsetWidth)
    }

  /** When a column appears after the user's last drag it has no stored width, and since
    * `is-fixed` pins the table to the sum of those widths, it would collapse to nothing —
    * so give it a default. Empty stays empty: that's auto layout, where the browser sizes
    * the columns.
    */
  def padToColumns(widths: Vector[Double], colCount: Int): Vector[Double] =
    if (widths.isEmpty) Vector.empty else widths.padTo(colCount, ResultsLayout.defaultColumnWidthPx)

  /** Right-edge drag handle for one header cell. Stops click propagation so a drag never
    * triggers the header's own click action (sorting, in [[ResultsTable]]).
    */
  def handle(colIdx: Int, measured: () => Vector[Double], vd: Observer[ViewerCommand]): HtmlElement = {
    // Per-handle drag state, captured on mousedown.
    var base: Vector[Double] = Vector.empty
    var startX = 0.0

    span(
      cls := Styles.colResize,
      title := "Drag to resize column",
      DragGesture.handle(
        onStart = e => {
          base = measured()
          if (base.indices.contains(colIdx)) {
            startX = e.clientX
            vd.onNext(ViewerCommand.SetColWidths(base)) // freeze to measured widths on first drag
            e.stopPropagation()
            true
          } else false
        },
        onMove = e => {
          val w = (base(colIdx) + (e.clientX - startX)).max(ResultsLayout.minColumnWidthPx)
          vd.onNext(ViewerCommand.SetColWidths(base.updated(colIdx, w)))
        },
        bodyClass = Some(Styles.colResizing),
      ),
      onClick --> (_.stopPropagation()),
    )
  }
}
