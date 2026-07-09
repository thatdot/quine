package com.thatdot.quine.webapp.resultspanel

import com.raquo.laminar.api.L._
import org.scalajs.dom

import com.thatdot.quine.routes.CypherQueryResult
import com.thatdot.quine.webapp.Styles

/** The everyday results table. Cell rendering is delegated to [[CellRender]]. Column
  * headers are click-to-sort (click again to reverse) and horizontally resizable by
  * dragging their right edge: the table starts in auto-fit layout and switches to
  * fixed widths on the first drag (so defaults stay content-sized, then become
  * user-controlled). It reads sort/width state as `Signal`s and emits [[ViewerCommand]]s
  * — selecting a row, toggling a sort, or setting column widths — never mutating state
  * directly.
  */
object ResultsTable {

  def apply(
    result: CypherQueryResult,
    sortCol: Signal[Option[Int]],
    sortDir: Signal[SortDir],
    colWidths: Signal[Vector[Double]],
    vd: Observer[ViewerCommand],
  ): HtmlElement = {
    var tableRef: Option[dom.html.Table] = None

    def measuredWidths(): Vector[Double] =
      tableRef.toVector.flatMap { t =>
        val ths = t.querySelectorAll("thead th")
        (0 until ths.length).map(i => ths(i).asInstanceOf[dom.html.Element].offsetWidth.toDouble)
      }

    table(
      cls := Styles.resultsGrid,
      cls(Styles.resultsGridFixed) <-- colWidths.map(_.nonEmpty),
      width <-- colWidths.map(ws => if (ws.nonEmpty) s"${ws.sum.toInt}px" else "100%"),
      onMountCallback(ctx => tableRef = Some(ctx.thisNode.ref)),
      thead(tr(result.columns.zipWithIndex.map { case (name, idx) =>
        headerCell(name, idx, sortCol, sortDir, colWidths, () => measuredWidths(), vd)
      })),
      tbody(
        result.results.map { row =>
          tr(
            onClick --> (_ => vd.onNext(ViewerCommand.SelectRow(row))),
            row.map(value => td(CellRender.value(value))),
          )
        },
      ),
    )
  }

  private def headerCell(
    name: String,
    colIdx: Int,
    sortCol: Signal[Option[Int]],
    sortDir: Signal[SortDir],
    colWidths: Signal[Vector[Double]],
    measuredWidths: () => Vector[Double],
    vd: Observer[ViewerCommand],
  ): HtmlElement = {
    // Per-handle drag state, captured on mousedown of the resize handle.
    var base: Vector[Double] = Vector.empty
    var startX = 0.0

    th(
      cls := Styles.resultsGridSortable,
      width <-- colWidths.map(ws => ws.lift(colIdx).map(w => s"${w.toInt}px").getOrElse("")),
      onClick --> (_ => vd.onNext(ViewerCommand.ToggleSort(colIdx))),
      span(name),
      span(
        cls := Styles.sortGlyph,
        cls(Styles.sortGlyphActive) <-- sortCol.map(_.contains(colIdx)),
        child.text <-- sortCol.combineWith(sortDir).map {
          case (Some(c), SortDir.Asc) if c == colIdx => "▲"
          case (Some(c), SortDir.Desc) if c == colIdx => "▼"
          case _ => "↕"
        },
      ),
      // Right-edge resize handle. Stops click propagation so dragging never sorts.
      span(
        cls := Styles.colResize,
        title := "Drag to resize column",
        DragGesture.handle(
          onStart = e => {
            base = measuredWidths()
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
      ),
    )
  }
}
