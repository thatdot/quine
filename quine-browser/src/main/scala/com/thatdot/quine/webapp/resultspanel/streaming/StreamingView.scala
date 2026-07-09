package com.thatdot.quine.webapp.resultspanel.streaming

import com.raquo.laminar.api.L._
import io.circe.Json
import org.scalajs.dom

import com.thatdot.quine.webapp.Styles
import com.thatdot.quine.webapp.resultspanel.{CellRender, ResultsLayout, TapEntry}

/** Renders a live tap's [[LiveStream]] buffer as a growing table. The buffer (rows + column
  * union) and the frame model live alongside in this package; this is just their view.
  */
object StreamingView {

  /** Live tap view: a table whose rows are keyed by `seq` so only newly-appended rows mount
    * (each plays the new-row highlight once); the header binds to the column set so columns
    * appearing mid-stream fill in.
    */
  def tapBody(tap: TapEntry): HtmlElement = {
    val columns = tap.stream.columns.signal
    // Tail-follow: while the view is at the bottom, keep new rows in view as they arrive;
    // once the user scrolls up they're left alone until they scroll back down.
    var scroller: Option[dom.Element] = None
    var stick = true
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
          thead(tr(children <-- columns.map(_.map(name => th(cls := Styles.resultsGridSortable, span(name)))))),
          tbody(children <-- tap.stream.rows.signal.splitSeq(_.seq)(rowSig => streamRow(rowSig.now(), columns))),
        ),
      ),
    )
  }

  /** One live row. Cells bind to the column set (blank for columns added after this row).
    * A retraction (`!isMatch`) renders struck with a RETRACTED badge on the first cell.
    */
  private def streamRow(row: StreamRow, columns: Signal[Vector[String]]): HtmlElement =
    tr(
      cls := Styles.streamRow,
      cls(Styles.streamRowRetraction) := !row.isMatch,
      children <-- columns.map { cols =>
        cols.zipWithIndex.map { case (col, idx) =>
          val value = CellRender.value(row.fields.getOrElse(col, Json.Null))
          if (idx == 0 && !row.isMatch) td(span(cls := Styles.retractedBadge, "RETRACTED"), value)
          else td(value)
        }
      },
    )
}
