package com.thatdot.quine.webapp.resultspanel

import com.raquo.laminar.api.L._
import io.circe.Json
import io.circe.Printer.{noSpaces, spaces2}

import com.thatdot.quine.webapp.Styles

/** Row-detail drawer that slides in from the surface's right edge. Stays mounted and
  * toggles the `open` class so the slide animation plays; its field list updates
  * reactively as `selected` changes. It reads the selected row as a `Signal` and emits
  * `CloseRow` to dismiss.
  */
object RowDrawer {

  def apply(columns: Seq[String], selected: Signal[Option[Seq[Json]]], vd: Observer[ViewerCommand]): HtmlElement =
    div(
      cls := Styles.resultsDrawer,
      cls(Styles.resultsDrawerOpen) <-- selected.map(_.isDefined),
      div(
        cls := Styles.resultsDrawerHeader,
        title := "Double-click to close",
        onDblClick --> (_ => vd.onNext(ViewerCommand.CloseRow)),
        span("Row detail"),
        span(cursor := "pointer", "✕", onClick --> (_ => vd.onNext(ViewerCommand.CloseRow))),
      ),
      div(
        cls := Styles.resultsDrawerBody,
        children <-- selected.map {
          case Some(row) => columns.zip(row).map { case (col, value) => fieldBlock(col, value) }
          case None => Nil
        },
      ),
    )

  private def fieldBlock(column: String, value: Json): HtmlElement =
    div(
      cls := Styles.drawerField,
      div(cls := Styles.drawerFieldLabel, column),
      div(cls := Styles.drawerFieldValue, renderValue(value)),
    )

  private def renderValue(value: Json): HtmlElement =
    if (value.isObject || value.isArray) pre(spaces2.print(value))
    else {
      val text: String = value.asString.getOrElse(noSpaces.print(value))
      span(text)
    }
}
