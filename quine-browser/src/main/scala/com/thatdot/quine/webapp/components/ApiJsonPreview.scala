package com.thatdot.quine.webapp.components

import com.raquo.laminar.api.L._
import io.circe.Json
import io.circe.Printer.spaces2

import com.thatdot.quine.webapp.Styles

object ApiJsonPreview {

  def apply(json: Signal[Json]): HtmlElement = {
    val expandedVar = Var(false)

    div(
      cls := Styles.apiJsonPreview,
      div(
        cls := Styles.apiJsonPreviewToggle,
        onClick --> (_ => expandedVar.update(!_)),
        child.text <-- expandedVar.signal.map(expanded => if (expanded) "▾ API JSON" else "▸ API JSON"),
      ),
      child <-- expandedVar.signal.combineWith(json).map {
        case (true, j) =>
          pre(
            cls := Styles.apiJsonPreviewCode,
            code(spaces2.print(j)),
          )
        case (false, _) => emptyNode
      },
    )
  }
}
