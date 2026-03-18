package com.thatdot.quine.webapp.components.dashboard

import com.raquo.laminar.api.L._

/** A Laminar component rendering as a bootstrap-compatible Card */
object Card {
  def apply(title: Modifier[HtmlElement], body: Modifier[HtmlElement]): HtmlElement =
    div(
      cls := "card",
      div(
        cls := "card-body",
        div(cls := "card-title", title),
        div(cls := "card-text", body),
      ),
    )
}
