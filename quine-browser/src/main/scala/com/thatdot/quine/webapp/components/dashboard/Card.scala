package com.thatdot.quine.webapp.components.dashboard

import com.raquo.laminar.api.L._

/** A Laminar component rendering as a bootstrap-compatible Card.
  *
  * Titles are rendered in a larger, centered style using the thatDot brite-blue brand
  * color so cards read as named panels rather than inline content blocks.
  */
object Card {
  def apply(title: Modifier[HtmlElement], body: Modifier[HtmlElement]): HtmlElement =
    div(
      cls := "card",
      div(
        cls := "card-body",
        div(
          cls := "card-title text-center",
          styleAttr := "font-size: 1.4rem; font-weight: 600; " +
          "color: var(--thatdot-brite-blue); margin-bottom: 1rem;",
          title,
        ),
        div(cls := "card-text", body),
      ),
    )
}
