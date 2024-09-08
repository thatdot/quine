package com.thatdot.quine.webapp.components.dashboard

import slinky.core.StatelessComponent
import slinky.core.annotations.react
import slinky.core.facade.ReactElement
import slinky.web.html.{className, div, key}

/** A react component rendering as a bootstrap-compatible Card
  */
@react class Card extends StatelessComponent {

  case class Props(
    title: ReactElement,
    body: ReactElement,
  )

  override def render(): ReactElement =
    div(
      className := "card",
    )(
      div(
        className := "card-body",
      )(
        div(
          className := "card-title",
          key := "card-title",
        )(props.title),
        div(
          className := "card-text",
          key := "card-text",
        )(props.body),
      ),
    )
}
