package com.thatdot.quine.webapp.components.dashboard

import scala.scalajs.js

import slinky.core.StatelessComponent
import slinky.core.annotations.react
import slinky.core.facade.ReactElement

import com.thatdot.quine.routes.TimerSummary
import com.thatdot.quine.webapp.components.BoxPlot

@react class TimerSummaryCard extends StatelessComponent {
  type Props = TimerSummary

  override def render(): ReactElement = Card(
    title = props.name,
    body = BoxPlot(
      min = props.`10`,
      max = props.`90`,
      q1 = props.q1,
      q3 = props.q3,
      median = props.median,
      mean = Some(props.mean),
      explicitlyVisible = Vector(
        props.`20` -> "20%",
        props.`80` -> "80%",
        props.`99` -> "99%",
        props.min -> "min",
        props.max -> "max",
      ),
      layout = js.Dynamic.literal(
        height = 200,
        margin = js.Dynamic.literal(
          t = 32,
          b = 32,
          l = 32,
        ),
      ),
      units = Some("milliseconds"),
    ),
  )
}
