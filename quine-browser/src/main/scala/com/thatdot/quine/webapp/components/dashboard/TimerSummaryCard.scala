package com.thatdot.quine.webapp.components.dashboard

import scala.scalajs.js

import com.raquo.laminar.api.L._

import com.thatdot.quine.routes.TimerSummary
import com.thatdot.quine.webapp.components.BoxPlot

object TimerSummaryCard {
  def apply(timer: TimerSummary): HtmlElement = Card(
    title = timer.name,
    body = BoxPlot(
      min = timer.`10`,
      max = timer.`90`,
      q1 = timer.q1,
      q3 = timer.q3,
      median = timer.median,
      mean = Some(timer.mean),
      explicitlyVisible = Vector(
        timer.`20` -> "20%",
        timer.`80` -> "80%",
        timer.`99` -> "99%",
        timer.min -> "min",
        timer.max -> "max",
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
