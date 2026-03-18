package com.thatdot.quine.webapp.components.dashboard

import com.raquo.laminar.api.L._

/** A bootstrap progress bar with label. Meter goes from 0 to `hardMax`, with any portion
  * of the bar between `softMax` and `hardMax` rendered in the `danger` theme color
  * (default: red) to indicate overfill.
  */
object ProgressBarMeter {
  def apply(
    name: String,
    value: Double,
    softMax: Double,
    hardMax: Double,
  ): HtmlElement = {
    // NB percentFill and percentOverfill are in the range [0, 1]
    val percentFill: Double = math.max((value / softMax) * (softMax / hardMax), 0)
    val percentOverfill: Double = math.max((value - softMax) / hardMax, 0)

    val labelText =
      (if (name.nonEmpty) s"$name " else "") +
      s"$value/$softMax" +
      (if (softMax < hardMax) s"  ($hardMax)" else "")

    div(
      cls := "p-2 d-flex flex-row",
      div(cls := "label me-3 align-self-center", labelText),
      div(
        cls := "progress flex-grow-1",
        height := "30px",
        div(
          cls := "progress-bar progress-bar-striped",
          role := "progressbar",
          width := s"${percentFill * 100}%",
        ),
        div(
          cls := "progress-bar progress-bar-striped bg-danger",
          role := "progressbar",
          width := s"${percentOverfill * 100}%",
        ),
      ),
    )
  }
}
