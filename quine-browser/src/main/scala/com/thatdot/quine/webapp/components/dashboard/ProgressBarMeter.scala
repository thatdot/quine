package com.thatdot.quine.webapp.components.dashboard

import scala.scalajs.js

import slinky.core.StatelessComponent
import slinky.core.annotations.react
import slinky.core.facade.ReactElement
import slinky.web.html.{className, div, key, role, style}

/** A bootstrap progress bar with label. Meter goes from 0 to `hardMax`, with any potion
  * of the bar between `softMax` and `hardMax` rendered in the `danger` theme color
  * (default: red) to indicate overfill
  */
@react class ProgressBarMeter extends StatelessComponent {
  case class Props(
    name: String,
    value: Double,
    softMax: Double,
    hardMax: Double,
  ) {
    // NB percentFill and percentOverfill are in the range [0, 1]
    def percentFill: Double = math.max((value / softMax) * (softMax / hardMax), 0)
    def percentOverfill: Double = math.max((value - softMax) / hardMax, 0)
  }

  override def render(): ReactElement =
    div(className := "p-2 d-flex flex-row", key := props.name)(
      div(className := "label me-3 align-self-center", key := "label")(
        (if (props.name.nonEmpty) s"${props.name} " else "")
        + s"${props.value}/${props.softMax}"
        + (if (props.softMax < props.hardMax) s"  (${props.hardMax})" else ""),
      ),
      div(className := "progress flex-grow-1", style := js.Dynamic.literal(height = "30px"), key := "bar")(
        div(
          className := "progress-bar progress-bar-striped",
          role := "progressbar",
          key := "normal-range-bar",
          style := js.Dynamic.literal(width = s"${props.percentFill * 100}%"),
        )(),
        div(
          className := "progress-bar progress-bar-striped bg-danger",
          role := "progressbar",
          key := "overfilled-range-bar",
          style := js.Dynamic.literal(width = s"${props.percentOverfill * 100}%"),
        )(),
      ),
    )
}
