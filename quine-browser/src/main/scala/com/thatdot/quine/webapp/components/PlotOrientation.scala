package com.thatdot.quine.webapp.components

object PlotOrientation {
  final case object Horizontal extends PlotOrientation("h", "x", "y")
  final case object Vertical extends PlotOrientation("v", "y", "x")
}
sealed abstract class PlotOrientation(
  val orientationVal: String,
  val primaryAxis: String,
  val secondaryAxis: String
) {
  val primaryAxisName: String = primaryAxis + "axis"
  val secondaryAxisName: String = secondaryAxis + "axis"
}
