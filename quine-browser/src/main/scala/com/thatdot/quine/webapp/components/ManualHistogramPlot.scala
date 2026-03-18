package com.thatdot.quine.webapp.components

import scala.scalajs.js
import scala.scalajs.js.JSConverters._

import com.raquo.laminar.api.L._

import com.thatdot.quine.Util.toJsObject
import com.thatdot.quine.webapp.components.PlotOrientation

/** Plotly bar chart for data already grouped into buckets (i.e., rather than
  * the plotting library determining the buckets).
  *
  * @param buckets mapping from bucket label to value
  * @param orientation horizontal or vertical
  * @param logScale whether the values should be plotted along a log axis
  * @param layout additional layout parameters/overrides to pass through to Plotly
  * @param sortBucketsBy ordering for bucket labels (defaults to alphabetical)
  */
object ManualHistogramPlot {
  def apply(
    buckets: Map[String, Double],
    orientation: PlotOrientation = PlotOrientation.Vertical,
    logScale: Boolean = true,
    layout: js.Object = js.Dynamic.literal(),
    sortBucketsBy: Ordering[String] = implicitly,
  ): HtmlElement = {
    val bucketsOrdered: Seq[(String, Double)] =
      buckets.toSeq.sorted(Ordering.by[(String, Double), String](_._1)(sortBucketsBy))

    val plotData: js.Object = toJsObject(
      Map(
        orientation.primaryAxis -> bucketsOrdered.map { case (_, count) => count }.toJSArray,
        orientation.secondaryAxis -> bucketsOrdered.map { case (label, _) => label }.toJSArray,
        "type" -> "bar",
      ),
    )

    val plotLayoutBase: Map[String, js.Any] = Map(
      orientation.primaryAxisName ->
      js.Dynamic.literal(
        `type` = if (logScale) "log" else "linear",
        fixedrange = true,
      ),
      orientation.secondaryAxisName ->
      js.Dynamic.literal(
        fixedrange = true,
        visible = false,
      ),
    )

    val plotLayout: js.Object = js.Object.assign(
      js.Dynamic.literal(),
      toJsObject(plotLayoutBase),
      layout,
    )

    Plotly(
      data = js.Array(plotData),
      layout = plotLayout,
    )
  }
}
