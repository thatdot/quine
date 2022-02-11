package com.thatdot.quine.webapp.components

import scala.scalajs.js
import scala.scalajs.js.JSConverters._

import slinky.core.StatelessComponent
import slinky.core.annotations.react
import slinky.core.facade.ReactElement

import com.thatdot.quine.Util.toJsObject

/** Basically a bar chart, except intended for quantitative data already grouped into buckets
  * (i.e., rather than the plotting library determining the buckets)
  */
@react class ManualHistogramPlot extends StatelessComponent {

  /** @param buckets mapping from bucket label to value
    * @param logScale whether the values should be plotted along a log axis
    * @param layout additional layout parameters/overrides to pass through to Plotly
    * @param sortBucketsBy the schema by which to order the buckets. Defaults to alphabetical
    */
  case class Props(
    buckets: Map[String, Double],
    orientation: PlotOrientation = PlotOrientation.Vertical,
    logScale: Boolean = true,
    layout: js.Object = js.Dynamic.literal(),
    sortBucketsBy: Ordering[String] = implicitly
  ) {
    private val bucketsOrdered: Seq[(String, Double)] =
      buckets.toSeq.sorted(Ordering.by[(String, Double), String](_._1)(sortBucketsBy))

    def asPlotlyData(): js.Object = toJsObject(
      Map(
        // values along primary axis
        orientation.primaryAxis -> bucketsOrdered.map { case (_, count) => count }.toJSArray,
        // labels along secondary (label) axis
        orientation.secondaryAxis -> bucketsOrdered.map { case (label, _) => label }.toJSArray,
        "type" -> "bar"
      )
    )

    private def asPlotlyLayoutBase(): Map[String, js.Any] = Map(
      orientation.primaryAxisName ->
      js.Dynamic.literal(
        `type` = if (logScale) "log" else "linear",
        fixedrange = true
      ),
      orientation.secondaryAxisName ->
      js.Dynamic.literal(
        fixedrange = true,
        visible = false
      )
    )

    def asPlotlyLayout(): js.Object = js.Object.assign(
      js.Dynamic.literal(),
      toJsObject(asPlotlyLayoutBase()),
      layout
    )
  }

  override def render(): ReactElement =
    Plotly(
      layout = props.asPlotlyLayout(),
      data = js.Array(props.asPlotlyData()),
      useResizeHandler = true,
      style = js.Dynamic.literal(
        width = "100%",
        height = "100%"
      )
    )
}
