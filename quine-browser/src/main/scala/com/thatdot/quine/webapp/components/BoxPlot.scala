package com.thatdot.quine.webapp.components

import scala.scalajs.js
import scala.scalajs.js.JSConverters._

import com.raquo.laminar.api.L._

import com.thatdot.quine.Util.toJsObject
import com.thatdot.quine.webapp.components.PlotOrientation

/** Plotly box/whisker plot rendered via the plain plotly.js API.
  *
  * @param min bottom/left whisker
  * @param max top/right whisker
  * @param median middle bar
  * @param q1 bottom/left bar
  * @param q3 top/right bar
  * @param mean solid line overlay
  * @param explicitlyVisible points to display over the box plot
  * @param orientation horizontal or vertical
  * @param units units to display along primary axis
  * @param logScale whether to use a log scale on the primary axis
  * @param layout additional parameters to Plotly layout (applied as overrides)
  */
object BoxPlot {
  def apply(
    min: Double,
    q1: Double,
    median: Double,
    q3: Double,
    max: Double,
    mean: Option[Double] = None,
    explicitlyVisible: Seq[(Double, String)] = Vector.empty,
    orientation: PlotOrientation = PlotOrientation.Horizontal,
    units: Option[String] = None,
    logScale: Boolean = true,
    layout: js.Object = js.Dynamic.literal(),
  ): HtmlElement = {
    val dataLayout: Map[String, js.Any] =
      Map(
        "type" -> "box",
        "boxpoints" -> "all",
        "pointpos" -> 0,
        "jitter" -> 0,
        "orientation" -> orientation.orientationVal,
        orientation.primaryAxis -> js.Array(explicitlyVisible.map(_._1).toJSArray),
        "text" -> js.Array(explicitlyVisible.map(_._2).toJSArray),
      )

    val statistics: Map[String, js.Any] =
      (Map(
        "lowerfence" -> min,
        "q1" -> q1,
        "median" -> median,
        "q3" -> q3,
        "upperfence" -> max,
      ) ++ mean.map(x => "mean" -> x))
        .map { case (k, v) => k -> js.Array(v).asInstanceOf[js.Any] }

    val plotData: js.Object = toJsObject((statistics ++ dataLayout).toMap)

    val plotLayoutBase: Map[String, js.Any] = Map(
      orientation.primaryAxisName ->
      toJsObject(
        Map[String, js.Any](
          "type" -> (if (logScale) "log" else "linear"),
          "fixedrange" -> true,
        ) ++ units.map[(String, js.Any)](s => "title" -> s),
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
