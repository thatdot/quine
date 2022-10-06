package com.thatdot.quine.webapp.components

import scala.scalajs.js
import scala.scalajs.js.JSConverters._

import slinky.core.StatelessComponent
import slinky.core.annotations.react
import slinky.core.facade.ReactElement

import com.thatdot.quine.Util.toJsObject

@react class BoxPlot extends StatelessComponent {

  /** @param min bottom/left whisker
    * @param max top/right whisker
    * @param median middle bar
    * @param q1 bottom/left bar
    * @param q3 top/right bar
    * @param mean solid line overlay
    * @param explicitlyVisible points to display over the box plot
    *                          NB: plotly's autorange may not consider these values if they are nonpositive
    * @param orientation
    * @param units units to display along primary axis
    * @param layout additional parameters to Plotly layout. Applied last, so these values will be respected as overrides.
    */
  case class Props(
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
    layout: js.Object = js.Dynamic.literal()
  ) {

    /** The part of Plotly's `data` that define layout or may change with orientation:
      *  - type
      *  - boxpoints
      *  - pointpos
      *  - jitter
      *  - orientation
      *  - x or y (depending on `orientation`)
      *  - text (ie labels for "explicitlyVisible" points)
      */
    private def dataLayout: Map[String, js.Any] =
      Map(
        "type" -> "box",
        "boxpoints" -> "all",
        // if any explicit points fall within the box, still display them on the same line.
        "pointpos" -> 0,
        "jitter" -> 0,
        "orientation" -> orientation.orientationVal,
        orientation.primaryAxis -> js
          .Array(
            explicitlyVisible.map(_._1).toJSArray
          ), // intentionally Array[Array[Double]] (where outer array is unary),
        "text" -> js.Array(explicitlyVisible.map(_._2).toJSArray)
      )

    /** The part of Plotly's `data` defining the values to be rendered independent of layout:
      *  - lowerfence
      *  - upperfence
      *  - q1
      *  - q3
      *  - median
      *  - mean
      */
    private def statistics =
      (Map(
        "lowerfence" -> min,
        "q1" -> q1,
        "median" -> median,
        "q3" -> q3,
        "upperfence" -> max
      ) ++ mean.map(x => "mean" -> x))
        .map { case (k, v) => k -> js.Array(v) } // make into unary arrays

    def asPlotlyData(): js.Object =
      toJsObject((statistics ++ dataLayout).toMap)

    private def asPlotlyLayoutBase(): Map[String, js.Any] =
      Map(
        orientation.primaryAxisName ->
        toJsObject(
          Map[String, js.Any](
            "type" -> (
              if (logScale) "log"
              else "linear"
            ),
            "fixedrange" -> true
          ) ++ units.map[(String, js.Any)](s => "title" -> s)
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

  def render(): ReactElement =
    Plotly(
      data = js.Array(
        props.asPlotlyData()
      ),
      layout = props.asPlotlyLayout(),
      useResizeHandler = true,
      style = js.Dynamic.literal(
        width = "100%",
        height = "100%"
      )
    )
}
