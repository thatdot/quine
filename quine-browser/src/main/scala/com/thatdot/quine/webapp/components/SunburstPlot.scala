package com.thatdot.quine.webapp.components

import scala.scalajs.js
import scala.scalajs.js.JSConverters._
import scala.util.Try

import com.raquo.laminar.api.L._

/** Plotly-backed sunburst plot rendered via the plain plotly.js API.
  *
  * @see [[https://plotly.com/javascript/sunburst-charts/]]
  */
object SunburstPlot {

  /** How to divide the radians of a parent among the children */
  sealed abstract class BranchValues(val name: String)
  object BranchValues {
    case object Remainder extends BranchValues("remainder")
    case object Total extends BranchValues("total")
  }

  /** A single datapoint in the chart.
    *
    * @param id unique (non-empty) identifier
    * @param parentId identifier of the parent (leave empty for the top-level)
    * @param value how big is this section
    * @param label label on the chart
    */
  final case class Point(
    id: String,
    parentId: Option[String],
    value: Double,
    label: String,
  )

  trait ClickEvent extends js.Object {
    val event: js.Any
    val nextLevel: js.UndefOr[String]
    val points: js.Array[ClickPoint]
  }

  trait ClickPoint extends js.Object {
    val id: String
    val parent: String
    val label: String
    val value: Double
  }

  /** Render a sunburst chart.
    *
    * @param branchValues how to subdivide a parent among children
    * @param points all data points (see parentId/id for how to encode hierarchy)
    * @param onSunburstClick what to do on a click event
    * @param level ID of the root point
    * @param layout options for laying out the plot
    */
  def apply(
    branchValues: BranchValues,
    points: Seq[Point],
    onSunburstClick: Option[ClickEvent => Unit] = None,
    level: Option[String] = None,
    layout: js.Object = js.Dynamic.literal(),
  ): HtmlElement = {
    val data = js.Dynamic.literal(
      `type` = "sunburst",
      branchvalues = branchValues.name,
      ids = points.view.map(_.id).toJSArray,
      labels = points.view.map(_.label).toJSArray,
      parents = points.view.map(_.parentId.getOrElse("")).toJSArray,
      values = points.view.map(_.value).toJSArray,
      outsidetextfont = js.Dynamic.literal(size = 20, color = "#377eb8"),
      marker = js.Dynamic.literal(line = js.Dynamic.literal(width = 2)),
    )
    for (lvl <- level)
      data.level = lvl

    val clickHandler: Option[js.Function1[js.Any, Unit]] = onSunburstClick.map { func => (a: js.Any) =>
      Try(a.asInstanceOf[ClickEvent]).foreach(func)
    }

    Plotly(
      data = js.Array(data),
      layout = layout,
      onSunburstClick = clickHandler,
    )
  }
}
