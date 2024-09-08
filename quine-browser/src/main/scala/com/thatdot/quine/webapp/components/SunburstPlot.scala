package com.thatdot.quine.webapp.components

import scala.scalajs.js
import scala.scalajs.js.JSConverters._
import scala.util.Try

import slinky.core.FunctionalComponent
import slinky.core.annotations.react

/** Plotly-backed [sunburst plot](https://plotly.com/javascript/sunburst-charts/) */
@react object SunburstPlot {

  /** How to divide the radians of a parent among the children */
  sealed abstract class BranchValues(val name: String)
  object BranchValues {
    case object Remainder extends BranchValues("remainder")
    case object Total extends BranchValues("total")
  }

  /** A single datapoint in the chart
    *
    * @param id unique (non-empty) identifier
    * @param parentId identifier of the parent (leave empty for the top-level)
    * @param value how big is this section?
    * @param label label on the chart
    */
  final case class Point(
    id: String,
    parentId: Option[String],
    value: Double,
    label: String,
  )

  /** @param branchValues how to subdivide a parent among children
    * @param points all data points (see `parentId`/`id` for how to encode hierarchy)
    * @param onSunburstClick what to do on a click event
    * @param level ID of the root point
    * @param style options for styling the plot
    * @param layout options for laying out the plot
    */
  case class Props(
    branchValues: BranchValues,
    points: Seq[Point],
    onSunburstClick: Option[ClickEvent => Unit] = None,
    level: Option[String] = None,
    style: js.Object = js.Dynamic.literal(),
    layout: js.Object = js.Dynamic.literal(),
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

  val component: FunctionalComponent[SunburstPlot.Props] = FunctionalComponent[Props] { props =>
    val data = js.Dynamic.literal(
      `type` = "sunburst",
      branchvalues = props.branchValues.name,
      ids = props.points.view.map(_.id).toJSArray,
      labels = props.points.view.map(_.label).toJSArray,
      parents = props.points.view.map(_.parentId.getOrElse("")).toJSArray,
      values = props.points.view.map(_.value).toJSArray,
      outsidetextfont = js.Dynamic.literal(size = 20, color = "#377eb8"),
      marker = js.Dynamic.literal(line = js.Dynamic.literal(width = 2)),
    )
    for (lvl <- props.level)
      data.level = lvl

    val onClick: Option[js.Function1[js.Any, Unit]] = props.onSunburstClick.map { func => (a: js.Any) =>
      Try(a.asInstanceOf[ClickEvent]).foreach(func)
    }

    Plotly(
      data = js.Array(data),
      layout = props.layout,
      useResizeHandler = true,
      style = props.style,
      onSunburstClick = onClick.orUndefined,
    )
  }
}
