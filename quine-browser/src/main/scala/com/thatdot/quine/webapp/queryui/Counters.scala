package com.thatdot.quine.webapp.queryui

import scala.scalajs.js.Dynamic.{literal => jsObj}

import slinky.core.FunctionalComponent
import slinky.web.html._

import com.thatdot.quine.webapp.Styles

/** Components related to the node and edge counters on the right edge of the
  * top navigation bar
  */
object Counters {

  /** Counter icons
    *
    * @param ionClass name of `ionicons` class
    * @param title tooltip description
    * @param count count of things found - if none, the counter is hidden
    */
  final case class CounterProps(
    ionClass: String,
    tooltipTitle: String,
    count: Option[Int]
  )

  /** Icon with a subscript counter beside it */
  val counter: FunctionalComponent[CounterProps] = FunctionalComponent[CounterProps] {
    case CounterProps(ionClass, tooltipTitle, countOpt) =>
      val classes = List(ionClass, Styles.navBarButton, Styles.rightIcon)
      val iStyle = jsObj(visibility = if (countOpt.isEmpty) "hidden" else "visible")

      i(className := classes.mkString(" "), title := tooltipTitle, style := iStyle)(
        span(
          span(style := jsObj(fontSize = "small"))(
            countOpt.getOrElse(0).toString
          )
        )
      )
  }

  /** Node counter beside an edge counter */
  val nodeEdgeCounters: FunctionalComponent[(Option[Int], Option[Int])] =
    FunctionalComponent[(Option[Int], Option[Int])] { case (nodeCount, edgeCount) =>
      val nodes = CounterProps(
        ionClass = "ion-android-radio-button-on",
        tooltipTitle = "Nodes returned by last query",
        count = nodeCount
      )
      val edges = CounterProps(
        ionClass = "ion-arrow-resize",
        tooltipTitle = "Edges returned by last query",
        count = edgeCount
      )

      div(style := jsObj(flexGrow = "0", display = "flex"))(counter(nodes), counter(edges))
    }
}
