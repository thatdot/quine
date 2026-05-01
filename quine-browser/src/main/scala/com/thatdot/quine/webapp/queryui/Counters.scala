package com.thatdot.quine.webapp.queryui

import com.raquo.laminar.api.L._

import com.thatdot.quine.webapp.Styles

/** Components related to the node and edge counters on the right edge of the
  * top navigation bar.
  */
object Counters {

  /** Icon with a subscript counter beside it
    *
    * @param ionClass name of `ionicons` class
    * @param tooltipTitle tooltip description
    * @param count count of things found - if none, the counter is hidden
    */
  def counter(ionClass: String, tooltipTitle: String, count: Option[Int]): HtmlElement =
    htmlTag("i")(
      cls := s"$ionClass ${Styles.navBarButton} ${Styles.rightIcon}",
      title := tooltipTitle,
      display := (if (count.isEmpty) "none" else ""),
      span(
        span(fontSize := "small", count.getOrElse(0).toString),
      ),
    )

  /** Node counter beside an edge counter */
  def nodeEdgeCounters(nodeCount: Option[Int], edgeCount: Option[Int]): HtmlElement =
    div(
      flexGrow := "0",
      display := "flex",
      alignItems := "center",
      counter(
        ionClass = "ion-android-radio-button-on",
        tooltipTitle = "Nodes returned by last query",
        count = nodeCount,
      ),
      counter(
        ionClass = "ion-arrow-resize",
        tooltipTitle = "Edges returned by last query",
        count = edgeCount,
      ),
    )
}
