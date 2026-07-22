package com.thatdot.quine.webapp.queryui

import com.raquo.laminar.api.L._

/** The node/edge counter visual — an ionicon with a subscript count beside it, one for nodes and
  * one for edges. Originally lived permanently in the top bar's right edge; the redesign's
  * ephemeral canvas indicator (design doc §5, [[ResultCountIndicator]]) now shows this same
  * visual instead of a plain count string, so it reads as *the counters* fading in over the
  * canvas rather than an unrelated toast. Host supplies the wrapping CSS classes (the top bar and
  * the indicator style the icons differently — nav-bar chrome vs. a translucent pill), so this
  * object stays presentation-agnostic about where it's mounted.
  */
object Counters {

  /** Icon with a subscript counter beside it
    *
    * @param ionClass name of `ionicons` class
    * @param tooltipTitle tooltip description
    * @param count count of things found - if none, the counter is hidden
    * @param iconClass CSS class(es) applied to the icon element, supplied by the host
    */
  def counter(ionClass: String, tooltipTitle: String, count: Option[Int], iconClass: String): HtmlElement =
    htmlTag("i")(
      cls := s"$ionClass $iconClass",
      title := tooltipTitle,
      display := (if (count.isEmpty) "none" else ""),
      span(
        span(fontSize := "small", count.getOrElse(0).toString),
      ),
    )

  /** Node counter beside an edge counter.
    *
    * @param wrapClass CSS class for the wrapping element
    * @param iconClass CSS class for each icon (see [[counter]])
    */
  def nodeEdgeCounters(
    nodeCount: Option[Int],
    edgeCount: Option[Int],
    wrapClass: String,
    iconClass: String,
  ): HtmlElement =
    div(
      cls := wrapClass,
      counter(
        ionClass = "ion-android-radio-button-on",
        tooltipTitle = "Nodes returned by last query",
        count = nodeCount,
        iconClass = iconClass,
      ),
      counter(
        ionClass = "ion-arrow-resize",
        tooltipTitle = "Edges returned by last query",
        count = edgeCount,
        iconClass = iconClass,
      ),
    )
}
