package com.thatdot.quine.webapp.components

import scala.scalajs.js
import scala.scalajs.js.annotation._

import org.scalajs.dom

/** Minimal Scala.js facade for the plotly.js JavaScript library.
  *
  * This wraps the plain plotly.js API (not react-plotly.js), enabling direct
  * imperative usage from Laminar via mount/unmount callbacks.
  *
  * @see [[https://plotly.com/javascript/plotlyjs-function-reference/]]
  */
@js.native
@JSImport("plotly.js", JSImport.Namespace)
object PlotlyJS extends js.Object {

  /** Create a new plot in the given DOM element.
    *
    * @see [[https://plotly.com/javascript/plotlyjs-function-reference/#plotlynewplot]]
    */
  def newPlot(
    element: dom.HTMLElement,
    data: js.Array[js.Object],
    layout: js.UndefOr[js.Object] = js.undefined,
    config: js.UndefOr[js.Object] = js.undefined,
  ): js.Promise[Unit] = js.native

  /** Efficiently update a plot. If the plot does not exist, creates it.
    *
    * @see [[https://plotly.com/javascript/plotlyjs-function-reference/#plotlyreact]]
    */
  def react(
    element: dom.HTMLElement,
    data: js.Array[js.Object],
    layout: js.UndefOr[js.Object] = js.undefined,
    config: js.UndefOr[js.Object] = js.undefined,
  ): js.Promise[Unit] = js.native

  /** Remove all plotly state from a DOM element and free associated memory.
    *
    * @see [[https://plotly.com/javascript/plotlyjs-function-reference/#plotlypurge]]
    */
  def purge(element: dom.HTMLElement): Unit = js.native

  /** Sub-object for plot-level operations like resize.
    *
    * @see [[https://plotly.com/javascript/plotlyjs-function-reference/#plotlyplotsresize]]
    */
  def Plots: PlotlyPlots = js.native
}

/** Facade for Plotly.Plots sub-object. */
@js.native
trait PlotlyPlots extends js.Object {

  /** Recompute the layout and redraw a plot to fit its container.
    *
    * @see [[https://plotly.com/javascript/plotlyjs-function-reference/#plotlyplotsresize]]
    */
  def resize(element: dom.HTMLElement): Unit = js.native
}

/** Trait representing a DOM element that has been enhanced by plotly.js with
  * an `.on()` method for event registration. After calling `PlotlyJS.newPlot`,
  * the target element gains this method.
  *
  * @see [[https://plotly.com/javascript/plotlyjs-events/]]
  */
@js.native
trait PlotlyElement extends js.Object {
  def on(eventName: String, handler: js.Function1[js.Any, Unit]): Unit = js.native
}
