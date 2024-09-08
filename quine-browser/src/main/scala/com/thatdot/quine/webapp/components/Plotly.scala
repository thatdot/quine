package com.thatdot.quine.webapp.components

import scala.scalajs.js

import org.scalajs.dom
import slinky.core.ExternalComponent
import slinky.core.annotations.react

import js.annotation._

@react object Plotly extends ExternalComponent {

  /* References:
   *
   * https://www.npmjs.com/package/react-plotly.js#api-reference
   * https://github.com/DefinitelyTyped/DefinitelyTyped/blob/master/types/react-plotly.js/index.d.ts
   *
   * @note this is still missing some handlers for various events
   */
  case class Props(
    data: js.Array[js.Object],
    layout: js.Object,
    frames: js.UndefOr[js.Array[Object]] = js.undefined,
    config: js.UndefOr[js.Object] = js.undefined,
    revision: js.UndefOr[Int] = js.undefined,
    onInitialized: js.UndefOr[js.Function2[Figure, dom.HTMLElement, Unit]] = js.undefined,
    onUpdate: js.UndefOr[js.Function2[Figure, dom.HTMLElement, Unit]] = js.undefined,
    onPurge: js.UndefOr[js.Function2[Figure, dom.HTMLElement, Unit]] = js.undefined,
    onError: js.UndefOr[js.Function1[js.Error, Unit]] = js.undefined,
    divId: js.UndefOr[String] = js.undefined,
    className: js.UndefOr[String] = js.undefined,
    style: js.UndefOr[js.Object] = js.undefined,
    debug: js.UndefOr[Boolean] = js.undefined,
    useResizeHandler: js.UndefOr[Boolean] = js.undefined,
    onClick: js.UndefOr[js.Function1[js.Any, Unit]] = js.undefined,
    onAfterPlot: js.UndefOr[js.Function1[js.Any, Unit]] = js.undefined,
    onSunburstClick: js.UndefOr[js.Function1[js.Any, Unit]] = js.undefined,
  )

  @js.native
  @JSImport("react-plotly.js", JSImport.Default)
  object Plot extends js.Object

  override val component = Plot
}

trait Figure extends js.Object {
  val data: js.Array[js.Object]
  val layout: js.Object
}
