package com.thatdot.quine.webapp.components
import com.raquo.laminar.api.L._

import com.thatdot.quine.webapp.components.RenderStrategy.{RenderAlwaysMountedPage, RenderRegularlyMountedPages}

object HybridViewsRenderer {
  def apply(
    alwaysRenderedView: HtmlElement,
    regularlyRenderedViews: Signal[HtmlElement],
    renderStrategy: Signal[RenderStrategy],
  ): HtmlElement = div(
    cls := "position-relative",
    flex := "1",
    div(
      cls := "h-100",
      position := "absolute",
      top := "0",
      left := "0",
      right := "0",
      bottom := "0",
      visibility <-- renderStrategy.map({
        case RenderAlwaysMountedPage => "visible"
        case RenderRegularlyMountedPages => "hidden"
      }),
      pointerEvents <-- renderStrategy.map({
        case RenderAlwaysMountedPage => "auto"
        case RenderRegularlyMountedPages => "none"
      }),
      alwaysRenderedView,
    ),
    div(
      cls := "h-100",
      display <-- renderStrategy.map({
        case RenderAlwaysMountedPage => "none"
        case RenderRegularlyMountedPages => "block"
      }),
      child <-- regularlyRenderedViews,
    ),
  )
}
