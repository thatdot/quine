package com.thatdot.quine.webapp2.components
import com.raquo.laminar.api.L._

import com.thatdot.quine.webapp2.components.RenderStrategy.{RenderAlwaysMountedPage, RenderRegularlyMountedPages}

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
      display <-- renderStrategy.map({
        case RenderAlwaysMountedPage => "block"
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
