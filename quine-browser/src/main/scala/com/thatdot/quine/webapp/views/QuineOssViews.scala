package com.thatdot.quine.webapp.views

import com.raquo.laminar.api.L._
import com.raquo.waypoint.{Router, SplitRender}

import com.thatdot.quine.routes.ClientRoutes
import com.thatdot.quine.webapp.QuineUiOptions
import com.thatdot.quine.webapp.components.HybridViewsRenderer
import com.thatdot.quine.webapp.components.RenderStrategy.{RenderAlwaysMountedPage, RenderRegularlyMountedPages}
import com.thatdot.quine.webapp.queryui.QueryMethod
import com.thatdot.quine.webapp.router.QuineOssPage
import com.thatdot.quine.webapp.router.QuineOssPage._

class QuineOssViews(
  router: Router[QuineOssPage],
  routes: ClientRoutes,
  queryMethod: QueryMethod,
  options: QuineUiOptions,
) {
  val staticViews: Signal[HtmlElement] = SplitRender(router.currentPageSignal)
    .collectStatic(ExplorerUi)(div(): HtmlElement)
    .collectStatic(DocsV1)(div(): HtmlElement)
    .collectStatic(DocsV2)(div(): HtmlElement)
    .collectStatic(Metrics)(MetricsView(routes, queryMethod))
    .collectStatic(Streams)(StreamsView(options))
    .collectStatic(Landing)(LandingView(routes))
    .signal

  val alwaysMountedWrapper: HtmlElement = div(
    cls := "h-100",
    position := "relative",
    div(
      cls := "h-100",
      display <-- router.currentPageSignal.map {
        case ExplorerUi => "block"
        case _ => "none"
      },
      ExplorationUiView(options = options, routes = routes),
    ),
    div(
      position := "absolute",
      top := "0",
      left := "0",
      right := "0",
      bottom := "0",
      overflow := "auto",
      visibility <-- router.currentPageSignal.map {
        case DocsV1 => "visible"
        case _ => "hidden"
      },
      pointerEvents <-- router.currentPageSignal.map {
        case DocsV1 => "auto"
        case _ => "none"
      },
      DocsV1View(options),
    ),
    div(
      position := "absolute",
      top := "0",
      left := "0",
      right := "0",
      bottom := "0",
      overflow := "auto",
      visibility <-- router.currentPageSignal.map {
        case DocsV2 => "visible"
        case _ => "hidden"
      },
      pointerEvents <-- router.currentPageSignal.map {
        case DocsV2 => "auto"
        case _ => "none"
      },
      DocsV2View(options),
    ),
  )

  val views: HtmlElement =
    HybridViewsRenderer(
      alwaysRenderedView = alwaysMountedWrapper,
      regularlyRenderedViews = staticViews,
      renderStrategy = router.currentPageSignal.map({
        case ExplorerUi | DocsV1 | DocsV2 => RenderAlwaysMountedPage
        case _ => RenderRegularlyMountedPages
      }),
    )
}

object QuineOssViews {
  def apply(
    router: Router[QuineOssPage],
    routes: ClientRoutes,
    queryMethod: QueryMethod,
    options: QuineUiOptions,
  ): HtmlElement =
    (new QuineOssViews(router, routes, queryMethod, options)).views
}
