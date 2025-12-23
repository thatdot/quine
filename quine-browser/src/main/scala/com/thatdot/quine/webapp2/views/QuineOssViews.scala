package com.thatdot.quine.webapp2.views

import com.raquo.laminar.api.L._
import com.raquo.waypoint.{Router, SplitRender}

import com.thatdot.quine.routes.ClientRoutes
import com.thatdot.quine.webapp.QuineUiOptions
import com.thatdot.quine.webapp.queryui.QueryMethod
import com.thatdot.quine.webapp2.components.HybridViewsRenderer
import com.thatdot.quine.webapp2.components.RenderStrategy.{RenderAlwaysMountedPage, RenderRegularlyMountedPages}
import com.thatdot.quine.webapp2.router.QuineOssPage
import com.thatdot.quine.webapp2.router.QuineOssPage._

class QuineOssViews(
  router: Router[QuineOssPage],
  routes: ClientRoutes,
  queryMethod: QueryMethod,
  options: QuineUiOptions,
) {
  val staticViews: Signal[HtmlElement] = SplitRender(router.currentPageSignal)
    .collectStatic(ExplorerUi)(div(): HtmlElement)
    .collectStatic(DocsV1)(DocsV1View(options))
    .collectStatic(DocsV2)(DocsV2View(options))
    .collectStatic(Metrics)(MetricsView(routes, queryMethod))
    .signal

  val views: HtmlElement =
    HybridViewsRenderer(
      alwaysRenderedView = ExplorationUiView(options = options, routes = routes),
      regularlyRenderedViews = staticViews,
      renderStrategy = router.currentPageSignal.map({
        case ExplorerUi => RenderAlwaysMountedPage
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
