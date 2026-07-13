package com.thatdot.quine.webapp.views

import com.raquo.laminar.api.L._
import com.raquo.waypoint.{Router, SplitRender}

import com.thatdot.quine.routes.ClientRoutes
import com.thatdot.quine.routes.exts.NamespaceParameter
import com.thatdot.quine.webapp.QuineUiOptions
import com.thatdot.quine.webapp.components.HybridViewsRenderer
import com.thatdot.quine.webapp.components.RenderStrategy.{RenderAlwaysMountedPage, RenderRegularlyMountedPages}
import com.thatdot.quine.webapp.queryui.QueryMethod.{Restful, WebSocket}
import com.thatdot.quine.webapp.queryui.{QueryMethod, TapQueryRuntime, WiretapStore}
import com.thatdot.quine.webapp.router.QuineOssPage
import com.thatdot.quine.webapp.router.QuineOssPage._
import com.thatdot.quine.webapp.v2api.V2ApiTypes

class QuineOssViews(
  router: Router[QuineOssPage],
  routes: ClientRoutes,
  wiretapStore: WiretapStore,
  queryMethod: QueryMethod,
  options: QuineUiOptions,
) {
  // Metadata for taps currently open in the store, read by QueryUi's dispatch host.
  // Populated by TapQueryRuntime alongside opening each handler.
  private val activeTapQueryMetadataVar: Var[Map[String, V2ApiTypes.V2TapQuery]] = Var(Map.empty)

  // "Enable locally" intent — which tap queries are enabled, per namespace. The single
  // source of truth: the Explorer Settings toggles mutate it, TapQueryRuntime persists it
  // to sessionStorage and applies it to the wiretap store. OSS has one graph, so the
  // runtime tracks the default namespace.
  private val enabledTapsVar: Var[Map[String, Set[String]]] = Var(Map.empty)
  private val defaultNamespace: String = NamespaceParameter.defaultNamespaceParameter.namespaceId

  val staticViews: Signal[HtmlElement] = SplitRender(router.currentPageSignal)
    .collectStatic(ExplorerUi)(div(): HtmlElement)
    .collectStatic(DocsV1)(div(): HtmlElement)
    .collectStatic(DocsV2)(div(): HtmlElement)
    .collectStatic(Metrics)(MetricsView(routes, queryMethod))
    .collectStatic(Streams)(StreamsView(options, wiretapStore))
    .collectStatic(Landing)(LandingView(routes))
    .collectStatic(ExplorerSettings)(
      ExplorerSettingsView(
        routes,
        useV2Api = queryMethod match {
          case Restful | WebSocket => false
          case _ => true
        },
        enabledTapsVar = enabledTapsVar,
      ),
    )
    .signal

  val alwaysMountedWrapper: HtmlElement = div(
    cls := "h-100",
    position := "relative",
    // Always-mounted: keeps enabled taps restored + firing on any page, not just Settings.
    TapQueryRuntime(
      routes = routes,
      currentNamespaceSignal = Val(Some(defaultNamespace)),
      wiretapStore = wiretapStore,
      enabledTapsVar = enabledTapsVar,
      activeTapQueryMetadataVar = activeTapQueryMetadataVar,
      // OSS has no auth, so the tap-query list is always readable.
      canReadTapQueries = true,
    ),
    div(
      cls := "h-100",
      display <-- router.currentPageSignal.map {
        case ExplorerUi => "block"
        case _ => "none"
      },
      ExplorationUiView(
        options = options,
        routes = routes,
        wiretapStore = wiretapStore,
        activeTapQueryMetadataVar = activeTapQueryMetadataVar,
      ),
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
    wiretapStore: WiretapStore,
    queryMethod: QueryMethod,
    options: QuineUiOptions,
  ): HtmlElement =
    (new QuineOssViews(router, routes, wiretapStore, queryMethod, options)).views
}
