package com.thatdot.quine

import scala.scalajs.js
import scala.scalajs.js.annotation.{JSExportTopLevel, JSImport}

import com.raquo.laminar.api.L._
import org.scalajs.dom

import com.thatdot.quine.routes.ClientRoutes
import com.thatdot.quine.webapp.LaminarRoot.LaminarRootProps
import com.thatdot.quine.webapp.queryui.QueryMethod
import com.thatdot.quine.webapp.router.QuineOssRouter
import com.thatdot.quine.webapp.views.QuineOssViews
import com.thatdot.{visnetwork => vis}

package object webapp {

  @JSImport("@fontsource-variable/inter/index.css", JSImport.Namespace)
  @js.native
  object InterFontCSS extends js.Object
  InterFontCSS

  @JSImport("@coreui/coreui/dist/css/coreui.min.css", JSImport.Namespace)
  @js.native
  object CoreuiCSS extends js.Object
  CoreuiCSS

  @JSImport("@coreui/icons/css/free.min.css", JSImport.Namespace)
  @js.native
  object CoreuiIconsCSS extends js.Object
  CoreuiIconsCSS

  @JSImport("@coreui/coreui/dist/js/coreui.bundle.min.js", JSImport.Namespace)
  @js.native
  object CoreuiBundle extends js.Object
  CoreuiBundle

  @JSImport("resources/index.css", JSImport.Default)
  @js.native
  object IndexCss extends js.Object
  locally(IndexCss) // something has to use this for it to actually load

  @JSImport("resources/logo.svg", JSImport.Default)
  @js.native
  object QuineLogo extends js.Object

  @JSImport("resources/logo-icon.svg", JSImport.Default)
  @js.native
  object QuineIcon extends js.Object

  /** Mount the Quine web app onto the DOM
    *
    * @param target DOM element onto which the webapp is mounted
    * @param options configuration options
    */
  @JSExportTopLevel("quineAppMount")
  def quineAppMount(target: dom.Element, options: QuineUiOptions): RootNode = {
    val clientRoutes = new ClientRoutes(options.serverUrl)
    val queryMethod = QueryMethod.parseQueryMethod(options)
    val apiV1 = queryMethod match {
      case QueryMethod.Restful | QueryMethod.WebSocket => true
      case QueryMethod.RestfulV2 | QueryMethod.WebSocketV2 => false
    }

    val basePath = options.baseURI.replaceAll("/$", "")
    val router = QuineOssRouter(apiV1, basePath)
    val laminarRoot = LaminarRoot(
      LaminarRootProps(
        productName = "Quine",
        logo = Some(
          span(
            img(src := QuineLogo.toString, alt := "Quine", cls := "sidebar-brand-full"),
            img(src := QuineIcon.toString, alt := "Quine", cls := "sidebar-brand-narrow"),
          ),
        ),
        navItems = QuineOssNavItems(apiV1),
        router = router,
        views = QuineOssViews(
          router,
          clientRoutes,
          queryMethod,
          options = options,
        ),
        userAvatar = None,
      ),
    )

    render(target, laminarRoot)
  }
}

package webapp {

  /** Configuration for making an instance of the Quine UI */
  trait QuineUiOptions extends QueryUiOptions {

    /** URL for loading the OpenAPI documentation API v1 */
    val documentationUrl: String

    /** URL for loading the OpenAPI documentation for API v2 */
    val documentationV2Url: String

    /** Initial baseURI of page */
    val baseURI: String
  }

  /** Configuration for making an instance of the Query UI */
  trait QueryUiOptions extends js.Object {

    /** initial query for the query bar */
    val initialQuery: js.UndefOr[String] = js.undefined

    /** maximum number of nodes to render without user confirmation * */
    val nodeResultSizeLimit: js.UndefOr[Int] = js.undefined

    /** mutable `vis` set of nodes (pass this in if you want a reference to it) */
    val visNodeSet: js.UndefOr[vis.DataSet[vis.Node]] = js.undefined

    /** mutable `vis` set of edges (pass this in if you want a reference to it) */
    val visEdgeSet: js.UndefOr[vis.DataSet[vis.Edge]] = js.undefined

    /** where should REST API calls be sent? */
    val serverUrl: js.UndefOr[String] = js.undefined

    /** should the query bar be visible? */
    val isQueryBarVisible: js.UndefOr[Boolean] = js.undefined

    /** should we run queries over a WebSocket connection or with multiple REST API calls */
    val queriesOverWs: js.UndefOr[Boolean] = js.undefined

    /** should we use API v2 REST endpoints instead of v1 when not using WebSocket */
    val queriesOverV2Api: js.UndefOr[Boolean] = js.undefined

    /** should the layout be in tree form or graph? */
    val layout: js.UndefOr[String] = js.undefined

    /** should edge labels be displayed (default: yes)? */
    val showEdgeLabels: js.UndefOr[Boolean] = js.undefined

    /** include "Served from Host" (default: yes)? */
    val showHostInTooltip: js.UndefOr[Boolean] = js.undefined

    /** historical millisecond unix time to query (`undefined` means the present) */
    val queryHistoricalTime: js.UndefOr[Int] = js.undefined

    /** call this when creating a `vis` network */
    val onNetworkCreate: js.UndefOr[js.Function1[vis.Network, js.Any]] = js.undefined

  }
}
