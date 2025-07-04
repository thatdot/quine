package com.thatdot.quine

import scala.scalajs.js
import scala.scalajs.js.annotation.{JSExportTopLevel, JSImport}

import org.scalajs.dom
import slinky.core.KeyAndRefAddingStage
import slinky.core.facade.{ReactElement, ReactInstance}

import com.thatdot.quine.Util.{DashboardIcon, DocumentationIcon, ExplorerIcon}
import com.thatdot.quine.routes.ClientRoutes
import com.thatdot.quine.webapp.components.{MetricsDashboard, PageWithSideBar, StoplightElements, Tab, VisData}
import com.thatdot.quine.webapp.queryui.{NetworkLayout, QueryMethod, QueryUi}
import com.thatdot.{visnetwork => vis}

package object webapp {

  @JSImport("bootstrap/dist/css/bootstrap.min.css", JSImport.Namespace)
  @js.native
  object BootstrapCSS extends js.Object
  BootstrapCSS

  @JSImport("bootstrap/dist/js/bootstrap.bundle.min.js", JSImport.Namespace)
  @js.native
  object BootstrapBundle extends js.Object
  BootstrapBundle

  @JSImport("resources/index.css", JSImport.Default)
  @js.native
  object IndexCss extends js.Object
  locally(IndexCss) // something has to use this for it to actually load

  @js.native
  @JSImport("QuineInteractiveTS", "InteractiveClient")
  def InteractiveClient(): ReactElement = js.native

  /** Make a Query UI
    *
    * @param options configuration for the UI
    * @param routes API client
    * @return react instance of the Query UI
    */
  def makeQueryUi(options: QueryUiOptions, routes: ClientRoutes): KeyAndRefAddingStage[QueryUi.Def] = {
    val nodeSet = options.visNodeSet.getOrElse(new vis.DataSet(js.Array[vis.Node]()))
    val edgeSet = options.visEdgeSet.getOrElse(new vis.DataSet(js.Array[vis.Edge]()))
    val visData = new vis.Data {
      override val nodes = nodeSet
      override val edges = edgeSet
    }
    val useWs = options.queriesOverWs.getOrElse(true)

    QueryUi(
      routes = routes,
      graphData = VisData(visData, nodeSet, edgeSet),
      initialQuery = options.initialQuery.getOrElse(""),
      nodeResultSizeLimit = options.nodeResultSizeLimit.getOrElse(100).toLong,
      onNetworkCreate = options.onNetworkCreate.toOption,
      isQueryBarVisible = options.isQueryBarVisible.getOrElse(true),
      showEdgeLabels = options.showEdgeLabels.getOrElse(true),
      showHostInTooltip = options.showHostInTooltip.getOrElse(true),
      initialAtTime = options.queryHistoricalTime.toOption.map(_.toLong),
      initialLayout = options.layout.getOrElse("graph").toLowerCase match {
        case "tree" => NetworkLayout.Tree
        case "graph" | _ => NetworkLayout.Graph
      },
      queryMethod = if (useWs) QueryMethod.WebSocket else QueryMethod.Restful,
      isQuineOSS = options.isQuineOSS,
    )
  }

  /** Mount the Quine web app onto the DOM
    *
    * @param target DOM element onto which the webapp is mounted
    * @param options configuration options
    */
  @JSExportTopLevel("quineAppMount")
  def quineAppMount(target: dom.Element, options: QuineUiOptions): ReactInstance = {
    val clientRoutes = new ClientRoutes(options.serverUrl)
    val component = if (!options.isQueryBarVisible.getOrElse(true)) {
      makeQueryUi(options, clientRoutes)
    } else {
      PageWithSideBar(
        Tab(ExplorerIcon, "Graph Explorer", "/", makeQueryUi(options, clientRoutes), options.baseURI),
        Tab(
          DocumentationIcon,
          "Interactive Docs",
          "/docs",
          StoplightElements(
            apiDescriptionUrl = options.documentationUrl,
            basePath = "/docs",
            logo = options.baseURI + (if (options.isQuineOSS) "favicon.ico" else "thatdot-t.ico"),
          ),
          options.baseURI,
        ),
        Tab(
          DocumentationIcon,
          "Interactive Docs v2",
          "/v2docs",
          StoplightElements(
            apiDescriptionUrl = options.documentationV2Url,
            basePath = "/v2docs",
            logo = options.baseURI + (if (options.isQuineOSS) "favicon.ico" else "thatdot-t.ico"),
          ),
          options.baseURI,
          hidden = true,
        ),
        Tab(DashboardIcon, "System Dashboard", "/dashboard", MetricsDashboard(clientRoutes), options.baseURI),
        // Tab(ExplorerIcon, "Interactive Client - TS", "/client-ts", InteractiveClient(), options.baseURI)
      )
    }

    slinky.web.ReactDOM.render(component, target)
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

    /** Boolean flagging which product is being launched */
    val isQuineOSS: Boolean
  }
}
