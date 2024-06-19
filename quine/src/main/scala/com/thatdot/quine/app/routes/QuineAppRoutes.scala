package com.thatdot.quine.app.routes

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success, Try}

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.model.{HttpEntity, StatusCodes, Uri}
import org.apache.pekko.http.scaladsl.server.Directives._
import org.apache.pekko.http.scaladsl.server.{Directives, Route}
import org.apache.pekko.util.Timeout

import com.typesafe.scalalogging.LazyLogging
import org.webjars.WebJarAssetLocator

import com.thatdot.quine.app.config.BaseConfig
import com.thatdot.quine.app.routes.websocketquinepattern.WebSocketQuinePatternServer
import com.thatdot.quine.app.v2api.{OssApiInterface, V2OssRoutes}
import com.thatdot.quine.app.{BaseApp, BuildInfo, QuineApp}
import com.thatdot.quine.graph._
import com.thatdot.quine.gremlin.GremlinQueryRunner
import com.thatdot.quine.model.QuineId

/** Main webserver routes for Quine
  *
  * This is responsible for serving up the REST API as well as static resources.
  *
  * @param graph underlying graph
  * @param quineApp quine application state
  * @param config current application config
  * @param uri The url from which these routes will be served (used for docs generation)
  * @param timeout timeout
  */
class QuineAppRoutes(
  val graph: LiteralOpsGraph with AlgorithmGraph with CypherOpsGraph with StandingQueryOpsGraph,
  val quineApp: BaseApp
    with AdministrationRoutesState
    with QueryUiConfigurationState
    with StandingQueryStore
    with IngestStreamState,
  val config: BaseConfig,
  val uri: Uri,
  val timeout: Timeout,
  val apiV2Enabled: Boolean
)(implicit val ec: ExecutionContext)
    extends BaseAppRoutes
    with QueryUiRoutesImpl
    with WebSocketQueryProtocolServer
    with QueryUiConfigurationRoutesImpl
    with DebugRoutesImpl
    with AlgorithmRoutesImpl
    with AdministrationRoutesImpl
    with IngestRoutesImpl
    with StandingQueryRoutesImpl
    with exts.ServerEntitiesWithExamples
    with com.thatdot.quine.routes.exts.CirceJsonAnySchema
    with LazyLogging {

  //
  //override val app: BaseApp with StandingQueryStore with IngestStreamState = ???
  implicit val system: ActorSystem = graph.system

  val currentConfig = config.loadedConfigJson
  private val webSocketQuinePatternServer = new WebSocketQuinePatternServer(system)

  val version = BuildInfo.version
  val gremlin: GremlinQueryRunner = GremlinQueryRunner(graph)(timeout)

  val webJarAssetLocator = new WebJarAssetLocator()

  override def hostIndex(qid: QuineId): Int = 0

  override def namespaceExists(namespace: String): Boolean =
    graph.getNamespaces.contains(namespaceFromString(namespace))

  /** Serves up the static assets from resources and for JS/CSS dependencies */
  lazy val staticFilesRoute: Route = {
    Directives.pathEndOrSingleSlash {
      getFromResource("web/quine-ui.html")
    } ~
    Directives.path("dashboard" | "docs") {
      getFromResource("web/quine-ui.html")
    } ~
    Directives.path("quine-ui-startup.js") {
      getFromResource("web/quine-ui-startup.js")
    } ~
    Directives.path("browserconfig.xml") {
      getFromResource("web/browserconfig.xml")
    } ~
    Directives.path("favicon.svg") {
      redirect("favicon.ico", StatusCodes.PermanentRedirect)
    } ~
    Directives.path("favicon.ico") {
      getFromResource("web/favicon.ico")
    } ~
    Directives.path("apple-touch-icon.png") {
      getFromResource("web/apple-touch-icon.png")
    } ~
    Directives.path("favicon-32x32.png") {
      getFromResource("web/favicon-32x32.png")
    } ~
    Directives.path("favicon-16x16.png") {
      getFromResource("web/favicon-16x16.png")
    } ~
    Directives.path("site.webmanifest") {
      getFromResource("web/site.webmanifest")
    } ~
    Directives.path("safari-pinned-tab.svg") {
      getFromResource("web/safari-pinned-tab.svg")
    } ~
    Directives.extractUnmatchedPath { path =>
      Try(webJarAssetLocator.getFullPath(path.toString)) match {
        case Success(fullPath) => getFromResource(fullPath)
        case Failure(_: IllegalArgumentException) => reject
        case Failure(err) => failWith(err)
      }
    }
  }

  /** OpenAPI route */
  lazy val openApiRoute: Route = QuineAppOpenApiDocsRoutes(graph, uri).route

  private val namespacesUnsupportedRoute =
    parameter("namespace")(_ => complete(StatusCodes.BadRequest, HttpEntity("Namespaces not supported")))

  /** Rest API route */
  lazy val apiRoute: Route = {

    val v1Routes = {
      namespacesUnsupportedRoute ~
      queryUiRoutes ~
      queryProtocolWS ~
      webSocketQuinePatternServer.languageServerWebsocketRoute ~
      queryUiConfigurationRoutes ~
      debugRoutes ~
      algorithmRoutes ~
      administrationRoutes ~
      ingestRoutes ~
      standingQueryRoutes
    }

    if (apiV2Enabled) {
      val v2Route = new V2OssRoutes(
        new OssApiInterface(graph.asInstanceOf[GraphService], quineApp.asInstanceOf[QuineApp], config, timeout)
      ).v2Routes
      logger.warn("Starting with Api V2 endpoints enabled")
      v1Routes ~ v2Route
    } else {
      v1Routes
    }

  }
}
