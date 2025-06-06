package com.thatdot.quine.app.routes

import java.net.URL

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success, Try}

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.model.{HttpEntity, StatusCodes}
import org.apache.pekko.http.scaladsl.server.Directives._
import org.apache.pekko.http.scaladsl.server.{Directives, Route}
import org.apache.pekko.util.Timeout

import com.thatdot.quine.app.config.QuineConfig

import org.webjars.WebJarAssetLocator

import com.thatdot.common.logging.Log.{LazySafeLogging, LogConfig, SafeLoggableInterpolator}
import com.thatdot.common.quineid.QuineId
import com.thatdot.quine.app.config.BaseConfig
import com.thatdot.quine.app.routes.websocketquinepattern.WebSocketQuinePatternServer
import com.thatdot.quine.app.v2api.{OssApiMethods, V2OssRoutes}
import com.thatdot.quine.app.{BaseApp, BuildInfo, QuineApp}
import com.thatdot.quine.graph._
import com.thatdot.quine.gremlin.GremlinQueryRunner

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
    with StandingQueryStoreV1
    with IngestStreamState,
  val config: BaseConfig,
  val uri: URL,
  val timeout: Timeout,
  val apiV2Enabled: Boolean,
  val v2IngestEnabled: Boolean,
)(implicit val ec: ExecutionContext, protected val logConfig: LogConfig)
    extends BaseAppRoutes
    with QueryUiRoutesImpl
    with WebSocketQueryProtocolServer
    with QueryUiConfigurationRoutesImpl
    with DebugRoutesImpl
    with AlgorithmRoutesImpl
    with AdministrationRoutesImpl
    with IngestRoutesImpl
    with StandingQueryRoutesV1Impl
    with exts.ServerEntitiesWithExamples
    with com.thatdot.quine.routes.exts.CirceJsonAnySchema
    with LazySafeLogging {

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
  // Get HTML with possibly modified base href based on configured path
  private def getHtmlWithBaseHref(): Route = {
    val pathOpt = config match {
      case qc: QuineConfig if qc.webserverAdvertise.exists(_.path.isDefined) => 
        qc.webserverAdvertise.flatMap(_.path)
      case _ => None
    }
    
    // If path is defined, serve modified HTML, otherwise serve original
    pathOpt match {
      case Some(path) =>
        val baseHref = if (path.isEmpty || path.endsWith("/")) path else path + "/"
        
        // Get resource as string
        val htmlResource = scala.io.Source.fromInputStream(
          getClass.getResourceAsStream("/web/quine-ui.html")
        ).mkString
        
        // Modify the base href
        val modifiedHtml = htmlResource.replaceFirst(
          """<base href=".*?"(.*?)>""", 
          s"""<base href="$baseHref"$$1>"""
        )
        
        // Serve the modified content
        import org.apache.pekko.http.scaladsl.model.ContentTypes
        complete(HttpEntity(ContentTypes.`text/html(UTF-8)`, modifiedHtml))
        
      case None =>
        // Serve the original file without modifications
        getFromResource("web/quine-ui.html")
    }
  }

  lazy val staticFilesRoute: Route = {
    Directives.pathEndOrSingleSlash {
      getHtmlWithBaseHref()
    } ~
    Directives.path("dashboard" | "docs" | "v2docs") {
      getHtmlWithBaseHref()
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

    val enableLanguageServerRoute: Boolean = sys.props.get("ls.enabled").flatMap(_.toBooleanOption).getOrElse(false)

    val v1Routes = {
      namespacesUnsupportedRoute ~
      queryUiRoutes ~
      queryProtocolWS ~
      (if (enableLanguageServerRoute) webSocketQuinePatternServer.languageServerWebsocketRoute else reject) ~
      queryUiConfigurationRoutes ~
      debugRoutes ~
      algorithmRoutes ~
      administrationRoutes ~
      ingestRoutes ~
      standingQueryRoutes
    }

    if (apiV2Enabled) {
      val v2Route = new V2OssRoutes(
        new OssApiMethods(graph.asInstanceOf[GraphService], quineApp.asInstanceOf[QuineApp], config, timeout),
      ).v2Routes(ingestOnly = false)
      logger.warn(safe"Starting with Api V2 endpoints enabled")
      v1Routes ~ v2Route
    } else if (v2IngestEnabled) {
      val v2Route = new V2OssRoutes(
        new OssApiMethods(graph.asInstanceOf[GraphService], quineApp.asInstanceOf[QuineApp], config, timeout),
      ).v2Routes(ingestOnly = true)
      logger.warn(safe"Starting with V2 ingest enabled")
      v1Routes ~ v2Route
    } else {
      v1Routes
    }

  }
}
