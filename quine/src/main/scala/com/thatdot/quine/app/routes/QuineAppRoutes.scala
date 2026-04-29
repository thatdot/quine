package com.thatdot.quine.app.routes

import java.net.URL
import java.util.concurrent.atomic.AtomicBoolean

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success, Try}

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.model.headers.RawHeader
import org.apache.pekko.http.scaladsl.model.{HttpEntity, StatusCodes}
import org.apache.pekko.http.scaladsl.server.Directives._
import org.apache.pekko.http.scaladsl.server.{Directive0, Directives, Route, RouteResult}
import org.apache.pekko.util.Timeout

import org.webjars.WebJarAssetLocator

import com.thatdot.common.logging.Log.{LazySafeLogging, LogConfig, Safe, SafeLoggableInterpolator}
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

  lazy val staticFilesRoute: Route = {
    Directives.pathEndOrSingleSlash {
      // RFC 8631 — point clients (including AI agents) at the OpenAPI spec for the latest API version.
      Directives.respondWithHeader(RawHeader("Link", "</openapi.json>; rel=\"service-desc\"")) {
        getFromResource("web/quine-ui.html")
      }
    } ~
    Directives.path("dashboard" | "docs" | "v1docs" | "home" | "streams") {
      getFromResource("web/quine-ui.html")
    } ~
    Directives.path("quine-ui-startup.js") {
      getJsWithInjectedConfig("web/quine-ui-startup.js", config.defaultApiVersion == "v2")
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

  /** Set on the first request that an actual V1 route handles, so the deprecation warning fires
    * once per process when V1 is genuinely in use rather than on every startup.
    */
  private val v1DeprecationWarned: AtomicBoolean = new AtomicBoolean(false)

  private val warnFirstV1Use: Directive0 = extractRequest.flatMap { req =>
    mapRouteResult { result =>
      result match {
        case _: RouteResult.Complete if v1DeprecationWarned.compareAndSet(false, true) =>
          logger.warn(
            safe"API V1 is in use (first hit on ${Safe(req.uri.path.toString)}); " +
            safe"it is planned for deprecation and will be removed in a future release.",
          )
        case _ =>
      }
      result
    }
  }

  /** Rest API route */
  lazy val apiRoute: Route = {

    val enableLanguageServerRoute: Boolean = sys.props.get("ls.enabled").flatMap(_.toBooleanOption).getOrElse(false)

    val v1Routes = warnFirstV1Use {
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

    // Always serve both V1 and V2 routes
    val v2Route = new V2OssRoutes(
      new OssApiMethods(graph.asInstanceOf[GraphService], quineApp.asInstanceOf[QuineApp], config, timeout),
      openApiServerUrl = uri.toString,
    ).v2Routes(ingestOnly = false)

    logger.info(safe"API V1 and V2 endpoints available (UI default: ${Safe(config.defaultApiVersion)})")
    v1Routes ~ v2Route
  }
}
