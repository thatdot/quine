package com.thatdot.quine.app.routes

import scala.util.{Failure, Success, Try}

import org.apache.pekko.http.scaladsl.model.{HttpEntity, HttpResponse, StatusCodes, Uri}
import org.apache.pekko.http.scaladsl.server.Directives._
import org.apache.pekko.http.scaladsl.server.{Directives, ExceptionHandler, Route}
import org.apache.pekko.util.Timeout

import com.typesafe.scalalogging.LazyLogging
import io.circe.Json
import org.webjars.WebJarAssetLocator

import com.thatdot.quine.app.BuildInfo
import com.thatdot.quine.graph._
import com.thatdot.quine.gremlin.GremlinQueryRunner
import com.thatdot.quine.model.QuineId

/** Main webserver routes for Quine
  *
  * This is responsible for serving up the REST API as well as static resources.
  *
  * @param graph underlying graph
  * @param quineApp quine application state
  * @param currentConfig rendered JSON config
  * @param uri The url from which these routes will be served (used for docs generation)
  * @param timeout timeout
  */
class QuineAppRoutes(
  val graph: LiteralOpsGraph with AlgorithmGraph with CypherOpsGraph with StandingQueryOpsGraph,
  val quineApp: AdministrationRoutesState with QueryUiConfigurationState with StandingQueryStore with IngestStreamState,
  val currentConfig: Json,
  val uri: Uri,
  val timeout: Timeout
) extends BaseAppRoutes
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
    namespacesUnsupportedRoute ~
    queryUiRoutes ~
    queryProtocolWS ~
    queryUiConfigurationRoutes ~
    debugRoutes ~
    algorithmRoutes ~
    administrationRoutes ~
    ingestRoutes ~
    standingQueryRoutes
  }
}
