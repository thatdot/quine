package com.thatdot.quine.app.routes

import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

import akka.http.scaladsl.Http
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{Directives, Route, StandardRoute}
import akka.stream.Materializer
import akka.util.Timeout

import com.typesafe.scalalogging.LazyLogging
import org.webjars.WebJarAssetLocator

import com.thatdot.quine.app.{BuildInfo, Config}
import com.thatdot.quine.graph._
import com.thatdot.quine.gremlin.GremlinQueryRunner
import com.thatdot.quine.model.{QuineId, QuineIdProvider}

/** Main webserver routes for Quine
  *
  * This is responsible for serving up the REST API as well as static resources.
  *
  * @param graph underlying graph
  * @param serviceState quine application state
  * @param ec execution context
  * @param timeout timeout
  */
class QuineAppRoutes(
  val graph: LiteralOpsGraph with CypherOpsGraph with StandingQueryOpsGraph,
  val serviceState: AdministrationRoutesState
    with QueryUiConfigurationState
    with StandingQueryStore
    with IngestStreamState,
  val ec: ExecutionContext,
  val timeout: Timeout
) extends QueryUiRoutesImpl
    with WebSocketQueryProtocolServer
    with QueryUiConfigurationRoutesImpl
    with LiteralRoutesImpl
    with AdministrationRoutesImpl
    with IngestRoutesImpl
    with StandingQueryRoutesImpl
    with exts.ServerEntitiesWithExamples
    with com.thatdot.quine.routes.exts.UjsonAnySchema
    with LazyLogging {

  implicit def idProvider: QuineIdProvider = graph.idProvider
  implicit lazy val materializer: Materializer = graph.materializer

  override def hostIndex(qid: QuineId): Int = 0

  override def handleServerError(throwable: Throwable): StandardRoute = {
    logger.error("Uncaught exception when handling HTTP request", throwable)
    super.handleServerError(throwable)
  }

  val version = BuildInfo.version
  val currentConfig = Config.loadedConfigJson
  def isLive = true
  def isReady = graph.isReady
  val nodeTitlePropKeys = List.empty // TODO?
  val gremlin: GremlinQueryRunner = GremlinQueryRunner(graph)(timeout)

  val webJarAssetLocator = new WebJarAssetLocator()

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
    Directives.path("favicon.svg") {
      getFromResource("web/favicon.svg")
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
  lazy val openApiRoute: Route = QuineAppOpenApiDocsRoutes(graph).route

  /** Rest API route */
  lazy val apiRoute: Route = {
    queryUiRoutes ~
    queryProtocolWS ~
    queryUiConfigurationRoutes ~
    literalRoutes ~
    administrationRoutes ~
    ingestRoutes ~
    standingQueryRoutes
  }

  /** Final HTTP route */
  def mainRoute: Route =
    Util.xssHarden(staticFilesRoute) ~
    redirectToNoTrailingSlashIfPresent(StatusCodes.PermanentRedirect) {
      apiRoute ~
      respondWithHeader(`Access-Control-Allow-Origin`.*) {
        // NB the following resources will be available to request from ANY source (including evilsite.com):
        // be sure this is what you want!
        openApiRoute
      }
    }

  /** Bind a webserver to server up the main route */
  def bindWebServer(interface: String, port: Int): Future[Http.ServerBinding] = {
    implicit val sys = graph.system
    val route = mainRoute
    Http()
      .newServerAt(interface, port)
      .adaptSettings(_.mapWebsocketSettings(_.withPeriodicKeepAliveMaxIdle(10.seconds)))
      .bind(route)
  }
}
