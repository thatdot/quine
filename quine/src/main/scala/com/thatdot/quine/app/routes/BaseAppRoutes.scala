package com.thatdot.quine.app.routes

import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future}

import akka.http.scaladsl.Http
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{Route, StandardRoute}
import akka.stream.Materializer
import akka.util.Timeout

import com.typesafe.scalalogging.LazyLogging

import com.thatdot.quine.graph.BaseGraph
import com.thatdot.quine.model.QuineIdProvider

trait BaseAppRoutes extends LazyLogging with endpoints4s.akkahttp.server.Endpoints {

  val graph: BaseGraph

  val ec: ExecutionContext

  val timeout: Timeout

  implicit def idProvider: QuineIdProvider = graph.idProvider
  implicit lazy val materializer: Materializer = graph.materializer

  override def handleServerError(throwable: Throwable): StandardRoute = {
    logger.error("Uncaught exception when handling HTTP request", throwable)
    super.handleServerError(throwable)
  }

  def isLive = true
  def isReady = graph.isReady
  val nodeTitlePropKeys = List.empty // TODO?

  /** Serves up the static assets from resources and for JS/CSS dependencies */
  def staticFilesRoute: Route

  /** OpenAPI route */
  def openApiRoute: Route

  /** Rest API route */
  def apiRoute: Route

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
