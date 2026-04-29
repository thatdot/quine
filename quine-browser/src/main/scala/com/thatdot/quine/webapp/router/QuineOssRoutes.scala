package com.thatdot.quine.webapp.router

import com.raquo.waypoint._

import com.thatdot.quine.webapp.router.QuineOssPage._

class QuineOssRoutes(apiV1: Boolean, basePath: String) {
  private val explorationUiRoute: Route.Total[ExplorerUi.type, Unit] =
    Route.static(staticPage = ExplorerUi, pattern = root, basePath = basePath)

  private val docsV1Route: Route.Total[DocsV1.type, Unit] =
    Route.static(staticPage = DocsV1, pattern = root / "docs", basePath = basePath)
  private val docsV2Route: Route.Total[DocsV2.type, Unit] =
    Route.static(staticPage = DocsV2, pattern = root / "v2docs", basePath = basePath)

  private val metricsRoute: Route.Total[Metrics.type, Unit] =
    Route.static(staticPage = Metrics, pattern = root / "dashboard", basePath = basePath)

  private val streamsRoute: Route.Total[Streams.type, Unit] =
    Route.static(staticPage = Streams, pattern = root / "streams", basePath = basePath)

  private val landingRoute: Route.Total[Landing.type, Unit] =
    Route.static(staticPage = Landing, pattern = root / "home", basePath = basePath)

  val routes: List[Route.Total[_ <: QuineOssPage, Unit]] =
    List(explorationUiRoute, docsV1Route, docsV2Route, metricsRoute, landingRoute) ++
    (if (!apiV1) List(streamsRoute) else Nil)
}
