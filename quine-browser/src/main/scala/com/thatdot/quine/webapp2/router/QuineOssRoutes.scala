package com.thatdot.quine.webapp2.router

import com.raquo.waypoint._

import com.thatdot.quine.webapp2.router.QuineOssPage._

class QuineOssRoutes(apiV1: Boolean) {
  private val explorationUiRoute: Route.Total[ExplorerUi.type, Unit] =
    Route.static(staticPage = ExplorerUi, pattern = root / endOfSegments)

  private val docsV1Route: Route.Total[DocsV1.type, Unit] =
    Route.static(staticPage = DocsV1, pattern = root / "docs" / endOfSegments)
  private val docsV2Route: Route.Total[DocsV2.type, Unit] =
    Route.static(staticPage = DocsV2, pattern = root / "v2docs" / endOfSegments)

  private val metricsRoute: Route.Total[Metrics.type, Unit] =
    Route.static(staticPage = Metrics, pattern = root / "dashboard" / endOfSegments)

  val routes: List[Route.Total[_ <: QuineOssPage, Unit]] =
    List(explorationUiRoute, docsV1Route, docsV2Route, metricsRoute)
}
