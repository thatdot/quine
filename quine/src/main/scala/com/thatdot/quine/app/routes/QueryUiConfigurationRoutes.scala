package com.thatdot.quine.app.routes

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route

import com.thatdot.quine.routes.QueryUiConfigurationRoutes

trait QueryUiConfigurationRoutesImpl
    extends QueryUiConfigurationRoutes
    with endpoints4s.akkahttp.server.Endpoints
    with endpoints4s.akkahttp.server.JsonEntitiesFromSchemas
    with exts.ServerQuineEndpoints {

  protected val serviceState: QueryUiConfigurationState

  def queryUiConfigurationRoutes: Route =
    queryUiSampleQueries.implementedByAsync(_ => serviceState.getSampleQueries) ~
    updateQueryUiSampleQueries.implementedByAsync(serviceState.setSampleQueries) ~
    queryUiQuickQueries.implementedByAsync(_ => serviceState.getQuickQueries) ~
    updateQueryUiQuickQueries.implementedByAsync(serviceState.setQuickQueries) ~
    queryUiAppearance.implementedByAsync(_ => serviceState.getNodeAppearances) ~
    updateQueryUiAppearance.implementedByAsync(serviceState.setNodeAppearances)
}
