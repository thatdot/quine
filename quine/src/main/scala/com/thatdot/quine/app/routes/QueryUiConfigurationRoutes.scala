package com.thatdot.quine.app.routes

import org.apache.pekko.http.scaladsl.server.Directives._
import org.apache.pekko.http.scaladsl.server.Route

import com.thatdot.quine.app.routes.exts.circe.JsonEntitiesFromSchemas
import com.thatdot.quine.routes.QueryUiConfigurationRoutes

trait QueryUiConfigurationRoutesImpl
    extends QueryUiConfigurationRoutes
    with endpoints4s.pekkohttp.server.Endpoints
    with JsonEntitiesFromSchemas
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
