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

  protected val quineApp: QueryUiConfigurationState

  def queryUiConfigurationRoutes: Route =
    queryUiSampleQueries.implementedByAsync(_ => quineApp.getSampleQueries) ~
    updateQueryUiSampleQueries.implementedByAsync(quineApp.setSampleQueries) ~
    queryUiQuickQueries.implementedByAsync(_ => quineApp.getQuickQueries) ~
    updateQueryUiQuickQueries.implementedByAsync(quineApp.setQuickQueries) ~
    queryUiAppearance.implementedByAsync(_ => quineApp.getNodeAppearances) ~
    updateQueryUiAppearance.implementedByAsync(quineApp.setNodeAppearances)
}
