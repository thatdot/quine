package com.thatdot.quine.app.routes

import org.apache.pekko.http.scaladsl.server.Directives._
import org.apache.pekko.http.scaladsl.server.Route

import com.thatdot.quine.app.routes.exts.circe.JsonEntitiesFromSchemas
import com.thatdot.quine.graph.BaseGraph
import com.thatdot.quine.routes.QueryUiConfigurationRoutes

trait QueryUiConfigurationRoutesImpl
    extends QueryUiConfigurationRoutes
    with endpoints4s.pekkohttp.server.Endpoints
    with JsonEntitiesFromSchemas
    with exts.ServerQuineEndpoints {

  protected val quineApp: QueryUiConfigurationState

  val graph: BaseGraph

  def queryUiConfigurationRoutes: Route =
    queryUiSampleQueries.implementedByAsync(_ => graph.requiredGraphIsReadyFuture(quineApp.getSampleQueries)) ~
    updateQueryUiSampleQueries.implementedByAsync(q => graph.requiredGraphIsReadyFuture(quineApp.setSampleQueries(q))) ~
    queryUiQuickQueries.implementedByAsync(_ => graph.requiredGraphIsReadyFuture(quineApp.getQuickQueries)) ~
    updateQueryUiQuickQueries.implementedByAsync(q => graph.requiredGraphIsReadyFuture(quineApp.setQuickQueries(q))) ~
    queryUiAppearance.implementedByAsync(_ => graph.requiredGraphIsReadyFuture(quineApp.getNodeAppearances)) ~
    updateQueryUiAppearance.implementedByAsync(q => graph.requiredGraphIsReadyFuture(quineApp.setNodeAppearances(q)))
}
