package com.thatdot.quine.webapp.views

import com.raquo.laminar.api.L._

import com.thatdot.quine.routes.ClientRoutes
import com.thatdot.quine.webapp.QueryUiOptions
import com.thatdot.quine.webapp.queryui.{QueryUi, WiretapStore}
import com.thatdot.quine.webapp.v2api.V2ApiTypes

object ExplorationUiView {
  def apply(
    options: QueryUiOptions,
    routes: ClientRoutes,
    wiretapStore: WiretapStore,
    activeTapQueryMetadataVar: Var[Map[String, V2ApiTypes.V2TapQuery]],
  ): HtmlElement =
    QueryUi.fromOptions(
      options,
      routes,
      wiretapStore = wiretapStore,
      externalActiveTapQueryMetadataVar = Some(activeTapQueryMetadataVar),
    )
}
