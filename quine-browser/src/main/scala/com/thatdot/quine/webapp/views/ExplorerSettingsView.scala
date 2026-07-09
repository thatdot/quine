package com.thatdot.quine.webapp.views

import com.raquo.laminar.api.L._

import com.thatdot.quine.routes.ClientRoutes
import com.thatdot.quine.webapp.queryui.ExplorerSettingsPage

object ExplorerSettingsView {
  def apply(
    routes: ClientRoutes,
    useV2Api: Boolean,
    enabledTapsVar: Var[Map[String, Set[String]]],
  ): HtmlElement =
    ExplorerSettingsPage(
      routes = routes,
      useV2Api = useV2Api,
      enabledTapsVar = enabledTapsVar,
    )
}
