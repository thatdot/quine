package com.thatdot.quine.webapp.views

import com.raquo.laminar.api.L._

import com.thatdot.quine.routes.ClientRoutes
import com.thatdot.quine.webapp.QueryUiOptions
import com.thatdot.quine.webapp.queryui.QueryUi

object ExplorationUiView {
  def apply(options: QueryUiOptions, routes: ClientRoutes): HtmlElement =
    QueryUi.fromOptions(options, routes)
}
