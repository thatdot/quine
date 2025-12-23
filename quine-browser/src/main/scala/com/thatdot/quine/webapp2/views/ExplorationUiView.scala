package com.thatdot.quine.webapp2.views

import com.raquo.laminar.api.L._

import com.thatdot.quine.routes.ClientRoutes
import com.thatdot.quine.webapp.{QueryUiOptions, makeQueryUi}
import com.thatdot.quine.webapp2.components.MountReactComponent

object ExplorationUiView {
  def apply(options: QueryUiOptions, routes: ClientRoutes): HtmlElement =
    MountReactComponent(
      makeQueryUi(options = options, routes = routes),
    )
}
