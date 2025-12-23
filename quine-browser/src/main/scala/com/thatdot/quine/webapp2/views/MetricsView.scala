package com.thatdot.quine.webapp2.views

import com.raquo.laminar.api.L._

import com.thatdot.quine.routes.ClientRoutes
import com.thatdot.quine.webapp.components.MetricsDashboard
import com.thatdot.quine.webapp.queryui.QueryMethod
import com.thatdot.quine.webapp2.components.MountReactComponent

object MetricsView {
  def apply(routes: ClientRoutes, queryMethod: QueryMethod): HtmlElement =
    MountReactComponent(MetricsDashboard(routes, queryMethod))
}
