package com.thatdot.quine.webapp.views

import com.raquo.laminar.api.L._

import com.thatdot.quine.routes.ClientRoutes
import com.thatdot.quine.webapp.components.dashboard.MetricsDashboard
import com.thatdot.quine.webapp.queryui.QueryMethod

object MetricsView {
  def apply(routes: ClientRoutes, queryMethod: QueryMethod): HtmlElement =
    MetricsDashboard(routes, queryMethod)
}
