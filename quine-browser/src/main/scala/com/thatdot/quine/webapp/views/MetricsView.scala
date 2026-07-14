package com.thatdot.quine.webapp.views

import com.raquo.laminar.api.L._

import com.thatdot.quine.webapp.components.dashboard.MetricsDashboard
import com.thatdot.quine.webapp.dataservice.DataService

object MetricsView {
  def apply(dataService: DataService): HtmlElement =
    MetricsDashboard(dataService)
}
