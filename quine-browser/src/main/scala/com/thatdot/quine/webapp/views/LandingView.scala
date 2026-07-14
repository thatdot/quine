package com.thatdot.quine.webapp.views

import com.raquo.laminar.api.L._

import com.thatdot.quine.webapp.components.landing.LandingPage
import com.thatdot.quine.webapp.dataservice.DataService

/** View wiring for the landing page.
  *
  * Binds the shared [[DataService]] signals into the page component.
  * OSS has no auth, so no permission set is threaded through.
  */
object LandingView {
  def apply(dataService: DataService): HtmlElement =
    LandingPage(
      metricsSignal = dataService.hostMetricsSignal(None),
      backpressureSignal = dataService.backpressureSignal,
    )
}
