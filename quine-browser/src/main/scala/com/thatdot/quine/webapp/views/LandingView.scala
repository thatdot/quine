package com.thatdot.quine.webapp.views

import com.raquo.laminar.api.L._

import com.thatdot.quine.routes.ClientRoutes
import com.thatdot.quine.webapp.components.landing.{LandingPage, LandingService, LandingStore}

/** View wiring for the landing page.
  *
  * Creates the service (capability), store (orchestration), and page (component).
  * OSS has no auth, so no permission set is threaded through.
  */
object LandingView {
  def apply(routes: ClientRoutes): HtmlElement = {
    val service = new LandingService(routes)
    val store = new LandingStore(service)
    LandingPage(
      metricsSignal = store.metricsSignal,
      backpressureSignal = store.backpressureSignal,
      subscriptions = store.subscriptions,
    )
  }
}
