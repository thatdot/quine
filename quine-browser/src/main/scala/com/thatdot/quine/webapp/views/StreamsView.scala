package com.thatdot.quine.webapp.views

import com.raquo.laminar.api.L._

import com.thatdot.quine.webapp.QuineUiOptions
import com.thatdot.quine.webapp.components.streams.StreamsPage

object StreamsView {
  def apply(options: QuineUiOptions): HtmlElement =
    StreamsPage(options)
}
