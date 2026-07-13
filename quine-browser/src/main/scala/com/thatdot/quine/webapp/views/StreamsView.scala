package com.thatdot.quine.webapp.views

import com.raquo.laminar.api.L._

import com.thatdot.quine.webapp.QuineUiOptions
import com.thatdot.quine.webapp.components.streams.{StreamsCapabilities, StreamsPage}
import com.thatdot.quine.webapp.queryui.WiretapStore

object StreamsView {
  def apply(
    options: QuineUiOptions,
    wiretapStore: WiretapStore,
  ): HtmlElement =
    StreamsPage(options, wiretapStore, StreamsCapabilities.all)
}
