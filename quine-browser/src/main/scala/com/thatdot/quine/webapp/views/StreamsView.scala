package com.thatdot.quine.webapp.views

import com.raquo.laminar.api.L._

import com.thatdot.quine.webapp.QuineUiOptions
import com.thatdot.quine.webapp.components.streams.{StreamsCapabilities, StreamsPage}
import com.thatdot.quine.webapp.dataservice.DataService

object StreamsView {
  def apply(
    options: QuineUiOptions,
    dataService: DataService,
  ): HtmlElement =
    // OSS has no auth, so every Streams control is available.
    StreamsPage(options, dataService, StreamsCapabilities.all)
}
