package com.thatdot.quine.webapp.views

import com.raquo.laminar.api.L._

import com.thatdot.quine.webapp.dataservice.DataService
import com.thatdot.quine.webapp.queryui.ExplorerSettingsPage

object ExplorerSettingsView {
  def apply(
    dataService: DataService,
  ): HtmlElement =
    ExplorerSettingsPage(
      dataService = dataService,
    )
}
