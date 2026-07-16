package com.thatdot.quine.webapp.views

import com.raquo.laminar.api.L._

import com.thatdot.quine.webapp.QuineUiOptions
import com.thatdot.quine.webapp.components.streams.EmbeddedEditorConfig
import com.thatdot.quine.webapp.dataservice.DataService
import com.thatdot.quine.webapp.queryui.ExplorerSettingsPage

object ExplorerSettingsView {
  def apply(
    dataService: DataService,
    options: QuineUiOptions,
  ): HtmlElement =
    ExplorerSettingsPage(
      dataService = dataService,
      editorConfig = EmbeddedEditorConfig(
        options.qpEnabled.getOrElse(false),
        options.serverUrl.toOption.filter(_.nonEmpty),
      ),
    )
}
