package com.thatdot.quine.webapp.views

import com.raquo.laminar.api.L._

import com.thatdot.quine.routes.ClientRoutes
import com.thatdot.quine.webapp.QuineUiOptions
import com.thatdot.quine.webapp.dataservice.DataService
import com.thatdot.quine.webapp.queryui.QueryUi

object ExplorationUiView {

  /** Takes the full [[QuineUiOptions]] rather than just the query-UI subset because the query
    * bar's run-in-background dialog is generated from the V2 OpenAPI document, whose URL only
    * that trait carries.
    */
  def apply(
    options: QuineUiOptions,
    routes: ClientRoutes,
    dataService: DataService,
  ): HtmlElement =
    QueryUi.fromOptions(
      options,
      routes,
      dataService,
      documentationV2Url = Some(options.documentationV2Url),
    )
}
