package com.thatdot.quine.webapp.views

import com.raquo.laminar.api.L._

import com.thatdot.quine.webapp.QuineUiOptions
import com.thatdot.quine.webapp.components.StoplightElements

object DocsV2View {
  def apply(options: QuineUiOptions): HtmlElement = StoplightElements(
    apiDescriptionUrl = options.documentationV2Url,
    basePath = "/v2docs",
  )
}
