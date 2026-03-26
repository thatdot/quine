package com.thatdot.quine.webapp.views

import com.raquo.laminar.api.L._

import com.thatdot.quine.webapp.QuineUiOptions
import com.thatdot.quine.webapp.components.StoplightElements

object DocsV1View {
  def apply(options: QuineUiOptions): HtmlElement = StoplightElements(
    apiDescriptionUrl = options.documentationUrl,
    basePath = "/docs",
  )
}
