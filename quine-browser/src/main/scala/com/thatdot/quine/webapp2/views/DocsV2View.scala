package com.thatdot.quine.webapp2.views

import com.raquo.laminar.api.L._

import com.thatdot.quine.webapp.QuineUiOptions
import com.thatdot.quine.webapp.components.StoplightElements
import com.thatdot.quine.webapp2.components.MountReactComponent

object DocsV2View {
  def apply(options: QuineUiOptions): HtmlElement = MountReactComponent(
    StoplightElements(
      apiDescriptionUrl = options.documentationV2Url,
      basePath = "/v2docs",
      logo = options.baseURI + "favicon.ico",
    ),
  )
}
