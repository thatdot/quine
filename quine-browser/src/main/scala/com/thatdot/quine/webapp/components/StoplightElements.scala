package com.thatdot.quine.webapp.components

import scala.scalajs.js

import slinky.core.ExternalComponent
import slinky.core.annotations.react

import js.annotation._

@react object StoplightElements extends ExternalComponent {

  /** See [[https://github.com/stoplightio/elements/blob/main/docs/getting-started/elements/elements-options.md]] */
  case class Props(
    apiDescriptionUrl: String,
    layout: String = "sidebar",
    basePath: String = "/docs",
    router: String = "memory", // TODO switching this to history or hash would allow deep-linking
    logo: String = "/favicon.ico",
    tryItCredentialsPolicy: String = "same-origin", // alternatives: "include" (for cross-origin REST API access), "omit"
  )

  @js.native
  @JSImport("NodeModules/@stoplight/elements/styles.min.css", "css")
  object Css extends js.Object
  locally(Css) // something has to use this for it to actually load

  @js.native
  @JSImport("@stoplight/elements", "API")
  object API extends js.Object

  override val component = API
}
