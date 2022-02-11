package com.thatdot.quine.webapp.components

import scala.scalajs.js

import slinky.core.ExternalComponent
import slinky.core.annotations.react

import js.annotation._

@react object SwaggerUi extends ExternalComponent {

  /** See <https://github.com/swagger-api/swagger-ui/tree/master/flavors/swagger-ui-react> */
  case class Props(
    url: String,
    docExpansion: String = "list"
  )

  @js.native
  @JSImport("NodeModules/swagger-ui-react/swagger-ui.css", "css")
  object Css extends js.Object
  locally(Css) // something has to use this for it to actually load

  @js.native
  @JSImport("swagger-ui-react", JSImport.Default)
  object SwaggerUI extends js.Object

  override val component = SwaggerUI
}
