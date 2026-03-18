package com.thatdot.quine.webapp.components

import scala.scalajs.js
import scala.scalajs.js.annotation._

import com.raquo.laminar.api.L._
import com.raquo.laminar.codecs.Codec

/** Laminar wrapper for the Stoplight Elements API documentation viewer.
  *
  * Uses the `<elements-api>` web component from `@stoplight/elements/web-components.min.js`,
  * which bundles React internally. No external React dependencies needed.
  *
  * @see [[https://github.com/stoplightio/elements/blob/main/docs/getting-started/elements/elements-options.md]]
  */
object StoplightElements {

  // Side-effect import: registers the <elements-api> custom element
  @js.native
  @JSImport("@stoplight/elements/web-components.min.js", JSImport.Namespace)
  private object WebComponents extends js.Object
  locally(WebComponents)

  @js.native
  @JSImport("NodeModules/@stoplight/elements/styles.min.css", "css")
  private object Css extends js.Object
  locally(Css) // force CSS side-effect import

  private val elementsApi = htmlTag("elements-api")
  private def attr(name: String): HtmlAttr[String] = htmlAttr(name, Codec.stringAsIs)

  def apply(
    apiDescriptionUrl: String,
    layout: String = "sidebar",
    basePath: String = "/docs",
    router: String = "memory",
    logo: String = "/favicon.ico",
    tryItCredentialsPolicy: String = "same-origin",
  ): HtmlElement =
    elementsApi(
      attr("apiDescriptionUrl") := apiDescriptionUrl,
      attr("layout") := layout,
      attr("basePath") := basePath,
      attr("router") := router,
      attr("logo") := logo,
      attr("tryItCredentialsPolicy") := tryItCredentialsPolicy,
      height := "100%",
    )
}
