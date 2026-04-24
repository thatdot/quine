package com.thatdot.quine.webapp.components

import scala.scalajs.js
import scala.scalajs.js.annotation._

import com.raquo.laminar.api.L._
import com.raquo.laminar.codecs.Codec
import org.scalajs.dom
import org.scalajs.macrotaskexecutor.MacrotaskExecutor.Implicits._

/** Laminar wrapper for the Stoplight Elements API documentation viewer.
  *
  * Uses the `<elements-api>` web component from `@stoplight/elements/web-components.min.js`,
  * which bundles React internally. No external React dependencies needed.
  *
  * Fetches the OpenAPI spec, replaces the `{{openapi_url}}` placeholder with the
  * actual URL, and passes the processed spec to Stoplight via `apiDescriptionDocument`.
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
  private val apiDescriptionDocumentProp = htmlProp("apiDescriptionDocument", None, Codec.stringAsIs)

  private val placeholder = "{{openapi_url}}"

  def apply(
    apiDescriptionUrl: String,
    layout: String = "stacked",
    basePath: String = "/docs",
    router: String = "memory",
    tryItCredentialsPolicy: String = "same-origin",
  ): HtmlElement = {
    val specVar = Var(Option.empty[String])

    val fetchFuture = for {
      response <- dom.fetch(apiDescriptionUrl).toFuture
      text <- response.text().toFuture
    } yield {
      val linkUrl = apiDescriptionUrl.takeWhile(_ != '?')
      text.replace(placeholder, linkUrl)
    }

    fetchFuture.foreach(processed => specVar.set(Some(processed)))

    div(
      cls := "sl-elements-wrapper",
      padding := "1rem 2rem",
      height := "100%",
      child <-- specVar.signal.map {
        case Some(spec) =>
          elementsApi(
            attr("layout") := layout,
            attr("basePath") := basePath,
            attr("router") := router,
            attr("tryItCredentialsPolicy") := tryItCredentialsPolicy,
            height := "100%",
            apiDescriptionDocumentProp := spec,
          )
        case None =>
          emptyNode
      },
    )
  }
}
