package com.thatdot.quine.webapp.components

import scala.scalajs.js
import scala.scalajs.js.annotation._

import com.raquo.laminar.api.L._
import com.raquo.laminar.codecs.Codec
import io.circe.{Json, JsonObject, Printer, parser}
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
      stripPathParameterExamples(text.replace(placeholder, linkUrl))
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
            attr("hideServerInfo") := "true",
            height := "100%",
            apiDescriptionDocumentProp := spec,
          )
        case None =>
          emptyNode
      },
    )
  }

  // Stoplight Elements pre-fills required path-parameter inputs in its TryIt panel from
  // OpenAPI `parameter.example` / `parameter.examples`. For path parameters that name real
  // resources, that means a click on "Send API Request" silently dispatches a call against
  // the example identifier instead of one the user typed. Strip path-parameter examples
  // here so the docs UI starts each input empty; the public OpenAPI spec served at
  // /api/v2/openapi.json is unaffected and still carries the examples for other consumers.
  private val httpMethodKeys: Set[String] =
    Set("get", "post", "put", "delete", "patch", "options", "head", "trace")

  private def stripPathParameterExamples(jsonText: String): String =
    parser.parse(jsonText) match {
      case Right(json) => stripExamplesInPaths(json).printWith(Printer.noSpaces)
      case Left(_) => jsonText
    }

  private def stripExamplesInPaths(spec: Json): Json =
    spec.hcursor
      .downField("paths")
      .withFocus(_.mapObject(_.mapValues(rewritePathItem)))
      .top
      .getOrElse(spec)

  private def rewritePathItem(item: Json): Json =
    item.mapObject(obj =>
      JsonObject.fromIterable(obj.toIterable.map {
        case ("parameters", v) => "parameters" -> stripExamplesInParamArray(v)
        case (k, v) if httpMethodKeys.contains(k) => k -> rewriteOperation(v)
        case other => other
      }),
    )

  private def rewriteOperation(op: Json): Json =
    op.mapObject(obj =>
      JsonObject.fromIterable(obj.toIterable.map {
        case ("parameters", v) => "parameters" -> stripExamplesInParamArray(v)
        case other => other
      }),
    )

  private def stripExamplesInParamArray(arr: Json): Json =
    arr.mapArray(_.map(stripExampleIfPathParam))

  private def stripExampleIfPathParam(param: Json): Json =
    if (param.hcursor.downField("in").as[String].contains("path"))
      param.mapObject(_.remove("example").remove("examples"))
    else
      param
}
