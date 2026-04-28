package com.thatdot.quine.webapp.components.streams

import scala.concurrent.Future

import com.raquo.laminar.api.L._
import org.scalajs.dom
import org.scalajs.macrotaskexecutor.MacrotaskExecutor.Implicits._

import com.thatdot.quine.openapi.{OpenApiParser, ParsedSpec, UiHintsSource}
import com.thatdot.quine.webapp.QuineUiOptions
import com.thatdot.quine.webapp.util.Pot

/** Top-level page component for the Streams UI.
  * Fetches the V2 OpenAPI spec, then renders the ingest + standing query panels.
  * Only available when the V2 API is enabled.
  */
object StreamsPage {

  def apply(options: QuineUiOptions): HtmlElement = {
    val specState = Var[Pot[ParsedSpec]](Pot.Empty)

    val specUrl = options.documentationV2Url

    div(
      cls := "container-fluid px-3 py-3",
      onMountCallback { _ =>
        specState.set(Pot.Pending)
        fetchAndParse(specUrl).foreach {
          case Right(spec) => specState.set(Pot.Ready(spec))
          case Left(err) => specState.set(Pot.Failed(err))
        }
      },
      // Header
      div(
        cls := "d-flex justify-content-between align-items-center mb-3",
        h2(cls := "mb-0", "Streams"),
      ),
      // Content
      child <-- specState.signal.map {
        case Pot.Empty | Pot.Pending =>
          div(
            cls := "text-center py-5",
            div(cls := "spinner-border text-primary", role := "status"),
            p(cls := "mt-3 text-body-secondary", "Loading API specification..."),
          )

        case Pot.Failed(msg) =>
          div(cls := "alert alert-danger", s"Failed to load API specification: $msg")

        case Pot.Ready(spec) =>
          val client = StreamsApiClient(spec)
          div(
            IngestStreamPanel(client),
            div(cls := "mt-4"),
            StandingQueryPanel(client),
          )

        case _ => emptyNode
      },
    )
  }

  private def fetchAndParse(url: String): Future[Either[String, ParsedSpec]] =
    (for {
      response <- dom.fetch(url).toFuture
      text <- response.text().toFuture
    } yield
      if (response.ok) OpenApiParser.parse(text).map(attachUiHints)
      else Left(s"HTTP ${response.status}")).recover { case ex: Throwable =>
      Left(ex.getMessage)
    }

  /** Attach the Streams UI overlay to the parsed spec and report any
    * drift (hints referring to schemas/fields not present in the spec) as a
    * console warning. The attached hints drive field ordering, promotion, and
    * hiding inside [[SchemaFormRenderer]].
    */
  private def attachUiHints(spec: ParsedSpec): ParsedSpec = {
    val source = StreamsUiHints.source
    UiHintsSource.checkDrift(source, spec.schemas, org.scalajs.dom.console.warn(_))
    spec.copy(hints = source)
  }
}
