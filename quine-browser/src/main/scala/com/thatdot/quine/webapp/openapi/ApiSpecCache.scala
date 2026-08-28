package com.thatdot.quine.webapp.openapi

import scala.collection.mutable
import scala.concurrent.Future

import org.scalajs.dom
import org.scalajs.macrotaskexecutor.MacrotaskExecutor.Implicits._

import com.thatdot.quine.openapi.{OpenApiParser, ParsedSpec, UiHintsSource}
import com.thatdot.quine.webapp.AuthEvents

/** Fetches and parses the V2 OpenAPI document, once per URL per page load.
  *
  * The document is what drives every schema-generated form in the app — the Streams page's
  * create forms and the query bar's run-in-background dialog — and it is large enough that
  * fetching it per consumer is worth avoiding. Callers get the same in-flight `Future`, so two
  * surfaces racing to open still cause one request.
  *
  * Cached for the lifetime of the page: the spec describes the server build, which cannot change
  * under a running tab without a reload.
  */
object ApiSpecCache {

  private val inFlight: mutable.Map[String, Future[Either[String, ParsedSpec]]] = mutable.Map.empty

  /** The parsed spec at `url`, with `hints` attached. Left carries a display-ready message.
    *
    * A failed load is not cached — a spec that failed because the user's session had expired
    * should be retried once they are back, rather than leaving every form permanently broken.
    */
  def load(url: String, hints: UiHintsSource): Future[Either[String, ParsedSpec]] =
    inFlight.getOrElseUpdate(
      url, {
        val pending = fetchAndParse(url, hints)
        pending.foreach {
          case Left(_) => inFlight.remove(url)
          case Right(_) => ()
        }
        pending
      },
    )

  private def fetchAndParse(url: String, hints: UiHintsSource): Future[Either[String, ParsedSpec]] =
    (for {
      response <- dom.fetch(url).toFuture
      text <- response.text().toFuture
    } yield
      if (response.status == 401) {
        AuthEvents.unauthorized.emit(())
        Left(s"HTTP ${response.status}")
      } else if (response.ok) OpenApiParser.parse(text).map(attachHints(_, hints))
      else Left(s"HTTP ${response.status}")).recover { case ex: Throwable =>
      dom.console.error("Failed to load API specification:", ex.getMessage)
      Left("Could not connect to the server.")
    }

  /** Attach the UI overlay to the parsed spec and report any drift (hints naming schemas or
    * fields the spec doesn't have) as a console warning. The attached hints drive field
    * ordering, promotion, labelling, and hiding inside `SchemaFormRenderer`.
    */
  private def attachHints(spec: ParsedSpec, hints: UiHintsSource): ParsedSpec = {
    UiHintsSource.checkDrift(hints, spec.schemas, dom.console.warn(_))
    spec.copy(hints = hints)
  }
}
