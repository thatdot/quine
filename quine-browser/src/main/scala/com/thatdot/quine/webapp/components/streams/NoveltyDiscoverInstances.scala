package com.thatdot.quine.webapp.components.streams

import scala.concurrent.ExecutionContext

import com.raquo.laminar.api.L._
import io.circe.Json

import com.thatdot.quine.webapp.openapi.HttpClient
import com.thatdot.quine.webapp.resultspanel.tapmodal.TapModalStyles

/** "Discover instances" button + modal for a Novelty output destination.
  *
  * Fetches the server-resolved candidate Novelty instances (from `quine.novelty.discovery` —
  * static seed addresses or a DNS entry) and lets the user tick a subset, which is appended to the
  * destination's `endpoints[]` in the order discovered; blank entries already there are dropped. Discovery only *suggests*: selecting never adds instances to a running output, and
  * the committed `endpoints` are exactly what the user chose. Manual entry on the field still works.
  */
object NoveltyDiscoverInstances {

  /** The Enterprise-only discovery endpoint, relative to the Quine API's base URL. */
  private val DiscoverPath = "/api/v2/system/noveltyInstances"

  def apply(
    destinationPath: List[String],
    formState: Var[Json],
    baseUrl: String,
  )(implicit ec: ExecutionContext): HtmlElement = {
    val openVar = Var(false)

    def renderModal(): HtmlElement = {
      // None = loading; Some(Left) = error; Some(Right) = resolved candidate URLs.
      val candidates = Var(Option.empty[Either[String, Vector[String]]])
      val selected = Var(Set.empty[String])

      def close(): Unit = openVar.set(false)

      def fetch(): Unit = {
        candidates.set(None)
        HttpClient.execute("GET", DiscoverPath, baseUrl = baseUrl).foreach {
          case Right(json) =>
            val urls = json.asArray
              .getOrElse(Vector.empty)
              .flatMap(_.asObject)
              .flatMap(_("url"))
              .flatMap(_.asString)
            candidates.set(Some(Right(urls)))
          case Left(err) => candidates.set(Some(Left(err)))
        }
      }

      def addSelectedAndClose(): Unit = {
        val chosen = selected.now()
        val sel = candidates.now().flatMap(_.toOption).getOrElse(Vector.empty).filter(chosen)
        if (sel.nonEmpty)
          formState.update { json =>
            val existing = SchemaFormState
              .getAt(json, destinationPath :+ "endpoints")
              .flatMap(_.asArray)
              .getOrElse(Vector.empty)
              .flatMap(_.asString)
              .filter(_.trim.nonEmpty)
            val merged = (existing ++ sel).distinct
            SchemaFormState.setAt(json, destinationPath :+ "endpoints", Json.fromValues(merged.map(Json.fromString)))
          }
        close()
      }

      def toggle(url: String): Unit =
        selected.update(s => if (s.contains(url)) s - url else s + url)

      def candidateRow(url: String): HtmlElement =
        label(
          cls := "form-check d-block",
          input(
            cls := "form-check-input me-2",
            typ := "checkbox",
            checked <-- selected.signal.map(_.contains(url)),
            onClick --> { _ => toggle(url) },
          ),
          code(cls := "small", url),
        )

      div(
        cls := TapModalStyles.overlay,
        display := "flex",
        onMountCallback(_ => fetch()),
        onClick.filter(e => e.target == e.currentTarget) --> (_ => close()),
        div(
          cls := TapModalStyles.dialog,
          onClick.stopPropagation --> (_ => ()),
          div(
            cls := TapModalStyles.header,
            span(cls := TapModalStyles.title, "Discover Novelty instances"),
            button(
              tpe := "button",
              cls := TapModalStyles.closeButton,
              title := "Close",
              "×",
              onClick --> (_ => close()),
            ),
          ),
          div(
            cls := TapModalStyles.body,
            p(
              cls := "small text-body-secondary",
              "Instances resolved from the server's configured discovery source. Tick the ones to add to this " +
              "output's endpoints — selecting never adds them to a running output.",
            ),
            child <-- candidates.signal.map {
              case None =>
                div(cls := "small text-body-secondary fst-italic", "Discovering…")
              case Some(Left(err)) =>
                div(cls := "alert alert-danger py-1 px-2 small mb-0", s"Discovery failed: $err")
              case Some(Right(urls)) if urls.isEmpty =>
                div(
                  cls := "small text-body-secondary fst-italic",
                  "No instances discovered. Configure quine.novelty.discovery, or type endpoints manually.",
                )
              case Some(Right(urls)) =>
                div(urls.map(candidateRow))
            },
          ),
          div(
            cls := "d-flex justify-content-end gap-2 p-2 border-top",
            button(tpe := "button", cls := "btn btn-sm btn-secondary", "Cancel", onClick --> (_ => close())),
            button(
              tpe := "button",
              cls := "btn btn-sm btn-primary",
              "Add selected",
              disabled <-- selected.signal.map(_.isEmpty),
              onClick --> (_ => addSelectedAndClose()),
            ),
          ),
        ),
      )
    }

    div(
      button(
        tpe := "button",
        cls := "btn btn-sm btn-outline-info mt-1 mb-2",
        i(cls := "cil-search me-1"),
        "Discover instances",
        onClick --> (_ => openVar.set(true)),
      ),
      child <-- openVar.signal.map(if (_) renderModal() else emptyNode),
    )
  }
}
