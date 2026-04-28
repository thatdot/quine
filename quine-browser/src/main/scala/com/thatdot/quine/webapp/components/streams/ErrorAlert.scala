package com.thatdot.quine.webapp.components.streams

import com.raquo.laminar.api.L._

/** Reactive error banner shared across the create forms.
  *
  * Recognises [[HttpClient]]'s `HTTP <status>: <messages>` shape and renders
  * the status as a badge with a bulleted list of messages. Anything else
  * falls through to a plain warning panel.
  */
object ErrorAlert {

  private val HttpErrorPattern = """^HTTP (\d+): (.+)$""".r

  /** Bind to a `Signal[Option[String]]`. Emits no DOM when the signal is `None`. */
  def apply(errorSignal: Signal[Option[String]]): Modifier[HtmlElement] =
    child <-- errorSignal.map {
      case Some(err) => render(err)
      case None => emptyNode
    }

  private def render(err: String): HtmlElement = err match {
    case HttpErrorPattern(status, detail) =>
      val messages = detail.split("; ").toList
      div(
        cls := "alert alert-danger d-flex align-items-start mt-3",
        div(
          cls := "me-3",
          span(cls := "badge bg-danger fs-6", status),
        ),
        div(
          cls := "flex-grow-1",
          strong(cls := "d-block mb-1", "Request failed"),
          if (messages.size == 1) span(messages.head)
          else
            ul(
              cls := "mb-0 ps-3",
              messages.map(m => li(m)),
            ),
        ),
      )
    case _ =>
      div(
        cls := "alert alert-danger d-flex align-items-start mt-3",
        div(cls := "me-3", i(cls := "cil-warning fs-5")),
        div(cls := "flex-grow-1", err),
      )
  }
}
