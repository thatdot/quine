package com.thatdot.quine.webapp.util

import scala.scalajs.js

/** Rendering for the RFC-3339 instants the V2 API reports as strings.
  *
  * The mirrors in `V2ApiTypes` keep these raw — they carry no `java.time` types — so every table
  * that shows one has to parse it, and each of them had grown its own copy of the same three
  * lines. One home instead, because the interesting part is the failure handling: a timestamp we
  * can't parse is shown as-is rather than swallowed, so a server-side format change reads as an
  * odd-looking cell instead of a blank one.
  */
object InstantDisplay {

  /** What an absent or empty instant renders as — a schedule that never fires again, or a job
    * that has not fired yet.
    */
  val Absent: String = "—"

  /** Render an instant in the viewer's locale, falling back to the raw text when it doesn't
    * parse and to [[Absent]] when there is nothing to render.
    */
  def localTime(instant: Option[String]): String =
    instant.filter(_.nonEmpty).fold(Absent) { text =>
      val parsed = js.Date.parse(text)
      if (js.isUndefined(parsed) || parsed.isNaN) text else new js.Date(parsed).toLocaleString()
    }

  /** [[localTime]] for a field the API always populates. */
  def localTime(instant: String): String = localTime(Some(instant))
}
