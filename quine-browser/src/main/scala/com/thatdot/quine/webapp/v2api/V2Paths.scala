package com.thatdot.quine.webapp.v2api

/** Relative V2 REST paths shared by the fetch and WebSocket clients, so an API path change
  * has one home instead of drifting string literals (the catalog fetch and the tap sockets
  * both address `standingQueries`).
  */
object V2Paths {

  /** The standing-queries collection for `graph` — no leading slash, ready for [[V2Fetch]] or
    * to suffix onto a WebSocket origin.
    */
  def standingQueries(graph: String): String = s"api/v2/graph/$graph/standingQueries"
}
