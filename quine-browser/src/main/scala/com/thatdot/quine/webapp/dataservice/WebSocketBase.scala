package com.thatdot.quine.webapp.dataservice

import org.scalajs.dom

import com.thatdot.quine.routes.ClientRoutes

/** The `ws(s)://…` origin the tap sockets hang off, derived from the configured API base URL
  * (falling back to the page's own origin when the UI is served by the server it talks to).
  *
  * Shared by every tap store rather than restated per store: the scheme rewrite is the kind of
  * detail that silently breaks TLS deployments when one copy of it drifts.
  */
object WebSocketBase {

  def of(routes: ClientRoutes): String =
    routes.baseUrlOpt
      .filter(_.nonEmpty)
      .getOrElse(dom.window.location.origin)
      .replaceFirst("^http", "ws")
}
