package com.thatdot.quine.app.routes.exts

import org.apache.pekko.http.scaladsl.model.StatusCodes

/** Full implementation of [[QuineEndpoints]] for pekko-http servers
  */
trait PekkoQuineEndpoints extends ServerQuineEndpoints with endpoints4s.pekkohttp.server.Endpoints {
  // A def, as endpoints4s declares every status code: endpoint vals in traits initialized before
  // this one read it during construction, and would capture null from a val
  def ServiceUnavailable: StatusCode = StatusCodes.ServiceUnavailable
}
