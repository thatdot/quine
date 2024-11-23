package com.thatdot.quine.app.routes.exts

import org.apache.pekko.http.scaladsl.model.StatusCodes

/** Full implementation of [[QuineEndpoints]] for pekko-http servers
  */
trait PekkoQuineEndpoints extends ServerQuineEndpoints with endpoints4s.pekkohttp.server.Endpoints {
  val ServiceUnavailable: StatusCode = StatusCodes.ServiceUnavailable
}
