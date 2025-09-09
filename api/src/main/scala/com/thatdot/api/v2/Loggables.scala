package com.thatdot.api.v2

import com.thatdot.api.v2.ErrorResponse.Unauthorized
import com.thatdot.common.logging.Log.AlwaysSafeLoggable

object Loggables {
  implicit val logUnauthorized: AlwaysSafeLoggable[Unauthorized] = unauthorized =>
    s"Unauthorized: ${unauthorized.errors.mkString("[", ", ", "]")}"

}
