package com.thatdot.outputs2

import com.thatdot.common.logging.Log.AlwaysSafeLoggable

object OutputsLoggables {
  implicit val LogStatusCode: AlwaysSafeLoggable[org.apache.pekko.http.scaladsl.model.StatusCode] = _.value
}
