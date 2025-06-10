package com.thatdot.model.v2.outputs

import com.thatdot.common.logging.Log.AlwaysSafeLoggable

object OutputsLoggables {
  implicit val LogStatusCode: AlwaysSafeLoggable[org.apache.pekko.http.scaladsl.model.StatusCode] = _.value
}
