package com.thatdot.quine.util

import com.thatdot.quine.util.Log.LogConfig

object TestLogging {
  implicit val logConfig: LogConfig = LogConfig.permissive
}
