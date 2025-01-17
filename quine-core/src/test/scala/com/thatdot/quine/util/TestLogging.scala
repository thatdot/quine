package com.thatdot.quine.util

import com.thatdot.common.logging.Log.LogConfig

object TestLogging {
  implicit val logConfig: LogConfig = LogConfig.permissive
}
