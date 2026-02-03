package com.thatdot.quine.exceptions

import com.thatdot.quine.util.QuineError

case class KinesisConfigurationError(msg: String) extends QuineError {
  override def getMessage: String = msg
}
