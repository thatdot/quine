package com.thatdot.quine.exceptions

import com.thatdot.quine.util.QuineError

case class KafkaValidationException(msg: String) extends QuineError {
  override def getMessage: String = msg
}
