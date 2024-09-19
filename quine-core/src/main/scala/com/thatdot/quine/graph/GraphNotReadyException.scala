package com.thatdot.quine.graph

import com.thatdot.quine.model.Milliseconds
import com.thatdot.quine.util.QuineError

/** Exception thrown when a graph operation is attempted but the graph is not
  * ready
  */
class GraphNotReadyException(val atTime: Milliseconds = Milliseconds.currentTime())
    extends IllegalStateException()
    with QuineError {

  override def getMessage: String =
    s"Graph not ready at time: ${atTime.millis}"
}

case class ShardNotAvailableException(msg: String) extends NoSuchElementException(msg) with QuineError
