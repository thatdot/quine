package com.thatdot.quine.graph

import com.thatdot.quine.model.Milliseconds

/** Exception thrown when a graph operation is attempted but the graph is not
  * ready
  */
class GraphNotReadyException(val atTime: Milliseconds = Milliseconds.currentTime()) extends IllegalStateException() {

  override def getMessage: String =
    s"Graph not ready at time: ${atTime.millis}"
}

class ShardNotAvailableException(val msg: String) extends NoSuchElementException(msg)
