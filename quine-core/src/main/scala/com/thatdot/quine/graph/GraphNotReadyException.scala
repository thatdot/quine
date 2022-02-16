package com.thatdot.quine.graph

import com.thatdot.quine.model.Milliseconds

/** Exception thrown when a graph operation is attempted but the graph is not
  * ready
  */
class GraphNotReadyException extends IllegalStateException() {

  val atTime: Milliseconds = Milliseconds.currentTime();

  override def getMessage: String =
    s"Graph not ready at time $atTime"
}
