package com.thatdot.quine.graph

import com.thatdot.quine.model.{Milliseconds, QuineId}

/** Thrown to indicate that there was an attempted change to historical state
  *
  * @param event mutating event
  * @param historicalTime historical moment at which mutation was attempted
  */
final case class IllegalHistoricalUpdate(
  event: NodeChangeEvent,
  node: QuineId,
  historicalTime: Milliseconds
) extends IllegalArgumentException() {
  override def getMessage: String = s"Tried to mutate $node at historical time $historicalTime"
}
