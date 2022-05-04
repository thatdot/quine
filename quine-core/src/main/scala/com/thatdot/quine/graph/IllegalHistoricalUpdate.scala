package com.thatdot.quine.graph

import com.thatdot.quine.model.{Milliseconds, QuineId}

/** Thrown to indicate that there was an attempted change to historical state
  *
  * @param event mutating event
  * @param historicalTime historical moment at which mutation was attempted
  */
final case class IllegalHistoricalUpdate(
  events: Seq[NodeChangeEvent],
  node: QuineId,
  historicalTime: Milliseconds
) extends IllegalArgumentException() {
  override def getMessage: String = s"Tried to mutate node at: $node with historical time: $historicalTime"
}

final case class IllegalTimeOverride(
  events: Seq[NodeChangeEvent],
  node: QuineId,
  atTimeOverride: EventTime
) extends IllegalArgumentException() {
  override def getMessage: String =
    s"Tried to process multiple events on: $node with a single atTimeOverride: $atTimeOverride"
}
