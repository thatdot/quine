package com.thatdot.quine.model

sealed abstract class EdgeDirection(val index: Byte) {
  def reverse: EdgeDirection

}

object EdgeDirection {
  case object Outgoing extends EdgeDirection(0) {
    def reverse = Incoming
  }
  case object Incoming extends EdgeDirection(1) {
    def reverse = Outgoing
  }
  case object Undirected extends EdgeDirection(2) {
    def reverse = Undirected
  }

  val values: IndexedSeq[EdgeDirection] = Vector(Outgoing, Incoming, Undirected)
  assert(
    values.zipWithIndex.forall { case (d, i) => d.index == i },
    "Edge indices must match their position in values list",
  )
}
