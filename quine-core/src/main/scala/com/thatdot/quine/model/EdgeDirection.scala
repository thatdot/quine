package com.thatdot.quine.model

sealed abstract class EdgeDirection {
  def reverse: EdgeDirection

  def isDirected: Boolean
}

object EdgeDirection {
  case object Outgoing extends EdgeDirection {
    def reverse = Incoming
    def isDirected = true
  }
  case object Incoming extends EdgeDirection {
    def reverse = Outgoing
    def isDirected = true
  }
  case object Undirected extends EdgeDirection {
    def reverse = Undirected
    def isDirected = false
  }

  val values: Seq[EdgeDirection] = Seq(Outgoing, Incoming, Undirected)
}
