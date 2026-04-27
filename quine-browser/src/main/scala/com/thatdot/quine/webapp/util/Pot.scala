package com.thatdot.quine.webapp.util

/** Lifecycle state for data fetched from the server.
  *
  * Models the full async lifecycle so components can exhaustively
  * handle every state (empty, loading, loaded, failed, stale-refresh, failed-with-stale).
  */
sealed abstract class Pot[+A] {
  def toOption: Option[A] = this match {
    case Pot.Ready(value) => Some(value)
    case Pot.PendingStale(value) => Some(value)
    case Pot.FailedStale(value, _) => Some(value)
    case _ => None
  }
}

object Pot {
  case object Empty extends Pot[Nothing]
  case object Pending extends Pot[Nothing]
  final case class Ready[A](value: A) extends Pot[A]
  final case class Failed(error: String) extends Pot[Nothing]
  final case class PendingStale[A](previousValue: A) extends Pot[A]
  final case class FailedStale[A](previousValue: A, error: String) extends Pot[A]
}
