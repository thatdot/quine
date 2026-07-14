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

  /** Transform the carried value (fresh or stale) without touching the lifecycle phase. */
  def map[B](f: A => B): Pot[B] = this match {
    case Pot.Ready(value) => Pot.Ready(f(value))
    case Pot.PendingStale(value) => Pot.PendingStale(f(value))
    case Pot.FailedStale(value, error) => Pot.FailedStale(f(value), error)
    case Pot.Empty => Pot.Empty
    case Pot.Pending => Pot.Pending
    case Pot.Failed(error) => Pot.Failed(error)
  }
}

object Pot {
  case object Empty extends Pot[Nothing]
  case object Pending extends Pot[Nothing]
  final case class Ready[A](value: A) extends Pot[A]
  final case class Failed(error: String) extends Pot[Nothing]
  final case class PendingStale[A](previousValue: A) extends Pot[A]
  final case class FailedStale[A](previousValue: A, error: String) extends Pot[A]

  /** Combine two lifecycle states into the lifecycle of the pair `f(a, b)`.
    *
    * The combined value exists only when both sides have one (including stale values);
    * phase-wise, failure dominates refresh, which dominates readiness: any failed side
    * fails the pair, any not-yet-ready side keeps the pair pending, and a stale side
    * makes the pair stale.
    */
  def map2[A, B, C](pa: Pot[A], pb: Pot[B])(f: (A, B) => C): Pot[C] = {
    val value: Option[C] = for { a <- pa.toOption; b <- pb.toOption } yield f(a, b)
    val error: Option[String] = Seq(pa, pb).collectFirst {
      case Failed(e) => e
      case FailedStale(_, e) => e
    }
    val bothReady = Seq(pa, pb).forall {
      case Ready(_) => true
      case _ => false
    }
    (error, value) match {
      case (Some(e), Some(v)) => FailedStale(v, e)
      case (Some(e), None) => Failed(e)
      case (None, Some(v)) => if (bothReady) Ready(v) else PendingStale(v)
      case (None, None) => if (pa == Empty || pb == Empty) Empty else Pending
    }
  }
}
