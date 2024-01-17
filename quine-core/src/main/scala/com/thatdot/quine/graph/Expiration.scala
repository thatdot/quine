package com.thatdot.quine.graph

import scala.concurrent.duration._

import org.apache.pekko.util.Timeout

import com.thatdot.quine.graph.messaging.QuineMessage
// TODO: adapt comment for type

/** Nested Futures should have their innermost Future time out before their outermost future.
  * Tracking how much to decrement the inner future is necessarily imprecise (because calculating
  * how much time has elapsed or is remainingrequires a non-measurable amount of time). This method
  * calculates a suitably-shortened timeout.
  *
  * This function has two primary purposes:
  * 1.) convert between relative to fixed timeouts (duration vs. moment)
  * 2.) decrement the timeout according to the `nestedTimeoutSafetyMargin` so that inner timeouts
  * fail first.
  *
  * Guidance:
  * - Call this function when a `Timeout` is needed. If no timeout is needed, then thread through the original nanos.
  * - This should be called as close to where it will be used as possible. All computation done after this is called will eat into the nestedTimeoutSafetyMargin.
  * - It is not needed when completing a future with `onComplete`, even if the success function creates another future via an ask. This is because they are sequential, but a new Timeout will have to be created for the second future.
  */
sealed abstract class Expiration {

  // fromRemoteDuration and fromLocalNanos are called in conjunction (by the sender on one host and
  // receiver on the other). The buffer time is "paid" at sending time

  // Used when receiving a message from a potentially remote source
  def toLocalNanos: Expiration = this match {
    case Expiration.RemoteDuration(nanosLeft) =>
      Expiration.LocalNanoSeconds(System.nanoTime() + nanosLeft)

    case localNanos =>
      localNanos
  }

  // Used when sending a message to a (definitely) remote source, shaving off a _remote_ margin of
  // error
  def fromLocalNanos: Expiration = this match {
    case Expiration.LocalNanoSeconds(failByNanoTime) =>
      val safeNanosLeft = failByNanoTime - System
        .nanoTime() - Expiration.remoteTimeoutSafetyMarginNanos
      Expiration.RemoteDuration(Math.max(safeNanosLeft, Expiration.minimumTimeoutNanos))

    case remoteDuration =>
      require(
        false,
        s"fromLocalNanos only expects 'LocalNanoSeconds', but received a $remoteDuration"
      )
      remoteDuration
  }

  // Used to shave a margin of error from the current (local) expiry time
  def nestedTimeout: (Timeout, Expiration) = this match {
    case Expiration.LocalNanoSeconds(failByNanoTime) =>
      val now = System.nanoTime()
      val safeEndDuration = Math.max(
        failByNanoTime - now - Expiration.localTimeoutSafetyMarginNanos,
        Expiration.minimumTimeoutNanos
      )
      Timeout(safeEndDuration, NANOSECONDS) -> Expiration.LocalNanoSeconds(now + safeEndDuration)

    case remoteDuration =>
      require(
        false,
        s"nestedTimeout only expects 'LocalNanoSeconds', but received a $remoteDuration"
      )
      this.toLocalNanos.nestedTimeout

  }
}

object Expiration {
  // Does NOT trim off a margin
  def fromLocalTimeout(timeout: Timeout): Expiration =
    LocalNanoSeconds(System.nanoTime() + timeout.duration.toNanos)

  final private[quine] case class RemoteDuration(nanosRemaining: Long) extends Expiration
  final private[quine] case class LocalNanoSeconds(failByNanoTime: Long) extends Expiration

  // Since elapsed time cannot be measured, set a conservative margin for local nesting.
  val localTimeoutSafetyMarginNanos: Long = 20.milliseconds.toNanos

  // Since elapsed time cannot be measured, set a conservative margin for remote nesting.
  val remoteTimeoutSafetyMarginNanos: Long = 200.milliseconds.toNanos

  val minimumTimeoutNanos: Long = 100.milliseconds.toNanos
}

// TODO: remove circular dependency w/ `Message`
trait Expires {
  val failAtMoment: Expiration
  def localFailAtMoment(): Expiration = failAtMoment.toLocalNanos
  def preparedForRemoteTell(): QuineMessage
}
