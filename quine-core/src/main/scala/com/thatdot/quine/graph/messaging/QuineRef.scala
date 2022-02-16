package com.thatdot.quine.graph.messaging

import akka.actor.ActorRef

import com.thatdot.quine.model.{Milliseconds, QuineId, QuineIdProvider}

/** Something to which we can send a message from inside the Quine actor system
  *
  * @see [[com.thatdot.quine.graph.BaseGraph.relayTell]] for a place where this matters
  */
sealed abstract class QuineRef

/** An actor in the Quine actor system.
  *
  * @param ref Akka reference to the actor
  */
final case class WrappedActorRef(
  ref: ActorRef
) extends QuineRef

/** An actor in the Quine actor system which represents a node at a point in
  * time.
  *
  * Every [[QuineIdAtTime]] corresponds to some node (which may never have
  * been accessed yet), and every node has a unique [[QuineIdAtTime]]
  * identifying it.
  *
  * @param id which node
  * @param atTime which moment
  */
final case class QuineIdAtTime(
  id: QuineId,
  atTime: Option[Milliseconds]
) extends QuineRef {

  /** Print a developer-facing representation of an ID */
  def debug(implicit idProvider: QuineIdProvider): String = atTime match {
    case None => id.debug
    case Some(t) => s"${id.debug} (at time $t)"
  }

  /** Print a user-facing representation of an ID (which is invertible via `qidFromPrettyString`) */
  def pretty(implicit idProvider: QuineIdProvider): String = atTime match {
    case None => id.pretty
    case Some(t) => s"${id.pretty} (at time $t)"
  }

  /** The internal unambiguous string representation of the ID.
    *
    * This is always either the literal string "empty" or else a non-empty even-length string
    * containing only numbers and uppercase A-F. The choice of using "empty" instead of an empty
    * string is because we use this in places where an empty string is problematic (eg. naming
    * Akka actors).
    *
    * @see [[QuineId.toInternalString]]
    * @see [[QuineIdAtTime.fromInternalString]]
    */
  def toInternalString: String = atTime match {
    case None => id.toInternalString
    case Some(t) => s"${id.toInternalString}-${java.lang.Long.toUnsignedString(t.millis)}"
  }
}
object QuineIdAtTime {

  /** Recover an ID and time from a string produced by [[QuineIdAtTime.toInternalString]].
    *
    * @see [[QuineId.fromInternalString]]
    * @see [[QuineIdAtTime.toInternalString]]
    */
  @throws[IllegalArgumentException]("if the input string is not a valid internal ID and time")
  def fromInternalString(str: String): QuineIdAtTime =
    str.indexOf('-') match {
      case -1 =>
        QuineIdAtTime(QuineId.fromInternalString(str), None)
      case dashIdx =>
        val qid = QuineId.fromInternalString(str.substring(0, dashIdx))
        val atTime = java.lang.Long.parseUnsignedLong(str.substring(dashIdx + 1, str.length), 10)
        QuineIdAtTime(qid, Some(Milliseconds(atTime)))
    }
}
