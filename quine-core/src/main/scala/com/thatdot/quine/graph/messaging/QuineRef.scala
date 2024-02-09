package com.thatdot.quine.graph.messaging

import org.apache.pekko.actor.ActorRef

import com.thatdot.quine.graph.{NamespaceId, namespaceFromString, namespaceToString}
import com.thatdot.quine.model.{Milliseconds, QuineId, QuineIdProvider}

/** Something to which we can send a message from inside the Quine actor system
  *
  * @see [[com.thatdot.quine.graph.BaseGraph.relayTell]] for a place where this matters
  */
sealed abstract class QuineRef

/** An actor in the Quine actor system.
  *
  * @param ref Pekko reference to the actor
  */
final case class WrappedActorRef(
  ref: ActorRef
) extends QuineRef

/** A fully qualified QuineId, allowing a specific actors to be addressed anywhere in the (name)space/(at)time universe.
  *
  * Every [[SpaceTimeQuineId]] corresponds to some node (which may never have been accessed yet), and every node
  * has a unique [[SpaceTimeQuineId]] identifying it.
  *
  * @param id which node
  * @param namespace the graph-level namespace responsible for hosting this node.
  * @param atTime if None represents the current moment, if specified, represents an (immutable) historical node
  */
final case class SpaceTimeQuineId(
  id: QuineId,
  namespace: NamespaceId,
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

  /** The internal unambiguous string representation of the ID + namespace + atTime.
    *
    * The `QuineId`` part is always either the literal string "empty" or else a non-empty even-length string
    * containing only numbers and uppercase A-F. The choice of using "empty" instead of an empty
    * string is because we use this in places where an empty string is problematic (eg. naming
    * Pekko actors).
    *
    * @see [[QuineId.toInternalString]]
    * @see [[namespaceToString]]
    * @see [[SpaceTimeQuineId.fromInternalString]]
    */
  def toInternalString: String =
    // Form: "QuineID-namespace-atTime"  Example: "3E74242E538F3BD1981449E6761551B1-default-present"
    s"${id.toInternalString}-${namespaceToString(namespace)}-${atTime
      .fold("present")(t => java.lang.Long.toUnsignedString(t.millis))}"
}
object SpaceTimeQuineId {

  /** Recover an ID and time from a string produced by [[SpaceTimeQuineId.toInternalString]].
    *
    * @see [[QuineId.fromInternalString]]
    * @see [[namespaceFromString]]
    * @see [[SpaceTimeQuineId.toInternalString]]
    */
  @throws[IllegalArgumentException]("if the input string is not a valid internal ID and time")
  def fromInternalString(str: String): SpaceTimeQuineId =
    // Form: "QuineID-namespace-atTime"  Example: "3E74242E538F3BD1981449E6761551B1-default-present"
    str.split('-') match {
      case Array(qidString, namespaceString, atTimeString) =>
        val qid = QuineId.fromInternalString(qidString)
        val namespace = namespaceFromString(namespaceString)
        val atTime = atTimeString match {
          case "present" => None
          case ts => Some(Milliseconds.fromString(ts))
        }
        SpaceTimeQuineId(qid, namespace, atTime)
      case other =>
        throw new IllegalArgumentException(
          s"Unexpected ID string: $str structure from internal string: ${other.toList}"
        )
    }
}
