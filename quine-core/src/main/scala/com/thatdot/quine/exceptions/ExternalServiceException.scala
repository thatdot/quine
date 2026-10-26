package com.thatdot.quine.exceptions

import com.thatdot.quine.util.QuineError

/** A query step called a service outside Quine, and the service failed it. Not a
  * [[com.thatdot.quine.graph.cypher.CypherException]]: the query as written is valid, and the same
  * query may succeed when the service recovers.
  *
  * @param service names what was called, for messages and logs, e.g. "llm.prompt"
  */
final case class ExternalServiceException(
  service: String,
  message: String,
  failure: ExternalServiceException.Failure,
) extends QuineError {
  override def getMessage: String = s"$service: $message"
}

object ExternalServiceException {

  /** Whether the same call can succeed without an operator changing something. */
  sealed trait Failure

  object Failure {

    /** Unreachable, slow, rate limited, or full. The same call may succeed later. */
    case object Unavailable extends Failure

    /** It answered with something unusable, rejected Quine's credentials or model, or Quine's own
      * client for it stopped. Sending the same call again fails the same way until an operator
      * changes something.
      */
    case object BadResponse extends Failure
  }
}
