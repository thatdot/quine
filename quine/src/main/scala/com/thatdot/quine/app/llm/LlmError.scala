package com.thatdot.quine.app.llm

import com.thatdot.quine.exceptions.ExternalServiceException
import com.thatdot.quine.graph.cypher.CypherException
import com.thatdot.quine.util.QuineError

/** A failure of a model call. Messages never carry the endpoint URL or key, since they reach query authors.
  *
  * @param kind what the failure says about the profile, which decides retry and fallback
  * @param fault who must act for the call to succeed, which decides the exception the query sees
  * @param status the HTTP status, when the endpoint answered with one
  * @param cause for operator logs only, since it may hold the host, see [[LlmError.toQuineError]];
  *              a second parameter list keeps the reference-compared `Throwable` out of `equals` and `unapply`
  */
final case class LlmError(message: String, kind: LlmError.Kind, fault: LlmError.Fault, status: Option[Int] = None)(
  val cause: Option[Throwable] = None,
) extends Exception(message, cause.orNull) {

  /** The call may succeed if sent again at once: a rate limit, a server error, or a transport failure. */
  def retryable: Boolean = kind == LlmError.Kind.Retryable

  /** The profile could not serve this call, so a fallback profile may. Every retryable failure also
    * counts: retries run first, within the profile, and fallback moves on when they are spent.
    */
  def cannotServe: Boolean = kind != LlmError.Kind.Final
}

object LlmError {

  /** The shape most raisers use: no status, no cause. An explicit overload hides the empty trailing
    * list; it declares no defaults and a lower arity than the constructor, because overloading
    * disables default arguments and a same-arity overload is ambiguous with the case class's own
    * `apply`.
    */
  def apply(message: String, kind: Kind, fault: Fault): LlmError = new LlmError(message, kind, fault)()

  /** What a failure says about the profile. The three values order by how far the call travels afterward:
    * retried on the same profile, moved to the next profile, or surfaced to the query.
    */
  sealed trait Kind extends Product with Serializable

  object Kind {

    /** Sending again may succeed, so the profile retries in place, then counts as unable to serve. */
    case object Retryable extends Kind

    /** The profile cannot serve this call and retrying would not help; a fallback profile may. */
    case object CannotServe extends Kind

    /** The endpoint answered, or the call itself is wrong, so no other profile changes the outcome. */
    case object Final extends Kind
  }

  /** Who must act for the call to succeed. Decides the exception the query sees, see
    * [[toQuineError]]: a `Query` fault is the author's, a runtime Cypher error; the other two are
    * the service's, an [[com.thatdot.quine.exceptions.ExternalServiceException]].
    */
  sealed trait Fault extends Product with Serializable

  object Fault {

    /** The call as written cannot succeed on this profile. */
    case object Query extends Fault

    /** The service is unreachable, slow, rate limited, or full. */
    case object Unavailable extends Fault

    /** An operator must act: the service answered with something unusable, rejected Quine's
      * credentials or model, or this profile's pool stopped.
      */
    case object BadResponse extends Fault
  }

  /** The error every query fault surfaces as; [[LlmProcedureOptions.error]] builds the same shape. */
  def queryError(procedureName: String, message: String): CypherException.Runtime =
    CypherException.Runtime(s"$procedureName: $message")

  /** Map an [[LlmError]] by its fault to the exception every Cypher consumer classifies, keeping its message. */
  def toQuineError(procedureName: String): PartialFunction[Throwable, QuineError] = { case e: LlmError =>
    e.fault match {
      case Fault.Query => queryError(procedureName, e.getMessage)
      case Fault.Unavailable =>
        ExternalServiceException(procedureName, e.getMessage, ExternalServiceException.Failure.Unavailable)
      case Fault.BadResponse =>
        ExternalServiceException(procedureName, e.getMessage, ExternalServiceException.Failure.BadResponse)
    }
  }

  def malformed(detail: String, cause: Option[Throwable] = None): LlmError =
    new LlmError(s"the model endpoint returned an unexpected response: $detail", Kind.Final, Fault.BadResponse)(cause)

  def refused(detail: String): LlmError =
    LlmError(s"the model declined the request: $detail", Kind.Final, Fault.BadResponse)

  /** Quine refused to send because the profile's waiting queue is full. Never retried. */
  def overloaded(profile: String, queueCapacity: Int): LlmError =
    LlmError(
      s"""profile "$profile" already has $queueCapacity requests waiting, """ +
      "lower the ingest parallelism or raise max-concurrent-requests",
      Kind.CannotServe,
      Fault.Unavailable,
    )

  /** The limit is the profile's own, so a fallback profile with a higher limit may serve the call. */
  def stateTooLarge(chars: Int, limit: Int): LlmError =
    LlmError(
      s"state is $chars characters, over the profile limit of $limit (max-state-chars)",
      Kind.CannotServe,
      Fault.Query,
    )

  /** The limit is the profile's own, so a fallback profile with a higher limit may serve the call. */
  def promptTooLarge(chars: Int, limit: Int): LlmError =
    LlmError(
      s"prompt is $chars characters, over the profile limit of $limit (max-prompt-chars)",
      Kind.CannotServe,
      Fault.Query,
    )

  def invalidQuestion(id: String, reason: String): LlmError =
    LlmError(s"""question "$id": $reason""", Kind.Final, Fault.Query)
}
