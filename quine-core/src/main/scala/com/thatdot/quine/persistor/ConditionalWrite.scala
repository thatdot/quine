package com.thatdot.quine.persistor

/** Outcome of a compare-and-set write to metadata storage.
  *
  * Three cases rather than a `Boolean`, because the two ways a conditional write can be turned
  * down mean opposite things to the caller and a boolean cannot carry the difference:
  *
  *   - [[ConditionalWriteResult.Conflict]] means "someone else wrote since you read": the caller
  *     should re-read, reapply its change to the new base, and try again.
  *   - [[ConditionalWriteResult.Fenced]] means "you are no longer entitled to write here": the
  *     caller must STOP. Retrying is precisely the wrong response.
  *
  * `Fenced` is unreachable today: no persistor issues it, because authority is not yet something
  * storage knows about. It exists anyway so that call sites branch on retry-versus-stop from the
  * first line of code written against this API. A caller that reads the result as "applied or
  * not" and retries on "not" is correct under compare-and-set alone and becomes a spin loop the day
  * a lease model lands: the one migration hazard worth pre-empting, and it costs one unreachable
  * case to remove.
  */
sealed abstract class ConditionalWriteResult extends Product with Serializable

object ConditionalWriteResult {

  /** The expected value matched, and the new value is durable. */
  case object Written extends ConditionalWriteResult

  /** The expected value did not match: storage holds `current` instead ([[None]] if the key is
    * absent). The current value is returned rather than merely reported as a mismatch because the
    * caller's next move is always to rebase onto it, and a follow-up read would race exactly the
    * way the first one did.
    */
  final case class Conflict(current: Option[Array[Byte]]) extends ConditionalWriteResult

  /** The writer's authority over this key has been revoked. Reserved; see the class scaladoc. */
  case object Fenced extends ConditionalWriteResult
}
