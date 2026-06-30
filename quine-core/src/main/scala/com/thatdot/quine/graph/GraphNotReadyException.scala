package com.thatdot.quine.graph

import com.thatdot.quine.model.Milliseconds
import com.thatdot.quine.util.QuineError

/** Exception thrown when a graph operation is attempted but the graph is not
  * ready
  */
class GraphNotReadyException(val atTime: Milliseconds = Milliseconds.currentTime())
    extends IllegalStateException()
    with QuineError {

  override def getMessage: String =
    s"Graph not ready at time: ${atTime.millis}"
}

case class ShardNotAvailableException(msg: String) extends NoSuchElementException(msg) with QuineError

/** Thrown when a query references a user-defined function or procedure that resolved against the
  * registry at compile time on the originating member, but is not (yet) registered on the member
  * evaluating it.
  *
  * Unknown functions/procedures are rejected at compile time — `resolveFunctions` / `resolveCalls`
  * only produce a `Func.UserDefined` / `Proc.UserDefined` for names found in the registry — so a
  * runtime miss can only mean this member's registry is not yet fully populated. That is a transient
  * condition during the member's startup (it warms its UDF/UDP registry shortly after it begins
  * hosting shards), so it is retryable; see
  * [[com.thatdot.quine.app.util.AtLeastOnceCypherQuery.RetriableQueryFailure]].
  *
  * @param kind    "function" or "procedure" (used only for the message)
  * @param udfName the name that could not be resolved on this member
  */
case class UnregisteredUserDefinedException(kind: String, udfName: String)
    extends NoSuchElementException(s"User-defined $kind '$udfName' is not yet registered on this member")
    with QuineError
