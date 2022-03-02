package com.thatdot.quine.graph

/** A node irrecoverably failed to wake up. May be thrown by the node or the shard.
  */
class NodeWakeupFailedException(msg: String, causeOpt: Option[Throwable] = None)
    extends RuntimeException(msg, causeOpt.orNull) {
  def this(msg: String, cause: Throwable) = this(msg, Some(cause))
}
