package com.thatdot.quine.persistor

// TODO this wrapper only works for 1:1 persistors (eg, no dice on SplitPersistor or ShardedPersistor)
// TODO add some safety tools (deprecation warnings, maybe?) to encourage usage of this utility over more natural
//      first-pass implementations
abstract class WrappedPersistenceAgent(val underlying: PersistenceAgent) extends PersistenceAgent
object WrappedPersistenceAgent {
  @scala.annotation.tailrec
  def unwrap(persistenceAgent: PersistenceAgent): PersistenceAgent = persistenceAgent match {
    case wrapped: WrappedPersistenceAgent => unwrap(wrapped.underlying)
    case other => other
  }
}
