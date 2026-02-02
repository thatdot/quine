package com.thatdot.quine.persistor

// TODO this wrapper only works for 1:1 persistors (eg, no dice on ShardedPersistor)
// TODO add some safety tools (deprecation warnings, maybe?) to encourage usage of this utility over more natural
//      first-pass implementations
abstract class WrappedPersistenceAgent(val underlying: NamespacedPersistenceAgent) extends NamespacedPersistenceAgent
object WrappedPersistenceAgent {
  @scala.annotation.tailrec
  def unwrap(persistenceAgent: NamespacedPersistenceAgent): NamespacedPersistenceAgent = persistenceAgent match {
    case wrapped: WrappedPersistenceAgent => unwrap(wrapped.underlying)
    case other => other
  }
}
