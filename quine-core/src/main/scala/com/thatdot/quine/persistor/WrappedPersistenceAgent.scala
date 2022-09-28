package com.thatdot.quine.persistor

abstract class WrappedPersistenceAgent(val underlying: PersistenceAgent) extends PersistenceAgent
object WrappedPersistenceAgent {
  def unwrap(persistenceAgent: PersistenceAgent): PersistenceAgent = persistenceAgent match {
    case wrapped: WrappedPersistenceAgent => wrapped.underlying
    case other => other
  }
}
