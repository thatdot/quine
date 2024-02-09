package com.thatdot.quine.persistor

class InMemoryPersistorSpec extends PersistenceAgentSpec {

  val persistor: PersistenceAgent = InMemoryPersistor.empty()
}
