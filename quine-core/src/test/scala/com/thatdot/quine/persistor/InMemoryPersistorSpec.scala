package com.thatdot.quine.persistor

class InMemoryPersistorSpec extends PersistenceAgentSpec {

  lazy val persistor: PersistenceAgent = InMemoryPersistor.empty
}
