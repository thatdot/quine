package com.thatdot.quine.persistor

class InMemoryPersistorSpec extends PersistenceAgentSpec {

  val persistor: PrimePersistor = InMemoryPersistor.namespacePersistor
}
