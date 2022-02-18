package com.thatdot.quine.graph

import akka.actor.ActorSystem

import com.thatdot.quine.persistor.{InMemoryPersistor, PersistenceAgent}

class InMemoryPersistorTests extends HistoricalQueryTests {

  override def makePersistor(system: ActorSystem): PersistenceAgent = InMemoryPersistor.empty
}
