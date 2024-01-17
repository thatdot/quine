package com.thatdot.quine.persistor

import org.apache.pekko.actor.ActorSystem

import com.thatdot.quine.graph.HistoricalQueryTests

class MapDbPersistorTests extends HistoricalQueryTests {

  override def makePersistor(system: ActorSystem): PersistenceAgent =
    new MapDbPersistor(filePath = MapDbPersistor.InMemoryDb)(system)
}
