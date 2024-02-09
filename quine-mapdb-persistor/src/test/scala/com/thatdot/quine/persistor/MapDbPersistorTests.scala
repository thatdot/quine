package com.thatdot.quine.persistor

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.Materializer

import com.thatdot.quine.graph.HistoricalQueryTests
import com.thatdot.quine.util.QuineDispatchers

class MapDbPersistorTests extends HistoricalQueryTests {

  override def makePersistor(system: ActorSystem): PrimePersistor = new StatelessPrimePersistor(
    PersistenceConfig(),
    None,
    (pc, ns) =>
      new MapDbPersistor(
        filePath = MapDbPersistor.InMemoryDb,
        ns,
        persistenceConfig = pc,
        quineDispatchers = new QuineDispatchers(system),
        scheduler = system.scheduler
      )
  )(Materializer.matFromSystem(system))

}
