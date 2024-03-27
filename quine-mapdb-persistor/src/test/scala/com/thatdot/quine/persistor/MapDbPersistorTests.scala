package com.thatdot.quine.persistor

import scala.concurrent.ExecutionContext

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.Materializer

import com.thatdot.quine.graph.HistoricalQueryTests
import com.thatdot.quine.util.FromSingleExecutionContext

class MapDbPersistorTests extends HistoricalQueryTests {

  override def makePersistor(system: ActorSystem): PrimePersistor = new StatelessPrimePersistor(
    PersistenceConfig(),
    None,
    (pc, ns) =>
      new MapDbPersistor(
        filePath = MapDbPersistor.InMemoryDb,
        ns,
        persistenceConfig = pc,
        ExecutionContext = new FromSingleExecutionContext(ExecutionContext.parasitic),
        scheduler = system.scheduler
      )
  )(Materializer.matFromSystem(system))

}
