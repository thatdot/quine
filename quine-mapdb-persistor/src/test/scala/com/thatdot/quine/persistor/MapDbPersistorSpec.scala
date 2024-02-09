package com.thatdot.quine.persistor

import com.thatdot.quine.util.QuineDispatchers

class MapDbPersistorSpec extends PersistenceAgentSpec {

  lazy val persistor: PersistenceAgent =
    new MapDbPersistor(
      filePath = MapDbPersistor.TemporaryDb,
      None,
      writeAheadLog = false,
      transactionCommitInterval = None,
      quineDispatchers = new QuineDispatchers(system),
      scheduler = system.scheduler
    )
}
