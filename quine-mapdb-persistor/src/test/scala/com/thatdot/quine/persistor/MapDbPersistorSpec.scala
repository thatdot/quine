package com.thatdot.quine.persistor

class MapDbPersistorSpec extends PersistenceAgentSpec {

  lazy val persistor: PersistenceAgent =
    new MapDbPersistor(
      filePath = MapDbPersistor.TemporaryDb,
      writeAheadLog = false,
      transactionCommitInterval = None,
      PersistenceConfig()
    )
}
