package com.thatdot.quine.persistor

import java.nio.file.Files
import java.util.Properties

class RocksDbPersistorSpec extends PersistenceAgentSpec {

  override val runnable: Boolean = RocksDbPersistor.loadRocksDbLibrary()

  lazy val persistor: PersistenceAgent =
    if (RocksDbPersistor.loadRocksDbLibrary()) {
      // TODO: delete on exit
      val f = Files.createTempDirectory("rocks.db")
      new RocksDbPersistor(
        filePath = f.toString,
        writeAheadLog = true,
        syncWrites = false,
        dbOptionProperties = new Properties(),
        PersistenceConfig()
      )
    } else {
      EmptyPersistor
    }
}
