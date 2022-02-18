package com.thatdot.quine.persistor

import java.nio.file.Files
import java.util.Properties

import akka.actor.ActorSystem

import com.thatdot.quine.graph.HistoricalQueryTests

class RocksDbPersistorTests extends HistoricalQueryTests {

  override val runnable: Boolean = RocksDbPersistor.loadRocksDbLibrary()

  override def makePersistor(system: ActorSystem): PersistenceAgent =
    if (RocksDbPersistor.loadRocksDbLibrary()) {
      // TODO: delete on exit
      val f = Files.createTempDirectory("rocks.db")
      new RocksDbPersistor(
        filePath = f.toString,
        writeAheadLog = true,
        syncWrites = false,
        dbOptionProperties = new Properties(),
        PersistenceConfig()
      )(system)
    } else {
      EmptyPersistor
    }
}
