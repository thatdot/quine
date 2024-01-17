package com.thatdot.quine.persistor

import java.nio.file.Files
import java.util.Properties

import org.apache.pekko.actor.{ActorSystem, CoordinatedShutdown}

import org.apache.commons.io.FileUtils

import com.thatdot.quine.graph.HistoricalQueryTests

class RocksDbPersistorTests extends HistoricalQueryTests {

  override val runnable: Boolean = RocksDbPersistor.loadRocksDbLibrary()

  override def makePersistor(system: ActorSystem): PersistenceAgent =
    if (RocksDbPersistor.loadRocksDbLibrary()) {
      val f = Files.createTempDirectory("rocks.db")
      CoordinatedShutdown(system).addJvmShutdownHook(() => FileUtils.forceDelete(f.toFile))
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
