package com.thatdot.quine.persistor

import java.nio.file.Files
import java.util.Properties
import akka.actor.{ActorSystem, CoordinatedShutdown}
import com.thatdot.quine.graph.HistoricalQueryTests
import org.apache.commons.io.FileUtils

class RocksDbPersistorTests extends HistoricalQueryTests {

  override val runnable: Boolean = RocksDbPersistor.loadRocksDbLibrary()

  override def makePersistor(system: ActorSystem): PersistenceAgent =
    if (RocksDbPersistor.loadRocksDbLibrary()) {
      val f = Files.createTempDirectory("rocks.db")
      CoordinatedShutdown(system).addJvmShutdownHook(() => {
        FileUtils.forceDelete(f.toFile) // cleanup when actor system is shutdown
      })

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
