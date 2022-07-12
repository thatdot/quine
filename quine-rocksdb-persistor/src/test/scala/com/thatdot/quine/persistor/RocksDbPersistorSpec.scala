package com.thatdot.quine.persistor

import akka.actor.CoordinatedShutdown
import org.apache.commons.io.FileUtils

import java.nio.file.Files
import java.util.Properties

class RocksDbPersistorSpec extends PersistenceAgentSpec {

  /** Tests should run if RocksDB could be started or if in CI (in CI, we want
    * to know if tests couldn't run).
    */
  override val runnable: Boolean = sys.env.contains("CI") || RocksDbPersistor.loadRocksDbLibrary()

  lazy val persistor: PersistenceAgent =
    if (RocksDbPersistor.loadRocksDbLibrary()) {
      val f = Files.createTempDirectory("rocks.db")
      CoordinatedShutdown(system).addJvmShutdownHook(() => FileUtils.forceDelete(f.toFile))
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
