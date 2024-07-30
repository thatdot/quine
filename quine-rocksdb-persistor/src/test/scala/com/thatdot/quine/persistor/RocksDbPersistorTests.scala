package com.thatdot.quine.persistor

import java.nio.file.Files
import java.util.Properties

import scala.concurrent.ExecutionContext

import org.apache.pekko.actor.{ActorSystem, CoordinatedShutdown}
import org.apache.pekko.stream.Materializer

import org.apache.commons.io.FileUtils

import com.thatdot.quine.graph.HistoricalQueryTests
import com.thatdot.quine.util.Log._

class RocksDbPersistorTests extends HistoricalQueryTests()(LogConfig.testing) {

  override val runnable: Boolean = RocksDbPersistor.loadRocksDbLibrary()

  override def makePersistor(system: ActorSystem): PrimePersistor =
    if (RocksDbPersistor.loadRocksDbLibrary()) {
      val f = Files.createTempDirectory("rocks.db")
      CoordinatedShutdown(system).addJvmShutdownHook(() => FileUtils.forceDelete(f.toFile))
      new RocksDbPrimePersistor(
        createParentDir = true,
        topLevelPath = f.toFile,
        writeAheadLog = true,
        syncWrites = false,
        dbOptionProperties = new Properties(),
        persistenceConfig = PersistenceConfig(),
        bloomFilterSize = None,
        ioDispatcher = ExecutionContext.parasitic
      )(Materializer.matFromSystem(system), logConfig)
    } else {
      new StatelessPrimePersistor(PersistenceConfig(), None, new EmptyPersistor(_, _))(
        Materializer.matFromSystem(system),
        logConfig
      )
    }
}
