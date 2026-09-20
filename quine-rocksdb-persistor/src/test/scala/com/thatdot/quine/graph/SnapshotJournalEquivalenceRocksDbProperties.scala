package com.thatdot.quine.graph

import java.nio.file.Files

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext}

import org.apache.pekko.stream.Materializer

import com.thatdot.quine.persistor.RocksDbPrimePersistor
import com.thatdot.quine.util.TestLogging._

/** The equivalence property on RocksDB: the store whose journal order, snapshot keys and read-back the in-memory
  * persistor cannot exercise. One fresh directory per graph.
  *
  * IO runs inline (`parasitic`), as the persistor's own tests run it: a pool dispatcher here is one the test is
  * also waiting on, and a wake that blocks on a store read from that pool never comes back.
  */
class SnapshotJournalEquivalenceRocksDbProperties
    extends SnapshotJournalEquivalenceProperties(
      "rocksdb",
      (pc, system) => {
        val persistor = new RocksDbPrimePersistor(
          topLevelPath = Files.createTempDirectory("equivalence-rocksdb-").toFile,
          persistenceConfig = pc,
          ioDispatcher = ExecutionContext.parasitic,
        )(Materializer.matFromSystem(system), logConfig)
        Await.result(persistor.syncVersion(), 10.seconds)
        persistor
      },
    )
