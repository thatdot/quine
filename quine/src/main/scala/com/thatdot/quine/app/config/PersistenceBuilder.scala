package com.thatdot.quine.app.config

import java.io.File

import akka.actor.ActorSystem

import com.typesafe.scalalogging.LazyLogging

import com.thatdot.quine.app.Metrics
import com.thatdot.quine.app.config.PersistenceAgentType.{Cassandra, MapDb}
import com.thatdot.quine.persistor._
import com.thatdot.quine.persistor.cassandra.vanilla.{CassandraPersistor, CassandraStatementSettings}

/** Options for persistence */
object PersistenceBuilder extends LazyLogging {
  def build(pt: PersistenceAgentType, persistenceConfig: PersistenceConfig)(implicit
    system: ActorSystem
  ): PersistenceAgent =
    pt match {
      case PersistenceAgentType.Empty => new EmptyPersistor(persistenceConfig)
      case PersistenceAgentType.InMemory => new InMemoryPersistor(persistenceConfig = persistenceConfig)
      case r: PersistenceAgentType.RocksDb =>
        if (r.createParentDir) {
          val parentDir = r.filepath.getAbsoluteFile.getParentFile
          if (parentDir.mkdirs())
            logger.warn(s"Configured persistence directory: $parentDir did not exist; created.")
        }
        val db =
          try new RocksDbPersistor(
            r.filepath.getAbsolutePath,
            r.writeAheadLog,
            r.syncAllWrites,
            new java.util.Properties(),
            persistenceConfig
          )
          catch {
            case err: UnsatisfiedLinkError =>
              logger.error(
                "RocksDB native library could not be loaded. " +
                "Consider using MapDB instead by specifying `quine.store.type=map-db`",
                err
              )
              sys.exit(1)
          }

        BloomFilteredPersistor.maybeBloomFilter(r.bloomFilterSize, db, persistenceConfig)

      case m: MapDb =>
        val interval = if (m.writeAheadLog) Some(m.commitInterval) else None

        def dbForPath(dbPath: MapDbPersistor.DbPath) =
          new MapDbPersistor(dbPath, m.writeAheadLog, interval, persistenceConfig, Metrics)

        def possiblyShardedDb(basePath: Option[File]) =
          if (m.numberPartitions == 1) {
            dbForPath(basePath match {
              case None => MapDbPersistor.TemporaryDb
              case Some(p) => MapDbPersistor.PersistedDb.makeDirIfNotExists(m.createParentDir, p)
            })
          } else {
            val dbPaths: Vector[MapDbPersistor.DbPath] = basePath match {
              case None => Vector.fill(m.numberPartitions)(MapDbPersistor.TemporaryDb)
              case Some(p) =>
                val name = p.getName
                val parent = p.getParent

                Vector.tabulate(m.numberPartitions) { (idx: Int) =>
                  MapDbPersistor.PersistedDb.makeDirIfNotExists(m.createParentDir, new File(parent, s"part$idx.$name"))
                }
            }

            new ShardedPersistor(dbPaths.map(dbForPath), persistenceConfig)
          }

        BloomFilteredPersistor.maybeBloomFilter(m.bloomFilterSize, possiblyShardedDb(m.filepath), persistenceConfig)

      case c: Cassandra =>
        val db = new CassandraPersistor(
          persistenceConfig,
          c.keyspace,
          c.replicationFactor,
          CassandraStatementSettings(
            c.readConsistency,
            c.readTimeout
          ),
          CassandraStatementSettings(
            c.writeConsistency,
            c.writeTimeout
          ),
          c.endpoints,
          c.localDatacenter,
          c.shouldCreateTables,
          c.shouldCreateKeyspace,
          Some(Metrics),
          c.snapshotPartMaxSizeBytes
        )
        BloomFilteredPersistor.maybeBloomFilter(c.bloomFilterSize, db, persistenceConfig)
    }

}
