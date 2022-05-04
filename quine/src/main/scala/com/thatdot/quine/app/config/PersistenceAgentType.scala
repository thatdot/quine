package com.thatdot.quine.app.config

import java.io.File
import java.net.InetSocketAddress

import scala.concurrent.duration.{DurationInt, FiniteDuration}

import akka.actor.ActorSystem

import com.datastax.oss.driver.api.core.DefaultConsistencyLevel
import com.typesafe.scalalogging.LazyLogging

import com.thatdot.quine.app.Metrics
import com.thatdot.quine.persistor._
import com.thatdot.quine.persistor.cassandra.vanilla.CassandraPersistor

/** Options for persistence */
sealed abstract class PersistenceAgentType {

  /** Does the persistor use instance storage */
  def isLocal: Boolean

  /** Size of the bloom filter, if enabled (not all persistors even support this) */
  def bloomFilterSize: Option[Long]

  /** Construct a persistence agent */
  def persistor(persistenceConfig: PersistenceConfig)(implicit system: ActorSystem): PersistenceAgent
}
object PersistenceAgentType {

  case object Empty extends PersistenceAgentType {
    def isLocal: Boolean = false

    def bloomFilterSize = None

    def persistor(persistenceConfig: PersistenceConfig)(implicit system: ActorSystem): PersistenceAgent =
      new EmptyPersistor(persistenceConfig)
  }

  case object InMemory extends PersistenceAgentType {
    def isLocal: Boolean = true

    def bloomFilterSize = None

    def persistor(persistenceConfig: PersistenceConfig)(implicit system: ActorSystem): PersistenceAgent =
      new InMemoryPersistor(persistenceConfig = persistenceConfig)
  }

  final case class RocksDb(
    filepath: File,
    writeAheadLog: Boolean = true,
    syncAllWrites: Boolean = false,
    createParentDir: Boolean = false,
    bloomFilterSize: Option[Long] = None
  ) extends PersistenceAgentType
      with LazyLogging {
    def isLocal: Boolean = true

    def persistor(persistenceConfig: PersistenceConfig)(implicit system: ActorSystem): PersistenceAgent = {
      if (createParentDir) {
        val parentDir = filepath.getAbsoluteFile.getParentFile
        if (parentDir.mkdirs())
          logger.warn("{} did not exist; created.", parentDir)
      }
      val db =
        try new RocksDbPersistor(
          filepath.getAbsolutePath,
          writeAheadLog,
          syncAllWrites,
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

      BloomFilteredPersistor.maybeBloomFilter(bloomFilterSize, db, persistenceConfig)
    }
  }

  final case class MapDb(
    filepath: Option[File],
    numberPartitions: Int = 1,
    writeAheadLog: Boolean = false,
    commitInterval: FiniteDuration = 10.seconds,
    createParentDir: Boolean = false,
    bloomFilterSize: Option[Long] = None
  ) extends PersistenceAgentType {
    def isLocal: Boolean = true

    def persistor(persistenceConfig: PersistenceConfig)(implicit system: ActorSystem): PersistenceAgent = {
      val interval = if (writeAheadLog) Some(commitInterval) else None

      def dbForPath(dbPath: MapDbPersistor.DbPath) =
        new MapDbPersistor(dbPath, writeAheadLog, interval, persistenceConfig, Metrics)

      def possiblyShardedDb(basePath: Option[File]) =
        if (numberPartitions == 1) {
          dbForPath(basePath match {
            case None => MapDbPersistor.TemporaryDb
            case Some(p) => MapDbPersistor.PersistedDb.makeDirIfNotExists(createParentDir, p)
          })
        } else {
          val dbPaths: Vector[MapDbPersistor.DbPath] = basePath match {
            case None => Vector.fill(numberPartitions)(MapDbPersistor.TemporaryDb)
            case Some(p) =>
              val name = p.getName
              val parent = p.getParent

              Vector.tabulate(numberPartitions) { (idx: Int) =>
                MapDbPersistor.PersistedDb.makeDirIfNotExists(createParentDir, new File(parent, s"part$idx.$name"))
              }
          }

          new ShardedPersistor(dbPaths.map(dbForPath), persistenceConfig)
        }

      BloomFilteredPersistor.maybeBloomFilter(bloomFilterSize, possiblyShardedDb(filepath), persistenceConfig)
    }
  }

  val defaultCassandraPort = 9042
  def defaultCassandraAddress: List[InetSocketAddress] =
    sys.env
      .getOrElse("CASSANDRA_ENDPOINTS", s"localhost:$defaultCassandraPort")
      .split(',')
      .map(Address.parseHostAndPort(_, defaultCassandraPort))
      .toList

  final case class Cassandra(
    keyspace: String = sys.env.getOrElse("CASSANDRA_KEYSPACE", "quine"),
    replicationFactor: Int = Integer.parseUnsignedInt(sys.env.getOrElse("CASSANDRA_REPLICATION_FACTOR", "1")),
    readConsistency: DefaultConsistencyLevel = DefaultConsistencyLevel.LOCAL_QUORUM,
    writeConsistency: DefaultConsistencyLevel = DefaultConsistencyLevel.LOCAL_QUORUM,
    endpoints: List[InetSocketAddress] = defaultCassandraAddress,
    localDatacenter: String = "datacenter1",
    insertTimeout: FiniteDuration = 10.seconds,
    selectTimeout: FiniteDuration = 10.seconds,
    shouldCreateTables: Boolean = true,
    shouldCreateKeyspace: Boolean = true,
    bloomFilterSize: Option[Long] = None,
    snapshotPartMaxSizeBytes: Int = 1000000
  ) extends PersistenceAgentType {
    def isLocal: Boolean = false

    def persistor(persistenceConfig: PersistenceConfig)(implicit system: ActorSystem): PersistenceAgent = {
      val db = new CassandraPersistor(
        persistenceConfig,
        keyspace,
        replicationFactor,
        readConsistency,
        writeConsistency,
        endpoints,
        localDatacenter,
        insertTimeout,
        selectTimeout,
        shouldCreateTables,
        shouldCreateKeyspace,
        Some(Metrics),
        snapshotPartMaxSizeBytes
      )
      BloomFilteredPersistor.maybeBloomFilter(bloomFilterSize, db, persistenceConfig)
    }
  }
}
