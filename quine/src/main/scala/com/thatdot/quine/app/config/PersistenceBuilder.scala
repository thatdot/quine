package com.thatdot.quine.app.config

import java.io.File
import java.util.Properties

import scala.concurrent.Await
import scala.concurrent.duration._

import org.apache.pekko.actor.ActorSystem

import com.thatdot.common.logging.Log.LogConfig
import com.thatdot.quine.app.Metrics
import com.thatdot.quine.app.config.PersistenceAgentType._
import com.thatdot.quine.persistor._
import com.thatdot.quine.persistor.cassandra.aws.PrimeKeyspacesPersistor
import com.thatdot.quine.persistor.cassandra.support.CassandraStatementSettings
import com.thatdot.quine.persistor.cassandra.vanilla.PrimeCassandraPersistor
import com.thatdot.quine.util.QuineDispatchers

/** Type aliases for the builder functions used by PersistenceBuilder.
  * Each builder function takes specific configuration and returns a PrimePersistor.
  */
object PersistenceBuilderTypes {

  /** Builder for empty/no-op persistence */
  type EmptyBuilder = (PersistenceConfig, ActorSystem, LogConfig) => PrimePersistor

  /** Builder for in-memory persistence */
  type InMemoryBuilder = (PersistenceConfig, ActorSystem, LogConfig) => PrimePersistor

  /** Builder for RocksDB persistence */
  type RocksDbBuilder = (RocksDb, PersistenceConfig, File, QuineDispatchers, ActorSystem, LogConfig) => PrimePersistor

  /** Builder for MapDB persistence */
  type MapDbBuilder = (MapDb, PersistenceConfig, QuineDispatchers, ActorSystem, LogConfig) => PrimePersistor

  /** Builder for Cassandra persistence */
  type CassandraBuilder = (Cassandra, PersistenceConfig, String, ActorSystem, LogConfig) => PrimePersistor

  /** Builder for AWS Keyspaces persistence */
  type KeyspacesBuilder = (Keyspaces, PersistenceConfig, String, ActorSystem, LogConfig) => PrimePersistor

  /** Builder for ClickHouse persistence */
  type ClickHouseBuilder = (ClickHouse, PersistenceConfig, ActorSystem, LogConfig) => PrimePersistor
}

import PersistenceBuilderTypes._

/** Case class for building persistence agents from configuration using composition.
  *
  * This class provides a unified pattern for constructing persistors across different products
  * (Quine, Novelty, Enterprise). It uses composition to allow products to customize behavior by
  * providing product-specific builder functions for each persistence type.
  *
  * @param defaultKeyspace Default Cassandra/Keyspaces keyspace name for this product.
  *                        Used when no keyspace is explicitly configured.
  * @param defaultRocksDbFilepath Default RocksDb file path for this product.
  *                               Used when no filepath is explicitly configured.
  * @param buildEmpty Builder for empty/no-op persistence
  * @param buildInMemory Builder for in-memory persistence
  * @param buildRocksDb Builder for RocksDB persistence
  * @param buildMapDb Builder for MapDB persistence
  * @param buildCassandra Builder for Cassandra persistence
  * @param buildKeyspaces Builder for AWS Keyspaces persistence
  * @param buildClickHouse Builder for ClickHouse persistence
  *
  * @see [[PersistenceBuilder]] for the Quine implementation
  * @see [[com.thatdot.novelty.app.config.PersistenceBuilder]] for the Novelty implementation
  * @see [[com.thatdot.quine.app.config.EnterprisePersistenceBuilder]] for the Enterprise implementation
  */
case class PersistenceBuilder(
  defaultKeyspace: String,
  defaultRocksDbFilepath: File,
  buildEmpty: EmptyBuilder = PersistenceBuilder.defaultBuildEmpty,
  buildInMemory: InMemoryBuilder = PersistenceBuilder.defaultBuildInMemory,
  buildRocksDb: RocksDbBuilder = PersistenceBuilder.defaultBuildRocksDb,
  buildMapDb: MapDbBuilder = PersistenceBuilder.defaultBuildMapDb,
  buildCassandra: CassandraBuilder = PersistenceBuilder.defaultBuildCassandra,
  buildKeyspaces: KeyspacesBuilder = PersistenceBuilder.defaultBuildKeyspaces,
  buildClickHouse: ClickHouseBuilder = PersistenceBuilder.defaultBuildClickHouse,
) {

  /** Build a PrimePersistor from the given persistence agent type and configuration.
    *
    * Dispatches to the appropriate builder function based on the configured persistence type.
    */
  def build(pt: PersistenceAgentType, persistenceConfig: PersistenceConfig)(implicit
    system: ActorSystem,
    logConfig: LogConfig,
  ): PrimePersistor = {
    val quineDispatchers = new QuineDispatchers(system)
    pt match {
      case Empty => buildEmpty(persistenceConfig, system, logConfig)
      case InMemory => buildInMemory(persistenceConfig, system, logConfig)
      case r: RocksDb => buildRocksDb(r, persistenceConfig, defaultRocksDbFilepath, quineDispatchers, system, logConfig)
      case m: MapDb => buildMapDb(m, persistenceConfig, quineDispatchers, system, logConfig)
      case c: Cassandra =>
        buildCassandra(c, persistenceConfig, c.keyspace.getOrElse(defaultKeyspace), system, logConfig)
      case c: Keyspaces =>
        buildKeyspaces(c, persistenceConfig, c.keyspace.getOrElse(defaultKeyspace), system, logConfig)
      case c: ClickHouse => buildClickHouse(c, persistenceConfig, system, logConfig)
    }
  }
}

/** Companion object containing default builder implementations.
  *
  * These defaults can be used directly or overridden when constructing a PersistenceBuilder.
  */
object PersistenceBuilder {

  /** Default builder for empty persistence (discards all data). */
  val defaultBuildEmpty: EmptyBuilder = { (persistenceConfig, system, logConfig) =>
    implicit val s: ActorSystem = system
    implicit val lc: LogConfig = logConfig
    new StatelessPrimePersistor(persistenceConfig, None, new EmptyPersistor(_, _))
  }

  /** Default builder for in-memory persistence (lost on shutdown). */
  val defaultBuildInMemory: InMemoryBuilder = { (persistenceConfig, system, logConfig) =>
    implicit val s: ActorSystem = system
    implicit val lc: LogConfig = logConfig
    new StatelessPrimePersistor(
      persistenceConfig,
      None,
      (pc, ns) => new InMemoryPersistor(persistenceConfig = pc, namespace = ns),
    )
  }

  /** Default builder for RocksDB persistence. */
  val defaultBuildRocksDb: RocksDbBuilder = {
    (r, persistenceConfig, defaultFilepath, quineDispatchers, system, logConfig) =>
      implicit val s: ActorSystem = system
      implicit val lc: LogConfig = logConfig
      new RocksDbPrimePersistor(
        r.createParentDir,
        r.filepath.getOrElse(defaultFilepath),
        r.writeAheadLog,
        r.syncAllWrites,
        new Properties(),
        persistenceConfig,
        r.bloomFilterSize,
        quineDispatchers.blockingDispatcherEC,
      )
  }

  /** Default builder for MapDB persistence. */
  val defaultBuildMapDb: MapDbBuilder = { (m, persistenceConfig, quineDispatchers, system, logConfig) =>
    implicit val s: ActorSystem = system
    implicit val lc: LogConfig = logConfig
    m.filepath match {
      case Some(path) =>
        new PersistedMapDbPrimePersistor(
          m.createParentDir,
          path,
          m.writeAheadLog,
          m.numberPartitions,
          m.commitInterval,
          Metrics,
          persistenceConfig,
          m.bloomFilterSize,
          quineDispatchers,
        )
      case None =>
        new TempMapDbPrimePersistor(
          m.writeAheadLog,
          m.numberPartitions,
          m.commitInterval,
          Metrics,
          persistenceConfig,
          m.bloomFilterSize,
          quineDispatchers,
        )
    }
  }

  /** Default builder for Cassandra persistence. */
  val defaultBuildCassandra: CassandraBuilder = { (c, persistenceConfig, keyspace, system, logConfig) =>
    implicit val s: ActorSystem = system
    implicit val lc: LogConfig = logConfig
    Await.result(
      PrimeCassandraPersistor.create(
        persistenceConfig,
        c.bloomFilterSize,
        c.endpoints,
        c.localDatacenter,
        c.replicationFactor,
        keyspace,
        c.shouldCreateKeyspace,
        c.shouldCreateTables,
        CassandraStatementSettings(c.readConsistency, c.readTimeout),
        CassandraStatementSettings(c.writeConsistency, c.writeTimeout),
        c.snapshotPartMaxSizeBytes,
        Some(Metrics),
      ),
      90.seconds,
    )
  }

  /** Default builder for AWS Keyspaces persistence. */
  val defaultBuildKeyspaces: KeyspacesBuilder = { (c, persistenceConfig, keyspace, system, logConfig) =>
    implicit val s: ActorSystem = system
    implicit val lc: LogConfig = logConfig
    Await.result(
      PrimeKeyspacesPersistor.create(
        persistenceConfig,
        c.bloomFilterSize,
        keyspace,
        c.awsRegion,
        c.awsRoleArn,
        CassandraStatementSettings(c.readConsistency, c.readTimeout),
        c.writeTimeout,
        c.shouldCreateKeyspace,
        c.shouldCreateTables,
        Some(Metrics),
        c.snapshotPartMaxSizeBytes,
      ),
      91.seconds,
    )
  }

  /** Default builder for ClickHouse persistence.
    * By default, ClickHouse is not available - only in Enterprise.
    */
  val defaultBuildClickHouse: ClickHouseBuilder = { (_, _, _, _) =>
    throw new IllegalArgumentException(
      "ClickHouse is not available in this product. If you are interested in using ClickHouse, please contact us.",
    )
  }
}
