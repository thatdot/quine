package com.thatdot.quine.app.config

import java.util.Properties

import scala.concurrent.Await
import scala.concurrent.duration._

import org.apache.pekko.stream.Materializer

import com.thatdot.common.logging.Log.LogConfig
import com.thatdot.quine.app.Metrics
import com.thatdot.quine.app.config.PersistenceAgentType.{Cassandra, ClickHouse, Keyspaces, MapDb}
import com.thatdot.quine.persistor._
import com.thatdot.quine.persistor.cassandra.aws.PrimeKeyspacesPersistor
import com.thatdot.quine.persistor.cassandra.support.CassandraStatementSettings
import com.thatdot.quine.persistor.cassandra.vanilla.PrimeCassandraPersistor
import com.thatdot.quine.util.QuineDispatchers

/** Options for persistence */
object PersistenceBuilder {
  def build(pt: PersistenceAgentType, persistenceConfig: PersistenceConfig)(implicit
    materializer: Materializer,
    logConfig: LogConfig,
  ): PrimePersistor = {
    val quineDispatchers = new QuineDispatchers(materializer.system)
    pt match {
      case PersistenceAgentType.Empty => new StatelessPrimePersistor(persistenceConfig, None, new EmptyPersistor(_, _))
      case PersistenceAgentType.InMemory =>
        new StatelessPrimePersistor(
          persistenceConfig,
          None,
          (pc, ns) => new InMemoryPersistor(persistenceConfig = pc, namespace = ns),
        )
      case r: PersistenceAgentType.RocksDb =>
        new RocksDbPrimePersistor(
          r.createParentDir,
          r.filepath,
          r.writeAheadLog,
          r.syncAllWrites,
          new Properties(),
          persistenceConfig,
          r.bloomFilterSize,
          quineDispatchers.blockingDispatcherEC,
        )

      case m: MapDb =>
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

      case c: Cassandra =>
        Await.result(
          PrimeCassandraPersistor.create(
            persistenceConfig,
            c.bloomFilterSize,
            c.endpoints,
            c.localDatacenter,
            c.replicationFactor,
            c.keyspace,
            c.shouldCreateKeyspace,
            c.shouldCreateTables,
            CassandraStatementSettings(
              c.readConsistency,
              c.readTimeout,
            ),
            CassandraStatementSettings(
              c.writeConsistency,
              c.writeTimeout,
            ),
            c.snapshotPartMaxSizeBytes,
            Some(Metrics),
          ),
          90.seconds,
        )

      case c: Keyspaces =>
        Await.result(
          PrimeKeyspacesPersistor.create(
            persistenceConfig,
            c.bloomFilterSize,
            c.keyspace,
            c.awsRegion,
            c.awsRoleArn,
            CassandraStatementSettings(
              c.readConsistency,
              c.readTimeout,
            ),
            c.writeTimeout,
            c.shouldCreateKeyspace,
            c.shouldCreateTables,
            Some(Metrics),
            c.snapshotPartMaxSizeBytes,
          ),
          91.seconds,
        )

      case _: ClickHouse =>
        throw new IllegalArgumentException(
          "ClickHouse is not available in Quine. If you are interested in using ClickHouse, please contact us to discuss upgrading to thatDot Streaming Graph.",
        )
    }
  }

}
