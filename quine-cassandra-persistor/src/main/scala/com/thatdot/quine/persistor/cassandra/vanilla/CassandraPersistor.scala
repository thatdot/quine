package com.thatdot.quine.persistor.cassandra.vanilla

import java.net.InetSocketAddress

import scala.concurrent.Future
import scala.jdk.CollectionConverters._
import scala.jdk.OptionConverters._

import akka.stream.Materializer

import com.codahale.metrics.MetricRegistry
import com.datastax.oss.driver.api.core.cql.SimpleStatement
import com.datastax.oss.driver.api.core.{CqlSession, CqlSessionBuilder, InvalidKeyspaceException}
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder.createKeyspace

import com.thatdot.quine.persistor.cassandra.support.CassandraStatementSettings
import com.thatdot.quine.persistor.cassandra.{JournalsTableDefinition, SnapshotsTableDefinition}
import com.thatdot.quine.persistor.{PersistenceConfig, cassandra}

/** Persistence implementation backed by Cassandra.
  *
  * @param keyspace The keyspace the quine tables should live in.
  * @param replicationFactor
  * @param readConsistency
  * @param writeConsistency
  * @param writeTimeout How long to wait for a response when running an INSERT statement.
  * @param readTimeout How long to wait for a response when running a SELECT statement.
  * @param endpoints address(s) (host and port) of the Cassandra cluster to connect to.
  * @param localDatacenter If endpoints are specified, this argument is required. Default value on a new Cassandra install is 'datacenter1'.
  * @param shouldCreateTables Whether or not to create the required tables if they don't already exist.
  * @param shouldCreateKeyspace Whether or not to create the specified keyspace if it doesn't already exist. If it doesn't exist, it'll run {{{CREATE KEYSPACE IF NOT EXISTS `keyspace` WITH replication={'class':'SimpleStrategy','replication_factor':1}}}}
  */
class CassandraPersistor(
  persistenceConfig: PersistenceConfig,
  keyspace: String,
  replicationFactor: Int,
  readSettings: CassandraStatementSettings,
  writeSettings: CassandraStatementSettings,
  endpoints: List[InetSocketAddress],
  localDatacenter: String,
  shouldCreateTables: Boolean,
  shouldCreateKeyspace: Boolean,
  metricRegistry: Option[MetricRegistry],
  snapshotPartMaxSizeBytes: Int
)(implicit
  materializer: Materializer
) extends cassandra.CassandraPersistor(
      persistenceConfig,
      readSettings,
      writeSettings,
      shouldCreateTables,
      shouldCreateKeyspace,
      snapshotPartMaxSizeBytes
    ) {

  protected val journalsTableDef: JournalsTableDefinition = Journals
  protected val snapshotsTableDef: SnapshotsTableDefinition = Snapshots

  // This is mutable, so needs to be a def to get a new one w/out prior settings.
  private def sessionBuilder: CqlSessionBuilder = CqlSession.builder
    .addContactPoints(endpoints.asJava)
    .withLocalDatacenter(localDatacenter)
    .withMetricRegistry(metricRegistry.orNull)

  private def createQualifiedSession: CqlSession = sessionBuilder
    .withKeyspace(keyspace)
    .build

  // CREATE KEYSPACE IF NOT EXISTS `keyspace` WITH replication={'class':'SimpleStrategy','replication_factor':1}
  private val createKeyspaceStatement: SimpleStatement =
    createKeyspace(keyspace).ifNotExists.withSimpleStrategy(replicationFactor).build

  protected val session: CqlSession =
    try {
      val sess = createQualifiedSession
      // Log a warning if the Cassandra keyspace replication factor does not match Quine configuration
      for {
        keyspaceMetadata <- sess.getMetadata.getKeyspace(keyspace).toScala
        keyspaceReplicationConfig = keyspaceMetadata.getReplication.asScala.toMap
        clazz <- keyspaceReplicationConfig.get("class")
        factor <- keyspaceReplicationConfig.get("replication_factor")
        if clazz == "org.apache.cassandra.locator.SimpleStrategy" && factor.toInt != replicationFactor
      } logger.info(
        s"Unexpected replication factor: $factor (expected: $replicationFactor) for Cassandra keyspace: $keyspace"
      )
      sess
    } catch {
      case _: InvalidKeyspaceException if shouldCreateKeyspace =>
        val sess = sessionBuilder.build
        sess.execute(createKeyspaceStatement)
        sess.close()
        createQualifiedSession
    }

  protected def verifyTable(session: CqlSession)(tableName: String): Future[Unit] = Future.unit
}
