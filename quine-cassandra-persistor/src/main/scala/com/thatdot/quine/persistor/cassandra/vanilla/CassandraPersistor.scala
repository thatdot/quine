package com.thatdot.quine.persistor.cassandra.vanilla

import java.net.InetSocketAddress

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.jdk.CollectionConverters._
import scala.jdk.FutureConverters._

import org.apache.pekko.stream.Materializer

import cats.syntax.all._
import com.codahale.metrics.MetricRegistry
import com.datastax.oss.driver.api.core.cql.SimpleStatement
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata
import com.datastax.oss.driver.api.core.{CqlSession, CqlSessionBuilder, InvalidKeyspaceException}
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder.createKeyspace
import shapeless.syntax.std.tuple._

import com.thatdot.quine.graph.NamespaceId
import com.thatdot.quine.persistor.cassandra.support.CassandraStatementSettings
import com.thatdot.quine.persistor.cassandra.{Chunker, JournalsTableDefinition, NoOpChunker, SnapshotsTableDefinition}
import com.thatdot.quine.persistor.{PersistenceConfig, cassandra}
import com.thatdot.quine.util.CompletionException
import com.thatdot.quine.util.Log._
import com.thatdot.quine.util.Log.implicits._

abstract class AbstractGlobalCassandraPersistor[C <: PrimeCassandraPersistor](
  constructor: (
    PersistenceConfig,
    Option[Long],
    CqlSession,
    CassandraStatementSettings,
    CassandraStatementSettings,
    Boolean,
    Int,
    Materializer,
    LogConfig
  ) => C
) extends LazySafeLogging {

  /** @param endpoints           address(s) (host and port) of the Cassandra cluster to connect to.
    * @param localDatacenter      If endpoints are specified, this argument is required. Default value on a new Cassandra install is 'datacenter1'.
    * @param replicationFactor
    * @param keyspace             The keyspace the quine tables should live in.
    * @param shouldCreateKeyspace Whether or not to create the specified keyspace if it doesn't already exist. If it doesn't exist, it'll run {{{CREATE KEYSPACE IF NOT EXISTS `keyspace` WITH replication={'class':'SimpleStrategy','replication_factor':1}}}}
    * @param metricRegistry
    * @return
    */
  def create(
    persistenceConfig: PersistenceConfig,
    bloomFilterSize: Option[Long],
    endpoints: List[InetSocketAddress],
    localDatacenter: String,
    replicationFactor: Int,
    keyspace: String,
    shouldCreateKeyspace: Boolean,
    shouldCreateTables: Boolean,
    readSettings: CassandraStatementSettings,
    writeSettings: CassandraStatementSettings,
    snapshotPartMaxSizeBytes: Int,
    metricRegistry: Option[MetricRegistry]
  )(implicit materializer: Materializer, logConfig: LogConfig): Future[PrimeCassandraPersistor] = {

    // This is mutable, so needs to be a def to get a new one w/out prior settings.
    def sessionBuilder: CqlSessionBuilder = CqlSession.builder
      .addContactPoints(endpoints.asJava)
      .withLocalDatacenter(localDatacenter)
      .withMetricRegistry(metricRegistry.orNull)

    def createQualifiedSession(): Future[CqlSession] = sessionBuilder
      .withKeyspace(keyspace)
      .buildAsync()
      .asScala

    // CREATE KEYSPACE IF NOT EXISTS `keyspace` WITH replication={'class':'SimpleStrategy','replication_factor':1}
    val createKeyspaceStatement: SimpleStatement =
      createKeyspace(keyspace).ifNotExists.withSimpleStrategy(replicationFactor).build

    // Log a warning if the Cassandra keyspace is using SimpleStrategy and the replication factor does not match Quine configuration
    def logWarningOnReplicationFactor(keyspaceMetadata: KeyspaceMetadata): Unit = {
      val keyspaceReplicationConfig = keyspaceMetadata.getReplication.asScala.toMap
      for {
        clazz <- keyspaceReplicationConfig.get("class") if clazz == "org.apache.cassandra.locator.SimpleStrategy"
        factor <- keyspaceReplicationConfig.get("replication_factor") if factor.toInt != replicationFactor
      } logger.info(
        safe"Unexpected replication factor: ${Safe(factor)} (expected: ${Safe(replicationFactor)}) for Cassandra keyspace: ${Safe(keyspace)}"
      )
    }

    val openSession: Future[CqlSession] = createQualifiedSession()
      .map { session =>
        session.getMetadata.getKeyspace(keyspace).ifPresent(logWarningOnReplicationFactor)
        session
      }(materializer.executionContext)
      .recoverWith {
        // Java Futures wrap all the exceptions in CompletionException apparently
        case CompletionException(_: InvalidKeyspaceException) if shouldCreateKeyspace =>
          import materializer.executionContext
          for {
            sess <- sessionBuilder.buildAsync().asScala
            _ <- sess.executeAsync(createKeyspaceStatement).asScala
            _ <- sess.closeAsync().asScala
            qualifiedSess <- createQualifiedSession()
          } yield qualifiedSess
      }(materializer.executionContext)

    openSession.map { session =>
      constructor(
        persistenceConfig,
        bloomFilterSize,
        session,
        readSettings,
        writeSettings,
        shouldCreateTables,
        snapshotPartMaxSizeBytes,
        materializer,
        logConfig
      )
    }(ExecutionContext.parasitic)
  }

}
object PrimeCassandraPersistor
    extends AbstractGlobalCassandraPersistor[PrimeCassandraPersistor](
      new PrimeCassandraPersistor(_, _, _, _, _, _, _, _)(_)
    )

/** A "factory" object to create per-namespace instances of CassandraPersistor.
  * Holds the state that's global to all CassandraPersistor instances
  */
class PrimeCassandraPersistor(
  persistenceConfig: PersistenceConfig,
  bloomFilterSize: Option[Long],
  session: CqlSession,
  readSettings: CassandraStatementSettings,
  writeSettings: CassandraStatementSettings,
  shouldCreateTables: Boolean,
  snapshotPartMaxSizeBytes: Int,
  materializer: Materializer
)(implicit val logConfig: LogConfig)
    extends cassandra.PrimeCassandraPersistor(
      persistenceConfig,
      bloomFilterSize,
      session,
      readSettings,
      writeSettings,
      shouldCreateTables,
      _ => _ => Future.unit
    )(materializer) {

  protected val chunker: Chunker = NoOpChunker

  override def prepareNamespace(namespace: NamespaceId): Future[Unit] =
    if (shouldCreateTables || namespace.nonEmpty) {
      CassandraPersistorDefinition.createTables(namespace, session, _ => _ => Future.unit)(
        materializer.executionContext,
        logConfig
      )
    } else {
      Future.unit
    }

  /** Persistence implementation backed by Cassandra.
    *
    * @param writeTimeout How long to wait for a response when running an INSERT statement.
    * @param readTimeout How long to wait for a response when running a SELECT statement.
    * @param shouldCreateTables Whether or not to create the required tables if they don't already exist.
    */

  protected def agentCreator(persistenceConfig: PersistenceConfig, namespace: NamespaceId): CassandraPersistor =
    new CassandraPersistor(
      persistenceConfig,
      session,
      namespace,
      readSettings,
      writeSettings,
      snapshotPartMaxSizeBytes
    )(materializer, logConfig)

}

// Add the two tables with `SELECT DISTINCT` queries (not supported on Keyspaces)
trait CassandraPersistorDefinition extends cassandra.CassandraPersistorDefinition {
  protected def journalsTableDef(namespace: NamespaceId): JournalsTableDefinition = new JournalsDefinition(namespace)
  protected def snapshotsTableDef(namespace: NamespaceId): SnapshotsTableDefinition = new SnapshotsDefinition(namespace)

}
object CassandraPersistorDefinition extends CassandraPersistorDefinition
class CassandraPersistor(
  persistenceConfig: PersistenceConfig,
  session: CqlSession,
  val namespace: NamespaceId,
  readSettings: CassandraStatementSettings,
  writeSettings: CassandraStatementSettings,
  snapshotPartMaxSizeBytes: Int
)(implicit
  materializer: Materializer,
  val logConfig: LogConfig
) extends cassandra.CassandraPersistor(
      persistenceConfig,
      session,
      namespace,
      snapshotPartMaxSizeBytes
    ) {

  protected val chunker: Chunker = NoOpChunker

  private object prepareStatements extends cassandra.PrepareStatements(session, chunker, readSettings, writeSettings)

  // TODO: Stop blocking on IO actions in this class constructor and run these futures somewhere else, the results
  // of which can be passed in as constructor params to this class.
  protected lazy val (
    journals,
    snapshots,
    standingQueries,
    standingQueryStates,
    domainIndexEvents
  ) = Await.result(
    CassandraPersistorDefinition.tablesForNamespace(namespace).map(prepareStatements).tupled,
    35.seconds
  )

}
