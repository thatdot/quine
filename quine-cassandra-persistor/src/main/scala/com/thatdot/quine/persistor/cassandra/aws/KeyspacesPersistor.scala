package com.thatdot.quine.persistor.cassandra.aws

import java.net.InetSocketAddress
import java.util.Collections.singletonMap
import javax.net.ssl.SSLContext

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.jdk.FutureConverters._

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.Source

import cats.syntax.all._
import com.codahale.metrics.MetricRegistry
import com.datastax.oss.driver.api.core.cql.SimpleStatement
import com.datastax.oss.driver.api.core.{
  ConsistencyLevel,
  CqlIdentifier,
  CqlSession,
  CqlSessionBuilder,
  InvalidKeyspaceException,
}
import com.datastax.oss.driver.api.querybuilder.QueryBuilder.{literal, selectFrom}
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder.createKeyspace
import shapeless.syntax.std.tuple._
import software.amazon.awssdk.auth.credentials.{AwsCredentialsProvider, DefaultCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.regions.Region._
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain
import software.amazon.awssdk.services.sts.StsClient
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest
import software.amazon.awssdk.utils.SdkAutoCloseable
import software.aws.mcs.auth.SigV4AuthProvider

import com.thatdot.common.logging.Log.{LazySafeLogging, LogConfig, Safe, SafeLoggableInterpolator}
import com.thatdot.common.quineid.QuineId
import com.thatdot.quine.graph.NamespaceId
import com.thatdot.quine.persistor.cassandra.support.CassandraStatementSettings
import com.thatdot.quine.persistor.cassandra.{
  Chunker,
  JournalsTableDefinition,
  SizeBoundedChunker,
  SnapshotsTableDefinition,
}
import com.thatdot.quine.persistor.{PersistenceConfig, cassandra}
import com.thatdot.quine.util.Log.implicits._
import com.thatdot.quine.util.PekkoStreams.distinctConsecutive
import com.thatdot.quine.util.Retry

abstract class AbstractGlobalKeyspacesPersistor[C <: PrimeKeyspacesPersistor](
  constructor: (
    PersistenceConfig,
    Option[Long],
    CqlSession,
    SdkAutoCloseable,
    CassandraStatementSettings,
    FiniteDuration,
    Boolean,
    (CqlSession => CqlIdentifier => Future[Unit]),
    Int,
    Materializer,
    LogConfig,
  ) => C,
) extends LazySafeLogging {

  def writeSettings(writeTimeout: FiniteDuration): CassandraStatementSettings = CassandraStatementSettings(
    ConsistencyLevel.LOCAL_QUORUM, // Write consistency fixed by AWS Keyspaces
    writeTimeout,
  )

  def create(
    persistenceConfig: PersistenceConfig,
    bloomFilterSize: Option[Long],
    keyspace: String,
    awsRegion: Option[Region],
    awsRoleArn: Option[String],
    readSettings: CassandraStatementSettings,
    writeTimeout: FiniteDuration,
    shouldCreateKeyspace: Boolean,
    shouldCreateTables: Boolean,
    metricRegistry: Option[MetricRegistry],
    snapshotPartMaxSizeBytes: Int,
  )(implicit materializer: Materializer, logConfig: LogConfig): Future[PrimeKeyspacesPersistor] = {
    val region: Region = awsRegion getOrElse new DefaultAwsRegionProviderChain().getRegion

    // From https://docs.aws.amazon.com/keyspaces/latest/devguide/programmatic.endpoints.html
    val keyspacesEndpoints: Map[Region, String] = Map(
      US_EAST_2 -> "cassandra.us-east-2.amazonaws.com",
      US_EAST_1 -> "cassandra.us-east-1.amazonaws.com",
      US_WEST_1 -> "cassandra.us-west-1.amazonaws.com",
      US_WEST_2 -> "cassandra.us-west-2.amazonaws.com",
      AP_EAST_1 -> "cassandra.ap-east-1.amazonaws.com",
      AP_SOUTH_1 -> "cassandra.ap-south-1.amazonaws.com",
      AP_NORTHEAST_2 -> "cassandra.ap-northeast-2.amazonaws.com",
      AP_SOUTHEAST_1 -> "cassandra.ap-southeast-1.amazonaws.com",
      AP_SOUTHEAST_2 -> "cassandra.ap-southeast-2.amazonaws.com",
      AP_NORTHEAST_1 -> "cassandra.ap-northeast-1.amazonaws.com",
      CA_CENTRAL_1 -> "cassandra.ca-central-1.amazonaws.com",
      EU_CENTRAL_1 -> "cassandra.eu-central-1.amazonaws.com",
      EU_WEST_1 -> "cassandra.eu-west-1.amazonaws.com",
      EU_WEST_2 -> "cassandra.eu-west-2.amazonaws.com",
      EU_WEST_3 -> "cassandra.eu-west-3.amazonaws.com",
      EU_NORTH_1 -> "cassandra.eu-north-1.amazonaws.com",
      ME_SOUTH_1 -> "cassandra.me-south-1.amazonaws.com",
      SA_EAST_1 -> "cassandra.sa-east-1.amazonaws.com",
      US_GOV_EAST_1 -> "cassandra.us-gov-east-1.amazonaws.com",
      US_GOV_WEST_1 -> "cassandra.us-gov-west-1.amazonaws.com",
      CN_NORTH_1 -> "cassandra.cn-north-1.amazonaws.com.cn",
      CN_NORTHWEST_1 -> "cassandra.cn-northwest-1.amazonaws.com.cn",
    )

    val endpoint = new InetSocketAddress(
      keyspacesEndpoints.getOrElse(
        region,
        sys.error(
          s"AWS Keyspaces is not available in $region. " +
          "See https://docs.aws.amazon.com/keyspaces/latest/devguide/programmatic.endpoints.html",
        ),
      ),
      9142,
    )

    val credsProvider: AwsCredentialsProvider with SdkAutoCloseable = awsRoleArn match {
      case None =>
        // TODO: support passing in key and secret explicitly, instead of getting from environment?
        DefaultCredentialsProvider.builder().build()
      case Some(roleArn) =>
        val sessionName = "quine-keyspaces"
        val stsClient = StsClient.builder.region(region).build
        val assumeRoleRequest = AssumeRoleRequest.builder.roleArn(roleArn).roleSessionName(sessionName).build
        StsAssumeRoleCredentialsProvider.builder
          .stsClient(stsClient)
          .refreshRequest(assumeRoleRequest)
          .asyncCredentialUpdateEnabled(true)
          .build
    }

    // This is mutable, so needs to be a def to get a new one w/out prior settings.
    def sessionBuilder: CqlSessionBuilder = CqlSession.builder
      .addContactPoint(endpoint)
      .withLocalDatacenter(region.id)
      .withMetricRegistry(metricRegistry.orNull)
      .withSslContext(SSLContext.getDefault)
      .withAuthProvider(new SigV4AuthProvider(credsProvider, region.id))

    // Keyspace names in AWS Keyspaces is case-sensitive
    val keyspaceInternalId = CqlIdentifier.fromInternal(keyspace)

    def createQualifiedSession: CqlSession = sessionBuilder
      .withKeyspace(keyspaceInternalId)
      .build

    // CREATE KEYSPACE IF NOT EXISTS `keyspace` WITH replication={'class':'SingleRegionStrategy'}
    val createKeyspaceStatement: SimpleStatement =
      createKeyspace(keyspaceInternalId).ifNotExists
        .withReplicationOptions(singletonMap("class", "SingleRegionStrategy"))
        .build

    val session: CqlSession =
      try createQualifiedSession
      catch {
        case _: InvalidKeyspaceException if shouldCreateKeyspace =>
          val sess = sessionBuilder.build
          sess.execute(createKeyspaceStatement)
          val keyspaceExistsQuery = selectFrom("system_schema_mcs", "keyspaces")
            .column("replication")
            .whereColumn("keyspace_name")
            .isEqualTo(literal(keyspaceInternalId.asInternal))
            .build
          while (!sess.execute(keyspaceExistsQuery).iterator.hasNext) {
            logger.info(safe"Keyspace ${Safe(keyspaceInternalId.asInternal)} does not yet exist, re-checking in 4s")
            Thread.sleep(4000)
          }
          sess.close()
          createQualifiedSession
      }

    // Query "system_schema_mcs.tables" for the table creation status
    def tableStatusQuery(tableName: CqlIdentifier): SimpleStatement = selectFrom("system_schema_mcs", "tables")
      .column("status")
      .whereColumn("keyspace_name")
      .isEqualTo(literal(keyspaceInternalId.asInternal))
      .whereColumn("table_name")
      .isEqualTo(literal(tableName.asInternal))
      .build

    // Delay by polling until  Keyspaces lists the table as ACTIVE, as per
    // https://docs.aws.amazon.com/keyspaces/latest/devguide/working-with-tables.html#tables-create
    def verifyTable(session: CqlSession)(tableName: CqlIdentifier): Future[Unit] = Retry
      .until(
        session
          .executeAsync(tableStatusQuery(tableName))
          .asScala
          .map(rs => Option(rs.one()).map(_.getString("status")))(ExecutionContext.parasitic),
        (status: Option[String]) =>
          (status contains "ACTIVE") || {
            logger.info(safe"${Safe(tableName.toString)} status is ${Safe(status)}; polling status again")
            false
          },
        15,
        4.seconds,
        materializer.system.scheduler,
      )(materializer.executionContext)
      .map(_ => ())(ExecutionContext.parasitic)

    Future.successful(
      constructor(
        persistenceConfig,
        bloomFilterSize,
        session,
        credsProvider,
        readSettings,
        writeTimeout,
        shouldCreateTables,
        verifyTable,
        snapshotPartMaxSizeBytes,
        materializer,
        logConfig,
      ),
    )
  }

}

object PrimeKeyspacesPersistor
    extends AbstractGlobalKeyspacesPersistor[PrimeKeyspacesPersistor](
      new PrimeKeyspacesPersistor(_, _, _, _, _, _, _, _, _, _)(_),
    )

class PrimeKeyspacesPersistor(
  persistenceConfig: PersistenceConfig,
  bloomFilterSize: Option[Long],
  session: CqlSession,
  credsProvider: SdkAutoCloseable,
  readSettings: CassandraStatementSettings,
  writeTimeout: FiniteDuration,
  shouldCreateTables: Boolean,
  verifyTable: CqlSession => CqlIdentifier => Future[Unit],
  snapshotPartMaxSizeBytes: Int,
  materializer: Materializer,
)(implicit val logConfig: LogConfig)
    extends cassandra.PrimeCassandraPersistor(
      persistenceConfig,
      bloomFilterSize,
      session,
      readSettings,
      PrimeKeyspacesPersistor.writeSettings(writeTimeout),
      shouldCreateTables,
      verifyTable,
    )(materializer) {

  override def shutdown(): Future[Unit] = super.shutdown() as credsProvider.close()

  protected val chunker: Chunker = new SizeBoundedChunker(maxBatchSize = 30, parallelism = 6, materializer)

  override def prepareNamespace(namespace: NamespaceId): Future[Unit] =
    if (shouldCreateTables || namespace.nonEmpty) {
      KeyspacesPersistorDefinition.createTables(namespace, session, verifyTable)(
        materializer.executionContext,
        logConfig,
      )
    } else {
      Future.unit
    }

  override def agentCreator(
    persistenceConfig: PersistenceConfig,
    namespace: NamespaceId,
  ): cassandra.CassandraPersistor = new KeyspacesPersistor(
    persistenceConfig,
    session,
    namespace,
    readSettings,
    writeTimeout,
    chunker,
    snapshotPartMaxSizeBytes,
  )(materializer, logConfig)
}

// Keyspaces doesn't differ from Cassandra in the schema, just in he lack of `DISTINCT` on the prepared
// statements for the two tables below. And the schema is kept next to the prepared statements right now.
// I.e. the schema part of this could be extracted and shared between Keyspaces and Cassandra
trait KeyspacesPersistorDefinition extends cassandra.CassandraPersistorDefinition {
  protected def journalsTableDef(namespace: NamespaceId): JournalsTableDefinition = new KeyspacesJournalsDefinition(
    namespace,
  )
  protected def snapshotsTableDef(namespace: NamespaceId): SnapshotsTableDefinition = new KeyspacesSnapshotsDefinition(
    namespace,
  )

}
object KeyspacesPersistorDefinition extends KeyspacesPersistorDefinition

/** Persistence implementation backed by AWS Keyspaces.
  *
  * @param keyspace The keyspace the quine tables should live in.
  * @param readConsistency
  * @param writeTimeout How long to wait for a response when running an INSERT statement.
  * @param readTimeout How long to wait for a response when running a SELECT statement.
  * @param shouldCreateTables Whether or not to create the required tables if they don't already exist.
  * @param shouldCreateKeyspace Whether or not to create the specified keyspace if it doesn't already exist. If it doesn't exist, it'll run {{{CREATE KEYSPACE IF NOT EXISTS `keyspace` WITH replication={'class':'SimpleStrategy'}}}}
  */
class KeyspacesPersistor(
  persistenceConfig: PersistenceConfig,
  session: CqlSession,
  val namespace: NamespaceId,
  readSettings: CassandraStatementSettings,
  writeTimeout: FiniteDuration,
  protected val chunker: Chunker,
  snapshotPartMaxSizeBytes: Int,
)(implicit
  materializer: Materializer,
  val logConfig: LogConfig,
) extends cassandra.CassandraPersistor(
      persistenceConfig,
      session,
      namespace,
      snapshotPartMaxSizeBytes,
    ) {

  private object prepareStatements
      extends cassandra.PrepareStatements(
        session,
        chunker,
        readSettings,
        PrimeKeyspacesPersistor.writeSettings(writeTimeout),
      )

  protected lazy val (
    journals,
    snapshots,
    standingQueries,
    standingQueryStates,
//    quinePatterns,
    domainIndexEvents,
  ) = Await.result(
    KeyspacesPersistorDefinition.tablesForNamespace(namespace).map(prepareStatements).tupled,
    35.seconds,
  )

  override def enumerateJournalNodeIds(): Source[QuineId, NotUsed] =
    super.enumerateJournalNodeIds().via(distinctConsecutive)

  override def enumerateSnapshotNodeIds(): Source[QuineId, NotUsed] =
    super.enumerateSnapshotNodeIds().via(distinctConsecutive)

}
