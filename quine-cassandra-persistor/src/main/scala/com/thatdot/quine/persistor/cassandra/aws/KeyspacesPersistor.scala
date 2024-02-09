package com.thatdot.quine.persistor.cassandra.aws

import java.net.InetSocketAddress
import java.util.Collections.singletonMap
import javax.net.ssl.SSLContext

import scala.collection.immutable
import scala.compat.ExecutionContexts
import scala.compat.java8.FutureConverters._
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.{Sink, Source}

import cats.syntax.all._
import com.codahale.metrics.MetricRegistry
import com.datastax.oss.driver.api.core.cql.SimpleStatement
import com.datastax.oss.driver.api.core.{
  ConsistencyLevel,
  CqlIdentifier,
  CqlSession,
  CqlSessionBuilder,
  InvalidKeyspaceException
}
import com.datastax.oss.driver.api.querybuilder.QueryBuilder.{literal, selectFrom}
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder.createKeyspace
import com.typesafe.scalalogging.LazyLogging
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

import com.thatdot.quine.graph.NamespaceId
import com.thatdot.quine.model.QuineId
import com.thatdot.quine.persistor.cassandra.support.CassandraStatementSettings
import com.thatdot.quine.persistor.cassandra.{
  CassandraPersistor,
  Chunker,
  DomainIndexEvents,
  Journals,
  JournalsTableDefinition,
  Snapshots,
  SnapshotsTableDefinition,
  StandingQueries,
  StandingQueryStates
}
import com.thatdot.quine.persistor.{PersistenceConfig, cassandra}
import com.thatdot.quine.util.PekkoStreams.distinct
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
    Materializer
  ) => C
) extends LazyLogging {

  def writeSettings(writeTimeout: FiniteDuration): CassandraStatementSettings = CassandraStatementSettings(
    ConsistencyLevel.LOCAL_QUORUM, // Write consistency fixed by AWS Keyspaces
    writeTimeout
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
    snapshotPartMaxSizeBytes: Int
  )(implicit materializer: Materializer): Future[PrimeKeyspacesPersistor] = {
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
      CN_NORTHWEST_1 -> "cassandra.cn-northwest-1.amazonaws.com.cn"
    )

    val endpoint = new InetSocketAddress(
      keyspacesEndpoints.getOrElse(
        region,
        sys.error(
          s"AWS Keyspaces is not available in $region. " +
          "See https://docs.aws.amazon.com/keyspaces/latest/devguide/programmatic.endpoints.html"
        )
      ),
      9142
    )

    val credsProvider: AwsCredentialsProvider with SdkAutoCloseable = awsRoleArn match {
      case None =>
        // TODO: support passing in key and secret explicitly, instead of getting from environment?
        DefaultCredentialsProvider.create
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

    def createQualifiedSession: CqlSession = sessionBuilder
      .withKeyspace(keyspace)
      .build

    // CREATE KEYSPACE IF NOT EXISTS `keyspace` WITH replication={'class':'SingleRegionStrategy'}
    val createKeyspaceStatement: SimpleStatement =
      createKeyspace(keyspace).ifNotExists.withReplicationOptions(singletonMap("class", "SingleRegionStrategy")).build

    val session: CqlSession =
      try createQualifiedSession
      catch {
        case _: InvalidKeyspaceException if shouldCreateKeyspace =>
          val sess = sessionBuilder.build
          sess.execute(createKeyspaceStatement)
          val keyspaceExistsQuery = selectFrom("system_schema_mcs", "keyspaces")
            .column("replication")
            .whereColumn("keyspace_name")
            .isEqualTo(literal(keyspace))
            .build
          while (!sess.execute(keyspaceExistsQuery).iterator.hasNext) {
            logger.info(s"Keyspace $keyspace does not yet exist, re-checking in 4s")
            Thread.sleep(4000)
          }
          sess.close()
          createQualifiedSession
      }

    // Query "system_schema_mcs.tables" for the table creation status
    def tableStatusQuery(tableName: CqlIdentifier): SimpleStatement = selectFrom("system_schema_mcs", "tables")
      .column("status")
      .whereColumn("keyspace_name")
      .isEqualTo(literal(keyspace))
      .whereColumn("table_name")
      .isEqualTo(literal(tableName.asInternal))
      .build

    // Delay by polling until  Keyspaces lists the table as ACTIVE, as per
    // https://docs.aws.amazon.com/keyspaces/latest/devguide/working-with-tables.html#tables-create
    def verifyTable(session: CqlSession)(tableName: CqlIdentifier): Future[Unit] = Retry
      .until(
        session
          .executeAsync(tableStatusQuery(tableName))
          .toScala
          .map(rs => Option(rs.one()).map(_.getString("status")))(ExecutionContexts.parasitic),
        (status: Option[String]) =>
          (status contains "ACTIVE") || {
            logger.info(s"$tableName status is $status; polling status again")
            false
          },
        15,
        4.seconds,
        materializer.system.scheduler
      )(materializer.executionContext)
      .map(_ => ())(ExecutionContexts.parasitic)

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
        materializer
      )
    )
  }

}

object PrimeKeyspacesPersistor
    extends AbstractGlobalKeyspacesPersistor[PrimeKeyspacesPersistor](
      new PrimeKeyspacesPersistor(_, _, _, _, _, _, _, _, _, _)
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
  materializer: Materializer
) extends cassandra.PrimeCassandraPersistor(
      persistenceConfig,
      bloomFilterSize,
      session,
      readSettings,
      PrimeKeyspacesPersistor.writeSettings(writeTimeout),
      shouldCreateTables,
      verifyTable
    )(materializer) {

  override def shutdown(): Future[Unit] = super.shutdown().map(_ => credsProvider.close())(ExecutionContexts.parasitic)

  protected val chunker: Chunker = new Chunker {

    import scala.collection.compat.{immutable => _, _} // for the .lengthIs

    def apply[A](things: immutable.Seq[A])(f: immutable.Seq[A] => Future[Unit]): Future[Unit] =
      if (things.lengthIs <= 30) // If it can be done as a single batch, just run it w/out Pekko Streams
        f(things)
      else
        Source(things)
          .grouped(30)
          .runWith(Sink.foreachAsync(6)(f))(materializer)
          .map(_ => ())(ExecutionContexts.parasitic)
  }

  override def prepareNamespace(namespace: NamespaceId): Future[Unit] =
    KeyspacesPersistorDefinition.createTables(namespace, session, verifyTable)(materializer.executionContext)
  override def agentCreator(
    persistenceConfig: PersistenceConfig,
    namespace: NamespaceId
  ): cassandra.CassandraPersistor = new KeyspacesPersistor(
    persistenceConfig,
    session,
    namespace,
    readSettings,
    writeTimeout,
    chunker,
    snapshotPartMaxSizeBytes
  )(materializer)
}

// Keyspaces doesn't differ from Cassandra in the schema, just in he lack of `DISTINCT` on the prepared
// statements for the two tables below. And the schema is kept next to the prepared statements right now.
// I.e. the schema part of this could be extracted and shared between Keyspaces and Cassandra
trait KeyspacesPersistorDefinition extends cassandra.CassandraPersistorDefinition {
  protected def journalsTableDef(namespace: NamespaceId): JournalsTableDefinition = new KeyspacesJournalsDefinition(
    namespace
  )
  protected def snapshotsTableDef(namespace: NamespaceId): SnapshotsTableDefinition = new KeyspacesSnapshotsDefinition(
    namespace
  )

}
object KeyspacesPersistorDefinition extends KeyspacesPersistorDefinition

/** Persistence implementation backed by AWS Keypaces.
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
  snapshotPartMaxSizeBytes: Int
)(implicit
  materializer: Materializer
) extends cassandra.CassandraPersistor(
      persistenceConfig,
      session,
      namespace,
      snapshotPartMaxSizeBytes
    ) {

  private object prepareStatements
      extends cassandra.PrepareStatements(
        session,
        chunker,
        readSettings,
        PrimeKeyspacesPersistor.writeSettings(writeTimeout)
      )

  protected lazy val (
    journals,
    snapshots,
    standingQueries,
    standingQueryStates,
    domainIndexEvents
  ) = Await.result(
    KeyspacesPersistorDefinition.tablesForNamespace(namespace).map(prepareStatements).tupled,
    35.seconds
  )

  override def enumerateJournalNodeIds(): Source[QuineId, NotUsed] = super.enumerateJournalNodeIds().via(distinct)

  override def enumerateSnapshotNodeIds(): Source[QuineId, NotUsed] = super.enumerateSnapshotNodeIds().via(distinct)

}
