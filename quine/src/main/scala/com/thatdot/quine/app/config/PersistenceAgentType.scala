package com.thatdot.quine.app.config

import java.io.File
import java.net.InetSocketAddress
import java.nio.file.Paths

import scala.concurrent.duration.{DurationInt, FiniteDuration}

import com.datastax.oss.driver.api.core.{ConsistencyLevel, DefaultConsistencyLevel}
import pureconfig.generic.semiauto.deriveConvert
import pureconfig.{ConfigConvert, ConfigReader, ConfigWriter}
import software.amazon.awssdk.regions.Region

import com.thatdot.common.logging.Log._
import com.thatdot.quine.persistor._

/** Options for persistence */
sealed abstract class PersistenceAgentType(val isLocal: Boolean, val label: String) {

  /** Size of the bloom filter, if enabled (not all persistors even support this) */
  def bloomFilterSize: Option[Long]

}
object PersistenceAgentType extends PureconfigInstances {

  case object Empty extends PersistenceAgentType(isLocal = false, "empty") {

    def bloomFilterSize = None

    def persistor(persistenceConfig: PersistenceConfig): NamespacedPersistenceAgent =
      new EmptyPersistor(persistenceConfig)
  }

  case object InMemory extends PersistenceAgentType(isLocal = true, "inmemory") {
    def bloomFilterSize = None

  }

  final case class RocksDb(
    filepath: Option[File] = sys.env.get("QUINE_DATA").map(new File(_)),
    writeAheadLog: Boolean = true,
    syncAllWrites: Boolean = false,
    createParentDir: Boolean = false,
    bloomFilterSize: Option[Long] = None,
  ) extends PersistenceAgentType(isLocal = true, "rocksdb") {}

  final case class MapDb(
    filepath: Option[File],
    numberPartitions: Int = 1,
    writeAheadLog: Boolean = false,
    commitInterval: FiniteDuration = 10.seconds,
    createParentDir: Boolean = false,
    bloomFilterSize: Option[Long] = None,
  ) extends PersistenceAgentType(isLocal = true, "mapdb") {
    assert(numberPartitions > 0, "Must have a positive number of partitions")
  }

  val defaultCassandraPort = 9042
  def defaultCassandraAddress: List[InetSocketAddress] =
    sys.env
      .getOrElse("CASSANDRA_ENDPOINTS", "localhost:9042")
      .split(',')
      .map(Address.parseHostAndPort(_, defaultCassandraPort))
      .toList

  final case class Cassandra(
    keyspace: Option[String] = sys.env.get("CASSANDRA_KEYSPACE"),
    replicationFactor: Int = Integer.parseUnsignedInt(sys.env.getOrElse("CASSANDRA_REPLICATION_FACTOR", "1")),
    readConsistency: ConsistencyLevel = ConsistencyLevel.LOCAL_QUORUM,
    writeConsistency: ConsistencyLevel = ConsistencyLevel.LOCAL_QUORUM,
    endpoints: List[InetSocketAddress] = defaultCassandraAddress,
    localDatacenter: String = "datacenter1",
    writeTimeout: FiniteDuration = 10.seconds,
    readTimeout: FiniteDuration = 10.seconds,
    shouldCreateTables: Boolean = true,
    shouldCreateKeyspace: Boolean = true,
    bloomFilterSize: Option[Long] = None,
    snapshotPartMaxSizeBytes: Int = 1000000,
    oauth: Option[OAuth2Config] = None,
  ) extends PersistenceAgentType(isLocal = false, "cassandra") {
    assert(endpoints.nonEmpty, "Must specify at least one Cassandra endpoint")
  }

  final case class OAuth2Config(
    clientId: String,
    certFile: String,
    certAlias: Option[String],
    certFilePassword: Array[Char],
    keyAlias: Option[String],
    adfsEnv: Option[String],
    resourceURI: Option[String],
    discoveryURL: Option[String],
  )

  final case class Keyspaces(
    keyspace: Option[String] = sys.env.get("CASSANDRA_KEYSPACE"),
    awsRegion: Option[Region] = None,
    awsRoleArn: Option[String] = None,
    readConsistency: ConsistencyLevel = ConsistencyLevel.LOCAL_QUORUM,
    writeTimeout: FiniteDuration = 10.seconds,
    readTimeout: FiniteDuration = 10.seconds,
    shouldCreateTables: Boolean = true,
    shouldCreateKeyspace: Boolean = true,
    bloomFilterSize: Option[Long] = None,
    snapshotPartMaxSizeBytes: Int = 1000000,
  ) extends PersistenceAgentType(isLocal = false, "keyspaces") {
    private val supportedReadConsistencies: Set[ConsistencyLevel] =
      Set(ConsistencyLevel.ONE, ConsistencyLevel.LOCAL_ONE, ConsistencyLevel.LOCAL_QUORUM)
    assert(
      supportedReadConsistencies.contains(readConsistency),
      "AWS Keyspaces only supports read constencies levels: " + supportedReadConsistencies.mkString(", "),
    )
  }

  final case class ClickHouse(
    url: String = sys.env.getOrElse("CLICKHOUSE_URL", "http://localhost:8123"),
    database: String = sys.env.getOrElse("CLICKHOUSE_DATABASE", "quine"),
    username: Option[String] = sys.env.get("CLICKHOUSE_USER"),
    password: Option[String] = sys.env.get("CLICKHOUSE_PASSWORD"),
    bloomFilterSize: Option[Long] = None,
  ) extends PersistenceAgentType(isLocal = false, "clickhouse")
      with LazySafeLogging {

    /** By default, the ClickHouse client uses the default SSLContext (configured by standard java truststore and
      * keystore properties). If the CLICKHOUSE_CERTIFICATE_PEM environment variable is set and points to a file,
      * we will instead construct an SSLContext that uses that file as the only trusted certificate.
      * Not recommended (see log line below).
      */
    val pemCertOverride: Option[String] = sys.env
      .get("CLICKHOUSE_CERTIFICATE_PEM")
      .filter(Paths.get(_).toFile.exists())
      .map { x =>
        logger.warn(
          safe"""Using certificate at: ${Safe(x)} to authenticate ClickHouse server. For better security, we
                |recommend using a password-protected Java truststore instead (this can be configured with the
                |`javax.net.ssl.trustStore` and `javax.net.ssl.trustStorePassword` properties)""".cleanLines,
        )
        x
      }
  }

  implicit val consistencyLevelConvert: ConfigConvert[ConsistencyLevel] = {
    import ConfigReader.javaEnumReader
    import ConfigWriter.javaEnumWriter
    val reader: ConfigReader[ConsistencyLevel] = javaEnumReader[DefaultConsistencyLevel].map(identity)
    val writer: ConfigWriter[ConsistencyLevel] = javaEnumWriter[DefaultConsistencyLevel].contramap {
      case defaultLevel: DefaultConsistencyLevel => defaultLevel
      case other => sys.error("Can't serialize custom consistency level:" + other)
    }
    ConfigConvert(reader, writer)
  }
  implicit val charArrayReader: ConfigReader[Array[Char]] = QuineConfig.charArrayReader
  implicit val charArrayWriter: ConfigWriter[Array[Char]] = QuineConfig.charArrayWriter

  // InetSocketAddress converter (assumes Cassandra port if port is omitted)
  implicit val inetSocketAddressConvert: ConfigConvert[InetSocketAddress] =
    ConfigConvert.viaNonEmptyString[InetSocketAddress](
      s => Right(Address.parseHostAndPort(s, PersistenceAgentType.defaultCassandraPort)),
      addr => addr.getHostString + ':' + addr.getPort,
    )

  implicit val emptyConfigConvert: ConfigConvert[Empty.type] = deriveConvert[Empty.type]
  implicit val inMemoryConfigConvert: ConfigConvert[InMemory.type] = deriveConvert[InMemory.type]
  implicit val rocksDbConfigConvert: ConfigConvert[RocksDb] = deriveConvert[RocksDb]
  implicit val mapDbConfigConvert: ConfigConvert[MapDb] = deriveConvert[MapDb]
  implicit val oauth2ConfigConvert: ConfigConvert[OAuth2Config] = deriveConvert[OAuth2Config]
  implicit val cassandraConfigConvert: ConfigConvert[Cassandra] = deriveConvert[Cassandra]
  implicit val keyspacesConfigConvert: ConfigConvert[Keyspaces] = deriveConvert[Keyspaces]
  implicit val clickHouseConfigConvert: ConfigConvert[ClickHouse] = deriveConvert[ClickHouse]

  implicit lazy val configConvert: ConfigConvert[PersistenceAgentType] =
    deriveConvert[PersistenceAgentType]
}
