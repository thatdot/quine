package com.thatdot.quine.app.config

import java.io.File
import java.net.InetSocketAddress

import scala.annotation.nowarn
import scala.concurrent.duration.{DurationInt, FiniteDuration}

import org.apache.pekko.actor.ActorSystem

import com.datastax.oss.driver.api.core.{ConsistencyLevel, DefaultConsistencyLevel}
import pureconfig.generic.auto._
import pureconfig.generic.semiauto.deriveConvert
import pureconfig.{ConfigConvert, ConfigReader, ConfigWriter}
import software.amazon.awssdk.regions.Region

import com.thatdot.quine.persistor._

/** Options for persistence */
sealed abstract class PersistenceAgentType(val isLocal: Boolean) {

  /** Size of the bloom filter, if enabled (not all persistors even support this) */
  def bloomFilterSize: Option[Long]

}
object PersistenceAgentType extends PureconfigInstances {

  case object Empty extends PersistenceAgentType(isLocal = false) {

    def bloomFilterSize = None

    def persistor(persistenceConfig: PersistenceConfig)(implicit system: ActorSystem): NamespacedPersistenceAgent =
      new EmptyPersistor(persistenceConfig)
  }

  case object InMemory extends PersistenceAgentType(isLocal = true) {
    def bloomFilterSize = None

  }

  final case class RocksDb(
    filepath: File = new File(sys.env.getOrElse("QUINE_DATA", "quine.db")),
    writeAheadLog: Boolean = true,
    syncAllWrites: Boolean = false,
    createParentDir: Boolean = false,
    bloomFilterSize: Option[Long] = None
  ) extends PersistenceAgentType(isLocal = true) {}

  final case class MapDb(
    filepath: Option[File],
    numberPartitions: Int = 1,
    writeAheadLog: Boolean = false,
    commitInterval: FiniteDuration = 10.seconds,
    createParentDir: Boolean = false,
    bloomFilterSize: Option[Long] = None
  ) extends PersistenceAgentType(isLocal = true) {
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
    keyspace: String = sys.env.getOrElse("CASSANDRA_KEYSPACE", "quine"),
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
    snapshotPartMaxSizeBytes: Int = 1000000
  ) extends PersistenceAgentType(isLocal = false) {
    assert(endpoints.nonEmpty, "Must specify at least one Cassandra endpoint")
  }

  final case class Keyspaces(
    keyspace: String = sys.env.getOrElse("CASSANDRA_KEYSPACE", "quine"),
    awsRegion: Option[Region] = None,
    awsRoleArn: Option[String] = None,
    readConsistency: ConsistencyLevel = ConsistencyLevel.LOCAL_QUORUM,
    writeTimeout: FiniteDuration = 10.seconds,
    readTimeout: FiniteDuration = 10.seconds,
    shouldCreateTables: Boolean = true,
    shouldCreateKeyspace: Boolean = true,
    bloomFilterSize: Option[Long] = None,
    snapshotPartMaxSizeBytes: Int = 1000000
  ) extends PersistenceAgentType(isLocal = false) {
    private val supportedReadConsistencies: Set[ConsistencyLevel] =
      Set(ConsistencyLevel.ONE, ConsistencyLevel.LOCAL_ONE, ConsistencyLevel.LOCAL_QUORUM)
    assert(
      supportedReadConsistencies.contains(readConsistency),
      "AWS Keyspaces only supports read constencies levels: " + supportedReadConsistencies.mkString(", ")
    )
  }

  final case class ClickHouse(
    url: String = sys.env.getOrElse("CLICKHOUSE_URL", "http://localhost:8123"),
    username: String = sys.env.getOrElse("CLICKHOUSE_USER", "quine"),
    password: String = sys.env.getOrElse("CLICKHOUSE_PASSWORD", "quine"),
    bloomFilterSize: Option[Long] = None
  ) extends PersistenceAgentType(isLocal = false)

  implicit val cassandraConfigConvert: ConfigConvert[ConsistencyLevel] = {
    import ConfigReader.javaEnumReader
    import ConfigWriter.javaEnumWriter
    val reader: ConfigReader[ConsistencyLevel] = javaEnumReader[DefaultConsistencyLevel].map(identity)
    val writer: ConfigWriter[ConsistencyLevel] = javaEnumWriter[DefaultConsistencyLevel].contramap {
      case defaultLevel: DefaultConsistencyLevel => defaultLevel
      case other => sys.error("Can't serialize custom consistency level:" + other)
    }
    ConfigConvert(reader, writer)
  }
  implicit lazy val configConvert: ConfigConvert[PersistenceAgentType] = {
    // This assumes the Cassandra port if port is omitted! (so beware about re-using it)
    @nowarn implicit val inetSocketAddressConvert: ConfigConvert[InetSocketAddress] =
      ConfigConvert.viaNonEmptyString[InetSocketAddress](
        s => Right(Address.parseHostAndPort(s, PersistenceAgentType.defaultCassandraPort)),
        addr => addr.getHostString + ':' + addr.getPort
      )

    deriveConvert[PersistenceAgentType]
  }
}
