package com.thatdot.quine.app.config

import java.io.File
import java.net.InetSocketAddress

import scala.annotation.nowarn
import scala.concurrent.duration.{DurationInt, FiniteDuration}

import akka.actor.ActorSystem

import com.datastax.oss.driver.api.core.DefaultConsistencyLevel
import pureconfig.ConfigConvert
import pureconfig.generic.auto._
import pureconfig.generic.semiauto.deriveConvert

import com.thatdot.quine.persistor._

/** Options for persistence */
sealed abstract class PersistenceAgentType(val isLocal: Boolean) {

  /** Size of the bloom filter, if enabled (not all persistors even support this) */
  def bloomFilterSize: Option[Long]

}
object PersistenceAgentType extends PureconfigInstances {

  case object Empty extends PersistenceAgentType(false) {

    def bloomFilterSize = None

    def persistor(persistenceConfig: PersistenceConfig)(implicit system: ActorSystem): PersistenceAgent =
      new EmptyPersistor(persistenceConfig)
  }

  case object InMemory extends PersistenceAgentType(true) {
    def bloomFilterSize = None

  }

  final case class RocksDb(
    filepath: File = new File(sys.env.getOrElse("QUINE_DATA", "quine.db")),
    writeAheadLog: Boolean = true,
    syncAllWrites: Boolean = false,
    createParentDir: Boolean = false,
    bloomFilterSize: Option[Long] = None
  ) extends PersistenceAgentType(true) {}

  final case class MapDb(
    filepath: Option[File],
    numberPartitions: Int = 1,
    writeAheadLog: Boolean = false,
    commitInterval: FiniteDuration = 10.seconds,
    createParentDir: Boolean = false,
    bloomFilterSize: Option[Long] = None
  ) extends PersistenceAgentType(true)

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
    writeTimeout: FiniteDuration = 10.seconds,
    readTimeout: FiniteDuration = 10.seconds,
    shouldCreateTables: Boolean = true,
    shouldCreateKeyspace: Boolean = true,
    bloomFilterSize: Option[Long] = None,
    snapshotPartMaxSizeBytes: Int = 1000000
  ) extends PersistenceAgentType(false)

  implicit lazy val configConvert: ConfigConvert[PersistenceAgentType] = {
    // TODO: this assumes the Cassandra port if port is omitted! (so beware about re-using it)
    @nowarn implicit val inetSocketAddressConvert: ConfigConvert[InetSocketAddress] =
      ConfigConvert.viaNonEmptyString[InetSocketAddress](
        s => Right(Address.parseHostAndPort(s, PersistenceAgentType.defaultCassandraPort)),
        addr => addr.getHostString + ':' + addr.getPort
      )

    deriveConvert[PersistenceAgentType]
  }
}
