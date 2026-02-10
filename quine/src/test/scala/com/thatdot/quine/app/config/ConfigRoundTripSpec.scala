package com.thatdot.quine.app.config

import java.net.InetSocketAddress

import scala.concurrent.duration.FiniteDuration
import scala.reflect.ClassTag

import com.datastax.oss.driver.api.core.ConsistencyLevel
import com.typesafe.config.ConfigFactory
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import pureconfig.{ConfigConvert, ConfigSource, ConfigWriter}

class ConfigRoundTripSpec extends AnyFunSpec with Matchers with ScalaCheckDrivenPropertyChecks {

  import ConfigGenerators.Arbs._

  /** Compare InetSocketAddress by hostString and port, not object equality.
    * InetSocketAddress equality considers resolved state which changes after round-trip.
    */
  def sameAddress(a: InetSocketAddress, b: InetSocketAddress): Boolean =
    a.getHostString == b.getHostString && a.getPort == b.getPort

  /** Compare OAuth2Config handling Array[Char] field */
  def sameOAuth2Config(a: PersistenceAgentType.OAuth2Config, b: PersistenceAgentType.OAuth2Config): Boolean =
    a.clientId == b.clientId &&
    a.certFile == b.certFile &&
    a.certAlias == b.certAlias &&
    (a.certFilePassword sameElements b.certFilePassword) &&
    a.keyAlias == b.keyAlias &&
    a.adfsEnv == b.adfsEnv &&
    a.resourceURI == b.resourceURI &&
    a.discoveryURL == b.discoveryURL

  /** Compare Cassandra configs handling nested types with problematic equality */
  def sameCassandra(a: PersistenceAgentType.Cassandra, b: PersistenceAgentType.Cassandra): Boolean =
    a.keyspace == b.keyspace &&
    a.replicationFactor == b.replicationFactor &&
    a.readConsistency == b.readConsistency &&
    a.writeConsistency == b.writeConsistency &&
    a.endpoints.length == b.endpoints.length &&
    a.endpoints.zip(b.endpoints).forall { case (ea, eb) => sameAddress(ea, eb) } &&
    a.localDatacenter == b.localDatacenter &&
    a.writeTimeout == b.writeTimeout &&
    a.readTimeout == b.readTimeout &&
    a.shouldCreateTables == b.shouldCreateTables &&
    a.shouldCreateKeyspace == b.shouldCreateKeyspace &&
    a.bloomFilterSize == b.bloomFilterSize &&
    a.snapshotPartMaxSizeBytes == b.snapshotPartMaxSizeBytes &&
    ((a.oauth, b.oauth) match {
      case (Some(oa), Some(ob)) => sameOAuth2Config(oa, ob)
      case (None, None) => true
      case _ => false
    })

  /** Helper to test round-trip: write to config, read back, compare.
    * Wraps the value in a key since ConfigSource.string expects valid HOCON.
    */
  def roundTrip[A: ConfigConvert: ClassTag](value: A): A = {
    val configValue = ConfigWriter[A].to(value)
    val wrapped = ConfigFactory.parseString(s"value = ${configValue.render()}")
    ConfigSource.fromConfig(wrapped).at("value").loadOrThrow[A]
  }

  /** Helper for types that need wrapping at a specific key (e.g., sealed traits) */
  def roundTripWrapped[A: ConfigConvert: ClassTag](value: A, key: String): A = {
    val configValue = ConfigWriter[A].to(value)
    val wrapped = ConfigFactory.parseString(s"$key = ${configValue.render()}")
    ConfigSource.fromConfig(wrapped).at(key).loadOrThrow[A]
  }

  describe("Custom converter round-trips") {

    it("Array[Char] should round-trip") {
      forAll { (chars: Array[Char]) =>
        val result = roundTrip(chars)
        result shouldEqual chars
      }
    }

    it("InetSocketAddress should round-trip") {
      forAll { (addr: InetSocketAddress) =>
        implicit val convert: ConfigConvert[InetSocketAddress] = PersistenceAgentType.inetSocketAddressConvert
        val result = roundTrip(addr)
        result.getHostString shouldEqual addr.getHostString
        result.getPort shouldEqual addr.getPort
      }
    }

    it("ConsistencyLevel should round-trip") {
      forAll { (level: ConsistencyLevel) =>
        implicit val convert: ConfigConvert[ConsistencyLevel] = PersistenceAgentType.consistencyLevelConvert
        val result = roundTrip(level)
        result shouldEqual level
      }
    }

    it("FiniteDuration should round-trip") {
      forAll { (duration: FiniteDuration) =>
        val result = roundTrip(duration)
        result shouldEqual duration
      }
    }
  }

  describe("IdProviderType round-trips") {

    it("IdProviderType.Long should round-trip") {
      forAll(ConfigGenerators.Gens.idProviderLong) { (idp: IdProviderType.Long) =>
        val result = roundTripWrapped(idp: IdProviderType, "id-provider")
        result shouldEqual idp
      }
    }

    it("IdProviderType.UUID should round-trip") {
      forAll(ConfigGenerators.Gens.idProviderUUID) { (idp: IdProviderType.UUID) =>
        val result = roundTripWrapped(idp: IdProviderType, "id-provider")
        result shouldEqual idp
      }
    }

    it("IdProviderType.Uuid4 should round-trip") {
      forAll(ConfigGenerators.Gens.idProviderUuid4) { (idp: IdProviderType.Uuid4) =>
        val result = roundTripWrapped(idp: IdProviderType, "id-provider")
        result shouldEqual idp
      }
    }

    it("IdProviderType.ByteArray should round-trip") {
      forAll(ConfigGenerators.Gens.idProviderByteArray) { (idp: IdProviderType.ByteArray) =>
        val result = roundTripWrapped(idp: IdProviderType, "id-provider")
        result shouldEqual idp
      }
    }

    it("IdProviderType (any subtype) should round-trip") {
      forAll { (idp: IdProviderType) =>
        val result = roundTripWrapped(idp, "id-provider")
        result shouldEqual idp
      }
    }
  }

  describe("MetricsReporter round-trips") {

    it("MetricsReporter.Jmx should round-trip") {
      val result = roundTripWrapped(MetricsReporter.Jmx: MetricsReporter, "reporter")
      result shouldEqual MetricsReporter.Jmx
    }

    it("MetricsReporter.Slf4j should round-trip") {
      forAll(ConfigGenerators.Gens.metricsReporterSlf4j) { (reporter: MetricsReporter.Slf4j) =>
        val result = roundTripWrapped(reporter: MetricsReporter, "reporter")
        result shouldEqual reporter
      }
    }

    it("MetricsReporter.Csv should round-trip") {
      forAll(ConfigGenerators.Gens.metricsReporterCsv) { (reporter: MetricsReporter.Csv) =>
        val result = roundTripWrapped(reporter: MetricsReporter, "reporter")
        result shouldEqual reporter
      }
    }

    it("MetricsReporter.Influxdb should round-trip") {
      forAll(ConfigGenerators.Gens.metricsReporterInfluxdb) { (reporter: MetricsReporter.Influxdb) =>
        val result = roundTripWrapped(reporter: MetricsReporter, "reporter")
        result shouldEqual reporter
      }
    }

    it("MetricsReporter (any subtype) should round-trip") {
      forAll { (reporter: MetricsReporter) =>
        val result = roundTripWrapped(reporter, "reporter")
        result shouldEqual reporter
      }
    }
  }

  describe("PersistenceAgentType round-trips") {

    it("PersistenceAgentType.Empty should round-trip") {
      val result = roundTripWrapped(PersistenceAgentType.Empty: PersistenceAgentType, "store")
      result shouldEqual PersistenceAgentType.Empty
    }

    it("PersistenceAgentType.InMemory should round-trip") {
      val result = roundTripWrapped(PersistenceAgentType.InMemory: PersistenceAgentType, "store")
      result shouldEqual PersistenceAgentType.InMemory
    }

    it("PersistenceAgentType.RocksDb should round-trip") {
      forAll(ConfigGenerators.Gens.persistenceRocksDb) { (store: PersistenceAgentType.RocksDb) =>
        val result = roundTripWrapped(store: PersistenceAgentType, "store")
        result shouldEqual store
      }
    }

    it("PersistenceAgentType.MapDb should round-trip") {
      forAll(ConfigGenerators.Gens.persistenceMapDb) { (store: PersistenceAgentType.MapDb) =>
        val result = roundTripWrapped(store: PersistenceAgentType, "store")
        result shouldEqual store
      }
    }

    it("PersistenceAgentType.ClickHouse should round-trip") {
      forAll(ConfigGenerators.Gens.persistenceClickHouse) { (store: PersistenceAgentType.ClickHouse) =>
        val result = roundTripWrapped(store: PersistenceAgentType, "store")
        result shouldEqual store
      }
    }

    it("PersistenceAgentType.OAuth2Config should round-trip") {
      forAll(ConfigGenerators.Gens.oAuth2Config) { (oauth: PersistenceAgentType.OAuth2Config) =>
        val result = roundTrip(oauth)
        result.clientId shouldEqual oauth.clientId
        result.certFile shouldEqual oauth.certFile
        result.certAlias shouldEqual oauth.certAlias
        result.certFilePassword shouldEqual oauth.certFilePassword
        result.keyAlias shouldEqual oauth.keyAlias
        result.adfsEnv shouldEqual oauth.adfsEnv
        result.resourceURI shouldEqual oauth.resourceURI
        result.discoveryURL shouldEqual oauth.discoveryURL
      }
    }

    it("PersistenceAgentType.Cassandra should round-trip") {
      forAll(ConfigGenerators.Gens.persistenceCassandra) { (store: PersistenceAgentType.Cassandra) =>
        val result = roundTripWrapped(store: PersistenceAgentType, "store").asInstanceOf[PersistenceAgentType.Cassandra]
        assert(sameCassandra(result, store), s"Cassandra configs differ:\nResult: $result\nExpected: $store")
      }
    }

    it("PersistenceAgentType.Keyspaces should round-trip") {
      forAll(ConfigGenerators.Gens.persistenceKeyspaces) { (store: PersistenceAgentType.Keyspaces) =>
        val result = roundTripWrapped(store: PersistenceAgentType, "store")
        result shouldEqual store
      }
    }

    it("PersistenceAgentType (any subtype) should round-trip") {
      forAll { (store: PersistenceAgentType) =>
        val result = roundTripWrapped(store, "store")
        // Cassandra has types with problematic equality (InetSocketAddress, Array[Char])
        (result, store) match {
          case (r: PersistenceAgentType.Cassandra, s: PersistenceAgentType.Cassandra) =>
            assert(sameCassandra(r, s), s"Cassandra configs differ:\nResult: $r\nExpected: $s")
          case _ => result shouldEqual store
        }
      }
    }
  }

  describe("WebServer config round-trips") {

    it("SslConfig should round-trip") {
      forAll { (ssl: SslConfig) =>
        val result = roundTrip(ssl)
        result.path shouldEqual ssl.path
        result.password shouldEqual ssl.password
      }
    }

    it("MtlsTrustStore should round-trip") {
      forAll { (store: MtlsTrustStore) =>
        val result = roundTrip(store)
        result shouldEqual store
      }
    }

    it("MtlsHealthEndpoints should round-trip") {
      forAll { (endpoints: MtlsHealthEndpoints) =>
        val result = roundTrip(endpoints)
        result shouldEqual endpoints
      }
    }

    it("UseMtls should round-trip") {
      forAll { (mtls: UseMtls) =>
        val result = roundTrip(mtls)
        result.enabled shouldEqual mtls.enabled
        result.healthEndpoints shouldEqual mtls.healthEndpoints
        // trustStore may have Array[Char] comparison issues, check fields
        (result.trustStore, mtls.trustStore) match {
          case (Some(r), Some(m)) =>
            r.path shouldEqual m.path
            r.password shouldEqual m.password
          case (None, None) => succeed
          case _ => fail("trustStore mismatch")
        }
      }
    }

    it("WebServerBindConfig should round-trip") {
      forAll { (config: WebServerBindConfig) =>
        val result = roundTrip(config)
        result.address shouldEqual config.address
        result.port shouldEqual config.port
        result.enabled shouldEqual config.enabled
        result.useTls shouldEqual config.useTls
        result.useMtls.enabled shouldEqual config.useMtls.enabled
      }
    }

    it("WebserverAdvertiseConfig should round-trip") {
      forAll { (config: WebserverAdvertiseConfig) =>
        val result = roundTrip(config)
        result shouldEqual config
      }
    }
  }

  describe("Simple config round-trips") {

    it("MetricsConfig should round-trip") {
      forAll { (config: MetricsConfig) =>
        val result = roundTrip(config)
        result shouldEqual config
      }
    }

    it("ResolutionMode should round-trip") {
      forAll { (mode: ResolutionMode) =>
        val result = roundTrip(mode)
        result shouldEqual mode
      }
    }

    it("FileIngestConfig should round-trip") {
      forAll { (config: FileIngestConfig) =>
        val result = roundTrip(config)
        result shouldEqual config
      }
    }
  }
}
