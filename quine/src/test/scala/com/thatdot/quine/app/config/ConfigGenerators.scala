package com.thatdot.quine.app.config

import java.io.File
import java.net.InetSocketAddress

import scala.concurrent.duration._

import com.datastax.oss.driver.api.core.{ConsistencyLevel, DefaultConsistencyLevel}
import org.scalacheck.{Arbitrary, Gen}
import software.amazon.awssdk.regions.Region

import com.thatdot.quine.ScalaPrimitiveGenerators
import com.thatdot.quine.util.{Host, Port}

/** Generators for pureconfig types used in QuineConfig and related configuration. */
object ConfigGenerators {

  import ScalaPrimitiveGenerators.Gens._

  object Gens {

    val host: Gen[Host] = nonEmptyAlphaNumStr.map(Host(_))
    val port: Gen[Port] = ScalaPrimitiveGenerators.Gens.port.map(Port(_))

    val finiteDuration: Gen[FiniteDuration] = for {
      amount <- mediumPosLong
      unit <- Gen.oneOf(SECONDS, MINUTES)
    } yield FiniteDuration(amount, unit)

    val file: Gen[File] = nonEmptyAlphaNumStr.map(name => new File(s"/tmp/$name"))
    val optFile: Gen[Option[File]] = Gen.option(file)

    val charArray: Gen[Array[Char]] = nonEmptyAlphaNumStr.map(_.toCharArray)

    val inetSocketAddress: Gen[InetSocketAddress] = for {
      host <- nonEmptyAlphaNumStr
      port <- ScalaPrimitiveGenerators.Gens.port
    } yield new InetSocketAddress(host, port)

    val consistencyLevel: Gen[ConsistencyLevel] = Gen.oneOf(
      DefaultConsistencyLevel.ONE,
      DefaultConsistencyLevel.LOCAL_ONE,
      DefaultConsistencyLevel.LOCAL_QUORUM,
      DefaultConsistencyLevel.QUORUM,
      DefaultConsistencyLevel.ALL,
    )

    val idProviderLong: Gen[IdProviderType.Long] = for {
      consecutiveStart <- Gen.option(largePosLong)
      partitioned <- bool
    } yield IdProviderType.Long(consecutiveStart, partitioned)

    val idProviderUUID: Gen[IdProviderType.UUID] = bool.map(IdProviderType.UUID(_))
    val idProviderUuid4: Gen[IdProviderType.Uuid4] = bool.map(IdProviderType.Uuid4(_))
    val idProviderByteArray: Gen[IdProviderType.ByteArray] = bool.map(IdProviderType.ByteArray(_))

    val idProviderType: Gen[IdProviderType] = Gen.oneOf(
      idProviderLong,
      idProviderUUID,
      idProviderUuid4,
      idProviderByteArray,
    )

    val metricsReporterJmx: Gen[MetricsReporter.Jmx.type] = Gen.const(MetricsReporter.Jmx)

    val metricsReporterSlf4j: Gen[MetricsReporter.Slf4j] = for {
      period <- finiteDuration
      loggerName <- nonEmptyAlphaNumStr
    } yield MetricsReporter.Slf4j(period, loggerName)

    val metricsReporterCsv: Gen[MetricsReporter.Csv] = for {
      period <- finiteDuration
      logDir <- file
    } yield MetricsReporter.Csv(period, logDir)

    val metricsReporterInfluxdb: Gen[MetricsReporter.Influxdb] = for {
      period <- finiteDuration
      database <- nonEmptyAlphaNumStr
      scheme <- Gen.oneOf("http", "https")
      host <- nonEmptyAlphaNumStr
      port <- ScalaPrimitiveGenerators.Gens.port
      user <- optNonEmptyAlphaNumStr
      password <- optNonEmptyAlphaNumStr
    } yield MetricsReporter.Influxdb(period, database, scheme, host, port, user, password)

    val metricsReporter: Gen[MetricsReporter] = Gen.oneOf(
      metricsReporterJmx,
      metricsReporterSlf4j,
      metricsReporterCsv,
      metricsReporterInfluxdb,
    )

    val persistenceEmpty: Gen[PersistenceAgentType.Empty.type] = Gen.const(PersistenceAgentType.Empty)
    val persistenceInMemory: Gen[PersistenceAgentType.InMemory.type] = Gen.const(PersistenceAgentType.InMemory)

    private val bloomFilterSize: Gen[Option[Long]] = Gen.option(largePosLong)

    val persistenceRocksDb: Gen[PersistenceAgentType.RocksDb] = for {
      filepath <- optFile
      writeAheadLog <- bool
      syncAllWrites <- bool
      createParentDir <- bool
      bloomFilterSize <- bloomFilterSize
    } yield PersistenceAgentType.RocksDb(filepath, writeAheadLog, syncAllWrites, createParentDir, bloomFilterSize)

    val persistenceMapDb: Gen[PersistenceAgentType.MapDb] = for {
      filepath <- optFile
      numberPartitions <- smallPosNum
      writeAheadLog <- bool
      commitInterval <- finiteDuration
      createParentDir <- bool
      bloomFilterSize <- bloomFilterSize
    } yield PersistenceAgentType.MapDb(
      filepath,
      numberPartitions,
      writeAheadLog,
      commitInterval,
      createParentDir,
      bloomFilterSize,
    )

    val persistenceClickHouse: Gen[PersistenceAgentType.ClickHouse] = for {
      url <- nonEmptyAlphaNumStr.map(h => s"http://$h:8123")
      database <- nonEmptyAlphaNumStr
      username <- optNonEmptyAlphaNumStr
      password <- optNonEmptyAlphaNumStr
      bloomFilterSize <- bloomFilterSize
    } yield PersistenceAgentType.ClickHouse(url, database, username, password, bloomFilterSize)

    // AWS Region - use a subset of common regions
    val awsRegion: Gen[Region] = Gen.oneOf(
      Region.US_EAST_1,
      Region.US_WEST_2,
      Region.EU_WEST_1,
      Region.AP_NORTHEAST_1,
    )

    val oAuth2Config: Gen[PersistenceAgentType.OAuth2Config] = for {
      clientId <- nonEmptyAlphaNumStr
      certFile <- nonEmptyAlphaNumStr.map(s => s"/path/to/$s.pem")
      certAlias <- optNonEmptyAlphaNumStr
      certFilePassword <- charArray
      keyAlias <- optNonEmptyAlphaNumStr
      adfsEnv <- optNonEmptyAlphaNumStr
      resourceURI <- optNonEmptyAlphaNumStr.map(_.map(s => s"https://$s"))
      discoveryURL <- optNonEmptyAlphaNumStr.map(_.map(s => s"https://$s/.well-known"))
    } yield PersistenceAgentType.OAuth2Config(
      clientId,
      certFile,
      certAlias,
      certFilePassword,
      keyAlias,
      adfsEnv,
      resourceURI,
      discoveryURL,
    )

    val persistenceCassandra: Gen[PersistenceAgentType.Cassandra] = for {
      keyspace <- optNonEmptyAlphaNumStr
      replicationFactor <- smallPosNum
      readConsistency <- consistencyLevel
      writeConsistency <- consistencyLevel
      endpoints <- Gen.nonEmptyListOf(inetSocketAddress)
      localDatacenter <- nonEmptyAlphaNumStr
      writeTimeout <- finiteDuration
      readTimeout <- finiteDuration
      shouldCreateTables <- bool
      shouldCreateKeyspace <- bool
      bloomFilterSize <- bloomFilterSize
      snapshotPartMaxSizeBytes <- largePosNum
      oauth <- Gen.option(oAuth2Config)
    } yield PersistenceAgentType.Cassandra(
      keyspace,
      replicationFactor,
      readConsistency,
      writeConsistency,
      endpoints,
      localDatacenter,
      writeTimeout,
      readTimeout,
      shouldCreateTables,
      shouldCreateKeyspace,
      bloomFilterSize,
      snapshotPartMaxSizeBytes,
      oauth,
    )

    // Keyspaces only supports ONE, LOCAL_ONE, LOCAL_QUORUM for read consistency
    val keyspacesReadConsistency: Gen[ConsistencyLevel] = Gen.oneOf(
      DefaultConsistencyLevel.ONE,
      DefaultConsistencyLevel.LOCAL_ONE,
      DefaultConsistencyLevel.LOCAL_QUORUM,
    )

    val persistenceKeyspaces: Gen[PersistenceAgentType.Keyspaces] = for {
      keyspace <- optNonEmptyAlphaNumStr
      region <- Gen.option(awsRegion)
      awsRoleArn <- optNonEmptyAlphaNumStr.map(_.map(s => s"arn:aws:iam::123456789:role/$s"))
      readConsistency <- keyspacesReadConsistency
      writeTimeout <- finiteDuration
      readTimeout <- finiteDuration
      shouldCreateTables <- bool
      shouldCreateKeyspace <- bool
      bloomFilterSize <- bloomFilterSize
      snapshotPartMaxSizeBytes <- largePosNum
    } yield PersistenceAgentType.Keyspaces(
      keyspace,
      region,
      awsRoleArn,
      readConsistency,
      writeTimeout,
      readTimeout,
      shouldCreateTables,
      shouldCreateKeyspace,
      bloomFilterSize,
      snapshotPartMaxSizeBytes,
    )

    val persistenceAgentType: Gen[PersistenceAgentType] = Gen.oneOf(
      persistenceEmpty,
      persistenceInMemory,
      persistenceRocksDb,
      persistenceMapDb,
      persistenceClickHouse,
      persistenceCassandra,
      persistenceKeyspaces,
    )

    val sslConfig: Gen[SslConfig] = for {
      path <- file
      password <- charArray
    } yield SslConfig(path, password)

    val mtlsTrustStore: Gen[MtlsTrustStore] = for {
      path <- file
      password <- nonEmptyAlphaNumStr
    } yield MtlsTrustStore(path, password)

    val mtlsHealthEndpoints: Gen[MtlsHealthEndpoints] = for {
      enabled <- bool
      p <- port
    } yield MtlsHealthEndpoints(enabled, p)

    val useMtls: Gen[UseMtls] = for {
      enabled <- bool
      trustStore <- Gen.option(mtlsTrustStore)
      healthEndpoints <- mtlsHealthEndpoints
    } yield UseMtls(enabled, trustStore, healthEndpoints)

    val webServerBindConfig: Gen[WebServerBindConfig] = for {
      address <- host
      p <- port
      enabled <- bool
      useTls <- bool
      mtls <- useMtls
    } yield WebServerBindConfig(address, p, enabled, useTls, mtls)

    val webserverAdvertiseConfig: Gen[WebserverAdvertiseConfig] = for {
      address <- host
      p <- port
      path <- optNonEmptyAlphaNumStr.map(_.map(s => s"/$s"))
    } yield WebserverAdvertiseConfig(address, p, path)

    val metricsConfig: Gen[MetricsConfig] = bool.map(MetricsConfig(_))

    val resolutionMode: Gen[ResolutionMode] = Gen.oneOf(ResolutionMode.Static, ResolutionMode.Dynamic)

    val fileIngestConfig: Gen[FileIngestConfig] = for {
      allowedDirs <- Gen.option(Gen.listOfN(2, nonEmptyAlphaNumStr.map(s => s"/dir/$s")))
      mode <- Gen.option(resolutionMode)
    } yield FileIngestConfig(allowedDirs, mode)
  }

  object Arbs {
    implicit val host: Arbitrary[Host] = Arbitrary(Gens.host)
    implicit val port: Arbitrary[Port] = Arbitrary(Gens.port)
    implicit val finiteDuration: Arbitrary[FiniteDuration] = Arbitrary(Gens.finiteDuration)
    implicit val file: Arbitrary[File] = Arbitrary(Gens.file)
    implicit val charArray: Arbitrary[Array[Char]] = Arbitrary(Gens.charArray)
    implicit val inetSocketAddress: Arbitrary[InetSocketAddress] = Arbitrary(Gens.inetSocketAddress)
    implicit val consistencyLevel: Arbitrary[ConsistencyLevel] = Arbitrary(Gens.consistencyLevel)
    implicit val idProviderType: Arbitrary[IdProviderType] = Arbitrary(Gens.idProviderType)
    implicit val metricsReporter: Arbitrary[MetricsReporter] = Arbitrary(Gens.metricsReporter)
    implicit val awsRegion: Arbitrary[Region] = Arbitrary(Gens.awsRegion)
    implicit val oAuth2Config: Arbitrary[PersistenceAgentType.OAuth2Config] = Arbitrary(Gens.oAuth2Config)
    implicit val persistenceCassandra: Arbitrary[PersistenceAgentType.Cassandra] = Arbitrary(Gens.persistenceCassandra)
    implicit val persistenceKeyspaces: Arbitrary[PersistenceAgentType.Keyspaces] = Arbitrary(Gens.persistenceKeyspaces)
    implicit val persistenceAgentType: Arbitrary[PersistenceAgentType] = Arbitrary(Gens.persistenceAgentType)
    implicit val sslConfig: Arbitrary[SslConfig] = Arbitrary(Gens.sslConfig)
    implicit val mtlsTrustStore: Arbitrary[MtlsTrustStore] = Arbitrary(Gens.mtlsTrustStore)
    implicit val mtlsHealthEndpoints: Arbitrary[MtlsHealthEndpoints] = Arbitrary(Gens.mtlsHealthEndpoints)
    implicit val useMtls: Arbitrary[UseMtls] = Arbitrary(Gens.useMtls)
    implicit val webServerBindConfig: Arbitrary[WebServerBindConfig] = Arbitrary(Gens.webServerBindConfig)
    implicit val webserverAdvertiseConfig: Arbitrary[WebserverAdvertiseConfig] = Arbitrary(
      Gens.webserverAdvertiseConfig,
    )
    implicit val metricsConfig: Arbitrary[MetricsConfig] = Arbitrary(Gens.metricsConfig)
    implicit val resolutionMode: Arbitrary[ResolutionMode] = Arbitrary(Gens.resolutionMode)
    implicit val fileIngestConfig: Arbitrary[FileIngestConfig] = Arbitrary(Gens.fileIngestConfig)
  }
}
