package com.thatdot.quine.app

import java.io.File
import java.net.InetSocketAddress

import scala.annotation.nowarn
import scala.concurrent.duration.{Duration, DurationInt, FiniteDuration}
import scala.sys.exit

import akka.util.Timeout

import cats.implicits._
import com.typesafe.config.ConfigValue
import pureconfig._
import pureconfig.error.ConfigReaderException
import pureconfig.generic.ProductHint
import pureconfig.generic.auto._
import pureconfig.generic.semiauto.deriveEnumerationConvert

import com.thatdot.quine.app.config.{
  Address,
  BaseConfig,
  EdgeIteration,
  IdProviderType,
  MetricsReporter,
  PersistenceAgentType
}
import com.thatdot.quine.persistor.{PersistenceConfig, PersistenceSchedule}

object Config extends BaseConfig {

  // Unknown keys anywhere inside the `quine { .. }` scope are errors
  // TODO: scala 2.12 incorrectly thinks it is unused
  @nowarn implicit private[this] def sealedProductHint[T]: ProductHint[T] = ProductHint[T](allowUnknownKeys = false)
  @nowarn implicit private[this] val topLevelProductHint: ProductHint[Config] =
    ProductHint[Config](allowUnknownKeys = true)

  final case class QuineConfig(
    dumpConfig: Boolean = false,
    timeout: Timeout = Timeout(120.seconds),
    inMemorySoftNodeLimit: Option[Int] = Some(10000),
    inMemoryHardNodeLimit: Option[Int] = Some(75000),
    declineSleepWhenWriteWithin: FiniteDuration = 100.millis,
    declineSleepWhenAccessWithin: FiniteDuration = Duration.Zero,
    webserver: WebServerConfig = WebServerConfig("0.0.0.0", 8080),
    shouldResumeIngest: Boolean = false,
    shardCount: Int = 4,
    id: IdProviderType = IdProviderType.UUID(),
    edgeIteration: EdgeIteration = EdgeIteration.ReverseInsertion,
    store: PersistenceAgentType = PersistenceAgentType.RocksDb(new File("quine.db")),
    persistence: PersistenceConfig = PersistenceConfig(),
    labelsProperty: String = "__LABEL",
    metricsReporters: List[MetricsReporter] = List(MetricsReporter.Jmx)
  )

  final case class WebServerConfig(
    address: String,
    port: Int
  )

  implicit val timeoutConvert: ConfigConvert[Timeout] = ConfigConvert[FiniteDuration].xmap(Timeout(_), _.duration)

  implicit val persistenceScheduleConvert: ConfigConvert[PersistenceSchedule] =
    deriveEnumerationConvert[PersistenceSchedule]

  // TODO: this assumes the Cassandra port if port is omitted! (so beware about re-using it)
  implicit val inetSocketAddressConvert: ConfigConvert[InetSocketAddress] =
    ConfigConvert.viaNonEmptyString[InetSocketAddress](
      s => Right(Address.parseHostAndPort(s, PersistenceAgentType.defaultCassandraPort)),
      addr => addr.getHostString + ':' + addr.getPort
    )

  // This class is necessary to make sure our config is always situated at the `quine` root
  final private case class Config(quine: QuineConfig = QuineConfig())

  val quineReader: ConfigReader[QuineConfig] = ConfigReader[Config].map(_.quine)
  val quineWriter: ConfigWriter[QuineConfig] = ConfigWriter[Config].contramap(c => Config(c))

  /** The config that gets loaded at startup from the `.conf` file */
  val config: QuineConfig = ConfigSource.default
    .load(quineReader)
    .valueOr { failures =>
      println(new ConfigReaderException[QuineConfig](failures).getMessage())
      println("Did you forget to pass in a config file?")
      println("  $ java -Dconfig.file=your-conf-file.conf -jar quine.jar")
      exit(1)
    }

  val configVal: ConfigValue = quineWriter.to(config)
}
