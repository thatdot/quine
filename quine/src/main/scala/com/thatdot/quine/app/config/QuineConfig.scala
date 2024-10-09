package com.thatdot.quine.app.config

import scala.concurrent.duration.{Duration, DurationInt, FiniteDuration}

import org.apache.pekko.util.Timeout

import com.typesafe.config.{Config, ConfigObject}
import pureconfig._
import pureconfig.generic.ProductHint
import pureconfig.generic.auto._
import pureconfig.generic.semiauto.deriveConvert
import shapeless.{Lens, lens}

import com.thatdot.quine.persistor.PersistenceConfig
import com.thatdot.quine.util.Log.LogConfig

/** Top-level config for Quine
  *
  * See `documented_config.conf` inside the test resources for documentation
  */
final case class QuineConfig(
  dumpConfig: Boolean = false,
  timeout: Timeout = Timeout(120.seconds),
  inMemorySoftNodeLimit: Option[Int] = Some(10000),
  inMemoryHardNodeLimit: Option[Int] = Some(75000),
  declineSleepWhenWriteWithin: FiniteDuration = 100.millis,
  declineSleepWhenAccessWithin: FiniteDuration = Duration.Zero,
  maxCatchUpSleep: FiniteDuration = 2000.millis,
  webserver: WebServerBindConfig = WebServerBindConfig(),
  webserverAdvertise: Option[WebserverAdvertiseConfig] = None,
  shouldResumeIngest: Boolean = false,
  shardCount: Int = 4,
  id: IdProviderType = IdProviderType.UUID(),
  edgeIteration: EdgeIteration = EdgeIteration.ReverseInsertion,
  store: PersistenceAgentType = PersistenceAgentType.RocksDb(),
  persistence: PersistenceConfig = PersistenceConfig(),
  labelsProperty: Symbol = Symbol("__LABEL"),
  metricsReporters: List[MetricsReporter] = List(MetricsReporter.Jmx),
  metrics: MetricsConfig = MetricsConfig(),
  helpMakeQuineBetter: Boolean = true,
  api2Enabled: Boolean = false,
  logConfig: LogConfig = LogConfig.strictest,
) extends BaseConfig {

  def configVal: Config = ConfigWriter[QuineConfig].to(this).asInstanceOf[ConfigObject].toConfig
}

object QuineConfig extends PureconfigInstances {

  val webserverLens: Lens[QuineConfig, WebServerBindConfig] = lens[QuineConfig] >> Symbol("webserver")
  val webserverPortLens: Lens[QuineConfig, Int] = webserverLens >> Symbol("port") >> Symbol("asInt")
  val webserverEnabledLens: Lens[QuineConfig, Boolean] = webserverLens >> Symbol("enabled")

  implicit val configConvert: ConfigConvert[QuineConfig] = {
    implicit val configConvert = deriveConvert[QuineConfig]

    // This class is necessary to make sure our config is always situated at the `quine` root
    case class QuineConfigRoot(quine: QuineConfig = QuineConfig())

    // Allow other top-level keys that are not "quine"
    implicit val topLevelProductHint: ProductHint[QuineConfigRoot] =
      ProductHint[QuineConfigRoot](allowUnknownKeys = true)

    deriveConvert[QuineConfigRoot].xmap[QuineConfig](_.quine, QuineConfigRoot(_))
  }
}
