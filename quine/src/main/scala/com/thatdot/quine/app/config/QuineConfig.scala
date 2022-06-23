package com.thatdot.quine.app.config

import java.io.File

import scala.annotation.nowarn
import scala.concurrent.duration.{Duration, DurationInt, FiniteDuration}

import akka.util.Timeout

import com.typesafe.config.ConfigValue
import pureconfig._
import pureconfig.generic.ProductHint
import pureconfig.generic.auto._
import pureconfig.generic.semiauto.deriveConvert

import com.thatdot.quine.persistor.PersistenceConfig

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
  webserver: WebServerConfig = WebServerConfig("0.0.0.0", 8080),
  shouldResumeIngest: Boolean = false,
  shardCount: Int = 4,
  id: IdProviderType = IdProviderType.UUID(),
  edgeIteration: EdgeIteration = EdgeIteration.ReverseInsertion,
  store: PersistenceAgentType = PersistenceAgentType.RocksDb(new File("quine.db")),
  persistence: PersistenceConfig = PersistenceConfig(),
  labelsProperty: String = "__LABEL",
  metricsReporters: List[MetricsReporter] = List(MetricsReporter.Jmx)
) extends BaseConfig {

  def configVal: ConfigValue = ConfigWriter[QuineConfig].to(this)
}

object QuineConfig {

  implicit val configConvert: ConfigConvert[QuineConfig] = {
    import Implicits._

    @nowarn implicit val configConvert = deriveConvert[QuineConfig]

    // This class is necessary to make sure our config is always situated at the `quine` root
    case class QuineConfigRoot(quine: QuineConfig = QuineConfig())

    // Allow other top-level keys that are not "quine"
    @nowarn implicit val topLevelProductHint: ProductHint[QuineConfigRoot] =
      ProductHint[QuineConfigRoot](allowUnknownKeys = true)

    deriveConvert[QuineConfigRoot].xmap[QuineConfig](_.quine, QuineConfigRoot(_))
  }
}
