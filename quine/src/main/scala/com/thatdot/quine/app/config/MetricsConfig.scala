package com.thatdot.quine.app.config

import pureconfig.ConfigConvert
import pureconfig.generic.semiauto.deriveConvert

case class MetricsConfig(enableDebugMetrics: Boolean = false)

object MetricsConfig extends PureconfigInstances {
  implicit val configConvert: ConfigConvert[MetricsConfig] = deriveConvert[MetricsConfig]
}
