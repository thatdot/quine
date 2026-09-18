package com.thatdot.quine.app.config

import pureconfig.ConfigConvert
import pureconfig.generic.semiauto.deriveConvert

import com.thatdot.quine.graph.metrics.HotNodeMetricsConfig

/** Settings under `quine.metrics`
  *
  * @param enableDebugMetrics whether to collect metrics whose collection has a noticeable runtime cost
  * @param hotNodes           the hot-node gauges (mailbox backlog and message rate per node), see
  *                           [[com.thatdot.quine.graph.metrics.HotNodeSampler]]
  */
case class MetricsConfig(
  enableDebugMetrics: Boolean = false,
  hotNodes: HotNodeMetricsConfig = HotNodeMetricsConfig(),
)

object MetricsConfig extends PureconfigInstances {
  implicit val hotNodeMetricsConfigConvert: ConfigConvert[HotNodeMetricsConfig] =
    deriveConvert[HotNodeMetricsConfig]
  implicit val configConvert: ConfigConvert[MetricsConfig] = deriveConvert[MetricsConfig]
}
