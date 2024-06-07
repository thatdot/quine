package com.thatdot.quine.app.v2api

import org.apache.pekko.util.Timeout

import com.typesafe.scalalogging.LazyLogging

import com.thatdot.quine.app.QuineApp
import com.thatdot.quine.app.config.{BaseConfig, QuineConfig}
import com.thatdot.quine.app.v2api.definitions.ApplicationApiInterface
import com.thatdot.quine.graph.GraphService
class OssApiInterface(
  val graph: GraphService,
  val app: QuineApp,
  val config: BaseConfig,
  val timeout: Timeout
) extends ApplicationApiInterface
    with LazyLogging {
  val thisMemberIdx: Int = 0

  override def emptyConfigExample: BaseConfig = QuineConfig()
}
