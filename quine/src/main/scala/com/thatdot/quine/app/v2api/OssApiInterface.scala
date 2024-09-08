package com.thatdot.quine.app.v2api

import org.apache.pekko.util.Timeout

import com.thatdot.quine.app.QuineApp
import com.thatdot.quine.app.config.{BaseConfig, QuineConfig}
import com.thatdot.quine.app.v2api.definitions.ApplicationApiInterface
import com.thatdot.quine.graph.GraphService
import com.thatdot.quine.util.Log._
class OssApiInterface(
  val graph: GraphService,
  val quineApp: QuineApp,
  val config: BaseConfig,
  val timeout: Timeout,
) extends ApplicationApiInterface
    with LazySafeLogging {
  val thisMemberIdx: Int = 0

  override def emptyConfigExample: BaseConfig = QuineConfig()

}
