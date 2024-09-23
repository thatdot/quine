package com.thatdot.quine.app.v2api

import org.apache.pekko.util.Timeout

import com.thatdot.quine.app.QuineApp
import com.thatdot.quine.app.config.{BaseConfig, QuineConfig}
import com.thatdot.quine.app.v2api.definitions.QuineApiMethods
import com.thatdot.quine.graph.GraphService
import com.thatdot.quine.util.Log._
class OssApiMethods(
  val graph: GraphService,
  val app: QuineApp,
  val config: BaseConfig,
  val timeout: Timeout,
)(implicit val logConfig: LogConfig)
    extends QuineApiMethods
    with LazySafeLogging {
  val thisMemberIdx: Int = 0

  override def emptyConfigExample: BaseConfig = QuineConfig()

}
