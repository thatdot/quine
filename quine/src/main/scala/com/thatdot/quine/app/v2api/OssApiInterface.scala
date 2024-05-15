package com.thatdot.quine.v2api

import com.typesafe.scalalogging.LazyLogging

import com.thatdot.quine.app.BaseApp
import com.thatdot.quine.app.v2api.definitions.ApplicationApiInterface
import com.thatdot.quine.graph.{BaseGraph, LiteralOpsGraph}
class OssApiInterface(val graph: BaseGraph with LiteralOpsGraph, val app: BaseApp)
    extends ApplicationApiInterface
    with LazyLogging {
  val thisMemberIdx: Int = 0

}
