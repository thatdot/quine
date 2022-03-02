package com.thatdot.quine.graph

import akka.actor.SupervisorStrategy._
import akka.actor.{OneForOneStrategy, SupervisorStrategy, SupervisorStrategyConfigurator}

import com.typesafe.scalalogging.LazyLogging
class NodeAndShardSupervisorStrategy extends SupervisorStrategyConfigurator with LazyLogging {
  private val specialCases: Decider = {
    // irrecoverable by definition
    case _: NodeWakeupFailedException =>
      // This will pass up the stack until reaching the [[NodeAndShardSupervisorStrategy]] instance supervising `/user`
      // at which point it will kill the actorsystem
      Escalate
  }
  val decider: Decider = specialCases orElse defaultDecider
  def create(): SupervisorStrategy = OneForOneStrategy()(decider)
}
