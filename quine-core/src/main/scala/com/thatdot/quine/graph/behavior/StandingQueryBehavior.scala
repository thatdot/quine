package com.thatdot.quine.graph.behavior
import com.thatdot.quine.graph.{StandingQueryLocalEventIndex, StandingQueryOpsGraph}

trait StandingQueryBehavior {

  protected def graph: StandingQueryOpsGraph

  protected def localEventIndex: StandingQueryLocalEventIndex
}
