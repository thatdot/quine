package com.thatdot.quine.graph.behavior
import com.thatdot.quine.graph.{StandingQueryOpsGraph, StandingQueryWatchableEventIndex}

trait StandingQueryBehavior {

  protected def graph: StandingQueryOpsGraph

  protected def watchableEventIndex: StandingQueryWatchableEventIndex
}
