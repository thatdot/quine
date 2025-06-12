package com.thatdot.quine.app.model.outputs2.query.standing

import com.thatdot.quine.graph.StandingQueryResult

sealed trait Predicate {
  def apply(standingQueryResult: StandingQueryResult): Boolean
}

object Predicate {
  case object OnlyPositiveMatch extends Predicate {
    override def apply(standingQueryResult: StandingQueryResult): Boolean = standingQueryResult.meta.isPositiveMatch
  }
}
