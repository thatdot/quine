package com.thatdot.quine.app.v2api.definitions.query.standing

sealed trait Predicate

object Predicate {
  case object OnlyPositiveMatch extends Predicate
}
