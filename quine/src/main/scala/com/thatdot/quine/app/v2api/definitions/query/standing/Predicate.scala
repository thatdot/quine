package com.thatdot.quine.app.v2api.definitions.query.standing

// TODO Consider lifting Quine and Novelty Predicates to shared modules
sealed trait Predicate

object Predicate {
  case object OnlyPositiveMatch extends Predicate
}
