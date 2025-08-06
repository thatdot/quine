package com.thatdot.quine.app.v2api.definitions.query.standing

import sttp.tapir.Schema.annotations.description

// TODO Consider lifting Quine and Novelty Predicates to shared modules
sealed trait Predicate

object Predicate {
  @description(
    "Returns `true` when a Standing Query Result's metadata includes a `true` value for the `isPositiveMatch` field.",
  )
  case object OnlyPositiveMatch extends Predicate
}
