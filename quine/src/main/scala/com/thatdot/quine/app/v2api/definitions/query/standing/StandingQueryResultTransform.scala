package com.thatdot.quine.app.v2api.definitions.query.standing

sealed trait StandingQueryResultTransform

object StandingQueryResultTransform {
  case object InlineData extends StandingQueryResultTransform
}
