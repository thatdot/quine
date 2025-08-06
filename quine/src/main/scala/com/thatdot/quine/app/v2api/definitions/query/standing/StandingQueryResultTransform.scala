package com.thatdot.quine.app.v2api.definitions.query.standing

import sttp.tapir.Schema.annotations.description

sealed trait StandingQueryResultTransform

object StandingQueryResultTransform {
  import com.thatdot.quine.app.util.StringOps.syntax._

  @description(
    """Extracts, or "lifts", the `data` field of a Standing Query Result such that the data is no longer wrapped,
      |but the root-level object. Assumes a Standing Query Result with a `data` field.""".asOneLine,
  )
  case object InlineData extends StandingQueryResultTransform
}
