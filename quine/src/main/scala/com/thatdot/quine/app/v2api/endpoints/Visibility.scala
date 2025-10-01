package com.thatdot.quine.app.v2api.endpoints

import sttp.tapir.AttributeKey

/** Endpoint visibility for doc generation */
sealed trait Visibility

object Visibility {
  case object Visible extends Visibility
  case object Hidden extends Visibility

  val attributeKey: AttributeKey[Visibility] = AttributeKey[Visibility]
}
