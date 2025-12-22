package com.thatdot.quine.app.v2api.definitions

import sttp.tapir.Schema

import com.thatdot.common.quineid.QuineId

/** Tapir schema for QuineId, defined in the API layer since `quine-id` doesn't depend on Tapir. */
trait QuineIdSchemas {
  implicit val quineIdSchema: Schema[QuineId] = Schema.string[QuineId]
}
