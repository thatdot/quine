package com.thatdot.quine.graph

import java.util.UUID

/** ID for a top-level standing query
  *
  * @param uuid identifier
  */
final case class StandingQueryId(uuid: UUID) extends AnyVal

object StandingQueryId {

  /** Generate a fresh standing query ID */
  def fresh(): StandingQueryId = StandingQueryId(UUID.randomUUID())
}

/** ID for a part of a standing query (ie. some sub-query component)
  *
  * @param uuid identifier
  */
final case class MultipleValuesStandingQueryPartId(uuid: UUID) extends AnyVal
