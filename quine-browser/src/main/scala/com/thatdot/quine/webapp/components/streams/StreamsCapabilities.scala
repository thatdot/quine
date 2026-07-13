package com.thatdot.quine.webapp.components.streams

/** What the current user may do on the Streams page beyond viewing it.
  *
  * The host app derives these from the user's permissions; controls whose
  * capability is false are not rendered. [[all]] is for deployments without
  * access control.
  */
final case class StreamsCapabilities(
  canCreateIngest: Boolean,
  canControlIngest: Boolean,
  canDeleteIngest: Boolean,
  canCreateStandingQuery: Boolean,
  canWriteStandingQueryOutputs: Boolean,
  canDeleteStandingQuery: Boolean,
)

object StreamsCapabilities {
  val all: StreamsCapabilities = StreamsCapabilities(
    canCreateIngest = true,
    canControlIngest = true,
    canDeleteIngest = true,
    canCreateStandingQuery = true,
    canWriteStandingQueryOutputs = true,
    canDeleteStandingQuery = true,
  )
}
