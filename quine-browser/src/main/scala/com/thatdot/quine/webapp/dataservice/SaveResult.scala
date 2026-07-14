package com.thatdot.quine.webapp.dataservice

/** Outcome of a save command, delivered point-to-point to that command's `replyTo`
  * observer (so only the surface that issued the save reacts — it owns optimistic UI,
  * rollback, and toasts; the data effect broadcasts through the slice's signal via
  * invalidation). Shared by every capability slice whose commands persist state
  * ([[QueryUiConfigService]], [[TapQueryService]]).
  */
sealed trait SaveResult
case object SaveSucceeded extends SaveResult
final case class SaveFailed(message: String) extends SaveResult
