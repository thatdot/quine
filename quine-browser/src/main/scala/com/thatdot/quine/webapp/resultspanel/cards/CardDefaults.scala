package com.thatdot.quine.webapp.resultspanel.cards

/** Tunable constants for the card system, named so their intent is legible at the use
  * site (rather than bare literals scattered through the components). Mirrors design
  * doc §7's single-constants-object contract. Constants whose consumer lives outside
  * `cards/` sit with that consumer instead: the sampling defaults are on
  * [[com.thatdot.quine.webapp.resultspanel.ViewerState]], the live-tap throttle and
  * ring-buffer cap are private to
  * [[com.thatdot.quine.webapp.resultspanel.streaming.LiveStream]].
  */
object CardDefaults {

  /** Minimized cards fully visible in the drawer before the list scrolls. */
  val MaxVisibleMinimizedCards = 5

  /** How long the ephemeral result-count indicator (Lane D) stays up before fading, in ms. */
  val IndicatorFadeMs = 4000
}
