package com.thatdot.quine.webapp.resultspanel

/** Layout constants for the results surface, named so their intent is legible at the
  * use site (rather than bare pixel literals scattered through the components).
  */
object ResultsLayout {

  /** Height the surface opens at, in px. */
  val initialHeightPx = 280.0

  /** Smallest the surface can be dragged to, in px. */
  val minSurfaceHeightPx = 140.0

  /** Vertical space kept clear above the surface for the query bar when the grab rail
    * sizes the surface to (nearly) fill the Explore area, in px.
    */
  val queryBarReservePx = 64.0

  /** A comfortable working height — a fraction of the Explore area, clamped to the draggable range.
    * Used when the source picker opens (so it isn't cramped) and for the grab-rail double-click snap.
    */
  val comfortableFraction = 0.58
  def comfortableHeightPx(containerHeightPx: Double): Double = {
    val ceil = (containerHeightPx - queryBarReservePx).max(minSurfaceHeightPx)
    (containerHeightPx * comfortableFraction).max(initialHeightPx).min(ceil)
  }

  /** Slack (px) for the double-click snap: heights within this of the comfortable target count as
    * "already there", so a second double-click restores the prior height instead of growing again.
    */
  val snapEpsilonPx = 6.0

  /** Smallest width a column can be resized to, in px. */
  val minColumnWidthPx = 60.0

  /** How close to the bottom (px) still counts as "at the bottom" for a live tap's
    * tail-follow auto-scroll.
    */
  val tailFollowSlackPx = 6.0
}
