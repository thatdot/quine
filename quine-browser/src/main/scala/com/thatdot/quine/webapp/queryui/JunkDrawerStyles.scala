package com.thatdot.quine.webapp.queryui

/** Class-name constants for [[JunkDrawer]] and [[ResultCountIndicator]].
  *
  * Follows the `Styles.scala` convention (String constants matching classes defined in CSS) but
  * lives in its own file per the Lane D scaffolding constraint — `Styles.scala` itself is not
  * edited here. The integrator folds these into `Styles.scala` (see `laneD-integration.md`);
  * until then components reference this object directly.
  */
object JunkDrawerStyles {

  // ── Junk drawer trigger + popover ──────────────────────────────────────────
  val junkDrawer = "junk-drawer"
  val junkDrawerTrigger = "junk-drawer-trigger"
  val junkDrawerTriggerIcon = "junk-drawer-trigger-icon"
  val junkDrawerTriggerName = "junk-drawer-trigger-name"
  val junkDrawerMenu = "junk-drawer-menu"
  val junkDrawerMenuOpen = "open"

  // Sections (Graph / Configure / Maintenance)
  val junkDrawerSection = "junk-drawer-section"
  val junkDrawerSectionHead = "junk-drawer-section-head"
  val junkDrawerSectionLabel = "junk-drawer-section-label"
  val junkDrawerSectionBody = "junk-drawer-section-body"
  val junkDrawerSeparator = "junk-drawer-separator"

  // Graph section: wraps the host-supplied GraphSelector content
  val junkDrawerGraphSlot = "junk-drawer-graph-slot"

  // Configure / Maintenance rows (simple action entries)
  val junkDrawerItem = "junk-drawer-item"
  val junkDrawerItemIcon = "junk-drawer-item-icon"
  val junkDrawerItemLabel = "junk-drawer-item-label"
  val junkDrawerItemDanger = "junk-drawer-item-danger"

  // ── Result-count indicator (semi-translucent overlay, loading-icon family) ──
  // Shows the same node/edge counter visual Counters renders in the top bar (see
  // ResultCountIndicator's doc) — these two classes style that reused structure for the
  // translucent-pill-over-canvas context instead of the nav bar's.
  val resultCountIndicator = "result-count-indicator"
  val resultCountIndicatorVisible = "visible"
  val resultCountIndicatorCounters = "result-count-indicator-counters"
  val resultCountIndicatorIcon = "result-count-indicator-icon"
}
