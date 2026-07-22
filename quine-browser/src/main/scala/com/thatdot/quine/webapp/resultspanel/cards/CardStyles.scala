package com.thatdot.quine.webapp.resultspanel.cards

/** Class-name constants for the card system (drawer, popup, card bodies). Mirrors
  * `Styles.scala`'s pattern — real CSS lives alongside (see the lane's CSS file); this
  * object is just the typed names components reference. Reuses the existing
  * `--kind-accent` kind-class system (`Styles.kindQuery` / `Styles.kindTap` /
  * `Styles.kindError`) for card color coding rather than duplicating it here.
  */
object CardStyles {

  // ── Minimized drawer (right-edge floating stack over the canvas) ──────────────────
  val drawer = "cards-drawer"
  val drawerPanel = "cards-drawer-panel"
  val drawerList = "cards-drawer-list"

  // One minimized card row
  val miniCard = "mini-card"
  val miniCardStrip = "mini-card-strip" // kind-accent color strip on the left edge
  val miniCardBody = "mini-card-body"
  val miniCardTitle = "mini-card-title"
  val miniCardStatus = "mini-card-status"
  val miniCardStatusDot = "mini-card-status-dot" // pulsing green live dot
  val miniCardStatusLive = "is-live"
  val miniCardClose = "mini-card-close"
  val miniCardKindFx = "mini-card-kind-fx" // the ƒ glyph for a transformation tap point

  // Search + collapse
  val drawerSearchWrap = "cards-drawer-search-wrap"
  val drawerSearchInput = "cards-drawer-search-input"
  val drawerCollapse = "cards-drawer-collapse"
  val drawerCollapsedButton = "cards-drawer-collapsed-button"
  val drawerCollapsedCount = "cards-drawer-collapsed-count"

  // ── Expanded popup (floating over the canvas, not a docked drawer) ────────────────
  val popup = "card-popup"
  val popupGrabRail = "card-popup-grab-rail"

  val popupHeader = "card-popup-header"
  val popupHeaderTitle = "card-popup-header-title" // middle-truncated query text
  val popupHeaderTitleHead = "card-popup-header-title-head"
  val popupHeaderTitleTail = "card-popup-header-title-tail"
  val popupKindPill = "card-popup-kind-pill" // "table" (adhoc only) — identity only, no glyphs
  val popupMeta = "card-popup-meta" // status segment: "10 rows" / "● 3 of 10" / "● 10 rows"
  val popupStatusDot = "card-popup-status-dot" // ● — pulsing green streaming, amber paused (with is-paused)
  val popupActions = "card-popup-actions"
  val popupEditButton = "card-popup-edit-button"
  val popupRunButton = "card-popup-run-button" // Re-run (adhoc)
  val popupLiveButton = "card-popup-live-button" // ▶ Go live / ■ Stop — the stream lifecycle toggle
  val popupToggleHidden = "is-hidden" // inactive toggle label — kept mounted to hold the width
  val popupMoreButton = "card-popup-more-button" // "Get N more" — sampled exit
  val popupBatchInput = "card-popup-batch-input" // batch size for the next fetch
  val popupPausedAccent = "is-paused" // paused amber on the status dot (streams page's badge color)
  val popupExportWrap = "card-popup-export-wrap"
  val popupMinimizeButton = "card-popup-minimize-button"
  val popupCloseButton = "card-popup-close-button"

  // Collapsed-by-default strip showing the query behind a tap card's data (its standing
  // query's match pattern, or its output's enrichment query).
  val popupQueryStrip = "card-popup-query-strip"
  val popupQueryToggle = "card-popup-query-toggle"
  val popupQueryPre = "card-popup-query-pre"
  val popupQueryNote = "card-popup-query-note" // Transformed cards: how the transformation reshaped the rows

  val popupBody = "card-popup-body"
}
