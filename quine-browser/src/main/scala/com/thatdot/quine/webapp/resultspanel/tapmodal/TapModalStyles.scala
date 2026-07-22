package com.thatdot.quine.webapp.resultspanel.tapmodal

/** Class-name constants for the unified tap modal (Lane C). Mirrors the pattern in the app's
  * `Styles` object: names only, the actual rules live in `laneC-css.css` (folded into
  * `index.css` at integration time). Existing `Styles` constants (e.g. `sqBlock`, `tapButton`,
  * `managerList*`, `editorForm`) are reused as-is where the modal's pipeline tree is
  * visually identical to the surface it replaces (`AddTapChooser`) — only genuinely new
  * structure (the modal shell) gets new classes here.
  */
object TapModalStyles {

  // ── Modal shell ──────────────────────────────────────────────────────────────
  val overlay = "tap-modal-overlay"
  val dialog = "tap-modal-dialog"
  val header = "tap-modal-header"
  val title = "tap-modal-title"
  val closeButton = "tap-modal-close"
  val body = "tap-modal-body"

  // ── Inspect Standing Queries (Step 1): SQ picker rail + stage diagram ────────
  val inspectRoot = "tap-inspect-root"
  val inspectRail = "tap-inspect-rail"
  val inspectRailSearchWrap = "tap-inspect-rail-search-wrap"
  val inspectRailSearch = "tap-inspect-rail-search"
  val inspectRailList = "tap-inspect-rail-list"
  val inspectRailItem = "tap-inspect-rail-item"
  val inspectRailItemActive = "tap-inspect-rail-item-active"
  val inspectRailItemName = "tap-inspect-rail-item-name"
  val inspectRailItemDot = "tap-inspect-rail-item-dot"

  val diagram = "tap-diagram"
  val diagramHint = "tap-diagram-hint"
  val diagramFlow = "tap-diagram-flow"
  val diagramRawStack = "tap-diagram-raw-stack"
  val diagramConnector = "tap-diagram-connector"
  val diagramBead = "tap-diagram-bead"
  val diagramBeadFx = "tap-diagram-bead-fx"
  val diagramBranch = "tap-diagram-branch"
  val diagramBranchRow = "tap-diagram-branch-row"
  val diagramBranchStub = "tap-diagram-branch-stub"
  val diagramBranchName = "tap-diagram-branch-name"
  val diagramTap = "tap-diagram-tap"
  val diagramTapTapped = "tap-diagram-tap-tapped"
  val diagramTapGlyph = "tap-diagram-tap-glyph"
  val diagramTapText = "tap-diagram-tap-text"
  val diagramTapHead = "tap-diagram-tap-head"
  val diagramTapLabel = "tap-diagram-tap-label"
  val diagramTapViewing = "tap-diagram-tap-viewing"
  val diagramTapCaption = "tap-diagram-tap-caption"
  val diagramEnds = "tap-diagram-ends"
  val diagramNote = "tap-diagram-note"
  val diagramEmpty = "tap-diagram-empty"
}
