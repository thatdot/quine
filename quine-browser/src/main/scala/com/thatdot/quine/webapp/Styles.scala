package com.thatdot.quine.webapp

/** Classes defined in `IndexCSS`.
  *
  * TODO: use `scalacss` to write this CSS inline
  */
object Styles {
  val grayClickable = "gray-clickable"
  val clickable = "clickable"
  val disabled = "disabled"
  val rightIcon = "right-icon"
  val navBarButton = "nav-bar-button"
  val navBar = "nav-bar"
  // The canvas region below the TopBar: VisNetwork plus every overlay anchored to it (Loader,
  // node-properties popup, result cards, the ephemeral count indicator, the context menu).
  // Its own `position: relative` is the containing block those overlays position against, kept
  // separate from the outer QueryUi container (which also spans the TopBar).
  val canvasRegion = "canvas-region"
  // Trailing slot at the far right of the nav-bar (graph selector)
  val navBarTrailing = "nav-bar-trailing"
  val navBarDivider = "nav-bar-divider"
  // Graph (namespace) selector in the query bar
  val graphSelector = "graph-selector"
  val graphSelectorButton = "graph-selector-button"
  val graphSelectorIcon = "graph-selector-icon"
  val graphSelectorName = "graph-selector-name"
  val graphSelectorChevron = "graph-selector-chevron"
  val graphSelectorMenu = "graph-selector-menu"
  val graphSelectorMenuHeader = "graph-selector-menu-header"
  val graphSelectorMenuFilter = "graph-selector-menu-filter"
  val graphSelectorMenuList = "graph-selector-menu-list"
  val graphSelectorMenuItem = "graph-selector-menu-item"
  val graphSelectorMenuName = "graph-selector-menu-name"
  val graphSelectorMenuDelete = "graph-selector-menu-delete"
  val graphSelectorMenuSeparator = "graph-selector-menu-separator"
  val graphSelectorMenuCreate = "graph-selector-menu-create"
  val graphSelectorMenuCreatePlus = "graph-selector-menu-create-plus"
  val graphSelectorMenuEmpty = "graph-selector-menu-empty"
  // Flat namespace list embedded in the junk drawer's Graph section
  val graphSelectorInline = "graph-selector-inline"
  val graphSelectorInlineFilter = "graph-selector-inline-filter"
  val graphSelectorInlineList = "graph-selector-inline-list"
  val graphSelectorInlineItem = "graph-selector-inline-item"
  val graphSelectorInlineName = "graph-selector-inline-name"
  val graphSelectorInlineCheck = "graph-selector-inline-check"
  val graphSelectorInlineEmpty = "graph-selector-inline-empty"
  // Query input bar
  val queryInput = "query-input"
  val queryInputInput = "query-input-input"
  val queryInputButton = "query-input-button"
  val bookmarkToggle = "query-bookmark-toggle"
  val bookmarkToggleActive = "active"
  val bookmarkDialog = "bookmark-dialog"
  val bookmarkDialogTitle = "bookmark-dialog-title"
  val bookmarkGlobalWarning = "bookmark-global-warning"
  val bookmarkFieldLabel = "bookmark-field-label"
  val bookmarkFieldLabelOptional = "bookmark-field-label-optional"
  val bookmarkFieldInput = "bookmark-field-input"
  val bookmarkQueryPreview = "bookmark-query-preview"
  val bookmarkDialogFooter = "bookmark-dialog-footer"
  val bookmarkRemoveButton = "bookmark-remove-button"
  val bookmarkCancelButton = "bookmark-cancel-button"
  val bookmarkSaveButton = "bookmark-save-button"
  val sampleQueryDropdown = "sample-query-dropdown"
  val sampleQueryItem = "sample-query-item"
  val sampleQueryItemHighlighted = "highlighted"
  val sampleQueryName = "sample-query-name"
  val sampleQueryText = "sample-query-text"

  // Query result sentiment
  val queryResultError = "query-result-error"
  val queryResultSuccess = "query-result-success"
  val queryResultEmpty = "query-result-empty"

  // Source kind identity (sets --kind-accent for the chip / picker entry / body edge subtree)
  val kindQuery = "kind-query"
  val kindTap = "kind-tap"
  val kindError = "kind-error"
  val sourceKindIcon = "source-kind-icon" // the leading kind glyph in a chip
  val resultsViewing = "is-viewing" // the currently-displayed source, marked in the picker

  // Results surface (Explore query results)
  val resultsSurface = "results-surface"
  val resultsCollapsed = "collapsed"
  val resultsSurfaceAnimating = "is-animating" // ease a snap/grow height change (not a drag)
  val resultsGrabRail = "results-grab-rail"
  // Canvas door: the collapsed / off-screen entry point on the canvas layer.
  val resultsCanvasDoor = "results-canvas-door"
  val resultsCanvasDoorFirstRun = "is-first-run" // pill-with-label variant when there's no history
  val resultsDoorGlyph = "results-door-glyph"
  val resultsDoorLabel = "results-door-label"
  val resultsHeader = "results-header"
  val resultsStatusDot = "results-status-dot"
  val resultsStatusOk = "is-ok"
  val resultsStatusEmpty = "is-empty"
  val resultsStatusError = "is-error"
  val resultsRowCount = "results-rowcount"
  val resultsQueryChip = "results-query-chip" // the query echo shown in the empty/error placeholder
  val resultsHeaderActions = "results-header-actions"
  // Top-bar browser nav: back/forward arrows · the identity bar · count
  val resultsNav = "results-nav" // the adjacent back/forward pair
  val resultsNavArrow = "results-nav-arrow"
  val resultsChip = "results-chip"
  val resultsChipFill = "results-chip-fill" // the bar's flexible middle (the name)
  val resultsChipName = "results-chip-name"
  val resultsChipNameHead = "results-chip-name-head" // middle-truncation: ellipsizing head…
  val resultsChipNameTail = "results-chip-name-tail" // …always-shown tail (where refinements differ)
  val resultsChipSuffix = "results-chip-suffix" // muted ` · point` tail on a tap chip name
  val resultsChipAction = "results-chip-action"
  val resultsChipRule = "results-chip-rule" // hairline before the switcher caret
  val resultsChipCaret = "results-chip-caret" // opens the switcher; carries the live-dot
  val resultsChipLiveDot = "results-chip-live-dot" // teal dot on the caret when taps are streaming
  val resultsLiveChip = "results-live-chip"
  val resultsLiveDot = "results-live-dot"
  // Switcher-open bar treatment: the bar becomes the switcher's search header.
  val resultsSwitcherSearch = "results-switcher-search"
  val resultsPickerCancel = "results-picker-cancel"
  val resultsViewToggle = "results-view-toggle"
  val resultsViewSeg = "seg"
  val resultsViewSegActive = "active"
  val resultsContentArea = "results-content-area"
  val resultsContentStack = "results-content-stack"
  val resultsBody = "results-body"
  val resultsGrid = "results-grid"
  val cellTruncate = "cell-truncate"
  val cellNumber = "cell-number"
  val cellNull = "cell-null"
  val cellChip = "cell-chip"
  val cellNode = "cell-node"
  val cellLabelChip = "cell-label-chip"
  val cellNodeProps = "cell-node-props"
  val cellRelChip = "cell-rel-chip"
  val resultsJson = "results-json"
  val resultsDrawer = "results-drawer"
  val resultsDrawerOpen = "open"
  val resultsDrawerHeader = "results-drawer-header"
  val resultsDrawerBody = "results-drawer-body"
  val drawerField = "drawer-field"
  val drawerFieldLabel = "drawer-field-label"
  val drawerFieldValue = "drawer-field-value"
  val resultsEmpty = "results-empty"
  val resultsEmptyIcon = "results-empty-icon"
  val resultsEmptyTitle = "results-empty-title"
  val resultsEmptyDesc = "results-empty-desc"
  // Results surface — sort / filter / export
  val resultsGridSortable = "results-grid-sortable"
  val resultsGridFixed = "is-fixed"
  val colResize = "results-col-resize"
  val sortGlyph = "sort-glyph"
  val sortGlyphActive = "active"
  val resultsFilterAffordance = "results-filter-affordance"
  val resultsFilterButton = "results-filter-button"
  val resultsFilterChip = "results-filter-chip"
  val resultsFilterInput = "results-filter-input"
  val resultsFilterIcon = "results-filter-icon"
  val resultsFilterField = "results-filter-field"
  val resultsExportWrap = "results-export-wrap"
  val resultsExportButton = "results-export-button"
  val resultsExportMenu = "results-export-menu"
  val resultsExportItem = "results-export-item"
  val resultsExportFlatten = "results-export-flatten"
  // Results surface — history
  val resultsKeepActive = "is-kept"
  // Source picker (drops into the content area below the header)
  val sourcesPanel = "sources-panel"
  val sourcesColEmpty = "sources-col-empty"
  // Switcher list (add-a-tap · taps · query results · GC footer)
  val sourcesSwitcher = "sources-switcher"
  val switcherAddTap = "switcher-add-tap"
  val switcherAddTapIcon = "switcher-add-tap-icon"
  val switcherAddTapLabel = "switcher-add-tap-label"
  val switcherAddTapChevron = "switcher-add-tap-chevron"
  val switcherSection = "switcher-section"
  val switcherSectionHead = "switcher-section-head"
  val switcherSectionLabel = "switcher-section-label"
  val switcherFilterSeg = "switcher-filter-seg"
  val switcherFilterSegBtn = "switcher-filter-seg-btn"
  val switcherTapRow = "switcher-tap-row"
  val switcherRowName = "switcher-row-name"
  val switcherRowMeta = "switcher-row-meta"
  val switcherRowActions = "switcher-row-actions"
  val switcherRowAction = "switcher-row-action"
  val switcherGcFooter = "switcher-gc-footer"
  val switcherGcIcon = "switcher-gc-icon"
  // Tap picker ("Add a tap" chooser)
  val tapsHeadingSpacer = "taps-heading-spacer"
  val tapChooser = "tap-chooser"
  val tapChooserFilterWrap = "tap-chooser-filter-wrap"
  val tapChooserFilter = "tap-chooser-filter"
  val tapChooserCount = "tap-chooser-count"
  // Chooser: back header + legend
  val chooserBackHeader = "chooser-back-header"
  val chooserBack = "chooser-back"
  val chooserTitleWrap = "chooser-title-wrap"
  val chooserTitle = "chooser-title"
  val chooserSubtitle = "chooser-subtitle"
  val chooserLegend = "chooser-legend"
  val chooserLegendItem = "chooser-legend-item"
  val chooserSwatch = "chooser-swatch"
  val chooserSwatchTapped = "is-tapped"
  // Chooser: one standing-query block (header + per-output pipeline rows)
  val sqBlock = "sq-block"
  val sqBlockHead = "sq-block-head"
  val sqBlockName = "sq-block-name"
  val sqBlockCount = "sq-block-count"
  val sqAllMatches = "sq-all-matches"
  val tapOutputRow = "tap-output-row"
  val tapOutputName = "tap-output-name"
  val prePostGroup = "pre-post-group"
  val prePostConnector = "pre-post-connector"
  val enrichNode = "enrich-node"
  val noEnrichment = "no-enrichment"
  val tapButton = "tap-button"
  val viewingPill = "viewing-pill"
  val viewingPillDot = "viewing-pill-dot" // teal dot on the switcher's "Viewing" pill
  val chooserSkeleton = "chooser-skeleton"
  val chooserSkeletonRow = "chooser-skeleton-row"
  val chooserMessage = "chooser-message"
  val chooserMessageTitle = "chooser-message-title"
  val chooserMessageDesc = "chooser-message-desc"
  val chooserError = "chooser-error"
  val chooserErrorHead = "chooser-error-head"
  val historyRow = "history-row"
  val historyRowQuery = "history-row-query"
  val historyRowQueryHead = "history-row-query-head" // ellipsizing leading part of the query
  val historyRowQueryTail = "history-row-query-tail" // always-shown trailing part (where refinements differ)
  val historyRowMeta = "history-row-meta"
  val historyRowPin = "history-row-pin"
  val colResizing = "results-col-resizing" // body class while dragging a column-resize handle
  val sourceDotLive = "is-live"
  // Live table treatments
  val streamRow = "results-stream-row"
  val streamRowRetraction = "is-retraction"
  val retractedBadge = "retracted-badge"

  // Context menu
  val contextMenu = "context-menu"
  val contextMenuHeader = "context-menu-header"
  val contextMenuHeaderDot = "context-menu-header-dot"
  val contextMenuSection = "context-menu-section"
  val contextMenuDivider = "context-menu-divider"
  val contextMenuAction = "context-menu-action"
  val contextMenuActionIcon = "context-menu-action-icon"
  val contextMenuActionBody = "context-menu-action-body"
  val contextMenuActionName = "context-menu-action-name"
  val contextMenuActionSuffix = "context-menu-action-suffix"
  val contextMenuSectionBody = "context-menu-section-body"
  val contextMenuSectionSearch = "context-menu-section-search"

  // Node properties popup
  val nodePropertiesPopup = "node-properties-popup"
  val nodePropertiesPopupHeader = "node-properties-popup-header"
  val nodePropertiesPopupContent = "node-properties-popup-content"
  val nodePropertiesHeaderMain = "node-properties-header-main"
  val nodePropertiesHeaderActions = "node-properties-header-actions"
  val nodePropertiesEyebrowRow = "node-properties-eyebrow-row"
  val nodePropertiesNodeDot = "node-properties-node-dot"
  val nodePropertiesNodeIcon = "node-properties-node-icon"
  val nodePropertiesEyebrow = "node-properties-eyebrow"
  val nodePropertiesLabels = "node-properties-labels"
  val nodePropertiesLabel = "node-properties-label"
  val nodePropertiesLabelRemoveBtn = "node-properties-label-remove-btn"
  val nodePropertiesNewLabelInput = "node-properties-new-label-input"
  // body / fields
  val nodePropertiesBody = "node-properties-body"
  val nodePropertiesField = "node-properties-field"
  // id
  val nodePropertiesId = "node-properties-id"
  // view values
  val nodePropertiesList = "node-properties-list"
  val nodePropertiesRow = "node-properties-row"
  val nodePropertiesKey = "node-properties-key"
  val nodePropertiesValCell = "node-properties-val-cell"
  val nodePropertiesVal = "node-properties-val"
  val nodePropertiesMultiline = "node-properties-multiline"
  val nodePropertiesMultilineFoot = "node-properties-multiline-foot"
  val nodePropertiesShowAllBtn = "node-properties-show-all-btn"
  val nodePropertiesCharcount = "node-properties-charcount"
  val nodePropertiesEmpty = "node-properties-empty"
  // edit rows
  val nodePropertiesEditList = "node-properties-edit-list"
  val nodePropertiesProw = "node-properties-prow"
  val nodePropertiesProwKey = "node-properties-prow-key"
  val nodePropertiesProwVal = "node-properties-prow-val"
  val nodePropertiesStatusTag = "node-properties-status-tag"
  val nodePropertiesTypeSelect = "node-properties-type-select"
  val nodePropertiesBoolSelect = "node-properties-bool-select"
  val nodePropertiesTextInput = "node-properties-text-input"
  val nodePropertiesEditInput = "node-properties-edit-input"
  val nodePropertiesIconBtn = "node-properties-icon-btn"
  val nodePropertiesAddBtn = "node-properties-add-btn"
  // footer
  val nodePropertiesFooter = "node-properties-footer"
  val nodePropertiesFooterRow = "node-properties-footer-row"
  val nodePropertiesDiffSummary = "node-properties-diff-summary"
  val nodePropertiesDiffItem = "node-properties-diff-item"
  val nodePropertiesDiffDot = "node-properties-diff-dot"
  val nodePropertiesConfirm = "node-properties-confirm"
  val nodePropertiesConfirmText = "node-properties-confirm-text"
  val nodePropertiesConfirmActions = "node-properties-confirm-actions"
  val nodePropertiesBtn = "node-properties-btn"
  // constructed-query disclosure (edit mode)
  val nodePropertiesCypher = "node-properties-cypher"
  val nodePropertiesCypherBox = "node-properties-cypher-box"
  // shared copy affordance
  val nodePropertiesCopyGlyph = "node-properties-copy-glyph"
  val nodePropertiesCopiedCheck = "node-properties-copied-check"

  // Loader related
  val loader = "loader"
  val loaderSpinner = "loader-spinner"
  val loaderCounter = "loader-counter"
  val loaderCancellable = "loader-cancellable"

  // Overlay
  val overlay = "overlay"
  val openOverlay = "open-overlay"
  val closedOverlay = "closed-overlay"

  // Sidebar
  val sideBar = "side-bar"
  val sideBarItem = "side-bar-item"
  val selectedSideBarItem = "side-bar-item selected"

  // Slide-over panel
  val slideOverPanel = "slide-over-panel"
  val slideOverPanelOpen = "slide-over-panel open"
  val slideOverPanelAnimated = "slide-over-panel-animated"
  val slideOverPanelHeader = "slide-over-panel-header"
  val slideOverPanelClose = "slide-over-panel-close"
  val slideOverPanelBody = "slide-over-panel-body"
  val slideOverPanelFooter = "slide-over-panel-footer"
  val slideOverPanelResizeHandle = "slide-over-panel-resize-handle"

  // Toast
  // "quine-toast", not "toast": the enterprise shell loads CoreUI (Bootstrap), whose
  // `.toast:not(.show) { display: none }` out-specifies our rule and hides the toast.
  val toast = "quine-toast"
  val toastIcon = "quine-toast-icon"
  val toastClose = "quine-toast-close"
  val toastAction = "quine-toast-action"

  // Predicate builder
  val predicateBuilder = "predicate-builder"
  val predicateField = "predicate-field"
  val predicateInput = "predicate-input"
  val predicateInputSmall = "predicate-input-small"
  val predicateChips = "predicate-chips"
  val predicateChip = "predicate-chip"
  val predicateChipRemove = "predicate-chip-remove"
  val predicateAddRow = "predicate-add-row"
  val predicateKvRow = "predicate-kv-row"
  val predicateKvKey = "predicate-kv-key"
  val predicateKvValue = "predicate-kv-value"
  val predicateMatchCount = "predicate-match-count"
  val predicateMatchDot = "predicate-match-dot"

  // API JSON preview
  val apiJsonPreview = "api-json-preview"
  val apiJsonPreviewToggle = "api-json-preview-toggle"
  val apiJsonPreviewCode = "api-json-preview-code"

  // Editor form
  val editorForm = "editor-form"
  val editorField = "editor-field"
  val editorFieldLabel = "editor-field-label"
  val editorInput = "editor-input"
  val editorTextarea = "editor-textarea"
  val editorToggle = "editor-toggle"
  val editorToggleActive = "editor-toggle active"
  val editorActions = "editor-actions"
  val editorAnchorChip = "editor-anchor-chip"

  // Manager list
  val managerList = "manager-list"
  val managerListItem = "manager-list-item"
  val managerListItemMain = "manager-list-item-main"
  val managerListItemName = "manager-list-item-name"
  val managerListItemDetail = "manager-list-item-detail"
  val managerListItemActions = "manager-list-item-actions"
  val managerListEmpty = "manager-list-empty"
  val managerSearch = "manager-search"
  val managerHeader = "manager-header"
  val managerFooter = "manager-footer"

  // Color swatch
  val colorSwatch = "color-swatch"
}
