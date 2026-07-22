package com.thatdot.quine.webapp

/** CSS class names for the Monaco-based query editor and its embedded (Streams form) variant,
  * kept apart from the shared [[Styles]] object so the editor feature owns its own class names.
  * The matching rules live in `shared-browser-resources/query-editor.css`.
  */
object QueryEditorStyles {
  val queryEditorHost = "query-editor-host"
  val queryEditorMulti = "query-editor-multi"
  val queryEditorChrome = "query-editor-chrome"
  val queryEditorContainer = "query-editor-container"
  val queryEditorRunButtonWrap = "query-editor-run-button-wrap"

  // Status bar + toggleable diagnostics panel, shown at the bottom of the expanded
  // (multi-line, focused) query editor.
  val queryStatusBar = "query-status-bar"
  val queryStatusChevron = "query-status-chevron"
  val queryStatusChevronOpen = "query-status-chevron open"
  val queryDiagnosticsPanel = "query-diagnostics-panel"
  val queryDiagnosticItem = "query-diagnostic-item"
  val queryDiagnosticMessage = "query-diagnostic-message"
  val queryDiagnosticPos = "query-diagnostic-pos"
  val queryDiagnosticError = "query-diagnostic-error"
  val queryDiagnosticWarning = "query-diagnostic-warning"
  val queryDiagnosticInfo = "query-diagnostic-info"

  // Embedded (form-field) query editor — the @thatdot/query-editor mounted inside
  // a Streams form Cypher field. Distinct from the nav-bar query-editor-* classes,
  // which position an overlay for the Query bar.
  val embeddedQueryEditor = "embedded-query-editor"
  // Inner Monaco mount inside the embedded editor's bordered box, so the status bar and
  // diagnostics panel can sit in flow below it (the box's height = Monaco + bar + panel).
  val embeddedQueryEditorContainer = "embedded-query-editor-container"
}
