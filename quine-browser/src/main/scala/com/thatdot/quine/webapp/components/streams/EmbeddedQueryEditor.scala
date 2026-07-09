package com.thatdot.quine.webapp.components.streams

import scala.scalajs.js

import com.raquo.laminar.api.L._

import com.thatdot.quine.webapp.QueryEditorStyles
import com.thatdot.quine.webapp.queryui.{
  DiagnosticSeverity,
  EditorDiagnostic,
  Position,
  QpMode,
  QueryDiagnosticsView,
  QueryEditor,
  QueryEditorHandle,
  QueryEditorOptions,
}

/** A form-friendly wrapper around the `@thatdot/query-editor` Monaco editor, for Cypher fields in
  * the Streams creation forms (ingest query, standing-query output queries, transformations,
  * result enrichment).
  *
  * It mounts the editor in `embedded` mode, so the three Query-bar-only behaviors are dropped:
  * Enter inserts a newline (no run), losing focus keeps the field's height (no blur-collapse),
  * and ArrowUp/ArrowDown move the cursor (no query history). Semantic highlighting, LSP
  * diagnostics, completions, hover, auto-grow, and the resize handle remain.
  *
  * Below the editor it mounts the shared [[QueryDiagnosticsView]] (status bar + toggleable
  * diagnostics panel). Unlike the nav-bar Query input — where the bar shows only while the editor
  * is expanded — the embedded field is always full-height, so the bar is always present; clicking
  * a diagnostic row jumps the editor's cursor to that position.
  *
  * The editor connects to the Quine language server (semantic highlighting, diagnostics,
  * completions) only when `editorConfig.qpEnabled`; otherwise it stays on client-side Monarch
  * highlighting and never opens a connection.
  *
  * The editor's buffer is bridged to the form state: edits flow out via `onUpdate`, and external
  * changes to `currentValue` (e.g. the form re-emitting after another field changes) flow back in.
  * The `latestQuery` mirror plus the `getValue() != value` guard prevent the cursor-reset /
  * focus-loss loop that would otherwise occur when the form `Var[Json]` re-emits on every
  * keystroke — the same imperative-bridge pattern used by the Query bar's `MonacoQueryInput`.
  */
object EmbeddedQueryEditor {

  def apply(
    currentValue: Signal[String],
    onUpdate: String => Unit,
    placeholderText: String,
    editorConfig: EmbeddedEditorConfig,
  ): HtmlElement = {
    // Mutable mirror of the buffer's current value, needed inside the editor's JS callbacks and
    // kept in sync by the binder + onChange below. Seeded from the form state so the initial
    // mount has the right content.
    var editorHandle: Option[QueryEditorHandle] = None
    var latestQuery: String = ""

    // Full list of LSP diagnostics on the editor's model, fed by `onDiagnosticsListChange`, and
    // whether the diagnostics panel is expanded (toggled by clicking the status bar).
    val diagnostics: Var[Seq[QueryDiagnosticsView.QueryDiagnostic]] = Var(Seq.empty)
    val showDiagnostics: Var[Boolean] = Var(false)

    // Pushes external form-state changes into the editor. `latestQuery` is updated first so the
    // resulting `onChange` knows the value is already in app state and skips the redundant round
    // trip. The `getValue() != value` guard avoids a setValue (cursor reset) when the editor
    // already holds the value — which is the common case, since the form `Var[Json]` re-emits on
    // every keystroke in any field.
    val externalQueryObserver = Observer[String] { value =>
      latestQuery = value
      editorHandle.foreach(handle => if (handle.getValue() != value) handle.setValue(value))
    }

    // Inner Monaco mount, in flow inside the bordered box. The editor package sets THIS element's
    // height to Monaco's content height, so the status bar + panel sit below it as siblings (the
    // box grows to Monaco + bar + panel). Clicking a diagnostic row reveals its position.
    val container: Div = div(cls := QueryEditorStyles.embeddedQueryEditorContainer)

    val host: Div = div(
      cls := QueryEditorStyles.embeddedQueryEditor,
      container,
      QueryDiagnosticsView(
        diagnostics.signal,
        showDiagnostics,
        position => editorHandle.foreach(_.revealRange(position.line, position.column)),
      ),
    )

    host.amend(
      currentValue --> externalQueryObserver,
      onMountCallback { _ =>
        // QuinePattern language-server mode. StreamsPage is V2-only, so the route is always the V2
        // LSP path; lspWebSocketUrl honors `serverUrl`. When off, the field stays on Monarch-only
        // highlighting with no connection.
        val qpMode =
          if (editorConfig.qpEnabled)
            QpMode.Enabled(QueryEditor.lspWebSocketUrl(editorConfig.serverUrl, "/api/v2/lsp"))
          else QpMode.Disabled
        val handle = QueryEditor.create(
          container.ref,
          new QueryEditorOptions {
            embedded = true
            placeholder = placeholderText
            initialValue = latestQuery
            onChange = ((value: String) => if (value != latestQuery) onUpdate(value)): js.Function1[String, Unit]
            onDiagnosticsListChange = (
              (arr: js.Array[EditorDiagnostic]) =>
                diagnostics.set(
                  arr.toSeq.map(d =>
                    QueryDiagnosticsView.QueryDiagnostic(
                      d.message,
                      DiagnosticSeverity.fromString(d.severity),
                      Position(d.startLineNumber, d.startColumn),
                    ),
                  ),
                ),
            ): js.Function1[js.Array[EditorDiagnostic], Unit]
          },
          qpMode,
        )
        editorHandle = Some(handle)
        // The external-query binder may have fired before the editor existed.
        if (handle.getValue() != latestQuery) handle.setValue(latestQuery)
      },
      onUnmountCallback { _ =>
        editorHandle.foreach(_.dispose())
        editorHandle = None
      },
    )

    host
  }
}
