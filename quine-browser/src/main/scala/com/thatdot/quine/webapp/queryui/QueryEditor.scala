package com.thatdot.quine.webapp.queryui

import scala.scalajs.js
import scala.scalajs.js.annotation.JSImport

import org.scalajs.dom

/** A single LSP diagnostic on the editor's model, in the serializable plain-object shape emitted
  * by `@thatdot/query-editor`'s `onDiagnosticsListChange`. Positions are 1-based, matching Monaco's
  * marker coordinates. Mirrors the package's `EditorDiagnostic` interface.
  */
@js.native
trait EditorDiagnostic extends js.Object {
  def message: String = js.native
  def severity: String = js.native
  def startLineNumber: Int = js.native
  def startColumn: Int = js.native
  def endLineNumber: Int = js.native
  def endColumn: Int = js.native
}

/** Options bag for [[QueryEditor.create]]. Mirrors the `QueryEditorOptions` interface of
  * `@thatdot/query-editor`.
  */
trait QueryEditorOptions extends js.Object {

  /** Buffer language: `"cypher"` (default) or `"plaintext"`. Plain text turns off all
    * highlighting, bracket rules, and the language-server connection — for buffers in languages
    * the package has no grammar for (e.g. Gremlin), where Cypher tokenization would be wrong.
    */
  var language: js.UndefOr[String] = js.undefined

  /** Hint text shown while the editor is empty. */
  var placeholder: js.UndefOr[String] = js.undefined

  /** Initial buffer content. */
  var initialValue: js.UndefOr[String] = js.undefined

  /** Height (px) at which the editor stops growing and scrolls internally. */
  var maxHeightPx: js.UndefOr[Double] = js.undefined

  /** localStorage key for the ArrowUp/ArrowDown query history. */
  var historyStorageKey: js.UndefOr[String] = js.undefined

  /** Embedded (form-field) mode. When `true`, the editor drops the three Query-bar-only
    * behaviors — run-on-Enter / Cmd+Enter (Enter inserts a newline), blur-collapse (the field
    * keeps its height), and ArrowUp/ArrowDown history (the arrows move the cursor). Everything
    * else (auto-grow, resize handle, semantic highlighting, diagnostics, the language-server
    * connection) is unchanged. Defaults to `false`.
    */
  var embedded: js.UndefOr[Boolean] = js.undefined

  // Language-server mode (the `qpEnabled` flag and, when enabled, the WebSocket URL) is NOT a
  // consumer-settable field: it is supplied as the typed [[QpMode]] argument to
  // [[QueryEditor.create]], which sets the underlying JS fields. That keeps the flag and the URL
  // from disagreeing — a URL can't be set without enabling the language server, nor vice versa.

  /** Run the query with graph-shaped results (Enter when the suggest widget is closed,
    * Cmd/Ctrl+Enter, or [[QueryEditorHandle.runGraph]]). Optional: omit in `embedded` mode,
    * where no keybinding triggers a run.
    */
  var onRunGraph: js.UndefOr[js.Function1[String, Unit]] = js.undefined

  /** Run the query with table-shaped results. No keybinding triggers this; the consumer wires
    * it to its own affordance via [[QueryEditorHandle.runTable]]. Optional: omit in `embedded`
    * mode.
    */
  var onRunTable: js.UndefOr[js.Function1[String, Unit]] = js.undefined

  /** Called with the full buffer content after every edit. */
  var onChange: js.UndefOr[js.Function1[String, Unit]] = js.undefined

  /** Called whenever the error-severity diagnostic state of this editor's model changes, and
    * once immediately after mounting with the initial state. `hasErrors` is `true` when at
    * least one `MarkerSeverity.Error` marker is present; `false` otherwise (including before
    * the first diagnostic report arrives — fail-open).
    *
    * Consumers use this to disable submit controls while the buffer has errors. Because LSP
    * diagnostics arrive after a ~500 ms debounce, a user can submit before the first report
    * lands; that is intentional.
    */
  var onDiagnosticsChange: js.UndefOr[js.Function1[Boolean, Unit]] = js.undefined

  /** Called with the full list of diagnostics on this editor's model whenever they change, and
    * once immediately after mounting with an empty array. Additive to [[onDiagnosticsChange]]:
    * that boolean keeps its fail-open button-disable role; this list feeds a problems summary /
    * diagnostics panel. Both fire for markers on this editor's model only, computed from the same
    * marker fetch.
    */
  var onDiagnosticsListChange: js.UndefOr[js.Function1[js.Array[EditorDiagnostic], Unit]] =
    js.undefined

  /** Called whenever the server's classification of the current buffer changes, and once
    * immediately with `"unknown"` when the LSP connects. Values:
    *   - `"node"` — buffer is a node query; run on the graph canvas.
    *   - `"table"` — buffer is a table query; run as rows.
    *   - `"unknown"` — empty/whitespace buffer, parse/symbol errors, or LSP unreachable.
    *
    * Fires only on transitions. The consumer uses this to route the single Query button.
    */
  var onQueryKindChange: js.UndefOr[js.Function1[String, Unit]] = js.undefined

  /** Called when the language-server connection opens (`true`) or closes (`false`), and once
    * immediately with `false` before the first attempt. Only fires in [[QpMode.Enabled]]. Consumers
    * use it to fall back to local query-kind classification while disconnected, so Run stays usable
    * through an outage instead of locking on the last `"unknown"`.
    */
  var onConnectionChange: js.UndefOr[js.Function1[Boolean, Unit]] = js.undefined

  /** Called when the editor transitions between single-line and multi-line mode. Fires once at
    * mount with the initial mode, then on every transition. `isMultiline` is `true` when the
    * buffer contains at least one hard newline. Consumers use this to apply wider-width or
    * resize CSS classes to the editor host.
    */
  var onModeChange: js.UndefOr[js.Function1[Boolean, Unit]] = js.undefined
}

/** Handle over a mounted query editor, as returned by `createQueryEditor`. */
@js.native
trait QueryEditorHandle extends js.Object {
  def getValue(): String = js.native
  def setValue(value: String): Unit = js.native
  def focus(): Unit = js.native
  def blur(): Unit = js.native

  /** Record the current query in history and fire `onRunGraph`. */
  def runGraph(): Unit = js.native

  /** Record the current query in history and fire `onRunTable`. */
  def runTable(): Unit = js.native

  /** Enter multi-line editing from the outside (e.g. a menu action): focus the editor and, when
    * the buffer is still single-line, insert a newline at the end of the buffer — the same edit
    * Shift+Enter makes, so every follow-on behavior (auto-grow, blur-collapse, refocus-expand)
    * is the keyboard path's. An already multi-line buffer is only focused.
    */
  def startMultilineEdit(): Unit = js.native

  /** Move the cursor to `line`/`column` (both 1-based, matching [[EditorDiagnostic]]), scroll it
    * into view, and focus the editor. Used to jump from a diagnostics-list entry to its source
    * position.
    */
  def revealRange(line: Int, column: Int): Unit = js.native

  /** Tear down the editor, its listeners, and observers. */
  def dispose(): Unit = js.native
}

/** Language-server mode for [[QueryEditor.create]], mirroring the TypeScript package's
  * `QueryEditorLanguageMode` discriminated union. Modeling the WebSocket URL as data carried only
  * by the enabled case makes the two illegal states unrepresentable: a URL without QuinePattern,
  * or QuinePattern without a URL. The consumer builds it straight from its `qp.enabled` boolean.
  */
sealed trait QpMode
object QpMode {

  /** QuinePattern off: the editor uses the client-side Monarch tokenizer and never connects. */
  case object Disabled extends QpMode

  /** QuinePattern on: the editor connects to the Quine language server at `lspUrl` and its
    * semantic tokens become the sole syntax highlighter.
    */
  final case class Enabled(lspUrl: String) extends QpMode
}

@js.native
@JSImport("@thatdot/query-editor", JSImport.Namespace)
private object QueryEditorModule extends js.Object {
  def createQueryEditor(container: dom.Element, opts: QueryEditorOptions): QueryEditorHandle = js.native
}

/** Scala-side entry point for the `@thatdot/query-editor` Monaco-based query editor. */
object QueryEditor {

  /** Monaco runs its text-model services in a web worker; the consumer (not the package) owns
    * worker wiring because it is bundler-specific. `editor.worker.js` is emitted as its own
    * un-hashed webpack entry (see `common.webpack.config.js`) and served from the app root by
    * the webjar fallback route alongside the main bundle.
    */
  private def ensureMonacoEnvironment(): Unit = {
    // Property access on `window` (not `js.Dynamic.global`, whose member selection compiles to
    // a bare global reference and throws ReferenceError when the global is undeclared).
    val globalScope = dom.window.asInstanceOf[js.Dynamic]
    if (js.isUndefined(globalScope.MonacoEnvironment)) {
      globalScope.MonacoEnvironment = js.Dynamic.literal(
        getWorker = (
          (
            _: String,
            _: String,
          ) =>
            // Resolve the worker against the document base, not the site root, so a path-prefixed
            // reverse-proxy deployment loads `editor.worker.js` from its sub-path instead of a
            // root-absolute `/editor.worker.js` that 404s behind the proxy.
            new dom.Worker(new dom.URL("editor.worker.js", dom.document.baseURI).href),
        ): js.Function2[
          String,
          String,
          dom.Worker,
        ],
      )
    }
  }

  /** Build the Quine language-server WebSocket URL, honoring an optional REST `serverUrl` the same
    * way every other network call does (via `ClientRoutes`): a non-empty `serverUrl` supplies the
    * scheme + host + base path (`http`→`ws`, `https`→`wss`), an empty/absent one falls back to the
    * page origin. `lspPath` is the route to append (`/api/v1/lsp` vs `/api/v2/lsp`). Mirrors
    * `ClientRoutes.baseWsUrl` so the editor dials the same backend as the REST calls.
    */
  def lspWebSocketUrl(serverUrl: Option[String], lspPath: String): String = {
    val httpBase = serverUrl.filter(_.nonEmpty).getOrElse(dom.window.location.origin)
    val wsBase = httpBase.replaceFirst("^http", "ws")
    s"$wsBase$lspPath"
  }

  /** Mount a query editor into `container` and return its control handle. `qp` selects the
    * language-server mode; see [[QpMode]]. When enabled, the editor connects to the language
    * server itself (semantic tokens, diagnostics, completions) — there is no separate connect step.
    */
  def create(container: dom.Element, opts: QueryEditorOptions, qp: QpMode): QueryEditorHandle = {
    ensureMonacoEnvironment()
    // `qpEnabled` / `languageServerUrl` are kept off the consumer-facing trait (see
    // QueryEditorOptions) so the only way to configure the language server is this typed mode.
    // Set them here on the underlying JS object, matching the TS QueryEditorLanguageMode union.
    val dyn = opts.asInstanceOf[js.Dynamic]
    qp match {
      case QpMode.Disabled =>
        dyn.qpEnabled = false
      case QpMode.Enabled(lspUrl) =>
        dyn.qpEnabled = true
        dyn.languageServerUrl = lspUrl
    }
    QueryEditorModule.createQueryEditor(container, opts)
  }
}
