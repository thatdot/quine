package com.thatdot.quine.webapp.queryui

import scala.scalajs.js

import com.raquo.laminar.api.L._
import com.raquo.laminar.keys.EventProp
import org.scalajs.dom

import com.thatdot.quine.routes.SampleQuery
import com.thatdot.quine.webapp.{QueryEditorStyles, Styles}

/** The query editor (Monaco-based, from `@thatdot/query-editor`), the single Query run button,
  * and the sample queries dropdown.
  *
  * Keybindings live in the editor package: Enter runs (graph), Shift+Enter inserts a newline,
  * Cmd/Ctrl+Enter runs (graph), Ctrl+Shift+Enter runs as a table, ArrowUp/ArrowDown recall query
  * history while single-line. Losing focus collapses the editor to single-line; refocus re-expands.
  *
  * The single Query button routes graph vs table by the server's `quine/queryKind` verdict in rich
  * mode, or by the buffer in basic mode (non-empty → graph, Shift → table) when `qpEnabled` is false.
  */
object MonacoQueryInput {

  /** `focusin`/`focusout` bubble (unlike `focus`/`blur`), so the query bar can track focus
    * moving in and out of the editor's internal DOM.
    */
  private val onFocusIn = new EventProp[dom.FocusEvent]("focusin")
  private val onFocusOut = new EventProp[dom.FocusEvent]("focusout")

  def apply(
    query: Signal[String],
    updateQuery: String => Unit,
    runningTextQuery: Signal[Boolean],
    queryBarColor: Signal[Option[String]],
    sampleQueries: Signal[Seq[SampleQuery]],
    submitButton: UiQueryType => Unit,
    cancelButton: () => Unit,
    canRead: Boolean,
    useV2Api: Boolean,
    qpEnabled: Boolean,
    serverUrl: Option[String],
    bookmark: BookmarkUi,
  ): HtmlElement = {
    // Mutable mirrors of reactive state needed inside the editor's JS callbacks (Signals expose
    // no current-value accessor). They are kept in sync by binders on the root element below.
    var editorHandle: Option[QueryEditorHandle] = None
    var latestQuery: String = ""
    var isRunning: Boolean = false
    // Tracks the last query text seen by the dropdown-reopen observer. The observer only reopens
    // the dropdown when the buffer text actually changes (different from the last seen value),
    // which prevents programmatic round-trips — e.g. Enter-to-run fires updateQuery(sameText)
    // and submitQuery fires an unrelated stateVar update — from reopening the dropdown.
    var lastSeenQueryForDropdown: String = ""

    // Reactive error-diagnostic state: true when the editor's model has at least one
    // error-severity LSP marker. Starts false (fail-open — no diagnostics = enabled). Updated
    // by the editor package's `onDiagnosticsChange` callback.
    val hasErrorDiagnostics: Var[Boolean] = Var(false)

    // Full list of LSP diagnostics on the editor's model, fed by the editor package's
    // `onDiagnosticsListChange` callback. Drives the status bar's problems summary and the
    // toggleable diagnostics panel (both shown only in the expanded multi-line editor).
    val diagnostics: Var[Seq[QueryDiagnosticsView.QueryDiagnostic]] = Var(Seq.empty)

    // Whether the diagnostics panel is expanded. Toggled by clicking the status bar; starts
    // collapsed so the bar stays quiet until the user asks to see the list.
    val showDiagnostics: Var[Boolean] = Var(false)

    // True when the editor buffer contains a hard newline (multi-line mode). Updated by the
    // editor package's `onModeChange` callback. Drives the `query-editor-multi` CSS class on
    // the editor host, which widens the container and shows the resize handle.
    val isMultilineMode: Var[Boolean] = Var(false)

    // Server verdict for the current buffer, parsed to a [[QueryKind]] at the LSP-callback edge.
    // `Unknown` means the LSP has not yet classified the buffer (no LSP, empty buffer, or parse
    // errors); a concrete kind enables the button and selects its run path.
    val queryKind: Var[QueryKind] = Var(QueryKind.Unknown)

    // Whether the language-server connection is currently open (rich mode only; false in basic mode).
    // Fed by the editor's `onConnectionChange`; drives the local query-kind fallback below.
    val lspConnected: Var[Boolean] = Var(false)

    // Local query-kind fallback when there is no server verdict: non-empty buffer → Node (routing
    // to graph is always safe), empty → Unknown (Run disabled).
    def localVerdict(q: String): QueryKind =
      if (q.trim.nonEmpty) QueryKind.Node else QueryKind.Unknown

    // True when keyboard focus is inside the editor host (independent of the dropdown's
    // open/closed state so that re-opening on query-change can be checked separately).
    val editorHasFocus: Var[Boolean] = Var(false)

    // The editor is "expanded" — shown as the downward-growing overlay with its chrome (status
    // bar, diagnostics panel, in-editor run button) — exactly when it is multi-line AND focused.
    // Derived once and reused so the gating can't drift between those affordances.
    val isExpanded: Signal[Boolean] =
      isMultilineMode.signal.combineWith(editorHasFocus.signal).map { case (multiline, focused) =>
        multiline && focused
      }

    // This being true is a prerequisite for showing the sample query dropdown.
    val showSampleQueryDropdown: Var[Boolean] = Var(false)

    // The sample queries filtered to match what the user typed, each paired with its index into
    // sampleQueries to serve as a stable key for `splitSeq`.
    val filteredSampleQueries: Var[Seq[(SampleQuery, Int)]] = Var(Seq.empty)

    // The index (into sampleQueries) of the currently highlighted (hovered) query in the sample
    // query dropdown.
    val highlightedIdx: Var[Option[Int]] = Var(None)

    // The Query buttons' run path: pick graph vs table, then delegate to the editor handle (which
    // routes back through submitFromEditor). forceTable = the Shift modifier was held.
    def runByVerdict(forceTable: Boolean): Unit = {
      val kind = queryKind.now()
      if (
        !isRunning && !RunAvailability.isBlocked(RunAvailability.evaluate(canRead, hasErrorDiagnostics.now(), kind))
      ) {
        editorHandle.foreach { h =>
          val runOnGraph = if (qpEnabled) QueryKind.runsOnGraph(kind) else !forceTable
          if (runOnGraph) h.runGraph() else h.runTable()
        }
      }
    }

    def runButtonTitle(kind: QueryKind): String =
      if (qpEnabled)
        kind match {
          case QueryKind.Node => "Run query, showing results on the graph canvas"
          case QueryKind.Table => "Run query, showing results as a table"
          case QueryKind.SideEffects => "Run query for its side effects (returns no rows)"
          case QueryKind.Unknown => "Waiting for query classification from the language server"
        }
      else
        kind match {
          case QueryKind.Unknown => "Enter a query to run"
          case _ => "Run query on the graph canvas (hold Shift to show results as a table)"
        }

    // Wired to the editor's `onRunGraph`/`onRunTable`; `asTable` reflects which fired. Rich mode
    // routes by the server verdict (so sideEffects queries, which fire `onRunTable`, stay distinct
    // from genuine table queries); basic mode has no verdict, so `asTable` picks text-vs-graph.
    def submitFromEditor(asTable: Boolean): js.Function1[String, Unit] = (queryText: String) => {
      val kind = queryKind.now()
      if (
        !isRunning && !RunAvailability.isBlocked(RunAvailability.evaluate(canRead, hasErrorDiagnostics.now(), kind))
      ) {
        updateQuery(queryText)
        val uiQueryType =
          if (qpEnabled) QueryKind.toUiQueryType(kind)
          else if (asTable) UiQueryType.Text
          else UiQueryType.Node
        submitButton(uiQueryType)
        // Unfocus the editor after any submit, which collapses a multi-line editor back to its
        // single-line appearance (via the package's blur handler and the focus-driven CSS class).
        editorHandle.foreach(_.blur())
      }
    }

    // Pushes external query updates (sample-query selection, context-menu quick queries,
    // failed-query recall) into the editor. `latestQuery` is updated first so the resulting
    // `onChange` knows the value is already in app state and skips the redundant round trip.
    val externalQueryObserver = Observer[String] { value =>
      latestQuery = value
      editorHandle.foreach(handle => if (handle.getValue() != value) handle.setValue(value))
    }

    // The element the editor is mounted into. The package drives its height (auto-grow with
    // cap); CSS in common.css positions it to overlay the content below the bar as it grows.
    val editorContainer: Div = div(
      cls := QueryEditorStyles.queryEditorContainer,
      cls <-- queryBarColor.map(_.getOrElse("")),
      styleAttr <-- runningTextQuery.map { running =>
        if (running) "animation: activequery 1.5s ease infinite" else ""
      },
      // Run affordance anchored to the top-right corner of the expanded container. Visible only
      // while the editor is expanded — multi-line AND focused — so a collapsed (blurred) editor
      // shows only the nav-bar Query button rather than two. The standard nav-bar Query/Cancel
      // buttons remain the sole controls while single-line. The button mirrors the nav-bar Query
      // button exactly: same runByVerdict() path, same disabled conditions (no verdict / error
      // diagnostics / !canRead / isRunning), same tooltip, and the .query-input-button:disabled
      // grey-out styling. Hidden during a running query so a stale enabled affordance is never
      // shown while Cancel is the right action; the nav-bar Cancel button remains reachable (it is
      // not covered by the expanded container).
      child <-- isExpanded
        .combineWith(runningTextQuery)
        .map {
          case (true, false) =>
            button(
              cls := s"${Styles.grayClickable} ${Styles.queryInputButton} ${QueryEditorStyles.queryEditorRunButton}",
              // Suppress the default mousedown focus shift so clicking the button doesn't blur the
              // editor first — a blur would flip editorHasFocus to false and unmount this button
              // before the click lands. (Same guard the sample-query dropdown items use.)
              onMouseDown --> (_.preventDefault()),
              onClick --> { e => runByVerdict(forceTable = e.shiftKey) },
              title <-- queryKind.signal.map(runButtonTitle),
              disabled <-- hasErrorDiagnostics.signal
                .combineWith(queryKind.signal)
                .map { case (hasErrors, kind) =>
                  RunAvailability.isBlocked(RunAvailability.evaluate(canRead, hasErrors, kind))
                },
              "Query",
            )
          case _ => span(display := "none")
        },
    )

    // Chrome wrapper: the absolutely-positioned overlay that grows downward as the editor
    // expands. The Monaco container, the status bar, and the diagnostics panel are in-flow
    // children so the overlay's height is Monaco + bar + (optional) panel. The shared status
    // bar / diagnostics panel renders only when the editor is expanded (multi-line AND focused)
    // — the exact gating the in-editor run button uses — so it appears and disappears with the
    // expanded overlay. Clicking a diagnostic row reveals its position via the editor handle.
    val editorChrome: Div = div(
      cls := QueryEditorStyles.queryEditorChrome,
      editorContainer,
      child <-- isExpanded.map {
        case true =>
          QueryDiagnosticsView(
            diagnostics.signal,
            showDiagnostics,
            position => editorHandle.foreach(_.revealRange(position.line, position.column)),
          )
        case false => span(display := "none")
      },
    )

    val editorHost: Div = div(
      cls := QueryEditorStyles.queryEditorHost,
      cls <-- isExpanded.map(expanded => if (expanded) QueryEditorStyles.queryEditorMulti else ""),
      editorChrome,
      onMountCallback { _ =>
        // QuinePattern language-server mode. The route depends on the API version (auth deployments
        // serve only V2); lspWebSocketUrl honors `serverUrl`. Disabled => client-side Monarch
        // highlighting, no connection.
        val qpMode =
          if (qpEnabled) {
            val lspPath = if (useV2Api) "/api/v2/lsp" else "/api/v1/lsp"
            QpMode.Enabled(QueryEditor.lspWebSocketUrl(serverUrl, lspPath))
          } else QpMode.Disabled
        val handle = QueryEditor.create(
          editorContainer.ref,
          new QueryEditorOptions {
            onRunGraph = submitFromEditor(asTable = false): js.Function1[String, Unit]
            onRunTable = submitFromEditor(asTable = true): js.Function1[String, Unit]
            placeholder = if (canRead) "Query returning nodes" else "Not Authorized to READ from graph"
            initialValue = latestQuery
            onChange = ((value: String) => if (value != latestQuery) updateQuery(value)): js.Function1[String, Unit]
            onDiagnosticsChange = ((errors: Boolean) => hasErrorDiagnostics.set(errors)): js.Function1[Boolean, Unit]
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
            onQueryKindChange =
              ((kind: String) => queryKind.set(QueryKind.fromString(kind))): js.Function1[String, Unit]
            onConnectionChange = ((connected: Boolean) => lspConnected.set(connected)): js.Function1[Boolean, Unit]
            onModeChange = ((multi: Boolean) => isMultilineMode.set(multi)): js.Function1[Boolean, Unit]
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

    div(
      cls := Styles.queryInput,
      styleAttr := "position: relative",
      query --> externalQueryObserver,
      // Local query-kind fallback: basic mode has no LSP so always classifies locally; rich mode
      // does so only while the LSP is disconnected (an outage must not lock the query bar). When
      // connected, the server verdict via onQueryKindChange is authoritative. Combining with
      // lspConnected reclassifies immediately on a connection drop, not just on the next edit.
      if (qpEnabled)
        query.combineWith(lspConnected.signal) --> Observer[(String, Boolean)] { case (q, connected) =>
          if (!connected) queryKind.set(localVerdict(q))
        }
      else
        query --> Observer[String](q => queryKind.set(localVerdict(q))),
      runningTextQuery --> Observer[Boolean](running => isRunning = running),
      sampleQueries.combineWith(query).map { case (queries, userInput) =>
        val normalizedUserInput = SampleQueryDropdown.normalizeQueryForFilter(userInput)
        if (normalizedUserInput.isEmpty) queries.zipWithIndex
        else
          queries.zipWithIndex.filter(q =>
            SampleQueryDropdown.normalizeQueryForFilter(q._1.query).contains(normalizedUserInput),
          )
      } --> filteredSampleQueries.writer,
      // The dropdown tracks focus of the editor specifically (not the run buttons): it opens
      // when focus enters the editor and closes when focus leaves it. Monaco moves focus among
      // its internal elements, so `relatedTarget` is checked against the whole editor host.
      onFocusIn --> { e =>
        e.target match {
          case node: dom.Node if editorHost.ref.contains(node) =>
            editorHasFocus.set(true)
            showSampleQueryDropdown.set(true)
          case _ => ()
        }
      },
      onFocusOut --> { e =>
        val focusStaysInEditor = e.relatedTarget match {
          case node: dom.Node => editorHost.ref.contains(node)
          case _ => false
        }
        if (!focusStaysInEditor) {
          editorHasFocus.set(false)
          showSampleQueryDropdown.set(false)
          highlightedIdx.set(None)
        }
      },
      // Re-open the dropdown when the user edits the buffer while the editor is focused.
      // This covers the case where the dropdown was closed after a sample selection and the
      // user immediately starts typing without leaving and re-entering the editor.
      // The guard on `queryText != lastSeenQueryForDropdown` prevents reopening from
      // programmatic round-trips: Enter-to-run calls updateQuery(sameText) and submitQuery
      // fires an unrelated stateVar update, both of which re-emit the query signal with the
      // unchanged text — neither should reopen the dropdown.
      query --> Observer[String] { queryText =>
        val changed = queryText != lastSeenQueryForDropdown
        lastSeenQueryForDropdown = queryText
        if (changed && editorHasFocus.now() && !showSampleQueryDropdown.now())
          showSampleQueryDropdown.set(true)
      },
      editorHost,
      SampleQueryDropdown(
        highlightedIdx,
        filteredSampleQueries.signal,
        updateQuery,
        showSampleQueryDropdown,
      ),
      bookmark.button,
      child <-- runningTextQuery.map { running =>
        if (running)
          button(
            cls := s"${Styles.grayClickable} ${Styles.queryInputButton}",
            onClick --> (_ => cancelButton()),
            title := "Cancel query",
            disabled := !canRead,
            "Cancel",
          )
        else
          button(
            cls := s"${Styles.grayClickable} ${Styles.queryInputButton}",
            onClick --> { e => runByVerdict(forceTable = e.shiftKey) },
            title <-- queryKind.signal.map(runButtonTitle),
            disabled <-- hasErrorDiagnostics.signal
              .combineWith(queryKind.signal)
              .map { case (hasErrors, kind) =>
                RunAvailability.isBlocked(RunAvailability.evaluate(canRead, hasErrors, kind))
              },
            "Query",
          )
      },
      bookmark.dialog,
    )
  }

}
