import * as monaco from "monaco-editor/esm/vs/editor/editor.api.js";
// Editor feature contributions (side-effect imports). Only what the query
// bar needs — `editor.main` would pull in every language and feature.
// Deep `esm/vs/` paths are unstable across Monaco minors; the narrow ^0.55
// peer range is the guard.
import "monaco-editor/esm/vs/editor/contrib/placeholderText/browser/placeholderText.contribution.js"; // `placeholder` option
import "monaco-editor/esm/vs/editor/contrib/suggest/browser/suggestController.js"; // suggest widget (completions)
import "monaco-editor/esm/vs/editor/contrib/hover/browser/hoverContribution.js"; // marker hovers (diagnostics)
import "monaco-editor/esm/vs/editor/contrib/semanticTokens/browser/documentSemanticTokens.js"; // semantic highlighting
import "monaco-editor/esm/vs/editor/contrib/bracketMatching/browser/bracketMatching.js";

import { CYPHER_LANGUAGE_ID, installCypherMonarchTokenizer, registerCypherLanguage } from "./cypher.js";
import { setupDiagnosticsTracking, type EditorDiagnostic } from "./editorDiagnostics.js";
import { QueryHistory, type HistoryStorage } from "./history.js";
import { createLspController } from "./lspController.js";
import { clampHeight, computeHeight, isMultiline } from "./mode.js";
import { defineQueryEditorTheme, QUERY_EDITOR_THEME } from "./theme.js";
import { type QueryKind } from "./queryKind.js";

// Re-export so the public surface (index.ts) and consumers keep a single import.
export type { EditorDiagnostic } from "./editorDiagnostics.js";

/**
 * Options common to every editor, independent of whether the Quine language
 * server is active. The public {@link QueryEditorOptions} type intersects this
 * with {@link QueryEditorLanguageMode}, which carries the `qpEnabled`
 * discriminant and (when enabled) the language-server URL.
 */
interface QueryEditorBaseOptions {
  /**
   * Buffer language. `"cypher"` (default) installs this package's Cypher
   * language configuration and, without a language server, its Monarch
   * tokenizer. `"plaintext"` uses Monaco's built-in plain-text mode — no
   * highlighting, no bracket rules, and no language-server connection even
   * when `qpEnabled` is set — for buffers in languages this package has no
   * grammar for (e.g. Gremlin), where Cypher tokenization would be wrong.
   */
  language?: "cypher" | "plaintext";
  /** Hint text shown while the editor is empty. */
  placeholder?: string;
  /** Initial buffer content. Defaults to empty. */
  initialValue?: string;
  /**
   * Height (px) at which the editor stops growing and scrolls internally.
   * Defaults to {@link DEFAULT_MAX_HEIGHT_PX}.
   */
  maxHeightPx?: number;
  /**
   * localStorage key for the ArrowUp/ArrowDown query history. Defaults to
   * {@link DEFAULT_HISTORY_STORAGE_KEY}. History degrades to in-memory when
   * localStorage is unavailable.
   */
  historyStorageKey?: string;
  /**
   * Embedded mode: drop the three Query-bar-only behaviors so the editor reads
   * as a plain multi-line form field. When `true` (default `false`), the editor
   * does NOT register the run-on-Enter / Cmd+Enter commands (Enter inserts a
   * newline), the blur/focus collapse handlers (the field keeps its height), or
   * the ArrowUp/ArrowDown history commands (the arrows move the cursor).
   * Everything else — auto-grow to `maxHeightPx`, the resize handle, semantic
   * highlighting, the diagnostics listener, and the language-server connection —
   * is unchanged.
   */
  embedded?: boolean;
  /**
   * Run the query with graph-shaped results. Fired by Enter (when the suggest
   * widget is closed), Cmd/Ctrl+Enter (always), and
   * {@link QueryEditorHandle.runGraph}. Optional: omit in `embedded` mode, where
   * no keybinding triggers a run.
   */
  onRunGraph?: (query: string) => void;
  /**
   * Run the query with table-shaped results. No keybinding triggers this — the
   * consumer wires it to its own affordance (e.g. a split Query button) via
   * {@link QueryEditorHandle.runTable}. Optional: omit in `embedded` mode.
   */
  onRunTable?: (query: string) => void;
  /** Called with the full buffer content after every edit. */
  onChange?: (value: string) => void;
  /**
   * Called whenever the error-severity diagnostic state of the editor's model
   * changes, and once immediately after the editor mounts with the initial
   * state. `hasErrors` is `true` when at least one `MarkerSeverity.Error`
   * marker is present on the model; `false` otherwise (including when no
   * diagnostics have arrived yet — the fail-open rule).
   *
   * Consumers use this to disable submit controls while the buffer contains
   * errors. Because LSP diagnostics arrive after a ~500 ms debounce, a user
   * can submit before the first diagnostic report lands; that is intentional
   * (fail-open).
   *
   * The page may host multiple editors. This callback fires only for markers
   * on THIS editor's model URI, not markers from other editors.
   */
  onDiagnosticsChange?: (hasErrors: boolean) => void;
  /**
   * Called with the full list of diagnostics on this editor's model whenever
   * they change, and once immediately after the editor mounts with an empty
   * list. Additive to {@link onDiagnosticsChange}: that boolean keeps its
   * fail-open button-disable semantics; this list feeds a textual summary /
   * problems panel.
   *
   * Like {@link onDiagnosticsChange}, this fires only for markers on THIS
   * editor's model URI, and both are computed from the same marker fetch.
   * Diagnostics arrive after a ~500 ms debounce, so the list briefly trails the
   * buffer between a keystroke and the server's report.
   */
  onDiagnosticsListChange?: (diagnostics: EditorDiagnostic[]) => void;
  /**
   * Called whenever the server's classification of the current buffer changes,
   * and once immediately after the editor connects to the language server with
   * `"unknown"`.
   *
   * - `"node"` — every final-RETURN column is a graph node; run as a graph
   *   query and render results on the canvas.
   * - `"table"` — well-formed but not provably all-node; run as a table query.
   * - `"unknown"` — empty/whitespace buffer, parse/symbol errors, or the LSP
   *   is unreachable. The consumer should disable the submit button.
   *
   * Fires only on transitions (plus the initial `"unknown"`), not on every
   * request. The server verdict arrives after a ~350 ms debounce following each
   * content change.
   */
  onQueryKindChange?: (kind: QueryKind) => void;
  /**
   * Called when the language-server connection opens (`true`) or closes (`false`), and once
   * immediately with `false` before the first attempt. Only fires when `qpEnabled` is `true`.
   *
   * Consumers use it to fall back to local query-kind classification while disconnected — the
   * server verdict (via {@link onQueryKindChange}) is unavailable then — so Run stays usable
   * through an outage instead of locking on the last `"unknown"`.
   */
  onConnectionChange?: (connected: boolean) => void;
  /**
   * Called when the editor's mode transitions between single-line and
   * multi-line. Fires once at mount with the initial mode, then on every
   * transition. `isMultiline` is `true` when the buffer contains at least one
   * hard newline. Consumers use this to apply wider-width or resize CSS classes
   * to the editor host.
   */
  onModeChange?: (isMultiline: boolean) => void;
}

/**
 * Language-server mode, discriminated on the QuinePattern feature flag
 * (`qpEnabled`, mirroring the server's `qp.enabled`).
 *
 * - `qpEnabled: true` REQUIRES a `languageServerUrl`: the editor connects to the
 *   Quine language server at that URL and its semantic tokens become the sole
 *   syntax highlighter.
 * - Otherwise (`false`/omitted) the editor installs the client-side Monarch
 *   tokenizer and never opens a language-server connection; passing a
 *   `languageServerUrl` in this branch is a type error.
 *
 * Encoding the URL as required-iff-enabled makes the previously hand-maintained
 * pairing (set the flag AND call `connectLsp`) impossible to get wrong.
 */
export type QueryEditorLanguageMode =
  | { qpEnabled?: false; languageServerUrl?: never }
  | { qpEnabled: true; languageServerUrl: string };

/** Options accepted by {@link createQueryEditor}. */
export type QueryEditorOptions = QueryEditorBaseOptions & QueryEditorLanguageMode;

// Re-export so consumers don't need a second import.
export type { QueryKind } from "./queryKind.js";

/** Handle returned by {@link createQueryEditor}. */
export interface QueryEditorHandle {
  /** Current buffer content. */
  getValue(): string;
  /** Replace the buffer content (fires `onChange`). */
  setValue(value: string): void;
  /** Give the editor keyboard focus. */
  focus(): void;
  /** Remove keyboard focus from the editor. */
  blur(): void;
  /** Record the current query in history and fire `onRunGraph`. */
  runGraph(): void;
  /** Record the current query in history and fire `onRunTable`. */
  runTable(): void;
  /**
   * Move the cursor to `lineNumber`/`column` (both 1-based, matching Monaco and
   * {@link EditorDiagnostic}), scroll that position into view, and focus the
   * editor. Used to jump from a diagnostics-list entry to its source position.
   */
  revealRange(lineNumber: number, column: number): void;
  /** Tear down the editor, its listeners, and observers. */
  dispose(): void;
}

/** Default growth cap — Neo4j Browser's measured cap (~20 lines). */
export const DEFAULT_MAX_HEIGHT_PX = 384;

/** Default localStorage key for query history. */
export const DEFAULT_HISTORY_STORAGE_KEY = "thatdot.query-editor.history";

const QUERY_EDITOR_FONT_SIZE_PX = 16;
const QUERY_EDITOR_LINE_HEIGHT_PX = 24;
const QUERY_EDITOR_SINGLE_LINE_CONTENT_HEIGHT_PX = 36;
const QUERY_EDITOR_VERTICAL_PADDING_PX =
  (QUERY_EDITOR_SINGLE_LINE_CONTENT_HEIGHT_PX - QUERY_EDITOR_LINE_HEIGHT_PX) / 2;

/** Per-instance context-key scoping so multiple editors don't cross-fire. */
let instanceCounter = 0;

function defaultHistoryStorage(): HistoryStorage | null {
  try {
    // Accessing localStorage can throw (e.g. sandboxed iframes).
    return typeof localStorage === "undefined" ? null : localStorage;
  } catch {
    return null;
  }
}

/**
 * Mount a multi-line Cypher query editor into `container` and return a handle to
 * drive it. The editor's behaviors — keybindings, auto-grow, blur-collapse, and
 * history recall — are documented on {@link QueryEditorOptions}.
 *
 * The consumer owns Monaco worker/CSS/font wiring (bundler-specific —
 * see README); this package never bundles Monaco or hardcodes worker URLs.
 */
export function createQueryEditor(
  container: HTMLElement,
  opts: QueryEditorOptions,
): QueryEditorHandle {
  const plainText = opts.language === "plaintext";
  if (!plainText) {
    registerCypherLanguage();
    // With qp.enabled the language server's semantic tokens are the sole highlighter; otherwise
    // the client-side Monarch tokenizer is.
    const useMonarchHighlighter = opts.qpEnabled !== true;
    if (useMonarchHighlighter) installCypherMonarchTokenizer();
  }
  defineQueryEditorTheme();

  const maxHeightPx = opts.maxHeightPx ?? DEFAULT_MAX_HEIGHT_PX;
  // Embedded (form-field) mode skips the three Query-bar-only behaviors below:
  // run-on-Enter, blur-collapse, and ArrowUp/Down history. See QueryEditorOptions.
  const embedded = opts.embedded ?? false;
  const history = new QueryHistory(
    opts.historyStorageKey ?? DEFAULT_HISTORY_STORAGE_KEY,
    defaultHistoryStorage(),
  );

  const editor = monaco.editor.create(container, {
    value: opts.initialValue ?? "",
    language: plainText ? "plaintext" : CYPHER_LANGUAGE_ID,
    theme: QUERY_EDITOR_THEME,
    placeholder: opts.placeholder,

    // --- Non-default options the query bar relies on; each inline comment is
    // the reason the option differs from the Monaco default.
    scrollBeyondLastLine: false, // REQUIRED: auto-grow feedback loop without it
    wordBasedSuggestions: "off", // suggest widget hangs at "Loading..." otherwise
    fixedOverflowWidgets: true, // suggest/hover escape small positioned containers
    minimap: { enabled: false },
    folding: false,
    glyphMargin: false,
    lineNumbers: "off", // morphs to "on" in multi-line mode
    lineNumbersMinChars: 3,
    lineDecorationsWidth: 8,
    overviewRulerLanes: 0,
    overviewRulerBorder: false,
    hideCursorInOverviewRuler: true,
    renderLineHighlight: "none",
    scrollbar: {
      vertical: "auto",
      horizontal: "hidden",
      useShadows: false,
      alwaysConsumeMouseWheel: false, // don't trap page scroll in a small bar
    },
    wordWrap: "off", // long lines scroll horizontally; the editor never soft-wraps
    tabFocusMode: true, // Tab moves focus — don't trap keyboard users
    contextmenu: false,
    automaticLayout: false, // we drive layout from content size + ResizeObserver

    // Monospace avoids the proportional-font `->` ligature rendering problem.
    fontFamily:
      '"JetBrains Mono Variable", "JetBrains Mono", ui-monospace, SFMono-Regular, Menlo, Consolas, monospace',
    fontLigatures: false,
    fontSize: QUERY_EDITOR_FONT_SIZE_PX,
    lineHeight: QUERY_EDITOR_LINE_HEIGHT_PX,
    padding: {
      top: QUERY_EDITOR_VERTICAL_PADDING_PX,
      bottom: QUERY_EDITOR_VERTICAL_PADDING_PX,
    },
    ariaLabel: opts.placeholder ?? "Query editor",
  });

  const model = editor.getModel();
  if (model === null) throw new Error("query editor created without a model");

  const disposables: monaco.IDisposable[] = [];

  // Owns the LSP connection + query-kind watcher lifecycle; the factory just
  // forwards connect/content-change/dispose to it.
  const lsp = createLspController(model, opts.onQueryKindChange, opts.onConnectionChange);

  // --- Per-instance + mode context keys (drive the keybinding `when`
  // clauses). `addCommand` keybindings are page-global in Monaco, so each
  // is scoped to this instance via `thatdotQueryEditorId`.
  const instanceId = `thatdotQueryEditor${++instanceCounter}`;
  editor.createContextKey<string>("thatdotQueryEditorId", instanceId);
  const multilineKey = editor.createContextKey<boolean>(
    "queryBarMultiline",
    isMultiline(model.getValue()),
  );
  const scoped = (expr: string): string =>
    `editorTextFocus && thatdotQueryEditorId == ${instanceId}${expr ? ` && ${expr}` : ""}`;

  // --- Resize handle DOM element (appended to container after editor creation).
  const resizeHandle = document.createElement("div");
  resizeHandle.className = "query-editor-resize-handle";
  resizeHandle.style.display = "none"; // shown by syncMode when multi-line
  resizeHandle.setAttribute("aria-hidden", "true");

  // --- "More lines" badge: shown only while a multi-line editor is collapsed by
  // blur, hinting that line 1 is a preview of a longer query. `pointer-events:
  // none` (in CSS) lets a click fall through to the editor and re-expand it.
  const moreLinesBadge = document.createElement("div");
  moreLinesBadge.className = "query-editor-more-lines";
  moreLinesBadge.style.display = "none";
  moreLinesBadge.setAttribute("aria-hidden", "true");

  // --- User-set height floor (drag-resize). null = auto-grow only.
  let userSetHeight: number | null = null;

  // --- Line-number morph: off single-line, on multi-line (the mode signal).
  // `lastMultiline` tracks the previous state so syncMode fires onModeChange
  // only on transitions (and once on the initial call).
  let lineNumbersOn = false;
  let lastMultiline: boolean | null = null; // null = not yet initialised
  const syncMode = (): void => {
    const multi = isMultiline(model.getValue());
    multilineKey.set(multi);
    if (multi !== lineNumbersOn) {
      lineNumbersOn = multi;
      editor.updateOptions({ lineNumbers: multi ? "on" : "off" });
    }
    // Show/hide the resize handle based on mode.
    resizeHandle.style.display = multi ? "" : "none";
    // Reset the user-set height floor when leaving multi-line mode so the bar
    // snaps back to the 48 px single-line height on the next fit() call.
    if (!multi && lastMultiline === true) {
      userSetHeight = null;
    }
    if (multi !== lastMultiline) {
      lastMultiline = multi;
      opts.onModeChange?.(multi);
    }
  };

  // --- Auto-grow: content height drives container height, clamped to the
  // cap; beyond the cap the editor scrolls internally.
  let lastWidth = container.clientWidth;
  // Most recent single-line layout height, captured by fit(). Used to collapse
  // a multi-line editor back to single-line appearance while it is blurred.
  let singleLineHeight = 0;
  // True while a multi-line editor is collapsed because it lost focus. fit()
  // honors this so the collapse survives any stray content-size-change re-fit
  // (e.g. a re-layout triggered by the submit path) until the editor refocuses.
  let blurCollapsed = false;
  const fit = (): void => {
    if (blurCollapsed && singleLineHeight > 0) {
      container.style.height = `${singleLineHeight}px`;
      editor.layout({ width: container.clientWidth, height: singleLineHeight });
      return;
    }
    const height = computeHeight(editor.getContentHeight(), maxHeightPx, userSetHeight);
    container.style.height = `${height}px`;
    editor.layout({ width: container.clientWidth, height });
    if (!isMultiline(model.getValue())) {
      singleLineHeight = height;
    }
  };
  disposables.push(editor.onDidContentSizeChange(fit));

  // --- Focus-driven visual collapse: losing focus while multi-line collapses
  // the editor to its single-line height (content preserved); refocusing
  // restores the full multi-line layout. `blurCollapsed` guards the focus
  // handler so it only restores after a blur-driven collapse, and gates fit()
  // so the collapse cannot be clobbered by a concurrent re-fit.
  //
  // Skipped in embedded mode: a form field keeps its full height when it loses
  // focus rather than collapsing to a one-line preview.
  if (!embedded) {
    disposables.push(
      editor.onDidBlurEditorWidget(() => {
        if (isMultiline(model.getValue()) && singleLineHeight > 0) {
          blurCollapsed = true;
          editor.updateOptions({ lineNumbers: "off" });
          resizeHandle.style.display = "none";
          fit();
          // Show line 1 (not wherever the buffer was scrolled) plus a "more lines" hint.
          editor.setScrollPosition({ scrollTop: 0, scrollLeft: 0 });
          moreLinesBadge.textContent = `⋯ ${model.getLineCount()}`;
          moreLinesBadge.style.display = "";
        }
      }),
    );
    disposables.push(
      editor.onDidFocusEditorWidget(() => {
        if (blurCollapsed) {
          blurCollapsed = false;
          moreLinesBadge.style.display = "none";
          // Restore line numbers and the resize handle directly from the current
          // content. syncMode() can't be reused here: its internal line-number
          // tracking still reads "on" (the blur turned them off without going
          // through syncMode), so it would treat this as a no-op transition.
          const multi = isMultiline(model.getValue());
          editor.updateOptions({ lineNumbers: multi ? "on" : "off" });
          resizeHandle.style.display = multi ? "" : "none";
          fit(); // restores height based on actual content
        }
      }),
    );
  }
  const resizeObserver = new ResizeObserver(() => {
    const width = container.clientWidth;
    if (width !== lastWidth) {
      lastWidth = width;
      fit(); // re-layout on container width changes
    }
  });
  resizeObserver.observe(container);

  // --- Content changes: mode sync, history-draft invalidation, onChange,
  // and query-kind re-classification trigger.
  let applyingHistory = false;
  disposables.push(
    model.onDidChangeContent(() => {
      syncMode();
      // A user edit (not our own history application) becomes the new live
      // draft: navigation state resets.
      if (!applyingHistory) history.endNavigation();
      opts.onChange?.(model.getValue());
      // Notify the query-kind watcher so it can debounce a re-classification.
      lsp.notifyContentChange();
    }),
  );

  // --- Diagnostic-error state: listen to marker changes for this model only
  // and expose them to the consumer via `onDiagnosticsChange`.
  //
  // `onDidChangeMarkers` is page-global — it fires for every model on the
  // page. We filter to this editor's model URI before computing the predicate.
  // The callback fires only on transitions (false→true or true→false) plus
  // once immediately below with the initial state, so the consumer never needs
  // to poll.
  setupDiagnosticsTracking(
    model,
    {
      onDiagnosticsChange: opts.onDiagnosticsChange,
      onDiagnosticsListChange: opts.onDiagnosticsListChange,
    },
    disposables,
  );

  // --- Running. The callback is optional (absent in embedded mode); `run`
  // no-ops cleanly when it is not supplied.
  const run = (callback: ((query: string) => void) | undefined): void => {
    if (callback === undefined) return;
    const query = model.getValue();
    if (query.trim() === "") return;
    history.push(query);
    callback(query);
  };
  const runGraph = (): void => run(opts.onRunGraph);
  const runTable = (): void => run(opts.onRunTable);

  // --- Keybindings.
  // Enter runs whenever the suggest widget is closed; Shift+Enter inserts a
  // newline in all modes. The run keybindings are skipped in embedded mode,
  // where Enter falls back to Monaco's native newline (a form field has no
  // "run").
  if (!embedded) {
    editor.addCommand(
      monaco.KeyCode.Enter,
      runGraph,
      scoped("!suggestWidgetVisible"),
    );
    // Cmd/Ctrl+Enter always runs (as a graph query), in both modes.
    editor.addCommand(monaco.KeyMod.CtrlCmd | monaco.KeyCode.Enter, runGraph, scoped(""));
    // Ctrl+Shift+Enter runs as a table — the keyboard equivalent of Shift-clicking Run (basic mode
    // forces a text/table query; rich mode still routes by the server verdict). WinCtrl is the
    // physical Control key on every platform (Control on macOS, Ctrl on Windows/Linux), so it is the
    // same chord everywhere — distinct from CtrlCmd, which is Cmd on macOS.
    editor.addCommand(
      monaco.KeyMod.WinCtrl | monaco.KeyMod.Shift | monaco.KeyCode.Enter,
      runTable,
      scoped(""),
    );
  }
  // Shift+Enter always inserts a newline (explicit, so nothing else can
  // claim it).
  editor.addCommand(
    monaco.KeyMod.Shift | monaco.KeyCode.Enter,
    () => {
      editor.trigger("keyboard", "type", { text: "\n" });
    },
    scoped(""),
  );

  // --- History navigation: single-line buffers only (Neo4j semantics).
  // In a single-line buffer the cursor is necessarily on both the first and
  // last line, so the boundary condition reduces to the mode check.
  //
  // Skipped in embedded mode: in a multi-line form field ArrowUp/ArrowDown must
  // move the cursor between lines, not recall persisted query history.
  const applyHistory = (text: string): void => {
    applyingHistory = true;
    try {
      model.setValue(text);
      const lastLine = model.getLineCount();
      editor.setPosition({ lineNumber: lastLine, column: model.getLineMaxColumn(lastLine) });
    } finally {
      applyingHistory = false;
    }
  };
  if (!embedded) {
    editor.addCommand(
      monaco.KeyCode.UpArrow,
      () => {
        const entry = history.previous(model.getValue());
        if (entry !== null) applyHistory(entry);
      },
      scoped("!queryBarMultiline && !suggestWidgetVisible"),
    );
    editor.addCommand(
      monaco.KeyCode.DownArrow,
      () => {
        const entry = history.next();
        if (entry !== null) applyHistory(entry);
      },
      scoped("!queryBarMultiline && !suggestWidgetVisible"),
    );
  }

  // Initial layout + mode state.
  syncMode();
  fit();

  // Append the resize handle into the container (after the editor DOM is ready).
  container.appendChild(resizeHandle);
  container.appendChild(moreLinesBadge);

  // --- Drag-resize: mousedown on the handle starts a drag that lets the user
  // set a minimum height floor for the editor while in multi-line mode.
  resizeHandle.addEventListener("mousedown", (startEvent: MouseEvent) => {
    startEvent.preventDefault(); // prevent text-selection during drag
    const startY = startEvent.clientY;
    const startHeight = container.clientHeight;
    const minDragHeight = clampHeight(editor.getContentHeight(), maxHeightPx);

    const onMouseMove = (e: MouseEvent): void => {
      const newHeight = Math.max(startHeight + (e.clientY - startY), minDragHeight);
      userSetHeight = newHeight;
      container.style.height = `${newHeight}px`;
      // Drive Monaco's layout directly to avoid a ResizeObserver round-trip
      // (the observer only re-fires fit() on width changes, but calling layout
      // here keeps Monaco in sync immediately during the drag).
      editor.layout({ width: container.clientWidth, height: newHeight });
    };

    const onMouseUp = (): void => {
      document.removeEventListener("mousemove", onMouseMove);
      document.removeEventListener("mouseup", onMouseUp);
    };

    document.addEventListener("mousemove", onMouseMove);
    document.addEventListener("mouseup", onMouseUp);
  });

  // Enable semantic token highlighting. The standalone Monaco theme service
  // defaults semanticHighlighting to false; enabling it via updateOptions
  // propagates through the configuration service so the document semantic
  // tokens contribution picks it up.
  editor.updateOptions({ "semanticHighlighting.enabled": true } as Parameters<typeof editor.updateOptions>[0]);

  // When QuinePattern is enabled the URL is guaranteed present (the discriminated
  // QueryEditorLanguageMode requires it), so the connection is wired here rather
  // than left to a separate imperative call the consumer might forget — or call
  // while the language server doesn't exist. The client performs the LSP 3.17
  // handshake, registers the semantic-token provider, auto-issues pull
  // diagnostics, and renders completions; it reconnects on close with bounded
  // backoff and degrades to Monarch-only (here: no highlighter) on a 404/close.
  // Plain-text buffers never connect: the Quine language server only speaks Cypher, so its
  // diagnostics/tokens would be nonsense for e.g. a Gremlin buffer.
  if (opts.qpEnabled && !plainText) lsp.connect(opts.languageServerUrl);

  return {
    getValue: () => model.getValue(),
    setValue: (value: string) => {
      model.setValue(value);
    },
    focus: () => {
      editor.focus();
    },
    blur: () => {
      // Monaco holds keyboard focus on an inner element (a hidden textarea on
      // older builds, a `native-edit-context` div on EditContext-API builds),
      // not on the root node returned by `getDomNode()`. Blur the element that
      // actually has focus inside the editor so `onDidBlurEditorWidget` fires.
      const dom = editor.getDomNode();
      const active = document.activeElement;
      if (dom !== null && active instanceof HTMLElement && dom.contains(active)) {
        active.blur();
      }
    },
    runGraph,
    runTable,
    revealRange: (lineNumber: number, column: number): void => {
      editor.setPosition({ lineNumber, column });
      editor.revealPositionInCenter({ lineNumber, column });
      editor.focus();
    },
    dispose: () => {
      lsp.dispose();
      resizeObserver.disconnect();
      for (const d of disposables) d.dispose();
      editor.dispose();
      model.dispose();
      // Monaco's dispose leaves an empty hidden `context-view` div behind;
      // clear it so repeated mount/unmount cycles (e.g. SPA navigation)
      // don't accumulate stray nodes. The container is dedicated to the
      // editor by contract.
      container.replaceChildren();
      container.style.height = "";
    },
  };
}
