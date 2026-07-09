import * as monaco from "monaco-editor/esm/vs/editor/editor.api.js";

/**
 * A single LSP diagnostic on the editor's model, in a serializable plain-object
 * shape (no Monaco enums) so Scala/JS consumers can read it directly. Positions
 * are 1-based, matching Monaco's marker coordinates.
 */
export interface EditorDiagnostic {
  /** Human-readable diagnostic message. */
  message: string;
  /** Severity, mapped from Monaco's `MarkerSeverity` enum to a string. */
  severity: "error" | "warning" | "info" | "hint";
  /** 1-based line of the range start. */
  startLineNumber: number;
  /** 1-based column of the range start. */
  startColumn: number;
  /** 1-based line of the range end. */
  endLineNumber: number;
  /** 1-based column of the range end. */
  endColumn: number;
}

/**
 * Maps Monaco's `MarkerSeverity` enum to the serializable string used by
 * {@link EditorDiagnostic}. Exhaustive: if Monaco ever adds a `MarkerSeverity`,
 * this fails to compile rather than silently mapping the new value to "hint".
 */
export function markerSeverityName(
  severity: monaco.MarkerSeverity,
): EditorDiagnostic["severity"] {
  switch (severity) {
    case monaco.MarkerSeverity.Error:
      return "error";
    case monaco.MarkerSeverity.Warning:
      return "warning";
    case monaco.MarkerSeverity.Info:
      return "info";
    case monaco.MarkerSeverity.Hint:
      return "hint";
    default: {
      const _exhaustive: never = severity;
      return _exhaustive;
    }
  }
}

/** Consumer callbacks notified when the model's diagnostics change. */
export interface DiagnosticsListeners {
  /**
   * Fires when the error-severity state flips (and once with the initial
   * `false`). `true` iff at least one `MarkerSeverity.Error` marker is present.
   */
  onDiagnosticsChange?: (hasErrors: boolean) => void;
  /** Fires with the full diagnostic list on every change (and once empty). */
  onDiagnosticsListChange?: (diagnostics: EditorDiagnostic[]) => void;
}

/**
 * Subscribe to marker changes for `model` only and forward them to `listeners`.
 *
 * `monaco.editor.onDidChangeMarkers` is page-global — it fires for every model
 * on the page — so this filters to `model`'s URI before recomputing. Both the
 * boolean predicate and the full list derive from a single marker fetch, so the
 * two callbacks never disagree. Fires once immediately with the initial state
 * (no errors, empty list — the fail-open rule), then only on transitions.
 *
 * The subscription is pushed onto `disposables`. No-op when neither listener is
 * supplied.
 */
export function setupDiagnosticsTracking(
  model: monaco.editor.ITextModel,
  listeners: DiagnosticsListeners,
  disposables: monaco.IDisposable[],
): void {
  const { onDiagnosticsChange, onDiagnosticsListChange } = listeners;
  if (onDiagnosticsChange === undefined && onDiagnosticsListChange === undefined) return;

  const modelUri = model.uri;
  let lastHasErrors = false;

  const notifyIfChanged = (): void => {
    const markers = monaco.editor.getModelMarkers({ resource: modelUri });
    if (onDiagnosticsChange !== undefined) {
      const hasErrors = markers.some((m) => m.severity === monaco.MarkerSeverity.Error);
      if (hasErrors !== lastHasErrors) {
        lastHasErrors = hasErrors;
        onDiagnosticsChange(hasErrors);
      }
    }
    if (onDiagnosticsListChange !== undefined) {
      onDiagnosticsListChange(
        markers.map((m) => ({
          message: m.message,
          severity: markerSeverityName(m.severity),
          startLineNumber: m.startLineNumber,
          startColumn: m.startColumn,
          endLineNumber: m.endLineNumber,
          endColumn: m.endColumn,
        })),
      );
    }
  };

  // Emit the initial state: no markers yet → false (enabled, per the fail-open
  // rule) and an empty list.
  onDiagnosticsChange?.(false);
  onDiagnosticsListChange?.([]);

  disposables.push(
    monaco.editor.onDidChangeMarkers((uris) => {
      // The event delivers the URIs whose markers changed. Only recompute when
      // our model is among them.
      if (uris.some((u) => u.toString() === modelUri.toString())) {
        notifyIfChanged();
      }
    }),
  );
}
