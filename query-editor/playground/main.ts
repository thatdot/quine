/**
 * Dev playground for @thatdot/query-editor. Not part of the published
 * package — which is why it may (and must) wire up Monaco's editor worker
 * itself: the package never hardcodes worker URLs; every consumer owns its
 * bundler-specific worker/CSS/font setup. This is the Vite flavor.
 */
import "./playground.css";
import editorWorker from "monaco-editor/esm/vs/editor/editor.worker.js?worker";

import { createQueryEditor, type QueryEditorHandle } from "../src/index.js";

self.MonacoEnvironment = {
  getWorker: () => new editorWorker(),
};

const byId = <T extends HTMLElement>(id: string): T => {
  const el = document.getElementById(id);
  if (el === null) throw new Error(`missing #${id}`);
  return el as T;
};

const log = byId<HTMLPreElement>("log");
const mode = byId<HTMLSpanElement>("mode");

const append = (kind: string, detail: string): void => {
  const time = new Date().toLocaleTimeString();
  log.textContent = `[${time}] ${kind}: ${detail}\n${log.textContent ?? ""}`;
};

// Connect to the Quine language server when a URI is configured. Folding the URL
// into the options (rather than a separate connectLsp call) means `qpEnabled`
// and the URL are set together: with the URI, semantic tokens highlight; without
// it, the editor stays on Monarch-only. The package degrades gracefully if the
// server is unreachable (WS 404/close → Monarch, no retry storm).
const lspUri = import.meta.env.VITE_LS_WEBSOCKET_URI;
byId("lsp-uri").textContent = lspUri ?? "(no .env — copy .env.example)";

const baseOptions = {
  placeholder: "Query returning nodes",
  historyStorageKey: "thatdot.query-editor.playground.history",
  onRunGraph: (query: string) => append("RUN GRAPH", JSON.stringify(query)),
  onRunTable: (query: string) => append("RUN TABLE", JSON.stringify(query)),
  onChange: (value: string) => {
    mode.textContent = value.includes("\n") ? "multi-line" : "single-line";
  },
};

// Each branch is a concrete literal so it matches one arm of the discriminated
// QueryEditorLanguageMode: with the URI, the enabled arm (qpEnabled + URL);
// without it, the disabled arm (Monarch-only, no connection).
let editor: QueryEditorHandle | null = createQueryEditor(
  byId("query-editor"),
  lspUri
    ? { ...baseOptions, qpEnabled: true, languageServerUrl: lspUri }
    : baseOptions,
);

const withEditor = (f: (e: QueryEditorHandle) => void) => (): void => {
  if (editor === null) {
    append("ERROR", "editor is disposed");
    return;
  }
  f(editor);
};

byId("run-graph").addEventListener("click", (event) => {
  if (editor === null) {
    append("ERROR", "editor is disposed");
    return;
  }
  // Shift+click runs as table (continuity with the old Quine Explorer).
  if (event.shiftKey) editor.runTable();
  else editor.runGraph();
});
byId("run-table").addEventListener(
  "click",
  withEditor((e) => {
    e.runTable();
  }),
);
byId("focus").addEventListener(
  "click",
  withEditor((e) => {
    e.focus();
  }),
);
byId("set-sample").addEventListener(
  "click",
  withEditor((e) => {
    e.setValue('MATCH (n)\nWHERE id(n) = idFrom("Matthew")\nRETURN n // sample');
    e.focus();
  }),
);
byId("dispose").addEventListener("click", () => {
  editor?.dispose();
  editor = null;
  append("DISPOSED", "editor torn down");
});
byId("clear-log").addEventListener("click", () => {
  log.textContent = "";
});
