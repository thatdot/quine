# @thatdot/query-editor

A multi-line Cypher query editor component built on **vanilla
[monaco-editor](https://github.com/microsoft/monaco-editor)** — no React, no
wrapper stacks, no bundled Monaco. It reproduces the query-bar UX of Neo4j
Browser: a single-line bar that grows into a real editor as you type.

> **Status: pre-release.** `"private": true` is set and nothing is published
> to any registry yet. The package lives in-tree at `public/query-editor` in
> quine-plus; the browser modules (`quine-browser`, `quine-enterprise-browser`,
> `novelty-browser`) compile its TypeScript source directly through their
> webpack `ts-loader`, resolved by a `@thatdot/query-editor` alias in each
> `common.webpack.config.js` — no separate build step and no registry. This
> directory still works as a standalone package for development: `npm install`
> then `npm run playground`, `npm test`, `npm run typecheck`, or `npm run build`.

## Behaviors

- **Mode-dependent Enter** — `Enter` runs the query while the buffer is
  single-line; once the buffer is multi-line it inserts a newline instead.
- **`Cmd/Ctrl+Enter`** always runs; **`Shift+Enter`** always inserts a newline.
- **Auto-grow** — the editor grows one line at a time up to a height cap
  (default 384px), then scrolls internally.
- **Line-number morph** — no gutter in single-line mode; line numbers appear
  when the buffer goes multi-line (the mode signal).
- **Query history** — `ArrowUp`/`ArrowDown` step through a
  localStorage-persisted history, only while the buffer is single-line; the
  unsent draft is preserved when stepping off it.
- **Cypher syntax highlighting** via Monaco's built-in Monarch grammar, with a
  provisional light theme (Neo4j-inspired palette; variables deliberately
  uncolored).
- Placeholder text, monospace font (no `->` → `→` ligature surprises), `Tab`
  moves focus instead of being trapped.

LSP support — semantic highlighting, diagnostics, completions, hover docs, and
a query-kind verdict — turns on when you pass `qpEnabled: true` with a
`languageServerUrl`. The editor opens a WebSocket to that Quine language server
and reconnects indefinitely with capped exponential backoff (held at 30 s), so a
server that restarts mid-session recovers on its own. While the server is
unavailable the editor has **no** syntax highlighting — the Monarch tokenizer is
skipped when `qpEnabled` — and the query-kind verdict falls back to a local
non-empty-buffer heuristic (via `onConnectionChange`) so the Run button stays
usable. Omit `qpEnabled` (or set it `false`) and the editor stays on Monarch-only
highlighting and never connects; the URL is a type error in that branch, so the
flag and the URL can't disagree.

## API

```ts
import { createQueryEditor } from "@thatdot/query-editor";

const editor = createQueryEditor(containerElement, {
  placeholder: "Query returning nodes",     // optional
  initialValue: "",                         // optional
  maxHeightPx: 384,                         // optional growth cap
  historyStorageKey: "my-app.query-history",// optional localStorage key
  onRunGraph: (query) => { /* run, render as graph */ },
  onRunTable: (query) => { /* run, render as table */ },
  onChange: (value) => { /* optional */ },
  onDiagnosticsChange: (hasErrors) => { /* optional: disable submit on errors */ },
  onDiagnosticsListChange: (diagnostics) => { /* optional: drive a problems panel */ },

  // Language server (optional, discriminated on qpEnabled):
  qpEnabled: true,                          // semantic tokens become the sole highlighter
  languageServerUrl: "wss://host/api/v2/lsp",// required when qpEnabled is true
});

editor.getValue();
editor.setValue("MATCH (n) RETURN n");
editor.focus();
editor.runGraph();        // record history + fire onRunGraph
editor.runTable();        // record history + fire onRunTable
editor.dispose();         // full teardown (listeners, observers, model)
```

No keybinding triggers `onRunTable`; wire it to your own affordance (e.g. a
split Query button). `runGraph`/`runTable` route button clicks through the
editor so history is recorded consistently.

## What the consumer owns (deliberately not in this package)

`monaco-editor` is a **peerDependency** (`^0.55`) and is never bundled —
Monaco is singleton-by-design, and worker/CSS/font wiring is
bundler-specific. The consuming app must:

1. Install `monaco-editor` itself, pinned within `^0.55` (this package uses
   deep `esm/vs/...` imports, which are internal paths that can move between
   Monaco minors — keep the pin in lockstep).
2. Wire `MonacoEnvironment.getWorker` to Monaco's `editor.worker` per its
   bundler's recipe (see `playground/main.ts` for the Vite flavor; webpack
   uses a plain ESM worker entry — no monaco-editor-webpack-plugin).
3. Let its bundler handle Monaco's CSS imports and the `codicon.ttf` font
   asset.
4. Load JetBrains Mono for exact editor typography. The in-repo browser bundles
   do this with `@fontsource-variable/jetbrains-mono`, whose font files are
   licensed under the SIL Open Font License 1.1 (`OFL-1.1`).

## Development

```sh
npm install
npm run playground   # Vite dev harness at http://localhost:5173
npm test             # vitest unit tests (history, mode logic)
npm run typecheck    # tsc --noEmit over src + test + playground
npm run build        # tsc emit to dist/ (ESM + .d.ts) — the package contents
```

The playground (`index.html` + `playground/`) is a dev harness only; it is
not part of the published package (`files: ["dist"]`).

To run the editor against a live language server, the repo keeps its
run-Quine-alongside setup: drop a `quine.jar` in the repo root, `cp
.env.example .env`, then `npm run dev` to launch Quine (LSP WebSocket at
`ws://127.0.0.1:8080/api/v1/lsp`, always on) next to the playground.

## Publishing

`.github/workflows/publish.yml` publishes on push to `main`, guarded by a
version-already-published check. It is currently inert by design: the
`@thatdot` scope and `NPM_TOKEN` secret don't exist yet, and
`"private": true` blocks `npm publish` outright. Arming it (npm org,
token/OIDC, removing the private flag) is a deliberate release step.
