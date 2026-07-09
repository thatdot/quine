import type * as monaco from "monaco-editor/esm/vs/editor/editor.api.js";

import { connectLspWithBackoff, type LspConnection } from "./lsp.js";
import { watchQueryKind, type QueryKind, type QueryKindWatcher } from "./queryKind.js";

/**
 * Owns the editor's language-server connection and the query-kind watcher that
 * rides on it. Keeping this lifecycle in one place means the editor factory
 * holds no LSP mutable state: it just forwards `connect`, content changes, and
 * teardown.
 */
export interface LspController {
  /**
   * Tear down any existing connection/watcher and open a new one to `url`. A
   * fresh query-kind watcher is attached on each (re)connect; the consumer's
   * `onQueryKindChange` fires `"unknown"` immediately so its initial state is
   * correct even when the socket never opens.
   */
  connect(url: string): void;
  /** Notify the active watcher that the buffer changed (debounced re-classify). */
  notifyContentChange(): void;
  /** Dispose the watcher and connection. */
  dispose(): void;
}

/**
 * Create an {@link LspController} for `model`. When `onQueryKindChange` is
 * undefined no watcher is attached — the connection still provides semantic
 * tokens, diagnostics, and completions via `MonacoLspClient`. `onConnectionChange`
 * (optional) is notified whenever the language-server connection opens or closes,
 * so the consumer can fall back to local classification while disconnected.
 */
export function createLspController(
  model: monaco.editor.ITextModel,
  onQueryKindChange: ((kind: QueryKind) => void) | undefined,
  onConnectionChange: ((connected: boolean) => void) | undefined,
): LspController {
  let lspConnection: LspConnection | null = null;
  let kindWatcher: QueryKindWatcher | null = null;

  return {
    connect(url: string): void {
      // Tear down any existing connection and watcher before opening a new one.
      kindWatcher?.dispose();
      kindWatcher = null;
      lspConnection?.dispose();

      lspConnection = connectLspWithBackoff(url);
      const modelUri = model.uri.toString();

      // Report "unknown" first, then the disconnected baseline LAST — so a consumer whose local
      // fallback keys on the connection signal has the final word on the initial verdict.
      onQueryKindChange?.("unknown");
      onConnectionChange?.(false);

      lspConnection.onConnected((ws: WebSocket) => {
        onConnectionChange?.(true);
        if (onQueryKindChange !== undefined) {
          // On reconnect, replace the previous watcher; the fresh one re-issues a classification.
          kindWatcher?.dispose();
          kindWatcher = watchQueryKind(ws, modelUri, onQueryKindChange);
        }
      });

      // On a mid-session close, dispose the watcher (it emits "unknown") and report the disconnect
      // so the consumer switches to local classification; onConnected reattaches on re-open.
      lspConnection.onDisconnected(() => {
        kindWatcher?.dispose();
        kindWatcher = null;
        onConnectionChange?.(false);
      });
    },
    notifyContentChange(): void {
      kindWatcher?.onContentChange(model.uri.toString());
    },
    dispose(): void {
      kindWatcher?.dispose();
      kindWatcher = null;
      lspConnection?.dispose();
      lspConnection = null;
    },
  };
}
