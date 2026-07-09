import { MonacoLspClient, WebSocketTransport } from "monaco-editor/esm/external/monaco-lsp-client/out/index.js";

/** Reconnect backoff (ms): 1 s, 2 s, 4 s, 8 s, 16 s, then 30 s for every attempt beyond. */
const LSP_BACKOFF_DELAYS_MS = [1_000, 2_000, 4_000, 8_000, 16_000, 30_000] as const;

/** A socket must stay open this long before its connection counts as stable and the backoff
 * resets. Resetting on the raw `open` event would let a flapping endpoint reconnect in a tight loop. */
const STABLE_CONNECTION_MS = 5_000;

/** Delay in milliseconds for the given zero-based retry index, clamped to the schedule's bounds. */
function lspBackoffDelay(attempt: number): number {
  const index = Math.min(Math.max(attempt, 0), LSP_BACKOFF_DELAYS_MS.length - 1);
  // `index` is always in bounds; the `?? ` fallback only satisfies
  // noUncheckedIndexedAccess and is never reached (tuple index 0 is defined).
  return LSP_BACKOFF_DELAYS_MS[index] ?? LSP_BACKOFF_DELAYS_MS[0];
}

/** Opaque handle returned by {@link connectLspWithBackoff}. Call `dispose()` to stop reconnects
 * and close any open WebSocket immediately. */
export interface LspConnection {
  dispose(): void;
  /**
   * Registers a callback that fires each time the connection opens a new
   * WebSocket. The callback receives the open socket so callers can piggyback
   * custom requests on the same connection.
   *
   * Only the most recently registered callback is retained. Pass `undefined`
   * to clear a previously registered callback.
   */
  onConnected(cb: ((ws: WebSocket) => void) | undefined): void;
  /**
   * Registers a callback that fires each time the WebSocket closes (including
   * mid-session drops before the retry fires). Called before any reconnect
   * attempt begins.
   *
   * Only the most recently registered callback is retained. Pass `undefined`
   * to clear a previously registered callback.
   */
  onDisconnected(cb: (() => void) | undefined): void;
}

/**
 * Open a WebSocket to `url`, wrap it with `WebSocketTransport.fromWebSocket`, and hand it to a
 * `MonacoLspClient`. On close, dispose the client and reconnect with capped exponential backoff.
 * Reconnect is indefinite — an outage recovers once the server returns. `dispose()` stops the
 * reconnect timers, disposes the client, and closes any open socket.
 */
export function connectLspWithBackoff(url: string): LspConnection {
  let disposed = false;
  let retryTimer: ReturnType<typeof setTimeout> | null = null;
  let stableTimer: ReturnType<typeof setTimeout> | null = null;
  let currentWs: WebSocket | null = null;
  let client: MonacoLspClient | null = null;
  let attempt = 0;
  let connectedCallback: ((ws: WebSocket) => void) | undefined;
  let disconnectedCallback: (() => void) | undefined;

  /**
   * Dispose the live client and drop the reference. This removes its global Monaco registrations
   * (provider + per-model listeners) that closing the socket alone leaves attached — without it,
   * each reconnect would leak a client and a dead semantic-token provider would freeze highlighting.
   */
  function disposeClient(): void {
    if (client !== null) {
      client.dispose();
      client = null;
    }
  }

  function tryConnect(): void {
    if (disposed) return;

    let ws: WebSocket;
    try {
      ws = new WebSocket(url);
    } catch {
      // Malformed URL or non-browser environment — don't retry.
      return;
    }
    currentWs = ws;

    ws.addEventListener("error", () => {
      // `onclose` drives retry; swallow the error here to keep the console clean.
    });

    ws.addEventListener("open", () => {
      if (disposed) {
        ws.close();
        return;
      }
      const transport = WebSocketTransport.fromWebSocket(ws);
      // Belt-and-suspenders: the close handler already disposed any prior client; ensure it.
      disposeClient();
      // Constructing the client runs initialize → initialized and registers all LSP features
      // (semantic tokens, diagnostics, completions, …) against the global Monaco registry.
      client = new MonacoLspClient(transport);
      // Let the caller attach custom-request watchers to the open socket.
      connectedCallback?.(ws);
      // Reset the backoff only once the connection proves stable (see STABLE_CONNECTION_MS).
      stableTimer = setTimeout(() => {
        attempt = 0;
        stableTimer = null;
      }, STABLE_CONNECTION_MS);
    });

    ws.addEventListener("close", () => {
      currentWs = null;
      if (stableTimer !== null) {
        clearTimeout(stableTimer);
        stableTimer = null;
      }
      disposeClient();
      // Let the caller dispose watchers on this socket (e.g. the stale query-kind verdict) before
      // any reconnect begins.
      if (!disposed) disconnectedCallback?.();
      if (disposed) return;
      // Reconnect indefinitely (backoff caps at 30 s); disposeClient above bounds the leak.
      const delay = lspBackoffDelay(attempt);
      attempt++;
      retryTimer = setTimeout(tryConnect, delay);
    });
  }

  tryConnect();

  return {
    onConnected(cb: ((ws: WebSocket) => void) | undefined): void {
      connectedCallback = cb;
    },
    onDisconnected(cb: (() => void) | undefined): void {
      disconnectedCallback = cb;
    },
    dispose(): void {
      disposed = true;
      connectedCallback = undefined;
      disconnectedCallback = undefined;
      if (retryTimer !== null) {
        clearTimeout(retryTimer);
        retryTimer = null;
      }
      if (stableTimer !== null) {
        clearTimeout(stableTimer);
        stableTimer = null;
      }
      disposeClient();
      if (currentWs !== null) {
        currentWs.close();
        currentWs = null;
      }
    },
  };
}
