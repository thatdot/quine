/**
 * Sends `quine/queryKind` custom LSP requests over the same WebSocket used by
 * `MonacoLspClient`, with debouncing and latest-wins stale-response safety.
 *
 * The `MonacoLspClient` uses `StreamBasedChannel` internally, which allocates
 * numeric JSON-RPC request IDs starting at 0 and increments them per request.
 * Our requests use string IDs of the form `"qk-<n>"` — the channel stores
 * callbacks in a Map keyed by `"" + id`, so `"qk-1"` never collides with
 * `"0"`, `"1"`, etc. Responses whose IDs the channel does not recognize are
 * silently discarded (StreamBasedChannel._processResponse, line 376-383 of the
 * bundled source), so MonacoLspClient's message flow is unaffected.
 *
 * The WebSocket's `onmessage` handler is owned by `WebSocketTransport` after
 * `fromWebSocket` is called. We wrap it so our custom responses are intercepted
 * before the transport sees them; standard LSP traffic is forwarded unchanged.
 */

import { z } from "zod";

/** The four verdict values the server can return. */
export const QUERY_KINDS = ["node", "table", "sideEffects", "unknown"] as const;
export type QueryKind = (typeof QUERY_KINDS)[number];

/** Schema for a single verdict value, derived from {@link QUERY_KINDS} so the
 * runtime check and the {@link QueryKind} type stay in lock-step. */
const queryKindSchema = z.enum(QUERY_KINDS);

/** Narrows an untrusted value (e.g. a field off a parsed JSON-RPC response) to a
 * {@link QueryKind}. */
export const isQueryKind = (value: unknown): value is QueryKind =>
  queryKindSchema.safeParse(value).success;

/**
 * Schema for a `quine/queryKind` JSON-RPC response. A message is one of ours iff
 * its `id` is a string with our `"qk-"` prefix — the server's numeric LSP ids
 * never match. `result.kind` is left untrusted (`unknown`) and normalised through
 * {@link isQueryKind} at the use site, so an unrecognised verdict degrades to
 * `"unknown"` rather than failing the parse; `result` itself is optional so a
 * JSON-RPC error response (no `result`) is still recognised as ours.
 */
const queryKindResponseSchema = z.object({
  id: z.string().startsWith("qk-"),
  result: z.object({ kind: z.unknown() }).optional(),
});

/** Disposable handle returned by {@link watchQueryKind}. */
export interface QueryKindWatcher {
  /** Notify of a content change; triggers a debounced re-classification. */
  onContentChange(uri: string): void;
  /** Stop debounce timers and suppress any pending callbacks. */
  dispose(): void;
}

/**
 * Attach a query-kind watcher to an open WebSocket. The socket must already be
 * open (readyState === OPEN) and have its `onmessage` handler set by
 * `WebSocketTransport.fromWebSocket`. The watcher wraps that handler so custom
 * `quine/queryKind` responses are routed to us; all other messages are
 * forwarded to the original handler.
 *
 * `onQueryKindChange` is called with `"unknown"` immediately on attachment and
 * again whenever the verdict changes. It is called only on transitions, not
 * on every request.
 *
 * @param ws - The open WebSocket that MonacoLspClient is using.
 * @param uri - The LSP document URI for this editor's model.
 * @param onQueryKindChange - Fires on verdict transitions and on attachment.
 * @param debounceMs - Debounce interval in milliseconds (default 350 ms).
 */
export function watchQueryKind(
  ws: WebSocket,
  uri: string,
  onQueryKindChange: (kind: QueryKind) => void,
  debounceMs = 350,
): QueryKindWatcher {
  let disposed = false;
  let debounceTimer: ReturnType<typeof setTimeout> | null = null;
  // Sequence number: monotonic per request. It both mints the request id (`qk-<seq>`) and
  // gates the callback — only the latest-issued request may fire.
  let latestSeq = 0;
  let lastKind: QueryKind | null = null;

  // Emit the initial "unknown" state immediately.
  onQueryKindChange("unknown");
  lastKind = "unknown";

  // Build a pending-response Map: seq → resolve fn.
  const pending = new Map<string, (kind: QueryKind) => void>();

  // Wrap the existing onmessage handler so our custom responses are intercepted.
  const originalOnMessage = ws.onmessage;
  ws.onmessage = (event: MessageEvent): void => {
    if (typeof event.data === "string") {
      let parsed: unknown;
      try {
        parsed = JSON.parse(event.data);
      } catch {
        // Not valid JSON — forward to original handler (shouldn't happen in LSP).
        originalOnMessage?.call(ws, event);
        return;
      }
      // Check if this is a response for one of our requests.
      const response = queryKindResponseSchema.safeParse(parsed);
      if (response.success) {
        const resolver = pending.get(response.data.id);
        if (resolver) {
          pending.delete(response.data.id);
          const kind = response.data.result?.kind;
          const verdict: QueryKind = isQueryKind(kind) ? kind : "unknown";
          resolver(verdict);
        }
        // Do NOT forward to original handler — this message is ours.
        return;
      }
    }
    // Forward all non-custom-response messages to the original handler.
    originalOnMessage?.call(ws, event);
  };

  function sendRequest(seq: number): void {
    if (disposed || ws.readyState !== WebSocket.OPEN) return;
    const id = `qk-${seq}`;
    pending.set(id, (verdict: QueryKind) => {
      // Stale-response guard: only the latest-issued request may fire.
      if (seq !== latestSeq) return;
      if (verdict !== lastKind) {
        lastKind = verdict;
        onQueryKindChange(verdict);
      }
    });
    try {
      ws.send(
        JSON.stringify({
          jsonrpc: "2.0",
          id,
          method: "quine/queryKind",
          params: { textDocument: { uri } },
        }),
      );
    } catch {
      // Socket closed between the OPEN check and the send; clean up.
      pending.delete(id);
    }
  }

  function scheduleRequest(): void {
    if (disposed) return;
    if (debounceTimer !== null) {
      clearTimeout(debounceTimer);
    }
    const seq = ++latestSeq;
    debounceTimer = setTimeout(() => {
      debounceTimer = null;
      sendRequest(seq);
    }, debounceMs);
  }

  // Fire an initial (undebounced) classification so a pre-filled buffer resolves without a
  // keystroke. The initialize/didOpen handshake is async, so a cold server answers the first
  // request "unknown" (document not open yet); retry with short backoff until a real verdict lands,
  // a content change takes over, or INITIAL_MAX_ATTEMPTS is hit. Each attempt mints the latest
  // sequence so the stale-response guard does not drop its response.
  const INITIAL_DELAY_MS = 600;
  const INITIAL_RETRY_MS = 400;
  const INITIAL_MAX_ATTEMPTS = 5;
  let initialAttempts = 0;
  let initialSeqOwned = 0;
  let initialTimer: ReturnType<typeof setTimeout> | null = null;

  function scheduleInitialClassification(delayMs: number): void {
    initialTimer = setTimeout(() => {
      initialTimer = null;
      if (disposed) return;
      // A content change advanced the sequence: the debounced watcher owns the verdict now, so stop.
      if (initialAttempts > 0 && latestSeq !== initialSeqOwned) return;
      // A real verdict already landed — nothing more to do.
      if (lastKind !== null && lastKind !== "unknown") return;
      if (initialAttempts >= INITIAL_MAX_ATTEMPTS) return;
      initialAttempts += 1;
      initialSeqOwned = ++latestSeq;
      sendRequest(initialSeqOwned);
      scheduleInitialClassification(INITIAL_RETRY_MS);
    }, delayMs);
  }
  scheduleInitialClassification(INITIAL_DELAY_MS);

  return {
    onContentChange(newUri: string): void {
      if (disposed) return;
      // Update the URI in case it changed (it shouldn't, but defensive).
      uri = newUri;
      scheduleRequest();
    },
    dispose(): void {
      disposed = true;
      if (debounceTimer !== null) {
        clearTimeout(debounceTimer);
        debounceTimer = null;
      }
      if (initialTimer !== null) {
        clearTimeout(initialTimer);
        initialTimer = null;
      }
      pending.clear();
      // Restore the original onmessage handler.
      ws.onmessage = originalOnMessage;
      // Emit "unknown" on disconnect so the consumer disables the button.
      if (lastKind !== "unknown") {
        onQueryKindChange("unknown");
      }
    },
  };
}
