import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { isQueryKind, watchQueryKind, type QueryKind } from "../src/queryKind.js";

// ---------------------------------------------------------------------------
// Minimal WebSocket stub
// ---------------------------------------------------------------------------

interface StubWs {
  readyState: number;
  onmessage: ((e: { data: string }) => void) | null;
  sentMessages: string[];
  send(data: string): void;
  simulateMessage(data: string): void;
}

function makeWs(): StubWs {
  const ws: StubWs = {
    readyState: WebSocket.OPEN,
    onmessage: null,
    sentMessages: [],
    send(data: string) {
      this.sentMessages.push(data);
    },
    simulateMessage(data: string) {
      this.onmessage?.({ data });
    },
  };
  return ws;
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function lastSentId(ws: StubWs): string {
  const last = ws.sentMessages.at(-1);
  if (!last) throw new Error("no messages sent");
  return JSON.parse(last).id as string;
}

function respondWith(ws: StubWs, id: string, kind: QueryKind): void {
  ws.simulateMessage(JSON.stringify({ jsonrpc: "2.0", id, result: { kind } }));
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("watchQueryKind", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });
  afterEach(() => {
    vi.useRealTimers();
  });

  it("fires 'unknown' immediately on attach before any server response", () => {
    const ws = makeWs();
    const calls: QueryKind[] = [];
    const watcher = watchQueryKind(ws as unknown as WebSocket, "file:///q.cypher", (k) =>
      calls.push(k),
    );

    expect(calls).toEqual(["unknown"]);
    watcher.dispose();
  });

  it("sends an initial quine/queryKind request after the initial delay", () => {
    const ws = makeWs();
    const watcher = watchQueryKind(
      ws as unknown as WebSocket,
      "file:///q.cypher",
      () => {},
      350,
    );

    expect(ws.sentMessages).toHaveLength(0);
    vi.advanceTimersByTime(600); // INITIAL_DELAY_MS
    expect(ws.sentMessages).toHaveLength(1);
    const msg = JSON.parse(ws.sentMessages[0] ?? "");
    expect(msg.method).toBe("quine/queryKind");
    expect(msg.params.textDocument.uri).toBe("file:///q.cypher");
    expect(typeof msg.id).toBe("string");
    expect(msg.id).toMatch(/^qk-/);

    watcher.dispose();
  });

  it.each<QueryKind>(["node", "table", "sideEffects"])(
    "fires the '%s' verdict when the server responds to the initial request",
    (kind) => {
      const ws = makeWs();
      const calls: QueryKind[] = [];
      const watcher = watchQueryKind(ws as unknown as WebSocket, "file:///q.cypher", (k) =>
        calls.push(k),
      );

      vi.advanceTimersByTime(600);
      respondWith(ws, lastSentId(ws), kind);

      expect(calls).toEqual(["unknown", kind]);
      watcher.dispose();
    },
  );

  it("retries the initial classification when a cold server first answers 'unknown'", () => {
    const ws = makeWs();
    const calls: QueryKind[] = [];
    const watcher = watchQueryKind(ws as unknown as WebSocket, "file:///q.cypher", (k) =>
      calls.push(k),
    );

    // First attempt at the initial delay: the cold server answers "unknown" (didOpen not yet
    // applied), which must not end the classification.
    vi.advanceTimersByTime(600);
    expect(ws.sentMessages).toHaveLength(1);
    respondWith(ws, lastSentId(ws), "unknown");
    expect(calls).toEqual(["unknown"]);

    // A retry fires after the backoff; now the server has the document and answers "node".
    vi.advanceTimersByTime(400); // INITIAL_RETRY_MS
    expect(ws.sentMessages).toHaveLength(2);
    respondWith(ws, lastSentId(ws), "node");
    expect(calls).toEqual(["unknown", "node"]);

    // Once a real verdict lands the retry loop stops — no further initial requests.
    vi.advanceTimersByTime(2000);
    expect(ws.sentMessages).toHaveLength(2);

    watcher.dispose();
  });

  it("stops retrying the initial classification once a real verdict lands on the first shot", () => {
    const ws = makeWs();
    const watcher = watchQueryKind(ws as unknown as WebSocket, "file:///q.cypher", () => {});

    vi.advanceTimersByTime(600);
    respondWith(ws, lastSentId(ws), "node");

    // No retries after a non-unknown verdict.
    vi.advanceTimersByTime(2000);
    expect(ws.sentMessages).toHaveLength(1);

    watcher.dispose();
  });

  it("bounds the initial-classification retries when the server never answers", () => {
    const ws = makeWs();
    const watcher = watchQueryKind(ws as unknown as WebSocket, "file:///q.cypher", () => {});

    // Never respond: the loop retries at the backoff interval but caps out rather than polling
    // forever (INITIAL_MAX_ATTEMPTS = 5).
    vi.advanceTimersByTime(600 + 400 * 10);
    expect(ws.sentMessages).toHaveLength(5);

    watcher.dispose();
  });

  it("debounces content changes and sends a new request after the interval", () => {
    const ws = makeWs();
    const watcher = watchQueryKind(
      ws as unknown as WebSocket,
      "file:///q.cypher",
      () => {},
      350,
    );

    // Let the initial request fire.
    vi.advanceTimersByTime(600);
    const countAfterInitial = ws.sentMessages.length;

    // Trigger multiple rapid content changes after the initial request.
    watcher.onContentChange("file:///q.cypher");
    watcher.onContentChange("file:///q.cypher");
    watcher.onContentChange("file:///q.cypher");

    // Before the debounce interval elapses — no new request yet.
    vi.advanceTimersByTime(200);
    expect(ws.sentMessages.length).toBe(countAfterInitial);

    // After the debounce interval — exactly one new request.
    vi.advanceTimersByTime(200); // total 400 ms since last change
    expect(ws.sentMessages.length).toBe(countAfterInitial + 1);

    watcher.dispose();
  });

  it("latest-wins: only the response matching the latest request fires the callback", () => {
    const ws = makeWs();
    const calls: QueryKind[] = [];
    const watcher = watchQueryKind(ws as unknown as WebSocket, "file:///q.cypher", (k) =>
      calls.push(k),
    );

    vi.advanceTimersByTime(600);
    const id1 = lastSentId(ws);

    // Trigger a second request before responding to the first.
    watcher.onContentChange("file:///q.cypher");
    vi.advanceTimersByTime(350);
    const id2 = lastSentId(ws);

    // Respond to the first (now stale) request — must be ignored.
    respondWith(ws, id1, "node");
    expect(calls).toEqual(["unknown"]); // no change yet

    // Respond to the second (latest) request — must fire.
    respondWith(ws, id2, "table");
    expect(calls).toEqual(["unknown", "table"]);

    watcher.dispose();
  });

  it("fires only on verdict transitions, not on repeat identical verdicts", () => {
    const ws = makeWs();
    const calls: QueryKind[] = [];
    const watcher = watchQueryKind(ws as unknown as WebSocket, "file:///q.cypher", (k) =>
      calls.push(k),
    );

    vi.advanceTimersByTime(600);
    respondWith(ws, lastSentId(ws), "node");
    expect(calls).toEqual(["unknown", "node"]);

    // Second request returns the same verdict — must not fire again.
    watcher.onContentChange("file:///q.cypher");
    vi.advanceTimersByTime(350);
    respondWith(ws, lastSentId(ws), "node");
    expect(calls).toEqual(["unknown", "node"]); // still just 2 entries

    watcher.dispose();
  });

  it("fires on genuine verdict changes (node → table → unknown)", () => {
    const ws = makeWs();
    const calls: QueryKind[] = [];
    const watcher = watchQueryKind(ws as unknown as WebSocket, "file:///q.cypher", (k) =>
      calls.push(k),
    );

    vi.advanceTimersByTime(600);
    respondWith(ws, lastSentId(ws), "node");

    watcher.onContentChange("file:///q.cypher");
    vi.advanceTimersByTime(350);
    respondWith(ws, lastSentId(ws), "table");

    watcher.onContentChange("file:///q.cypher");
    vi.advanceTimersByTime(350);
    respondWith(ws, lastSentId(ws), "unknown");

    expect(calls).toEqual(["unknown", "node", "table", "unknown"]);
    watcher.dispose();
  });

  it("emits 'unknown' on dispose if the last known verdict was non-unknown", () => {
    const ws = makeWs();
    const calls: QueryKind[] = [];
    const watcher = watchQueryKind(ws as unknown as WebSocket, "file:///q.cypher", (k) =>
      calls.push(k),
    );

    vi.advanceTimersByTime(600);
    respondWith(ws, lastSentId(ws), "node");
    expect(calls).toEqual(["unknown", "node"]);

    watcher.dispose();
    expect(calls).toEqual(["unknown", "node", "unknown"]);
  });

  it("does not emit 'unknown' on dispose if already unknown", () => {
    const ws = makeWs();
    const calls: QueryKind[] = [];
    const watcher = watchQueryKind(ws as unknown as WebSocket, "file:///q.cypher", (k) =>
      calls.push(k),
    );

    // Never respond — still "unknown".
    watcher.dispose();
    // Should still be just the initial "unknown", no duplicate.
    expect(calls).toEqual(["unknown"]);
  });

  it("restores the original onmessage handler on dispose", () => {
    const ws = makeWs();
    const original = vi.fn();
    ws.onmessage = original as unknown as StubWs["onmessage"];

    const watcher = watchQueryKind(ws as unknown as WebSocket, "file:///q.cypher", () => {});
    expect(ws.onmessage).not.toBe(original); // wrapped

    watcher.dispose();
    expect(ws.onmessage).toBe(original); // restored
  });

  it("forwards non-qk messages to the original handler", () => {
    const ws = makeWs();
    const received: string[] = [];
    ws.onmessage = ({ data }: { data: string }) => received.push(data);

    const watcher = watchQueryKind(ws as unknown as WebSocket, "file:///q.cypher", () => {});

    const lspMsg = JSON.stringify({ jsonrpc: "2.0", id: 5, result: {} });
    ws.simulateMessage(lspMsg);
    expect(received).toEqual([lspMsg]);

    watcher.dispose();
  });

  it("does not forward qk response messages to the original handler", () => {
    const ws = makeWs();
    const received: string[] = [];
    ws.onmessage = ({ data }: { data: string }) => received.push(data);

    const watcher = watchQueryKind(ws as unknown as WebSocket, "file:///q.cypher", () => {});
    vi.advanceTimersByTime(600);
    const id = lastSentId(ws);

    const qkMsg = JSON.stringify({ jsonrpc: "2.0", id, result: { kind: "node" } });
    ws.simulateMessage(qkMsg);
    expect(received).toEqual([]); // NOT forwarded

    watcher.dispose();
  });

  it("treats a qk response with no result field as 'unknown' and does not forward it", () => {
    const ws = makeWs();
    const received: string[] = [];
    ws.onmessage = ({ data }: { data: string }) => received.push(data);
    const calls: QueryKind[] = [];
    const watcher = watchQueryKind(ws as unknown as WebSocket, "file:///q.cypher", (k) =>
      calls.push(k),
    );

    vi.advanceTimersByTime(600);
    const id = lastSentId(ws);
    // A JSON-RPC error response carries no `result` — still one of ours.
    ws.simulateMessage(JSON.stringify({ jsonrpc: "2.0", id, error: { code: -32_603 } }));

    expect(received).toEqual([]); // consumed, not forwarded
    expect(calls).toEqual(["unknown"]); // no transition off the initial unknown
    watcher.dispose();
  });

  it("forwards a qk-prefixed-id message only when its id matches; non-qk ids pass through", () => {
    const ws = makeWs();
    const received: string[] = [];
    ws.onmessage = ({ data }: { data: string }) => received.push(data);
    const watcher = watchQueryKind(ws as unknown as WebSocket, "file:///q.cypher", () => {});

    // A string id without the qk- prefix is not ours.
    const foreign = JSON.stringify({ jsonrpc: "2.0", id: "lsp-7", result: { kind: "node" } });
    ws.simulateMessage(foreign);
    expect(received).toEqual([foreign]);
    watcher.dispose();
  });

  it.each(["bogus", "sideEffects2"])(
    "treats an unrecognised kind '%s' in the server response as 'unknown'",
    (kind) => {
      const ws = makeWs();
      const calls: QueryKind[] = [];
      const watcher = watchQueryKind(ws as unknown as WebSocket, "file:///q.cypher", (k) =>
        calls.push(k),
      );

      vi.advanceTimersByTime(600);
      const id = lastSentId(ws);
      ws.simulateMessage(JSON.stringify({ jsonrpc: "2.0", id, result: { kind } }));
      // Unrecognised kinds normalise to "unknown" — no transition (already "unknown").
      expect(calls).toEqual(["unknown"]);

      watcher.dispose();
    },
  );

  it("WS close while verdict is 'node': dispose emits 'unknown' (button disables)", () => {
    // Simulates what lsp.ts does when the WebSocket closes mid-session:
    // onDisconnected fires → watcher.dispose() → consumer gets "unknown".
    const ws = makeWs();
    const calls: QueryKind[] = [];
    const watcher = watchQueryKind(ws as unknown as WebSocket, "file:///q.cypher", (k) =>
      calls.push(k),
    );

    // Establish a non-unknown verdict.
    vi.advanceTimersByTime(600);
    respondWith(ws, lastSentId(ws), "node");
    expect(calls).toEqual(["unknown", "node"]);

    // The WebSocket closes — lsp.ts calls watcher.dispose().
    watcher.dispose();
    expect(calls).toEqual(["unknown", "node", "unknown"]);
  });

  it("isQueryKind narrows the four verdict values and rejects everything else", () => {
    for (const valid of ["node", "table", "sideEffects", "unknown"]) {
      expect(isQueryKind(valid)).toBe(true);
    }
    for (const invalid of ["bogus", "NODE", "", 0, null, undefined, {}]) {
      expect(isQueryKind(invalid)).toBe(false);
    }
  });

  it("reconnect re-attaches a fresh watcher that classifies correctly", () => {
    // After a disconnect, lsp.ts creates a new watchQueryKind on the new WS.
    // The fresh watcher fires "unknown" immediately and then the server verdict.
    const ws2 = makeWs();
    const calls: QueryKind[] = [];

    // First connection: establish "node", then WS closes.
    const ws1 = makeWs();
    const watcher1 = watchQueryKind(ws1 as unknown as WebSocket, "file:///q.cypher", (k) =>
      calls.push(k),
    );
    vi.advanceTimersByTime(600);
    respondWith(ws1, lastSentId(ws1), "node");
    expect(calls).toEqual(["unknown", "node"]);
    // WS closes → dispose fires "unknown".
    watcher1.dispose();
    expect(calls).toEqual(["unknown", "node", "unknown"]);

    // Reconnect: lsp.ts calls watchQueryKind again on the new socket.
    const watcher2 = watchQueryKind(ws2 as unknown as WebSocket, "file:///q.cypher", (k) =>
      calls.push(k),
    );
    // New watcher fires "unknown" immediately.
    expect(calls).toEqual(["unknown", "node", "unknown", "unknown"]);

    // Server re-classifies via the new connection.
    vi.advanceTimersByTime(600);
    respondWith(ws2, lastSentId(ws2), "table");
    expect(calls).toEqual(["unknown", "node", "unknown", "unknown", "table"]);

    watcher2.dispose();
  });
});
