import { describe, expect, it } from "vitest";

import { DEFAULT_HISTORY_LIMIT, QueryHistory, type HistoryStorage } from "../src/history.js";

const memoryStorage = (initial: Record<string, string> = {}): HistoryStorage & {
  data: Map<string, string>;
} => {
  const data = new Map(Object.entries(initial));
  return {
    data,
    getItem: (key) => data.get(key) ?? null,
    setItem: (key, value) => {
      data.set(key, value);
    },
  };
};

const KEY = "test.history";

describe("QueryHistory recording", () => {
  it("records executed queries oldest-first", () => {
    const h = new QueryHistory(KEY, memoryStorage());
    h.push("MATCH (a) RETURN a");
    h.push("MATCH (b) RETURN b");
    expect(h.list()).toEqual(["MATCH (a) RETURN a", "MATCH (b) RETURN b"]);
  });

  it("ignores blank queries", () => {
    const h = new QueryHistory(KEY, memoryStorage());
    h.push("");
    h.push("   \n  ");
    expect(h.list()).toEqual([]);
  });

  it("dedupes consecutive identical queries but keeps non-consecutive repeats", () => {
    const h = new QueryHistory(KEY, memoryStorage());
    h.push("A");
    h.push("A");
    h.push("B");
    h.push("A");
    expect(h.list()).toEqual(["A", "B", "A"]);
  });

  it("drops the oldest entries beyond the limit", () => {
    const h = new QueryHistory(KEY, memoryStorage(), 3);
    for (const q of ["1", "2", "3", "4", "5"]) h.push(q);
    expect(h.list()).toEqual(["3", "4", "5"]);
  });

  it("persists to storage and reloads", () => {
    const storage = memoryStorage();
    const h1 = new QueryHistory(KEY, storage);
    h1.push("A");
    h1.push("B");
    const h2 = new QueryHistory(KEY, storage);
    expect(h2.list()).toEqual(["A", "B"]);
  });

  it("treats corrupt storage as empty history", () => {
    const h = new QueryHistory(KEY, memoryStorage({ [KEY]: "not json {" }));
    expect(h.list()).toEqual([]);
  });

  it("filters non-string entries from storage", () => {
    const h = new QueryHistory(KEY, memoryStorage({ [KEY]: '["A", 42, null, "B"]' }));
    expect(h.list()).toEqual(["A", "B"]);
  });

  it("treats a non-array JSON payload as empty history", () => {
    const h = new QueryHistory(KEY, memoryStorage({ [KEY]: '{"not":"an array"}' }));
    expect(h.list()).toEqual([]);
  });

  it("truncates oversized stored history to the limit (newest kept)", () => {
    const h = new QueryHistory(KEY, memoryStorage({ [KEY]: '["1","2","3","4"]' }), 2);
    expect(h.list()).toEqual(["3", "4"]);
  });

  it("works without storage (in-memory only)", () => {
    const h = new QueryHistory(KEY, null);
    h.push("A");
    expect(h.list()).toEqual(["A"]);
  });

  it("has a sane default limit", () => {
    expect(DEFAULT_HISTORY_LIMIT).toBeGreaterThan(0);
  });
});

describe("QueryHistory navigation", () => {
  const loaded = (): QueryHistory => {
    const h = new QueryHistory(KEY, null);
    h.push("oldest");
    h.push("middle");
    h.push("newest");
    return h;
  };

  it("returns null when stepping back with no history", () => {
    const h = new QueryHistory(KEY, null);
    expect(h.previous("draft")).toBeNull();
    expect(h.isNavigating).toBe(false);
  });

  it("steps back newest-first and stops at the oldest entry", () => {
    const h = loaded();
    expect(h.previous("draft")).toBe("newest");
    expect(h.previous("ignored")).toBe("middle");
    expect(h.previous("ignored")).toBe("oldest");
    expect(h.previous("ignored")).toBeNull(); // pinned at oldest
    expect(h.isNavigating).toBe(true);
  });

  it("returns null when stepping forward while not navigating", () => {
    const h = loaded();
    expect(h.next()).toBeNull();
  });

  it("preserves the draft when stepping off and back onto the live entry", () => {
    const h = loaded();
    expect(h.previous("my unsent draft")).toBe("newest");
    expect(h.next()).toBe("my unsent draft"); // forward past newest restores draft
    expect(h.isNavigating).toBe(false);
  });

  it("round-trips back and forward through entries", () => {
    const h = loaded();
    h.previous("draft"); // newest
    h.previous("x"); // middle
    h.previous("x"); // oldest
    expect(h.next()).toBe("middle");
    expect(h.next()).toBe("newest");
    expect(h.next()).toBe("draft");
    expect(h.next()).toBeNull();
  });

  it("only captures the draft when navigation begins", () => {
    const h = loaded();
    expect(h.previous("the real draft")).toBe("newest");
    expect(h.previous("NOT a draft")).toBe("middle");
    expect(h.next()).toBe("newest");
    expect(h.next()).toBe("the real draft");
  });

  it("endNavigation drops the draft and cursor (user edited the buffer)", () => {
    const h = loaded();
    h.previous("draft");
    h.endNavigation();
    expect(h.isNavigating).toBe(false);
    expect(h.next()).toBeNull();
    // A fresh ArrowUp starts from the newest entry again.
    expect(h.previous("new draft")).toBe("newest");
  });

  it("push during navigation resets navigation state", () => {
    const h = loaded();
    h.previous("draft"); // navigating
    h.push("ran a query");
    expect(h.isNavigating).toBe(false);
    expect(h.list()).toEqual(["oldest", "middle", "newest", "ran a query"]);
    expect(h.previous("d")).toBe("ran a query");
  });

  it("navigation works over multi-line history entries", () => {
    const h = new QueryHistory(KEY, null);
    h.push("MATCH (n)\nRETURN n");
    expect(h.previous("")).toBe("MATCH (n)\nRETURN n");
  });
});
