/**
 * localStorage-backed query history with shell-style ArrowUp/ArrowDown
 * navigation semantics (Neo4j Browser parity):
 *
 * - Entries are stored oldest -> newest; consecutive duplicates are not
 *   re-recorded; blank queries are never recorded.
 * - Navigation starts from the "live draft" (whatever is currently typed).
 *   Stepping back off the draft saves it; stepping forward past the newest
 *   history entry restores it.
 * - Any user edit ends navigation: the edited buffer becomes the new live
 *   draft (see `endNavigation`).
 *
 * Storage is injected so the class is unit-testable without a DOM; the
 * editor passes `window.localStorage` (when available).
 */

import { z } from "zod";

/**
 * Persisted history is an array of query strings. Non-string entries are
 * dropped (not rejected) so a single corrupt element can't discard the whole
 * history; a non-array payload fails the parse and yields empty history.
 */
const historyEntriesSchema = z
  .array(z.unknown())
  .transform((entries) => entries.filter((entry): entry is string => typeof entry === "string"));

/** The subset of the Web Storage API the history needs. */
export interface HistoryStorage {
  getItem(key: string): string | null;
  setItem(key: string, value: string): void;
}

/** Maximum number of entries retained (oldest are dropped first). */
export const DEFAULT_HISTORY_LIMIT = 100;

/**
 * Navigation state. Either on the live draft, or stepping through history at
 * `cursor` with the draft preserved — `cursor` and `draft` are only meaningful
 * together, so they travel as one variant.
 */
type NavState =
  | { kind: "live" }
  | { kind: "navigating"; cursor: number; draft: string };

export class QueryHistory {
  private entries: string[] = [];
  private nav: NavState = { kind: "live" };

  constructor(
    private readonly storageKey: string,
    private readonly storage: HistoryStorage | null,
    private readonly limit: number = DEFAULT_HISTORY_LIMIT,
  ) {
    this.entries = this.load();
  }

  /** All entries, oldest first. */
  list(): readonly string[] {
    return this.entries;
  }

  /** True while ArrowUp/Down navigation is in progress. */
  get isNavigating(): boolean {
    return this.nav.kind === "navigating";
  }

  /**
   * Record an executed query. Blank queries and consecutive duplicates are
   * ignored. Recording always ends any in-progress navigation.
   */
  push(query: string): void {
    this.endNavigation();
    if (query.trim() === "") return;
    if (this.entries[this.entries.length - 1] === query) return;
    this.entries.push(query);
    if (this.entries.length > this.limit) {
      this.entries.splice(0, this.entries.length - this.limit);
    }
    this.persist();
  }

  /**
   * Step to an older entry (ArrowUp). `currentValue` is the live buffer
   * content; it is preserved as the draft when navigation begins.
   *
   * Returns the entry to display, or `null` when there is nothing older
   * (the caller should leave the buffer untouched).
   */
  previous(currentValue: string): string | null {
    if (this.entries.length === 0) return null;
    // On the live draft the cursor sits one past the newest entry; stepping
    // back lands on the newest. While navigating it steps one entry older.
    const cursor = this.nav.kind === "navigating" ? this.nav.cursor : this.entries.length;
    if (cursor === 0) return null; // already pinned at the oldest entry
    const draft = this.nav.kind === "navigating" ? this.nav.draft : currentValue;
    const nextCursor = cursor - 1;
    this.nav = { kind: "navigating", cursor: nextCursor, draft };
    return this.entries[nextCursor] ?? null;
  }

  /**
   * Step to a newer entry (ArrowDown). Stepping forward past the newest
   * entry ends navigation and returns the preserved draft.
   *
   * Returns the text to display, or `null` when not navigating (the caller
   * should leave the buffer untouched).
   */
  next(): string | null {
    if (this.nav.kind !== "navigating") return null;
    if (this.nav.cursor >= this.entries.length - 1) {
      const draft = this.nav.draft;
      this.endNavigation();
      return draft;
    }
    const nextCursor = this.nav.cursor + 1;
    this.nav = { kind: "navigating", cursor: nextCursor, draft: this.nav.draft };
    return this.entries[nextCursor] ?? null;
  }

  /**
   * End navigation and drop the saved draft. Called when the user edits the
   * buffer (the edit becomes the new live draft) and when a query runs.
   */
  endNavigation(): void {
    this.nav = { kind: "live" };
  }

  private load(): string[] {
    if (this.storage === null) return [];
    try {
      const raw = this.storage.getItem(this.storageKey);
      if (raw === null) return [];
      const parsed = historyEntriesSchema.safeParse(JSON.parse(raw));
      if (!parsed.success) return [];
      return parsed.data.slice(-this.limit);
    } catch {
      return []; // corrupt storage is treated as empty history
    }
  }

  private persist(): void {
    if (this.storage === null) return;
    try {
      this.storage.setItem(this.storageKey, JSON.stringify(this.entries));
    } catch {
      // Quota exceeded / storage unavailable: history degrades to in-memory.
    }
  }
}
