import { describe, expect, it } from "vitest";

import { escapeSnippet, ghostCompletion, suggestionText, type QuerySuggestion } from "../src/suggestion.js";

const raw: QuerySuggestion = {
  before: "MATCH (n) WHERE id(n) = $data.",
  fill: "`id(p)`",
  after: " RETURN n",
};

const unfilled: QuerySuggestion = { ...raw, fill: "" };

describe("ghostCompletion", () => {
  it("offers the whole suggestion as a snippet in an empty buffer, tab stop on the fill", () => {
    expect(ghostCompletion("", "", raw)).toEqual({
      insertText: { snippet: "MATCH (n) WHERE id(n) = \\$data.${1:`id(p)`} RETURN n" },
      replaceColumns: 0,
    });
  });

  it("uses a bare tab stop when there is nothing to pre-fill", () => {
    expect(ghostCompletion("", "", unfilled)).toEqual({
      insertText: { snippet: "MATCH (n) WHERE id(n) = \\$data.$1 RETURN n" },
      replaceColumns: 0,
    });
  });

  it("continues a typed prefix, replacing the typed part of the line", () => {
    expect(ghostCompletion("MATCH (n) WHERE", "", raw)).toEqual({
      insertText: { snippet: "MATCH (n) WHERE id(n) = \\$data.${1:`id(p)`} RETURN n" },
      replaceColumns: "MATCH (n) WHERE".length,
    });
  });

  it("offers plain text once the user has typed into the fill", () => {
    const typed = "MATCH (n) WHERE id(n) = $data.`id";
    expect(ghostCompletion(typed, "", raw)).toEqual({
      insertText: suggestionText(raw),
      replaceColumns: typed.length,
    });
  });

  it("confines the replaced range to the cursor's line", () => {
    const multi: QuerySuggestion = {
      before: "UNWIND [",
      fill: "$a, $b",
      after: "] AS nodeId\nMATCH (n) WHERE id(n) = nodeId\nRETURN n",
    };
    const typed = "UNWIND [$a, $b] AS nodeId\nMATCH";
    expect(ghostCompletion(typed, "", multi)).toEqual({
      insertText: "MATCH (n) WHERE id(n) = nodeId\nRETURN n",
      replaceColumns: "MATCH".length,
    });
  });

  it("shows nothing when the typed text diverges, is complete, or has text after the cursor", () => {
    expect(ghostCompletion("MATCH (x)", "", raw)).toBeNull();
    expect(ghostCompletion(suggestionText(raw), "", raw)).toBeNull();
    expect(ghostCompletion("MATCH", " (n)", raw)).toBeNull();
    expect(ghostCompletion("", "", null)).toBeNull();
  });
});

describe("escapeSnippet", () => {
  it("makes snippet syntax characters literal", () => {
    expect(escapeSnippet("$data.`id(p)` } \\")).toBe("\\$data.`id(p)` \\} \\\\");
  });
});
