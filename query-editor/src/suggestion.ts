/**
 * A whole-query suggestion shown as ghost text after the cursor and accepted
 * with Tab, with one spot the user is expected to fill in.
 *
 * The consumer describes the suggestion in three parts: the text before the
 * fill-in spot, what to put there (possibly nothing), and the text after it.
 * Accepting inserts all three and leaves the cursor at the spot, with the
 * pre-filled value selected so it can be typed over or kept.
 *
 * Only the shape decision lives here, kept free of Monaco so it can be tested
 * on strings: given what is typed before and after the cursor, is there
 * something to show, and what single-line edit expresses it?
 */
export interface QuerySuggestion {
  readonly before: string;
  readonly fill: string;
  readonly after: string;
}

/** The suggestion as the plain text accepting it produces. */
export function suggestionText(suggestion: QuerySuggestion): string {
  return suggestion.before + suggestion.fill + suggestion.after;
}

/**
 * The inline completion to offer, in Monaco's terms: replace the last
 * `replaceColumns` characters of the cursor's line with `insertText`, which
 * starts with those same characters and continues with the rest of the
 * suggestion. Monaco renders the continuation as ghost text. A snippet form
 * carries the fill-in spot as a tab stop.
 */
export interface GhostCompletion {
  readonly insertText: string | { readonly snippet: string };
  readonly replaceColumns: number;
}

/**
 * The completion to offer for `suggestion`, or `null` when there is nothing to
 * show: the cursor is not at the end of the buffer, what is typed is not a
 * prefix of the suggestion, or the suggestion is already typed out in full.
 *
 * While the typed prefix ends before the fill-in spot the completion is a
 * snippet whose tab stop is that spot; once the user has typed into or past
 * it there is no spot left to land on, and the rest is offered as plain text.
 *
 * The replaced range is confined to the cursor's line because Monaco requires
 * a single-line range; the text of earlier lines is part of the matched prefix
 * but is left in place.
 */
export function ghostCompletion(
  typedBefore: string,
  typedAfter: string,
  suggestion: QuerySuggestion | null,
): GhostCompletion | null {
  if (suggestion === null || typedAfter !== "") return null;
  const text = suggestionText(suggestion);
  if (typedBefore === text || !text.startsWith(typedBefore)) return null;
  const lineStart = typedBefore.lastIndexOf("\n") + 1;
  const replaceColumns = typedBefore.length - lineStart;
  if (typedBefore.length > suggestion.before.length) {
    return { insertText: text.slice(lineStart), replaceColumns };
  }
  const head = escapeSnippet(text.slice(lineStart, suggestion.before.length));
  const stop = suggestion.fill === "" ? "$1" : `\${1:${escapeSnippet(suggestion.fill)}}`;
  return {
    insertText: { snippet: head + stop + escapeSnippet(suggestion.after) },
    replaceColumns,
  };
}

/**
 * Text made literal for Monaco's snippet syntax, where `$` introduces a tab
 * stop or variable, `}` closes one, and `\` escapes. Without this a Cypher
 * parameter such as `$data` would be read as a snippet variable and vanish.
 */
export function escapeSnippet(text: string): string {
  return text.replace(/[\\$}]/g, (c) => `\\${c}`);
}
