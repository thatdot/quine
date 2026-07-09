import * as monaco from "monaco-editor/esm/vs/editor/editor.api.js";

/** Theme name registered by {@link defineQueryEditorTheme}. */
export const QUERY_EDITOR_THEME = "thatdot-query-light";

/**
 * The query-editor light-theme palette, mirroring Neo4j Browser's light-theme
 * token colors:
 *
 * - keywords + operators/punctuation: green      rgb(63,120,36)   #3F7824
 * - node labels / relationship types: red-orange rgb(212,51,0)    #D43300
 * - property keys:                    maroon     rgb(115,14,0)    #730E00
 * - strings:                          amber      rgb(152,100,0)   #986400
 * - numbers:                          purple     rgb(117,78,200)  #754EC8
 * - comments:                         gray
 * - variables:                        deliberately uncolored
 *
 * Monarch rules cover what the built-in Cypher grammar tokenizes. Semantic
 * token rules cover the full 22-type legend from the Quine language server —
 * keyed by the exact type names the server advertises in its `initialize`
 * response. Monaco's `getTokenStyleMetadata` resolves these names through the
 * theme's rule trie, producing zero-conversion coloring.
 *
 * Semantic highlighting is opt-in per editor instance: call
 * `editor.updateOptions({ "semanticHighlighting.enabled": true })` after
 * construction. The theme itself cannot set the flag because it has no
 * reference to an editor instance.
 */
export function defineQueryEditorTheme(): void {
  monaco.editor.defineTheme(QUERY_EDITOR_THEME, {
    base: "vs",
    inherit: true,
    rules: [
      // --- Monarch rules (Cypher grammar shipped in monaco-editor) ---
      // Keywords and operators/punctuation share Neo4j's green.
      { token: "keyword", foreground: "3F7824" },
      { token: "delimiter", foreground: "3F7824" },
      // Built-in functions and literals (true/false/null) grouped with
      // keywords — provisional, pending the palette decision.
      { token: "predefined.function", foreground: "3F7824" },
      { token: "predefined.literal", foreground: "3F7824" },
      // `(n:Label)` / `-[:REL_TYPE]` — the Monarch grammar tokenizes both
      // as `type.identifier`.
      { token: "type.identifier", foreground: "D43300" },
      { token: "string", foreground: "986400" },
      { token: "string.invalid", foreground: "986400" },
      { token: "number", foreground: "754EC8" },
      { token: "comment", foreground: "737373" },
      // Variables/identifiers: no rule — deliberately uncolored so schema
      // tokens pop (Neo4j principle).

      // --- Semantic token rules (Quine LSP 22-type legend) ---
      // Monaco's standalone theme service resolves semantic token type names
      // through the same TextMate rule trie as Monarch tokens. Adding a rule
      // keyed by the exact legend name (e.g. `MatchKeyword`) is sufficient —
      // no format conversion needed. Palette mirrors Neo4j principles above.

      // Keyword tokens → green
      { token: "MatchKeyword", foreground: "3F7824" },
      { token: "ReturnKeyword", foreground: "3F7824" },
      { token: "AsKeyword", foreground: "3F7824" },
      { token: "WhereKeyword", foreground: "3F7824" },
      { token: "CreateKeyword", foreground: "3F7824" },
      { token: "AndKeyword", foreground: "3F7824" },
      // Operators → green (same as delimiters)
      { token: "AssignmentOperator", foreground: "3F7824" },
      { token: "AdditionOperator", foreground: "3F7824" },
      // Schema tokens: labels/rel-types → red-orange; properties → maroon
      { token: "NodeLabel", foreground: "D43300" },
      { token: "EdgeLabel", foreground: "D43300" },
      { token: "Property", foreground: "730E00" },
      // Edge structure → red-orange (same visual weight as labels)
      { token: "Edge", foreground: "D43300" },
      // Literals
      { token: "StringLiteral", foreground: "986400" },
      { token: "IntLiteral", foreground: "754EC8" },
      { token: "DoubleLiteral", foreground: "754EC8" },
      { token: "BooleanLiteral", foreground: "3F7824" },
      { token: "NullLiteral", foreground: "3F7824" },
      // Function calls → green (grouped with keywords)
      { token: "FunctionApplication", foreground: "3F7824" },
      // Parameters → purple (marks injection points, same visual weight as numbers)
      { token: "Parameter", foreground: "754EC8" },
      // Variables: NodeVariable, Variable, PatternVariable — no rule,
      // deliberately uncolored so schema tokens pop (Neo4j principle).
    ],
    colors: {},
  });
}
