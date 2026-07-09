/**
 * The Quine LSP semantic-token type names — the TypeScript mirror of the
 * `SemanticType` cases the language server advertises (in quine-plus
 * `public/quine-language/src/main/scala/com/thatdot/quine/language/semantic/Semantics.scala`).
 *
 * The editor reads the authoritative legend, including its index ordering, from
 * the server's LSP `initialize` result at runtime. This list mirrors the names
 * so the `QueryEditorParityTest` munit test fails the build if the editor and
 * server drift apart.
 */
export const semanticTokenLegend: ReadonlyArray<{
  /** Token-type name exactly as advertised in the server's legend. */
  name: string;
}> = [
  { name: "MatchKeyword" },
  { name: "ReturnKeyword" },
  { name: "AsKeyword" },
  { name: "WhereKeyword" },
  { name: "CreateKeyword" },
  { name: "AndKeyword" },
  { name: "PatternVariable" },
  { name: "AssignmentOperator" },
  { name: "AdditionOperator" },
  { name: "NodeLabel" },
  { name: "EdgeLabel" },
  { name: "NodeVariable" },
  { name: "Variable" },
  { name: "Edge" },
  { name: "FunctionApplication" },
  { name: "Parameter" },
  { name: "StringLiteral" },
  { name: "NullLiteral" },
  { name: "BooleanLiteral" },
  { name: "IntLiteral" },
  { name: "DoubleLiteral" },
  { name: "Property" },
];
