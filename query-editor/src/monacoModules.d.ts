/**
 * Type declarations for the deep `monaco-editor/...` imports that ship without
 * their own `.d.ts`. These paths are internal and can move between Monaco
 * minors — the narrow `^0.56` peer range is the guard (see README). Specifiers
 * are rooted at `esm/vs/` by Monaco's `exports` map, so they omit that prefix.
 */
declare module "monaco-editor/languages/definitions/cypher/cypher.js" {
  import type { languages } from "monaco-editor/editor/editor.api.js";

  export const conf: languages.LanguageConfiguration;
  export const language: languages.IMonarchLanguage;
}
