import * as monaco from "monaco-editor/esm/vs/editor/editor.api.js";
// Deep import of the Monarch grammar only: its `.contribution.js` sibling
// drags in `editor.all` (+270 KB gzip). Deep `esm/vs/` paths are unstable
// across Monaco minors — the narrow ^0.55 peer range is the guard.
import { conf, language } from "monaco-editor/esm/vs/basic-languages/cypher/cypher.js";

export const CYPHER_LANGUAGE_ID = "cypher";

/**
 * Register the Cypher language id and its configuration (brackets, comments) with Monaco.
 * Idempotent: Monaco's language registry is the source of truth, so a repeat call — e.g. a second
 * editor on the page — is a no-op.
 */
export function registerCypherLanguage(): void {
  if (monaco.languages.getLanguages().some((l) => l.id === CYPHER_LANGUAGE_ID)) return;
  monaco.languages.register({
    id: CYPHER_LANGUAGE_ID,
    extensions: [".cypher"],
    aliases: ["Cypher", "cypher"],
  });
  monaco.languages.setLanguageConfiguration(CYPHER_LANGUAGE_ID, conf);
}

/**
 * Install the client-side Monarch tokenizer as Cypher's highlighter. Re-installing replaces the
 * provider, so calling it more than once is harmless. The caller skips it in the editor's
 * `qp.enabled` mode, where the language server's semantic tokens are the sole highlighter.
 */
export function installCypherMonarchTokenizer(): void {
  monaco.languages.setMonarchTokensProvider(CYPHER_LANGUAGE_ID, language);
}
