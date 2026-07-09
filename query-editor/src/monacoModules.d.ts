/**
 * Type declarations for deep `monaco-editor/esm/vs/...` imports that ship
 * without their own `.d.ts`. These paths are internal and can move between
 * Monaco minors — the narrow `^0.55` peer range is the guard (see README).
 */
declare module "monaco-editor/esm/vs/basic-languages/cypher/cypher.js" {
  import type { languages } from "monaco-editor/esm/vs/editor/editor.api.js";

  export const conf: languages.LanguageConfiguration;
  export const language: languages.IMonarchLanguage;
}

/**
 * The first-party native LSP client shipped in monaco-editor ≥0.55.
 * `monaco.lsp` re-exports these symbols from `editor.main.js`; the direct
 * path below lets `queryEditor.ts` import them without pulling in the full
 * `editor.main` bundle (which adds hundreds of KB of unused contributions).
 */
declare module "monaco-editor/esm/external/monaco-lsp-client/out/index.js" {
  export class WebSocketTransport {
    static fromWebSocket(socket: WebSocket): WebSocketTransport;
    close(): void;
    dispose(): void;
  }

  export class MonacoLspClient {
    constructor(transport: WebSocketTransport);
    /**
     * Tears down every global Monaco registration the client created (the feature providers and the
     * document synchronizer's model listeners). Not shipped by upstream — injected into the vendored
     * bundle by `webpack/patch-monaco-lsp-client-loader.cjs`. Closing the WebSocket does not do this.
     */
    dispose(): void;
  }
}
