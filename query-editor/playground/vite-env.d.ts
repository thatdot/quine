/// <reference types="vite/client" />

interface ImportMetaEnv {
  /** Quine LSP WebSocket (Phase 2) — set via .env, see .env.example. */
  readonly VITE_LS_WEBSOCKET_URI: string | undefined;
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}
