// Patches monaco-editor 0.55.x's vendored monaco-lsp-client
// (esm/external/monaco-lsp-client/out/index.js), the native LSP client this editor uses. The
// webpack rule scopes this loader to that one file. Two patches, each asserting its anchor so a
// monaco-editor bump that moves the code fails the build loudly instead of silently reverting:
//
// 1. Strip the stray `debugger;` in the capability-registration path, which halts language-server
//    startup when DevTools is open. (Terser drops it in production; this covers dev builds.)
//
// 2. Make MonacoLspClient disposable. Its constructor discards the DisposableStore that
//    createFeatures() returns, and the class exposes no dispose() — so its ~22 global Monaco
//    provider registrations and the TextDocumentSynchronizer's per-model listeners outlive the
//    WebSocket, leaking on every reconnect (and a dead semantic-token provider freezes
//    highlighting). We retain the store and inject a dispose() that tears down everything the
//    client registered. The deficiency is present on monaco-editor `main` too, so a version bump
//    is not a fix. lsp.ts calls dispose() before reconnecting.

const CREATE_FEATURES_CALL = "this.createFeatures();";
const CLASS_TAIL = "    return store;\n  }\n};";

module.exports = function patchMonacoLspClient(source) {
  let patched = source.replace(/^[ \t]*debugger;[ \t]*\r?\n/gm, "");

  if (!patched.includes(CREATE_FEATURES_CALL)) {
    throw new Error(
      "patch-monaco-lsp-client-loader: could not find 'this.createFeatures();' in monaco-lsp-client " +
        "(monaco-editor bump?). Refusing to build without applying the MonacoLspClient dispose() " +
        "patch, which would silently reintroduce the LSP connection leak.",
    );
  }
  // Retain the DisposableStore that createFeatures() returns (upstream throws it away).
  patched = patched.replace(CREATE_FEATURES_CALL, "this._featureStore = this.createFeatures();");

  if (!patched.includes(CLASS_TAIL)) {
    throw new Error(
      "patch-monaco-lsp-client-loader: could not find the MonacoLspClient class tail to inject " +
        "dispose() (monaco-editor bump?). Refusing to build without the dispose() patch, which " +
        "would silently reintroduce the LSP connection leak.",
    );
  }
  // Inject dispose() before the class closes: `_featureStore` removes the provider registrations,
  // `_bridge` (the TextDocumentSynchronizer) the model listeners, `_capabilitiesRegistry` the
  // capability handlers. Each is guarded — private internals of an alpha client with no contract.
  const disposeInjection =
    "    return store;\n" +
    "  }\n" +
    "  dispose() {\n" +
    "    if (this._featureStore) this._featureStore.dispose();\n" +
    "    if (this._bridge) this._bridge.dispose();\n" +
    "    if (this._capabilitiesRegistry) this._capabilitiesRegistry.dispose();\n" +
    "  }\n" +
    "};";
  patched = patched.replace(CLASS_TAIL, disposeInjection);

  return patched;
};
