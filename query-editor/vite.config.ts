import { fileURLToPath } from "node:url";

import { defineConfig } from "vite";

// Vite serves the dev playground only (index.html + playground/).
// It is NOT the library build (that's plain tsc, see tsconfig.build.json)
// and it does not validate the production bundler of any consumer —
// consumers own their Monaco worker/CSS/font wiring.
export default defineConfig({
  worker: {
    format: "es",
  },
  resolve: {
    alias: {
      // The playground's counterpart to the `monaco-lsp-client` alias each browser module's
      // common.webpack.config.js defines — see src/lsp.ts for why the client needs one.
      "monaco-lsp-client": fileURLToPath(
        new URL("./node_modules/monaco-editor/esm/external/monaco-lsp-client/out/index.js", import.meta.url),
      ),
    },
  },
});
