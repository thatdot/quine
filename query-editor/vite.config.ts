import { defineConfig } from "vite";

// Vite serves the dev playground only (index.html + playground/).
// It is NOT the library build (that's plain tsc, see tsconfig.build.json)
// and it does not validate the production bundler of any consumer —
// consumers own their Monaco worker/CSS/font wiring.
export default defineConfig({
  worker: {
    format: "es",
  },
});
