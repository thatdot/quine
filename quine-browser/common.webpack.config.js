const webpack = require('webpack');
const path = require("path");

module.exports = {
    // Monaco editor web worker (used by @thatdot/query-editor). Monaco runs its text-model
    // services off the main thread; the 62 KB base editor.worker suffices because language
    // smarts come from a remote language server, not in-process workers. Bundled as its own
    // entry (the plain-ESM path recommended by Monaco — no monaco-editor-webpack-plugin) and
    // loaded at runtime via `MonacoEnvironment.getWorker` (see QueryEditor.scala).
    entry: {
        'editor.worker': 'monaco-editor/esm/vs/editor/editor.worker.js'
    },
    module: {
        rules: [
            {
                // Patches monaco-editor 0.55.x's vendored monaco-lsp-client: strips a stray
                // `debugger;` and injects a dispose() for reconnect teardown (see the loader header).
                test: /monaco-lsp-client[\\/]out[\\/]index\.js$/,
                loader: path.resolve(__dirname, "../../../../../query-editor/webpack/patch-monaco-lsp-client-loader.cjs")
            }, {
                test: /\.(ts|tsx)$/,
                use: {
                    loader: 'ts-loader',
                    options: {
                        // The query editor package source (public/query-editor) is the only
                        // first-party TypeScript webpack compiles here. Pin its compile config
                        // explicitly: ts-loader would otherwise pick the package's own
                        // tsconfig.json, whose TS 5.x options the repo's TypeScript 4.9.5
                        // rejects. transpileOnly leaves type-checking to the package's own
                        // `npm run typecheck` and module resolution to webpack.
                        configFile: path.resolve(__dirname, "../../../../../query-editor/tsconfig.webpack.json"),
                        transpileOnly: true
                    }
                },
                exclude: /node_modules/
            }, {
                test: /\.css$/,
                use: ['style-loader', 'css-loader']
            }, {
                test: /\.(gif|png|jpe?g|svg)$/i,
                type: 'asset/resource'
            }, {
                test: /\.(woff|woff2|eot|ttf|otf)$/i,
                type: 'asset/resource'
            }
        ]
    },
    resolve: {
        modules: [
            "node_modules",
            path.resolve(__dirname, "../../scalajs-bundler/main/node_modules"),
            path.resolve(__dirname, "../../../../src/main/scala/com/thatdot/quine/webapp")
        ],
        // good packages for fallbacks are listed at
        // https://webpack.js.org/configuration/resolve/#resolvefallback
        // or https://github.com/browserify/browserify#compatibility
        // such must also be added to devDependencies
        // Also note that these deps should be suffixed with "/", as this tells npm to resolve a module
        // rather than a built-in library
        fallback: {
            buffer: require.resolve('buffer/'),
            stream: require.resolve('stream-browserify/'),
            path: require.resolve('path-browserify/'),
        },
        modules: [
            "node_modules",
            path.resolve(__dirname, "../../scalajs-bundler/main/node_modules"),
            path.resolve(__dirname, "../../../../src/main/scala/com/thatdot/quine/webapp"),
        ],
        alias: {
            "NodeModules": path.resolve(__dirname, "../../scalajs-bundler/main/node_modules"),
            "resources": path.resolve(__dirname, "../../../../src/main/resources"),
            // The query editor package: TypeScript source vendored in-tree at
            // public/query-editor. Webpack compiles it through the ts-loader rule above —
            // there is no separate build step and no node_modules copy. From this bundler
            // working dir (`<module>/target/scala-2.13/scalajs-bundler/main/`) that's
            // 5 levels up, landing at `public/`.
            "@thatdot/query-editor": path.resolve(__dirname, "../../../../../query-editor/src/index.ts"),
            // Shared browser assets (SVG/PNG icons, etc.) consumed by every browser
            // module — see `public/shared-browser-resources/`. From this bundler
            // working dir (`<module>/target/scala-2.13/scalajs-bundler/main/`) that's
            // 5 levels up from public/quine-browser, landing at `public/`.
            "shared-resources": path.resolve(__dirname, "../../../../../shared-browser-resources"),
        },
        extensions: ['.js', '.jsx', '.ts', '.tsx'],
        // The query editor package's TypeScript source imports its own modules with explicit
        // `.js` specifiers (the ESM/Bundler convention its standalone tsc build relies on);
        // map those back to the `.ts` sources so webpack resolves them. `.ts` files only exist
        // in that package, so the `.js` fallback covers every other import unchanged.
        extensionAlias: {
            '.js': ['.ts', '.js']
        }
    },
    plugins: [
        // "process" is assumed by Stoplight elements to be available globally -- this is what the ProvidePlugin does
        new webpack.ProvidePlugin({
            process: require.resolve("process/browser")
        })
    ],
    output: {
        // The Monaco worker entry keeps a stable (un-hashed) filename: the worker URL is
        // referenced from compiled Scala.js code (QueryEditor.scala), which cannot know a
        // content hash at compile time. Everything else keeps the content-hashed name that
        // BrowserBundleRoutes discovers and serves with immutable caching.
        filename: (pathData) => pathData.chunk.name === 'editor.worker'
            ? 'editor.worker.js'
            : 'quine-browser-bundle.[contenthash].js',
        library: 'quineBrowser',
        libraryTarget: 'umd',
        // By default, webpack 5 asset modules include query strings in filenames (e.g., "abc123.svg?64h6xh").
        // scalajs-bundler checks if these files exist, but the actual files don't have query strings.
        // This setting ensures clean filenames without query strings for compatibility with scalajs-bundler.
        assetModuleFilename: '[hash][ext]'
    },
    externals: {
        'plotly.js/dist/plotly': 'Plotly'
    }
}
