const webpack = require('webpack');
const path = require("path");

module.exports = {
    module: {
        rules: [
            { 
                test: /\.(ts|tsx)$/, 
                use: 'ts-loader',
                exclude: /node_modules/
            }, {
                test: /\.css$/,
                use: ['style-loader', 'css-loader']
            }, {
                test: /\.(gif|png|jpe?g|svg)$/i,
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
        },
        extensions: ['.js', '.jsx', '.ts', '.tsx']
    },
    plugins: [
        // "process" is assumed by Stoplight elements to be available globally -- this is what the ProvidePlugin does
        new webpack.ProvidePlugin({
            process: require.resolve("process/browser")
        })
    ],
    output: {
        filename: 'quine-browser-bundle.js',
        library: 'quineBrowser',
        libraryTarget: 'umd'
    },
    externals: {
        vis: 'vis',
        'plotly.js/dist/plotly': 'Plotly'
    }
}
