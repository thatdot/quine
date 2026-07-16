// Webpack config for quine-browser's Scala.js test bundle.
//
// The app's `common.webpack.config.js` compiles the Monaco-based query editor from TypeScript
// source and pulls in plotly/vis/Stoplight. None of that is reachable from the unit tests, but
// webpack resolves the `@JSImport`s anyway, and the TypeScript is what makes the test bundle fail
// to parse. Stub those modules out instead: the tests exercise plain Scala logic, so an empty
// object in their place is enough for the bundle to link.
const path = require("path");
const webpack = require("webpack");

module.exports = {
  module: {
    rules: [
      {
        test: /\.css$/,
        use: ['style-loader', 'css-loader']
      }, {
        test: /\.(gif|png|jpe?g|svg)$/i,
        type: 'asset/resource'
      }
    ]
  },
  resolve: {
    fallback: {
      buffer: require.resolve('buffer/'),
      stream: require.resolve('stream-browserify/'),
      path: require.resolve('path-browserify/'),
    },
    // Same aliases as common.webpack.config.js. The bundler working dir is
    // `<module>/target/scala-2.13/scalajs-bundler/test/` — the same depth as the `main/` dir the
    // app config resolves from, so the relative paths are identical.
    alias: {
      "NodeModules": path.resolve(__dirname, "node_modules"),
      "resources": path.resolve(__dirname, "../../../../src/main/resources"),
      "shared-resources": path.resolve(__dirname, "../../../../../shared-browser-resources"),
    },
    extensions: ['.js', '.jsx'],
  },
  plugins: [
    new webpack.ProvidePlugin({
      process: require.resolve("process/browser")
    })
  ],
  output: {
    filename: 'quine-browser-test-bundle.js',
  },
  externals: [
    // Browser-only modules that the tests never instantiate. Returning an empty object for each
    // keeps them out of the bundle rather than dragging Monaco, plotly and friends into a headless
    // test run.
    function ({ request }, callback) {
      const stubbed = [
        'vis',
        'plotly.js/dist/plotly',
        'plotly.js',
        '@stoplight/elements',
        '@stoplight/elements/web-components.min.js',
        '@coreui/coreui/dist/js/coreui.bundle.min.js',
        '@thatdot/query-editor',
      ];
      if (stubbed.includes(request)) {
        return callback(null, 'var {}');
      }
      callback();
    }
  ]
}
