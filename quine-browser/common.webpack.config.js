var path = require("path");

module.exports = {
  module: {
    rules: [
      {
        test: /\.css$/,
        use: [ 'style-loader', 'css-loader' ]
      }, {
        test: /\.(gif|png|jpe?g|svg)$/i,
        use: [ 'url-loader' ]
      }
    ]
  },
  resolve: {
    alias: {
      "NodeModules": path.resolve(__dirname, "../../scalajs-bundler/main/node_modules"),
      "resources": path.resolve(__dirname, "../../../../src/main/resources")
    }
  },
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
