const webpack = require("webpack");
const { merge } = require("webpack-merge");

const generatedConfig = require("./scalajs.webpack.config");
const commonConfig = require("./common.webpack.config.js");

module.exports = merge(generatedConfig, commonConfig);
module.exports.mode = "development";
module.exports.devtool = "source-map"; // CSP-compliant external source maps
