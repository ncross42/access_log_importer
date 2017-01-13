// webpack.config.js
module.exports = {
  entry: './assets/js/summary.js',
  output: {
    path: 'public/js',
    filename: 'bundle.js'
  },
  module: {
    loaders: [
      { test: /\.sass$/, loader: 'style!css!sass' }
    ]
  }
};
