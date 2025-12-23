import { mergeConfig } from 'vite';
import path from 'path';
import {
  createBaseConfig,
  createMockApiPlugin,
  sampleNodes,
  sampleEdges,
  sampleQueryResult,
  metrics,
  sampleQueries,
  nodeAppearances,
  quickQueries
} from '../../vite-shared';

const baseConfig = createBaseConfig({
  projectName: 'quine-browser',
  bundleName: 'quine-browser-fastopt.js',
  port: 5173,
  devRoot: __dirname,
  staticAssetsPath: path.resolve(__dirname, '../../quine/src/main/resources/web'),
});

export default mergeConfig(baseConfig, {
  plugins: [
    createMockApiPlugin({
      fixtures: {
        sampleQueries,
        nodeAppearances,
        quickQueries,
        sampleNodes,
        sampleEdges,
        sampleQueryResult,
        metrics,
      },
      productName: 'Quine',
    }),
  ],
});
