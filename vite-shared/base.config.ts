import { defineConfig, searchForWorkspaceRoot, type UserConfig } from 'vite';
import path from 'path';
import { createScalaJSBundlePlugin } from './plugins/serve-scalajs-bundle';

export interface ScalaJSProjectConfig {
  /**
   * Project name for identification (e.g., 'quine-browser')
   */
  projectName: string;

  /**
   * Name of the fastOptJS bundle file (e.g., 'quine-browser-fastopt.js')
   */
  bundleName: string;

  /**
   * Dev server port (default: 5173)
   */
  port?: number;

  /**
   * Path to the dev directory (typically __dirname from the calling config)
   */
  devRoot: string;

  /**
   * Optional path to static assets (favicon, manifest, etc.)
   * e.g., path.resolve(__dirname, '../../quine/src/main/resources/web')
   */
  staticAssetsPath?: string;

  /**
   * Additional resolve aliases beyond @bundle
   */
  additionalAliases?: Record<string, string>;
}

/**
 * Create a base Vite configuration for ScalaJS browser projects.
 *
 * This provides:
 * - Proper file system access for ScalaJS output and node_modules
 * - Watch configuration for HMR on ScalaJS changes
 * - @bundle alias for ScalaJS output
 * - Serve plugin for /@bundle/, /node_modules/, and static assets
 *
 * Usage:
 * ```ts
 * import { createBaseConfig } from '../../vite-shared';
 *
 * export default createBaseConfig({
 *   projectName: 'quine-browser',
 *   bundleName: 'quine-browser-fastopt.js',
 *   port: 5173,
 *   devRoot: __dirname,
 *   staticAssetsPath: path.resolve(__dirname, '../../quine/src/main/resources/web'),
 * });
 * ```
 */
export function createBaseConfig(config: ScalaJSProjectConfig): UserConfig {
  const {
    projectName,
    bundleName,
    port = 5173,
    devRoot,
    staticAssetsPath,
    additionalAliases = {},
  } = config;

  const bundlePath = path.resolve(devRoot, '../target/scala-2.13/scalajs-bundler/main');
  const workspaceRoot = searchForWorkspaceRoot(devRoot);
  const nodeModulesPath = path.resolve(workspaceRoot, 'node_modules');

  // Build fs.allow list
  const fsAllow = [
    workspaceRoot,
    bundlePath,
    nodeModulesPath,
  ];
  if (staticAssetsPath) {
    fsAllow.push(staticAssetsPath);
  }

  return defineConfig({
    root: devRoot,

    plugins: [
      createScalaJSBundlePlugin({
        bundlePath,
        nodeModulesPath,
        staticAssetsPath,
      }),
    ],

    server: {
      port,
      open: true,
      fs: {
        strict: true,
        allow: fsAllow,
      },
      watch: {
        // Watch the ScalaJS output for HMR
        ignored: ['!**/target/scala-2.13/scalajs-bundler/main/**'],
      },
    },

    resolve: {
      alias: {
        '@bundle': bundlePath,
        ...additionalAliases,
      },
    },

    optimizeDeps: {
      exclude: [`@bundle/${bundleName}`],
    },
  });
}
