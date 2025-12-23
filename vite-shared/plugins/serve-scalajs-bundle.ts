import type { Plugin } from 'vite';
import fs from 'fs';
import path from 'path';
import { getMimeType } from '../utils/mime-types';

export interface ScalaJSBundleOptions {
  /**
   * Path to the ScalaJS bundler output directory.
   * Typically: ../target/scala-2.13/scalajs-bundler/main
   */
  bundlePath: string;

  /**
   * Path to the parent node_modules directory.
   * Typically: ../node_modules
   */
  nodeModulesPath: string;

  /**
   * Optional path to static assets (favicon, manifest, etc.)
   * e.g., ../../quine/src/main/resources/web
   */
  staticAssetsPath?: string;
}

/**
 * Vite plugin to serve ScalaJS bundles and related assets from outside the dev root.
 *
 * Handles three types of requests:
 * 1. /@bundle/* - ScalaJS compiled output
 * 2. /node_modules/* - Dependencies from parent directory
 * 3. Static assets - Favicon, manifests, etc. from Scala resource directories
 * 4. Root-level hashed assets - Webpack-bundled assets (SVGs, etc.)
 */
export function createScalaJSBundlePlugin(options: ScalaJSBundleOptions): Plugin {
  const { bundlePath, nodeModulesPath, staticAssetsPath } = options;

  return {
    name: 'serve-scalajs-bundle',
    configureServer(server) {
      server.middlewares.use((req, res, next) => {
        const url = req.url || '';
        const urlPath = url.split('?')[0]; // Remove query string

        // Handle /@bundle/ requests for ScalaJS output
        if (url.startsWith('/@bundle/')) {
          const filePath = url.replace('/@bundle/', '');
          const fullPath = path.resolve(bundlePath, filePath);

          if (fs.existsSync(fullPath) && fs.statSync(fullPath).isFile()) {
            res.setHeader('Content-Type', getMimeType(fullPath));
            fs.createReadStream(fullPath).pipe(res);
            return;
          }
        }

        // Handle /node_modules/ requests from parent directory
        if (url.startsWith('/node_modules/')) {
          const filePath = url.replace('/node_modules/', '');
          const fullPath = path.resolve(nodeModulesPath, filePath);

          if (fs.existsSync(fullPath) && fs.statSync(fullPath).isFile()) {
            res.setHeader('Content-Type', getMimeType(fullPath));
            fs.createReadStream(fullPath).pipe(res);
            return;
          }
        }

        // Serve static assets from Scala web resources (favicon, manifest, etc.)
        if (staticAssetsPath) {
          const staticFilePath = path.join(staticAssetsPath, urlPath);
          if (fs.existsSync(staticFilePath) && fs.statSync(staticFilePath).isFile()) {
            res.setHeader('Content-Type', getMimeType(staticFilePath));
            fs.createReadStream(staticFilePath).pipe(res);
            return;
          }
        }

        // Serve webpack-bundled assets (hashed SVGs, etc.) from bundle directory at root
        // This handles @JSImport resources that webpack outputs with hashed names
        const bundleFilePath = path.join(bundlePath, urlPath);
        if (fs.existsSync(bundleFilePath) && fs.statSync(bundleFilePath).isFile()) {
          res.setHeader('Content-Type', getMimeType(bundleFilePath));
          fs.createReadStream(bundleFilePath).pipe(res);
          return;
        }

        next();
      });
    },
  };
}
