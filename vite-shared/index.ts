// Base configuration factory
export { createBaseConfig, type ScalaJSProjectConfig } from './base.config';

// Plugins
export { createScalaJSBundlePlugin, type ScalaJSBundleOptions } from './plugins/serve-scalajs-bundle';
export {
  createMockApiPlugin,
  respondJson,
  wrapV2Response,
  type MockApiHandler,
  type MockApiHandlerMap,
  type MockApiOptions,
} from './plugins/mock-api-factory';

// Utilities
export { getMimeType, MIME_TYPES } from './utils/mime-types';

// Shared fixtures
export * from './fixtures/query-results';
export * from './fixtures/metrics';
export * from './fixtures/ui-config'