import type { Plugin } from 'vite';
import type { IncomingMessage, ServerResponse } from 'http';

export type MockApiHandler = (
  req: IncomingMessage,
  res: ServerResponse,
  body?: unknown
) => void;

export type MockApiHandlerMap = Record<string, MockApiHandler>;

export interface MockApiOptions {
  /**
   * Additional handlers to merge with base handlers.
   * Keys should be in format: "METHOD /path"
   * e.g., "GET /api/v2/auth/me"
   */
  additionalHandlers?: MockApiHandlerMap;

  /**
   * Fixture data providers for the base handlers.
   */
  fixtures: {
    sampleQueries: unknown[];
    nodeAppearances: Record<string, unknown>[];
    quickQueries: unknown[];
    sampleNodes: unknown[];
    sampleEdges: unknown[];
    sampleQueryResult: unknown;
    metrics: unknown;
  };

  /**
   * Product name for OpenAPI info (e.g., "Quine", "Quine Enterprise", "Novelty")
   */
  productName?: string;
}

/**
 * Send a JSON response with CORS headers.
 */
export function respondJson(res: ServerResponse, data: unknown, status = 200): void {
  res.writeHead(status, {
    'Content-Type': 'application/json',
    'Access-Control-Allow-Origin': '*',
  });
  res.end(JSON.stringify(data, null, 2));
}

/**
 * Wrap v1 response data in v2 envelope format.
 */
export function wrapV2Response(content: unknown): unknown {
  return {
    content,
    message: null,
    warnings: [],
  };
}

/**
 * Parse JSON request body.
 */
function parseRequestBody(req: IncomingMessage): Promise<unknown> {
  return new Promise((resolve, reject) => {
    let body = '';
    req.on('data', (chunk) => {
      body += chunk.toString();
    });
    req.on('end', () => {
      try {
        resolve(body ? JSON.parse(body) : null);
      } catch {
        resolve(null);
      }
    });
    req.on('error', reject);
  });
}

/**
 * Create base mock API handlers shared across all projects.
 */
function createBaseHandlers(fixtures: MockApiOptions['fixtures']): MockApiHandlerMap {
  const {
    sampleQueries,
    nodeAppearances,
    quickQueries,
    sampleNodes,
    sampleEdges,
    sampleQueryResult,
    metrics,
  } = fixtures;

  return {
    // Sample queries
    'GET /api/v1/query-ui/sample-queries': (_req, res) => {
      respondJson(res, sampleQueries);
    },
    'GET /api/v2/query-ui/sample-queries': (_req, res) => {
      respondJson(res, wrapV2Response(sampleQueries));
    },

    // Node appearances
    'GET /api/v1/query-ui/node-appearances': (_req, res) => {
      respondJson(res, nodeAppearances);
    },
    'GET /api/v2/query-ui/node-appearances': (_req, res) => {
      respondJson(res, wrapV2Response(nodeAppearances));
    },

    // Quick queries
    'GET /api/v1/query-ui/quick-queries': (_req, res) => {
      respondJson(res, quickQueries);
    },
    'GET /api/v2/query-ui/quick-queries': (_req, res) => {
      respondJson(res, wrapV2Response(quickQueries));
    },

    // Metrics
    'GET /api/v1/admin/metrics': (_req, res) => {
      respondJson(res, metrics);
    },
    'GET /api/v2/admin/metrics': (_req, res) => {
      respondJson(res, wrapV2Response(metrics));
    },

    // Shard sizes
    'POST /api/v1/admin/shard-sizes': (_req, res) => {
      respondJson(res, {});
    },
    'GET /api/v2/admin/shards/size-limits': (_req, res) => {
      respondJson(res, wrapV2Response({}));
    },

    // Query endpoints (v1)
    'POST /api/v1/query/cypher': (_req, res, body) => {
      console.log('[Mock API] Cypher query:', body);
      respondJson(res, sampleQueryResult);
    },
    'POST /api/v1/query/cypher/nodes': (_req, res, body) => {
      console.log('[Mock API] Cypher nodes query:', body);
      console.log(`[Mock API] Returning ${sampleNodes.length} nodes`);
      respondJson(res, sampleNodes);
    },
    'POST /api/v1/query/cypher/edges': (_req, res, body) => {
      console.log('[Mock API] Cypher edges query:', body);
      console.log(`[Mock API] Returning ${sampleEdges.length} edges`);
      respondJson(res, sampleEdges);
    },
    'POST /api/v1/query/gremlin/nodes': (_req, res, body) => {
      console.log('[Mock API] Gremlin nodes query:', body);
      respondJson(res, sampleNodes);
    },
    'POST /api/v1/query/gremlin/edges': (_req, res, body) => {
      console.log('[Mock API] Gremlin edges query:', body);
      respondJson(res, sampleEdges);
    },

    // Query endpoints (v2)
    'POST /api/v2/cypher-queries/query-graph': (_req, res, body) => {
      console.log('[Mock API] Cypher query (v2):', body);
      respondJson(res, wrapV2Response(sampleQueryResult));
    },
    'POST /api/v2/cypher-queries/query-nodes': (_req, res, body) => {
      console.log('[Mock API] Cypher nodes query (v2):', body);
      console.log(`[Mock API] Returning ${sampleNodes.length} nodes`);
      respondJson(res, wrapV2Response(sampleNodes));
    },
    'POST /api/v2/cypher-queries/query-edges': (_req, res, body) => {
      console.log('[Mock API] Cypher edges query (v2):', body);
      console.log(`[Mock API] Returning ${sampleEdges.length} edges`);
      respondJson(res, wrapV2Response(sampleEdges));
    },
  };
}

/**
 * Create OpenAPI doc handlers.
 */
function createOpenApiHandlers(productName: string): MockApiHandlerMap {
  return {
    'GET /docs/openapi.json': (_req, res) => {
      respondJson(res, {
        openapi: '3.0.0',
        info: {
          title: `${productName} API (v1)`,
          version: '1.0.0',
          description: 'Mock API for development',
        },
        paths: {},
      });
    },
    'GET /api/v2/openapi.json': (_req, res) => {
      respondJson(res, {
        openapi: '3.0.0',
        info: {
          title: `${productName} API (v2)`,
          version: '2.0.0',
          description: 'Mock API for development',
        },
        paths: {},
      });
    },
  };
}

/**
 * Create a composable mock API Vite plugin.
 *
 * Usage:
 * ```ts
 * createMockApiPlugin({
 *   fixtures: { ... },
 *   productName: 'Quine',
 *   additionalHandlers: {
 *     'GET /api/v2/auth/me': (req, res) => respondJson(res, { ... }),
 *   },
 * })
 * ```
 */
export function createMockApiPlugin(options: MockApiOptions): Plugin {
  const { additionalHandlers = {}, fixtures, productName = 'Quine' } = options;

  const handlers: MockApiHandlerMap = {
    ...createBaseHandlers(fixtures),
    ...createOpenApiHandlers(productName),
    ...additionalHandlers,
  };

  return {
    name: 'mock-api',
    configureServer(server) {
      server.middlewares.use(async (req, res, next) => {
        const url = req.url || '';
        const method = req.method || 'GET';

        // Handle OPTIONS for CORS preflight
        if (method === 'OPTIONS') {
          res.writeHead(204, {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS',
            'Access-Control-Allow-Headers': 'Content-Type, Authorization',
          });
          res.end();
          return;
        }

        // Check if this is an API request
        if (url.startsWith('/api/') || url.startsWith('/docs/')) {
          const pathWithoutQuery = url.split('?')[0];
          const handlerKey = `${method} ${pathWithoutQuery}`;

          console.log(`[Mock API] ${handlerKey}`);

          const handler = handlers[handlerKey];
          if (handler) {
            try {
              const body = await parseRequestBody(req);
              handler(req, res, body);
              return;
            } catch (error) {
              console.error('[Mock API] Error:', error);
              res.writeHead(500, { 'Content-Type': 'application/json' });
              res.end(JSON.stringify({ error: 'Internal server error' }));
              return;
            }
          } else {
            console.warn(`[Mock API] No handler for ${handlerKey}`);
            res.writeHead(404, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ error: `No mock handler for ${handlerKey}` }));
            return;
          }
        }

        next();
      });
    },
  };
}
