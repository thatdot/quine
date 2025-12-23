/**
 * Centralized MIME type mapping for Vite dev server file serving.
 * This replaces the duplicated content-type mappings across project configs.
 */

const MIME_TYPES: Record<string, string> = {
  // JavaScript and related
  '.js': 'application/javascript',
  '.mjs': 'application/javascript',
  '.ts': 'application/typescript',

  // Stylesheets
  '.css': 'text/css',

  // Data formats
  '.json': 'application/json',
  '.xml': 'application/xml',

  // Images
  '.svg': 'image/svg+xml',
  '.png': 'image/png',
  '.jpg': 'image/jpeg',
  '.jpeg': 'image/jpeg',
  '.gif': 'image/gif',
  '.ico': 'image/x-icon',
  '.webp': 'image/webp',

  // Fonts
  '.woff': 'font/woff',
  '.woff2': 'font/woff2',
  '.ttf': 'font/ttf',
  '.otf': 'font/otf',
  '.eot': 'application/vnd.ms-fontobject',

  // Web manifests
  '.webmanifest': 'application/manifest+json',

  // HTML
  '.html': 'text/html',
  '.htm': 'text/html',

  // Source maps
  '.map': 'application/json',
};

/**
 * Get the MIME type for a file based on its extension.
 * Returns 'application/octet-stream' for unknown extensions.
 */
export function getMimeType(filePath: string): string {
  const ext = filePath.substring(filePath.lastIndexOf('.')).toLowerCase();
  return MIME_TYPES[ext] || 'application/octet-stream';
}

export { MIME_TYPES };
