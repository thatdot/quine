# Quine Browser Development

Development setup for running the Vite dev server with Scala.js.

## Prerequisites

- sbt
- Node.js and npm

## Development Setup

You need to run two terminals simultaneously:

### Terminal 1: Scala.js Compilation

Start sbt and run continuous compilation with webpack bundling:

```bash
sbt "project quine-browser" "~fastOptJS::webpack"
```

### Terminal 2: Vite Dev Server

From the `dev` folder, start the Vite development server:

```bash
npm install
npm run dev
```

The browser will open automatically at http://localhost:5173

## Available URL Parameters

- `#<query>` - Set initial Cypher query (URL encoded)
- `?interactive=false` - Hide query bar
- `?layout=graph|tree` - Set layout mode
- `?wsQueries=false` - Disable WebSocket queries
- `?v2Api=true|false` - Use v2 API (default: true)
- `?atTime=<millis>` - Query at historical time

## Mock API

The development server includes a mock API that simulates the Quine backend:

- `/api/v1/*` and `/api/v2/*` - Query and configuration endpoints
- `/docs/openapi.json` - OpenAPI documentation

Check the console for mock API logs.
