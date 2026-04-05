#!/usr/bin/env tsx
/**
 * Build the SQLite database from scratch.
 * Re-runs ingestion and populates the FTS5 index.
 */

console.log('Building database... (delegates to ingest.ts)');
await import('./ingest.js');
