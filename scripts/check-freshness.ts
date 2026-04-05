#!/usr/bin/env tsx
/**
 * Check data freshness and report staleness.
 * Used by the check-freshness GitHub Actions workflow.
 */

import { createDatabase } from '../src/db.js';
import { handleCheckFreshness } from '../src/tools/check-freshness.js';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const dbPath = join(__dirname, '..', 'data', 'database.db');

const db = createDatabase(dbPath);
const result = handleCheckFreshness(db);

console.log(JSON.stringify(result, null, 2));

if (result.status === 'stale') {
  console.error(`WARNING: Data is stale (${result.days_since_ingest} days old)`);
  process.exit(1);
}

db.close();
