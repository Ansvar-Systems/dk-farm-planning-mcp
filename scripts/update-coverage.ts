#!/usr/bin/env tsx
/**
 * Update coverage.json with current database statistics.
 */

import { createDatabase } from '../src/db.js';
import { writeFileSync } from 'fs';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const dbPath = join(__dirname, '..', 'data', 'database.db');
const coveragePath = join(__dirname, '..', 'data', 'coverage.json');

const db = createDatabase(dbPath);

function count(table: string): number {
  const row = db.get<{ c: number }>(`SELECT COUNT(*) as c FROM ${table}`);
  return row?.c ?? 0;
}

const coverage = {
  mcp_name: 'UK Farm Planning MCP',
  jurisdiction: 'GB',
  build_date: new Date().toISOString().split('T')[0],
  rotation_guidance: count('rotation_guidance'),
  gross_margins: count('gross_margins'),
  tax_rules: count('tax_rules'),
  apr_guidance: count('apr_guidance'),
  tenancy_rules: count('tenancy_rules'),
  diversification: count('diversification'),
  fts_entries: count('search_index'),
  source_hash: 'computed-at-build',
};

writeFileSync(coveragePath, JSON.stringify(coverage, null, 2) + '\n');
console.log('Coverage updated:', coverage);

db.close();
