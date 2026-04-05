import { describe, test, expect, beforeAll, afterAll } from 'vitest';
import { handleSearchFarmPlanning } from '../../src/tools/search-farm-planning.js';
import { createSeededDatabase } from '../helpers/seed-db.js';
import type { Database } from '../../src/db.js';
import { existsSync, unlinkSync } from 'fs';

const TEST_DB = 'tests/test-search-farm.db';

describe('search_farm_planning tool', () => {
  let db: Database;

  beforeAll(() => {
    db = createSeededDatabase(TEST_DB);
  });

  afterAll(() => {
    db.close();
    if (existsSync(TEST_DB)) unlinkSync(TEST_DB);
  });

  test('returns results for rotation query', () => {
    const result = handleSearchFarmPlanning(db, { query: 'rotation wheat' });
    expect(result).toHaveProperty('results_count');
    expect((result as { results_count: number }).results_count).toBeGreaterThan(0);
  });

  test('returns results for tax query', () => {
    const result = handleSearchFarmPlanning(db, { query: 'MTD income tax' });
    expect((result as { results_count: number }).results_count).toBeGreaterThan(0);
  });

  test('respects topic filter', () => {
    const result = handleSearchFarmPlanning(db, { query: 'wheat', topic: 'rotation' });
    const results = (result as { results: { topic: string }[] }).results;
    for (const r of results) {
      expect(r.topic).toBe('rotation');
    }
  });

  test('rejects unsupported jurisdiction', () => {
    const result = handleSearchFarmPlanning(db, { query: 'wheat', jurisdiction: 'FR' });
    expect(result).toHaveProperty('error', 'jurisdiction_not_supported');
  });
});
