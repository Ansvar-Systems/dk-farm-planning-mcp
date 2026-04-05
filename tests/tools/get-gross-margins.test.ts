import { describe, test, expect, beforeAll, afterAll } from 'vitest';
import { handleGetGrossMargins } from '../../src/tools/get-gross-margins.js';
import { createSeededDatabase } from '../helpers/seed-db.js';
import type { Database } from '../../src/db.js';
import { existsSync, unlinkSync } from 'fs';

const TEST_DB = 'tests/test-gross-margins.db';

describe('get_gross_margins tool', () => {
  let db: Database;

  beforeAll(() => {
    db = createSeededDatabase(TEST_DB);
  });

  afterAll(() => {
    db.close();
    if (existsSync(TEST_DB)) unlinkSync(TEST_DB);
  });

  test('returns gross margin for winter wheat', () => {
    const result = handleGetGrossMargins(db, { enterprise: 'winter wheat' });
    expect(result).toHaveProperty('results_count');
    const r = (result as { results: { output_per_unit: number; gross_margin_per_unit: number }[] }).results[0];
    expect(r.output_per_unit).toBe(1530);
    expect(r.gross_margin_per_unit).toBe(880);
  });

  test('returns gross margin for dairy cow', () => {
    const result = handleGetGrossMargins(db, { enterprise: 'dairy' });
    const r = (result as { results: { enterprise: string; unit: string }[] }).results[0];
    expect(r.enterprise).toBe('Dairy cow');
    expect(r.unit).toBe('per_head');
  });

  test('returns not_found for unknown enterprise', () => {
    const result = handleGetGrossMargins(db, { enterprise: 'alpacas' });
    expect(result).toHaveProperty('error', 'not_found');
  });

  test('rejects unsupported jurisdiction', () => {
    const result = handleGetGrossMargins(db, { enterprise: 'wheat', jurisdiction = 'DK' });
    expect(result).toHaveProperty('error', 'jurisdiction_not_supported');
  });

  test('includes top and bottom quartile', () => {
    const result = handleGetGrossMargins(db, { enterprise: 'winter wheat' });
    const r = (result as { results: { top_quartile: number; bottom_quartile: number }[] }).results[0];
    expect(r.top_quartile).toBe(1100);
    expect(r.bottom_quartile).toBe(600);
  });
});
