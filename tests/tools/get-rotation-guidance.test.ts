import { describe, test, expect, beforeAll, afterAll } from 'vitest';
import { handleGetRotationGuidance } from '../../src/tools/get-rotation-guidance.js';
import { createSeededDatabase } from '../helpers/seed-db.js';
import type { Database } from '../../src/db.js';
import { existsSync, unlinkSync } from 'fs';

const TEST_DB = 'tests/test-rotation.db';

describe('get_rotation_guidance tool', () => {
  let db: Database;

  beforeAll(() => {
    db = createSeededDatabase(TEST_DB);
  });

  afterAll(() => {
    db.close();
    if (existsSync(TEST_DB)) unlinkSync(TEST_DB);
  });

  test('returns guidance for single crop', () => {
    const result = handleGetRotationGuidance(db, { crops: 'winter wheat' });
    expect(result).toHaveProperty('results_count');
    expect((result as { results_count: number }).results_count).toBeGreaterThan(0);
  });

  test('returns guidance for crop pair', () => {
    const result = handleGetRotationGuidance(db, { crops: 'winter wheat,oilseed rape' });
    const results = (result as { results: { suitability: string }[] }).results;
    expect(results.length).toBeGreaterThan(0);
    expect(results[0].suitability).toBe('good');
  });

  test('flags continuous wheat as poor', () => {
    const result = handleGetRotationGuidance(db, { crops: 'winter wheat,winter wheat continuous' });
    const results = (result as { results: { suitability: string; blackgrass_risk: string }[] }).results;
    expect(results[0].suitability).toBe('poor');
    expect(results[0].blackgrass_risk).toBe('very high');
  });

  test('returns not_found for unknown crop', () => {
    const result = handleGetRotationGuidance(db, { crops: 'quinoa' });
    expect(result).toHaveProperty('error', 'not_found');
  });

  test('rejects unsupported jurisdiction', () => {
    const result = handleGetRotationGuidance(db, { crops: 'wheat', jurisdiction: 'SE' });
    expect(result).toHaveProperty('error', 'jurisdiction_not_supported');
  });

  test('includes blackgrass risk and yield impact', () => {
    const result = handleGetRotationGuidance(db, { crops: 'winter wheat,winter wheat' });
    const r = (result as { results: { blackgrass_risk: string; yield_impact_pct: number }[] }).results[0];
    expect(r.blackgrass_risk).toBe('high');
    expect(r.yield_impact_pct).toBe(-12.5);
  });
});
