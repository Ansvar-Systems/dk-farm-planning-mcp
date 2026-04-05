import { describe, test, expect, beforeAll, afterAll } from 'vitest';
import { handleGetAprGuidance } from '../../src/tools/get-apr-guidance.js';
import { createSeededDatabase } from '../helpers/seed-db.js';
import type { Database } from '../../src/db.js';
import { existsSync, unlinkSync } from 'fs';

const TEST_DB = 'tests/test-apr.db';

describe('get_apr_guidance tool', () => {
  let db: Database;

  beforeAll(() => {
    db = createSeededDatabase(TEST_DB);
  });

  afterAll(() => {
    db.close();
    if (existsSync(TEST_DB)) unlinkSync(TEST_DB);
  });

  test('returns all APR guidance when no scenario specified', () => {
    const result = handleGetAprGuidance(db, {});
    expect(result).toHaveProperty('results_count');
    expect((result as { results_count: number }).results_count).toBeGreaterThanOrEqual(8);
  });

  test('returns owner-occupier guidance at 100%', () => {
    const result = handleGetAprGuidance(db, { scenario: 'owner-occupier' });
    const r = (result as { results: { relief_available: boolean; relief_rate: string }[] }).results[0];
    expect(r.relief_available).toBe(true);
    expect(r.relief_rate).toBe('100%');
  });

  test('returns pre-1995 tenanted at 50%', () => {
    const result = handleGetAprGuidance(db, { scenario: 'pre-1' });
    const results = (result as { results: { relief_rate: string; scenario: string }[] }).results;
    const pre1995 = results.find(r => r.scenario.includes('pre-1 Sep 1995'));
    expect(pre1995).toBeDefined();
    expect(pre1995!.relief_rate).toBe('50%');
  });

  test('diversified land gets no APR', () => {
    const result = handleGetAprGuidance(db, { scenario: 'diversified' });
    const r = (result as { results: { relief_available: boolean }[] }).results[0];
    expect(r.relief_available).toBe(false);
  });

  test('farmhouse scenario includes character appropriate test', () => {
    const result = handleGetAprGuidance(db, { scenario: 'farmhouse' });
    const r = (result as { results: { occupation_test: string }[] }).results[0];
    expect(r.occupation_test.toLowerCase()).toContain('character appropriate');
  });

  test('rejects unsupported jurisdiction', () => {
    const result = handleGetAprGuidance(db, { scenario: 'farmhouse', jurisdiction: 'NL' });
    expect(result).toHaveProperty('error', 'jurisdiction_not_supported');
  });

  test('includes _meta with disclaimer', () => {
    const result = handleGetAprGuidance(db, {});
    expect((result as { _meta: { disclaimer: string } })._meta.disclaimer).toContain('tax');
  });
});
