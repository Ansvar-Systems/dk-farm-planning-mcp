import { describe, test, expect, beforeAll, afterAll } from 'vitest';
import { handleCalculateBreakEven } from '../../src/tools/calculate-break-even.js';
import { createSeededDatabase } from '../helpers/seed-db.js';
import type { Database } from '../../src/db.js';
import { existsSync, unlinkSync } from 'fs';

const TEST_DB = 'tests/test-break-even.db';

describe('calculate_break_even tool', () => {
  let db: Database;

  beforeAll(() => {
    db = createSeededDatabase(TEST_DB);
  });

  afterAll(() => {
    db.close();
    if (existsSync(TEST_DB)) unlinkSync(TEST_DB);
  });

  test('calculates break-even for winter wheat using DB data', () => {
    const result = handleCalculateBreakEven(db, { enterprise: 'winter wheat' });
    expect(result).toHaveProperty('break_even');
    const be = (result as { break_even: { profitable: boolean; break_even_output_per_unit: number } }).break_even;
    // variable costs 650, fixed 0, total 650. Output 1530, margin 880. Profitable.
    expect(be.profitable).toBe(true);
    expect(be.break_even_output_per_unit).toBe(650);
  });

  test('accounts for fixed costs', () => {
    const result = handleCalculateBreakEven(db, { enterprise: 'winter wheat', fixed_costs: 400 });
    const be = (result as { break_even: { break_even_output_per_unit: number; margin_per_unit: number } }).break_even;
    // variable 650 + fixed 400 = 1050. Output 1530. Margin 480.
    expect(be.break_even_output_per_unit).toBe(1050);
    expect(be.margin_per_unit).toBe(480);
  });

  test('shows unprofitable when costs exceed output', () => {
    const result = handleCalculateBreakEven(db, { enterprise: 'winter wheat', fixed_costs: 1000 });
    const be = (result as { break_even: { profitable: boolean } }).break_even;
    // 650 + 1000 = 1650 > 1530
    expect(be.profitable).toBe(false);
  });

  test('uses override variable costs', () => {
    const result = handleCalculateBreakEven(db, { enterprise: 'winter wheat', variable_costs: 800 });
    const costs = (result as { costs: { variable_costs_per_unit: number; total_costs_per_unit: number } }).costs;
    expect(costs.variable_costs_per_unit).toBe(800);
    expect(costs.total_costs_per_unit).toBe(800);
  });

  test('returns not_found for unknown enterprise', () => {
    const result = handleCalculateBreakEven(db, { enterprise: 'llamas' });
    expect(result).toHaveProperty('error', 'not_found');
  });

  test('rejects unsupported jurisdiction', () => {
    const result = handleCalculateBreakEven(db, { enterprise: 'wheat', jurisdiction: 'US' });
    expect(result).toHaveProperty('error', 'jurisdiction_not_supported');
  });
});
