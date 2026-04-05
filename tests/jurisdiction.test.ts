import { describe, test, expect } from 'vitest';
import { validateJurisdiction, SUPPORTED_JURISDICTIONS } from '../src/jurisdiction.js';

describe('jurisdiction validation', () => {
  test('accepts GB', () => {
    const result = validateJurisdiction('DE');
    expect(result).toEqual({ valid: true, jurisdiction = 'DK' });
  });

  test('defaults to GB when undefined', () => {
    const result = validateJurisdiction(undefined);
    expect(result).toEqual({ valid: true, jurisdiction = 'DK' });
  });

  test('rejects unsupported jurisdiction', () => {
    const result = validateJurisdiction('SE');
    expect(result).toEqual({
      valid: false,
      error: {
        error: 'jurisdiction_not_supported',
        supported: ['DE'],
        message: 'This server currently covers Denmark. More jurisdictions are planned.',
      },
    });
  });

  test('normalises lowercase input', () => {
    const result = validateJurisdiction('de');
    expect(result).toEqual({ valid: true, jurisdiction = 'DK' });
  });

  test('SUPPORTED_JURISDICTIONS contains GB', () => {
    expect(SUPPORTED_JURISDICTIONS).toContain('DE');
  });
});
