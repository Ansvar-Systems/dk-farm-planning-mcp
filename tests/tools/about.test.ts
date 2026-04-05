import { describe, test, expect } from 'vitest';
import { handleAbout } from '../../src/tools/about.js';

describe('about tool', () => {
  test('returns server metadata', () => {
    const result = handleAbout();
    expect(result.name).toBe('UK Farm Planning MCP');
    expect(result.description).toContain('farm');
    expect(result.jurisdiction).toEqual(['GB']);
    expect(result.tools_count).toBe(11);
    expect(result.links).toHaveProperty('homepage');
    expect(result._meta).toHaveProperty('disclaimer');
  });
});
