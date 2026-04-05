export interface Meta {
  disclaimer: string;
  data_age: string;
  source_url: string;
  copyright: string;
  server: string;
  version: string;
}

const DISCLAIMER =
  'This server provides general guidance on UK farm planning, tax, and tenancy law. ' +
  'Tax rules change frequently — always verify with HMRC or a qualified agricultural accountant. ' +
  'Tenancy disputes should be referred to a specialist agricultural solicitor. ' +
  'Gross margins are indicative benchmarks, not forecasts.';

export function buildMeta(overrides?: Partial<Meta>): Meta {
  return {
    disclaimer: DISCLAIMER,
    data_age: overrides?.data_age ?? 'unknown',
    source_url: overrides?.source_url ?? 'https://ahdb.org.uk/farm-business',
    copyright: 'Data: Crown Copyright, AHDB, HMRC. Server: Apache-2.0 Ansvar Systems.',
    server: 'dk-farm-planning-mcp',
    version: '0.1.0',
    ...overrides,
  };
}
