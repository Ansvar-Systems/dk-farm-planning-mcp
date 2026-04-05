export interface Meta {
  disclaimer: string;
  data_age: string;
  source_url: string;
  copyright: string;
  server: string;
  version: string;
}

const DISCLAIMER =
  'Data er vejledende og baseret på SEGES Farmtal Online, Skattestyrelsen og DLBR. ' +
  'Skatteregler ændres løbende — bekræft altid med Skattestyrelsen eller en kvalificeret landbrugsrevisor. ' +
  'Forpagtningsspørgsmål bør rettes til en specialiseret landbrugsadvokat. ' +
  'Dækningsbidrag er vejledende nøgletal, ikke prognoser.';

export function buildMeta(overrides?: Partial<Meta>): Meta {
  return {
    disclaimer: DISCLAIMER,
    data_age: overrides?.data_age ?? 'unknown',
    source_url: overrides?.source_url ?? 'https://farmtalonline.dk/',
    copyright: 'Data: SEGES Innovation, Skattestyrelsen, DLBR. Server: Apache-2.0 Ansvar Systems.',
    server: 'dk-farm-planning-mcp',
    version: '0.1.0',
    ...overrides,
  };
}
