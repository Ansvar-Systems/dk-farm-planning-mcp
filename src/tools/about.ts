import { buildMeta } from '../metadata.js';
import { SUPPORTED_JURISDICTIONS } from '../jurisdiction.js';

export function handleAbout() {
  return {
    name: 'Denmark Farm Planning MCP',
    description:
      'UK farm business planning data: crop rotation guidance, gross margins (AHDB Farmbench-style), ' +
      'agricultural tax rules (Making Tax Digital, farmers averaging), Agricultural Property Relief for IHT, ' +
      'tenancy law (AHA 1986 vs ATA 1995 FBT), diversification permitted development, and break-even calculations.',
    version: '0.1.0',
    jurisdiction: [...SUPPORTED_JURISDICTIONS],
    data_sources: [
      'AHDB Farmbench / Pocketbook',
      'HMRC Manuals and Guidance',
      'Agricultural Holdings Act 1986',
      'Agricultural Tenancies Act 1995',
      'Town and Country Planning (General Permitted Development) Order 2015',
    ],
    tools_count: 11,
    links: {
      homepage: 'https://ansvar.eu/open-agriculture',
      repository: 'https://github.com/Ansvar-Systems/dk-farm-planning-mcp',
      mcp_network: 'https://ansvar.ai/mcp',
    },
    _meta: buildMeta(),
  };
}
