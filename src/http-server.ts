import { createServer, type IncomingMessage, type ServerResponse } from 'http';
import { randomUUID } from 'crypto';
import { Server } from '@modelcontextprotocol/sdk/server/index.js';
import { StreamableHTTPServerTransport } from '@modelcontextprotocol/sdk/server/streamableHttp.js';
import {
  ListToolsRequestSchema,
  CallToolRequestSchema,
} from '@modelcontextprotocol/sdk/types.js';
import { z } from 'zod';
import { createDatabase, type Database } from './db.js';
import { handleAbout } from './tools/about.js';
import { handleListSources } from './tools/list-sources.js';
import { handleCheckFreshness } from './tools/check-freshness.js';
import { handleSearchFarmPlanning } from './tools/search-farm-planning.js';
import { handleGetRotationGuidance } from './tools/get-rotation-guidance.js';
import { handleGetGrossMargins } from './tools/get-gross-margins.js';
import { handleGetTaxRules } from './tools/get-tax-rules.js';
import { handleGetAprGuidance } from './tools/get-apr-guidance.js';
import { handleGetTenancyRules } from './tools/get-tenancy-rules.js';
import { handleGetDiversificationGuidance } from './tools/get-diversification-guidance.js';
import { handleCalculateBreakEven } from './tools/calculate-break-even.js';

const SERVER_NAME = 'dk-farm-planning-mcp';
const SERVER_VERSION = '0.1.0';
const PORT = parseInt(process.env.PORT ?? '3000', 10);

const SearchArgsSchema = z.object({
  query: z.string(),
  topic: z.string().optional(),
  jurisdiction: z.string().optional(),
  limit: z.number().optional(),
});

const RotationArgsSchema = z.object({
  crops: z.string(),
  soil_type: z.string().optional(),
  jurisdiction: z.string().optional(),
});

const GrossMarginArgsSchema = z.object({
  enterprise: z.string(),
  year: z.string().optional(),
  jurisdiction: z.string().optional(),
});

const TaxRulesArgsSchema = z.object({
  topic: z.string(),
  jurisdiction: z.string().optional(),
});

const AprArgsSchema = z.object({
  scenario: z.string().optional(),
  jurisdiction: z.string().optional(),
});

const TenancyArgsSchema = z.object({
  tenancy_type: z.string().optional(),
  topic: z.string().optional(),
  jurisdiction: z.string().optional(),
});

const DiversificationArgsSchema = z.object({
  activity: z.string().optional(),
  jurisdiction: z.string().optional(),
});

const BreakEvenArgsSchema = z.object({
  enterprise: z.string(),
  fixed_costs: z.number().optional(),
  variable_costs: z.number().optional(),
  yield: z.number().optional(),
  jurisdiction: z.string().optional(),
});

const TOOLS = [
  {
    name: 'about',
    description: 'Get server metadata: name, version, coverage, data sources, and links.',
    inputSchema: { type: 'object' as const, properties: {} },
  },
  {
    name: 'list_sources',
    description: 'List all data sources with authority, URL, license, and freshness info.',
    inputSchema: { type: 'object' as const, properties: {} },
  },
  {
    name: 'check_data_freshness',
    description: 'Check when data was last ingested, staleness status, and how to trigger a refresh.',
    inputSchema: { type: 'object' as const, properties: {} },
  },
  {
    name: 'search_farm_planning',
    description: 'Full-text search across all farm planning data: rotation, margins, tax, APR, tenancy, diversification.',
    inputSchema: {
      type: 'object' as const,
      properties: {
        query: { type: 'string', description: 'Free-text search query' },
        topic: { type: 'string', description: 'Filter by topic (e.g. rotation, tax, tenancy, diversification)' },
        jurisdiction: { type: 'string', description: 'ISO 3166-1 alpha-2 code (default: GB)' },
        limit: { type: 'number', description: 'Max results (default: 20, max: 50)' },
      },
      required: ['query'],
    },
  },
  {
    name: 'get_rotation_guidance',
    description: 'Get crop rotation guidance: suitability, disease breaks, blackgrass risk, yield impact. Query a single crop or a pair (e.g. "winter wheat,oilseed rape").',
    inputSchema: {
      type: 'object' as const,
      properties: {
        crops: { type: 'string', description: 'Crop name or comma-separated pair (e.g. "winter wheat,oilseed rape")' },
        soil_type: { type: 'string', description: 'Soil type for context (optional, not yet filtered)' },
        jurisdiction: { type: 'string', description: 'ISO 3166-1 alpha-2 code (default: GB)' },
      },
      required: ['crops'],
    },
  },
  {
    name: 'get_gross_margins',
    description: 'Get gross margin benchmarks for a farm enterprise: output, variable costs, GM, top/bottom quartile.',
    inputSchema: {
      type: 'object' as const,
      properties: {
        enterprise: { type: 'string', description: 'Enterprise name (e.g. winter wheat, dairy cow, lowland ewe)' },
        year: { type: 'string', description: 'Year (e.g. 2024/25). Default: latest available.' },
        jurisdiction: { type: 'string', description: 'ISO 3166-1 alpha-2 code (default: GB)' },
      },
      required: ['enterprise'],
    },
  },
  {
    name: 'get_tax_rules',
    description: 'Get agricultural tax rules by topic: Making Tax Digital, farmers averaging, capital allowances, VAT, partnership, inheritance.',
    inputSchema: {
      type: 'object' as const,
      properties: {
        topic: { type: 'string', description: 'Tax topic keyword (e.g. MTD, averaging, capital allowances, VAT)' },
        jurisdiction: { type: 'string', description: 'ISO 3166-1 alpha-2 code (default: GB)' },
      },
      required: ['topic'],
    },
  },
  {
    name: 'get_apr_guidance',
    description: 'Get Agricultural Property Relief (IHT) guidance: relief rates, occupation tests, clawback rules, farmhouse proportionality.',
    inputSchema: {
      type: 'object' as const,
      properties: {
        scenario: { type: 'string', description: 'APR scenario (e.g. owner-occupier, tenanted, farmhouse, trust, diversified)' },
        jurisdiction: { type: 'string', description: 'ISO 3166-1 alpha-2 code (default: GB)' },
      },
    },
  },
  {
    name: 'get_tenancy_rules',
    description: 'Get agricultural tenancy rules: AHA 1986 (lifetime security, succession) vs ATA 1995 FBT (fixed term). Topics: succession, rent, compensation, termination.',
    inputSchema: {
      type: 'object' as const,
      properties: {
        tenancy_type: { type: 'string', description: 'AHA_1986 or ATA_1995' },
        topic: { type: 'string', description: 'Topic (e.g. succession, rent, compensation, termination)' },
        jurisdiction: { type: 'string', description: 'ISO 3166-1 alpha-2 code (default: GB)' },
      },
    },
  },
  {
    name: 'get_diversification_guidance',
    description: 'Get farm diversification guidance: permitted development classes (Q, R, S), floor area limits, business rates, planning requirements.',
    inputSchema: {
      type: 'object' as const,
      properties: {
        activity: { type: 'string', description: 'Activity (e.g. housing, farm shop, camping, solar, school)' },
        jurisdiction: { type: 'string', description: 'ISO 3166-1 alpha-2 code (default: GB)' },
      },
    },
  },
  {
    name: 'calculate_break_even',
    description: 'Calculate break-even for a farm enterprise using gross margin benchmarks. Shows costs vs output and whether the enterprise is profitable at benchmark levels.',
    inputSchema: {
      type: 'object' as const,
      properties: {
        enterprise: { type: 'string', description: 'Enterprise name (e.g. winter wheat, dairy cow)' },
        fixed_costs: { type: 'number', description: 'Fixed costs per unit (GBP). Default: 0' },
        variable_costs: { type: 'number', description: 'Override variable costs per unit (GBP). Default: from database' },
        yield: { type: 'number', description: 'Override yield per unit (not yet used in calculation)' },
        jurisdiction: { type: 'string', description: 'ISO 3166-1 alpha-2 code (default: GB)' },
      },
      required: ['enterprise'],
    },
  },
];

function textResult(data: unknown) {
  return { content: [{ type: 'text' as const, text: JSON.stringify(data, null, 2) }] };
}

function errorResult(message: string) {
  return { content: [{ type: 'text' as const, text: JSON.stringify({ error: message }) }], isError: true };
}

function registerTools(server: Server, db: Database): void {
  server.setRequestHandler(ListToolsRequestSchema, async () => ({ tools: TOOLS }));

  server.setRequestHandler(CallToolRequestSchema, async (request) => {
    const { name, arguments: args = {} } = request.params;

    try {
      switch (name) {
        case 'about':
          return textResult(handleAbout());
        case 'list_sources':
          return textResult(handleListSources(db));
        case 'check_data_freshness':
          return textResult(handleCheckFreshness(db));
        case 'search_farm_planning':
          return textResult(handleSearchFarmPlanning(db, SearchArgsSchema.parse(args)));
        case 'get_rotation_guidance':
          return textResult(handleGetRotationGuidance(db, RotationArgsSchema.parse(args)));
        case 'get_gross_margins':
          return textResult(handleGetGrossMargins(db, GrossMarginArgsSchema.parse(args)));
        case 'get_tax_rules':
          return textResult(handleGetTaxRules(db, TaxRulesArgsSchema.parse(args)));
        case 'get_apr_guidance':
          return textResult(handleGetAprGuidance(db, AprArgsSchema.parse(args)));
        case 'get_tenancy_rules':
          return textResult(handleGetTenancyRules(db, TenancyArgsSchema.parse(args)));
        case 'get_diversification_guidance':
          return textResult(handleGetDiversificationGuidance(db, DiversificationArgsSchema.parse(args)));
        case 'calculate_break_even':
          return textResult(handleCalculateBreakEven(db, BreakEvenArgsSchema.parse(args)));
        default:
          return errorResult(`Unknown tool: ${name}`);
      }
    } catch (err) {
      return errorResult(err instanceof Error ? err.message : String(err));
    }
  });
}

const db = createDatabase();
const sessions = new Map<string, { transport: StreamableHTTPServerTransport; server: Server }>();

function createMcpServer(): Server {
  const mcpServer = new Server(
    { name: SERVER_NAME, version: SERVER_VERSION },
    { capabilities: { tools: {} } }
  );
  registerTools(mcpServer, db);
  return mcpServer;
}

async function handleMCPRequest(req: IncomingMessage, res: ServerResponse): Promise<void> {
  const sessionId = req.headers['mcp-session-id'] as string | undefined;

  if (sessionId && sessions.has(sessionId)) {
    const session = sessions.get(sessionId)!;
    await session.transport.handleRequest(req, res);
    return;
  }

  if (req.method === 'GET' || req.method === 'DELETE') {
    res.writeHead(400, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ error: 'Invalid or missing session ID' }));
    return;
  }

  const mcpServer = createMcpServer();
  const transport = new StreamableHTTPServerTransport({
    sessionIdGenerator: () => randomUUID(),
  });

  await mcpServer.connect(transport);

  transport.onclose = () => {
    if (transport.sessionId) {
      sessions.delete(transport.sessionId);
    }
    mcpServer.close().catch(() => {});
  };

  await transport.handleRequest(req, res);

  if (transport.sessionId) {
    sessions.set(transport.sessionId, { transport, server: mcpServer });
  }
}

const httpServer = createServer(async (req, res) => {
  const url = new URL(req.url || '/', `http://localhost:${PORT}`);

  if (url.pathname === '/health' && req.method === 'GET') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ status: 'healthy', server: SERVER_NAME, version: SERVER_VERSION }));
    return;
  }

  if (url.pathname === '/mcp' || url.pathname === '/') {
    try {
      await handleMCPRequest(req, res);
    } catch (err) {
      if (!res.headersSent) {
        res.writeHead(500, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: err instanceof Error ? err.message : 'Internal server error' }));
      }
    }
    return;
  }

  res.writeHead(404, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify({ error: 'Not found' }));
});

httpServer.listen(PORT, () => {
  console.log(`${SERVER_NAME} v${SERVER_VERSION} listening on port ${PORT}`);
});
