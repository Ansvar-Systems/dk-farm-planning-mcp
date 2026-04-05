import { buildMeta } from '../metadata.js';
import { validateJurisdiction } from '../jurisdiction.js';
import type { Database } from '../db.js';

interface TenancyArgs {
  tenancy_type?: string;
  topic?: string;
  jurisdiction?: string;
}

export function handleGetTenancyRules(db: Database, args: TenancyArgs) {
  const jv = validateJurisdiction(args.jurisdiction);
  if (!jv.valid) return jv.error;

  let sql = `SELECT * FROM tenancy_rules WHERE jurisdiction = ?`;
  const params: unknown[] = [jv.jurisdiction];

  if (args.tenancy_type) {
    sql += ' AND LOWER(tenancy_type) = LOWER(?)';
    params.push(args.tenancy_type);
  }

  if (args.topic) {
    sql += ' AND (LOWER(topic) LIKE ? OR LOWER(rule) LIKE ?)';
    const term = `%${args.topic.toLowerCase()}%`;
    params.push(term, term);
  }

  sql += ' ORDER BY tenancy_type, topic';

  const results = db.all<{
    id: number; tenancy_type: string; topic: string; rule: string;
    conditions: string; act_section: string; jurisdiction: string;
  }>(sql, params);

  if (results.length === 0) {
    return {
      error: 'not_found',
      message: `No tenancy rules found` +
        (args.tenancy_type ? ` for type '${args.tenancy_type}'` : '') +
        (args.topic ? ` on topic '${args.topic}'` : '') +
        `. Try tenancy_type: AHA_1986 or ATA_1995. Topics: succession, rent, compensation, termination.`,
    };
  }

  return {
    tenancy_type: args.tenancy_type ?? 'all',
    topic: args.topic ?? 'all',
    jurisdiction: jv.jurisdiction,
    results_count: results.length,
    results: results.map(r => ({
      tenancy_type: r.tenancy_type,
      topic: r.topic,
      rule: r.rule,
      conditions: r.conditions,
      act_section: r.act_section,
    })),
    _meta: buildMeta({ source_url: 'https://www.legislation.gov.uk' }),
  };
}
