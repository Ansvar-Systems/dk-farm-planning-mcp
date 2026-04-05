import { buildMeta } from '../metadata.js';
import { validateJurisdiction } from '../jurisdiction.js';
import type { Database } from '../db.js';

interface DiversificationArgs {
  activity?: string;
  jurisdiction?: string;
}

export function handleGetDiversificationGuidance(db: Database, args: DiversificationArgs) {
  const jv = validateJurisdiction(args.jurisdiction);
  if (!jv.valid) return jv.error;

  let sql = `SELECT * FROM diversification WHERE jurisdiction = ?`;
  const params: unknown[] = [jv.jurisdiction];

  if (args.activity) {
    sql += ' AND (LOWER(activity) LIKE ? OR LOWER(pd_class) LIKE ? OR LOWER(planning_notes) LIKE ?)';
    const term = `%${args.activity.toLowerCase()}%`;
    params.push(term, term, term);
  }

  sql += ' ORDER BY activity';

  const results = db.all<{
    id: number; activity: string; pd_class: string; max_floor_area_m2: number;
    business_rates_impact: string; planning_notes: string; jurisdiction: string;
  }>(sql, params);

  if (results.length === 0) {
    return {
      error: 'not_found',
      message: `No diversification guidance found` +
        (args.activity ? ` for '${args.activity}'` : '') +
        `. Try: housing, farm shop, camping, solar, school.`,
    };
  }

  return {
    activity: args.activity ?? 'all',
    jurisdiction: jv.jurisdiction,
    results_count: results.length,
    results: results.map(r => ({
      activity: r.activity,
      pd_class: r.pd_class,
      max_floor_area_m2: r.max_floor_area_m2,
      business_rates_impact: r.business_rates_impact,
      planning_notes: r.planning_notes,
    })),
    _meta: buildMeta({ source_url: 'https://www.legislation.gov.uk/uksi/2015/596' }),
  };
}
