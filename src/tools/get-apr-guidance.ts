import { buildMeta } from '../metadata.js';
import { validateJurisdiction } from '../jurisdiction.js';
import type { Database } from '../db.js';

interface AprArgs {
  scenario?: string;
  jurisdiction?: string;
}

export function handleGetAprGuidance(db: Database, args: AprArgs) {
  const jv = validateJurisdiction(args.jurisdiction);
  if (!jv.valid) return jv.error;

  let sql = `SELECT * FROM apr_guidance WHERE jurisdiction = ?`;
  const params: unknown[] = [jv.jurisdiction];

  if (args.scenario) {
    sql += ' AND (LOWER(scenario) LIKE ? OR LOWER(conditions) LIKE ? OR LOWER(notes) LIKE ?)';
    const term = `%${args.scenario.toLowerCase()}%`;
    params.push(term, term, term);
  }

  sql += ' ORDER BY scenario';

  const results = db.all<{
    id: number; scenario: string; relief_available: number; conditions: string;
    occupation_test: string; clawback_period: string; notes: string;
    hmrc_ref: string; jurisdiction: string;
  }>(sql, params);

  if (results.length === 0) {
    return {
      error: 'not_found',
      message: `No APR guidance found` + (args.scenario ? ` for '${args.scenario}'` : '') +
        `. Try: owner-occupier, tenanted, farmhouse, cottage, trust, company, diversified.`,
    };
  }

  return {
    scenario: args.scenario ?? 'all',
    jurisdiction: jv.jurisdiction,
    results_count: results.length,
    results: results.map(r => ({
      scenario: r.scenario,
      relief_available: r.relief_available === 1,
      relief_rate: r.relief_available === 1
        ? (r.notes?.includes('50%') ? '50%' : '100%')
        : 'none',
      conditions: r.conditions,
      occupation_test: r.occupation_test,
      clawback_period: r.clawback_period,
      notes: r.notes,
      hmrc_ref: r.hmrc_ref,
    })),
    _meta: buildMeta({ source_url: 'https://www.gov.uk/inheritance-tax/agricultural-relief' }),
  };
}
