import { buildMeta } from '../metadata.js';
import { validateJurisdiction } from '../jurisdiction.js';
import type { Database } from '../db.js';

interface GrossMarginArgs {
  enterprise: string;
  year?: string;
  jurisdiction?: string;
}

export function handleGetGrossMargins(db: Database, args: GrossMarginArgs) {
  const jv = validateJurisdiction(args.jurisdiction);
  if (!jv.valid) return jv.error;

  let sql = `SELECT * FROM gross_margins WHERE LOWER(enterprise) LIKE ? AND jurisdiction = ?`;
  const params: unknown[] = [`%${args.enterprise.toLowerCase()}%`, jv.jurisdiction];

  if (args.year) {
    sql += ' AND year = ?';
    params.push(args.year);
  }

  sql += ' ORDER BY enterprise';

  const results = db.all<{
    id: number; enterprise: string; year: string;
    output_per_unit: number; variable_costs_per_unit: number;
    gross_margin_per_unit: number; unit: string;
    top_quartile: number; bottom_quartile: number;
    source: string; jurisdiction: string;
  }>(sql, params);

  if (results.length === 0) {
    return {
      error: 'not_found',
      message: `No gross margin data found for '${args.enterprise}'. Try: winter wheat, dairy cow, lowland ewe, potatoes, etc.`,
    };
  }

  return {
    enterprise: args.enterprise,
    jurisdiction: jv.jurisdiction,
    results_count: results.length,
    results: results.map(r => ({
      enterprise: r.enterprise,
      year: r.year,
      output_per_unit: r.output_per_unit,
      variable_costs_per_unit: r.variable_costs_per_unit,
      gross_margin_per_unit: r.gross_margin_per_unit,
      unit: r.unit,
      top_quartile: r.top_quartile,
      bottom_quartile: r.bottom_quartile,
      source: r.source,
    })),
    _meta: buildMeta({ source_url: 'https://ahdb.org.uk/farmbench' }),
  };
}
