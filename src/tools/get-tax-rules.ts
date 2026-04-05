import { buildMeta } from '../metadata.js';
import { validateJurisdiction } from '../jurisdiction.js';
import type { Database } from '../db.js';

interface TaxRulesArgs {
  topic: string;
  jurisdiction?: string;
}

export function handleGetTaxRules(db: Database, args: TaxRulesArgs) {
  const jv = validateJurisdiction(args.jurisdiction);
  if (!jv.valid) return jv.error;

  const results = db.all<{
    id: number; topic: string; rule: string; conditions: string;
    deadlines: string; penalties: string; hmrc_ref: string; jurisdiction: string;
  }>(
    `SELECT * FROM tax_rules
     WHERE (LOWER(topic) LIKE ? OR LOWER(rule) LIKE ?) AND jurisdiction = ?
     ORDER BY topic`,
    [`%${args.topic.toLowerCase()}%`, `%${args.topic.toLowerCase()}%`, jv.jurisdiction]
  );

  if (results.length === 0) {
    return {
      error: 'not_found',
      message: `No tax rules found for '${args.topic}'. Try: MTD, averaging, capital allowances, VAT, partnership, inheritance.`,
    };
  }

  return {
    topic: args.topic,
    jurisdiction: jv.jurisdiction,
    results_count: results.length,
    results: results.map(r => ({
      topic: r.topic,
      rule: r.rule,
      conditions: r.conditions,
      deadlines: r.deadlines,
      penalties: r.penalties,
      hmrc_ref: r.hmrc_ref,
    })),
    _meta: buildMeta({ source_url: 'https://www.gov.uk/hmrc-internal-manuals' }),
  };
}
