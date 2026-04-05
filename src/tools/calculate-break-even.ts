import { buildMeta } from '../metadata.js';
import { validateJurisdiction } from '../jurisdiction.js';
import type { Database } from '../db.js';

interface BreakEvenArgs {
  enterprise: string;
  fixed_costs?: number;
  variable_costs?: number;
  yield?: number;
  jurisdiction?: string;
}

export function handleCalculateBreakEven(db: Database, args: BreakEvenArgs) {
  const jv = validateJurisdiction(args.jurisdiction);
  if (!jv.valid) return jv.error;

  // Look up gross margin data for the enterprise
  const gm = db.get<{
    enterprise: string; output_per_unit: number;
    variable_costs_per_unit: number; gross_margin_per_unit: number;
    unit: string; top_quartile: number; bottom_quartile: number;
  }>(
    `SELECT * FROM gross_margins WHERE LOWER(enterprise) LIKE ? AND jurisdiction = ? LIMIT 1`,
    [`%${args.enterprise.toLowerCase()}%`, jv.jurisdiction]
  );

  if (!gm) {
    return {
      error: 'not_found',
      message: `No gross margin data found for '${args.enterprise}'. Provide variable_costs and yield manually, or try: winter wheat, dairy cow, lowland ewe.`,
    };
  }

  // Use provided values or fall back to database values
  const variableCosts = args.variable_costs ?? gm.variable_costs_per_unit;
  const fixedCosts = args.fixed_costs ?? 0;
  const totalCosts = variableCosts + fixedCosts;

  // For per_ha enterprises, yield is output / price. We estimate break-even price.
  // Break-even price = total costs per unit (since output = price * yield, and GM data is already per unit)
  const breakEvenOutput = totalCosts;
  const currentOutput = gm.output_per_unit;
  const margin = currentOutput - totalCosts;

  // Calculate how much output needs to change to break even
  const breakEvenRatio = totalCosts / currentOutput;

  return {
    enterprise: gm.enterprise,
    unit: gm.unit,
    jurisdiction: jv.jurisdiction,
    costs: {
      variable_costs_per_unit: variableCosts,
      fixed_costs_per_unit: fixedCosts,
      total_costs_per_unit: Math.round(totalCosts * 100) / 100,
    },
    benchmark: {
      output_per_unit: gm.output_per_unit,
      gross_margin_per_unit: gm.gross_margin_per_unit,
      top_quartile_gm: gm.top_quartile,
      bottom_quartile_gm: gm.bottom_quartile,
    },
    break_even: {
      break_even_output_per_unit: Math.round(breakEvenOutput * 100) / 100,
      current_output_per_unit: currentOutput,
      margin_per_unit: Math.round(margin * 100) / 100,
      break_even_ratio: Math.round(breakEvenRatio * 1000) / 1000,
      profitable: margin > 0,
    },
    _meta: buildMeta({ source_url: 'https://ahdb.org.uk/farmbench' }),
  };
}
