import { buildMeta } from '../metadata.js';
import { validateJurisdiction } from '../jurisdiction.js';
import type { Database } from '../db.js';

interface RotationArgs {
  crops: string;
  soil_type?: string;
  jurisdiction?: string;
}

interface RotationRow {
  id: number; crop: string; following_crop: string; suitability: string;
  reason: string; disease_break_years: number; blackgrass_risk: string;
  yield_impact_pct: number; source: string; jurisdiction: string;
}

export function handleGetRotationGuidance(db: Database, args: RotationArgs) {
  const jv = validateJurisdiction(args.jurisdiction);
  if (!jv.valid) return jv.error;

  const cropParts = args.crops.split(',').map(c => c.trim().toLowerCase());

  let results: RotationRow[];

  if (cropParts.length >= 2) {
    // Two crops: look for specific rotation pair
    results = db.all<RotationRow>(
      `SELECT * FROM rotation_guidance
       WHERE LOWER(crop) = ? AND LOWER(following_crop) = ? AND jurisdiction = ?`,
      [cropParts[0], cropParts[1], jv.jurisdiction]
    );

    // Also check reverse direction
    if (results.length === 0) {
      results = db.all<RotationRow>(
        `SELECT * FROM rotation_guidance
         WHERE LOWER(crop) = ? AND LOWER(following_crop) = ? AND jurisdiction = ?`,
        [cropParts[1], cropParts[0], jv.jurisdiction]
      );
    }
  } else {
    // Single crop: return all rotation guidance involving this crop
    results = db.all<RotationRow>(
      `SELECT * FROM rotation_guidance
       WHERE (LOWER(crop) = ? OR LOWER(following_crop) = ?) AND jurisdiction = ?`,
      [cropParts[0], cropParts[0], jv.jurisdiction]
    );
  }

  if (results.length === 0) {
    return {
      error: 'not_found',
      message: `No rotation guidance found for '${args.crops}'. Try a common UK arable crop name (e.g. winter wheat, oilseed rape, spring barley).`,
    };
  }

  return {
    query: args.crops,
    jurisdiction: jv.jurisdiction,
    results_count: results.length,
    results: results.map(r => ({
      crop: r.crop,
      following_crop: r.following_crop,
      suitability: r.suitability,
      reason: r.reason,
      disease_break_years: r.disease_break_years,
      blackgrass_risk: r.blackgrass_risk,
      yield_impact_pct: r.yield_impact_pct,
      source: r.source,
    })),
    _meta: buildMeta({ source_url: 'https://ahdb.org.uk/knowledge-library/rotation' }),
  };
}
