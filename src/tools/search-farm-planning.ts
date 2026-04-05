import { buildMeta } from '../metadata.js';
import { validateJurisdiction } from '../jurisdiction.js';
import { ftsSearch, type Database } from '../db.js';

interface SearchArgs {
  query: string;
  topic?: string;
  jurisdiction?: string;
  limit?: number;
}

export function handleSearchFarmPlanning(db: Database, args: SearchArgs) {
  const jv = validateJurisdiction(args.jurisdiction);
  if (!jv.valid) return jv.error;

  const limit = Math.min(args.limit ?? 20, 50);
  let results = ftsSearch(db, args.query, limit);

  if (args.topic) {
    results = results.filter(r => r.topic.toLowerCase() === args.topic!.toLowerCase());
  }

  return {
    query: args.query,
    jurisdiction: jv.jurisdiction,
    results_count: results.length,
    results: results.map(r => ({
      title: r.title,
      body: r.body,
      topic: r.topic,
      relevance_rank: r.rank,
    })),
    _meta: buildMeta(),
  };
}
