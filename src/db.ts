import BetterSqlite3 from 'better-sqlite3';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';

export interface Database {
  get<T>(sql: string, params?: unknown[]): T | undefined;
  all<T>(sql: string, params?: unknown[]): T[];
  run(sql: string, params?: unknown[]): void;
  close(): void;
  readonly instance: BetterSqlite3.Database;
}

export function createDatabase(dbPath?: string): Database {
  const resolvedPath =
    dbPath ??
    join(dirname(fileURLToPath(import.meta.url)), '..', 'data', 'database.db');
  const db = new BetterSqlite3(resolvedPath);

  db.pragma('journal_mode = DELETE');
  db.pragma('foreign_keys = ON');

  initSchema(db);

  return {
    get<T>(sql: string, params: unknown[] = []): T | undefined {
      return db.prepare(sql).get(...params) as T | undefined;
    },
    all<T>(sql: string, params: unknown[] = []): T[] {
      return db.prepare(sql).all(...params) as T[];
    },
    run(sql: string, params: unknown[] = []): void {
      db.prepare(sql).run(...params);
    },
    close(): void {
      db.close();
    },
    get instance() {
      return db;
    },
  };
}

function initSchema(db: BetterSqlite3.Database): void {
  db.exec(`
    CREATE TABLE IF NOT EXISTS rotation_guidance (
      id INTEGER PRIMARY KEY,
      crop TEXT NOT NULL,
      following_crop TEXT,
      suitability TEXT,
      reason TEXT,
      disease_break_years INTEGER,
      blackgrass_risk TEXT,
      yield_impact_pct REAL,
      source TEXT,
      jurisdiction = 'DK'
    );

    CREATE TABLE IF NOT EXISTS gross_margins (
      id INTEGER PRIMARY KEY,
      enterprise TEXT NOT NULL,
      year TEXT,
      output_per_unit REAL,
      variable_costs_per_unit REAL,
      gross_margin_per_unit REAL,
      unit TEXT,
      top_quartile REAL,
      bottom_quartile REAL,
      source TEXT,
      jurisdiction = 'DK'
    );

    CREATE TABLE IF NOT EXISTS tax_rules (
      id INTEGER PRIMARY KEY,
      topic TEXT NOT NULL,
      rule TEXT NOT NULL,
      conditions TEXT,
      deadlines TEXT,
      penalties TEXT,
      hmrc_ref TEXT,
      jurisdiction = 'DK'
    );

    CREATE TABLE IF NOT EXISTS apr_guidance (
      id INTEGER PRIMARY KEY,
      scenario TEXT NOT NULL,
      relief_available INTEGER,
      conditions TEXT,
      occupation_test TEXT,
      clawback_period TEXT,
      notes TEXT,
      hmrc_ref TEXT,
      jurisdiction = 'DK'
    );

    CREATE TABLE IF NOT EXISTS tenancy_rules (
      id INTEGER PRIMARY KEY,
      tenancy_type TEXT NOT NULL,
      topic TEXT NOT NULL,
      rule TEXT NOT NULL,
      conditions TEXT,
      act_section TEXT,
      jurisdiction = 'DK'
    );

    CREATE TABLE IF NOT EXISTS diversification (
      id INTEGER PRIMARY KEY,
      activity TEXT NOT NULL,
      pd_class TEXT,
      max_floor_area_m2 REAL,
      business_rates_impact TEXT,
      planning_notes TEXT,
      jurisdiction = 'DK'
    );

    CREATE VIRTUAL TABLE IF NOT EXISTS search_index USING fts5(
      title, body, topic, jurisdiction
    );

    CREATE TABLE IF NOT EXISTS db_metadata (
      key TEXT PRIMARY KEY,
      value TEXT
    );

    INSERT OR IGNORE INTO db_metadata (key, value) VALUES ('schema_version', '1.0');
    INSERT OR IGNORE INTO db_metadata (key, value) VALUES ('mcp_name', 'Denmark Farm Planning MCP');
    INSERT OR IGNORE INTO db_metadata (key, value) VALUES ('jurisdiction = 'DK');
  `);
}

export function ftsSearch(
  db: Database,
  query: string,
  limit: number = 20
): { title: string; body: string; topic: string; jurisdiction: string; rank: number }[] {
  return db.all(
    `SELECT title, body, topic, jurisdiction, rank
     FROM search_index
     WHERE search_index MATCH ?
     ORDER BY rank
     LIMIT ?`,
    [query, limit]
  );
}
