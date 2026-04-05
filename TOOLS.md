# Tools Reference

## Meta Tools

### `about`

Get server metadata: name, version, coverage, data sources, and links.

**Parameters:** None

**Returns:** Server name, version, jurisdiction list, data source names, tool count, homepage/repository links.

---

### `list_sources`

List all data sources with authority, URL, license, and freshness info.

**Parameters:** None

**Returns:** Array of data sources, each with `name`, `authority`, `official_url`, `retrieval_method`, `update_frequency`, `license`, `coverage`, `last_retrieved`.

---

### `check_data_freshness`

Check when data was last ingested, staleness status, and how to trigger a refresh.

**Parameters:** None

**Returns:** `status` (fresh/stale/unknown), `last_ingest`, `days_since_ingest`, `staleness_threshold_days`, `refresh_command`.

---

## Domain Tools

### `search_farm_planning`

Full-text search across all farm planning data: rotation, margins, tax, APR, tenancy, diversification.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `query` | string | Yes | Free-text search query |
| `topic` | string | No | Filter by topic (e.g. rotation, tax, tenancy, diversification, margins, apr) |
| `jurisdiction` | string | No | ISO 3166-1 alpha-2 code (default: GB) |
| `limit` | number | No | Max results (default: 20, max: 50) |

**Example:** `{ "query": "blackgrass continuous wheat" }`

---

### `get_rotation_guidance`

Get crop rotation guidance: suitability, disease breaks, blackgrass risk, yield impact. Query a single crop or a rotation pair.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `crops` | string | Yes | Crop name or comma-separated pair (e.g. "winter wheat,oilseed rape") |
| `soil_type` | string | No | Soil type for context (not yet filtered) |
| `jurisdiction` | string | No | ISO 3166-1 alpha-2 code (default: GB) |

**Returns:** Suitability rating, reason, disease break years, blackgrass risk level, yield impact percentage.

**Example:** `{ "crops": "winter wheat,beans" }`

---

### `get_gross_margins`

Get gross margin benchmarks for a farm enterprise: output, variable costs, GM, top/bottom quartile.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `enterprise` | string | Yes | Enterprise name (e.g. winter wheat, dairy cow, lowland ewe) |
| `year` | string | No | Year (e.g. 2024/25). Default: latest available |
| `jurisdiction` | string | No | ISO 3166-1 alpha-2 code (default: GB) |

**Returns:** Output per unit, variable costs, gross margin, top/bottom quartile, unit (per_ha, per_head, etc.).

**Example:** `{ "enterprise": "dairy cow" }`

---

### `get_tax_rules`

Get agricultural tax rules by topic: Making Tax Digital, farmers averaging, capital allowances, VAT, partnership, inheritance.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `topic` | string | Yes | Tax topic keyword (e.g. MTD, averaging, capital allowances, VAT, partnership, inheritance) |
| `jurisdiction` | string | No | ISO 3166-1 alpha-2 code (default: GB) |

**Returns:** Rule description, conditions, deadlines, penalties, HMRC reference.

**Example:** `{ "topic": "averaging" }`

---

### `get_apr_guidance`

Get Agricultural Property Relief (IHT) guidance: relief rates (100% or 50%), occupation tests, clawback rules, farmhouse proportionality.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `scenario` | string | No | APR scenario (e.g. owner-occupier, tenanted, farmhouse, trust, company, diversified). Omit for all. |
| `jurisdiction` | string | No | ISO 3166-1 alpha-2 code (default: GB) |

**Returns:** Scenario, relief available (boolean), relief rate (100%/50%/none), conditions, occupation test, clawback period, HMRC reference.

**Example:** `{ "scenario": "farmhouse" }`

---

### `get_tenancy_rules`

Get agricultural tenancy rules: AHA 1986 (lifetime security, succession) vs ATA 1995 FBT (fixed term, no succession).

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `tenancy_type` | string | No | `AHA_1986` or `ATA_1995` |
| `topic` | string | No | Topic (e.g. succession, rent, compensation, termination, Gladstone) |
| `jurisdiction` | string | No | ISO 3166-1 alpha-2 code (default: GB) |

**Returns:** Tenancy type, topic, rule description, conditions, act section reference.

**Example:** `{ "tenancy_type": "AHA_1986", "topic": "succession" }`

---

### `get_diversification_guidance`

Get farm diversification guidance: permitted development classes (Q, R, S), floor area limits, business rates impact, planning requirements.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `activity` | string | No | Activity keyword (e.g. housing, farm shop, camping, solar, school). Omit for all. |
| `jurisdiction` | string | No | ISO 3166-1 alpha-2 code (default: GB) |

**Returns:** Activity, PD class, max floor area (m2), business rates impact, planning notes.

**Example:** `{ "activity": "housing" }`

---

### `calculate_break_even`

Calculate break-even for a farm enterprise using gross margin benchmarks. Shows costs vs output and whether the enterprise is profitable at benchmark levels.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `enterprise` | string | Yes | Enterprise name (e.g. winter wheat, dairy cow) |
| `fixed_costs` | number | No | Fixed costs per unit (GBP). Default: 0 |
| `variable_costs` | number | No | Override variable costs per unit (GBP). Default: from database |
| `yield` | number | No | Override yield per unit (not yet used) |
| `jurisdiction` | string | No | ISO 3166-1 alpha-2 code (default: GB) |

**Returns:** Cost breakdown, benchmark comparison, break-even output, margin, profitability flag.

**Example:** `{ "enterprise": "winter wheat", "fixed_costs": 400 }`
