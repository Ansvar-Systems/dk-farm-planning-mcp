# Denmark Farm Planning MCP

[![CI](https://github.com/Ansvar-Systems/dk-farm-planning-mcp/actions/workflows/ci.yml/badge.svg)](https://github.com/Ansvar-Systems/dk-farm-planning-mcp/actions/workflows/ci.yml)
[![GHCR](https://github.com/Ansvar-Systems/dk-farm-planning-mcp/actions/workflows/ghcr-build.yml/badge.svg)](https://github.com/Ansvar-Systems/dk-farm-planning-mcp/actions/workflows/ghcr-build.yml)
[![License: Apache-2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

UK farm business planning data via the [Model Context Protocol](https://modelcontextprotocol.io). Crop rotation guidance, gross margins, agricultural tax rules, APR for inheritance tax, tenancy law (AHA 1986 vs ATA 1995), diversification permitted development, and break-even calculations -- all from your AI assistant.

Part of [Ansvar Open Agriculture](https://ansvar.eu/open-agriculture).

## Why This Exists

UK farm businesses need quick access to rotation planning data, gross margin benchmarks, tax rules (Making Tax Digital, farmers averaging, APR), tenancy law comparisons, and diversification planning rules. This information is spread across AHDB publications, HMRC manuals, and legislation. This MCP server makes it all searchable from any AI assistant.

## Quick Start

### Claude Desktop

Add to `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "uk-farm-planning": {
      "command": "npx",
      "args": ["-y", "@ansvar/dk-farm-planning-mcp"]
    }
  }
}
```

### Claude Code

```bash
claude mcp add uk-farm-planning npx @ansvar/dk-farm-planning-mcp
```

### Streamable HTTP (remote)

```
https://mcp.ansvar.eu/dk-farm-planning/mcp
```

### Docker (self-hosted)

```bash
docker run -p 3000:3000 ghcr.io/ansvar-systems/dk-farm-planning-mcp:latest
```

### npm (stdio)

```bash
npx @ansvar/dk-farm-planning-mcp
```

## Example Queries

Ask your AI assistant:

- "What should I grow after oilseed rape?"
- "What's the gross margin for winter wheat this year?"
- "Explain the APR rules for a farmhouse"
- "Compare AHA 1986 succession rights vs ATA 1995"
- "Can I convert my barn to dwellings under permitted development?"
- "What are the Making Tax Digital deadlines for farmers?"
- "Is my dairy enterprise profitable at benchmark costs?"

## Stats

| Metric | Value |
|--------|-------|
| Tools | 11 (3 meta + 8 domain) |
| Jurisdiction | GB |
| Data sources | AHDB Farmbench, HMRC, AHA 1986, ATA 1995, GPDO 2015 |
| License (data) | Open Government Licence v3 / Crown Copyright |
| License (code) | Apache-2.0 |
| Transport | stdio + Streamable HTTP |

## Tools

| Tool | Description |
|------|-------------|
| `about` | Server metadata and links |
| `list_sources` | Data sources with freshness info |
| `check_data_freshness` | Staleness status and refresh command |
| `search_farm_planning` | FTS5 search across all farm planning data |
| `get_rotation_guidance` | Crop rotation suitability, disease breaks, blackgrass risk |
| `get_gross_margins` | Enterprise gross margins with quartile benchmarks |
| `get_tax_rules` | Agricultural tax rules (MTD, averaging, allowances, VAT) |
| `get_apr_guidance` | Agricultural Property Relief for IHT (100%/50%, occupation tests) |
| `get_tenancy_rules` | AHA 1986 vs ATA 1995 tenancy rules |
| `get_diversification_guidance` | Permitted development (Class Q/R/S), planning requirements |
| `calculate_break_even` | Break-even analysis using gross margin benchmarks |

See [TOOLS.md](TOOLS.md) for full parameter documentation.

## Security Scanning

This repository runs 6 security checks on every push:

- **CodeQL** -- static analysis for JavaScript/TypeScript
- **Gitleaks** -- secret detection across full history
- **Dependency review** -- via Dependabot
- **Container scanning** -- via GHCR build pipeline

See [SECURITY.md](SECURITY.md) for reporting policy.

## Disclaimer

This tool provides reference data for informational purposes only. It is not professional agricultural, tax, or legal advice. Tax rules change frequently -- always verify with HMRC or a qualified agricultural accountant. Tenancy disputes should involve a specialist agricultural solicitor. See [DISCLAIMER.md](DISCLAIMER.md).

## Contributing

Issues and pull requests welcome. For security vulnerabilities, email security@ansvar.eu (do not open a public issue).

## License

Apache-2.0. Data sourced under Open Government Licence v3 and Crown Copyright.
