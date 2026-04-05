# Coverage

## What Is Included

- **Crop rotation guidance**: 13+ rotation pairs with suitability ratings, disease break requirements, blackgrass risk levels, and yield impact estimates
- **Gross margins**: 13 enterprise types (arable + livestock) with output, variable costs, gross margin, and top/bottom quartile benchmarks (AHDB Farmbench-style, 2024/25)
- **Agricultural tax rules**: 8 topics covering MTD, farmers averaging, capital allowances, SBA, flat rate VAT, partnership tax, and IHT overview
- **Agricultural Property Relief (APR)**: 10 scenarios with relief rates (100%/50%), occupation tests, clawback periods, and HMRC references
- **Tenancy law**: 10 rules covering AHA 1986 (succession, rent, compensation, security) and ATA 1995 FBT (term, rent, compensation, termination), plus Gladstone v Bower and tenant right
- **Diversification**: 6 activities covering Class Q/R/S permitted development, camping, and solar, with floor area limits and business rates impact

## Jurisdictions

| Code | Country | Status |
|------|---------|--------|
| GB | Great Britain | Supported |

## What Is NOT Included

- **Scotland-specific tenancy rules** -- Scottish equivalents (Agricultural Holdings (Scotland) Acts) are not yet ingested
- **Northern Ireland** -- NI follows separate agricultural legislation
- **Real-time commodity prices** -- Gross margins are benchmark estimates, not live prices
- **Individual farm analysis** -- This is reference data, not a farm management tool
- **Environmental scheme payments** -- SFI, Countryside Stewardship rates not yet included
- **Land values** -- No land price or rental value data
- **Subsidy/support payment calculations** -- BPS/delinked payments not included
- **Welsh-specific planning rules** -- PD rights may differ in Wales

## Known Gaps

1. Gross margin data is indicative (based on published AHDB benchmarks) -- individual farm results vary widely
2. Tax rules are a summary, not a substitute for professional advice -- thresholds and rates change in each Budget
3. APR rules are complex in practice -- HMRC interpretation and case law evolve
4. Tenancy rules are simplified -- real disputes involve detailed statutory interpretation
5. Diversification PD rights are subject to local planning authority interpretation and prior approval conditions

## Data Freshness

Run `check_data_freshness` to see when data was last updated. The ingestion pipeline runs monthly; manual triggers available via `gh workflow run ingest.yml`.
