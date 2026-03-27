# Data Center Adjacency Commercial Property Thesis — Design Spec

**Date:** 2026-03-26  
**Status:** Draft (internal review complete; pending human approval)  
**Authors:** Kartik + AI  
**Prior context:** Multifamily pipeline and Fund Explorer (`2026-03-25-data-pipeline-property-finder-design.md`, `2026-03-26-fund-explorer-ui-design.md`)

## Overview

A **national screening + thesis** track for commercial real estate opportunities adjacent to the data center build-out: **land banking**, **conversion** (industrial/warehouse to DC-adjacent use), and **infrastructure adjacency** (spillover demand near clusters). Focus **emerging Tier 2/3 markets** where power and political alignment exist but commercial property may not yet be fully repriced—explicitly **not** operating data centers.

Delivery is **hybrid**: static HTML narrative pages (credibility and LP-facing methodology) plus **Parquet → Firebase Storage → Fund Explorer** (DuckDB, map, SQL Studio) for the same scored truth.

## Goals

1. **Tier 1 market score** (county/metro): reproducible 0–100 composite from public data + Tavily intelligence, with industry-cited rationale per category.
2. **Tavily enrichment**: batched Search API queries with URL citations, staleness policy, optional Extract for stable government/utility pages; aligned with [Tavily Agent Skills](https://docs.tavily.com/documentation/agent-skills.md) and production guidance ([docs index](https://docs.tavily.com/llms.txt)).
3. **LP-facing methodology**: thesis page + Explorer “How we score” with **primary data links** and **validation links** (Kearney, Analytics.loan, CBRE, JLL, etc.).
4. **Phased delivery**: Phase 1 = national leaderboard + thesis + Explorer integration; Phase 2 = sub-county site suitability for top markets.

## Non-Goals (v1)

- Operating or leasing colocation/data center space as the fund strategy.
- Parcel-level acquisition modeling or MLS integration in Phase 1.
- Full replacement of static research pages with a single-page app (see “SPA note” below).

---

## Investment Thesis (Three Plays)

| Play | Description |
|------|-------------|
| **A — Land banking** | Raw or industrial-zoned land near major power and fiber corridors, sale/lease to DC developers or future industrial users. |
| **B — Conversion** | Existing commercial/industrial stock with strong utility service and footprint suitable for retrofit or secondary lease to DC-adjacent demand. |
| **C — Adjacency** | Commercial property benefiting from economic spillover near announced or operating DC clusters (services, logistics, limited office). |

Geography: **national screen**, emphasis on **emerging** markets (skip “priced in” **geographic Tier 1** DC hubs—e.g. saturated core markets—as primary targets unless drill-down finds pockets).

---

## Approach: Multi-Resolution Scoring

**Naming note:** “**Tier 1 market score**” means the **first resolution** of the model (county/metro). It is **not** the same as “geographic Tier 1” hubs in the paragraph above.

**Tier 1 — Market score (county/metro):** leaderboard for all US counties (or screened universe consistent with multifamily filters). Drives ranking and Fund Explorer choropleth.

**Phase 1 score shape:** Ship **one primary composite** `market_score` (0–100) per county, plus **explanatory subscores** per category column in Parquet (electrical, water, political, pipeline, etc.). Optionally add **lightweight play-affinity columns** in Phase 1 (e.g. `land_signal`, `conversion_signal`, `adjacency_signal` as 0–1 or percentile proxies derived from the same inputs) so the thesis can discuss A/B/C without waiting for Tier 2—exact columns **TBD in implementation plan**. Tier 2 adds **site-level** play fit and geospatial precision.

**Tier 2 — Site suitability (sub-county):** only for **top 20–30** markets from Tier 1; geocoded infrastructure and play-type alignment. Deferred to roadmap Phase 2 (see Phasing table below).

---

## Tier 1 Market Score (100 points + penalties)

Weights align with industry frameworks: combined **electrical + water/cooling ≈ 40%** (compare Kearney power-heavy index and Analytics.loan **Power 40%**); **pipeline** intentionally **lower than Analytics.loan’s 30%** to avoid biasing only toward already-hot markets.

### Category breakdown

| Category | Pts | Notes |
|----------|-----|--------|
| Electrical infrastructure | 30 | Rates, capacity headroom, SAIDI/SAIFI, planned generation, interconnection queue proxy |
| Water & cooling | 10 | Watershed stress; cooling degree days / climate proxy |
| Political & regulatory | 20 | Incentives, permitting/zoning stance, state energy policy, community acceptance |
| DC pipeline & momentum | 15 | Announced/under-construction MW, absorption signals, hyperscaler/NAICS 518 presence |
| Connectivity | 10 | Fiber density; IX / carrier hotel proximity (seed list + FCC) |
| Labor & cost | 10 | Construction trades, DC workforce concentration, land cost proxy |
| Unique advantages | 5 | Renewable proximity, disaster risk (FEMA NRI) |
| **Penalties** | **up to −10** | Moratorium/opposition, congestion/curtailment, interconnection freeze |

**Validation references (methodology UI must link):**

- [Kearney — AI Data Center Location Attractiveness Index](https://www.kearney.com/industry/technology/article/ai-data-center-location-attractiveness-index)
- [Analytics.loan — Data Center Site Readiness Scorecard](https://www.analytics.loan/post/data-center-site-readiness-scorecard-ranking-u-s-metros-on-power-land-pipeline) (Power 40%, Land 30%, Pipeline 30%)
- [CBRE — Global Data Center Trends](https://www.cbre.com/insights/reports/data-center-trends-2025)
- [JLL — Data Center Outlook](https://www.jll.com/en-us/insights/data-center-outlook.html)
- [Datacenters.com — Power, water, permits](https://www.datacenterenergy.com/news/power-water-and-permits-the-new-pillars-of-data-center-site-selection) (pillar framing)
- [GE Vernova — Smart data center site selection](https://www.gevernova.com/consulting/resources/articles/2025/smart-data-center-site-selection)
- [BDO — Strategic guide to data center site selection](https://www.bdo.com/insights/industries/technology/strategic-guide-to-data-center-site-selection)
- [Ramboll — Data center site selection criteria](https://www.ramboll.com/en-us/insights/resilient-societies-and-liveability/data-center-site-selection-criteria)

---

## Tier 2 Site Suitability (Phase 2)

100-point sub-model per zone/tract within selected markets:

- Power proximity (substation, transmission)
- Connectivity (fiber, IX)
- Land/building suitability (zoning, parcel size, flood)
- **Investment play alignment** (land / conversion / adjacency)—differentiator vs generic DC operator models

---

## Data Sources

### Structured public (Tier 1)

| Source | Use |
|--------|-----|
| [EIA Form 861](https://www.eia.gov/electricity/data/eia861/) | Utility territories, industrial rates |
| [EIA Form 860](https://www.eia.gov/electricity/data/eia860/) | Generation, planned additions |
| [EIA Grid Monitor](https://www.eia.gov/electricity/gridmonitor/) | Regional load/generation context (as needed) |
| [FEMA NRI](https://hazards.fema.gov/nri/) | Natural hazard composite |
| [FCC Broadband Map](https://broadbandmap.fcc.gov/) | Fiber/fixed broadband |
| [Census ACS API](https://api.census.gov/data/2023/acs/acs5) | Labor, demographics |
| [BLS QCEW](https://www.bls.gov/cew/) | NAICS 518210 concentration |
| USGS / water stress proxies | Watershed stress (exact layer TBD in implementation plan) |

### Intelligence (Tavily)

- **Production:** Python calls **Tavily Search API** with parameterized queries per county/metro; store snippets, **URLs**, timestamps; optional **Extract** for known stable URLs.
- **Developer tooling:** [Tavily Agent Skills](https://docs.tavily.com/documentation/agent-skills.md) (CLI `tvly`, skills: search, extract, map, crawl, research) for local experiments—not a substitute for persisted API results in the pipeline.
- **Documentation:** [Tavily docs index](https://docs.tavily.com/llms.txt); follow Tavily best practices for rate limits, deduplication, and citation storage.
- **Secrets:** API key via environment (e.g. `.env` locally, CI secrets in GitHub)—never committed.

### Future commercial (roadmap only)

ATTOM, CoStar, Datacenter Hawk, LoopNet/Crexi, utility premium datasets—documented as follow-on when licensed.

---

## Pipeline Architecture

Stages:

1. **Pull** — `pull_eia_*`, `pull_fema_*`, `pull_fcc_*`, `pull_census_*`, `pull_bls_*`, etc.
2. **Enrich** — `enrich_tavily.py` (Search + optional Extract; staleness e.g. 90 days for repeat queries)
3. **Score** — `score_dc_markets.py` — join on **5-digit county FIPS**, percentile-normalize, apply weights from `dc_weights.json`, apply penalties, emit Tier 1 table
4. **Export** — `export_dc.py` — upload Parquet + metadata to Firebase Storage

Orchestration: `run_dc_pipeline.py` with `--stage pull|enrich|score|export|all` and `--dry-run`.

**Artifacts (Firebase Storage paths TBD in implementation plan):**

- `dc_market_scores.parquet`
- `dc_tavily_intel.parquet` (raw + normalized intel)
- `dc_weights.json`
- `dc_metadata.json` (run id, timestamps, schema version)

---

## UI & Methodology

### Static pages

- **`public/dc_thesis.html`** — Executive summary; three plays; full methodology (scoring + inline index links); Tavily subsection with official doc links; data sources; disclaimer; prominent link to leaderboard.
- **`public/dc_market_leaderboard.html`** — **Default:** dedicated sortable national table page (matches the pattern of standalone multifamily reports). Alternative: a single long page with leaderboard anchored below the thesis—allowed only if scope is explicitly chosen in the implementation plan to reduce surface area.

### Hub

- **`public/index.html`** — New section labeled **“Research track — Data center adjacency”** (or similar). **Do not** use “Phase 3” here—that label is reserved for the **roadmap** phase “commercial data integrations” in the Phasing table below. Cards link thesis, leaderboard, and Fund Explorer.

### Fund Explorer

- DC **dashboard** summary, **map layer** for county scores, **SQL Studio** registration of DC tables + example queries.
- Collapsible **“How we score”** short brief + link to full thesis methodology.

All user-visible numbers must match Parquet (single source of truth). Fund Explorer and static pages must not diverge on the same FIPS.

---

## Phasing

| Phase | Scope |
|-------|--------|
| **1** | Tier 1 pipeline + Tavily + Parquet + thesis + leaderboard + index + Explorer DC surface + methodology with links |
| **2** | Tier 2 geospatial site suitability + drill-down UI |
| **3** | Commercial data integrations (licensed) |

---

## Risks & Limitations

- **Interconnection queue** and **utility headroom** are often incomplete at national scale; proxies must be documented honestly.
- **Tavily** reflects web coverage and recency; it complements but does not replace structured regulatory data.
- **Emerging market** definition is partly judgment; sensitivity on weights (reuse v2 pattern) recommended.

## Success Criteria (Phase 1)

- One-command pipeline produces consistent county-level scores and uploaded artifacts.
- Thesis page is citation-complete (data + industry validation + Tavily docs).
- Explorer and leaderboard show identical scores for the same FIPS.

---

## SPA / Frontend Architecture Note

**Recommendation:** Do **not** block Phase 1 on migrating the whole site to a heavy SPA. The current model—static HTML reports plus **Fund Explorer** (Alpine + DuckDB WASM)—already delivers a focused interactive shell. A full SPA migration (build tooling, client routing, shared layout, auth boundaries) is a **large cross-cutting project**; it should be a **separate decision** after DC Phase 1 ships, unless the primary pain is unified navigation across many interactive routes—in that case, consider an **incremental** approach (e.g. Vite + small app bundle for Explorer only) rather than rewriting all research pages at once.

---

## Next Steps

1. Human review and approval of this document (request changes or approve).
2. Invoke **writing-plans** skill to produce implementation plan (scripts, storage paths, HTML/Fund Explorer diffs, tests, verification).
