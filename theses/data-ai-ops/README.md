# Data/AI Ops Thesis — Kartik

The quantitative engine powering the fund. A full-stack data platform that pulls from federal primary sources, scores markets and properties, and serves an interactive research dashboard to the team — all from code in this repo.

---

## Core Thesis

Every fund thesis the strategy team writes needs to be **falsifiable against data**. The Data/AI Ops layer exists to ensure that every market claim — "Syracuse has 97.4% occupancy," "Midwest construction starts dropped 74%," "insurance costs $400/unit in Idaho vs $1,800 in Florida" — can be traced back to a primary data source, reproduced by running a pipeline, and queried interactively by the team.

This is not a dashboard bolted on after the fact. The data pipeline *is* the thesis development process:

1. **Pull** primary data from Census ACS, BLS, HUD, Census Building Permits, County Business Patterns
2. **Score** markets on 7 weighted signals; score properties on 4 deal-level signals
3. **Upload** Parquet files to Firebase Storage
4. **Serve** via DuckDB-WASM in the browser — team members query the same data in SQL, maps, and charts

---

## Architecture

```
Census API ─┐
BLS OEWS   ─┤
HUD FHA    ─┤──→ scripts/  ──→ data/ (Parquet)  ──→ Firebase Storage
Permits    ─┤     Python        gitignored            auth-gated
CBP/HHI    ─┤     pipeline
EIA (DC)   ─┤
Tavily (DC)─┘

Firebase Storage ──→ public/explorer.html ──→ DuckDB-WASM (in-browser)
                                              ├── Dashboard tab
                                              ├── Map tab (Leaflet)
                                              ├── SQL Studio tab
                                              ├── DC Markets tab
                                              └── Scenarios tab
```

**CI/CD**: GitHub Actions runs `deploy.yml` on push to `main` (Firebase Hosting + Functions). `refresh.yml` runs monthly on cron or on Firestore config changes (full pipeline).

---

## Parquet Data Catalog

All files written to `data/` (gitignored locally, uploaded to Firebase Storage at `gs://mvv-fund.firebasestorage.app/data/`).

### Census ACS (`data/census/`)

| File | Source | Contents |
|------|--------|----------|
| `acs_2023.parquet` | Census ACS 5-Year API | County-level: pop, total_units, occupied, vacant, renter_occupied, median_rent, median_home_value, owner_cost, rent_to_cost_ratio, vacancy_rate, multifamily stock (3-4, 5-9, 10-19, 20-49, 50+ units) |
| `acs_2022.parquet` | Census ACS 5-Year API | Same schema, 2022 vintage — used for trend calculation |
| `acs_2021.parquet` | Census ACS 5-Year API | Same schema, 2021 vintage — earliest year for 3-year delta |
| `occupations_2023.parquet` | Census ACS B24010 | County-level occupation by sex, total_employed, resilience_index (workforce diversity score) |
| `occupations_2022.parquet` | Census ACS B24010 | Same, 2022 vintage |
| `occupations_2021.parquet` | Census ACS B24010 | Same, 2021 vintage |

**Census tables pulled**: B25001, B25002, B25003, B25004, B25024, B25064, B25077, B25105, B01003, B24010

### BLS (`data/bls/`)

| File | Source | Contents |
|------|--------|----------|
| BLS OEWS files | BLS Occupational Employment & Wage Statistics | Metro-level electrician employment, wages, location quotients — feeds the job diversity thesis |

### HUD (`data/hud/`)

| File | Source | Contents |
|------|--------|----------|
| `fha_multifamily.parquet` | HUD FHA Multifamily Portfolio | Property-level: fips, units, maturity_years, section8 flag — used for deal-layer scoring |
| `usps_vacancy.parquet` | HUD USPS Vacancy Data | Address-level vacancy aggregated to county — secondary vacancy signal |

### Building Permits (`data/permits/`)

| File | Source | Contents |
|------|--------|----------|
| `permits_2023.parquet` | Census Building Permits Survey | County-level multifamily units permitted — supply pressure signal |

### County Business Patterns (`data/cbp/`)

| File | Source | Contents |
|------|--------|----------|
| `employment_2023.parquet` | Census CBP | County-level employment by industry, HHI (Herfindahl–Hirschman Index) for employment concentration |

### Scored Output (`data/scored/`)

| File | Source | Contents |
|------|--------|----------|
| `market_scores.parquet` | `scripts/score.py` | County-level market scores (0–100) with 7 component signals and Monte Carlo sensitivity bands |
| `properties.parquet` | `scripts/score.py` | Property-level: market_score + deal_score combined into total_score with signal_rank, Zillow URLs |

### Data Center Adjacency (`data/dc/`)

| File | Source | Contents |
|------|--------|----------|
| `dc_market_scores.parquet` | `scripts/dc/score_dc_markets.py` | County-level DC readiness score (0–100) with subscores: electrical, water, political, pipeline, connectivity, labor, unique, penalty |
| `dc_tavily_state.parquet` | `scripts/dc/enrich_tavily.py` | State-level political intelligence from Tavily Search API — incentive vs risk language scores |
| `cbp_naics518_*.parquet` | `scripts/dc/pull_cbp_naics518.py` | NAICS 518210 (Data Processing) employment per county — proxy for existing DC pipeline |

---

## Scoring Model

### Market Score (county-level, 100 pts)

Two-layer engine in `scripts/score.py`. Seven signals, min-max normalized and weighted:

| Signal | Weight | Direction | Source |
|--------|--------|-----------|--------|
| Vacancy trend (3yr delta) | 20 | Lower is better | Census ACS B25002 |
| Rent growth (3yr delta) | 15 | Higher is better | Census ACS B25064 |
| Rent-to-cost ratio | 15 | Higher is better | Census ACS B25064 / B25105 |
| Workforce resilience | 20 | Higher is better | Census ACS B24010 (occupation diversity) |
| Employment concentration (HHI) | 10 | Lower is better | Census CBP |
| Population growth (3yr delta) | 10 | Higher is better | Census ACS B01003 |
| Supply pressure (permits/stock) | 10 | Lower is better | Census Building Permits / B25001 |

**Hard filters** (must pass all): pop >= 20K, housing units >= 10K, renter HH >= 5K, employed >= 10K.

### Deal Score (property-level, 100 pts)

Properties in counties with market_score >= 40 get scored on:

| Signal | Weight | Direction | Source |
|--------|--------|-----------|--------|
| Mortgage maturity (years to expiry) | 40 | Lower is better | HUD FHA |
| Unit count | 20 | Higher is better | HUD FHA |
| Section 8 designation | 20 | Yes is better | HUD FHA |
| Area vacancy (distance from 6.5% optimal) | 20 | Closer is better | Census ACS |

### Total Score

```
total_score = (60 × market_score + 40 × deal_score) / 100
```

### Monte Carlo Sensitivity

1,000 iterations with ±20% random jitter on each weight. Produces confidence bands on total_score to identify markets where ranking is stable vs volatile under weight assumptions.

---

## DC Adjacency Score (county-level, 100 pts)

Separate pipeline in `scripts/dc/`. Scores counties for commercial property adjacency to data center buildout — not operating DCs, but land banking and conversion plays.

| Category | Weight | Source |
|----------|--------|--------|
| Electrical (industrial ¢/kWh) | 30 | EIA Form 861 (CSV in-repo) |
| Water & cooling | 10 | Placeholder (neutral until USGS layer) |
| Political (incentive vs risk language) | 20 | Tavily Search API, state-level |
| Pipeline (NAICS 518210 employment/1k pop) | 15 | Census CBP |
| Connectivity | 10 | Placeholder (neutral until FCC/IX data) |
| Labor & cost (workforce resilience) | 10 | Census ACS B24010 |
| Unique (renewable/disaster) | 5 | Placeholder |
| Penalty (moratorium/opposition) | -10 max | Tavily-derived |

**Eligibility screen**: pop >= 50K, housing units >= 20K, US territories excluded.

---

## Web Dashboard

Firebase-hosted at `mvv-fund.web.app`, auth-gated to `@multiversal.ventures` emails.

### Pages

| Page | What it does |
|------|-------------|
| `index.html` | Research hub — 3 theses, 3 workstreams, 6 California/national reports, sources, methodology |
| `explorer.html` | **Fund Explorer** — DuckDB-WASM loads all Parquet tables; tabs for Dashboard, Map, SQL Studio, DC Markets, Scenarios |
| `dc_thesis.html` | DC adjacency methodology, weight definitions, validation sources (Kearney, CBRE, JLL, Analytics.loan) |
| `dc_market_leaderboard.html` | Static entry to DC leaderboard (data lives in Explorer) |
| `national_multifamily_fund_dashboard.html` | 528 US counties screened — vacancy trends, rent-to-cost ratios, sparkline charts |
| `multifamily_fund_target_markets.html` | Sacramento, Inland Empire, Fresno, Kern deep dives with scoring |
| `california_housing_vacancy_occupancy.html` | All 58 CA counties — vacancy, rent, home value tables |
| `california_housing_vacancy_5yr_trends.html` | 2018–2023 vacancy trend analysis with sparklines |
| `electricians_california_by_metro.html` | BLS OEWS employment data for job diversity thesis (25 metros) |
| `electricians_housing_profile_california.html` | Employment × housing affordability cross-reference |

### Explorer Modules (`public/js/explorer/`)

| Module | Function |
|--------|----------|
| `app.js` | Alpine.js root state, tab switching, weight baseline tracking |
| `duckdb.js` | DuckDB-WASM init, Parquet loading from Firebase Storage, query helper |
| `schema.js` | Table schema definitions, column metadata, formatting |
| `dashboard.js` | Market dashboard: top markets table, summary stats |
| `dc-dashboard.js` | DC Markets tab: leaderboard, subscore breakdown, Tavily news integration |
| `sql-studio.js` | SQL query editor with presets (Top 40 Markets, DC Top 40, etc.) |
| `map.js` | Leaflet choropleth: color-coded counties by market_score or dc_market_score |
| `charts.js` | Chart rendering utilities |
| `results.js` | Property results table with Zillow links |
| `scenarios.js` | Weight adjustment UI, Firestore save, estimated preview mode |
| `zillow.js` | Zillow URL generation per county/property |

### Cloud Functions (`functions/`)

| Function | Trigger | Purpose |
|----------|---------|---------|
| `triggerPipelineRefresh` | Firestore `config/pipeline` write | Dispatches GitHub Actions `refresh.yml` via API when team updates pipeline config in Explorer |
| `dcLocalNews` | HTTP POST (auth'd) | Real-time Tavily news search for DC context by state/county — serves Explorer DC Markets tab |

---

## Running the Pipeline

```bash
cd scripts

# Full pipeline (all sources + score + upload)
uv run python run.py --all

# Individual stages
uv run python run.py --census          # Census ACS 2021–2023
uv run python run.py --bls             # BLS OEWS
uv run python run.py --hud             # HUD FHA + USPS
uv run python run.py --permits         # Building permits
uv run python run.py --cbp             # County Business Patterns / HHI
uv run python run.py --score           # Market + deal scoring
uv run python run.py --sensitivity     # Monte Carlo (requires --score first)
uv run python run.py --dc              # DC adjacency pipeline
uv run python run.py --dc --dc-skip-tavily  # DC without Tavily API calls
uv run python run.py --upload          # Firebase Storage upload

# Local only (no upload)
uv run python run.py --all --local-only

# Config from Firestore instead of YAML
uv run python run.py --all --config firestore
```

**Environment variables**: `CENSUS_API_KEY` (required for Census/permits/CBP), `GOOGLE_APPLICATION_CREDENTIALS` (for upload + Firestore config), `TAVILY_API_KEY` (for DC enrichment).

---

## Test Suite

```bash
cd scripts
uv run pytest tests/ -v
```

13 test modules covering all pipeline stages: census pull, BLS, HUD, permits, CBP, scoring, sensitivity, upload, DC scoring, occupation resilience, Zillow URL generation, and config loading.

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Data pipeline | Python 3.12, pandas, pyarrow, duckdb, requests, playwright, openpyxl |
| Package management | uv (astral) |
| Testing | pytest, pytest-mock |
| Hosting | Firebase Hosting |
| Auth | Firebase Auth (Google OAuth, email allowlist) |
| Storage | Firebase Storage (Parquet files) |
| Database | Firestore (pipeline config + run tracking) |
| Functions | Cloud Functions v2 (Node.js 22) |
| Frontend query | DuckDB-WASM 1.29 (in-browser SQL on Parquet) |
| Frontend framework | Alpine.js |
| Maps | Leaflet |
| CI/CD | GitHub Actions (deploy on push, monthly data refresh) |
