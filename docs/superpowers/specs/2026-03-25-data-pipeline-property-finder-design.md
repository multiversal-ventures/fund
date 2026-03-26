# Data Pipeline & Property Finder — Design Spec

**Date:** 2026-03-25
**Status:** Approved
**Authors:** Kartik + Claude

## Overview

Two systems in one repo: (1) an automated data refresh pipeline that pulls Census/BLS/HUD data into per-year Parquet files, and (2) a property finder that scores multifamily acquisition targets from free public data sources. Both feed a DuckDB WASM-powered browser explorer with configurable parameters, Zillow deep-links, and Firebase auth gating.

## Architecture

```
fund/
├── scripts/                    # Python data pipeline
│   ├── requirements.txt        # pandas, pyarrow, duckdb, requests,
│   │                           # firebase-admin, pyyaml
│   ├── run.py                  # CLI entry point
│   ├── config_loader.py        # Load from local YAML or Firestore
│   ├── pull_census.py          # ACS 5-Year tables → per-year parquet
│   ├── pull_bls.py             # OEWS data → per-year parquet
│   ├── pull_hud.py             # FHA multifamily DB + USPS vacancy
│   ├── score.py                # Acquisition signal scoring
│   ├── zillow.py               # Build Zillow deep-link URLs
│   ├── upload.py               # Push parquet → Firebase Storage
│   └── config.default.yaml     # Default markets + weights
├── functions/
│   └── index.js                # Cloud Function: Firestore onWrite
│                               # triggers GitHub workflow_dispatch
├── public/
│   ├── index.html              # Existing auth-gated dashboard
│   ├── explorer.html           # NEW: DuckDB WASM query UI
│   └── *.html                  # Existing 6 reports
├── .github/workflows/
│   ├── refresh.yml             # Monthly cron + workflow_dispatch
│   └── deploy.yml              # Firebase deploy on push to main
├── firebase.json
├── firestore.rules
└── storage.rules
```

## Data Flow

```
┌──────────────────────────────────────────────────────┐
│  Trigger: GitHub Actions cron / workflow_dispatch     │
│           OR local `python scripts/run.py --all`     │
└──────────────┬───────────────────────────────────────┘
               │
               ▼
┌──────────────────────────────────────────────────────┐
│  Python scripts                                      │
│  1. config_loader.py — read YAML or Firestore        │
│  2. pull_census.py — Census ACS API → parquet/year   │
│  3. pull_bls.py — BLS OEWS files → parquet/year      │
│  4. pull_hud.py — HUD SODA API → parquet             │
│  5. score.py — compute acquisition scores            │
│  6. zillow.py — generate deep-link URLs              │
│  7. upload.py — push to Firebase Storage              │
└──────────────┬───────────────────────────────────────┘
               │
               ▼
┌──────────────────────────────────────────────────────┐
│  Firebase Storage (auth-gated)                       │
│  /data/census/acs_{2021,2022,2023}.parquet           │
│  /data/bls/oews_{2022,2023,2024}.parquet             │
│  /data/hud/fha_multifamily.parquet                   │
│  /data/hud/usps_vacancy.parquet                      │
│  /data/scored/properties.parquet                     │
│  /data/meta/last_run.json                            │
└──────────────┬───────────────────────────────────────┘
               │ fetch with auth token
               ▼
┌──────────────────────────────────────────────────────┐
│  Browser — explorer.html                             │
│  DuckDB WASM loads parquet as tables                 │
│  SQL query bar + presets + Zillow links + exports    │
└──────────────────────────────────────────────────────┘
```

## Data Sources

### Free APIs (implemented at launch)

| Source | API | Data |
|--------|-----|------|
| Census ACS 5-Year | api.census.gov (REST, free key) | B25001, B25002, B25003, B25004, B25024, B25064, B25077, B25105, B01003 |
| BLS OEWS | bls.gov/oes/special-requests | Employment, wages by metro/occupation |
| HUD FHA Multifamily | data.hud.gov (SODA) | FHA-insured properties, units, mortgage maturity |
| HUD USPS Vacancy | data.hud.gov (SODA) | Quarterly vacancy rates by ZIP/tract |

### Paid APIs (architected for future plug-in)

ATTOM Data API — foreclosure filings, tax delinquency, transaction history, owner/mortgage info. `score.py` has a pluggable data source interface so ATTOM can be added without restructuring.

## Parquet Schemas

### census/acs_YYYY.parquet

| Column | Type | Source |
|--------|------|--------|
| fips | varchar | County FIPS code |
| county | varchar | County name |
| state | varchar | State abbreviation |
| total_units | int | B25001 |
| occupied | int | B25002 |
| vacant | int | B25002 |
| owner_occupied | int | B25003 |
| renter_occupied | int | B25003 |
| for_rent_vacant | int | B25004 |
| median_rent | int | B25064 |
| median_home_value | int | B25077 |
| median_owner_cost | int | B25105 |
| mf_units | int | B25024 (5+ unit buildings) |
| mf_pct | float | mf_units / total_units |
| pop | int | B01003 |
| vacancy_rate | float | vacant / total_units |
| rental_vac_rate | float | for_rent_vacant / (renter_occupied + for_rent_vacant) |
| rent_to_cost_ratio | float | median_rent / median_owner_cost |
| year | int | ACS year |

### scored/properties.parquet

| Column | Type | Source |
|--------|------|--------|
| fips | varchar | County FIPS |
| county | varchar | County name |
| state | varchar | State |
| property_name | varchar | HUD FHA |
| address | varchar | HUD FHA |
| units | int | HUD FHA |
| mortgage_amount | float | HUD FHA |
| maturity_date | date | HUD FHA |
| maturity_years | float | Computed |
| section8 | bool | HUD FHA |
| vacancy_rate | float | Census |
| rent_to_cost | float | Census |
| vac_trend_5yr_chg | float | Census (latest - earliest) |
| pop_growth | float | Census |
| mf_stock_depth | float | Census |
| score_maturity | float | Configurable weight |
| score_vacancy | float | Configurable weight |
| score_rent_cost | float | Configurable weight |
| total_score | float | Sum of weighted scores (0-100) |
| signal_rank | int | Rank by total_score |
| zillow_url | varchar | Generated deep-link |
| lat | float | County centroid or property geocode |
| lng | float | County centroid or property geocode |

## Scoring Model

Default weights (configurable via dashboard or config.yaml):

| Signal | Default Weight | Logic |
|--------|---------------|-------|
| Mortgage maturity | 20 | Linear: 20pts if matures within 1yr, 0pts if 5+ yrs |
| Vacancy trend | 25 | Tightening markets score higher (negative 5yr change) |
| Rent/cost ratio | 30 | Higher ratio = better cash flow potential |
| Area vacancy (USPS) | 10 | Moderate vacancy = opportunity, extreme = risk |
| Population growth | 15 | Growing markets score higher |

Total: 100 points. Normalized per-signal so each maxes at its weight.

## Target Markets

Default: all 147 counties from the national screening (50K+ units, 10K+ renters, rent > owner cost, vacancy falling). Stored in `config.default.yaml` as a list of FIPS codes. Users can add/remove markets and adjust the screening thresholds from the dashboard config panel.

## Zillow Deep-Links

Each county FIPS maps to lat/lng bounding box. URL format:
```
https://www.zillow.com/homes/for_sale/?searchQueryState={
  "mapBounds": {"west": W, "east": E, "south": S, "north": N},
  "filterState": {"sort": {"value": "globalrelevanceex"}},
  "isMapVisible": true,
  "isListVisible": true,
  "mapZoom": 11
}
```

For individual HUD FHA properties with known addresses, the bounding box is tighter (~0.01 degree radius) centered on the property location.

## Frontend — explorer.html

### Components

1. **Config Panel** (collapsible) — target markets multiselect, scoring weight sliders, "Save & Refresh Pipeline" button, run status indicator
2. **Query Bar** — raw SQL textarea, Run button, preset query buttons (Top 50, Maturing <2yr, By State, High Vacancy)
3. **Results Table** — sortable columns, Zillow link per row, pagination
4. **Quick Charts** — score distribution histogram, top markets by state bar chart
5. **Export** — CSV and Parquet download of current query results

### DuckDB WASM Loading

On page load after auth:
1. Initialize DuckDB WASM
2. Fetch parquet files from Firebase Storage with auth token
3. Register as tables: `properties`, `census_2021`, `census_2022`, `census_2023`, `bls_2024`, `hud_fha`, `usps_vacancy`
4. Run default query, render results

Users can JOIN across all tables in the query bar.

## Cloud Function

Single function in `functions/index.js`:
- Trigger: Firestore onWrite on `/config/pipeline`
- Action: POST to GitHub API `workflow_dispatch` for `refresh.yml`
- Side effect: Write run record to `/config/runs/{timestamp}` with status "triggered"

GitHub Actions workflow updates run status to "running" then "complete" (or "failed") via Firestore write using the service account.

## GitHub Actions

```yaml
on:
  schedule:
    - cron: '0 6 1 * *'       # Monthly on the 1st
  workflow_dispatch:
    inputs:
      config_source:
        default: 'firestore'
```

### Secrets Required

| Secret | Purpose |
|--------|---------|
| FIREBASE_SERVICE_ACCOUNT | Storage upload + Firestore writes |
| CENSUS_API_KEY | Census API (free from api.census.gov) |

## Security Rules

### Firebase Storage
- Read: authenticated users in allowlist only
- Write: service account only (deny all client writes)

### Firestore
- `/config/pipeline`: read/write for allowlisted users
- `/config/runs/*`: read for allowlisted users, write for service account only

### Allowlist
- holly@multiversal.ventures
- akshay@multiversal.ventures
- kartik@multiversal.ventures

## CLI Usage

```bash
# Full refresh locally
python scripts/run.py --all

# Individual steps
python scripts/run.py --census
python scripts/run.py --bls
python scripts/run.py --hud
python scripts/run.py --score
python scripts/run.py --upload

# Config source
python scripts/run.py --all --config scripts/config.default.yaml   # local
python scripts/run.py --all --config firestore                     # remote

# Output locally without uploading
python scripts/run.py --all --local-only --output ./data/
```
