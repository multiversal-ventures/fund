# V2: Layered Scoring, New Data Sources, Sensitivity Analysis — Design Spec

**Date:** 2026-03-26
**Status:** Approved
**Authors:** Kartik + Claude
**Prior spec:** `2026-03-25-data-pipeline-property-finder-design.md`

## Overview

Three integrated improvements to the fund scoring system:

1. **New data sources** — building permits (supply pipeline), rent growth (demand trajectory), employment concentration (HHI diversification). All free Census APIs.
2. **Two-layer scoring model** — Layer 1 "Investor View" scores counties on macro fundamentals. Layer 2 "Ops View" scores properties on acquisition signals within qualifying markets. Combined score bridges both.
3. **Sensitivity analysis** — interactive client-side sliders for real-time re-scoring + pipeline-side Monte Carlo stability scores.

## Architecture Changes

### New Files

```
scripts/
├── pull_permits.py          # Census Building Permits Survey → parquet
├── pull_cbp.py              # Census County Business Patterns → HHI parquet
├── sensitivity.py           # Monte Carlo stability scoring
├── tests/
│   ├── test_pull_permits.py
│   ├── test_pull_cbp.py
│   └── test_sensitivity.py
```

### Modified Files

```
scripts/
├── score.py                 # Rewrite: two-layer scoring (market + deal)
├── pull_census.py           # Add rent_growth computation
├── run.py                   # Add --permits, --cbp, --sensitivity flags
├── config.default.yaml      # New weight sections
├── tests/test_score.py      # Updated for two-layer model

public/
├── explorer.html            # Investor/Ops/Combined views, sensitivity panel, updated thesis
```

### New Parquet Files in Firebase Storage

```
/data/permits/permits_YYYY.parquet    # Building permits by county
/data/cbp/employment_YYYY.parquet     # Employment by NAICS sector, HHI
/data/scored/properties.parquet       # Now includes market_score, deal_score,
                                      # total_score, stability_score, rank_std
```

---

## 1. New Data Sources

### Building Permits — `pull_permits.py`

**API:** Census Building Permits Survey (BPS)
**URL:** `https://api.census.gov/data/timeseries/bps`
**Cost:** Free
**Frequency:** Annual, county-level

**Variables:**
- `BLDGS` — total building permits
- `UNITS` — total units permitted
- `BLDGS_5PLUS` or structure-type breakdown — multifamily permits (5+ units)

**Output schema: `permits/permits_YYYY.parquet`**

| Column | Type | Description |
|--------|------|-------------|
| fips | varchar | County FIPS |
| county | varchar | County name |
| state | varchar | State abbreviation |
| year | int | Permit year |
| total_permits | int | Total building permits issued |
| total_units_permitted | int | Total housing units permitted |
| mf_permits | int | Multifamily permits (5+ units) |
| mf_units_permitted | int | Multifamily units permitted |
| sf_permits | int | Single-family permits |
| mf_pct | float | mf_permits / total_permits |
| permits_per_1k_units | float | mf_units_permitted / (total_housing_units / 1000) |

`permits_per_1k_units` is the supply pressure signal. Cross-referenced with total housing units from Census ACS.

### Rent Growth — computed from existing Census ACS data

No new API pull. Computed in `score.py` from existing parquet:

```python
rent_growth = (median_rent_latest - median_rent_earliest) / median_rent_earliest
```

Uses ACS 2021 vs 2023 median rent (B25064). Added as a column during scoring, not a separate file.

### Employment Concentration — `pull_cbp.py`

**API:** Census County Business Patterns
**URL:** `https://api.census.gov/data/YYYY/cbp`
**Cost:** Free
**Frequency:** Annual, county-level

**Variables:** Employment (`EMP`) by 2-digit NAICS sector (`NAICS2017` with `meaning_of_naics_code`).

**Processing:** For each county, compute Herfindahl-Hirschman Index:
```
HHI = Σ (sector_share_i × 100)²
```
Where `sector_share_i` = sector employment / total employment. HHI ranges 0-10,000.

**Output schema: `cbp/employment_YYYY.parquet`**

| Column | Type | Description |
|--------|------|-------------|
| fips | varchar | County FIPS |
| county | varchar | County name |
| state | varchar | State abbreviation |
| year | int | Year |
| total_employment | int | Total private employment |
| hhi | float | Herfindahl-Hirschman Index (0-10000) |
| top_sector_name | varchar | Largest sector by employment |
| top_sector_share | float | Share of employment in top sector |
| top3_share | float | Share of employment in top 3 sectors |
| num_sectors | int | Number of active NAICS sectors |

**HHI interpretation:**
- < 1500: Diversified (good)
- 1500-2500: Moderate concentration
- > 2500: Highly concentrated (risky — one employer/industry dominates)

---

## 2. Two-Layer Scoring Model

### Hard Filters (must pass all to enter scoring)

| Filter | Threshold | Source |
|--------|-----------|--------|
| Population | ≥ 20,000 | Census B01003 |
| Total housing units | ≥ 10,000 | Census B25001 |
| Renter households | ≥ 5,000 | Census B25003 |
| Total employed | ≥ 10,000 | Census C24010 |

Counties failing any filter are excluded from all scoring and results.

### Layer 1: Market Score — Investor View

"Is this a good market? Will tenants keep paying rent?"

Scored at county level. Default weights (configurable, sum to 100):

| Signal | Weight | Source | Direction | Logic |
|--------|--------|--------|-----------|-------|
| Workforce resilience | 20 | C24010 + Frey/Osborne/Eloundou | Higher = better | AI-proof tenants |
| Rent growth | 15 | ACS median rent 2021→2023 | Higher = better | Demand strength, pricing power |
| Rent/cost ratio | 15 | ACS B25064/B25105 | Higher = better | Cash flow potential |
| Vacancy trend | 20 | ACS vacancy 2021→2023 | More negative = better | Tightening market |
| Employment concentration | 10 | CBP HHI | Lower = better | Diversified economy |
| Population growth | 10 | ACS B01003 2021→2023 | Higher = better | Expanding demand |
| Supply pressure | 10 | Building permits per 1K units | Lower = better | Less incoming competition |

Output: `market_score` (0-100) per county.

### Layer 2: Deal Score — Ops View

"Given a good market, which property should I go after?"

Scored at property level, only within counties passing hard filters and Layer 1 threshold (default: market_score ≥ 40).

| Signal | Weight | Source | Direction | Logic |
|--------|--------|--------|-----------|-------|
| Mortgage maturity | 40 | HUD FHA maturity_years | Lower = better | Maturing mortgage = motivated seller |
| Unit count | 20 | HUD FHA units | Higher = better | Larger = more operationally efficient |
| Section 8 | 20 | HUD FHA section8 flag | True = better | Guaranteed government income stream |
| Area vacancy | 20 | Census/USPS | Moderate (~5-8%) = best | Opportunity without distress |

Output: `deal_score` (0-100) per property.

### Combined Score

```
total_score = (market_weight × market_score + deal_weight × deal_score) / 100
```

Default: `market_weight = 60, deal_weight = 40`

Configurable via UI slider. A great deal in a mediocre market scores lower than a decent deal in a great market.

### Preset Scenarios

| Scenario | Market/Deal Split | Market Focus | Deal Focus |
|----------|-------------------|--------------|------------|
| Balanced | 60/40 | Even weights | Even weights |
| Risk-Aware Investor | 80/20 | Heavy resilience (30) + HHI (15) | Light |
| Ops / Cash Flow | 30/70 | Light | Heavy maturity (50) + Section 8 (25) |
| Durable Tenants | 70/30 | Heavy resilience (30) + rent growth (25) | Moderate |

---

## 3. Sensitivity Analysis

### Client-Side — Interactive Sliders (DuckDB WASM)

New collapsible panel in explorer with sliders for all market and deal weights. On "Recalculate":

1. Build a DuckDB SQL query that recomputes normalized scores using slider values
2. Execute against loaded parquet tables
3. Compare new ranks to baseline ranks
4. Show biggest rank movers (▲/▼) and % of top-20 unchanged

All client-side, instant. No backend round-trip needed.

**SQL approach:** The normalization formula can be expressed as:
```sql
(value - MIN(value) OVER()) / NULLIF(MAX(value) OVER() - MIN(value) OVER(), 0) * weight
```
Applied per signal, summed, ranked.

### Pipeline-Side — Monte Carlo Stability (`sensitivity.py`)

Runs during `uv run scripts/run.py --sensitivity`:

1. Load scored properties with baseline weights
2. For 1000 iterations:
   a. Jitter each weight by uniform random ±20%
   b. Renormalize weights to sum to 100
   c. Recompute market_score, deal_score, total_score
   d. Record each property's rank
3. Compute per-property:
   - `stability_score` (0-100): % of iterations property stayed in same rank decile
   - `rank_std`: standard deviation of rank
   - `rank_min` / `rank_max`: range of ranks observed

**Output:** Added as columns to `scored/properties.parquet`.

**Display:** Badge in explorer results — green "Stable" (≥70), yellow "Moderate" (40-70), red "Volatile" (<40).

---

## 4. Explorer UI Updates

### Thesis Banner (top of page, updated)

```
Fund Thesis

Invest in multifamily properties where renters have durable, AI-proof
jobs. We use a two-layer scoring model:

🔍 Investor View — Is this a good market?
Scores counties on workforce automation resilience, employment
diversification, vacancy trends, rent growth, supply pipeline, and
population growth. Markets where tenants keep paying rent regardless
of tech disruption.

🔧 Ops View — Is this a good deal?
Scores properties on mortgage maturity (motivated sellers), unit count
(operational efficiency), Section 8 status (guaranteed income), and
local vacancy. Finds acquisition targets within qualifying markets.

Combined: great deals in great markets, with sensitivity analysis to
stress-test assumptions.
```

### View Toggle

Three-way toggle above results table: **Investor | Ops | Combined**

- **Investor**: Shows counties ranked by `market_score`. Columns: county, state, market_score, resilience_index, hhi, rent_growth, vacancy_trend, supply_pressure, Zillow link.
- **Ops**: Shows properties ranked by `deal_score` within selected market(s). Columns: property_name, address, deal_score, maturity_years, units, section8, market_score, Zillow link.
- **Combined**: Shows properties ranked by `total_score`. All columns. Default view.

### Sensitivity Panel

Collapsible panel with:
- Market weight sliders (7 signals)
- Deal weight sliders (4 signals)
- Market/Deal split slider (0-100)
- "Recalculate" button
- Rank change display (top movers)
- Stability badge explanation

### Config Panel Updates

- Add `market_weights` and `deal_weights` as separate sections (replacing single `scoring_weights`)
- Add `market_deal_split` slider (default 60/40)
- Update scenario presets for two-layer model

### New SQL Presets

- "Top Markets" — `SELECT * FROM market_scores ORDER BY market_score DESC LIMIT 30`
- "Best Deals in Top Markets" — properties in top-20 market_score counties, ordered by deal_score
- "Most Stable Picks" — `WHERE stability_score > 70 ORDER BY total_score DESC`
- "Supply Risk" — counties with highest permits_per_1k_units
- "Most Diversified" — counties with lowest HHI

---

## 5. Config Updates

### config.default.yaml

```yaml
hard_filters:
  min_population: 20000
  min_housing_units: 10000
  min_renter_households: 5000
  min_employed: 10000

market_weights:
  vacancy_trend: 20
  rent_growth: 15
  rent_cost_ratio: 15
  workforce_resilience: 20
  employment_concentration: 10
  pop_growth: 10
  supply_pressure: 10

deal_weights:
  mortgage_maturity: 40
  unit_count: 20
  section8: 20
  area_vacancy: 20

market_deal_split:
  market: 60
  deal: 40

sensitivity:
  iterations: 1000
  jitter_pct: 20

# Existing fields preserved
target_markets: [...]
auto_discover: true
screening_thresholds: {...}
census:
  years: [2021, 2022, 2023]
```

---

## 6. CLI Updates

```bash
# New individual flags
uv run scripts/run.py --permits     # Pull building permits
uv run scripts/run.py --cbp         # Pull County Business Patterns
uv run scripts/run.py --sensitivity # Run Monte Carlo after scoring

# Full pipeline now includes all sources
uv run scripts/run.py --all         # census + permits + cbp + hud + score + sensitivity + upload
```

---

## 7. Data Flow (Updated)

```
Census ACS ─────────────┐
Census C24010 ──────────┤
Census BPS (permits) ───┤
Census CBP (employment) ┤──→ score.py ──→ Layer 1: Market Score
                        │                          │
HUD FHA ────────────────┤──→ score.py ──→ Layer 2: Deal Score
USPS Vacancy ───────────┘                          │
                                                   ▼
                                        Combined total_score
                                                   │
                                        sensitivity.py (Monte Carlo)
                                                   │
                                                   ▼
                                        properties.parquet
                                        (market_score, deal_score,
                                         total_score, stability_score)
                                                   │
                                        Firebase Storage
                                                   │
                                        Explorer (DuckDB WASM)
                                        ├── Investor View
                                        ├── Ops View
                                        ├── Combined View
                                        └── Sensitivity Sliders
```
