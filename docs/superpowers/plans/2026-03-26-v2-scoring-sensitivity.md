# V2: Layered Scoring, New Data Sources, Sensitivity Analysis — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add building permits + employment HHI data, rewrite scoring into two layers (Market/Investor + Deal/Ops), add Monte Carlo sensitivity analysis, and update the explorer with view toggles and interactive sensitivity sliders.

**Architecture:** New data pull scripts for Census BPS (permits) and CBP (employment), rewritten score.py with `score_markets()` and `score_deals()` functions, a sensitivity.py Monte Carlo module, and a significantly updated explorer.html with three view modes and sensitivity controls.

**Tech Stack:** Python 3.12+ (uv), pandas, pyarrow, numpy, Census API, DuckDB WASM, Firebase

**Spec:** `docs/superpowers/specs/2026-03-26-v2-scoring-sensitivity-design.md`

---

## File Map

### New Files
| File | Responsibility |
|------|---------------|
| `scripts/pull_permits.py` | Census Building Permits Survey → parquet per year |
| `scripts/pull_cbp.py` | Census County Business Patterns → HHI parquet per year |
| `scripts/sensitivity.py` | Monte Carlo stability scoring (1000 iterations) |
| `scripts/tests/test_pull_permits.py` | Tests for permits pull |
| `scripts/tests/test_pull_cbp.py` | Tests for CBP/HHI pull |
| `scripts/tests/test_sensitivity.py` | Tests for Monte Carlo |

### Modified Files
| File | Changes |
|------|---------|
| `scripts/score.py` | Rewrite: hard filters, `score_markets()`, `score_deals()`, combined score |
| `scripts/run.py` | Add --permits, --cbp, --sensitivity flags |
| `scripts/config.default.yaml` | New market_weights, deal_weights, hard_filters, sensitivity sections |
| `scripts/tests/test_score.py` | Rewrite for two-layer model |
| `public/explorer.html` | View toggle, sensitivity panel, updated thesis, new presets |

---

## Task 1: Building Permits Data Pull — pull_permits.py

**Files:**
- Create: `scripts/pull_permits.py`
- Create: `scripts/tests/test_pull_permits.py`

- [ ] **Step 1: Write failing test**

```python
# scripts/tests/test_pull_permits.py
import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from pull_permits import parse_permits_response, build_permits_url

def test_build_permits_url():
    url = build_permits_url(2023)
    assert "api.census.gov" in url
    assert "bps" in url

def test_parse_permits_response():
    raw = [
        ["BLDGS", "UNITS", "state", "county"],
        ["150", "400", "06", "067"],
        ["80", "200", "12", "105"],
    ]
    df = parse_permits_response(raw, 2023)
    assert len(df) == 2
    assert "fips" in df.columns
    assert "total_permits" in df.columns
    assert "total_units_permitted" in df.columns
    assert df.iloc[0]["fips"] == "06067"
    assert df.iloc[0]["total_permits"] == 150

def test_parse_permits_with_structure_type():
    """Test parsing when structure type breakdown is available."""
    raw = [
        ["BLDGS", "UNITS", "BLDGS_1", "UNITS_1", "BLDGS_5PLUS", "UNITS_5PLUS", "state", "county"],
        ["150", "400", "100", "100", "30", "250", "06", "067"],
    ]
    df = parse_permits_response(raw, 2023)
    assert df.iloc[0]["mf_permits"] == 30
    assert df.iloc[0]["mf_units_permitted"] == 250
    assert df.iloc[0]["sf_permits"] == 100
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/kartikthakore/Development/fund/scripts && uv run pytest tests/test_pull_permits.py -v`

- [ ] **Step 3: Write pull_permits.py**

```python
# scripts/pull_permits.py
"""Pull Census Building Permits Survey data and write per-year Parquet files."""
import os
import requests
import pandas as pd
from pathlib import Path
from pull_census import STATE_FIPS_TO_ABBR

BPS_BASE = "https://api.census.gov/data/timeseries/bps"


def build_permits_url(year: int, api_key: str = None) -> str:
    """Build Census BPS API URL for county-level permits."""
    variables = "BLDGS,UNITS"
    url = f"{BPS_BASE}?get={variables}&for=county:*&in=state:*&time={year}"
    if api_key:
        url += f"&key={api_key}"
    return url


def parse_permits_response(raw: list[list], year: int) -> pd.DataFrame:
    """Parse Census BPS JSON response into a DataFrame."""
    header = raw[0]
    data = raw[1:]
    df = pd.DataFrame(data, columns=header)

    df["fips"] = df["state"] + df["county"]
    df["state"] = df["state"].map(STATE_FIPS_TO_ABBR)
    df["year"] = year

    # Total permits and units
    df["total_permits"] = pd.to_numeric(df.get("BLDGS", 0), errors="coerce").fillna(0).astype(int)
    df["total_units_permitted"] = pd.to_numeric(df.get("UNITS", 0), errors="coerce").fillna(0).astype(int)

    # Structure type breakdown if available
    if "BLDGS_5PLUS" in df.columns:
        df["mf_permits"] = pd.to_numeric(df["BLDGS_5PLUS"], errors="coerce").fillna(0).astype(int)
        df["mf_units_permitted"] = pd.to_numeric(df.get("UNITS_5PLUS", 0), errors="coerce").fillna(0).astype(int)
    else:
        df["mf_permits"] = 0
        df["mf_units_permitted"] = 0

    if "BLDGS_1" in df.columns:
        df["sf_permits"] = pd.to_numeric(df["BLDGS_1"], errors="coerce").fillna(0).astype(int)
    else:
        df["sf_permits"] = df["total_permits"] - df["mf_permits"]

    df["mf_pct"] = (df["mf_permits"] / df["total_permits"].replace(0, float("nan"))).round(4)

    keep = [
        "fips", "state", "year", "total_permits", "total_units_permitted",
        "mf_permits", "mf_units_permitted", "sf_permits", "mf_pct",
    ]
    return df[[c for c in keep if c in df.columns]].copy()


def fetch_permits_year(year: int, output_path: str = None, api_key: str = None) -> pd.DataFrame:
    """Fetch building permits for a single year."""
    api_key = api_key or os.environ.get("CENSUS_API_KEY", "")
    url = build_permits_url(year, api_key=api_key)
    print(f"  Fetching building permits for {year}...")
    resp = requests.get(url, timeout=120)
    resp.raise_for_status()
    raw = resp.json()
    df = parse_permits_response(raw, year)
    if output_path:
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(output_path, index=False)
        print(f"  Wrote {len(df)} rows -> {output_path}")
    return df


def pull_permits(years: list[int], output_dir: str, api_key: str = None) -> dict[int, pd.DataFrame]:
    """Pull building permits for multiple years."""
    print("Pulling building permits data...")
    results = {}
    for year in years:
        output_path = str(Path(output_dir) / f"permits_{year}.parquet")
        results[year] = fetch_permits_year(year, output_path=output_path, api_key=api_key)
    return results
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/kartikthakore/Development/fund/scripts && uv run pytest tests/test_pull_permits.py -v`

- [ ] **Step 5: Commit**

```bash
git add scripts/pull_permits.py scripts/tests/test_pull_permits.py
git commit -m "feat: add building permits data pull from Census BPS"
```

---

## Task 2: Employment Concentration (HHI) — pull_cbp.py

**Files:**
- Create: `scripts/pull_cbp.py`
- Create: `scripts/tests/test_pull_cbp.py`

- [ ] **Step 1: Write failing test**

```python
# scripts/tests/test_pull_cbp.py
import pytest
import pandas as pd
from pull_cbp import compute_hhi, parse_cbp_response

def test_compute_hhi_diversified():
    """Equal distribution across 10 sectors = HHI 1000."""
    shares = [0.1] * 10
    assert compute_hhi(shares) == 1000.0

def test_compute_hhi_concentrated():
    """One sector dominates = high HHI."""
    shares = [0.9, 0.05, 0.05]
    assert compute_hhi(shares) == 8150.0

def test_compute_hhi_monopoly():
    """Single sector = HHI 10000."""
    shares = [1.0]
    assert compute_hhi(shares) == 10000.0

def test_parse_cbp_response():
    raw = [
        ["EMP", "NAICS2017", "NAICS2017_LABEL", "state", "county"],
        ["5000", "44-45", "Retail Trade", "06", "067"],
        ["3000", "62", "Health Care and Social Assistance", "06", "067"],
        ["2000", "23", "Construction", "06", "067"],
    ]
    df = parse_cbp_response(raw, 2022)
    assert len(df) == 1  # One county, aggregated
    assert "hhi" in df.columns
    assert "top_sector_name" in df.columns
    assert df.iloc[0]["top_sector_name"] == "Retail Trade"
    assert df.iloc[0]["fips"] == "06067"
```

- [ ] **Step 2: Run test to verify it fails**

- [ ] **Step 3: Write pull_cbp.py**

```python
# scripts/pull_cbp.py
"""Pull Census County Business Patterns data and compute employment HHI."""
import os
import requests
import pandas as pd
from pathlib import Path
from pull_census import STATE_FIPS_TO_ABBR

CBP_BASE = "https://api.census.gov/data"

# 2-digit NAICS sectors to query
NAICS_SECTORS = "00"  # 00 = all sectors; API returns breakdown by 2-digit


def compute_hhi(shares: list[float]) -> float:
    """Compute Herfindahl-Hirschman Index from market shares (0-1 scale).

    Returns HHI on 0-10000 scale.
    """
    return round(sum((s * 100) ** 2 for s in shares), 1)


def build_cbp_url(year: int, api_key: str = None) -> str:
    """Build Census CBP API URL for county-level employment by sector."""
    url = f"{CBP_BASE}/{year}/cbp?get=EMP,NAICS2017,NAICS2017_LABEL&for=county:*&in=state:*&NAICS2017=*"
    if api_key:
        url += f"&key={api_key}"
    return url


def parse_cbp_response(raw: list[list], year: int) -> pd.DataFrame:
    """Parse CBP response and compute HHI per county."""
    header = raw[0]
    data = raw[1:]
    df = pd.DataFrame(data, columns=header)

    df["fips"] = df["state"] + df["county"]
    df["EMP"] = pd.to_numeric(df["EMP"], errors="coerce").fillna(0)

    # Filter to 2-digit NAICS sectors only (length 2 or pattern XX-XX)
    df = df[df["NAICS2017"].str.match(r"^\d{2}(-\d{2})?$", na=False)].copy()
    # Exclude total (00)
    df = df[df["NAICS2017"] != "00"].copy()

    results = []
    for fips, group in df.groupby("fips"):
        total_emp = group["EMP"].sum()
        if total_emp == 0:
            continue

        group = group.sort_values("EMP", ascending=False)
        shares = (group["EMP"] / total_emp).tolist()
        hhi = compute_hhi(shares)

        top = group.iloc[0]
        top3_share = group["EMP"].head(3).sum() / total_emp

        results.append({
            "fips": fips,
            "state": STATE_FIPS_TO_ABBR.get(str(fips)[:2], ""),
            "year": year,
            "total_employment": int(total_emp),
            "hhi": hhi,
            "top_sector_name": top["NAICS2017_LABEL"],
            "top_sector_share": round(top["EMP"] / total_emp, 4),
            "top3_share": round(top3_share, 4),
            "num_sectors": len(group),
        })

    return pd.DataFrame(results)


def fetch_cbp_year(year: int, output_path: str = None, api_key: str = None) -> pd.DataFrame:
    """Fetch CBP employment data for a single year and compute HHI."""
    api_key = api_key or os.environ.get("CENSUS_API_KEY", "")
    url = build_cbp_url(year, api_key=api_key)
    print(f"  Fetching County Business Patterns for {year}...")
    resp = requests.get(url, timeout=180)
    resp.raise_for_status()
    raw = resp.json()
    df = parse_cbp_response(raw, year)
    if output_path:
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(output_path, index=False)
        print(f"  Wrote {len(df)} rows -> {output_path}")
    return df


def pull_cbp(years: list[int], output_dir: str, api_key: str = None) -> dict[int, pd.DataFrame]:
    """Pull CBP employment data for multiple years."""
    print("Pulling County Business Patterns data...")
    results = {}
    for year in years:
        output_path = str(Path(output_dir) / f"employment_{year}.parquet")
        results[year] = fetch_cbp_year(year, output_path=output_path, api_key=api_key)
    return results
```

- [ ] **Step 4: Run tests, commit**

```bash
git add scripts/pull_cbp.py scripts/tests/test_pull_cbp.py
git commit -m "feat: add County Business Patterns pull with HHI computation"
```

---

## Task 3: Update Config — config.default.yaml

**Files:**
- Modify: `scripts/config.default.yaml`

- [ ] **Step 1: Rewrite config with two-layer structure**

Replace the `scoring_weights` section with the new structure. Keep existing `target_markets`, `auto_discover`, `screening_thresholds`, and `census` sections. Add `hard_filters`, `market_weights`, `deal_weights`, `market_deal_split`, `sensitivity`.

See spec section 5 for exact YAML content. Ensure backward compatibility — old `scoring_weights` key should be ignored if present.

- [ ] **Step 2: Update config_loader.py**

Add backward compatibility: if config has old `scoring_weights` key but no `market_weights`, map it to `market_weights` for the signals that exist, defaulting new signals (rent_growth, employment_concentration, supply_pressure) to 0.

- [ ] **Step 3: Update test_config_loader.py**

Add test that new config loads correctly with all new sections.

- [ ] **Step 4: Run tests, commit**

```bash
git add scripts/config.default.yaml scripts/config_loader.py scripts/tests/test_config_loader.py
git commit -m "feat: update config for two-layer scoring model"
```

---

## Task 4: Rewrite Scoring Engine — score.py

**Files:**
- Rewrite: `scripts/score.py`
- Rewrite: `scripts/tests/test_score.py`

This is the largest task. The scoring engine gets split into:
- `apply_hard_filters(census_df, occupation_df, config)` → filtered county FIPS list
- `score_markets(census_latest, census_earliest, occupation_data, permits_data, cbp_data, config)` → DataFrame with `market_score` per county
- `score_deals(hud_fha, market_scores, census_latest, config)` → DataFrame with `deal_score` per property
- `combine_scores(market_scores, deal_scores, config)` → DataFrame with `total_score`
- `run_scoring(data_dir, config, output_path)` → orchestrates all the above

- [ ] **Step 1: Write failing tests for new scoring model**

```python
# scripts/tests/test_score.py
import pytest
import pandas as pd
from score import apply_hard_filters, score_markets, score_deals, combine_scores, normalize_signal

def test_hard_filters():
    census = pd.DataFrame({
        "fips": ["06067", "48243", "01001"],
        "pop": [1500000, 2100, 50000],
        "total_units": [600000, 800, 20000],
        "renter_occupied": [260000, 300, 8000],
    })
    occupation = pd.DataFrame({
        "fips": ["06067", "48243", "01001"],
        "total_employed": [750000, 1000, 20000],
    })
    config = {
        "hard_filters": {
            "min_population": 20000,
            "min_housing_units": 10000,
            "min_renter_households": 5000,
            "min_employed": 10000,
        }
    }
    result = apply_hard_filters(census, occupation, config)
    # 48243 (Jeff Davis TX) should be filtered out — too small
    assert "06067" in result
    assert "48243" not in result
    assert "01001" in result

def test_score_markets():
    census_latest = pd.DataFrame({
        "fips": ["06067", "12105"],
        "county": ["Sacramento", "Polk"],
        "state": ["CA", "FL"],
        "vacancy_rate": [0.05, 0.04],
        "rent_to_cost_ratio": [0.95, 1.11],
        "median_rent": [1600, 1400],
        "pop": [1500000, 750000],
        "total_units": [600000, 300000],
        "renter_occupied": [260000, 120000],
    })
    census_earliest = pd.DataFrame({
        "fips": ["06067", "12105"],
        "vacancy_rate": [0.07, 0.06],
        "median_rent": [1400, 1200],
        "pop": [1450000, 720000],
    })
    occupation = pd.DataFrame({
        "fips": ["06067", "12105"],
        "resilience_index": [0.52, 0.58],
        "total_employed": [750000, 350000],
    })
    permits = pd.DataFrame({
        "fips": ["06067", "12105"],
        "mf_units_permitted": [3000, 500],
    })
    cbp = pd.DataFrame({
        "fips": ["06067", "12105"],
        "hhi": [800, 1200],
    })
    config = {
        "market_weights": {
            "vacancy_trend": 20, "rent_growth": 15, "rent_cost_ratio": 15,
            "workforce_resilience": 20, "employment_concentration": 10,
            "pop_growth": 10, "supply_pressure": 10,
        }
    }
    result = score_markets(census_latest, census_earliest, occupation, permits, cbp, config)
    assert "market_score" in result.columns
    assert len(result) == 2
    # Polk should score higher (better vacancy trend, better rent/cost, better resilience)
    polk = result[result["fips"] == "12105"].iloc[0]
    sacto = result[result["fips"] == "06067"].iloc[0]
    assert polk["market_score"] >= sacto["market_score"]

def test_score_deals():
    hud = pd.DataFrame({
        "fips": ["06067", "06067", "12105"],
        "property_name": ["Apts A", "Apts B", "Apts C"],
        "units": [120, 50, 200],
        "maturity_years": [1.0, 4.0, 2.0],
        "section8": [True, False, True],
        "lat": [38.58, 38.55, 27.95],
        "lng": [-121.49, -121.50, -81.70],
    })
    market_scores = pd.DataFrame({
        "fips": ["06067", "12105"],
        "market_score": [65.0, 78.0],
        "vacancy_rate": [0.05, 0.04],
    })
    config = {
        "deal_weights": {
            "mortgage_maturity": 40, "unit_count": 20,
            "section8": 20, "area_vacancy": 20,
        }
    }
    result = score_deals(hud, market_scores, pd.DataFrame(), config)
    assert "deal_score" in result.columns
    # Apts A should score highest deal (close maturity + section8 + decent units)
    apts_a = result[result["property_name"] == "Apts A"].iloc[0]
    apts_b = result[result["property_name"] == "Apts B"].iloc[0]
    assert apts_a["deal_score"] > apts_b["deal_score"]

def test_combine_scores():
    market = pd.DataFrame({"fips": ["06067"], "market_score": [70.0]})
    deals = pd.DataFrame({
        "fips": ["06067"], "property_name": ["Apts A"],
        "deal_score": [80.0], "market_score": [70.0],
    })
    config = {"market_deal_split": {"market": 60, "deal": 40}}
    result = combine_scores(market, deals, config)
    # total = (60*70 + 40*80) / 100 = 74.0
    assert abs(result.iloc[0]["total_score"] - 74.0) < 0.1
```

- [ ] **Step 2: Run tests to verify they fail**

- [ ] **Step 3: Rewrite score.py**

Implement `apply_hard_filters`, `score_markets`, `score_deals`, `combine_scores`, and updated `run_scoring`. Keep `normalize_signal` from existing code. Add rent_growth computation inline (latest rent - earliest rent / earliest rent). Add `permits_per_1k_units` computation (mf_units_permitted / total_units * 1000). Import `add_zillow_urls` from zillow.py for the final output.

Key: `score_markets` outputs a `market_scores.parquet` separately so the explorer can load it as its own table for the Investor View.

- [ ] **Step 4: Run tests, commit**

```bash
git add scripts/score.py scripts/tests/test_score.py
git commit -m "feat: rewrite scoring engine with two-layer market + deal model"
```

---

## Task 5: Monte Carlo Sensitivity — sensitivity.py

**Files:**
- Create: `scripts/sensitivity.py`
- Create: `scripts/tests/test_sensitivity.py`

- [ ] **Step 1: Write failing test**

```python
# scripts/tests/test_sensitivity.py
import pytest
import pandas as pd
import numpy as np
from sensitivity import run_monte_carlo, jitter_weights

def test_jitter_weights():
    weights = {"a": 50, "b": 30, "c": 20}
    jittered = jitter_weights(weights, jitter_pct=20)
    # Should sum to 100 after renormalization
    assert abs(sum(jittered.values()) - 100) < 0.1
    # Should be different from original (probabilistically)
    # Just check structure is preserved
    assert set(jittered.keys()) == set(weights.keys())

def test_run_monte_carlo():
    scores = pd.DataFrame({
        "fips": ["A", "B", "C"],
        "property_name": ["P1", "P2", "P3"],
        "total_score": [80.0, 60.0, 40.0],
        "signal_rank": [1, 2, 3],
        # Raw signal values needed for re-scoring
        "market_score": [70.0, 55.0, 45.0],
        "deal_score": [90.0, 65.0, 35.0],
    })
    config = {
        "market_deal_split": {"market": 60, "deal": 40},
        "sensitivity": {"iterations": 100, "jitter_pct": 20},
    }
    result = run_monte_carlo(scores, config)
    assert "stability_score" in result.columns
    assert "rank_std" in result.columns
    assert "rank_min" in result.columns
    assert "rank_max" in result.columns
    assert len(result) == 3
    # All stability scores should be 0-100
    assert result["stability_score"].between(0, 100).all()
```

- [ ] **Step 2: Write sensitivity.py**

```python
# scripts/sensitivity.py
"""Monte Carlo sensitivity analysis for scoring stability."""
import numpy as np
import pandas as pd


def jitter_weights(weights: dict, jitter_pct: float = 20) -> dict:
    """Jitter weights by ±jitter_pct% and renormalize to sum to 100."""
    jittered = {}
    for k, v in weights.items():
        low = v * (1 - jitter_pct / 100)
        high = v * (1 + jitter_pct / 100)
        jittered[k] = max(0, np.random.uniform(low, high))

    # Renormalize to 100
    total = sum(jittered.values())
    if total > 0:
        jittered = {k: round(v / total * 100, 2) for k, v in jittered.items()}
    return jittered


def run_monte_carlo(scored_df: pd.DataFrame, config: dict) -> pd.DataFrame:
    """Run Monte Carlo simulation to assess ranking stability.

    Jitters the market/deal split and recomputes total_score + rank
    across N iterations. Outputs stability metrics per property.
    """
    iterations = config.get("sensitivity", {}).get("iterations", 1000)
    jitter_pct = config.get("sensitivity", {}).get("jitter_pct", 20)
    split = config.get("market_deal_split", {"market": 60, "deal": 40})

    df = scored_df.copy()
    n = len(df)
    if n == 0:
        return df

    # Store ranks from each iteration
    all_ranks = np.zeros((iterations, n))

    baseline_rank = df["signal_rank"].values if "signal_rank" in df.columns else np.arange(1, n + 1)

    for i in range(iterations):
        # Jitter market/deal split
        jittered_split = jitter_weights(split, jitter_pct)
        m_weight = jittered_split.get("market", 60)
        d_weight = jittered_split.get("deal", 40)

        # Recompute total score with jittered split
        new_total = (
            m_weight * df["market_score"].fillna(0) +
            d_weight * df["deal_score"].fillna(0)
        ) / 100

        # Rank
        all_ranks[i] = (-new_total).argsort().argsort() + 1  # 1-based rank

    # Compute stability metrics
    rank_mean = all_ranks.mean(axis=0)
    rank_std = all_ranks.std(axis=0)
    rank_min = all_ranks.min(axis=0)
    rank_max = all_ranks.max(axis=0)

    # Stability score: % of iterations property stayed in same decile
    n_deciles = max(1, n // 10)
    baseline_decile = (baseline_rank - 1) // n_deciles
    decile_match = np.zeros(n)
    for i in range(iterations):
        iter_decile = (all_ranks[i] - 1) // n_deciles
        decile_match += (iter_decile == baseline_decile).astype(float)
    stability = (decile_match / iterations * 100).round(1)

    df["stability_score"] = stability
    df["rank_std"] = rank_std.round(2)
    df["rank_min"] = rank_min.astype(int)
    df["rank_max"] = rank_max.astype(int)

    return df
```

- [ ] **Step 3: Run tests, commit**

```bash
git add scripts/sensitivity.py scripts/tests/test_sensitivity.py
git commit -m "feat: add Monte Carlo sensitivity analysis for ranking stability"
```

---

## Task 6: Update run.py — New CLI Flags

**Files:**
- Modify: `scripts/run.py`

- [ ] **Step 1: Add new imports and CLI flags**

Add imports for `pull_permits`, `pull_cbp`, `run_monte_carlo`. Add click options:
- `--permits` — pull building permits
- `--cbp` — pull County Business Patterns
- `--sensitivity` — run Monte Carlo after scoring

Add logic blocks for each new flag, following existing pattern. `--all` should include all new steps.

Ensure `--sensitivity` runs after `--score` and before `--upload`.

- [ ] **Step 2: Verify CLI help**

Run: `cd /Users/kartikthakore/Development/fund/scripts && uv run python run.py --help`

- [ ] **Step 3: Commit**

```bash
git add scripts/run.py
git commit -m "feat: add --permits, --cbp, --sensitivity CLI flags"
```

---

## Task 7: Add numpy dependency

**Files:**
- Modify: `scripts/pyproject.toml`

- [ ] **Step 1: Add numpy to dependencies**

Add `"numpy>=1.26"` to the `dependencies` list in pyproject.toml. Run `uv sync`.

- [ ] **Step 2: Commit**

```bash
git add scripts/pyproject.toml scripts/uv.lock
git commit -m "chore: add numpy dependency for Monte Carlo"
```

Note: This should be done before Task 5 (sensitivity) since it imports numpy. Order in execution: Task 7 → Task 5.

---

## Task 8: Update Explorer — View Toggle + Sensitivity Panel + Thesis

**Files:**
- Modify: `public/explorer.html`

This is a large frontend task. Key changes:

- [ ] **Step 1: Update thesis banner**

Replace the current thesis banner with the two-layer description (Investor View + Ops View + Combined).

- [ ] **Step 2: Add view toggle**

Add three-way toggle buttons above the query bar: **Investor | Ops | Combined**

Each button sets a default SQL query and re-runs it:
- Investor: `SELECT fips, county, state, market_score, resilience_index, hhi, rent_growth, vacancy_trend, supply_pressure FROM market_scores ORDER BY market_score DESC LIMIT 50`
- Ops: `SELECT fips, property_name, city, state, deal_score, maturity_years, units, section8, market_score FROM properties WHERE market_score >= 40 ORDER BY deal_score DESC LIMIT 50`
- Combined: `SELECT * FROM properties ORDER BY total_score DESC LIMIT 50`

- [ ] **Step 3: Add market_scores table to DuckDB loading**

Add `{ path: 'data/scored/market_scores.parquet', table: 'market_scores' }` to the files array.

- [ ] **Step 4: Update config panel with two-layer weights**

Replace single scoring weights section with:
- Market Weights (7 sliders)
- Deal Weights (4 sliders)
- Market/Deal Split slider

Update scenario presets for two-layer model (Balanced, Risk-Aware Investor, Ops/Cash Flow, Durable Tenants).

- [ ] **Step 5: Add sensitivity panel**

New collapsible panel below config with:
- All market + deal weight sliders (mirrors config but for live recalculation)
- Market/Deal split slider
- "Recalculate" button that builds and executes a DuckDB SQL query with inline min-max normalization
- Rank change display showing biggest movers
- Stability badge rendering for stability_score column

- [ ] **Step 6: Update SQL presets**

Replace old presets with:
- Top Markets, Best Deals in Top Markets, Most Stable Picks, Supply Risk, Most Diversified

- [ ] **Step 7: Add stability_score badge rendering**

In `renderResults`, add badge rendering for `stability_score` column (same pattern as total_score: green ≥70, yellow 40-70, red <40).

- [ ] **Step 8: Commit**

```bash
git add public/explorer.html
git commit -m "feat: add Investor/Ops/Combined views, sensitivity panel, updated thesis"
```

---

## Task 9: Run Full Pipeline + Upload + Deploy

- [ ] **Step 1: Run all tests**

```bash
cd /Users/kartikthakore/Development/fund/scripts && uv run pytest tests/ -v
```

- [ ] **Step 2: Run full pipeline locally**

```bash
CENSUS_API_KEY=... uv run python run.py --all --local-only --output ../data
```

- [ ] **Step 3: Upload to Firebase Storage**

```bash
gsutil -m cp -r data/census/*.parquet gs://mvv-fund.firebasestorage.app/data/census/
gsutil -m cp -r data/permits/*.parquet gs://mvv-fund.firebasestorage.app/data/permits/
gsutil -m cp -r data/cbp/*.parquet gs://mvv-fund.firebasestorage.app/data/cbp/
gsutil cp data/scored/properties.parquet gs://mvv-fund.firebasestorage.app/data/scored/
gsutil cp data/scored/market_scores.parquet gs://mvv-fund.firebasestorage.app/data/scored/
```

- [ ] **Step 4: Push and deploy**

```bash
git push
firebase deploy --only hosting
```

- [ ] **Step 5: Verify explorer loads all new tables and views**
