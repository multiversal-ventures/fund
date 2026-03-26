# Data Pipeline & Property Finder — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build an automated data pipeline that pulls Census/BLS/HUD data into Parquet files, scores multifamily acquisition targets, and serves them through a DuckDB WASM browser explorer with Firebase auth, configurable parameters, and Zillow deep-links.

**Architecture:** Python scripts (managed with uv) pull data from free public APIs, compute acquisition scores, and upload per-year Parquet files to Firebase Storage. A browser-based explorer loads Parquet via DuckDB WASM for SQL querying. Config is editable from the dashboard (stored in Firestore), and a Cloud Function triggers GitHub Actions to re-run the pipeline.

**Tech Stack:** Python 3.12+ (uv), pandas, pyarrow, duckdb, firebase-admin, DuckDB WASM, Firebase (Hosting/Storage/Firestore/Functions), GitHub Actions

**Spec:** `docs/superpowers/specs/2026-03-25-data-pipeline-property-finder-design.md`

---

## File Map

### Scripts (Python, managed with uv)
| File | Responsibility |
|------|---------------|
| `scripts/pyproject.toml` | uv project: dependencies (pandas, pyarrow, duckdb, requests, firebase-admin, pyyaml, click) |
| `scripts/config.default.yaml` | Default 147 target markets (FIPS codes), scoring weights, thresholds |
| `scripts/config_loader.py` | Load config from local YAML or Firestore; merge defaults |
| `scripts/pull_census.py` | Census ACS 5-Year API → per-year parquet (rolling 3yr window) |
| `scripts/pull_bls.py` | BLS OEWS special requests → per-year parquet |
| `scripts/pull_hud.py` | HUD FHA multifamily + USPS vacancy via SODA API → parquet |
| `scripts/score.py` | Join Census + HUD data, compute weighted acquisition scores |
| `scripts/zillow.py` | County FIPS → lat/lng bounds → Zillow deep-link URLs |
| `scripts/upload.py` | Push parquet files to Firebase Storage with service account |
| `scripts/run.py` | CLI entry point (click): --all, --census, --bls, --hud, --score, --upload, --config, --local-only |

### Tests
| File | Tests |
|------|-------|
| `scripts/tests/test_config_loader.py` | YAML loading, Firestore loading, defaults merging |
| `scripts/tests/test_pull_census.py` | API response parsing, parquet schema, year filtering |
| `scripts/tests/test_pull_bls.py` | OEWS file parsing, parquet schema |
| `scripts/tests/test_pull_hud.py` | SODA response parsing, parquet schema |
| `scripts/tests/test_score.py` | Scoring math, weight normalization, ranking |
| `scripts/tests/test_zillow.py` | URL generation, bounds encoding |

### Frontend
| File | Responsibility |
|------|---------------|
| `public/explorer.html` | DuckDB WASM query UI: config panel, SQL bar, results table, charts, exports, Zillow links |

### Firebase
| File | Responsibility |
|------|---------------|
| `firestore.rules` | Auth-gated config read/write, run status read |
| `storage.rules` | Auth-gated parquet read, deny client writes |
| `firebase.json` | Hosting + Firestore + Storage + Functions config |
| `functions/package.json` | Cloud Function dependencies |
| `functions/index.js` | Firestore onWrite → GitHub workflow_dispatch |

### CI/CD
| File | Responsibility |
|------|---------------|
| `.github/workflows/refresh.yml` | Monthly cron + workflow_dispatch: uv sync, run pipeline, upload |
| `.github/workflows/deploy.yml` | Deploy Firebase Hosting on push to main |

---

## Task 1: Project Setup — uv + pyproject.toml

**Files:**
- Create: `scripts/pyproject.toml`
- Create: `scripts/.python-version`
- Create: `.gitignore` additions

- [ ] **Step 1: Create pyproject.toml**

```toml
[project]
name = "fund-pipeline"
version = "0.1.0"
description = "Multifamily fund data pipeline"
requires-python = ">=3.12"
dependencies = [
    "pandas>=2.2",
    "pyarrow>=17.0",
    "duckdb>=1.1",
    "requests>=2.32",
    "firebase-admin>=6.5",
    "pyyaml>=6.0",
    "click>=8.1",
]

[tool.uv]
dev-dependencies = [
    "pytest>=8.3",
    "pytest-mock>=3.14",
]
```

- [ ] **Step 2: Create .python-version**

```
3.12
```

- [ ] **Step 3: Update .gitignore**

Append to existing `.gitignore`:
```
# Python / uv
__pycache__/
*.pyc
.venv/
scripts/.venv/
data/

# Firebase
.firebase/
firebase-debug.log
*.log

# Node (functions)
functions/node_modules/
```

- [ ] **Step 4: Run uv sync**

Run: `cd scripts && uv sync`
Expected: Creates `.venv/`, installs all dependencies, generates `uv.lock`

- [ ] **Step 5: Verify**

Run: `cd scripts && uv run python -c "import pandas, pyarrow, duckdb, requests, yaml, click; print('all deps OK')"`
Expected: `all deps OK`

- [ ] **Step 6: Commit**

```bash
git add scripts/pyproject.toml scripts/.python-version scripts/uv.lock .gitignore
git commit -m "feat: initialize uv project with pipeline dependencies"
```

---

## Task 2: Config System — config.default.yaml + config_loader.py

**Files:**
- Create: `scripts/config.default.yaml`
- Create: `scripts/config_loader.py`
- Create: `scripts/tests/test_config_loader.py`

- [ ] **Step 1: Write config.default.yaml**

```yaml
# Target markets: FIPS codes of qualifying counties
# Default: 147 counties with 50K+ units, 10K+ renters, rent > owner cost, vacancy falling
# This list is seeded with the top-scoring markets from our national screening.
# Full list generated on first pipeline run via screening thresholds below.
target_markets:
  # California
  - "06067"  # Sacramento
  - "06065"  # Riverside (Inland Empire)
  - "06071"  # San Bernardino (Inland Empire)
  - "06019"  # Fresno
  - "06029"  # Kern / Bakersfield
  # Florida
  - "12105"  # Polk
  - "12083"  # Marion
  - "12101"  # Pasco
  - "12115"  # Sarasota
  - "12097"  # Osceola
  # Arizona
  - "04013"  # Maricopa
  # Add more or set to "auto" to use screening thresholds

# Set to true to auto-discover markets matching thresholds
auto_discover: true

screening_thresholds:
  min_total_units: 50000
  min_renter_households: 10000
  rent_exceeds_owner_cost: true
  vacancy_falling: true

scoring_weights:
  mortgage_maturity: 20
  vacancy_trend: 25
  rent_cost_ratio: 30
  area_vacancy: 10
  pop_growth: 15

census:
  years: [2021, 2022, 2023]
  tables:
    - B25001  # Total housing units
    - B25002  # Occupancy status
    - B25003  # Tenure
    - B25004  # Vacancy status
    - B25024  # Units in structure
    - B25064  # Median gross rent
    - B25077  # Median home value
    - B25105  # Median monthly owner cost
    - B01003  # Population
```

- [ ] **Step 2: Write failing test for config_loader**

```python
# scripts/tests/test_config_loader.py
import pytest
from pathlib import Path
from config_loader import load_config

def test_load_yaml_config():
    config = load_config(str(Path(__file__).parent.parent / "config.default.yaml"))
    assert "target_markets" in config
    assert "scoring_weights" in config
    assert config["scoring_weights"]["rent_cost_ratio"] == 30
    assert len(config["target_markets"]) >= 11

def test_load_yaml_weights_sum_to_100():
    config = load_config(str(Path(__file__).parent.parent / "config.default.yaml"))
    total = sum(config["scoring_weights"].values())
    assert total == 100

def test_load_config_missing_file():
    with pytest.raises(FileNotFoundError):
        load_config("/nonexistent/config.yaml")

def test_load_config_merges_defaults():
    """Partial config should be filled with defaults."""
    config = load_config(str(Path(__file__).parent.parent / "config.default.yaml"))
    assert "census" in config
    assert "years" in config["census"]
```

- [ ] **Step 3: Run test to verify it fails**

Run: `cd scripts && uv run pytest tests/test_config_loader.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'config_loader'`

- [ ] **Step 4: Write config_loader.py**

```python
# scripts/config_loader.py
"""Load pipeline configuration from YAML file or Firestore."""
import yaml
from pathlib import Path

DEFAULT_CONFIG_PATH = Path(__file__).parent / "config.default.yaml"

def load_config(source: str = None) -> dict:
    """Load config from a YAML file path or 'firestore'.

    Args:
        source: Path to YAML file, or 'firestore' to load from Firestore.
                Defaults to config.default.yaml.
    Returns:
        Merged configuration dict.
    """
    if source is None:
        source = str(DEFAULT_CONFIG_PATH)

    if source == "firestore":
        return _load_from_firestore()

    path = Path(source)
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {source}")

    with open(path) as f:
        config = yaml.safe_load(f)

    return _merge_defaults(config)


def _load_from_firestore() -> dict:
    """Load config from Firestore /config/pipeline document."""
    import firebase_admin
    from firebase_admin import firestore

    if not firebase_admin._apps:
        firebase_admin.initialize_app()

    db = firestore.client()
    doc = db.collection("config").document("pipeline").get()

    if doc.exists:
        return _merge_defaults(doc.to_dict())

    # Fall back to defaults if no Firestore config exists
    return _merge_defaults({})


def _merge_defaults(config: dict) -> dict:
    """Merge provided config with defaults. Provided values take precedence."""
    with open(DEFAULT_CONFIG_PATH) as f:
        defaults = yaml.safe_load(f)

    merged = defaults.copy()
    for key, value in config.items():
        if isinstance(value, dict) and key in merged and isinstance(merged[key], dict):
            merged[key] = {**merged[key], **value}
        else:
            merged[key] = value

    return merged
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cd scripts && uv run pytest tests/test_config_loader.py -v`
Expected: 4 passed

- [ ] **Step 6: Commit**

```bash
git add scripts/config.default.yaml scripts/config_loader.py scripts/tests/test_config_loader.py
git commit -m "feat: add config system with YAML and Firestore support"
```

---

## Task 3: Census ACS Data Pull — pull_census.py

**Files:**
- Create: `scripts/pull_census.py`
- Create: `scripts/tests/test_pull_census.py`

- [ ] **Step 1: Write failing test**

```python
# scripts/tests/test_pull_census.py
import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from pull_census import fetch_acs_year, build_census_url, parse_census_response

def test_build_census_url():
    url = build_census_url(2023, ["B25001_001E", "B25002_002E"])
    assert "api.census.gov" in url
    assert "2023" in url
    assert "B25001_001E" in url
    assert "county:*" in url

def test_parse_census_response():
    """Census API returns header row + data rows."""
    raw = [
        ["NAME", "B25001_001E", "B25002_002E", "state", "county"],
        ["Los Angeles County, California", "3500000", "3200000", "06", "037"],
        ["Kern County, California", "300000", "270000", "06", "029"],
    ]
    df = parse_census_response(raw, 2023)
    assert len(df) == 2
    assert "fips" in df.columns
    assert "year" in df.columns
    assert df.iloc[0]["fips"] == "06037"
    assert df.iloc[0]["year"] == 2023

def test_fetch_acs_year_parquet_schema(tmp_path):
    """Output parquet should have the expected schema."""
    mock_response = [
        ["NAME", "B25001_001E", "B25002_002E", "B25002_003E",
         "B25003_002E", "B25003_003E", "B25004_002E",
         "B25024_007E", "B25024_008E", "B25024_009E", "B25024_010E", "B25024_011E",
         "B25064_001E", "B25077_001E", "B25105_001E", "B01003_001E",
         "state", "county"],
        ["Test County, State", "100000", "90000", "10000",
         "50000", "40000", "3000",
         "2000", "3000", "4000", "5000", "6000",
         "1500", "350000", "1600", "500000",
         "06", "001"],
    ]
    with patch("pull_census.requests.get") as mock_get:
        mock_resp = MagicMock()
        mock_resp.json.return_value = mock_response
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        output = tmp_path / "acs_2023.parquet"
        df = fetch_acs_year(2023, output_path=str(output), api_key="test")

        assert output.exists()
        schema_cols = {"fips", "county", "state", "total_units", "occupied",
                       "vacant", "owner_occupied", "renter_occupied", "for_rent_vacant",
                       "median_rent", "median_home_value", "median_owner_cost",
                       "mf_units", "mf_pct", "pop", "vacancy_rate",
                       "rental_vac_rate", "rent_to_cost_ratio", "year"}
        assert schema_cols.issubset(set(df.columns))
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd scripts && uv run pytest tests/test_pull_census.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'pull_census'`

- [ ] **Step 3: Write pull_census.py**

```python
# scripts/pull_census.py
"""Pull Census ACS 5-Year data and write per-year Parquet files."""
import os
import requests
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path

CENSUS_BASE = "https://api.census.gov/data"

# ACS variable codes
VARIABLES = [
    "B25001_001E",  # Total housing units
    "B25002_002E",  # Occupied
    "B25002_003E",  # Vacant
    "B25003_002E",  # Owner occupied
    "B25003_003E",  # Renter occupied
    "B25004_002E",  # For rent vacant
    "B25024_007E",  # 10-19 units
    "B25024_008E",  # 20-49 units
    "B25024_009E",  # 50+ units (2020+ coding)
    "B25024_010E",  # Additional MF category
    "B25024_011E",  # Additional MF category
    "B25064_001E",  # Median gross rent
    "B25077_001E",  # Median home value
    "B25105_001E",  # Median monthly owner cost
    "B01003_001E",  # Population
]

STATE_FIPS_TO_ABBR = {
    "01": "AL", "02": "AK", "04": "AZ", "05": "AR", "06": "CA",
    "08": "CO", "09": "CT", "10": "DE", "11": "DC", "12": "FL",
    "13": "GA", "15": "HI", "16": "ID", "17": "IL", "18": "IN",
    "19": "IA", "20": "KS", "21": "KY", "22": "LA", "23": "ME",
    "24": "MD", "25": "MA", "26": "MI", "27": "MN", "28": "MS",
    "29": "MO", "30": "MT", "31": "NE", "32": "NV", "33": "NH",
    "34": "NJ", "35": "NM", "36": "NY", "37": "NC", "38": "ND",
    "39": "OH", "40": "OK", "41": "OR", "42": "PA", "44": "RI",
    "45": "SC", "46": "SD", "47": "TN", "48": "TX", "49": "UT",
    "50": "VT", "51": "VA", "53": "WA", "54": "WV", "55": "WI",
    "56": "WY", "72": "PR",
}


def build_census_url(year: int, variables: list[str] = None, api_key: str = None) -> str:
    """Build Census ACS 5-Year API URL for all counties."""
    if variables is None:
        variables = VARIABLES
    var_str = ",".join(variables)
    url = f"{CENSUS_BASE}/{year}/acs/acs5?get=NAME,{var_str}&for=county:*&in=state:*"
    if api_key:
        url += f"&key={api_key}"
    return url


def parse_census_response(raw: list[list], year: int) -> pd.DataFrame:
    """Parse Census API JSON response into a DataFrame."""
    header = raw[0]
    data = raw[1:]
    df = pd.DataFrame(data, columns=header)

    # Build FIPS code
    df["fips"] = df["state"] + df["county"]

    # Extract county name (remove ", State" suffix)
    df["county"] = df["NAME"].str.replace(r",.*$", "", regex=True).str.strip()
    df["state"] = df["state"].map(STATE_FIPS_TO_ABBR)

    # Convert numeric columns
    numeric_cols = [c for c in VARIABLES if c in df.columns]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Compute derived fields
    df["total_units"] = df["B25001_001E"]
    df["occupied"] = df["B25002_002E"]
    df["vacant"] = df["B25002_003E"]
    df["owner_occupied"] = df["B25003_002E"]
    df["renter_occupied"] = df["B25003_003E"]
    df["for_rent_vacant"] = df["B25004_002E"]

    # Multifamily: sum of 10-19, 20-49, 50+ unit structures
    mf_cols = ["B25024_007E", "B25024_008E", "B25024_009E", "B25024_010E", "B25024_011E"]
    existing_mf = [c for c in mf_cols if c in df.columns]
    df["mf_units"] = df[existing_mf].sum(axis=1)
    df["mf_pct"] = (df["mf_units"] / df["total_units"]).round(4)

    df["median_rent"] = df["B25064_001E"]
    df["median_home_value"] = df["B25077_001E"]
    df["median_owner_cost"] = df["B25105_001E"]
    df["pop"] = df["B01003_001E"]

    df["vacancy_rate"] = (df["vacant"] / df["total_units"]).round(4)
    df["rental_vac_rate"] = (
        df["for_rent_vacant"] / (df["renter_occupied"] + df["for_rent_vacant"])
    ).round(4)
    df["rent_to_cost_ratio"] = (df["median_rent"] / df["median_owner_cost"]).round(4)

    df["year"] = year

    # Select final columns
    keep = [
        "fips", "county", "state", "total_units", "occupied", "vacant",
        "owner_occupied", "renter_occupied", "for_rent_vacant",
        "median_rent", "median_home_value", "median_owner_cost",
        "mf_units", "mf_pct", "pop", "vacancy_rate",
        "rental_vac_rate", "rent_to_cost_ratio", "year",
    ]
    return df[keep].copy()


def fetch_acs_year(year: int, output_path: str = None, api_key: str = None) -> pd.DataFrame:
    """Fetch ACS data for a single year and optionally write to Parquet."""
    api_key = api_key or os.environ.get("CENSUS_API_KEY", "")
    url = build_census_url(year, api_key=api_key)

    resp = requests.get(url, timeout=120)
    resp.raise_for_status()
    raw = resp.json()

    df = parse_census_response(raw, year)

    if output_path:
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(output_path, index=False)
        print(f"  Wrote {len(df)} rows → {output_path}")

    return df


def pull_census(years: list[int], output_dir: str, api_key: str = None) -> dict[int, pd.DataFrame]:
    """Pull Census ACS data for multiple years."""
    print(f"Pulling Census ACS data for {years}...")
    results = {}
    for year in years:
        print(f"  Fetching {year}...")
        output_path = str(Path(output_dir) / f"acs_{year}.parquet")
        results[year] = fetch_acs_year(year, output_path=output_path, api_key=api_key)
    return results
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd scripts && uv run pytest tests/test_pull_census.py -v`
Expected: 3 passed

- [ ] **Step 5: Commit**

```bash
git add scripts/pull_census.py scripts/tests/test_pull_census.py
git commit -m "feat: add Census ACS data pull with per-year parquet output"
```

---

## Task 4: BLS OEWS Data Pull — pull_bls.py

**Files:**
- Create: `scripts/pull_bls.py`
- Create: `scripts/tests/test_pull_bls.py`

- [ ] **Step 1: Write failing test**

```python
# scripts/tests/test_pull_bls.py
import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from pull_bls import parse_oews_data, build_oews_url

def test_build_oews_url():
    url = build_oews_url(2024)
    assert "bls.gov" in url
    assert "oesm24" in url.lower() or "2024" in url

def test_parse_oews_data():
    """Parse BLS OEWS Excel/CSV format into DataFrame."""
    raw_df = pd.DataFrame({
        "AREA": ["31080", "40140"],
        "AREA_TITLE": ["Los Angeles-Long Beach-Anaheim, CA", "Riverside-San Bernardino-Ontario, CA"],
        "OCC_CODE": ["47-2111", "47-2111"],
        "OCC_TITLE": ["Electricians", "Electricians"],
        "TOT_EMP": ["21070", "7570"],
        "H_MEDIAN": ["35.12", "30.50"],
        "A_MEDIAN": ["73050", "63440"],
        "LOC_QUOTIENT": ["1.15", "0.98"],
    })
    df = parse_oews_data(raw_df, 2024)
    assert len(df) == 2
    assert "year" in df.columns
    assert "metro_code" in df.columns
    assert df.iloc[0]["total_employment"] == 21070
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd scripts && uv run pytest tests/test_pull_bls.py -v`
Expected: FAIL

- [ ] **Step 3: Write pull_bls.py**

```python
# scripts/pull_bls.py
"""Pull BLS OEWS employment data and write per-year Parquet files."""
import os
import io
import requests
import pandas as pd
from pathlib import Path

BLS_OEWS_BASE = "https://www.bls.gov/oes/special-requests"


def build_oews_url(year: int) -> str:
    """Build URL for BLS OEWS metro-level data file."""
    short_year = str(year)[2:]
    return f"{BLS_OEWS_BASE}/oesm{short_year}ma.zip"


def parse_oews_data(raw_df: pd.DataFrame, year: int) -> pd.DataFrame:
    """Parse raw OEWS data into cleaned DataFrame."""
    df = raw_df.copy()

    # Standardize column names
    col_map = {
        "AREA": "metro_code",
        "AREA_TITLE": "metro_name",
        "OCC_CODE": "occ_code",
        "OCC_TITLE": "occ_title",
        "TOT_EMP": "total_employment",
        "H_MEDIAN": "hourly_median",
        "A_MEDIAN": "annual_median",
        "LOC_QUOTIENT": "location_quotient",
    }
    df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})

    # Convert numeric columns
    for col in ["total_employment", "hourly_median", "annual_median", "location_quotient"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df["year"] = year
    return df


def fetch_oews_year(year: int, output_path: str = None) -> pd.DataFrame:
    """Fetch OEWS data for a single year."""
    url = build_oews_url(year)
    print(f"  Downloading {url}...")

    resp = requests.get(url, timeout=120)
    resp.raise_for_status()

    # OEWS files are zipped Excel
    import zipfile
    with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
        # Find the main data file
        excel_files = [f for f in zf.namelist() if f.endswith((".xlsx", ".xls"))]
        if not excel_files:
            csv_files = [f for f in zf.namelist() if f.endswith(".csv")]
            if csv_files:
                with zf.open(csv_files[0]) as f:
                    raw_df = pd.read_csv(f)
            else:
                raise ValueError(f"No data files found in {url}")
        else:
            with zf.open(excel_files[0]) as f:
                raw_df = pd.read_excel(f)

    df = parse_oews_data(raw_df, year)

    if output_path:
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(output_path, index=False)
        print(f"  Wrote {len(df)} rows → {output_path}")

    return df


def pull_bls(years: list[int], output_dir: str) -> dict[int, pd.DataFrame]:
    """Pull BLS OEWS data for multiple years."""
    print(f"Pulling BLS OEWS data for {years}...")
    results = {}
    for year in years:
        print(f"  Fetching {year}...")
        output_path = str(Path(output_dir) / f"oews_{year}.parquet")
        results[year] = fetch_oews_year(year, output_path=output_path)
    return results
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd scripts && uv run pytest tests/test_pull_bls.py -v`
Expected: 2 passed

- [ ] **Step 5: Commit**

```bash
git add scripts/pull_bls.py scripts/tests/test_pull_bls.py
git commit -m "feat: add BLS OEWS data pull with per-year parquet output"
```

---

## Task 5: HUD Data Pull — pull_hud.py

**Files:**
- Create: `scripts/pull_hud.py`
- Create: `scripts/tests/test_pull_hud.py`

- [ ] **Step 1: Write failing test**

```python
# scripts/tests/test_pull_hud.py
import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from pull_hud import parse_fha_response, parse_usps_vacancy, build_fha_url

def test_build_fha_url():
    url = build_fha_url(limit=10, offset=0)
    assert "data.hud.gov" in url

def test_parse_fha_response():
    raw = [
        {
            "property_name": "Sunset Apartments",
            "property_street": "123 Main St",
            "city_name_text": "Sacramento",
            "state_code": "CA",
            "zip_code": "95814",
            "units_tot_cnt": "120",
            "fha_loan_id": "12345",
            "orig_mortgage_amt": "5000000",
            "maturity_date": "2027-06-15T00:00:00.000",
            "soa_cd_txt": "Section 8",
            "latitude": "38.58",
            "longitude": "-121.49",
            "fips_state_cd": "06",
            "fips_cnty_cd": "067",
        }
    ]
    df = parse_fha_response(raw)
    assert len(df) == 1
    assert df.iloc[0]["fips"] == "06067"
    assert df.iloc[0]["units"] == 120
    assert df.iloc[0]["property_name"] == "Sunset Apartments"
    assert df.iloc[0]["section8"] == True

def test_parse_usps_vacancy():
    raw = [
        {
            "geoid": "06067",
            "year": "2024",
            "quarter": "4",
            "tot_res": "600000",
            "res_vac": "25000",
        }
    ]
    df = parse_usps_vacancy(raw)
    assert len(df) == 1
    assert abs(df.iloc[0]["usps_vacancy_rate"] - 0.0417) < 0.001
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd scripts && uv run pytest tests/test_pull_hud.py -v`
Expected: FAIL

- [ ] **Step 3: Write pull_hud.py**

```python
# scripts/pull_hud.py
"""Pull HUD FHA multifamily and USPS vacancy data via SODA API."""
import requests
import pandas as pd
from pathlib import Path
from datetime import datetime

FHA_ENDPOINT = "https://data.hud.gov/resource/fyha-h8fe.json"
USPS_ENDPOINT = "https://data.hud.gov/resource/bik6-2gqh.json"
SODA_LIMIT = 50000


def build_fha_url(limit: int = SODA_LIMIT, offset: int = 0) -> str:
    """Build SODA API URL for FHA multifamily insured mortgages."""
    return f"{FHA_ENDPOINT}?$limit={limit}&$offset={offset}"


def parse_fha_response(raw: list[dict]) -> pd.DataFrame:
    """Parse FHA multifamily SODA response into DataFrame."""
    df = pd.DataFrame(raw)

    col_map = {
        "property_name": "property_name",
        "property_street": "address",
        "city_name_text": "city",
        "state_code": "state",
        "zip_code": "zip",
        "units_tot_cnt": "units",
        "fha_loan_id": "loan_id",
        "orig_mortgage_amt": "mortgage_amount",
        "maturity_date": "maturity_date",
        "soa_cd_txt": "program",
        "latitude": "lat",
        "longitude": "lng",
        "fips_state_cd": "fips_state",
        "fips_cnty_cd": "fips_county",
    }
    df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})

    df["fips"] = df["fips_state"].astype(str).str.zfill(2) + df["fips_county"].astype(str).str.zfill(3)
    df["units"] = pd.to_numeric(df["units"], errors="coerce").fillna(0).astype(int)
    df["mortgage_amount"] = pd.to_numeric(df["mortgage_amount"], errors="coerce")
    df["lat"] = pd.to_numeric(df["lat"], errors="coerce")
    df["lng"] = pd.to_numeric(df["lng"], errors="coerce")

    # Parse maturity date
    df["maturity_date"] = pd.to_datetime(df["maturity_date"], errors="coerce")
    now = datetime.now()
    df["maturity_years"] = ((df["maturity_date"] - now).dt.days / 365.25).round(2)

    # Section 8 flag
    df["section8"] = df["program"].str.contains("Section 8", case=False, na=False)

    keep = [
        "fips", "property_name", "address", "city", "state", "zip",
        "units", "mortgage_amount", "maturity_date", "maturity_years",
        "section8", "lat", "lng", "loan_id",
    ]
    return df[[c for c in keep if c in df.columns]].copy()


def parse_usps_vacancy(raw: list[dict]) -> pd.DataFrame:
    """Parse USPS vacancy SODA response into DataFrame."""
    df = pd.DataFrame(raw)

    df["geoid"] = df["geoid"].astype(str).str.zfill(5)
    df["tot_res"] = pd.to_numeric(df["tot_res"], errors="coerce")
    df["res_vac"] = pd.to_numeric(df["res_vac"], errors="coerce")
    df["usps_vacancy_rate"] = (df["res_vac"] / df["tot_res"]).round(4)
    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df["quarter"] = pd.to_numeric(df["quarter"], errors="coerce")

    return df.rename(columns={"geoid": "fips"})


def fetch_fha_multifamily(output_path: str = None) -> pd.DataFrame:
    """Fetch all FHA multifamily insured mortgages, paginating through SODA."""
    print("  Fetching HUD FHA multifamily data...")
    all_records = []
    offset = 0

    while True:
        url = build_fha_url(limit=SODA_LIMIT, offset=offset)
        resp = requests.get(url, timeout=120)
        resp.raise_for_status()
        batch = resp.json()

        if not batch:
            break

        all_records.extend(batch)
        offset += SODA_LIMIT
        print(f"    Fetched {len(all_records)} records so far...")

        if len(batch) < SODA_LIMIT:
            break

    df = parse_fha_response(all_records)

    if output_path:
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(output_path, index=False)
        print(f"  Wrote {len(df)} rows → {output_path}")

    return df


def fetch_usps_vacancy(output_path: str = None) -> pd.DataFrame:
    """Fetch latest USPS vacancy data at county level."""
    print("  Fetching HUD USPS vacancy data...")
    url = f"{USPS_ENDPOINT}?$limit={SODA_LIMIT}&$order=year DESC,quarter DESC"
    resp = requests.get(url, timeout=120)
    resp.raise_for_status()

    df = parse_usps_vacancy(resp.json())

    if output_path:
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(output_path, index=False)
        print(f"  Wrote {len(df)} rows → {output_path}")

    return df


def pull_hud(output_dir: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Pull all HUD data."""
    print("Pulling HUD data...")
    fha = fetch_fha_multifamily(str(Path(output_dir) / "fha_multifamily.parquet"))
    usps = fetch_usps_vacancy(str(Path(output_dir) / "usps_vacancy.parquet"))
    return fha, usps
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd scripts && uv run pytest tests/test_pull_hud.py -v`
Expected: 3 passed

- [ ] **Step 5: Commit**

```bash
git add scripts/pull_hud.py scripts/tests/test_pull_hud.py
git commit -m "feat: add HUD FHA multifamily and USPS vacancy data pull"
```

---

## Task 6: Zillow Deep-Link Generator — zillow.py

**Files:**
- Create: `scripts/zillow.py`
- Create: `scripts/tests/test_zillow.py`

- [ ] **Step 1: Write failing test**

```python
# scripts/tests/test_zillow.py
import pytest
import json
from urllib.parse import unquote
from zillow import build_zillow_url, county_bounds, COUNTY_CENTROIDS

def test_build_zillow_url_from_bounds():
    url = build_zillow_url(south=27.6, north=27.9, west=-81.2, east=-80.5)
    assert "zillow.com/homes/for_sale" in url
    assert "searchQueryState" in url
    # Decode and verify bounds
    qs = unquote(url.split("searchQueryState=")[1])
    state = json.loads(qs)
    assert state["mapBounds"]["south"] == 27.6
    assert state["isMapVisible"] == True
    assert state["isListVisible"] == True

def test_build_zillow_url_from_point():
    """Build URL from lat/lng point with radius."""
    url = build_zillow_url(lat=38.58, lng=-121.49, radius_deg=0.01)
    qs = unquote(url.split("searchQueryState=")[1])
    state = json.loads(qs)
    assert abs(state["mapBounds"]["south"] - 38.57) < 0.001

def test_county_bounds_known_fips():
    bounds = county_bounds("06067")  # Sacramento
    assert bounds is not None
    assert "south" in bounds
    assert bounds["south"] < bounds["north"]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd scripts && uv run pytest tests/test_zillow.py -v`
Expected: FAIL

- [ ] **Step 3: Write zillow.py**

```python
# scripts/zillow.py
"""Generate Zillow deep-link URLs from county FIPS or lat/lng coordinates."""
import json
from urllib.parse import quote

# County FIPS → approximate centroid lat/lng and bounding box
# Generated from Census TIGER/Line county boundaries
# This is a subset for target markets; full list loaded from data file if available
COUNTY_CENTROIDS = {
    # California
    "06067": {"lat": 38.45, "lng": -121.34, "south": 38.02, "north": 38.74, "west": -121.86, "east": -120.99},
    "06065": {"lat": 33.74, "lng": -116.17, "south": 33.42, "north": 34.08, "west": -117.67, "east": -114.43},
    "06071": {"lat": 34.84, "lng": -116.18, "south": 34.03, "north": 35.81, "west": -117.65, "east": -114.13},
    "06019": {"lat": 36.76, "lng": -119.65, "south": 36.39, "north": 37.27, "west": -120.32, "east": -118.36},
    "06029": {"lat": 35.35, "lng": -118.73, "south": 34.79, "north": 35.79, "west": -119.86, "east": -117.63},
    # Florida
    "12105": {"lat": 27.95, "lng": -81.70, "south": 27.64, "north": 28.26, "west": -82.11, "east": -81.19},
    "12083": {"lat": 29.21, "lng": -82.07, "south": 28.85, "north": 29.48, "west": -82.66, "east": -81.43},
    "12101": {"lat": 28.32, "lng": -82.46, "south": 28.13, "north": 28.52, "west": -82.90, "east": -82.05},
    "12115": {"lat": 27.18, "lng": -82.36, "south": 26.94, "north": 27.46, "west": -82.85, "east": -82.02},
    "12097": {"lat": 28.07, "lng": -81.16, "south": 27.82, "north": 28.31, "west": -81.66, "east": -80.85},
    # Arizona
    "04013": {"lat": 33.35, "lng": -112.49, "south": 32.51, "north": 34.04, "west": -113.33, "east": -111.04},
}


def county_bounds(fips: str) -> dict | None:
    """Get bounding box for a county FIPS code."""
    return COUNTY_CENTROIDS.get(fips)


def build_zillow_url(
    south: float = None, north: float = None,
    west: float = None, east: float = None,
    lat: float = None, lng: float = None,
    radius_deg: float = 0.15,
    zoom: int = 11,
) -> str:
    """Build a Zillow search URL with map bounds.

    Either provide explicit bounds (south/north/west/east) or a center point
    (lat/lng) with radius_deg to auto-compute bounds.
    """
    if lat is not None and lng is not None:
        south = lat - radius_deg
        north = lat + radius_deg
        west = lng - radius_deg
        east = lng + radius_deg
        if radius_deg <= 0.02:
            zoom = 16
        elif radius_deg <= 0.05:
            zoom = 14

    search_state = {
        "pagination": {},
        "isMapVisible": True,
        "mapBounds": {
            "west": west,
            "east": east,
            "south": south,
            "north": north,
        },
        "filterState": {
            "sort": {"value": "globalrelevanceex"},
        },
        "isListVisible": True,
        "mapZoom": zoom,
    }

    encoded = quote(json.dumps(search_state, separators=(",", ":")))
    return f"https://www.zillow.com/homes/for_sale/?searchQueryState={encoded}"


def add_zillow_urls(df, fips_col: str = "fips", lat_col: str = "lat", lng_col: str = "lng") -> list[str]:
    """Generate Zillow URLs for each row in a DataFrame.

    Uses property lat/lng if available, falls back to county bounds.
    """
    urls = []
    for _, row in df.iterrows():
        # Try property-level coordinates first
        if lat_col in row and lng_col in row and pd.notna(row.get(lat_col)) and pd.notna(row.get(lng_col)):
            url = build_zillow_url(lat=float(row[lat_col]), lng=float(row[lng_col]), radius_deg=0.01)
        # Fall back to county bounds
        elif fips_col in row:
            bounds = county_bounds(str(row[fips_col]))
            if bounds:
                url = build_zillow_url(**{k: v for k, v in bounds.items() if k in ("south", "north", "west", "east")})
            else:
                url = ""
        else:
            url = ""
        urls.append(url)
    return urls


# Needed for add_zillow_urls
import pandas as pd
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd scripts && uv run pytest tests/test_zillow.py -v`
Expected: 3 passed

- [ ] **Step 5: Commit**

```bash
git add scripts/zillow.py scripts/tests/test_zillow.py
git commit -m "feat: add Zillow deep-link URL generator from county FIPS/coords"
```

---

## Task 7: Acquisition Signal Scoring — score.py

**Files:**
- Create: `scripts/score.py`
- Create: `scripts/tests/test_score.py`

- [ ] **Step 1: Write failing test**

```python
# scripts/tests/test_score.py
import pytest
import pandas as pd
from score import score_properties, normalize_signal

def test_normalize_signal_linear():
    values = pd.Series([0, 5, 10])
    result = normalize_signal(values, weight=20, higher_is_better=True)
    assert result.iloc[0] == 0.0
    assert result.iloc[2] == 20.0

def test_normalize_signal_inverse():
    """Lower maturity_years = better (closer to maturing)."""
    values = pd.Series([1, 3, 5])
    result = normalize_signal(values, weight=20, higher_is_better=False)
    assert result.iloc[0] == 20.0  # 1yr to maturity = max score
    assert result.iloc[2] == 0.0   # 5yr = min score

def test_score_properties():
    census = pd.DataFrame({
        "fips": ["06067", "12105"],
        "county": ["Sacramento", "Polk"],
        "state": ["CA", "FL"],
        "vacancy_rate": [0.05, 0.04],
        "rent_to_cost_ratio": [0.95, 1.11],
        "mf_pct": [0.15, 0.10],
        "pop": [1500000, 750000],
        "year": [2023, 2023],
    })
    census_prev = census.copy()
    census_prev["year"] = 2021
    census_prev["vacancy_rate"] = [0.07, 0.06]
    census_prev["pop"] = [1450000, 720000]

    hud = pd.DataFrame({
        "fips": ["06067", "12105"],
        "property_name": ["Sunset Apts", "Lakeland Place"],
        "address": ["123 Main", "456 Oak"],
        "units": [120, 200],
        "mortgage_amount": [5000000, 8000000],
        "maturity_years": [2.0, 1.0],
        "section8": [False, True],
        "lat": [38.58, 27.95],
        "lng": [-121.49, -81.70],
    })

    weights = {
        "mortgage_maturity": 20,
        "vacancy_trend": 25,
        "rent_cost_ratio": 30,
        "area_vacancy": 10,
        "pop_growth": 15,
    }

    result = score_properties(census, census_prev, hud, weights)
    assert "total_score" in result.columns
    assert "signal_rank" in result.columns
    assert "zillow_url" in result.columns
    assert len(result) == 2
    # Polk should score higher (better rent/cost, closer maturity)
    polk = result[result["county"] == "Polk"].iloc[0]
    sacto = result[result["county"] == "Sacramento"].iloc[0]
    assert polk["total_score"] > sacto["total_score"]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd scripts && uv run pytest tests/test_score.py -v`
Expected: FAIL

- [ ] **Step 3: Write score.py**

```python
# scripts/score.py
"""Compute acquisition signal scores for multifamily properties."""
import pandas as pd
from pathlib import Path
from zillow import add_zillow_urls


def normalize_signal(values: pd.Series, weight: float, higher_is_better: bool = True) -> pd.Series:
    """Normalize a signal to [0, weight] range using min-max scaling."""
    vmin = values.min()
    vmax = values.max()

    if vmax == vmin:
        return pd.Series([weight / 2] * len(values), index=values.index)

    if higher_is_better:
        normalized = (values - vmin) / (vmax - vmin)
    else:
        normalized = (vmax - values) / (vmax - vmin)

    return (normalized * weight).round(2)


def score_properties(
    census_latest: pd.DataFrame,
    census_earliest: pd.DataFrame,
    hud_fha: pd.DataFrame,
    weights: dict,
    usps_vacancy: pd.DataFrame = None,
) -> pd.DataFrame:
    """Score HUD FHA properties by acquisition signal strength.

    Args:
        census_latest: Census data for most recent year.
        census_earliest: Census data for earliest year (for trend calc).
        hud_fha: HUD FHA multifamily property data.
        weights: Dict of signal_name → weight (must sum to 100).
        usps_vacancy: Optional USPS vacancy data.

    Returns:
        Scored DataFrame with total_score, signal_rank, and zillow_url.
    """
    # Compute vacancy trend (5yr change)
    trend = census_latest[["fips", "vacancy_rate", "pop"]].merge(
        census_earliest[["fips", "vacancy_rate", "pop"]].rename(
            columns={"vacancy_rate": "vac_prev", "pop": "pop_prev"}
        ),
        on="fips",
        how="left",
    )
    trend["vac_trend_5yr_chg"] = trend["vacancy_rate"] - trend["vac_prev"]
    trend["pop_growth"] = ((trend["pop"] - trend["pop_prev"]) / trend["pop_prev"]).round(4)

    # Merge HUD properties with census county data
    df = hud_fha.merge(
        census_latest[["fips", "county", "state", "vacancy_rate", "rent_to_cost_ratio", "mf_pct"]],
        on="fips",
        how="left",
        suffixes=("", "_census"),
    )
    df = df.merge(
        trend[["fips", "vac_trend_5yr_chg", "pop_growth"]],
        on="fips",
        how="left",
    )

    # Use county name/state from census if not in HUD
    if "county" not in hud_fha.columns:
        pass  # Already merged from census

    # Score each signal
    # Mortgage maturity: closer = better (lower is better)
    if "maturity_years" in df.columns:
        # Cap at 5 years
        maturity_capped = df["maturity_years"].clip(0, 5)
        df["score_maturity"] = normalize_signal(maturity_capped, weights["mortgage_maturity"], higher_is_better=False)
    else:
        df["score_maturity"] = 0

    # Vacancy trend: more negative = tightening = better
    df["score_vacancy"] = normalize_signal(
        df["vac_trend_5yr_chg"].fillna(0),
        weights["vacancy_trend"],
        higher_is_better=False,
    )

    # Rent/cost ratio: higher = better
    df["score_rent_cost"] = normalize_signal(
        df["rent_to_cost_ratio"].fillna(0),
        weights["rent_cost_ratio"],
        higher_is_better=True,
    )

    # Area vacancy: moderate is best, use USPS if available, else census
    if usps_vacancy is not None and "usps_vacancy_rate" in usps_vacancy.columns:
        df = df.merge(usps_vacancy[["fips", "usps_vacancy_rate"]], on="fips", how="left")
        vac_col = "usps_vacancy_rate"
    else:
        vac_col = "vacancy_rate"
    # Moderate vacancy (~5-8%) scores highest; very low or very high scores low
    optimal_vacancy = 0.065
    df["vac_distance"] = abs(df[vac_col].fillna(optimal_vacancy) - optimal_vacancy)
    df["score_area_vac"] = normalize_signal(df["vac_distance"], weights["area_vacancy"], higher_is_better=False)

    # Population growth: higher = better
    df["score_pop"] = normalize_signal(
        df["pop_growth"].fillna(0),
        weights["pop_growth"],
        higher_is_better=True,
    )

    # Total score
    df["total_score"] = (
        df["score_maturity"] + df["score_vacancy"] + df["score_rent_cost"] +
        df["score_area_vac"] + df["score_pop"]
    ).round(1)

    # Rank
    df["signal_rank"] = df["total_score"].rank(ascending=False, method="min").astype(int)

    # Add Zillow URLs
    df["zillow_url"] = add_zillow_urls(df)

    # Sort by rank
    df = df.sort_values("signal_rank").reset_index(drop=True)

    return df


def run_scoring(data_dir: str, config: dict, output_path: str = None) -> pd.DataFrame:
    """Load data from parquet files and run scoring."""
    years = sorted(config["census"]["years"])
    latest_year = years[-1]
    earliest_year = years[0]

    census_latest = pd.read_parquet(Path(data_dir) / "census" / f"acs_{latest_year}.parquet")
    census_earliest = pd.read_parquet(Path(data_dir) / "census" / f"acs_{earliest_year}.parquet")
    hud_fha = pd.read_parquet(Path(data_dir) / "hud" / "fha_multifamily.parquet")

    usps_path = Path(data_dir) / "hud" / "usps_vacancy.parquet"
    usps = pd.read_parquet(usps_path) if usps_path.exists() else None

    # Filter to target markets
    target_fips = config.get("target_markets", [])
    if target_fips:
        hud_fha = hud_fha[hud_fha["fips"].isin(target_fips)]

    df = score_properties(census_latest, census_earliest, hud_fha, config["scoring_weights"], usps)

    if output_path:
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(output_path, index=False)
        print(f"  Wrote {len(df)} scored properties → {output_path}")

    return df
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd scripts && uv run pytest tests/test_score.py -v`
Expected: 3 passed

- [ ] **Step 5: Commit**

```bash
git add scripts/score.py scripts/tests/test_score.py
git commit -m "feat: add acquisition signal scoring with configurable weights"
```

---

## Task 8: Firebase Upload — upload.py

**Files:**
- Create: `scripts/upload.py`
- Create: `scripts/tests/test_upload.py`

- [ ] **Step 1: Write failing test**

```python
# scripts/tests/test_upload.py
import pytest
from unittest.mock import patch, MagicMock, call
from pathlib import Path
from upload import upload_to_storage, collect_parquet_files

def test_collect_parquet_files(tmp_path):
    (tmp_path / "census").mkdir()
    (tmp_path / "census" / "acs_2023.parquet").write_bytes(b"fake")
    (tmp_path / "hud").mkdir()
    (tmp_path / "hud" / "fha_multifamily.parquet").write_bytes(b"fake")
    (tmp_path / "scored").mkdir()
    (tmp_path / "scored" / "properties.parquet").write_bytes(b"fake")

    files = collect_parquet_files(str(tmp_path))
    assert len(files) == 3
    assert any("census/acs_2023.parquet" in f[1] for f in files)

@patch("upload.storage")
@patch("upload.firebase_admin")
def test_upload_to_storage(mock_admin, mock_storage, tmp_path):
    (tmp_path / "test.parquet").write_bytes(b"fake parquet data")

    mock_bucket = MagicMock()
    mock_storage.bucket.return_value = mock_bucket
    mock_blob = MagicMock()
    mock_bucket.blob.return_value = mock_blob

    upload_to_storage([(str(tmp_path / "test.parquet"), "data/test.parquet")])

    mock_bucket.blob.assert_called_once_with("data/test.parquet")
    mock_blob.upload_from_filename.assert_called_once()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd scripts && uv run pytest tests/test_upload.py -v`
Expected: FAIL

- [ ] **Step 3: Write upload.py**

```python
# scripts/upload.py
"""Upload Parquet files to Firebase Storage."""
import json
import firebase_admin
from firebase_admin import storage
from pathlib import Path
from datetime import datetime, timezone


def collect_parquet_files(data_dir: str) -> list[tuple[str, str]]:
    """Collect all parquet files and map to Storage paths.

    Returns list of (local_path, storage_path) tuples.
    """
    data_path = Path(data_dir)
    files = []

    for pq_file in data_path.rglob("*.parquet"):
        relative = pq_file.relative_to(data_path)
        storage_path = f"data/{relative}"
        files.append((str(pq_file), storage_path))

    return sorted(files, key=lambda x: x[1])


def upload_to_storage(file_pairs: list[tuple[str, str]], bucket_name: str = None):
    """Upload files to Firebase Storage.

    Args:
        file_pairs: List of (local_path, storage_path) tuples.
        bucket_name: Firebase Storage bucket name. Uses default if None.
    """
    if not firebase_admin._apps:
        firebase_admin.initialize_app()

    bucket = storage.bucket(bucket_name)

    for local_path, storage_path in file_pairs:
        blob = bucket.blob(storage_path)
        blob.upload_from_filename(local_path, content_type="application/octet-stream")
        print(f"  Uploaded {storage_path} ({Path(local_path).stat().st_size:,} bytes)")


def upload_meta(data_dir: str, config: dict, bucket_name: str = None):
    """Upload run metadata to Firebase Storage."""
    if not firebase_admin._apps:
        firebase_admin.initialize_app()

    bucket = storage.bucket(bucket_name)
    blob = bucket.blob("data/meta/last_run.json")

    meta = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "config": config,
        "files": [f[1] for f in collect_parquet_files(data_dir)],
    }

    blob.upload_from_string(json.dumps(meta, indent=2, default=str), content_type="application/json")
    print(f"  Uploaded data/meta/last_run.json")


def upload_all(data_dir: str, config: dict, bucket_name: str = None):
    """Upload all parquet files and metadata."""
    print("Uploading to Firebase Storage...")
    file_pairs = collect_parquet_files(data_dir)
    upload_to_storage(file_pairs, bucket_name)
    upload_meta(data_dir, config, bucket_name)
    print(f"  Done — {len(file_pairs)} files uploaded.")
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd scripts && uv run pytest tests/test_upload.py -v`
Expected: 2 passed

- [ ] **Step 5: Commit**

```bash
git add scripts/upload.py scripts/tests/test_upload.py
git commit -m "feat: add Firebase Storage upload for parquet files"
```

---

## Task 9: CLI Entry Point — run.py

**Files:**
- Create: `scripts/run.py`

- [ ] **Step 1: Write run.py**

```python
#!/usr/bin/env python3
# scripts/run.py
"""CLI entry point for the fund data pipeline."""
import os
import click
from pathlib import Path

from config_loader import load_config
from pull_census import pull_census
from pull_bls import pull_bls
from pull_hud import pull_hud
from score import run_scoring
from upload import upload_all

DEFAULT_OUTPUT = str(Path(__file__).parent.parent / "data")


@click.command()
@click.option("--all", "run_all", is_flag=True, help="Run entire pipeline")
@click.option("--census", is_flag=True, help="Pull Census ACS data")
@click.option("--bls", is_flag=True, help="Pull BLS OEWS data")
@click.option("--hud", is_flag=True, help="Pull HUD FHA + USPS data")
@click.option("--score", "run_score", is_flag=True, help="Run scoring")
@click.option("--upload", "run_upload", is_flag=True, help="Upload to Firebase Storage")
@click.option("--config", "config_source", default=None, help="Config file path or 'firestore'")
@click.option("--local-only", is_flag=True, help="Skip upload, output locally only")
@click.option("--output", default=DEFAULT_OUTPUT, help="Local output directory")
def main(run_all, census, bls, hud, run_score, run_upload, config_source, local_only, output):
    """Fund data pipeline — pull, score, and upload multifamily property data."""
    config = load_config(config_source)
    os.makedirs(output, exist_ok=True)

    if run_all or census:
        census_dir = str(Path(output) / "census")
        os.makedirs(census_dir, exist_ok=True)
        pull_census(
            years=config["census"]["years"],
            output_dir=census_dir,
            api_key=os.environ.get("CENSUS_API_KEY"),
        )

    if run_all or bls:
        bls_dir = str(Path(output) / "bls")
        os.makedirs(bls_dir, exist_ok=True)
        bls_years = config.get("bls", {}).get("years", config["census"]["years"])
        pull_bls(years=bls_years, output_dir=bls_dir)

    if run_all or hud:
        hud_dir = str(Path(output) / "hud")
        os.makedirs(hud_dir, exist_ok=True)
        pull_hud(output_dir=hud_dir)

    if run_all or run_score:
        scored_dir = str(Path(output) / "scored")
        os.makedirs(scored_dir, exist_ok=True)
        run_scoring(
            data_dir=output,
            config=config,
            output_path=str(Path(scored_dir) / "properties.parquet"),
        )

    if (run_all or run_upload) and not local_only:
        upload_all(data_dir=output, config=config)

    if local_only:
        print(f"\nLocal-only mode — output at {output}/")

    print("\nPipeline complete.")


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Verify CLI help works**

Run: `cd scripts && uv run python run.py --help`
Expected: Shows help with all options listed

- [ ] **Step 3: Commit**

```bash
git add scripts/run.py
git commit -m "feat: add CLI entry point for pipeline with click"
```

---

## Task 10: Firebase Rules — firestore.rules + storage.rules

**Files:**
- Create: `firestore.rules`
- Create: `storage.rules`
- Modify: `firebase.json`

- [ ] **Step 1: Write firestore.rules**

```
rules_version = '2';

service cloud.firestore {
  match /databases/{database}/documents {
    function isAllowedUser() {
      return request.auth != null &&
             request.auth.token.email in [
               'holly@multiversal.ventures',
               'akshay@multiversal.ventures',
               'kartik@multiversal.ventures'
             ];
    }

    match /config/pipeline {
      allow read, write: if isAllowedUser();
    }

    match /config/runs/{run} {
      allow read: if isAllowedUser();
    }
  }
}
```

- [ ] **Step 2: Write storage.rules**

```
rules_version = '2';

service firebase.storage {
  match /b/{bucket}/o {
    match /data/{allPaths=**} {
      allow read: if request.auth != null &&
                   request.auth.token.email in [
                     'holly@multiversal.ventures',
                     'akshay@multiversal.ventures',
                     'kartik@multiversal.ventures'
                   ];
      allow write: if false;
    }
  }
}
```

- [ ] **Step 3: Update firebase.json**

```json
{
  "hosting": {
    "public": "public",
    "ignore": [
      "firebase.json",
      "**/.*",
      "**/node_modules/**"
    ],
    "rewrites": [
      {
        "source": "**",
        "destination": "/index.html"
      }
    ]
  },
  "firestore": {
    "rules": "firestore.rules"
  },
  "storage": {
    "rules": "storage.rules"
  },
  "functions": [
    {
      "source": "functions",
      "codebase": "default",
      "runtime": "nodejs20"
    }
  ]
}
```

- [ ] **Step 4: Deploy rules**

Run: `firebase deploy --only firestore:rules,storage`
Expected: Rules deployed successfully

- [ ] **Step 5: Commit**

```bash
git add firestore.rules storage.rules firebase.json
git commit -m "feat: add Firebase security rules for Storage and Firestore"
```

---

## Task 11: Cloud Function — functions/index.js

**Files:**
- Create: `functions/package.json`
- Create: `functions/index.js`

- [ ] **Step 1: Initialize functions directory**

```json
// functions/package.json
{
  "name": "fund-functions",
  "description": "Cloud Function to trigger pipeline refresh",
  "engines": { "node": "20" },
  "main": "index.js",
  "dependencies": {
    "firebase-admin": "^12.0.0",
    "firebase-functions": "^6.0.0"
  }
}
```

- [ ] **Step 2: Write index.js**

```javascript
// functions/index.js
const { onDocumentWritten } = require("firebase-functions/v2/firestore");
const { initializeApp } = require("firebase-admin/app");
const { getFirestore } = require("firebase-admin/firestore");

initializeApp();

exports.triggerPipelineRefresh = onDocumentWritten(
  "config/pipeline",
  async (event) => {
    const db = getFirestore();
    const config = event.data.after.data();
    const runId = Date.now().toString();

    // Record the run
    await db.collection("config").doc("runs").collection("history").doc(runId).set({
      status: "triggered",
      triggeredAt: new Date().toISOString(),
      config: config,
    });

    // Trigger GitHub Actions workflow via repository dispatch
    const ghToken = process.env.GITHUB_TOKEN;
    if (!ghToken) {
      console.error("GITHUB_TOKEN not set — cannot trigger workflow");
      return;
    }

    const response = await fetch(
      "https://api.github.com/repos/multiversal-ventures/fund/actions/workflows/refresh.yml/dispatches",
      {
        method: "POST",
        headers: {
          Authorization: `Bearer ${ghToken}`,
          Accept: "application/vnd.github.v3+json",
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          ref: "main",
          inputs: {
            config_source: "firestore",
            run_id: runId,
          },
        }),
      }
    );

    if (response.ok) {
      console.log(`Triggered workflow for run ${runId}`);
    } else {
      const body = await response.text();
      console.error(`GitHub API error: ${response.status} ${body}`);
      await db.collection("config").doc("runs").collection("history").doc(runId).update({
        status: "trigger_failed",
        error: body,
      });
    }
  }
);
```

- [ ] **Step 3: Install dependencies**

Run: `cd functions && npm install`

- [ ] **Step 4: Commit**

```bash
git add functions/
git commit -m "feat: add Cloud Function to trigger pipeline via GitHub Actions"
```

---

## Task 12: GitHub Actions Workflows

**Files:**
- Create: `.github/workflows/refresh.yml`
- Create: `.github/workflows/deploy.yml`

- [ ] **Step 1: Write refresh.yml**

```yaml
# .github/workflows/refresh.yml
name: Data Pipeline Refresh

on:
  schedule:
    - cron: '0 6 1 * *'  # 1st of each month at 6am UTC
  workflow_dispatch:
    inputs:
      config_source:
        description: 'Config source: firestore or path to yaml'
        required: false
        default: 'firestore'
      run_id:
        description: 'Firestore run ID for status tracking'
        required: false

jobs:
  refresh:
    runs-on: ubuntu-latest
    env:
      CENSUS_API_KEY: ${{ secrets.CENSUS_API_KEY }}
      GOOGLE_APPLICATION_CREDENTIALS: /tmp/sa-key.json

    steps:
      - uses: actions/checkout@v4

      - name: Install uv
        uses: astral-sh/setup-uv@v4

      - name: Setup Python
        run: uv python install 3.12

      - name: Install dependencies
        run: cd scripts && uv sync

      - name: Write service account key
        run: echo '${{ secrets.FIREBASE_SERVICE_ACCOUNT }}' > /tmp/sa-key.json

      - name: Run pipeline
        run: |
          cd scripts
          uv run python run.py --all \
            --config ${{ inputs.config_source || 'firestore' }} \
            --output ../data

      - name: Cleanup
        if: always()
        run: rm -f /tmp/sa-key.json
```

- [ ] **Step 2: Write deploy.yml**

```yaml
# .github/workflows/deploy.yml
name: Deploy to Firebase

on:
  push:
    branches: [main]
    paths:
      - 'public/**'
      - 'firebase.json'
      - 'firestore.rules'
      - 'storage.rules'
      - 'functions/**'

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Install Firebase CLI
        run: npm install -g firebase-tools

      - name: Install function dependencies
        run: cd functions && npm ci

      - name: Deploy
        run: firebase deploy --token "${{ secrets.FIREBASE_TOKEN }}"
```

- [ ] **Step 3: Commit**

```bash
git add .github/workflows/
git commit -m "feat: add GitHub Actions for monthly data refresh and Firebase deploy"
```

---

## Task 13: DuckDB WASM Explorer — explorer.html

**Files:**
- Create: `public/explorer.html`
- Modify: `public/index.html` (add link to explorer)

- [ ] **Step 1: Write explorer.html**

This is the largest file. It contains:
1. Firebase auth gate (same as index.html)
2. Config panel (collapsible) — writes to Firestore
3. DuckDB WASM initialization — loads parquet from Firebase Storage with auth token
4. SQL query bar with preset buttons
5. Results table with sortable columns and Zillow links
6. Quick charts (score distribution, top states)
7. Export (CSV/Parquet)
8. Run status indicator

The file is a single self-contained HTML page with inline CSS and JS. Key dependencies loaded from CDN:
- `@duckdb/duckdb-wasm` — DuckDB WASM bundle
- `firebase/12.11.0` — Firebase SDK (auth, firestore, storage)

Full implementation in the step below. The file follows the same dark theme as index.html.

```html
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Fund Explorer — Multiversal Ventures</title>
<script src="/__/firebase/12.11.0/firebase-app-compat.js"></script>
<script src="/__/firebase/12.11.0/firebase-auth-compat.js"></script>
<script src="/__/firebase/12.11.0/firebase-firestore-compat.js"></script>
<script src="/__/firebase/12.11.0/firebase-storage-compat.js"></script>
<script src="/__/firebase/init.js"></script>
<script src="https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.29.0/dist/duckdb-browser-blocking.js"></script>
<style>
  * { margin:0; padding:0; box-sizing:border-box; }
  body { font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,monospace; background:#0f172a; color:#e2e8f0; min-height:100vh; }

  /* Auth gate */
  #auth-gate { display:flex; align-items:center; justify-content:center; min-height:100vh; flex-direction:column; }
  #auth-gate h1 { font-size:28px; background:linear-gradient(90deg,#34d399,#60a5fa,#a78bfa);-webkit-background-clip:text;-webkit-text-fill-color:transparent; margin-bottom:24px; }

  /* Layout */
  .header { background:linear-gradient(135deg,#0f172a,#1e293b,#0f172a); border-bottom:1px solid #334155; padding:20px 24px; display:flex; justify-content:space-between; align-items:center; }
  .header h1 { font-size:20px; color:#f1f5f9; }
  .header .user { color:#94a3b8; font-size:12px; }
  .container { max-width:1400px; margin:0 auto; padding:20px; }

  /* Config panel */
  .config-panel { background:#1e293b; border:1px solid #334155; border-radius:10px; padding:20px; margin-bottom:16px; }
  .config-toggle { cursor:pointer; color:#60a5fa; font-size:13px; font-weight:600; }
  .config-body { display:none; margin-top:16px; }
  .config-body.open { display:grid; grid-template-columns:1fr 1fr; gap:16px; }
  .config-section h3 { font-size:12px; color:#64748b; text-transform:uppercase; letter-spacing:1px; margin-bottom:8px; }
  .weight-row { display:flex; justify-content:space-between; align-items:center; padding:4px 0; font-size:13px; color:#94a3b8; }
  .weight-row input { width:50px; background:#0f172a; border:1px solid #334155; color:#e2e8f0; padding:4px 8px; border-radius:4px; text-align:center; }
  .btn { background:#60a5fa; color:#0f172a; border:none; padding:8px 16px; border-radius:6px; font-size:13px; font-weight:600; cursor:pointer; }
  .btn:hover { background:#93c5fd; }
  .btn-sm { padding:4px 12px; font-size:11px; }
  .btn-outline { background:transparent; color:#60a5fa; border:1px solid #334155; }
  .btn-outline:hover { border-color:#60a5fa; }
  .status { font-size:11px; color:#64748b; margin-top:8px; }

  /* Query bar */
  .query-bar { background:#1e293b; border:1px solid #334155; border-radius:10px; padding:16px; margin-bottom:16px; }
  .query-bar textarea { width:100%; background:#0f172a; border:1px solid #334155; color:#e2e8f0; padding:12px; border-radius:6px; font-family:monospace; font-size:13px; resize:vertical; min-height:60px; }
  .query-actions { display:flex; gap:8px; margin-top:8px; align-items:center; flex-wrap:wrap; }
  .preset-label { font-size:11px; color:#64748b; margin-left:8px; }

  /* Results */
  .results { background:#1e293b; border:1px solid #334155; border-radius:10px; overflow:hidden; margin-bottom:16px; }
  .results-header { padding:12px 16px; border-bottom:1px solid #334155; display:flex; justify-content:space-between; align-items:center; }
  .results-header span { font-size:13px; color:#94a3b8; }
  .results-table-wrap { overflow-x:auto; }
  table { width:100%; border-collapse:collapse; font-size:12px; }
  th { background:#0f172a; color:#64748b; text-transform:uppercase; letter-spacing:0.5px; font-size:10px; padding:8px 12px; text-align:left; position:sticky; top:0; cursor:pointer; white-space:nowrap; }
  th:hover { color:#60a5fa; }
  td { padding:8px 12px; border-top:1px solid rgba(255,255,255,0.04); white-space:nowrap; }
  tr:hover td { background:rgba(96,165,250,0.05); }
  .zillow-link { color:#60a5fa; text-decoration:none; font-weight:600; }
  .zillow-link:hover { text-decoration:underline; }
  .score-badge { display:inline-block; padding:2px 8px; border-radius:10px; font-weight:600; font-size:11px; }
  .score-high { background:rgba(52,211,153,0.15); color:#34d399; }
  .score-mid { background:rgba(251,191,36,0.15); color:#fbbf24; }
  .score-low { background:rgba(248,113,113,0.15); color:#f87171; }

  /* Charts */
  .charts { display:grid; grid-template-columns:1fr 1fr; gap:16px; margin-bottom:16px; }
  .chart-card { background:#1e293b; border:1px solid #334155; border-radius:10px; padding:16px; }
  .chart-card h3 { font-size:12px; color:#64748b; text-transform:uppercase; letter-spacing:0.5px; margin-bottom:12px; }
  .bar { display:flex; align-items:center; gap:8px; margin-bottom:4px; font-size:12px; }
  .bar-label { width:40px; text-align:right; color:#94a3b8; }
  .bar-fill { height:16px; border-radius:3px; background:linear-gradient(90deg,#60a5fa,#a78bfa); min-width:2px; }
  .bar-count { color:#64748b; font-size:11px; }

  /* Loading */
  .loading { text-align:center; padding:60px; color:#64748b; }
  .loading .spinner { display:inline-block; width:24px; height:24px; border:3px solid #334155; border-top-color:#60a5fa; border-radius:50%; animation:spin 1s linear infinite; }
  @keyframes spin { to { transform:rotate(360deg); } }

  /* Footer */
  .footer { text-align:center; padding:20px; color:#475569; font-size:11px; }
</style>
</head>
<body>

<!-- Auth gate -->
<div id="auth-gate">
  <h1>Multiversal Ventures — Fund Explorer</h1>
  <p style="color:#94a3b8; margin-bottom:24px; font-size:14px;">Sign in to access the property explorer.</p>
  <button class="btn" id="sign-in-btn" style="padding:12px 32px; font-size:15px; display:flex; align-items:center; gap:8px;">
    <svg width="18" height="18" viewBox="0 0 48 48"><path fill="#EA4335" d="M24 9.5c3.54 0 6.71 1.22 9.21 3.6l6.85-6.85C35.9 2.38 30.47 0 24 0 14.62 0 6.51 5.38 2.56 13.22l7.98 6.19C12.43 13.72 17.74 9.5 24 9.5z"/><path fill="#4285F4" d="M46.98 24.55c0-1.57-.15-3.09-.38-4.55H24v9.02h12.94c-.58 2.96-2.26 5.48-4.78 7.18l7.73 6c4.51-4.18 7.09-10.36 7.09-17.65z"/><path fill="#34A853" d="M10.53 28.59A14.5 14.5 0 0 1 9.5 24c0-1.59.28-3.14.76-4.59l-7.98-6.19A23.99 23.99 0 0 0 0 24c0 3.77.9 7.35 2.56 10.52l7.97-5.93z"/><path fill="#FBBC05" d="M24 48c6.48 0 11.93-2.13 15.89-5.81l-7.73-6c-2.15 1.45-4.92 2.3-8.16 2.3-6.26 0-11.57-4.22-13.47-9.91l-7.98 5.93C6.51 42.62 14.62 48 24 48z"/></svg>
    Sign in with Google
  </button>
  <p id="auth-error" style="color:#f87171; margin-top:16px; font-size:13px; display:none;"></p>
</div>

<!-- Main app (hidden until auth) -->
<div id="app" style="display:none;">
  <div class="header">
    <h1>Fund Explorer</h1>
    <div>
      <span class="user" id="user-email"></span>
      <button class="btn btn-sm btn-outline" onclick="firebase.auth().signOut()" style="margin-left:8px;">Sign Out</button>
    </div>
  </div>

  <div class="container">
    <!-- Config Panel -->
    <div class="config-panel">
      <span class="config-toggle" id="config-toggle">&#9662; Pipeline Configuration</span>
      <div class="config-body" id="config-body">
        <div class="config-section">
          <h3>Scoring Weights</h3>
          <div class="weight-row"><span>Mortgage Maturity</span><input type="number" id="w-maturity" value="20" min="0" max="100"></div>
          <div class="weight-row"><span>Vacancy Trend</span><input type="number" id="w-vacancy" value="25" min="0" max="100"></div>
          <div class="weight-row"><span>Rent/Cost Ratio</span><input type="number" id="w-rent" value="30" min="0" max="100"></div>
          <div class="weight-row"><span>Area Vacancy</span><input type="number" id="w-area" value="10" min="0" max="100"></div>
          <div class="weight-row"><span>Pop Growth</span><input type="number" id="w-pop" value="15" min="0" max="100"></div>
          <div class="weight-row" style="font-weight:600; color:#e2e8f0;"><span>Total</span><span id="w-total">100</span></div>
        </div>
        <div class="config-section">
          <h3>Actions</h3>
          <button class="btn" id="save-config-btn">Save & Refresh Pipeline</button>
          <div class="status" id="run-status">Loading run status...</div>
        </div>
      </div>
    </div>

    <!-- Query Bar -->
    <div class="query-bar">
      <textarea id="sql-input">SELECT * FROM properties ORDER BY total_score DESC LIMIT 50</textarea>
      <div class="query-actions">
        <button class="btn" id="run-query-btn">Run Query</button>
        <span class="preset-label">Presets:</span>
        <button class="btn btn-sm btn-outline preset" data-sql="SELECT * FROM properties ORDER BY total_score DESC LIMIT 50">Top 50</button>
        <button class="btn btn-sm btn-outline preset" data-sql="SELECT * FROM properties WHERE maturity_years < 2 ORDER BY maturity_years ASC">Maturing &lt;2yr</button>
        <button class="btn btn-sm btn-outline preset" data-sql="SELECT state, COUNT(*) as cnt, ROUND(AVG(total_score),1) as avg_score FROM properties GROUP BY state ORDER BY cnt DESC">By State</button>
        <button class="btn btn-sm btn-outline preset" data-sql="SELECT * FROM properties WHERE vacancy_rate > 0.08 ORDER BY total_score DESC LIMIT 50">High Vacancy</button>
      </div>
    </div>

    <!-- Loading state -->
    <div class="loading" id="loading">
      <div class="spinner"></div>
      <p style="margin-top:12px;">Loading DuckDB WASM + Parquet data...</p>
    </div>

    <!-- Results -->
    <div class="results" id="results" style="display:none;">
      <div class="results-header">
        <span id="result-count">0 rows</span>
        <div>
          <button class="btn btn-sm btn-outline" id="export-csv">Export CSV</button>
          <button class="btn btn-sm btn-outline" id="export-parquet" style="margin-left:4px;">Export Parquet</button>
        </div>
      </div>
      <div class="results-table-wrap">
        <table id="results-table"><thead></thead><tbody></tbody></table>
      </div>
    </div>

    <!-- Charts -->
    <div class="charts" id="charts" style="display:none;">
      <div class="chart-card">
        <h3>Score Distribution</h3>
        <div id="chart-scores"></div>
      </div>
      <div class="chart-card">
        <h3>Top States</h3>
        <div id="chart-states"></div>
      </div>
    </div>
  </div>

  <div class="footer">
    Multiversal Ventures — Fund Explorer<br>
    <a href="index.html" style="color:#60a5fa; text-decoration:none;">Back to Dashboard</a>
  </div>
</div>

<script>
// --- Auth ---
const ALLOWED = ['holly@multiversal.ventures','akshay@multiversal.ventures','kartik@multiversal.ventures'];
const authGate = document.getElementById('auth-gate');
const app = document.getElementById('app');
const authError = document.getElementById('auth-error');

firebase.auth().onAuthStateChanged(user => {
  if (user && ALLOWED.includes(user.email.toLowerCase())) {
    authGate.style.display = 'none';
    app.style.display = 'block';
    document.getElementById('user-email').textContent = user.email;
    initExplorer(user);
  } else if (user) {
    firebase.auth().signOut();
    authError.textContent = 'Access restricted to authorized team members.';
    authError.style.display = 'block';
  } else {
    authGate.style.display = 'flex';
    app.style.display = 'none';
  }
});

document.getElementById('sign-in-btn').addEventListener('click', () => {
  firebase.auth().signInWithPopup(new firebase.auth.GoogleAuthProvider()).catch(err => {
    authError.textContent = err.message;
    authError.style.display = 'block';
  });
});

// --- Config Panel ---
document.getElementById('config-toggle').addEventListener('click', () => {
  document.getElementById('config-body').classList.toggle('open');
});

const weightInputs = ['w-maturity','w-vacancy','w-rent','w-area','w-pop'];
weightInputs.forEach(id => {
  document.getElementById(id).addEventListener('input', () => {
    const total = weightInputs.reduce((s, i) => s + parseInt(document.getElementById(i).value || 0), 0);
    document.getElementById('w-total').textContent = total;
    document.getElementById('w-total').style.color = total === 100 ? '#34d399' : '#f87171';
  });
});

document.getElementById('save-config-btn').addEventListener('click', async () => {
  const weights = {
    mortgage_maturity: parseInt(document.getElementById('w-maturity').value),
    vacancy_trend: parseInt(document.getElementById('w-vacancy').value),
    rent_cost_ratio: parseInt(document.getElementById('w-rent').value),
    area_vacancy: parseInt(document.getElementById('w-area').value),
    pop_growth: parseInt(document.getElementById('w-pop').value),
  };
  const total = Object.values(weights).reduce((a,b) => a+b, 0);
  if (total !== 100) { alert('Weights must sum to 100'); return; }

  await firebase.firestore().doc('config/pipeline').set({ scoring_weights: weights }, { merge: true });
  document.getElementById('run-status').textContent = 'Pipeline refresh triggered...';
});

// --- DuckDB WASM + Data Loading ---
let db = null;
let conn = null;

async function initExplorer(user) {
  try {
    // Init DuckDB WASM
    const DUCKDB_BUNDLES = duckdb.getJsDelivrBundles();
    const bundle = await duckdb.selectBundle(DUCKDB_BUNDLES);
    const worker = new Worker(bundle.mainWorker);
    const logger = new duckdb.ConsoleLogger();
    db = new duckdb.AsyncDuckDB(logger, worker);
    await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
    conn = await db.connect();

    // Fetch parquet files from Firebase Storage with auth token
    const token = await user.getIdToken();
    const storageRef = firebase.storage().ref();

    const files = [
      { path: 'data/scored/properties.parquet', table: 'properties' },
      { path: 'data/census/acs_2023.parquet', table: 'census_2023' },
      { path: 'data/census/acs_2022.parquet', table: 'census_2022' },
      { path: 'data/census/acs_2021.parquet', table: 'census_2021' },
      { path: 'data/hud/fha_multifamily.parquet', table: 'hud_fha' },
      { path: 'data/hud/usps_vacancy.parquet', table: 'usps_vacancy' },
    ];

    for (const f of files) {
      try {
        const url = await storageRef.child(f.path).getDownloadURL();
        const resp = await fetch(url);
        const buffer = await resp.arrayBuffer();
        await db.registerFileBuffer(f.table + '.parquet', new Uint8Array(buffer));
        await conn.query(`CREATE TABLE ${f.table} AS SELECT * FROM '${f.table}.parquet'`);
      } catch (e) {
        console.warn(`Skipping ${f.path}: ${e.message}`);
      }
    }

    document.getElementById('loading').style.display = 'none';
    document.getElementById('results').style.display = 'block';
    document.getElementById('charts').style.display = 'grid';

    // Run default query
    await runQuery();

    // Load run status
    loadRunStatus();
  } catch (err) {
    document.getElementById('loading').innerHTML = `<p style="color:#f87171;">Error: ${err.message}</p>`;
    console.error(err);
  }
}

// --- Query Execution ---
async function runQuery() {
  const sql = document.getElementById('sql-input').value.trim();
  if (!sql || !conn) return;

  try {
    const result = await conn.query(sql);
    renderResults(result);
    renderCharts(result);
  } catch (err) {
    document.getElementById('results-table').innerHTML = `<tr><td style="color:#f87171; padding:16px;">${err.message}</td></tr>`;
  }
}

document.getElementById('run-query-btn').addEventListener('click', runQuery);
document.getElementById('sql-input').addEventListener('keydown', e => {
  if ((e.metaKey || e.ctrlKey) && e.key === 'Enter') runQuery();
});

document.querySelectorAll('.preset').forEach(btn => {
  btn.addEventListener('click', () => {
    document.getElementById('sql-input').value = btn.dataset.sql;
    runQuery();
  });
});

// --- Render Results ---
let lastResult = null;

function renderResults(result) {
  lastResult = result;
  const schema = result.schema.fields.map(f => f.name);
  const rows = result.toArray().map(r => {
    const obj = {};
    schema.forEach(col => { obj[col] = r[col]; });
    return obj;
  });

  document.getElementById('result-count').textContent = `${rows.length} rows`;

  const thead = document.querySelector('#results-table thead');
  const tbody = document.querySelector('#results-table tbody');

  thead.innerHTML = '<tr>' + schema.map(col =>
    `<th>${col}</th>`
  ).join('') + '</tr>';

  tbody.innerHTML = rows.map(row => '<tr>' + schema.map(col => {
    let val = row[col];
    if (col === 'zillow_url' && val) {
      return `<td><a class="zillow-link" href="${val}" target="_blank" rel="noopener">View on Zillow</a></td>`;
    }
    if (col === 'total_score' && typeof val === 'number') {
      const cls = val >= 70 ? 'score-high' : val >= 50 ? 'score-mid' : 'score-low';
      return `<td><span class="score-badge ${cls}">${val.toFixed(1)}</span></td>`;
    }
    if (typeof val === 'number') {
      val = Number.isInteger(val) ? val.toLocaleString() : val.toFixed(2);
    }
    return `<td>${val ?? ''}</td>`;
  }).join('') + '</tr>').join('');
}

// --- Charts ---
function renderCharts(result) {
  const schema = result.schema.fields.map(f => f.name);
  const rows = result.toArray().map(r => {
    const obj = {};
    schema.forEach(col => { obj[col] = r[col]; });
    return obj;
  });

  // Score distribution
  if (schema.includes('total_score')) {
    const buckets = {'90-100':0,'80-90':0,'70-80':0,'60-70':0,'50-60':0,'<50':0};
    rows.forEach(r => {
      const s = r.total_score || 0;
      if (s >= 90) buckets['90-100']++;
      else if (s >= 80) buckets['80-90']++;
      else if (s >= 70) buckets['70-80']++;
      else if (s >= 60) buckets['60-70']++;
      else if (s >= 50) buckets['50-60']++;
      else buckets['<50']++;
    });
    const max = Math.max(...Object.values(buckets), 1);
    document.getElementById('chart-scores').innerHTML = Object.entries(buckets).map(([k,v]) =>
      `<div class="bar"><span class="bar-label">${k}</span><div class="bar-fill" style="width:${(v/max)*200}px"></div><span class="bar-count">${v}</span></div>`
    ).join('');
  }

  // Top states
  if (schema.includes('state')) {
    const states = {};
    rows.forEach(r => { states[r.state] = (states[r.state] || 0) + 1; });
    const sorted = Object.entries(states).sort((a,b) => b[1]-a[1]).slice(0, 10);
    const max = sorted[0]?.[1] || 1;
    document.getElementById('chart-states').innerHTML = sorted.map(([st,cnt]) =>
      `<div class="bar"><span class="bar-label">${st}</span><div class="bar-fill" style="width:${(cnt/max)*200}px"></div><span class="bar-count">${cnt}</span></div>`
    ).join('');
  }
}

// --- Export ---
document.getElementById('export-csv').addEventListener('click', () => {
  if (!lastResult) return;
  const schema = lastResult.schema.fields.map(f => f.name);
  const rows = lastResult.toArray();
  let csv = schema.join(',') + '\n';
  rows.forEach(r => {
    csv += schema.map(col => {
      let v = r[col];
      if (typeof v === 'string' && v.includes(',')) v = `"${v}"`;
      return v ?? '';
    }).join(',') + '\n';
  });
  downloadBlob(csv, 'fund-export.csv', 'text/csv');
});

document.getElementById('export-parquet').addEventListener('click', async () => {
  if (!conn) return;
  const sql = document.getElementById('sql-input').value.trim();
  await conn.query(`COPY (${sql}) TO 'export.parquet' (FORMAT PARQUET)`);
  const buf = await db.copyFileToBuffer('export.parquet');
  downloadBlob(buf, 'fund-export.parquet', 'application/octet-stream');
});

function downloadBlob(data, filename, type) {
  const blob = new Blob([data], { type });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url; a.download = filename; a.click();
  URL.revokeObjectURL(url);
}

// --- Run Status ---
async function loadRunStatus() {
  try {
    const ref = firebase.storage().ref('data/meta/last_run.json');
    const url = await ref.getDownloadURL();
    const resp = await fetch(url);
    const meta = await resp.json();
    const dt = new Date(meta.timestamp).toLocaleDateString();
    const fileCount = meta.files?.length || 0;
    document.getElementById('run-status').textContent = `Last run: ${dt} · ${fileCount} files`;
  } catch (e) {
    document.getElementById('run-status').textContent = 'No pipeline runs yet.';
  }
}
</script>
</body>
</html>
```

- [ ] **Step 2: Add explorer link to index.html**

In `public/index.html`, add a link in the header section after the date line:

```html
<div class="date">Research compiled March 25, 2026 · 6 reports · 528 US counties screened · Census ACS 2018–2023</div>
<div style="margin-top:12px;"><a href="explorer.html" style="color:#60a5fa; font-weight:600; text-decoration:none; font-size:14px;">Open Fund Explorer →</a></div>
```

- [ ] **Step 3: Commit**

```bash
git add public/explorer.html public/index.html
git commit -m "feat: add DuckDB WASM explorer with auth, config, SQL queries, Zillow links"
```

---

## Task 14: Add tests/__init__.py and conftest

**Files:**
- Create: `scripts/tests/__init__.py`
- Create: `scripts/tests/conftest.py`

- [ ] **Step 1: Create test infrastructure**

```python
# scripts/tests/__init__.py
```

```python
# scripts/tests/conftest.py
import sys
from pathlib import Path

# Add scripts dir to path so imports work
sys.path.insert(0, str(Path(__file__).parent.parent))
```

- [ ] **Step 2: Run all tests**

Run: `cd scripts && uv run pytest tests/ -v`
Expected: All tests pass

- [ ] **Step 3: Commit**

```bash
git add scripts/tests/__init__.py scripts/tests/conftest.py
git commit -m "feat: add test infrastructure with path setup"
```

---

## Task 15: Integration Test — Full Local Pipeline

**Files:**
- Create: `scripts/tests/test_integration.py`

- [ ] **Step 1: Write integration test**

```python
# scripts/tests/test_integration.py
"""Integration test: run pipeline locally with mocked API responses."""
import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock
from click.testing import CliRunner
from run import main

@pytest.fixture
def mock_apis():
    """Mock all external API calls."""
    census_response = [
        ["NAME", "B25001_001E", "B25002_002E", "B25002_003E",
         "B25003_002E", "B25003_003E", "B25004_002E",
         "B25024_007E", "B25024_008E", "B25024_009E", "B25024_010E", "B25024_011E",
         "B25064_001E", "B25077_001E", "B25105_001E", "B01003_001E",
         "state", "county"],
        ["Sacramento County, California", "600000", "560000", "40000",
         "300000", "260000", "8000",
         "10000", "15000", "20000", "5000", "3000",
         "1600", "400000", "1700", "1500000",
         "06", "067"],
    ]

    hud_response = [
        {
            "property_name": "Test Apts",
            "property_street": "123 Test St",
            "city_name_text": "Sacramento",
            "state_code": "CA",
            "zip_code": "95814",
            "units_tot_cnt": "100",
            "fha_loan_id": "99999",
            "orig_mortgage_amt": "5000000",
            "maturity_date": "2028-01-01T00:00:00.000",
            "soa_cd_txt": "Section 8",
            "latitude": "38.58",
            "longitude": "-121.49",
            "fips_state_cd": "06",
            "fips_cnty_cd": "067",
        }
    ]

    usps_response = [
        {"geoid": "06067", "year": "2024", "quarter": "4", "tot_res": "600000", "res_vac": "30000"}
    ]

    with patch("pull_census.requests.get") as mock_census, \
         patch("pull_hud.requests.get") as mock_hud:

        def side_effect(url, **kwargs):
            resp = MagicMock()
            resp.raise_for_status = MagicMock()
            if "census" in url:
                resp.json.return_value = census_response
            elif "fyha" in url:
                resp.json.return_value = hud_response
            elif "bik6" in url:
                resp.json.return_value = usps_response
            else:
                resp.json.return_value = []
            return resp

        mock_census.side_effect = side_effect
        mock_hud.side_effect = side_effect
        yield

def test_full_local_pipeline(tmp_path, mock_apis):
    """Run full pipeline locally without uploads."""
    runner = CliRunner()
    result = runner.invoke(main, [
        "--census", "--hud", "--score",
        "--local-only",
        "--output", str(tmp_path),
    ])

    assert result.exit_code == 0, result.output

    # Check census parquet was created
    census_dir = tmp_path / "census"
    assert any(census_dir.glob("*.parquet"))

    # Check scored properties were created
    scored = tmp_path / "scored" / "properties.parquet"
    assert scored.exists()
```

- [ ] **Step 2: Run integration test**

Run: `cd scripts && uv run pytest tests/test_integration.py -v`
Expected: PASS

- [ ] **Step 3: Commit**

```bash
git add scripts/tests/test_integration.py
git commit -m "test: add integration test for full local pipeline"
```

---

## Task 16: Final Push and Deploy

- [ ] **Step 1: Push all commits**

Run: `git push`

- [ ] **Step 2: Deploy Firebase Hosting + Rules**

Run: `firebase deploy --only hosting,firestore:rules,storage`

- [ ] **Step 3: Test locally**

Run: `cd scripts && uv run python run.py --census --local-only --output ../data`
Verify parquet files appear in `data/census/`

- [ ] **Step 4: Verify explorer.html loads**

Open `https://mvv-fund.web.app/explorer.html`, sign in, confirm DuckDB WASM initializes (will show "No pipeline runs yet" until first data upload).
