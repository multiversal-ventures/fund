# scripts/pull_permits.py
"""Pull Census Building Permits Survey (BPS) data and write per-year Parquet files."""
import os
import requests
import pandas as pd
from pathlib import Path

from pull_census import STATE_FIPS_TO_ABBR

BPS_BASE = "https://api.census.gov/data/timeseries/bps"


def build_permits_url(year: int, api_key: str = None) -> str:
    """Build the Census BPS API URL for county-level annual permit data."""
    url = (
        f"{BPS_BASE}?get=BLDGS,UNITS,BLDGS_5PLUS,UNITS_5PLUS"
        f"&for=county:*&in=state:*&time={year}"
    )
    if api_key:
        url += f"&key={api_key}"
    return url


def parse_permits_response(raw: list[list], year: int) -> pd.DataFrame:
    """Parse the JSON response from Census BPS API into a clean DataFrame.

    Expected columns in output:
        fips, state, year, total_permits, total_units_permitted,
        mf_permits, mf_units_permitted, sf_permits, mf_pct
    """
    header = raw[0]
    data = raw[1:]
    df = pd.DataFrame(data, columns=header)

    df["fips"] = df["state"] + df["county"]
    df["state"] = df["state"].map(STATE_FIPS_TO_ABBR)
    df["year"] = year

    # Core totals — always present
    df["total_permits"] = pd.to_numeric(df["BLDGS"], errors="coerce")
    df["total_units_permitted"] = pd.to_numeric(df["UNITS"], errors="coerce")

    # Multi-family breakdown — may not be present in every response
    has_mf = "BLDGS_5PLUS" in df.columns and "UNITS_5PLUS" in df.columns
    if has_mf:
        df["mf_permits"] = pd.to_numeric(df["BLDGS_5PLUS"], errors="coerce")
        df["mf_units_permitted"] = pd.to_numeric(df["UNITS_5PLUS"], errors="coerce")
        df["sf_permits"] = df["total_permits"] - df["mf_permits"]
        df["mf_pct"] = (df["mf_units_permitted"] / df["total_units_permitted"]).round(4)
    else:
        df["mf_permits"] = pd.Series(pd.NA, index=df.index, dtype="Float64")
        df["mf_units_permitted"] = pd.Series(pd.NA, index=df.index, dtype="Float64")
        df["sf_permits"] = pd.Series(pd.NA, index=df.index, dtype="Float64")
        df["mf_pct"] = pd.Series(pd.NA, index=df.index, dtype="Float64")

    keep = [
        "fips", "state", "year",
        "total_permits", "total_units_permitted",
        "mf_permits", "mf_units_permitted", "sf_permits", "mf_pct",
    ]
    return df[keep].copy()


def fetch_permits_year(year: int, output_path: str = None, api_key: str = None) -> pd.DataFrame:
    """Fetch BPS data for a single year and optionally save to Parquet."""
    api_key = api_key or os.environ.get("CENSUS_API_KEY", "")
    url = build_permits_url(year, api_key=api_key)
    resp = requests.get(url, timeout=120)
    resp.raise_for_status()
    raw = resp.json()
    df = parse_permits_response(raw, year)
    if output_path:
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(output_path, index=False)
        print(f"  Wrote {len(df)} rows → {output_path}")
    return df


def pull_permits(years: list[int], output_dir: str, api_key: str = None) -> dict[int, pd.DataFrame]:
    """Fetch BPS building permits data for multiple years."""
    print(f"Pulling Census BPS permits data for {years}...")
    results = {}
    for year in years:
        print(f"  Fetching {year}...")
        output_path = str(Path(output_dir) / f"permits_{year}.parquet")
        results[year] = fetch_permits_year(year, output_path=output_path, api_key=api_key)
    return results
