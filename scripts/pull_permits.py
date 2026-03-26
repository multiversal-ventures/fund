# scripts/pull_permits.py
"""Pull Census Building Permits Survey data from bulk download files."""
import os
import requests
import pandas as pd
from pathlib import Path
from pull_census import STATE_FIPS_TO_ABBR

BPS_BULK_URL = "https://www2.census.gov/econ/bps/County/co{year}a.txt"


def build_permits_url(year: int) -> str:
    """Build URL for Census BPS county-level bulk file."""
    return BPS_BULK_URL.format(year=year)


def parse_permits_response(raw_text: str, year: int) -> pd.DataFrame:
    """Parse Census BPS bulk text file into DataFrame.

    The file has a two-row header and comma-separated data with columns:
    Survey Date, FIPS State, FIPS County, Region Code, Division Code, County Name,
    1-unit Bldgs/Units/Value, 2-units Bldgs/Units/Value,
    3-4 units Bldgs/Units/Value, 5+ units Bldgs/Units/Value, ...rep columns
    """
    lines = raw_text.strip().split("\n")
    data_lines = [l for l in lines[2:] if l.strip()]

    rows = []
    for line in data_lines:
        parts = [p.strip() for p in line.split(",")]
        if len(parts) < 18:
            continue
        try:
            fips_state = parts[1].zfill(2)
            fips_county = parts[2].zfill(3)
            fips = fips_state + fips_county
            county_name = parts[5].strip()

            sf_bldgs = int(parts[6] or 0)
            sf_units = int(parts[7] or 0)
            two_bldgs = int(parts[9] or 0)
            two_units = int(parts[10] or 0)
            three4_bldgs = int(parts[12] or 0)
            three4_units = int(parts[13] or 0)
            mf_bldgs = int(parts[15] or 0)  # 5+ unit buildings
            mf_units = int(parts[16] or 0)  # 5+ unit units

            total_bldgs = sf_bldgs + two_bldgs + three4_bldgs + mf_bldgs
            total_units = sf_units + two_units + three4_units + mf_units

            rows.append({
                "fips": fips,
                "state": STATE_FIPS_TO_ABBR.get(fips_state, ""),
                "county": county_name,
                "year": year,
                "total_permits": total_bldgs,
                "total_units_permitted": total_units,
                "sf_permits": sf_bldgs,
                "mf_permits": mf_bldgs,
                "mf_units_permitted": mf_units,
            })
        except (ValueError, IndexError):
            continue

    df = pd.DataFrame(rows)
    if len(df) > 0:
        df["mf_pct"] = (df["mf_permits"] / df["total_permits"].replace(0, float("nan"))).round(4)
    else:
        df["mf_pct"] = pd.Series(dtype=float)
    return df


def fetch_permits_year(year: int, output_path: str = None, api_key: str = None) -> pd.DataFrame:
    """Fetch building permits for a single year from bulk download."""
    url = build_permits_url(year)
    print(f"  Fetching building permits for {year}...")
    resp = requests.get(url, timeout=120)
    resp.raise_for_status()
    df = parse_permits_response(resp.text, year)
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
