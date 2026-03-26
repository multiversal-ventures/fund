# scripts/pull_bls.py
"""Pull BLS OEWS employment data and write per-year Parquet files."""
import os
import io
import requests
import pandas as pd
from pathlib import Path

BLS_OEWS_BASE = "https://www.bls.gov/oes/special-requests"


def build_oews_url(year: int) -> str:
    short_year = str(year)[2:]
    return f"{BLS_OEWS_BASE}/oesm{short_year}ma.zip"


def parse_oews_data(raw_df: pd.DataFrame, year: int) -> pd.DataFrame:
    df = raw_df.copy()
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
    for col in ["total_employment", "hourly_median", "annual_median", "location_quotient"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    df["year"] = year
    return df


def fetch_oews_year(year: int, output_path: str = None) -> pd.DataFrame:
    url = build_oews_url(year)
    print(f"  Downloading {url}...")
    resp = requests.get(url, timeout=120)
    resp.raise_for_status()

    import zipfile
    with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
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
    print(f"Pulling BLS OEWS data for {years}...")
    results = {}
    for year in years:
        print(f"  Fetching {year}...")
        output_path = str(Path(output_dir) / f"oews_{year}.parquet")
        results[year] = fetch_oews_year(year, output_path=output_path)
    return results
