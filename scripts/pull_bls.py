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
    # Select only columns we care about (raw data has many extra cols with ** values)
    keep_raw = [c for c in col_map if c in df.columns]
    df = df[keep_raw].copy()
    df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})
    for col in ["total_employment", "hourly_median", "annual_median", "location_quotient"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    df["year"] = year
    return df


def _download_with_playwright(url: str, download_dir: str) -> Path:
    """Download a file using headless browser to bypass bot protection."""
    from playwright.sync_api import sync_playwright
    import time
    import random

    time.sleep(random.uniform(1.0, 3.0))

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context()
        page = context.new_page()

        # Navigate to the BLS page first to get cookies
        page.goto("https://www.bls.gov/oes/", wait_until="domcontentloaded")
        time.sleep(random.uniform(0.5, 1.5))

        # Now download the file
        with page.expect_download(timeout=120000) as download_info:
            page.evaluate(f"window.location.href = '{url}'")
        download = download_info.value

        dest = Path(download_dir) / download.suggested_filename
        download.save_as(str(dest))
        browser.close()
        print(f"    Downloaded via headless browser → {dest}")
        return dest


def fetch_oews_year(year: int, output_path: str = None) -> pd.DataFrame:
    import time
    import random

    url = build_oews_url(year)
    print(f"  Downloading {url}...")

    # First try direct request
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36",
        "Accept": "*/*",
        "Referer": "https://www.bls.gov/oes/",
    }
    resp = requests.get(url, headers=headers, timeout=180)

    if resp.status_code == 403:
        print(f"    Direct download blocked (403). Using headless browser...")
        download_dir = str(Path(output_path).parent) if output_path else "/tmp"
        Path(download_dir).mkdir(parents=True, exist_ok=True)
        zip_path = _download_with_playwright(url, download_dir)
        content = zip_path.read_bytes()
        zip_path.unlink()  # clean up zip after reading
    else:
        resp.raise_for_status()
        content = resp.content

    import zipfile
    with zipfile.ZipFile(io.BytesIO(content)) as zf:
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
