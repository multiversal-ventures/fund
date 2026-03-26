# scripts/pull_cbp.py
"""Pull Census County Business Patterns data and compute HHI per county."""
import os
import re
import requests
import pandas as pd
from pathlib import Path

from pull_census import STATE_FIPS_TO_ABBR

CBP_BASE = "https://api.census.gov/data"

# 2-digit NAICS pattern: exactly 2 digits, or range like "44-45"
_NAICS2_PATTERN = re.compile(r"^\d{2}(-\d{2})?$")


def compute_hhi(shares: list[float]) -> float:
    """Compute Herfindahl-Hirschman Index from market share fractions (0-1).

    Returns HHI on 0-10000 scale: sum((s * 100) ** 2 for s in shares).
    """
    return sum((s * 100) ** 2 for s in shares)


def build_cbp_url(year: int, api_key: str = None) -> str:
    """Build Census CBP API URL for the given year."""
    url = (
        f"{CBP_BASE}/{year}/cbp"
        f"?get=EMP,NAICS2017,NAICS2017_LABEL"
        f"&for=county:*&in=state:*&NAICS2017=*"
    )
    if api_key:
        url += f"&key={api_key}"
    return url


def _is_2digit_naics(code: str) -> bool:
    """Return True if code is a 2-digit NAICS sector (e.g. '31', '44-45')."""
    return bool(_NAICS2_PATTERN.match(str(code).strip()))


def parse_cbp_response(raw: list[list], year: int) -> pd.DataFrame:
    """Parse raw CBP API response and return one row per county with HHI metrics.

    Parameters
    ----------
    raw:
        JSON list-of-lists from the Census CBP API (first row = header).
    year:
        The data year, added as a column.

    Returns
    -------
    DataFrame with columns: fips, state, year, total_employment, hhi,
        top_sector_name, top_sector_share, top3_share, num_sectors.
    """
    header = raw[0]
    data = raw[1:]
    df = pd.DataFrame(data, columns=header)

    # Normalise column names: the API may return NAICS2017 or similar variants
    naics_col = next((c for c in df.columns if c.upper().startswith("NAICS2017") and "LABEL" not in c.upper()), None)
    label_col = next((c for c in df.columns if "LABEL" in c.upper()), None)
    emp_col = next((c for c in df.columns if c.upper() == "EMP"), "EMP")

    if naics_col is None:
        raise ValueError(f"Cannot find NAICS column in response. Columns: {header}")

    df = df.rename(columns={
        naics_col: "naics",
        label_col: "naics_label",
        emp_col: "emp",
    })

    # Build FIPS and map state
    df["fips"] = df["state"] + df["county"]
    df["state_abbr"] = df["state"].map(STATE_FIPS_TO_ABBR)

    # Convert employment to numeric
    df["emp"] = pd.to_numeric(df["emp"], errors="coerce").fillna(0)

    # Filter to 2-digit NAICS sectors only, exclude "00" (total)
    mask = df["naics"].apply(_is_2digit_naics) & (df["naics"] != "00")
    df = df[mask].copy()

    rows = []
    for fips, group in df.groupby("fips"):
        total_emp = group["emp"].sum()
        if total_emp <= 0:
            continue

        group = group.copy()
        group["share"] = group["emp"] / total_emp
        group = group.sort_values("share", ascending=False)

        shares = group["share"].tolist()
        hhi = compute_hhi(shares)

        top_row = group.iloc[0]
        top_sector_name = top_row["naics_label"] if label_col is not None else top_row["naics"]
        top_sector_share = round(top_row["share"], 4)

        top3_share = round(group["share"].head(3).sum(), 4)
        num_sectors = len(group)

        rows.append({
            "fips": fips,
            "state": top_row["state_abbr"],
            "year": year,
            "total_employment": total_emp,
            "hhi": round(hhi, 2),
            "top_sector_name": top_sector_name,
            "top_sector_share": top_sector_share,
            "top3_share": top3_share,
            "num_sectors": num_sectors,
        })

    return pd.DataFrame(rows)


def fetch_cbp_year(year: int, output_path: str = None, api_key: str = None) -> pd.DataFrame:
    """Fetch CBP data for one year, optionally save as parquet."""
    api_key = api_key or os.environ.get("CENSUS_API_KEY", "")
    url = build_cbp_url(year, api_key=api_key)
    print(f"  Fetching CBP {year}: {url[:80]}...")
    resp = requests.get(url, timeout=300)
    resp.raise_for_status()
    raw = resp.json()
    df = parse_cbp_response(raw, year)
    if output_path:
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(output_path, index=False)
        print(f"  Wrote {len(df)} rows → {output_path}")
    return df


def pull_cbp(
    years: list[int],
    output_dir: str,
    api_key: str = None,
) -> dict[int, pd.DataFrame]:
    """Pull CBP employment data for multiple years.

    Parameters
    ----------
    years:
        List of years to fetch (e.g. [2019, 2020, 2021]).
    output_dir:
        Directory where parquet files will be written as ``cbp_{year}.parquet``.
    api_key:
        Census API key (falls back to CENSUS_API_KEY env var).

    Returns
    -------
    Dict mapping year -> DataFrame.
    """
    print(f"Pulling County Business Patterns data for {years}...")
    results = {}
    for year in years:
        print(f"  Fetching {year}...")
        output_path = str(Path(output_dir) / f"cbp_{year}.parquet")
        results[year] = fetch_cbp_year(year, output_path=output_path, api_key=api_key)
    return results
