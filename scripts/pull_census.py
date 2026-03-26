# scripts/pull_census.py
"""Pull Census ACS 5-Year data and write per-year Parquet files."""
import os
import requests
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path

CENSUS_BASE = "https://api.census.gov/data"

VARIABLES = [
    "B25001_001E",  # Total housing units
    "B25002_002E",  # Occupied
    "B25002_003E",  # Vacant
    "B25003_002E",  # Owner occupied
    "B25003_003E",  # Renter occupied
    "B25004_002E",  # For rent vacant
    "B25024_007E",  # 10-19 units
    "B25024_008E",  # 20-49 units
    "B25024_009E",  # 50+ units
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
    if variables is None:
        variables = VARIABLES
    var_str = ",".join(variables)
    url = f"{CENSUS_BASE}/{year}/acs/acs5?get=NAME,{var_str}&for=county:*&in=state:*"
    if api_key:
        url += f"&key={api_key}"
    return url


def parse_census_response(raw: list[list], year: int) -> pd.DataFrame:
    header = raw[0]
    data = raw[1:]
    df = pd.DataFrame(data, columns=header)
    df["fips"] = df["state"] + df["county"]
    df["county"] = df["NAME"].str.replace(r",.*$", "", regex=True).str.strip()
    df["state"] = df["state"].map(STATE_FIPS_TO_ABBR)

    numeric_cols = [c for c in VARIABLES if c in df.columns]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    def _col(src: str):
        return df[src] if src in df.columns else pd.Series(pd.NA, index=df.index, dtype="Float64")

    df["total_units"] = _col("B25001_001E")
    df["occupied"] = _col("B25002_002E")
    df["vacant"] = _col("B25002_003E")
    df["owner_occupied"] = _col("B25003_002E")
    df["renter_occupied"] = _col("B25003_003E")
    df["for_rent_vacant"] = _col("B25004_002E")

    mf_cols = ["B25024_007E", "B25024_008E", "B25024_009E", "B25024_010E", "B25024_011E"]
    existing_mf = [c for c in mf_cols if c in df.columns]
    df["mf_units"] = df[existing_mf].sum(axis=1) if existing_mf else pd.Series(pd.NA, index=df.index, dtype="Float64")
    df["mf_pct"] = (df["mf_units"] / df["total_units"]).round(4)

    df["median_rent"] = _col("B25064_001E")
    df["median_home_value"] = _col("B25077_001E")
    df["median_owner_cost"] = _col("B25105_001E")
    df["pop"] = _col("B01003_001E")

    df["vacancy_rate"] = (df["vacant"] / df["total_units"]).round(4)
    df["rental_vac_rate"] = (
        df["for_rent_vacant"] / (df["renter_occupied"] + df["for_rent_vacant"])
    ).round(4)
    df["rent_to_cost_ratio"] = (df["median_rent"] / df["median_owner_cost"]).round(4)
    df["year"] = year

    keep = [
        "fips", "county", "state", "total_units", "occupied", "vacant",
        "owner_occupied", "renter_occupied", "for_rent_vacant",
        "median_rent", "median_home_value", "median_owner_cost",
        "mf_units", "mf_pct", "pop", "vacancy_rate",
        "rental_vac_rate", "rent_to_cost_ratio", "year",
    ]
    return df[keep].copy()


def fetch_acs_year(year: int, output_path: str = None, api_key: str = None) -> pd.DataFrame:
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
    print(f"Pulling Census ACS data for {years}...")
    results = {}
    for year in years:
        print(f"  Fetching {year}...")
        output_path = str(Path(output_dir) / f"acs_{year}.parquet")
        results[year] = fetch_acs_year(year, output_path=output_path, api_key=api_key)
    return results
