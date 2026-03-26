# scripts/pull_hud.py
"""Pull HUD FHA multifamily data via ArcGIS API and USPS vacancy data."""
import requests
import pandas as pd
from pathlib import Path
from datetime import datetime

FHA_ENDPOINT = (
    "https://services.arcgis.com/VTyQ9soqVukalItT/ArcGIS/rest/services/"
    "HUD_Insured_Multifamily_Properties/FeatureServer/0/query"
)
ARCGIS_LIMIT = 2000  # ArcGIS max per request

# USPS vacancy data from HUD User
USPS_URL = "https://www.huduser.gov/hudapi/public/usps"


def build_fha_url(limit: int = ARCGIS_LIMIT, offset: int = 0) -> str:
    """Build ArcGIS query URL for FHA multifamily properties."""
    fields = (
        "PROPERTY_NAME_TEXT,ADDRESS_LINE1_TEXT,PLACED_BASE_CITY_NAME_TEXT,"
        "STD_ZIP5,TOTAL_UNIT_COUNT,PRIMARY_FHA_NUMBER,"
        "LOAN_MATURITY_DATE,SOA_NAME1,LAT,LON,"
        "STATE2KX,COUNTY_LEVEL,PROPERTY_CATEGORY_NAME,"
        "TOTAL_ASSISTED_UNIT_COUNT"
    )
    return (
        f"{FHA_ENDPOINT}?where=1%3D1&outFields={fields}"
        f"&resultRecordCount={limit}&resultOffset={offset}&f=json"
    )


def parse_fha_response(raw: list[dict]) -> pd.DataFrame:
    """Parse ArcGIS feature attributes into DataFrame."""
    df = pd.DataFrame(raw)

    col_map = {
        "PROPERTY_NAME_TEXT": "property_name",
        "ADDRESS_LINE1_TEXT": "address",
        "PLACED_BASE_CITY_NAME_TEXT": "city",
        "STD_ZIP5": "zip",
        "TOTAL_UNIT_COUNT": "units",
        "PRIMARY_FHA_NUMBER": "loan_id",
        "LOAN_MATURITY_DATE": "maturity_date_ms",
        "SOA_NAME1": "program",
        "LAT": "lat",
        "LON": "lng",
        "STATE2KX": "fips_state",
        "COUNTY_LEVEL": "county_fips_full",
        "PROPERTY_CATEGORY_NAME": "category",
        "TOTAL_ASSISTED_UNIT_COUNT": "assisted_units",
    }
    df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})

    # Build FIPS: COUNTY_LEVEL is the full county FIPS (e.g. 18097)
    # Pad to 5 digits: state(2) + county(3)
    df["fips"] = df["county_fips_full"].astype(str).str.zfill(5)
    # Extract state abbreviation from fips_state
    from pull_census import STATE_FIPS_TO_ABBR
    df["state"] = df["fips_state"].astype(str).str.zfill(2).map(STATE_FIPS_TO_ABBR)

    df["units"] = pd.to_numeric(df["units"], errors="coerce").fillna(0).astype(int)
    df["lat"] = pd.to_numeric(df["lat"], errors="coerce")
    df["lng"] = pd.to_numeric(df["lng"], errors="coerce")

    # Maturity date is epoch milliseconds
    df["maturity_date"] = pd.to_datetime(df["maturity_date_ms"], unit="ms", errors="coerce")
    now = datetime.now()
    df["maturity_years"] = ((df["maturity_date"] - now).dt.days / 365.25).round(2)

    # Section 8 indicator from category or program
    df["section8"] = (
        df["category"].str.contains("Subsidized", case=False, na=False) |
        df.get("program", pd.Series(dtype=str)).str.contains("Section 8|Sec 8", case=False, na=False)
    )

    keep = [
        "fips", "property_name", "address", "city", "state", "zip",
        "units", "maturity_date", "maturity_years",
        "section8", "lat", "lng", "loan_id",
    ]
    return df[[c for c in keep if c in df.columns]].copy()


def parse_usps_vacancy(raw: list[dict]) -> pd.DataFrame:
    """Parse USPS vacancy data into DataFrame."""
    df = pd.DataFrame(raw)
    df["geoid"] = df["geoid"].astype(str).str.zfill(5)
    df["tot_res"] = pd.to_numeric(df["tot_res"], errors="coerce")
    df["res_vac"] = pd.to_numeric(df["res_vac"], errors="coerce")
    df["usps_vacancy_rate"] = (df["res_vac"] / df["tot_res"]).round(4)
    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df["quarter"] = pd.to_numeric(df["quarter"], errors="coerce")
    return df.rename(columns={"geoid": "fips"})


def fetch_fha_multifamily(output_path: str = None) -> pd.DataFrame:
    """Fetch all FHA multifamily insured properties, paginating through ArcGIS."""
    print("  Fetching HUD FHA multifamily data via ArcGIS...")
    all_records = []
    offset = 0

    while True:
        url = build_fha_url(limit=ARCGIS_LIMIT, offset=offset)
        resp = requests.get(url, timeout=120)
        resp.raise_for_status()
        data = resp.json()

        features = data.get("features", [])
        if not features:
            break

        batch = [f["attributes"] for f in features]
        all_records.extend(batch)
        offset += ARCGIS_LIMIT
        print(f"    Fetched {len(all_records)} records so far...")

        # ArcGIS signals more data with exceededTransferLimit
        if not data.get("exceededTransferLimit", False):
            break

    df = parse_fha_response(all_records)

    if output_path:
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(output_path, index=False)
        print(f"  Wrote {len(df)} rows → {output_path}")

    return df


def fetch_usps_vacancy(output_path: str = None) -> pd.DataFrame:
    """Fetch USPS vacancy data. Falls back to empty DataFrame if unavailable."""
    print("  Fetching USPS vacancy data...")
    try:
        # HUD User API requires registration; skip if not available
        # For now, return empty DataFrame — USPS data is optional for scoring
        print("    USPS vacancy API requires HUD User token — skipping (optional)")
        df = pd.DataFrame(columns=["fips", "tot_res", "res_vac", "usps_vacancy_rate", "year", "quarter"])
        if output_path:
            Path(output_path).parent.mkdir(parents=True, exist_ok=True)
            df.to_parquet(output_path, index=False)
        return df
    except Exception as e:
        print(f"    USPS vacancy fetch failed: {e}")
        return pd.DataFrame(columns=["fips", "usps_vacancy_rate"])


def pull_hud(output_dir: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Pull all HUD data."""
    print("Pulling HUD data...")
    fha = fetch_fha_multifamily(str(Path(output_dir) / "fha_multifamily.parquet"))
    usps = fetch_usps_vacancy(str(Path(output_dir) / "usps_vacancy.parquet"))
    return fha, usps
