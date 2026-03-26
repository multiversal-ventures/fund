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
    return f"{FHA_ENDPOINT}?$limit={limit}&$offset={offset}"


def parse_fha_response(raw: list[dict]) -> pd.DataFrame:
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
    df["maturity_date"] = pd.to_datetime(df["maturity_date"], errors="coerce")
    now = datetime.now()
    df["maturity_years"] = ((df["maturity_date"] - now).dt.days / 365.25).round(2)
    df["section8"] = df["program"].str.contains("Section 8", case=False, na=False)

    keep = [
        "fips", "property_name", "address", "city", "state", "zip",
        "units", "mortgage_amount", "maturity_date", "maturity_years",
        "section8", "lat", "lng", "loan_id",
    ]
    return df[[c for c in keep if c in df.columns]].copy()


def parse_usps_vacancy(raw: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(raw)
    df["geoid"] = df["geoid"].astype(str).str.zfill(5)
    df["tot_res"] = pd.to_numeric(df["tot_res"], errors="coerce")
    df["res_vac"] = pd.to_numeric(df["res_vac"], errors="coerce")
    df["usps_vacancy_rate"] = (df["res_vac"] / df["tot_res"]).round(4)
    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df["quarter"] = pd.to_numeric(df["quarter"], errors="coerce")
    return df.rename(columns={"geoid": "fips"})


def fetch_fha_multifamily(output_path: str = None) -> pd.DataFrame:
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
    print("Pulling HUD data...")
    fha = fetch_fha_multifamily(str(Path(output_dir) / "fha_multifamily.parquet"))
    usps = fetch_usps_vacancy(str(Path(output_dir) / "usps_vacancy.parquet"))
    return fha, usps
