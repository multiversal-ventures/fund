# scripts/dc/pull_cbp_naics518.py
"""Census County Business Patterns — NAICS 518210 (data processing, hosting, related)."""
from __future__ import annotations

import os
import sys
from pathlib import Path

import pandas as pd
import requests

_scripts = Path(__file__).resolve().parent.parent
if str(_scripts) not in sys.path:
    sys.path.insert(0, str(_scripts))

from pull_census import STATE_FIPS_TO_ABBR

CBP_BASE = "https://api.census.gov/data"


def build_url(year: int, api_key: str | None) -> str:
    url = (
        f"{CBP_BASE}/{year}/cbp"
        "?get=EMP,ESTAB,NAICS2017"
        "&for=county:*&in=state:*"
        "&NAICS2017=518210"
    )
    if api_key:
        url += f"&key={api_key}"
    return url


def parse_cbp_naics518(raw: list, year: int) -> pd.DataFrame:
    header = raw[0]
    data = raw[1:]
    df = pd.DataFrame(data, columns=header)
    df["fips"] = df["state"] + df["county"]
    df["EMP"] = pd.to_numeric(df["EMP"], errors="coerce").fillna(0).astype(int)
    df["ESTAB"] = pd.to_numeric(df["ESTAB"], errors="coerce").fillna(0).astype(int)
    df["year"] = year
    df["state"] = df["state"].map(STATE_FIPS_TO_ABBR)
    return df[["fips", "state", "year", "EMP", "ESTAB"]].rename(
        columns={"EMP": "naics518_emp", "ESTAB": "naics518_estab"}
    )


def pull_cbp_naics518(year: int, output_dir: str, api_key: str | None = None) -> Path:
    """Fetch all US counties; write Parquet; return path."""
    url = build_url(year, api_key or os.environ.get("CENSUS_API_KEY"))
    r = requests.get(url, timeout=120)
    r.raise_for_status()
    raw = r.json()
    df = parse_cbp_naics518(raw, year)
    out = Path(output_dir) / f"cbp_naics518_{year}.parquet"
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out, index=False)
    print(f"  Wrote {len(df)} counties → {out}")
    return out


def load_cbp_naics518_parquet(path: Path) -> pd.DataFrame:
    return pd.read_parquet(path)
