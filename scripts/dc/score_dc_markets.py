# scripts/dc/score_dc_markets.py
"""Tier 1 DC adjacency market score (0–100) + subscores — see design spec."""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd

_scripts = Path(__file__).resolve().parent.parent
if str(_scripts) not in sys.path:
    sys.path.insert(0, str(_scripts))

_scripts_dc = Path(__file__).resolve().parent
if str(_scripts_dc) not in sys.path:
    sys.path.insert(0, str(_scripts_dc))

from score import normalize_signal
from load_eia_state import load_eia_state_industrial
from zillow import zillow_url_for_county_row


def _county_zillow_url_column(df: pd.DataFrame) -> list[str]:
    return [
        zillow_url_for_county_row(row["fips"], row.get("county"), row.get("state")) for _, row in df.iterrows()
    ]


def load_weights(path: Path | None = None) -> dict:
    p = path or (_scripts_dc / "dc_weights.default.json")
    with open(p, encoding="utf-8") as f:
        return json.load(f)


def _screen_mask(df: pd.DataFrame, screen: dict) -> pd.Series:
    """Eligibility for DC leaderboard (percentiles computed on this subset only)."""
    fips = df["fips"].astype(str).str.replace(r"\.0$", "", regex=True).str.zfill(5)
    st = fips.str[:2]
    prefixes = tuple(screen.get("exclude_state_fips_prefix") or ())
    territory_ok = ~st.isin(prefixes)
    min_pop = int(screen.get("min_population") or 0)
    pop_ok = df["pop"].fillna(0) >= min_pop
    out = territory_ok & pop_ok
    min_units = screen.get("min_housing_units")
    if min_units is not None and "total_units" in df.columns:
        out = out & (df["total_units"].fillna(0) >= int(min_units))
    return out


def score_dc_markets(
    census_path: Path,
    cbp_naics_path: Path,
    eia_df: pd.DataFrame,
    tavily_path: Path | None,
    occupations_path: Path | None,
    weights: dict,
) -> pd.DataFrame:
    """Return county-level DC scores for full census universe; ineligible rows have null scores."""
    cen = pd.read_parquet(census_path)
    need = ["fips", "county", "state", "pop"]
    for c in need:
        if c not in cen.columns:
            raise ValueError(f"census missing {c}: {census_path}")

    extra = ["total_units"] if "total_units" in cen.columns else []
    cbp = pd.read_parquet(cbp_naics_path)
    df = cen[need + extra].merge(cbp[["fips", "naics518_emp"]], on="fips", how="left")
    df["naics518_emp"] = df["naics518_emp"].fillna(0)
    df["naics518_per_1k"] = (df["naics518_emp"] / (df["pop"].replace(0, float("nan")) / 1000.0)).fillna(0.0)

    df = df.merge(eia_df, left_on="state", right_on="state_abbr", how="left")
    df = df.drop(columns=["state_abbr"], errors="ignore")
    df["industrial_cents_kwh"] = df["industrial_cents_kwh"].fillna(df["industrial_cents_kwh"].median())

    if tavily_path and Path(tavily_path).exists():
        tv = pd.read_parquet(tavily_path)
        df = df.merge(
            tv[["state_abbr", "tavily_political_score", "tavily_penalty"]],
            left_on="state",
            right_on="state_abbr",
            how="left",
        )
        df = df.drop(columns=["state_abbr"], errors="ignore")
    else:
        df["tavily_political_score"] = 0.5
        df["tavily_penalty"] = 0.0

    df["tavily_political_score"] = df["tavily_political_score"].fillna(0.5)
    df["tavily_penalty"] = df["tavily_penalty"].fillna(0.0).clip(0, weights.get("penalty_max", 10))

    if occupations_path and Path(occupations_path).exists():
        occ = pd.read_parquet(occupations_path)
        if "resilience_index" in occ.columns:
            df = df.merge(occ[["fips", "resilience_index"]], on="fips", how="left")
            df["labor_proxy"] = df["resilience_index"].fillna(0)
        else:
            df["labor_proxy"] = 0.0
    else:
        df["labor_proxy"] = 0.0

    screen = weights.get("screen") or {}
    elig = _screen_mask(df, screen)
    df["dc_eligible"] = elig

    w = weights
    score_cols = [
        "s_electrical",
        "s_water",
        "s_political",
        "s_pipeline",
        "s_connectivity",
        "s_labor",
        "s_unique",
    ]
    for c in score_cols:
        df[c] = pd.Series(pd.NA, index=df.index, dtype="Float64")

    df["dc_penalty"] = pd.Series(pd.NA, index=df.index, dtype="Float64")
    df["dc_market_score"] = pd.Series(pd.NA, index=df.index, dtype="Float64")

    if not elig.any():
        df["zillow_url"] = _county_zillow_url_column(df)
        return df

    sub = df.loc[elig].copy()
    n_elig = len(sub)
    n_all = len(df)
    print(f"  DC screen: {n_elig} / {n_all} counties eligible (min_pop={screen.get('min_population')}, territories excluded)")

    # Percentiles only on eligible counties (see score.py for normalize_signal)
    sub["s_electrical"] = normalize_signal(sub["industrial_cents_kwh"], w["electrical"], higher_is_better=False)
    sub["s_water"] = normalize_signal(
        pd.Series([1.0] * n_elig, index=sub.index), w["water_cooling"], higher_is_better=True
    )
    sub["s_political"] = normalize_signal(sub["tavily_political_score"], w["political"], higher_is_better=True)
    sub["s_pipeline"] = normalize_signal(sub["naics518_per_1k"], w["pipeline"], higher_is_better=True)
    sub["s_connectivity"] = normalize_signal(
        pd.Series([1.0] * n_elig, index=sub.index), w["connectivity"], higher_is_better=True
    )
    sub["s_labor"] = normalize_signal(sub["labor_proxy"], w["labor_cost"], higher_is_better=True)
    sub["s_unique"] = normalize_signal(
        pd.Series([1.0] * n_elig, index=sub.index), w["unique"], higher_is_better=True
    )

    parts = [c for c in score_cols if c in sub.columns]
    sub["dc_penalty"] = sub["tavily_penalty"].fillna(0.0).clip(0, w.get("penalty_max", 10))
    sub["dc_market_score"] = (sub[parts].sum(axis=1) - sub["dc_penalty"]).clip(0, 100).round(2)

    for c in score_cols + ["dc_penalty", "dc_market_score"]:
        df.loc[elig, c] = sub[c].astype("Float64")

    df["zillow_url"] = _county_zillow_url_column(df)

    return df


def run_score_dc(
    data_dir: str,
    weights_path: Path | None = None,
) -> Path:
    data = Path(data_dir)
    census_p = data / "census" / "acs_2023.parquet"
    if not census_p.exists():
        raise FileNotFoundError(f"Missing {census_p} — run Census pull first (uv run run.py --census)")

    cbp_files = sorted(data.glob("dc/cbp_naics518_*.parquet"))
    if not cbp_files:
        raise FileNotFoundError(f"No dc/cbp_naics518_*.parquet under {data}")
    cbp_p = cbp_files[-1]

    eia_df = load_eia_state_industrial()

    tavily_p = data / "dc" / "dc_tavily_state.parquet"
    occ_p = data / "census" / "occupations_2023.parquet"
    wts = load_weights(weights_path)

    out = score_dc_markets(
        census_p,
        cbp_p,
        eia_df,
        tavily_p if tavily_p.exists() else None,
        occ_p if occ_p.exists() else None,
        wts,
    )

    out_path = data / "dc" / "dc_market_scores.parquet"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(out_path, index=False)
    print(f"  Wrote {len(out)} counties → {out_path}")
    return out_path
