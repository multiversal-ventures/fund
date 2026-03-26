# scripts/score.py
"""Two-layer scoring engine: Market Score + Deal Score for multifamily acquisition signals."""
import pandas as pd
from pathlib import Path
from zillow import add_zillow_urls


def normalize_signal(values: pd.Series, weight: float, higher_is_better: bool = True) -> pd.Series:
    vmin = values.min()
    vmax = values.max()
    if vmax == vmin:
        return pd.Series([weight / 2] * len(values), index=values.index)
    if higher_is_better:
        normalized = (values - vmin) / (vmax - vmin)
    else:
        normalized = (vmax - values) / (vmax - vmin)
    return (normalized * weight).round(2)


def apply_hard_filters(census_df: pd.DataFrame, occupation_df: pd.DataFrame, config: dict) -> list:
    """Return list of FIPS codes that pass all hard filters."""
    hf = config["hard_filters"]
    mask = (
        (census_df["pop"] >= hf["min_population"])
        & (census_df["total_units"] >= hf["min_housing_units"])
        & (census_df["renter_occupied"] >= hf["min_renter_households"])
    )
    passing_fips = set(census_df.loc[mask, "fips"])

    # Employment filter from occupation data
    emp_mask = occupation_df["total_employed"] >= hf["min_employed"]
    emp_fips = set(occupation_df.loc[emp_mask, "fips"])

    return list(passing_fips & emp_fips)


def score_markets(
    census_latest: pd.DataFrame,
    census_earliest: pd.DataFrame,
    occupation_data: pd.DataFrame,
    permits_data: pd.DataFrame,
    cbp_data: pd.DataFrame,
    config: dict,
) -> pd.DataFrame:
    """Compute county-level market scores."""
    weights = config["market_weights"]

    df = census_latest[["fips", "county", "state"]].copy()

    # Vacancy trend: lower/negative = better
    merged = census_latest[["fips", "vacancy_rate"]].merge(
        census_earliest[["fips", "vacancy_rate"]].rename(columns={"vacancy_rate": "vac_prev"}),
        on="fips", how="left",
    )
    df["vacancy_trend"] = (merged["vacancy_rate"] - merged["vac_prev"]).fillna(0)

    # Rent growth: higher = better
    rent_merged = census_latest[["fips", "median_rent"]].merge(
        census_earliest[["fips", "median_rent"]].rename(columns={"median_rent": "rent_prev"}),
        on="fips", how="left",
    )
    df["rent_growth"] = ((rent_merged["median_rent"] - rent_merged["rent_prev"]) / rent_merged["rent_prev"]).fillna(0)

    # Rent cost ratio: higher = better
    df["rent_cost_ratio"] = census_latest["rent_to_cost_ratio"].fillna(0).values

    # Workforce resilience: higher = better
    if occupation_data is not None and "resilience_index" in occupation_data.columns:
        df = df.merge(occupation_data[["fips", "resilience_index"]], on="fips", how="left")
        df["workforce_resilience"] = df["resilience_index"].fillna(0)
    else:
        df["workforce_resilience"] = 0.0

    # Employment concentration (HHI): lower = better
    if cbp_data is not None and "hhi" in cbp_data.columns:
        df = df.merge(cbp_data[["fips", "hhi"]], on="fips", how="left")
        df["employment_concentration"] = df["hhi"].fillna(0)
    else:
        df["employment_concentration"] = 0.0

    # Pop growth: higher = better
    pop_merged = census_latest[["fips", "pop"]].merge(
        census_earliest[["fips", "pop"]].rename(columns={"pop": "pop_prev"}),
        on="fips", how="left",
    )
    df["pop_growth"] = ((pop_merged["pop"] - pop_merged["pop_prev"]) / pop_merged["pop_prev"]).fillna(0)

    # Supply pressure: lower = better
    if permits_data is not None and "mf_units_permitted" in permits_data.columns:
        df = df.merge(permits_data[["fips", "mf_units_permitted"]], on="fips", how="left")
        total_units = census_latest.set_index("fips")["total_units"]
        units_per_1k = total_units / 1000
        # Handle division by zero
        units_per_1k = units_per_1k.replace(0, float("nan"))
        df["supply_pressure"] = (
            df["mf_units_permitted"].fillna(0) / df["fips"].map(units_per_1k)
        ).fillna(0)
    else:
        df["supply_pressure"] = 0.0

    # Score each signal
    df["s_vacancy_trend"] = normalize_signal(df["vacancy_trend"], weights["vacancy_trend"], higher_is_better=False)
    df["s_rent_growth"] = normalize_signal(df["rent_growth"], weights["rent_growth"], higher_is_better=True)
    df["s_rent_cost_ratio"] = normalize_signal(df["rent_cost_ratio"], weights["rent_cost_ratio"], higher_is_better=True)
    df["s_workforce_resilience"] = normalize_signal(df["workforce_resilience"], weights["workforce_resilience"], higher_is_better=True)
    df["s_employment_concentration"] = normalize_signal(df["employment_concentration"], weights["employment_concentration"], higher_is_better=False)
    df["s_pop_growth"] = normalize_signal(df["pop_growth"], weights["pop_growth"], higher_is_better=True)
    df["s_supply_pressure"] = normalize_signal(df["supply_pressure"], weights["supply_pressure"], higher_is_better=False)

    score_cols = [c for c in df.columns if c.startswith("s_")]
    df["market_score"] = df[score_cols].sum(axis=1).round(1)

    return df


def score_deals(
    hud_fha: pd.DataFrame,
    market_scores: pd.DataFrame,
    census_latest: pd.DataFrame,
    config: dict,
) -> pd.DataFrame:
    """Score individual properties (deal layer). Only considers counties with sufficient market score."""
    weights = config["deal_weights"]
    min_market = config.get("deal_min_market_score", 40)

    # Filter to counties meeting market score threshold
    qualifying_fips = market_scores.loc[market_scores["market_score"] >= min_market, "fips"].tolist()
    df = hud_fha[hud_fha["fips"].isin(qualifying_fips)].copy()

    if df.empty:
        df["deal_score"] = pd.Series(dtype=float)
        df["market_score"] = pd.Series(dtype=float)
        return df

    # Mortgage maturity: capped at 5, lower = better
    maturity_capped = df["maturity_years"].clip(0, 5).fillna(5)
    df["s_mortgage_maturity"] = normalize_signal(maturity_capped, weights["mortgage_maturity"], higher_is_better=False)

    # Unit count: higher = better
    df["s_unit_count"] = normalize_signal(df["units"].fillna(0), weights["unit_count"], higher_is_better=True)

    # Section 8: boolean → 1/0, higher = better
    s8_values = df["section8"].astype(int)
    df["s_section8"] = normalize_signal(s8_values, weights["section8"], higher_is_better=True)

    # Area vacancy: distance from 0.065 optimal, lower distance = better
    df = df.merge(census_latest[["fips", "vacancy_rate"]], on="fips", how="left", suffixes=("", "_census"))
    vac_col = "vacancy_rate" if "vacancy_rate" in df.columns else "vacancy_rate_census"
    optimal_vacancy = 0.065
    df["vac_distance"] = (df[vac_col].fillna(optimal_vacancy) - optimal_vacancy).abs()
    df["s_area_vacancy"] = normalize_signal(df["vac_distance"], weights["area_vacancy"], higher_is_better=False)

    score_cols = [c for c in df.columns if c.startswith("s_")]
    df["deal_score"] = df[score_cols].sum(axis=1).round(1)

    # Merge market score
    df = df.merge(market_scores[["fips", "market_score"]], on="fips", how="left", suffixes=("", "_dup"))
    # Drop duplicate market_score column if it exists
    if "market_score_dup" in df.columns:
        df = df.drop(columns=["market_score_dup"])

    return df


def combine_scores(
    market_scores: pd.DataFrame,
    deal_scores: pd.DataFrame,
    config: dict,
) -> pd.DataFrame:
    """Combine market and deal scores into total score."""
    split = config["market_deal_split"]
    market_weight = split["market"]
    deal_weight = split["deal"]

    df = deal_scores.copy()
    df["total_score"] = (
        (market_weight * df["market_score"] + deal_weight * df["deal_score"]) / 100
    ).round(1)

    df["signal_rank"] = df["total_score"].rank(ascending=False, method="min").astype("Int64")
    df["zillow_url"] = add_zillow_urls(df)
    df = df.sort_values("signal_rank").reset_index(drop=True)

    return df


def run_scoring(data_dir: str, config: dict, output_path: str = None) -> pd.DataFrame:
    """Orchestrate the full scoring pipeline."""
    data = Path(data_dir)
    years = sorted(config["census"]["years"])
    latest_year = years[-1]
    earliest_year = years[0]

    # Load required data
    census_latest = pd.read_parquet(data / "census" / f"acs_{latest_year}.parquet")
    census_earliest = pd.read_parquet(data / "census" / f"acs_{earliest_year}.parquet")
    hud_fha = pd.read_parquet(data / "hud" / "fha_multifamily.parquet")

    # Load occupation data
    occ_path = data / "census" / f"occupations_{latest_year}.parquet"
    occupation_data = pd.read_parquet(occ_path) if occ_path.exists() else None

    # Load optional data (permits, cbp)
    permits_path = data / "census" / "permits.parquet"
    if not permits_path.exists():
        permits_path = data / "permits" / "permits.parquet"
    permits_data = pd.read_parquet(permits_path) if permits_path.exists() else None

    cbp_path = data / "cbp" / "cbp.parquet"
    cbp_data = pd.read_parquet(cbp_path) if cbp_path.exists() else None

    # Hard filters
    if occupation_data is not None:
        passing_fips = apply_hard_filters(census_latest, occupation_data, config)
        census_latest = census_latest[census_latest["fips"].isin(passing_fips)]
        census_earliest = census_earliest[census_earliest["fips"].isin(passing_fips)]

    # Apply target market filter if configured
    target_fips = config.get("target_markets", [])
    if target_fips:
        hud_fha = hud_fha[hud_fha["fips"].isin(target_fips)]

    # Score markets
    mkt = score_markets(census_latest, census_earliest, occupation_data, permits_data, cbp_data, config)

    # Save market scores separately
    out_dir = Path(output_path).parent if output_path else data / "output"
    out_dir.mkdir(parents=True, exist_ok=True)
    mkt.to_parquet(out_dir / "market_scores.parquet", index=False)
    print(f"  Wrote {len(mkt)} market scores → {out_dir / 'market_scores.parquet'}")

    # Score deals
    deals = score_deals(hud_fha, mkt, census_latest, config)

    if deals.empty:
        print("  No deals passed market score threshold.")
        return deals

    # Combine
    df = combine_scores(mkt, deals, config)

    # Save properties
    props_path = output_path or str(out_dir / "properties.parquet")
    Path(props_path).parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(props_path, index=False)
    print(f"  Wrote {len(df)} scored properties → {props_path}")

    return df
