# scripts/score.py
"""Compute acquisition signal scores for multifamily properties."""
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


def score_properties(
    census_latest: pd.DataFrame,
    census_earliest: pd.DataFrame,
    hud_fha: pd.DataFrame,
    weights: dict,
    usps_vacancy: pd.DataFrame = None,
    occupation_data: pd.DataFrame = None,
) -> pd.DataFrame:
    # Compute vacancy trend
    trend = census_latest[["fips", "vacancy_rate", "pop"]].merge(
        census_earliest[["fips", "vacancy_rate", "pop"]].rename(
            columns={"vacancy_rate": "vac_prev", "pop": "pop_prev"}
        ),
        on="fips",
        how="left",
    )
    trend["vac_trend_5yr_chg"] = trend["vacancy_rate"] - trend["vac_prev"]
    trend["pop_growth"] = ((trend["pop"] - trend["pop_prev"]) / trend["pop_prev"]).round(4)

    # Merge HUD with census
    df = hud_fha.merge(
        census_latest[["fips", "county", "state", "vacancy_rate", "rent_to_cost_ratio", "mf_pct"]],
        on="fips",
        how="left",
        suffixes=("", "_census"),
    )
    df = df.merge(
        trend[["fips", "vac_trend_5yr_chg", "pop_growth"]],
        on="fips",
        how="left",
    )

    # Score each signal
    if "maturity_years" in df.columns:
        maturity_capped = df["maturity_years"].clip(0, 5)
        df["score_maturity"] = normalize_signal(maturity_capped, weights["mortgage_maturity"], higher_is_better=False)
    else:
        df["score_maturity"] = 0

    df["score_vacancy"] = normalize_signal(
        df["vac_trend_5yr_chg"].fillna(0),
        weights["vacancy_trend"],
        higher_is_better=False,
    )

    df["score_rent_cost"] = normalize_signal(
        df["rent_to_cost_ratio"].fillna(0),
        weights["rent_cost_ratio"],
        higher_is_better=True,
    )

    if usps_vacancy is not None and "usps_vacancy_rate" in usps_vacancy.columns:
        df = df.merge(usps_vacancy[["fips", "usps_vacancy_rate"]], on="fips", how="left")
        vac_col = "usps_vacancy_rate"
    else:
        vac_col = "vacancy_rate"
    optimal_vacancy = 0.065
    df["vac_distance"] = abs(df[vac_col].fillna(optimal_vacancy) - optimal_vacancy)
    df["score_area_vac"] = normalize_signal(df["vac_distance"], weights["area_vacancy"], higher_is_better=False)

    df["score_pop"] = normalize_signal(
        df["pop_growth"].fillna(0),
        weights["pop_growth"],
        higher_is_better=True,
    )

    # Optional: AI-resistant workforce resilience score
    resilience_weight = weights.get("workforce_resilience", 0)
    if resilience_weight > 0 and occupation_data is not None and "resilience_index" in occupation_data.columns:
        df = df.merge(
            occupation_data[["fips", "resilience_index"]],
            on="fips",
            how="left",
        )
        df["score_resilience"] = normalize_signal(
            df["resilience_index"].fillna(0),
            resilience_weight,
            higher_is_better=True,
        )
    else:
        df["score_resilience"] = 0.0

    df["total_score"] = (
        df["score_maturity"].fillna(0) + df["score_vacancy"].fillna(0) +
        df["score_rent_cost"].fillna(0) + df["score_area_vac"].fillna(0) +
        df["score_pop"].fillna(0) + df["score_resilience"].fillna(0)
    ).round(1)

    df["signal_rank"] = df["total_score"].rank(ascending=False, method="min").astype("Int64")
    df["zillow_url"] = add_zillow_urls(df)
    df = df.sort_values("signal_rank").reset_index(drop=True)

    return df


def run_scoring(data_dir: str, config: dict, output_path: str = None) -> pd.DataFrame:
    years = sorted(config["census"]["years"])
    latest_year = years[-1]
    earliest_year = years[0]

    census_latest = pd.read_parquet(Path(data_dir) / "census" / f"acs_{latest_year}.parquet")
    census_earliest = pd.read_parquet(Path(data_dir) / "census" / f"acs_{earliest_year}.parquet")
    hud_fha = pd.read_parquet(Path(data_dir) / "hud" / "fha_multifamily.parquet")

    usps_path = Path(data_dir) / "hud" / "usps_vacancy.parquet"
    usps = pd.read_parquet(usps_path) if usps_path.exists() else None

    occ_path = Path(data_dir) / "census" / f"occupations_{latest_year}.parquet"
    occupation_data = pd.read_parquet(occ_path) if occ_path.exists() else None

    target_fips = config.get("target_markets", [])
    if target_fips:
        hud_fha = hud_fha[hud_fha["fips"].isin(target_fips)]

    df = score_properties(census_latest, census_earliest, hud_fha, config["scoring_weights"], usps, occupation_data)

    if output_path:
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(output_path, index=False)
        print(f"  Wrote {len(df)} scored properties → {output_path}")

    return df
