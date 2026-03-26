# scripts/sensitivity.py
"""Monte Carlo sensitivity analysis for scoring stability."""
import numpy as np
import pandas as pd


def jitter_weights(weights: dict, jitter_pct: float = 20) -> dict:
    """Jitter weights by ±jitter_pct% and renormalize to sum to 100."""
    jittered = {}
    for k, v in weights.items():
        low = v * (1 - jitter_pct / 100)
        high = v * (1 + jitter_pct / 100)
        jittered[k] = max(0, np.random.uniform(low, high))
    total = sum(jittered.values())
    if total > 0:
        jittered = {k: round(v / total * 100, 2) for k, v in jittered.items()}
    return jittered


def run_monte_carlo(scored_df: pd.DataFrame, config: dict) -> pd.DataFrame:
    """Run Monte Carlo simulation to assess ranking stability.

    Jitters the market/deal split across N iterations and tracks
    how each property's rank changes. Outputs stability metrics.

    Args:
        scored_df: DataFrame with market_score, deal_score, total_score, signal_rank columns
        config: dict with sensitivity.iterations, sensitivity.jitter_pct, market_deal_split

    Returns:
        Same DataFrame with added columns: stability_score, rank_std, rank_min, rank_max
    """
    iterations = config.get("sensitivity", {}).get("iterations", 1000)
    jitter_pct = config.get("sensitivity", {}).get("jitter_pct", 20)
    split = config.get("market_deal_split", {"market": 60, "deal": 40})

    df = scored_df.copy()
    n = len(df)
    if n == 0:
        df["stability_score"] = pd.Series(dtype=float)
        df["rank_std"] = pd.Series(dtype=float)
        df["rank_min"] = pd.Series(dtype=int)
        df["rank_max"] = pd.Series(dtype=int)
        return df

    market_scores = df["market_score"].fillna(0).values
    deal_scores = df["deal_score"].fillna(0).values

    baseline_rank = df["signal_rank"].values if "signal_rank" in df.columns else np.arange(1, n + 1)

    all_ranks = np.zeros((iterations, n))

    for i in range(iterations):
        jittered_split = jitter_weights(split, jitter_pct)
        m_weight = jittered_split.get("market", 60)
        d_weight = jittered_split.get("deal", 40)
        new_total = (m_weight * market_scores + d_weight * deal_scores) / 100
        # Rank: higher score = lower rank number (rank 1 = best)
        all_ranks[i] = (-new_total).argsort().argsort() + 1

    rank_std = all_ranks.std(axis=0)
    rank_min = all_ranks.min(axis=0)
    rank_max = all_ranks.max(axis=0)

    # Stability: % of iterations property stayed in same rank decile
    n_deciles = max(1, n // 10)
    baseline_decile = (baseline_rank - 1) // n_deciles
    decile_match = np.zeros(n)
    for i in range(iterations):
        iter_decile = (all_ranks[i].astype(int) - 1) // n_deciles
        decile_match += (iter_decile == baseline_decile).astype(float)
    stability = (decile_match / iterations * 100).round(1)

    df["stability_score"] = stability
    df["rank_std"] = rank_std.round(2)
    df["rank_min"] = rank_min.astype(int)
    df["rank_max"] = rank_max.astype(int)

    return df
