# scripts/tests/test_sensitivity.py
import pytest
import pandas as pd
import numpy as np
from sensitivity import run_monte_carlo, jitter_weights

def test_jitter_weights():
    weights = {"a": 50, "b": 30, "c": 20}
    jittered = jitter_weights(weights, jitter_pct=20)
    assert abs(sum(jittered.values()) - 100) < 0.1
    assert set(jittered.keys()) == set(weights.keys())

def test_jitter_weights_preserves_relative_order():
    """With small jitter, relative ordering should usually be preserved."""
    weights = {"a": 60, "b": 30, "c": 10}
    np.random.seed(42)
    jittered = jitter_weights(weights, jitter_pct=10)
    assert jittered["a"] > jittered["b"] > jittered["c"]

def test_run_monte_carlo():
    scores = pd.DataFrame({
        "fips": ["A", "B", "C"],
        "property_name": ["P1", "P2", "P3"],
        "total_score": [80.0, 60.0, 40.0],
        "signal_rank": [1, 2, 3],
        "market_score": [70.0, 55.0, 45.0],
        "deal_score": [90.0, 65.0, 35.0],
    })
    config = {
        "market_deal_split": {"market": 60, "deal": 40},
        "sensitivity": {"iterations": 100, "jitter_pct": 20},
    }
    result = run_monte_carlo(scores, config)
    assert "stability_score" in result.columns
    assert "rank_std" in result.columns
    assert "rank_min" in result.columns
    assert "rank_max" in result.columns
    assert len(result) == 3
    assert result["stability_score"].between(0, 100).all()

def test_run_monte_carlo_empty():
    scores = pd.DataFrame(columns=["market_score", "deal_score", "total_score", "signal_rank"])
    config = {"sensitivity": {"iterations": 10, "jitter_pct": 20}, "market_deal_split": {"market": 60, "deal": 40}}
    result = run_monte_carlo(scores, config)
    assert len(result) == 0
    assert "stability_score" in result.columns
