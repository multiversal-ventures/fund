import pytest
import pandas as pd
from occupation_resilience import compute_resilience_index, compute_job_stability, OCCUPATION_RESILIENCE, C24010_VARIABLES

def test_resilience_scores_valid():
    """All resilience scores should be between 0 and 1."""
    for name, info in OCCUPATION_RESILIENCE.items():
        assert 0 <= info["resilience"] <= 1, f"{name} has invalid resilience: {info['resilience']}"

def test_compute_resilience_index():
    """County with all construction workers should have high resilience."""
    data = {var: [0] for var in C24010_VARIABLES}
    data["C24010_001E"] = [1000]  # total employed
    data["C24010_032E"] = [500]   # construction male
    data["C24010_068E"] = [500]   # construction female
    data["fips"] = ["06067"]
    data["county"] = ["Sacramento"]
    data["state"] = ["CA"]
    data["year"] = [2023]

    df = pd.DataFrame(data)
    result = compute_resilience_index(df)

    assert len(result) == 1
    assert result.iloc[0]["resilience_index"] == 0.82  # construction resilience score
    assert result.iloc[0]["blue_collar_safe_pct"] == 1.0

def test_compute_resilience_mixed():
    """County with mixed occupations should have moderate resilience."""
    data = {var: [0] for var in C24010_VARIABLES}
    data["C24010_001E"] = [1000]
    data["C24010_032E"] = [250]   # construction male (0.82 resilience)
    data["C24010_029E"] = [250]   # office admin male (0.20 resilience)
    data["C24010_065E"] = [250]   # office admin female (0.20 resilience)
    data["C24010_017E"] = [250]   # health practitioners male (0.90 resilience)
    data["fips"] = ["06067"]
    data["county"] = ["Sacramento"]
    data["state"] = ["CA"]
    data["year"] = [2023]

    df = pd.DataFrame(data)
    result = compute_resilience_index(df)

    # Expected: (250*0.82 + 500*0.20 + 250*0.90) / 1000 = 0.53
    assert abs(result.iloc[0]["resilience_index"] - 0.53) < 0.01

def test_job_stability():
    """Employment growth should be computed correctly."""
    latest = pd.DataFrame({
        "fips": ["06067"], "total_employed": [1100],
        "blue_collar_safe_pct": [0.30],
    })
    earliest = pd.DataFrame({
        "fips": ["06067"], "total_employed": [1000],
        "blue_collar_safe_pct": [0.25],
    })
    result = compute_job_stability(latest, earliest)
    assert abs(result.iloc[0]["employment_growth"] - 0.10) < 0.001
    assert abs(result.iloc[0]["blue_collar_trend"] - 0.05) < 0.001
