import pytest
import pandas as pd
from occupation_resilience import compute_resilience_index, OCCUPATION_RESILIENCE, B24010_VARIABLES

def test_resilience_scores_valid():
    """All resilience scores should be between 0 and 1."""
    for name, info in OCCUPATION_RESILIENCE.items():
        assert 0 <= info["resilience"] <= 1, f"{name} has invalid resilience: {info['resilience']}"

def test_compute_resilience_index():
    """County with all construction workers should have high resilience."""
    data = {var: [0] for var in B24010_VARIABLES}
    data["B24010_001E"] = [1000]  # total employed
    data["B24010_031E"] = [500]   # construction male
    data["B24010_067E"] = [500]   # construction female
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
    data = {var: [0] for var in B24010_VARIABLES}
    data["B24010_001E"] = [1000]
    data["B24010_031E"] = [250]   # construction male (0.82 resilience)
    data["B24010_028E"] = [250]   # office admin male (0.20 resilience)
    data["B24010_064E"] = [250]   # office admin female (0.20 resilience)
    data["B24010_019E"] = [250]   # healthcare practitioners male (0.90 resilience)
    data["fips"] = ["06067"]
    data["county"] = ["Sacramento"]
    data["state"] = ["CA"]
    data["year"] = [2023]

    df = pd.DataFrame(data)
    result = compute_resilience_index(df)

    # Expected: (250*0.82 + 500*0.20 + 250*0.90) / 1000 = 0.53
    assert abs(result.iloc[0]["resilience_index"] - 0.53) < 0.01
