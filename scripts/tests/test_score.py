import pytest
import pandas as pd
from score import score_properties, normalize_signal

def test_normalize_signal_linear():
    values = pd.Series([0, 5, 10])
    result = normalize_signal(values, weight=20, higher_is_better=True)
    assert result.iloc[0] == 0.0
    assert result.iloc[2] == 20.0

def test_normalize_signal_inverse():
    values = pd.Series([1, 3, 5])
    result = normalize_signal(values, weight=20, higher_is_better=False)
    assert result.iloc[0] == 20.0
    assert result.iloc[2] == 0.0

def test_score_properties():
    census = pd.DataFrame({
        "fips": ["06067", "12105"],
        "county": ["Sacramento", "Polk"],
        "state": ["CA", "FL"],
        "vacancy_rate": [0.05, 0.04],
        "rent_to_cost_ratio": [0.95, 1.11],
        "mf_pct": [0.15, 0.10],
        "pop": [1500000, 750000],
        "year": [2023, 2023],
    })
    census_prev = census.copy()
    census_prev["year"] = 2021
    census_prev["vacancy_rate"] = [0.07, 0.06]
    census_prev["pop"] = [1450000, 720000]

    hud = pd.DataFrame({
        "fips": ["06067", "12105"],
        "property_name": ["Sunset Apts", "Lakeland Place"],
        "address": ["123 Main", "456 Oak"],
        "units": [120, 200],
        "mortgage_amount": [5000000, 8000000],
        "maturity_years": [2.0, 1.0],
        "section8": [False, True],
        "lat": [38.58, 27.95],
        "lng": [-121.49, -81.70],
    })

    weights = {
        "mortgage_maturity": 20,
        "vacancy_trend": 25,
        "rent_cost_ratio": 30,
        "area_vacancy": 10,
        "pop_growth": 15,
    }

    result = score_properties(census, census_prev, hud, weights)
    assert "total_score" in result.columns
    assert "signal_rank" in result.columns
    assert "zillow_url" in result.columns
    assert len(result) == 2
    # Polk should score higher (better rent/cost, closer maturity)
    polk = result[result["county"] == "Polk"].iloc[0]
    sacto = result[result["county"] == "Sacramento"].iloc[0]
    assert polk["total_score"] > sacto["total_score"]
