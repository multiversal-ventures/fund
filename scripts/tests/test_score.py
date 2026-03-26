import pytest
import pandas as pd
from score import (
    normalize_signal,
    apply_hard_filters,
    score_markets,
    score_deals,
    combine_scores,
)


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


def test_apply_hard_filters():
    census = pd.DataFrame({
        "fips": ["01001", "01002", "01003"],
        "pop": [1500000, 2100, 50000],
        "total_units": [600000, 800, 20000],
        "renter_occupied": [260000, 300, 8000],
    })
    occupation = pd.DataFrame({
        "fips": ["01001", "01002", "01003"],
        "total_employed": [750000, 1000, 20000],
    })
    config = {
        "hard_filters": {
            "min_population": 20000,
            "min_housing_units": 10000,
            "min_renter_households": 5000,
            "min_employed": 10000,
        }
    }
    result = apply_hard_filters(census, occupation, config)
    assert "01001" in result  # big county passes
    assert "01002" not in result  # tiny county filtered
    assert "01003" in result  # medium county passes


def test_score_markets():
    census_latest = pd.DataFrame({
        "fips": ["06067", "12105"],
        "county": ["Sacramento", "Polk"],
        "state": ["CA", "FL"],
        "vacancy_rate": [0.07, 0.04],
        "median_rent": [1400, 1200],
        "rent_to_cost_ratio": [0.90, 1.15],
        "pop": [1550000, 780000],
        "total_units": [500000, 300000],
    })
    census_earliest = pd.DataFrame({
        "fips": ["06067", "12105"],
        "vacancy_rate": [0.05, 0.06],
        "median_rent": [1200, 1000],
        "pop": [1450000, 720000],
    })
    occupation = pd.DataFrame({
        "fips": ["06067", "12105"],
        "resilience_index": [0.45, 0.72],
        "total_employed": [700000, 350000],
    })
    permits = pd.DataFrame({
        "fips": ["06067", "12105"],
        "mf_units_permitted": [3000, 1000],
    })
    cbp = pd.DataFrame({
        "fips": ["06067", "12105"],
        "hhi": [800, 400],
    })
    config = {
        "market_weights": {
            "vacancy_trend": 20,
            "rent_growth": 15,
            "rent_cost_ratio": 15,
            "workforce_resilience": 20,
            "employment_concentration": 10,
            "pop_growth": 10,
            "supply_pressure": 10,
        }
    }
    result = score_markets(census_latest, census_earliest, occupation, permits, cbp, config)
    assert "market_score" in result.columns
    assert len(result) == 2
    polk = result[result["fips"] == "12105"].iloc[0]
    sacto = result[result["fips"] == "06067"].iloc[0]
    # Polk has better vacancy trend (dropping), better rent/cost, better resilience
    assert polk["market_score"] > sacto["market_score"]


def test_score_deals():
    hud_fha = pd.DataFrame({
        "fips": ["12105", "12105", "06067"],
        "property_name": ["Alpha", "Beta", "Gamma"],
        "address": ["1 A St", "2 B St", "3 C St"],
        "units": [200, 80, 150],
        "maturity_years": [1.0, 4.5, 3.0],
        "section8": [True, False, False],
        "lat": [27.95, 27.96, 38.58],
        "lng": [-81.70, -81.71, -121.49],
    })
    market_scores = pd.DataFrame({
        "fips": ["12105", "06067"],
        "market_score": [72.0, 50.0],
    })
    census_latest = pd.DataFrame({
        "fips": ["12105", "06067"],
        "vacancy_rate": [0.06, 0.08],
    })
    config = {
        "deal_weights": {
            "mortgage_maturity": 40,
            "unit_count": 20,
            "section8": 20,
            "area_vacancy": 20,
        },
        "deal_min_market_score": 40,
    }
    result = score_deals(hud_fha, market_scores, census_latest, config)
    assert "deal_score" in result.columns
    assert "market_score" in result.columns
    # Alpha: close maturity + section8 + large units → should score highest
    alpha = result[result["property_name"] == "Alpha"].iloc[0]
    beta = result[result["property_name"] == "Beta"].iloc[0]
    gamma = result[result["property_name"] == "Gamma"].iloc[0]
    assert alpha["deal_score"] > beta["deal_score"]
    assert alpha["deal_score"] > gamma["deal_score"]


def test_combine_scores():
    market_scores = pd.DataFrame({
        "fips": ["12105"],
        "market_score": [70.0],
    })
    deal_scores = pd.DataFrame({
        "fips": ["12105"],
        "property_name": ["Alpha"],
        "deal_score": [80.0],
        "market_score": [70.0],
        "lat": [27.95],
        "lng": [-81.70],
    })
    config = {
        "market_deal_split": {
            "market": 60,
            "deal": 40,
        }
    }
    result = combine_scores(market_scores, deal_scores, config)
    assert "total_score" in result.columns
    expected = (60 * 70 + 40 * 80) / 100  # 74.0
    assert result.iloc[0]["total_score"] == expected
