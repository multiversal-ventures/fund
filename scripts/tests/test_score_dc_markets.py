# scripts/tests/test_score_dc_markets.py
"""Tests for DC Tier 1 scoring (no network)."""
from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

import sys

_scripts = Path(__file__).resolve().parent.parent
_dc = _scripts / "dc"
for _p in (_scripts, _dc):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

from score_dc_markets import score_dc_markets  # noqa: E402


@pytest.fixture
def weights(tmp_path):
    w = {
        "electrical": 30,
        "water_cooling": 10,
        "political": 20,
        "pipeline": 15,
        "connectivity": 10,
        "labor_cost": 10,
        "unique": 5,
        "penalty_max": 10,
        "screen": {"min_population": 5000, "exclude_state_fips_prefix": []},
    }
    p = tmp_path / "w.json"
    p.write_text(json.dumps(w), encoding="utf-8")
    return w


def test_score_dc_markets_shape_and_bounds(tmp_path, weights):
    cen = pd.DataFrame(
        {
            "fips": ["01001", "01003", "06037"],
            "county": ["A", "B", "C"],
            "state": ["AL", "AL", "CA"],
            "pop": [10000, 20000, 10_000_000],
            "total_units": [5000, 12000, 4_000_000],
        }
    )
    cen_p = tmp_path / "acs.parquet"
    cen.to_parquet(cen_p, index=False)

    cbp = pd.DataFrame(
        {
            "fips": ["01001", "01003", "06037"],
            "naics518_emp": [10, 0, 50000],
        }
    )
    cbp_p = tmp_path / "cbp.parquet"
    cbp.to_parquet(cbp_p, index=False)

    eia = pd.DataFrame(
        {
            "state_abbr": ["AL", "CA"],
            "industrial_cents_kwh": [10.0, 17.0],
        }
    )

    tv = pd.DataFrame(
        {
            "state_abbr": ["AL", "CA"],
            "tavily_political_score": [0.8, 0.4],
            "tavily_penalty": [0.0, 2.0],
        }
    )
    tv_p = tmp_path / "tv.parquet"
    tv.to_parquet(tv_p, index=False)

    out = score_dc_markets(cen_p, cbp_p, eia, tv_p, None, weights)

    assert len(out) == 3
    assert out["dc_eligible"].all()
    assert out["dc_market_score"].max() <= 100
    assert out["dc_market_score"].min() >= 0
    assert "s_electrical" in out.columns
    assert "zillow_url" in out.columns
    by_fips = out.set_index(out["fips"].astype(str).str.zfill(5))["zillow_url"]
    assert by_fips["06037"].startswith("https://www.zillow.com/")
    assert by_fips["01001"].startswith("https://www.zillow.com/")
    assert "06037" in out["fips"].values


def test_dc_screen_excludes_puerto_rico(tmp_path):
    w = {
        "electrical": 30,
        "water_cooling": 10,
        "political": 20,
        "pipeline": 15,
        "connectivity": 10,
        "labor_cost": 10,
        "unique": 5,
        "penalty_max": 10,
        "screen": {
            "min_population": 50000,
            "min_housing_units": 20000,
            "exclude_state_fips_prefix": ["60", "66", "69", "72", "78"],
        },
    }
    cen = pd.DataFrame(
        {
            "fips": ["72001", "06037"],
            "county": ["Adjuntas", "Los Angeles"],
            "state": ["PR", "CA"],
            "pop": [80_000, 10_000_000],
            "total_units": [40_000, 3_500_000],
        }
    )
    cen_p = tmp_path / "acs2.parquet"
    cen.to_parquet(cen_p, index=False)
    cbp = pd.DataFrame({"fips": ["72001", "06037"], "naics518_emp": [0, 1000]})
    cbp_p = tmp_path / "cbp2.parquet"
    cbp.to_parquet(cbp_p, index=False)
    eia = pd.DataFrame({"state_abbr": ["PR", "CA"], "industrial_cents_kwh": [8.0, 17.0]})
    tv = pd.DataFrame(
        {"state_abbr": ["PR", "CA"], "tavily_political_score": [0.9, 0.5], "tavily_penalty": [0.0, 0.0]}
    )
    tv_p = tmp_path / "tv2.parquet"
    tv.to_parquet(tv_p, index=False)

    out = score_dc_markets(cen_p, cbp_p, eia, tv_p, None, w)
    pr = out[out["fips"] == "72001"].iloc[0]
    ca = out[out["fips"] == "06037"].iloc[0]
    assert not bool(pr["dc_eligible"])
    assert pd.isna(pr["dc_market_score"])
    assert bool(ca["dc_eligible"])
    assert not pd.isna(ca["dc_market_score"])
