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
    assert out["dc_market_score"].max() <= 100
    assert out["dc_market_score"].min() >= 0
    assert "s_electrical" in out.columns
    assert "06037" in out["fips"].values
