# scripts/tests/test_pull_permits.py
import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from pull_permits import build_permits_url, parse_permits_response


def test_build_permits_url():
    url = build_permits_url(2023, api_key="TESTKEY")
    assert "api.census.gov" in url
    assert "bps" in url
    assert "2023" in url
    assert "county" in url
    assert "TESTKEY" in url


def test_build_permits_url_no_key():
    url = build_permits_url(2022, api_key=None)
    assert "api.census.gov" in url
    assert "bps" in url
    assert "2022" in url


def test_parse_permits_response():
    raw = [
        ["BLDGS", "UNITS", "state", "county", "time"],
        ["150", "200", "06", "037", "2023"],
        ["80", "95", "48", "201", "2023"],
    ]
    df = parse_permits_response(raw, 2023)
    assert len(df) == 2
    assert "fips" in df.columns
    assert "state" in df.columns
    assert "year" in df.columns
    assert "total_permits" in df.columns
    assert "total_units_permitted" in df.columns
    assert df.iloc[0]["fips"] == "06037"
    assert df.iloc[0]["state"] == "CA"
    assert df.iloc[0]["year"] == 2023
    assert df.iloc[0]["total_permits"] == 150
    assert df.iloc[0]["total_units_permitted"] == 200
    # Without structure-type columns, mf fields should be NA/NaN
    assert pd.isna(df.iloc[0]["mf_permits"])
    assert pd.isna(df.iloc[0]["mf_units_permitted"])
    assert pd.isna(df.iloc[0]["sf_permits"])
    assert pd.isna(df.iloc[0]["mf_pct"])


def test_parse_permits_with_structure_type():
    raw = [
        ["BLDGS", "UNITS", "BLDGS_5PLUS", "UNITS_5PLUS", "state", "county", "time"],
        ["200", "350", "20", "180", "06", "037", "2022"],
        ["100", "120", "5", "40", "36", "061", "2022"],
    ]
    df = parse_permits_response(raw, 2022)
    assert len(df) == 2

    row = df.iloc[0]
    assert row["fips"] == "06037"
    assert row["total_permits"] == 200
    assert row["total_units_permitted"] == 350
    assert row["mf_permits"] == 20
    assert row["mf_units_permitted"] == 180
    # sf_permits = total_permits - mf_permits
    assert row["sf_permits"] == 180
    # mf_pct = mf_units_permitted / total_units_permitted
    assert abs(row["mf_pct"] - round(180 / 350, 4)) < 1e-6

    row2 = df.iloc[1]
    assert row2["fips"] == "36061"
    assert row2["state"] == "NY"
    assert row2["mf_permits"] == 5
    assert row2["sf_permits"] == 95
    assert abs(row2["mf_pct"] - round(40 / 120, 4)) < 1e-6
