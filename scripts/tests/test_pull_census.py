import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from pull_census import fetch_acs_year, build_census_url, parse_census_response

def test_build_census_url():
    url = build_census_url(2023, ["B25001_001E", "B25002_002E"])
    assert "api.census.gov" in url
    assert "2023" in url
    assert "B25001_001E" in url
    assert "county:*" in url

def test_parse_census_response():
    raw = [
        ["NAME", "B25001_001E", "B25002_002E", "state", "county"],
        ["Los Angeles County, California", "3500000", "3200000", "06", "037"],
        ["Kern County, California", "300000", "270000", "06", "029"],
    ]
    df = parse_census_response(raw, 2023)
    assert len(df) == 2
    assert "fips" in df.columns
    assert "year" in df.columns
    assert df.iloc[0]["fips"] == "06037"
    assert df.iloc[0]["year"] == 2023

def test_fetch_acs_year_parquet_schema(tmp_path):
    mock_response = [
        ["NAME", "B25001_001E", "B25002_002E", "B25002_003E",
         "B25003_002E", "B25003_003E", "B25004_002E",
         "B25024_007E", "B25024_008E", "B25024_009E", "B25024_010E", "B25024_011E",
         "B25064_001E", "B25077_001E", "B25105_001E", "B01003_001E",
         "state", "county"],
        ["Test County, State", "100000", "90000", "10000",
         "50000", "40000", "3000",
         "2000", "3000", "4000", "5000", "6000",
         "1500", "350000", "1600", "500000",
         "06", "001"],
    ]
    with patch("pull_census.requests.get") as mock_get:
        mock_resp = MagicMock()
        mock_resp.json.return_value = mock_response
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        output = tmp_path / "acs_2023.parquet"
        df = fetch_acs_year(2023, output_path=str(output), api_key="test")

        assert output.exists()
        schema_cols = {"fips", "county", "state", "total_units", "occupied",
                       "vacant", "owner_occupied", "renter_occupied", "for_rent_vacant",
                       "median_rent", "median_home_value", "median_owner_cost",
                       "mf_units", "mf_pct", "pop", "vacancy_rate",
                       "rental_vac_rate", "rent_to_cost_ratio", "year"}
        assert schema_cols.issubset(set(df.columns))
