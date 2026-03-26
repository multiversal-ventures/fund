import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from pull_hud import parse_fha_response, parse_usps_vacancy, build_fha_url

def test_build_fha_url():
    url = build_fha_url(limit=10, offset=0)
    assert "data.hud.gov" in url

def test_parse_fha_response():
    raw = [
        {
            "property_name": "Sunset Apartments",
            "property_street": "123 Main St",
            "city_name_text": "Sacramento",
            "state_code": "CA",
            "zip_code": "95814",
            "units_tot_cnt": "120",
            "fha_loan_id": "12345",
            "orig_mortgage_amt": "5000000",
            "maturity_date": "2027-06-15T00:00:00.000",
            "soa_cd_txt": "Section 8",
            "latitude": "38.58",
            "longitude": "-121.49",
            "fips_state_cd": "06",
            "fips_cnty_cd": "067",
        }
    ]
    df = parse_fha_response(raw)
    assert len(df) == 1
    assert df.iloc[0]["fips"] == "06067"
    assert df.iloc[0]["units"] == 120
    assert df.iloc[0]["property_name"] == "Sunset Apartments"
    assert df.iloc[0]["section8"] == True

def test_parse_usps_vacancy():
    raw = [
        {
            "geoid": "06067",
            "year": "2024",
            "quarter": "4",
            "tot_res": "600000",
            "res_vac": "25000",
        }
    ]
    df = parse_usps_vacancy(raw)
    assert len(df) == 1
    assert abs(df.iloc[0]["usps_vacancy_rate"] - 0.0417) < 0.001
