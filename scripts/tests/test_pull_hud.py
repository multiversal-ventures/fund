import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from pull_hud import parse_fha_response, parse_usps_vacancy, build_fha_url

def test_build_fha_url():
    url = build_fha_url(limit=10, offset=0)
    assert "arcgis.com" in url
    assert "resultRecordCount=10" in url

def test_parse_fha_response():
    raw = [
        {
            "PROPERTY_NAME_TEXT": "Sunset Apartments",
            "ADDRESS_LINE1_TEXT": "123 Main St",
            "PLACED_BASE_CITY_NAME_TEXT": "Sacramento",
            "STD_ZIP5": "95814",
            "TOTAL_UNIT_COUNT": 120,
            "PRIMARY_FHA_NUMBER": "12345",
            "LOAN_MATURITY_DATE": 1813276800000,  # ~2027-06-15 epoch ms
            "SOA_NAME1": "Section 8",
            "LAT": 38.58,
            "LON": -121.49,
            "STATE2KX": "06",
            "COUNTY_LEVEL": "06067",
            "PROPERTY_CATEGORY_NAME": "Insured-Subsidized",
            "TOTAL_ASSISTED_UNIT_COUNT": 120,
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
