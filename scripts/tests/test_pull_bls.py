import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from pull_bls import parse_oews_data, build_oews_url

def test_build_oews_url():
    url = build_oews_url(2024)
    assert "bls.gov" in url
    assert "oesm24" in url.lower() or "2024" in url

def test_parse_oews_data():
    raw_df = pd.DataFrame({
        "AREA": ["31080", "40140"],
        "AREA_TITLE": ["Los Angeles-Long Beach-Anaheim, CA", "Riverside-San Bernardino-Ontario, CA"],
        "OCC_CODE": ["47-2111", "47-2111"],
        "OCC_TITLE": ["Electricians", "Electricians"],
        "TOT_EMP": ["21070", "7570"],
        "H_MEDIAN": ["35.12", "30.50"],
        "A_MEDIAN": ["73050", "63440"],
        "LOC_QUOTIENT": ["1.15", "0.98"],
    })
    df = parse_oews_data(raw_df, 2024)
    assert len(df) == 2
    assert "year" in df.columns
    assert "metro_code" in df.columns
    assert df.iloc[0]["total_employment"] == 21070
