import pytest
import pandas as pd
from pull_cbp import compute_hhi, build_cbp_url, parse_cbp_response


def test_compute_hhi_diversified():
    """10 equal sectors (each 10% share) => HHI = 10 * (10**2) = 1000."""
    shares = [0.1] * 10
    assert compute_hhi(shares) == pytest.approx(1000.0)


def test_compute_hhi_concentrated():
    """[0.9, 0.05, 0.05] => HHI = 90**2 + 5**2 + 5**2 = 8100 + 25 + 25 = 8150."""
    shares = [0.9, 0.05, 0.05]
    assert compute_hhi(shares) == pytest.approx(8150.0)


def test_compute_hhi_monopoly():
    """Single sector with 100% share => HHI = 10000."""
    shares = [1.0]
    assert compute_hhi(shares) == pytest.approx(10000.0)


def test_parse_cbp_response():
    """Parse mock CBP data: one row per county, correct top sector."""
    # Mock raw API response: header + data rows
    # Columns: EMP, NAICS2017, NAICS2017_LABEL, state, county
    raw = [
        ["EMP", "NAICS2017", "NAICS2017_LABEL", "state", "county"],
        # County 06001 - sector 31 (Manufacturing) dominates
        ["1000", "31", "Manufacturing", "06", "001"],
        ["200",  "44", "Retail Trade", "06", "001"],
        ["100",  "72", "Accommodation and Food Services", "06", "001"],
        # County 06003 - sector 62 (Health Care) dominates
        ["500",  "62", "Health Care and Social Assistance", "06", "003"],
        ["300",  "52", "Finance and Insurance", "06", "003"],
        # These should be filtered out (not 2-digit sectors)
        ["9999", "00", "Total", "06", "001"],
        ["500",  "311", "Food Manufacturing", "06", "001"],
        ["200",  "311311", "Sugarcane Mills", "06", "001"],
    ]

    df = parse_cbp_response(raw, 2021)

    # Should have exactly one row per county
    assert len(df) == 2

    # Check required columns
    required_cols = {"fips", "state", "year", "total_employment", "hhi",
                     "top_sector_name", "top_sector_share", "top3_share", "num_sectors"}
    assert required_cols.issubset(set(df.columns))

    # Check county 06001
    row_001 = df[df["fips"] == "06001"].iloc[0]
    assert row_001["state"] == "CA"
    assert row_001["year"] == 2021
    assert row_001["top_sector_name"] == "Manufacturing"
    assert row_001["num_sectors"] == 3
    assert row_001["total_employment"] == pytest.approx(1300.0)

    # Check county 06003
    row_003 = df[df["fips"] == "06003"].iloc[0]
    assert row_003["top_sector_name"] == "Health Care and Social Assistance"
    assert row_003["num_sectors"] == 2
