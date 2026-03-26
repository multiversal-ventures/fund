import pytest
import json
from urllib.parse import unquote
from zillow import build_zillow_url, county_bounds, COUNTY_CENTROIDS

def test_build_zillow_url_from_bounds():
    url = build_zillow_url(south=27.6, north=27.9, west=-81.2, east=-80.5)
    assert "zillow.com/homes/for_sale" in url
    assert "searchQueryState" in url
    qs = unquote(url.split("searchQueryState=")[1])
    state = json.loads(qs)
    assert state["mapBounds"]["south"] == 27.6
    assert state["isMapVisible"] == True
    assert state["isListVisible"] == True

def test_build_zillow_url_from_point():
    url = build_zillow_url(lat=38.58, lng=-121.49, radius_deg=0.01)
    qs = unquote(url.split("searchQueryState=")[1])
    state = json.loads(qs)
    assert abs(state["mapBounds"]["south"] - 38.57) < 0.001

def test_county_bounds_known_fips():
    bounds = county_bounds("06067")  # Sacramento
    assert bounds is not None
    assert "south" in bounds
    assert bounds["south"] < bounds["north"]
