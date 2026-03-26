# scripts/zillow.py
"""Generate Zillow deep-link URLs from county FIPS or lat/lng coordinates."""
import json
import pandas as pd
from urllib.parse import quote

COUNTY_CENTROIDS = {
    "06067": {"lat": 38.45, "lng": -121.34, "south": 38.02, "north": 38.74, "west": -121.86, "east": -120.99},
    "06065": {"lat": 33.74, "lng": -116.17, "south": 33.42, "north": 34.08, "west": -117.67, "east": -114.43},
    "06071": {"lat": 34.84, "lng": -116.18, "south": 34.03, "north": 35.81, "west": -117.65, "east": -114.13},
    "06019": {"lat": 36.76, "lng": -119.65, "south": 36.39, "north": 37.27, "west": -120.32, "east": -118.36},
    "06029": {"lat": 35.35, "lng": -118.73, "south": 34.79, "north": 35.79, "west": -119.86, "east": -117.63},
    "12105": {"lat": 27.95, "lng": -81.70, "south": 27.64, "north": 28.26, "west": -82.11, "east": -81.19},
    "12083": {"lat": 29.21, "lng": -82.07, "south": 28.85, "north": 29.48, "west": -82.66, "east": -81.43},
    "12101": {"lat": 28.32, "lng": -82.46, "south": 28.13, "north": 28.52, "west": -82.90, "east": -82.05},
    "12115": {"lat": 27.18, "lng": -82.36, "south": 26.94, "north": 27.46, "west": -82.85, "east": -82.02},
    "12097": {"lat": 28.07, "lng": -81.16, "south": 27.82, "north": 28.31, "west": -81.66, "east": -80.85},
    "04013": {"lat": 33.35, "lng": -112.49, "south": 32.51, "north": 34.04, "west": -113.33, "east": -111.04},
}


def county_bounds(fips: str) -> dict | None:
    return COUNTY_CENTROIDS.get(fips)


def build_zillow_url(
    south: float = None, north: float = None,
    west: float = None, east: float = None,
    lat: float = None, lng: float = None,
    radius_deg: float = 0.15,
    zoom: int = 11,
) -> str:
    if lat is not None and lng is not None:
        south = lat - radius_deg
        north = lat + radius_deg
        west = lng - radius_deg
        east = lng + radius_deg
        if radius_deg <= 0.02:
            zoom = 16
        elif radius_deg <= 0.05:
            zoom = 14

    search_state = {
        "pagination": {},
        "isMapVisible": True,
        "mapBounds": {
            "west": west,
            "east": east,
            "south": south,
            "north": north,
        },
        "filterState": {
            "sort": {"value": "globalrelevanceex"},
        },
        "isListVisible": True,
        "mapZoom": zoom,
    }

    encoded = quote(json.dumps(search_state, separators=(",", ":")))
    return f"https://www.zillow.com/homes/for_sale/?searchQueryState={encoded}"


def add_zillow_urls(df, fips_col: str = "fips", lat_col: str = "lat", lng_col: str = "lng") -> list[str]:
    urls = []
    for _, row in df.iterrows():
        if lat_col in row.index and lng_col in row.index and pd.notna(row.get(lat_col)) and pd.notna(row.get(lng_col)):
            url = build_zillow_url(lat=float(row[lat_col]), lng=float(row[lng_col]), radius_deg=0.01)
        elif fips_col in row.index:
            bounds = county_bounds(str(row[fips_col]))
            if bounds:
                url = build_zillow_url(**{k: v for k, v in bounds.items() if k in ("south", "north", "west", "east")})
            else:
                url = ""
        else:
            url = ""
        urls.append(url)
    return urls
