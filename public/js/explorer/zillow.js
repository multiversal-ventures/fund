/**
 * Zillow URL helpers for property / county context.
 */

export const COUNTY_BOUNDS = {
  '06067': { s: 38.02, n: 38.74, w: -121.86, e: -120.99 },
  '06065': { s: 33.42, n: 34.08, w: -117.67, e: -114.43 },
  '06071': { s: 34.03, n: 35.81, w: -117.65, e: -114.13 },
  '06019': { s: 36.39, n: 37.27, w: -120.32, e: -118.36 },
  '06029': { s: 34.79, n: 35.79, w: -119.86, e: -117.63 },
  '12105': { s: 27.64, n: 28.26, w: -82.11, e: -81.19 },
  '12083': { s: 28.85, n: 29.48, w: -82.66, e: -81.43 },
  '12101': { s: 28.13, n: 28.52, w: -82.9, e: -82.05 },
  '12115': { s: 26.94, n: 27.46, w: -82.85, e: -82.02 },
  '12097': { s: 27.82, n: 28.31, w: -81.66, e: -80.85 },
  '04013': { s: 32.51, n: 34.04, w: -113.33, e: -111.04 },
};

export function buildZillowUrl(fips, lat, lng, county, state) {
  if (lat != null && lng != null && !Number.isNaN(lat) && !Number.isNaN(lng)) {
    const r = 0.01;
    const bounds = { south: lat - r, north: lat + r, west: lng - r, east: lng + r };
    return `https://www.zillow.com/homes/for_sale/?searchQueryState=${encodeURIComponent(JSON.stringify({ pagination: {}, isMapVisible: true, mapBounds: bounds, filterState: { sort: { value: 'globalrelevanceex' } }, isListVisible: true, mapZoom: 16 }))}`;
  }
  if (fips && COUNTY_BOUNDS[fips]) {
    const b = COUNTY_BOUNDS[fips];
    const bounds = { south: b.s, north: b.n, west: b.w, east: b.e };
    return `https://www.zillow.com/homes/for_sale/?searchQueryState=${encodeURIComponent(JSON.stringify({ pagination: {}, isMapVisible: true, mapBounds: bounds, filterState: { sort: { value: 'globalrelevanceex' } }, isListVisible: true, mapZoom: 11 }))}`;
  }
  if (county && state) {
    const clean = String(county).replace(/ County$/i, '').replace(/ Parish$/i, '').replace(/ Municipio$/i, '').trim();
    return `https://www.zillow.com/homes/${encodeURIComponent(`${clean}-${state}`)}/`;
  }
  return '';
}
