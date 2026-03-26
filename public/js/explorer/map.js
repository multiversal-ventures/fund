/**
 * Leaflet county map — choropleth joined to DuckDB `market_scores` on normalized FIPS.
 *
 * County GeoJSON: Plotly datasets (US Census-derived), loaded from jsDelivr CDN.
 * Feature `id` is 5-digit county FIPS (e.g. "01001"); join key matches `market_scores.fips`
 * after `normalizeFips()` (pad to 5 digits).
 */
import { runQuery } from '/js/explorer/duckdb.js';
import { buildZillowUrl } from '/js/explorer/zillow.js';
import { MARKET_WEIGHT_META } from '/js/explorer/scenarios.js';

/** CDN GeoJSON — Plotly public datasets (same file referenced in explorer plan). */
export const COUNTY_GEOJSON_URL =
  'https://cdn.jsdelivr.net/gh/plotly/datasets@master/geojson-counties-fips.json';

const MKEY_TO_COL = {
  vacancy: 'vacancy_trend',
  rentgrowth: 'rent_growth',
  rent: 'rent_cost_ratio',
  resilience: 'resilience_index',
  hhi: 'employment_concentration',
  pop: 'pop_growth',
  supply: 'supply_pressure',
};

const COLOR_METRICS = [
  { value: 'market_score', label: 'Market score' },
  { value: 'resilience_index', label: 'Resilience index' },
  { value: 'vacancy_trend', label: 'Vacancy trend' },
  { value: 'rent_growth', label: 'Rent growth' },
  { value: 'rent_cost_ratio', label: 'Rent / cost ratio' },
  { value: 'employment_concentration', label: 'Employment HHI' },
  { value: 'pop_growth', label: 'Pop growth' },
  { value: 'supply_pressure', label: 'Supply pressure' },
];

let _instance = null;

function esc(s) {
  if (s == null) return '';
  return String(s)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

/** @param {unknown} v */
export function normalizeFips(v) {
  if (v == null) return '';
  const s = String(v).trim();
  if (!/^\d+$/.test(s)) return s;
  return s.padStart(5, '0');
}

/**
 * @param {GeoJSON.Feature} feature
 * @returns {string} 5-digit FIPS or ''
 */
export function fipsFromGeoFeature(feature) {
  if (!feature) return '';
  const id = feature.id;
  if (id != null) return normalizeFips(id);
  const p = feature.properties || {};
  if (p.GEOID) return normalizeFips(String(p.GEOID).replace(/^0500000US/i, ''));
  if (p.STATE != null && p.COUNTY != null) {
    return normalizeFips(String(p.STATE) + String(p.COUNTY));
  }
  return '';
}

function lerp(a, b, t) {
  return a + (b - a) * t;
}

/** Choropleth: low → slate, high → emerald/teal (readable on dark basemap). */
function colorForT(t) {
  const r = Math.round(lerp(30, 52, t));
  const g = Math.round(lerp(41, 211, t));
  const b = Math.round(lerp(59, 153, t));
  return `rgb(${r},${g},${b})`;
}

function formatMetricVal(col, v) {
  if (v == null || Number.isNaN(Number(v))) return '—';
  const n = Number(v);
  if (col === 'resilience_index') return n.toFixed(3);
  return n.toFixed(2);
}

/**
 * @param {import('/js/explorer/duckdb.js').runQuery} run
 */
async function loadMarketRows(conn) {
  const result = await runQuery(
    conn,
    `SELECT ms.*, COALESCE(p.cnt, 0)::BIGINT AS deal_count
     FROM market_scores ms
     LEFT JOIN (
       SELECT fips, COUNT(*)::BIGINT AS cnt FROM properties GROUP BY fips
     ) p ON CAST(ms.fips AS VARCHAR) = CAST(p.fips AS VARCHAR)`,
  );
  const byFips = new Map();
  for (const row of result.toArray()) {
    const f = normalizeFips(row.fips);
    if (f) byFips.set(f, row);
  }
  return byFips;
}

function passesFilter(row, filterKey) {
  if (!row) return filterKey === 'all';
  const ms = Number(row.market_score);
  const dc = Number(row.deal_count) || 0;
  switch (filterKey) {
    case 'all':
      return true;
    case 'm40':
      return !Number.isNaN(ms) && ms >= 40;
    case 'm50':
      return !Number.isNaN(ms) && ms >= 50;
    case 'deals':
      return dc > 0;
    default:
      return true;
  }
}

function computeLegendRange(byFips, metric, filterKey) {
  let minV = Infinity;
  let maxV = -Infinity;
  for (const row of byFips.values()) {
    if (!passesFilter(row, filterKey)) continue;
    const v = row[metric];
    if (v == null || Number.isNaN(Number(v))) continue;
    const n = Number(v);
    minV = Math.min(minV, n);
    maxV = Math.max(maxV, n);
  }
  if (!Number.isFinite(minV) || !Number.isFinite(maxV)) {
    return { minV: 0, maxV: 1 };
  }
  if (minV === maxV) {
    return { minV: minV - 0.5, maxV: maxV + 0.5 };
  }
  return { minV, maxV };
}

class ExplorerMap {
  /**
   * @param {*} conn
   */
  constructor(conn) {
    this.conn = conn;
    /** @type {import('leaflet').Map | null} */
    this.map = null;
    /** @type {import('leaflet').GeoJSON | null} */
    this.geoLayer = null;
    this.byFips = new Map();
    this.geoData = null;
    this.selectedFips = null;
    this._colorMetric = 'market_score';
    this._filterKey = 'all';
  }

  async init() {
    const wrap = document.getElementById('map-main-wrap');
    const errEl = document.getElementById('map-geojson-error');
    const el = document.getElementById('explorer-map');
    if (!el || typeof L === 'undefined') return;

    this.populateMetricSelect();
    this.map = L.map(el, { scrollWheelZoom: true }).setView([39.8283, -98.5795], 4);
    L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
      attribution: '&copy; OpenStreetMap contributors &copy; CARTO',
      subdomains: 'abcd',
      maxZoom: 19,
    }).addTo(this.map);

    this.byFips = await loadMarketRows(this.conn);

    try {
      const res = await fetch(COUNTY_GEOJSON_URL);
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      this.geoData = await res.json();
      if (errEl) errEl.style.display = 'none';
      if (wrap) wrap.style.display = '';
    } catch (e) {
      console.warn('County GeoJSON load failed:', e);
      if (errEl) {
        errEl.style.display = 'block';
        const p = errEl.querySelector('.map-error-msg');
        if (p) p.textContent = `Could not load county boundaries (${e.message || 'network error'}).`;
      }
      if (wrap) wrap.style.display = 'none';
      return;
    }

    this.geoLayer = L.geoJSON(this.geoData, {
      style: (feat) => this._styleForFeature(feat),
      onEachFeature: (feat, layer) => this._onEachFeature(feat, layer),
    }).addTo(this.map);

    try {
      this.map.fitBounds(this.geoLayer.getBounds(), { padding: [16, 16] });
    } catch (_) {
      /* empty */
    }

    document.getElementById('map-color-metric')?.addEventListener('change', (e) => {
      this._colorMetric = e.target.value;
      this._restyleAll();
      this._updateLegend();
    });
    document.getElementById('map-filter')?.addEventListener('change', (e) => {
      this._filterKey = e.target.value;
      this._restyleAll();
      this._updateLegend();
    });
    document.getElementById('map-retry-btn')?.addEventListener('click', () => this.retryGeojson());
    document.getElementById('map-panel-close')?.addEventListener('click', () => this.closePanel());

    this._updateLegend();
  }

  populateMetricSelect() {
    const sel = document.getElementById('map-color-metric');
    if (!sel || sel.options.length) return;
    sel.innerHTML = COLOR_METRICS.map(
      (m) => `<option value="${esc(m.value)}">${esc(m.label)}</option>`,
    ).join('');
  }

  _metricValue(row, metric) {
    if (!row) return null;
    const v = row[metric];
    return v == null || Number.isNaN(Number(v)) ? null : Number(v);
  }

  _styleForFeature(feature) {
    const fips = fipsFromGeoFeature(feature);
    const row = this.byFips.get(fips);
    const base = { color: '#475569', weight: 0.6, fillOpacity: 0.85 };

    if (!row) {
      return { ...base, fillColor: '#1e293b' };
    }
    if (!passesFilter(row, this._filterKey)) {
      return { ...base, fillColor: '#0f172a', fillOpacity: 0.35 };
    }

    const { minV, maxV } = computeLegendRange(this.byFips, this._colorMetric, this._filterKey);
    const v = this._metricValue(row, this._colorMetric);
    if (v == null) {
      return { ...base, fillColor: '#334155' };
    }
    const t = maxV > minV ? (v - minV) / (maxV - minV) : 0.5;
    const fillColor = colorForT(Math.min(1, Math.max(0, t)));
    if (this.selectedFips && fips === this.selectedFips) {
      return { ...base, fillColor, color: '#fbbf24', weight: 2 };
    }
    return { ...base, fillColor };
  }

  _restyleAll() {
    if (!this.geoLayer) return;
    this.geoLayer.eachLayer((layer) => {
      if (layer.feature) {
        layer.setStyle(this._styleForFeature(layer.feature));
      }
    });
  }

  _onEachFeature(feature, layer) {
    const fips = fipsFromGeoFeature(feature);
    const row = this.byFips.get(fips);
    const name = feature.properties?.NAME || 'County';

    layer.on({
      mouseover: (e) => {
        const feat = e.target.feature;
        const base = this._styleForFeature(feat);
        e.target.setStyle({ ...base, weight: 2, color: '#93c5fd' });
        e.target.bringToFront();
      },
      mouseout: (e) => {
        e.target.setStyle(this._styleForFeature(e.target.feature));
      },
    });

    const zUrl = row
      ? buildZillowUrl(fips, null, null, row.county || name, row.state || st)
      : '';

    const ms = row?.market_score != null ? Number(row.market_score).toFixed(1) : '—';
    const stateLabel = row?.state ? esc(String(row.state)) : '';
    const tip = `<div class="map-tooltip-inner">
<strong>${esc(name)}${stateLabel ? `, ${stateLabel}` : ''}</strong><br/>
Market score: ${esc(ms)}
${zUrl ? `<br/><a href="${esc(zUrl)}" target="_blank" rel="noopener">View listings on Zillow →</a>` : ''}
</div>`;

    layer.bindTooltip(tip, { sticky: true, direction: 'auto', className: 'map-tooltip-wrap' });

    layer.on('click', () => {
      this.selectedFips = fips;
      this._restyleAll();
      this.openPanel(fips, feature);
    });
  }

  _updateLegend() {
    const el = document.getElementById('map-legend');
    if (!el) return;
    const { minV, maxV } = computeLegendRange(this.byFips, this._colorMetric, this._filterKey);
    const label = COLOR_METRICS.find((m) => m.value === this._colorMetric)?.label || this._colorMetric;
    const shown = [...this.byFips.values()].filter((r) => passesFilter(r, this._filterKey)).length;
    const est =
      typeof window !== 'undefined' &&
      window.__explorerEstimatedPreview === true;
    const estNote = est
      ? `<span class="map-legend-est"> · Weights differ from last pipeline run (Parquet scores unchanged)</span>`
      : '';
    el.innerHTML = `<span class="map-legend-label">${esc(label)}</span>
      <span class="map-legend-scale">${formatMetricVal(this._colorMetric, minV)} … ${formatMetricVal(this._colorMetric, maxV)}</span>
      <span class="map-legend-n">${shown} counties match filter</span>${estNote}`;
  }

  closePanel() {
    this.selectedFips = null;
    const panel = document.getElementById('map-side-panel');
    if (panel) panel.hidden = true;
    this._restyleAll();
  }

  /**
   * @param {string} fips
   * @param {GeoJSON.Feature} feature
   */
  async openPanel(fips, feature) {
    const panel = document.getElementById('map-side-panel');
    const title = document.getElementById('map-panel-title');
    const body = document.getElementById('map-panel-body');
    if (!panel || !title || !body) return;

    const row = this.byFips.get(fips);
    const countyName = row?.county || feature.properties?.NAME || 'County';
    const state = row?.state || '';
    title.textContent = state ? `${countyName}, ${state}` : countyName;

    if (!row) {
      body.innerHTML = '<p class="map-panel-muted">No market data for this county in the model.</p>';
      panel.hidden = false;
      return;
    }

    const zCounty = buildZillowUrl(fips, null, null, row.county, row.state);
    const signalsHtml = MARKET_WEIGHT_META.map((meta) => {
      const col = MKEY_TO_COL[meta.mKey];
      const val = col ? row[col] : null;
      return `<tr>
        <td>${esc(meta.label)} <span class="map-info" title="${esc(meta.explain)}">ⓘ</span></td>
        <td class="map-panel-val">${esc(formatMetricVal(col, val))}</td>
      </tr>`;
    }).join('');

    let dealsHtml = '';
    try {
      const deals = await runQuery(
        this.conn,
        `SELECT property_name, city, state, total_score, market_score, deal_score, lat, lng, county, zillow_url
         FROM properties
         WHERE lpad(cast(fips AS VARCHAR), 5, '0') = ${JSON.stringify(fips)}
         ORDER BY total_score DESC NULLS LAST LIMIT 5`,
      );
      const drows = deals.toArray();
      dealsHtml =
        drows.length === 0
          ? '<p class="map-panel-muted">No scored properties in this county.</p>'
          : `<ul class="map-deal-list">${drows
              .map((d) => {
                let z = d.zillow_url;
                if (!z && fips) z = buildZillowUrl(fips, d.lat, d.lng, d.county, d.state);
                const ts = d.total_score != null ? Number(d.total_score).toFixed(1) : '—';
                const nm = esc(d.property_name || d.city || 'Property');
                const zl = z
                  ? `<a class="zillow-link" href="${esc(z)}" target="_blank" rel="noopener">Zillow</a>`
                  : '';
                return `<li><span class="map-deal-name">${nm}</span> <span class="map-deal-score">${esc(ts)}</span> ${zl}</li>`;
              })
              .join('')}</ul>`;
    } catch (e) {
      dealsHtml = `<p class="map-panel-muted">Could not load deals (${esc(e.message)}).</p>`;
    }

    body.innerHTML = `
      <div class="map-panel-actions">
        ${zCounty ? `<a class="btn btn-sm" href="${esc(zCounty)}" target="_blank" rel="noopener">Browse county on Zillow</a>` : ''}
      </div>
      <h3 class="map-panel-h3">Market signals</h3>
      <table class="map-signals-table">${signalsHtml}</table>
      <h3 class="map-panel-h3">Top deals in county</h3>
      ${dealsHtml}`;

    panel.hidden = false;
  }

  async retryGeojson() {
    const errEl = document.getElementById('map-geojson-error');
    const wrap = document.getElementById('map-main-wrap');
    if (this.map && this.geoLayer) {
      this.map.removeLayer(this.geoLayer);
      this.geoLayer = null;
    }
    try {
      const res = await fetch(COUNTY_GEOJSON_URL);
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      this.geoData = await res.json();
      if (errEl) errEl.style.display = 'none';
      if (wrap) wrap.style.display = '';
      if (!this.map) return;
      this.geoLayer = L.geoJSON(this.geoData, {
        style: (feat) => this._styleForFeature(feat),
        onEachFeature: (feat, layer) => this._onEachFeature(feat, layer),
      }).addTo(this.map);
      this.map.fitBounds(this.geoLayer.getBounds(), { padding: [16, 16] });
      this._restyleAll();
      this._updateLegend();
    } catch (e) {
      if (errEl) {
        errEl.style.display = 'block';
        const p = errEl.querySelector('.map-error-msg');
        if (p) p.textContent = `Could not load county boundaries (${e.message || 'network error'}).`;
      }
      if (wrap) wrap.style.display = 'none';
    }
  }

  invalidateSize() {
    if (this.map) {
      setTimeout(() => this.map.invalidateSize(), 50);
    }
  }
}

/**
 * @param {*} conn DuckDB connection
 */
export async function initExplorerMapIfNeeded(conn) {
  if (!conn || typeof L === 'undefined') return;
  if (_instance) {
    _instance.invalidateSize();
    return;
  }
  const em = new ExplorerMap(conn);
  await em.init();
  _instance = em;
}

export function invalidateExplorerMapSize() {
  _instance?.invalidateSize();
}

/** Re-read legend when global “Estimated” preview flag changes (Task 8). */
export function refreshMapLegendIfAny() {
  _instance?._updateLegend();
}
