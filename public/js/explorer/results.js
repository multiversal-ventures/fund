/**
 * Shared query result rendering for Dashboard + SQL Studio.
 */
import { buildZillowUrl } from '/js/explorer/zillow.js';

let _lastResult = null;
let _lastSql = '';

export function getLastResult() {
  return _lastResult;
}

export function getLastSql() {
  return _lastSql;
}

/**
 * @param {*} result DuckDB query result
 * @param {{ sql?: string, countEl?: HTMLElement | null, thead?: HTMLElement | null, tbody?: HTMLElement | null }} [opts]
 */
export function renderResultsTable(result, opts = {}) {
  _lastResult = result;
  if (opts.sql != null) _lastSql = opts.sql;

  const countEl = opts.countEl ?? document.getElementById('result-count');
  const thead = opts.thead ?? document.querySelector('#results-table thead');
  const tbody = opts.tbody ?? document.querySelector('#results-table tbody');
  if (!thead || !tbody) return;

  const schema = result.schema.fields.map((f) => f.name);
  const hasFips = schema.includes('fips');
  const hasZillow = schema.includes('zillow_url');
  const hasLat = schema.includes('lat');
  const hasLng = schema.includes('lng');
  const hasCounty = schema.includes('county');
  const hasState = schema.includes('state');

  const rows = result.toArray().map((r) => {
    const obj = {};
    schema.forEach((col) => {
      obj[col] = r[col];
    });
    if (hasFips && !hasZillow) {
      obj._zillow = buildZillowUrl(
        obj.fips,
        hasLat ? obj.lat : null,
        hasLng ? obj.lng : null,
        hasCounty ? obj.county : null,
        hasState ? obj.state : null,
      );
    }
    return obj;
  });

  const displayCols = hasZillow ? schema : hasFips ? [...schema, '_zillow'] : schema;

  if (countEl) countEl.textContent = `${rows.length} rows`;

  thead.innerHTML =
    '<tr>' +
    displayCols.map((col) => `<th>${col === '_zillow' ? 'Zillow' : col}</th>`).join('') +
    '</tr>';

  tbody.innerHTML = rows
    .map(
      (row) =>
        '<tr>' +
        displayCols
          .map((col) => {
            let val = row[col];
            if ((col === 'zillow_url' || col === '_zillow') && val) {
              return `<td><a class="zillow-link" href="${val}" target="_blank" rel="noopener">View on Zillow</a></td>`;
            }
            if (col === 'total_score' && typeof val === 'number') {
              const cls = val >= 70 ? 'score-high' : val >= 50 ? 'score-mid' : 'score-low';
              return `<td><span class="score-badge ${cls}">${val.toFixed(1)}</span></td>`;
            }
            if (col === 'stability_score' && typeof val === 'number') {
              const cls = val >= 70 ? 'score-high' : val >= 40 ? 'score-mid' : 'score-low';
              return `<td><span class="score-badge ${cls}">${val.toFixed(0)}</span></td>`;
            }
            if (col === 'market_score' && typeof val === 'number') {
              const cls = val >= 65 ? 'score-high' : val >= 45 ? 'score-mid' : 'score-low';
              return `<td><span class="score-badge ${cls}">${val.toFixed(1)}</span></td>`;
            }
            if (col === 'deal_score' && typeof val === 'number') {
              const cls = val >= 65 ? 'score-high' : val >= 45 ? 'score-mid' : 'score-low';
              return `<td><span class="score-badge ${cls}">${val.toFixed(1)}</span></td>`;
            }
            if (col === 'resilience_index' && typeof val === 'number') {
              const cls = val >= 0.6 ? 'score-high' : val >= 0.45 ? 'score-mid' : 'score-low';
              return `<td><span class="score-badge ${cls}">${val.toFixed(3)}</span></td>`;
            }
            if (typeof val === 'number') {
              val = Number.isInteger(val) ? val.toLocaleString() : val.toFixed(2);
            }
            return `<td>${val ?? ''}</td>`;
          })
          .join('') +
        '</tr>',
    )
    .join('');
}

/**
 * @param {string} message
 * @param {{ tbody?: HTMLElement | null, thead?: HTMLElement | null, countEl?: HTMLElement | null }} [opts]
 */
export function renderQueryError(message, opts = {}) {
  if (opts.countEl) opts.countEl.textContent = 'Error';
  const tbody = opts.tbody ?? document.querySelector('#results-table tbody');
  const thead = opts.thead ?? document.querySelector('#results-table thead');
  if (thead) thead.innerHTML = '';
  if (tbody) {
    tbody.innerHTML = `<tr><td style="color:#f87171; padding:16px;">${String(message)}</td></tr>`;
  }
}

/**
 * Dashboard charts only — uses #chart-scores / #chart-states.
 * @param {*} result
 */
export function renderCharts(result) {
  const schema = result.schema.fields.map((f) => f.name);
  const rows = result.toArray().map((r) => {
    const obj = {};
    schema.forEach((col) => {
      obj[col] = r[col];
    });
    return obj;
  });

  const scoreCol = ['total_score', 'market_score', 'deal_score'].find((c) => schema.includes(c));
  if (scoreCol) {
    const buckets = { '90-100': 0, '80-90': 0, '70-80': 0, '60-70': 0, '50-60': 0, '<50': 0 };
    rows.forEach((r) => {
      const s = r[scoreCol] || 0;
      if (s >= 90) buckets['90-100']++;
      else if (s >= 80) buckets['80-90']++;
      else if (s >= 70) buckets['70-80']++;
      else if (s >= 60) buckets['60-70']++;
      else if (s >= 50) buckets['50-60']++;
      else buckets['<50']++;
    });
    const max = Math.max(...Object.values(buckets), 1);
    const el = document.getElementById('chart-scores');
    if (el) {
      el.innerHTML = Object.entries(buckets)
        .map(
          ([k, v]) =>
            `<div class="bar"><span class="bar-label">${k}</span><div class="bar-fill" style="width:${(v / max) * 200}px"></div><span class="bar-count">${v}</span></div>`,
        )
        .join('');
    }
  }

  if (schema.includes('state')) {
    const states = {};
    rows.forEach((r) => {
      states[r.state] = (states[r.state] || 0) + 1;
    });
    const sorted = Object.entries(states)
      .sort((a, b) => b[1] - a[1])
      .slice(0, 10);
    const max = sorted[0]?.[1] || 1;
    const el = document.getElementById('chart-states');
    if (el) {
      el.innerHTML = sorted
        .map(
          ([st, cnt]) =>
            `<div class="bar"><span class="bar-label">${st}</span><div class="bar-fill" style="width:${(cnt / max) * 200}px"></div><span class="bar-count">${cnt}</span></div>`,
        )
        .join('');
    }
  }
}

export function downloadBlob(data, filename, type) {
  const blob = new Blob([data], { type });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = filename;
  a.click();
  URL.revokeObjectURL(url);
}

export function exportLastResultCsv() {
  if (!_lastResult) return;
  const schema = _lastResult.schema.fields.map((f) => f.name);
  const rows = _lastResult.toArray();
  let csv = schema.join(',') + '\n';
  rows.forEach((r) => {
    csv +=
      schema
        .map((col) => {
          let v = r[col];
          if (typeof v === 'string' && v.includes(',')) v = `"${v}"`;
          return v ?? '';
        })
        .join(',') + '\n';
  });
  downloadBlob(csv, 'fund-export.csv', 'text/csv');
}

/**
 * @param {*} conn
 * @param {*} db
 */
export async function exportLastResultParquet(conn, db) {
  if (!conn || !db || !_lastSql) return;
  await conn.query(`COPY (${_lastSql}) TO 'export.parquet' (FORMAT PARQUET)`);
  const buf = await db.copyFileToBuffer('export.parquet');
  downloadBlob(buf, 'fund-export.parquet', 'application/octet-stream');
}
