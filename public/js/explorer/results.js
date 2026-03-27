/**
 * Shared query result rendering for Dashboard + SQL Studio.
 */
import { buildZillowUrl } from '/js/explorer/zillow.js?v=20260327-10';

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
