/**
 * DC Markets tab — top counties + stats from `dc_market_scores`.
 */
function dcExplorerImport(url) {
  const v =
    typeof window !== 'undefined' && window.__explorerAssetV
      ? String(window.__explorerAssetV)
      : '1';
  return import(`${url}?v=${encodeURIComponent(v)}`);
}

function esc(s) {
  if (s == null) return '';
  return String(s)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

/** @param {*} conn */
export async function refreshDcDashboard(conn) {
  if (!conn) return;

  const { runQuery } = await dcExplorerImport('/js/explorer/duckdb.js');

  const topEl = document.getElementById('dc-stat-top');
  const subEl = document.getElementById('dc-stat-top-sub');
  const nEl = document.getElementById('dc-stat-n');
  const tableMount = document.getElementById('dc-top-markets');

  try {
    const n = await runQuery(conn, 'SELECT COUNT(*)::BIGINT AS n FROM dc_market_scores');
    const top = await runQuery(
      conn,
      `SELECT county, state, dc_market_score FROM dc_market_scores
       ORDER BY dc_market_score DESC NULLS LAST LIMIT 1`,
    );
    const top30 = await runQuery(
      conn,
      `SELECT fips, county, state, dc_market_score, s_electrical, s_political, s_pipeline
       FROM dc_market_scores ORDER BY dc_market_score DESC NULLS LAST LIMIT 25`,
    );

    if (nEl && n[0]) nEl.textContent = String(n[0].n ?? '—');
    if (topEl && top[0]) {
      topEl.textContent =
        top[0].dc_market_score != null ? Number(top[0].dc_market_score).toFixed(1) : '—';
    }
    if (subEl && top[0]) {
      subEl.textContent = top[0].county && top[0].state ? `${top[0].county}, ${top[0].state}` : '—';
    }

    if (tableMount && top30?.length) {
      const rows = top30
        .map(
          (r) =>
            `<tr><td>${esc(r.county)}, ${esc(r.state)}</td><td>${esc(r.fips)}</td><td class="stat-green">${esc(r.dc_market_score)}</td><td>${esc(r.s_electrical)}</td><td>${esc(r.s_political)}</td><td>${esc(r.s_pipeline)}</td></tr>`,
        )
        .join('');
      tableMount.innerHTML = `<table class="dashboard-mini-table"><thead><tr><th>County</th><th>FIPS</th><th>DC score</th><th>Electrical</th><th>Political</th><th>Pipeline</th></tr></thead><tbody>${rows}</tbody></table>`;
    } else if (tableMount) {
      tableMount.innerHTML =
        '<p class="dc-empty-hint">No <code>dc_market_scores</code> table — run the DC pipeline and upload Parquet to Storage.</p>';
    }
  } catch (e) {
    if (topEl) topEl.textContent = '—';
    if (subEl) subEl.textContent = '—';
    if (nEl) nEl.textContent = '—';
    if (tableMount) {
      tableMount.innerHTML = `<p class="dc-empty-hint">${esc(e?.message || e)}</p>`;
    }
  }
}
