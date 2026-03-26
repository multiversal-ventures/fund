/**
 * Dashboard aggregates: stats, top markets/deals, “How We Score” panel.
 */
import { runQuery as duckQuery } from '/js/explorer/duckdb.js';
import { buildZillowUrl } from '/js/explorer/zillow.js';
import {
  MARKET_WEIGHT_META,
  DEAL_WEIGHT_META,
  SCENARIO_LABELS,
} from '/js/explorer/scenarios.js';

const TOP_N = 6;

function esc(s) {
  if (s == null) return '';
  return String(s)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

function scoreBadgeClass(score, kind) {
  if (kind === 'total') {
    if (score >= 70) return 'score-high';
    if (score >= 50) return 'score-mid';
    return 'score-low';
  }
  if (kind === 'market' || kind === 'deal') {
    if (score >= 65) return 'score-high';
    if (score >= 45) return 'score-mid';
    return 'score-low';
  }
  return 'score-mid';
}

/**
 * @param {*} conn
 */
export async function refreshDashboard(conn) {
  if (!conn) return;

  try {
    const marketsCount = await duckQuery(
      conn,
      'SELECT COUNT(*)::BIGINT AS n FROM market_scores',
    );
    const dealsCount = await duckQuery(conn, 'SELECT COUNT(*)::BIGINT AS n FROM properties');
    const topMarket = await duckQuery(
      conn,
      `SELECT county, state, market_score FROM market_scores ORDER BY market_score DESC NULLS LAST LIMIT 1`,
    );

    const mc = marketsCount.toArray()[0]?.n;
    const dc = dealsCount.toArray()[0]?.n;
    const mcStr = mc != null ? String(Number(mc)) : '—';
    const dcStr = dc != null ? String(Number(dc)) : '—';
    const tm = topMarket.toArray()[0];

    const elN = (id) => document.getElementById(id);
    if (elN('dashboard-stat-markets')) elN('dashboard-stat-markets').textContent = mcStr;
    if (elN('dashboard-stat-deals')) elN('dashboard-stat-deals').textContent = dcStr;
    if (elN('dashboard-stat-top-ms')) {
      elN('dashboard-stat-top-ms').textContent = tm?.market_score != null ? Number(tm.market_score).toFixed(1) : '—';
    }
    if (elN('dashboard-stat-top-ms-sub')) {
      elN('dashboard-stat-top-ms-sub').textContent =
        tm?.county && tm?.state ? `${tm.county}, ${tm.state}` : '—';
    }

    const topMarketsRes = await duckQuery(
      conn,
      `SELECT fips, county, state, market_score, resilience_index
       FROM market_scores ORDER BY market_score DESC NULLS LAST LIMIT ${TOP_N}`,
    );
    renderTopMarkets(topMarketsRes);

    const topDealsRes = await duckQuery(
      conn,
      `SELECT property_name, address, city, state, total_score, market_score, deal_score,
              fips, lat, lng, county, zillow_url
       FROM properties ORDER BY total_score DESC NULLS LAST LIMIT ${TOP_N}`,
    );
    renderTopDeals(topDealsRes);

    const firstDeal = topDealsRes.toArray()[0] || null;
    window.__explorerSampleDeal = firstDeal;
    renderHowWeScore(firstDeal);
  } catch (e) {
    console.warn('refreshDashboard:', e.message);
  }
}

function renderTopMarkets(result) {
  const rows = result.toArray();
  const host = document.getElementById('dashboard-top-markets');
  if (!host) return;
  if (!rows.length) {
    host.innerHTML = '<p class="dashboard-empty">No market rows.</p>';
    return;
  }
  host.innerHTML = `<div class="dashboard-market-grid">${rows
    .map((r, i) => {
      const ms = r.market_score != null ? Number(r.market_score).toFixed(1) : '—';
      const cls = scoreBadgeClass(Number(r.market_score) || 0, 'market');
      return `<div class="dashboard-market-card">
        <div class="dashboard-market-rank">${i + 1}</div>
        <div class="dashboard-market-info">
          <div class="dashboard-market-name">${esc(r.county)}</div>
          <div class="dashboard-market-meta">${esc(r.state)} · Resilience ${r.resilience_index != null ? Number(r.resilience_index).toFixed(2) : '—'}</div>
        </div>
        <div class="dashboard-market-score">
          <span class="score-badge ${cls}">${ms}</span>
          <div class="dashboard-market-score-label">market</div>
        </div>
      </div>`;
    })
    .join('')}</div>`;
}

function renderTopDeals(result) {
  const rows = result.toArray();
  const host = document.getElementById('dashboard-top-deals');
  if (!host) return;
  if (!rows.length) {
    host.innerHTML = '<p class="dashboard-empty">No property rows.</p>';
    return;
  }
  host.innerHTML = `<div class="dashboard-deal-grid">${rows
    .map((r) => {
      const total = r.total_score != null ? Number(r.total_score).toFixed(1) : '—';
      const ms = r.market_score != null ? Number(r.market_score).toFixed(1) : '—';
      const ds = r.deal_score != null ? Number(r.deal_score).toFixed(1) : '—';
      let z = r.zillow_url;
      if (!z && r.fips) {
        z = buildZillowUrl(r.fips, r.lat, r.lng, r.county, r.state);
      }
      const zillowCell = z
        ? `<a class="zillow-link" href="${esc(z)}" target="_blank" rel="noopener">Zillow →</a>`
        : '<span class="dashboard-muted">—</span>';
      const title = r.property_name || r.address || '—';
      return `<div class="dashboard-deal-card">
        <div class="dashboard-deal-title">${esc(title)}</div>
        <div class="dashboard-deal-loc">${esc(r.city)}, ${esc(r.state)}</div>
        <div class="dashboard-deal-stats">
          <div><span class="score-badge ${scoreBadgeClass(Number(r.total_score) || 0, 'total')}">${total}</span><div class="dashboard-deal-stat-label">total</div></div>
          <div><span class="score-badge ${scoreBadgeClass(Number(r.market_score) || 0, 'market')}">${ms}</span><div class="dashboard-deal-stat-label">market</div></div>
          <div><span class="score-badge ${scoreBadgeClass(Number(r.deal_score) || 0, 'deal')}">${ds}</span><div class="dashboard-deal-stat-label">deal</div></div>
        </div>
        <div class="dashboard-deal-z">${zillowCell}</div>
      </div>`;
    })
    .join('')}</div>`;
}

/**
 * @param {*} sampleDeal — first row from top deals for numeric formula example
 */
function renderHowWeScore(sampleDeal) {
  const row = sampleDeal || window.__explorerSampleDeal || null;
  const marketPct = parseInt(document.getElementById('w-split-market')?.value || '60', 10);
  const dealPct = parseInt(document.getElementById('w-split-deal')?.value || '40', 10);

  const marketHtml = MARKET_WEIGHT_META.map((meta) => {
    const v = parseInt(document.getElementById(meta.domId)?.value || '0', 10);
    const w = Math.min(100, Math.max(0, v));
    return `<div class="how-weight-row">
      <div class="how-weight-label">${esc(meta.label)}</div>
      <div class="how-weight-bar-wrap"><div class="how-weight-bar how-weight-bar-market" style="width:${w}%"></div></div>
      <div class="how-weight-val">${v}</div>
    </div>
    <div class="how-weight-explain">${esc(meta.explain)}</div>`;
  }).join('');

  const dealHtml = DEAL_WEIGHT_META.map((meta) => {
    const v = parseInt(document.getElementById(meta.domId)?.value || '0', 10);
    const w = Math.min(100, Math.max(0, v));
    return `<div class="how-weight-row">
      <div class="how-weight-label">${esc(meta.label)}</div>
      <div class="how-weight-bar-wrap"><div class="how-weight-bar how-weight-bar-deal" style="width:${w}%"></div></div>
      <div class="how-weight-val">${v}</div>
    </div>
    <div class="how-weight-explain">${esc(meta.explain)}</div>`;
  }).join('');

  const activeKey = window.__explorerActiveScenario || 'balanced';
  const scenarioName = SCENARIO_LABELS[activeKey] || 'Custom';

  let formulaLine = '—';
  if (
    row &&
    row.market_score != null &&
    row.deal_score != null &&
    !Number.isNaN(marketPct) &&
    !Number.isNaN(dealPct)
  ) {
    const m = Number(row.market_score);
    const d = Number(row.deal_score);
    const combined = (m * marketPct) / 100 + (d * dealPct) / 100;
    formulaLine = `(${m.toFixed(1)} × ${marketPct}%) + (${d.toFixed(1)} × ${dealPct}%) = ${combined.toFixed(1)}`;
  }

  const host = document.getElementById('how-we-score-body');
  if (!host) return;
  host.innerHTML = `
    <div class="how-we-score-head">
      <h2 class="how-we-score-title">How We Score</h2>
      <span class="how-scenario-pill">${esc(scenarioName)}</span>
    </div>
    <div class="how-we-score-grid">
      <div class="how-layer">
        <div class="how-layer-head">
          <span class="how-layer-title market">Market score</span>
          <span class="how-layer-pct">${marketPct}%</span>
        </div>
        <p class="how-layer-sub">Is this a good market?</p>
        ${marketHtml}
      </div>
      <div class="how-merge">+</div>
      <div class="how-layer">
        <div class="how-layer-head">
          <span class="how-layer-title deal">Deal score</span>
          <span class="how-layer-pct">${dealPct}%</span>
        </div>
        <p class="how-layer-sub">Is this a good deal?</p>
        ${dealHtml}
      </div>
    </div>
    <div class="how-formula">
      <strong>Total score</strong> = (Market score × ${marketPct}%) + (Deal score × ${dealPct}%)
      <span class="how-formula-eq">e.g. ${formulaLine}</span>
    </div>`;
}

/** Call after weight inputs change (sample deal from last refresh). */
export function updateHowWeScorePanelOnly() {
  renderHowWeScore(window.__explorerSampleDeal || null);
}

export function setActiveScenarioBar(key) {
  window.__explorerActiveScenario = key;
  document.querySelectorAll('.scenario-pill').forEach((btn) => {
    const k = btn.getAttribute('data-scenario-key');
    btn.classList.toggle('scenario-pill-active', k === key);
  });
}
