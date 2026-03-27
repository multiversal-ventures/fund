/**
 * DC Markets tab — top counties + stats from `dc_market_scores`.
 * Row click opens state-level Tavily digest from `dc_tavily_state`.
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

function escAttr(s) {
  if (s == null) return '';
  return String(s).replace(/&/g, '&amp;').replace(/"/g, '&quot;').replace(/</g, '&lt;');
}

function formatPublishedDate(s) {
  if (s == null || !String(s).trim()) return '';
  const raw = String(s).trim();
  const d = new Date(raw);
  if (!Number.isNaN(d.getTime())) {
    return d.toLocaleDateString(undefined, { month: 'short', day: 'numeric', year: 'numeric' });
  }
  return raw;
}

/** @param {*} g firebase compat app */
function getDcLiveNewsUrl(g) {
  const m = typeof document !== 'undefined' ? document.querySelector('meta[name="explorer-dclocalnews-url"]') : null;
  const custom = m?.getAttribute('content')?.trim();
  if (custom) return custom;
  const pid = g?.app?.()?.options?.projectId;
  return pid ? `https://us-central1-${pid}.cloudfunctions.net/dcLocalNews` : '';
}

/** @type {((e: KeyboardEvent) => void) | null} */
let _dcTavilyKeyHandler = null;

function closeDcTavilyModal() {
  const modal = document.getElementById('dc-tavily-modal');
  if (modal) modal.hidden = true;
  if (_dcTavilyKeyHandler) {
    document.removeEventListener('keydown', _dcTavilyKeyHandler);
    _dcTavilyKeyHandler = null;
  }
}

function ensureDcTavilyModalWired() {
  if (typeof window !== 'undefined' && window.__dcTavilyModalWired) return;
  if (typeof window !== 'undefined') window.__dcTavilyModalWired = true;
  const modal = document.getElementById('dc-tavily-modal');
  if (!modal) return;
  modal.querySelector('.dc-tavily-modal-backdrop')?.addEventListener('click', closeDcTavilyModal);
  modal.querySelector('.dc-tavily-modal-close')?.addEventListener('click', closeDcTavilyModal);
  modal.querySelector('#dc-tavily-live-btn')?.addEventListener('click', () => {
    void fetchDcLiveNews();
  });
}

/** Calls Cloud Function `dcLocalNews` (Tavily topic=news). Requires Firebase Auth + allowlisted email. */
async function fetchDcLiveNews() {
  const ctx = typeof window !== 'undefined' ? window.__dcTavilyModalContext : null;
  const btn = document.getElementById('dc-tavily-live-btn');
  const status = document.getElementById('dc-tavily-live-status');
  const summaryEl = document.getElementById('dc-tavily-live-summary');
  const grid = document.getElementById('dc-tavily-live-results');

  if (!ctx?.state) {
    if (status) status.textContent = 'Missing location context.';
    return;
  }

  /** @type {typeof import('firebase/compat') | undefined} */
  const g = typeof window !== 'undefined' && window.firebase ? window.firebase : undefined;
  if (!g?.auth) {
    if (status) status.textContent = 'Firebase not loaded.';
    return;
  }
  const user = g.auth().currentUser;
  if (!user) {
    if (status) status.textContent = 'Sign in to load live news.';
    return;
  }

  if (btn) btn.disabled = true;
  if (status) {
    status.textContent = 'Fetching recent news…';
    status.removeAttribute('title');
  }
  if (summaryEl) {
    summaryEl.hidden = true;
    summaryEl.innerHTML = '';
  }
  if (grid) grid.innerHTML = '';

  try {
    const url = getDcLiveNewsUrl(g);
    if (!url) {
      throw new Error('Could not resolve news service URL (Firebase project id missing).');
    }
    const token = await user.getIdToken();
    const ac = new AbortController();
    const timer = setTimeout(() => ac.abort(), 55000);
    let r;
    try {
      r = await fetch(url, {
        method: 'POST',
        cache: 'no-store',
        headers: {
          'Content-Type': 'application/json',
          Authorization: `Bearer ${token}`,
        },
        body: JSON.stringify({
          state: String(ctx.state).trim().toUpperCase(),
          county: ctx.county ? String(ctx.county).trim() : undefined,
          fips: ctx.fips ? String(ctx.fips).trim() : undefined,
        }),
        signal: ac.signal,
      });
    } finally {
      clearTimeout(timer);
    }
    let data = {};
    try {
      data = await r.json();
    } catch {
      data = {};
    }
    if (r.status === 404) {
      throw new Error(
        'News service not found (HTTP 404). Deploy Cloud Function dcLocalNews: enable Secret Manager, set TAVILY_API_KEY, then firebase deploy --only functions:dcLocalNews.',
      );
    }
    if (!r.ok) {
      throw new Error(data.error || `Request failed (${r.status})`);
    }
    const items = Array.isArray(data.results) ? data.results : [];

    const paras = Array.isArray(data.summaryParagraphs)
      ? data.summaryParagraphs.filter((p) => p != null && String(p).trim())
      : [];
    const summaryFallback =
      paras.length === 0 && data.summary != null && String(data.summary).trim()
        ? [String(data.summary).trim()]
        : [];

    if (summaryEl && (paras.length > 0 || summaryFallback.length > 0)) {
      const blocks = paras.length > 0 ? paras : summaryFallback;
      summaryEl.hidden = false;
      summaryEl.innerHTML = `<div class="dc-live-summary-inner">
        <span class="dc-live-summary-label">Tavily overview</span>
        ${blocks.map((p) => `<p class="dc-live-summary-text">${esc(String(p))}</p>`).join('')}
      </div>`;
    } else if (summaryEl) {
      summaryEl.hidden = true;
      summaryEl.innerHTML = '';
    }

    if (grid) {
      if (!items.length) {
        grid.innerHTML =
          '<p class="dc-tavily-muted dc-live-empty">No articles returned for this query.</p>';
      } else {
        grid.innerHTML = items
          .map((x) => {
            const u = esc(x.url);
            const t = esc(x.title || x.url);
            const sn = x.snippet != null ? String(x.snippet) : '';
            const src = x.source != null ? String(x.source) : '';
            const rank = x.rank != null ? Number(x.rank) : 0;
            const pdRaw = x.published_date != null && String(x.published_date).trim();
            const pd = pdRaw ? formatPublishedDate(pdRaw) : '';
            const sc = x.score != null && Number.isFinite(Number(x.score)) ? Number(x.score) : null;
            return `<article class="dc-live-card" role="listitem">
              <div class="dc-live-card-meta">
                <span class="dc-live-rank" aria-hidden="true">#${rank || '—'}</span>
                ${src ? `<span class="dc-live-source">${esc(src)}</span>` : ''}
                ${pd ? `<time class="dc-live-time" datetime="${escAttr(pdRaw)}">${esc(pd)}</time>` : ''}
                ${sc != null ? `<span class="dc-live-score" title="Tavily relevance">score ${esc(String(sc))}</span>` : ''}
              </div>
              <h5 class="dc-live-card-title"><a class="dc-live-card-link" href="${u}" target="_blank" rel="noopener noreferrer">${t}</a></h5>
              ${sn ? `<p class="dc-live-card-snippet">${esc(sn)}</p>` : ''}
            </article>`;
          })
          .join('');
      }
    }

    if (status) {
      const n = items.length;
      const label = data.queryLabel ? String(data.queryLabel) : String(ctx.state || '');
      status.textContent = n
        ? `${n} article${n === 1 ? '' : 's'} · ${label}`
        : `No articles · ${label}`;
      if (data.query) status.title = data.query;
    }
  } catch (e) {
    const raw = e?.message || String(e);
    const netFail =
      raw === 'Failed to fetch' ||
      raw.includes('Failed to fetch') ||
      e?.name === 'TypeError' ||
      e?.name === 'AbortError';
    const msg = netFail
      ? e?.name === 'AbortError'
        ? 'Request timed out. Try again or check Cloud Function logs.'
        : 'Could not reach the news service (network / CORS / not deployed). Deploy dcLocalNews with public invoker + CORS, set secret TAVILY_API_KEY, then redeploy functions.'
      : raw;
    if (status) {
      status.textContent = msg;
      status.removeAttribute('title');
    }
    if (summaryEl) {
      summaryEl.hidden = true;
      summaryEl.innerHTML = '';
    }
    if (grid) grid.innerHTML = '';
  } finally {
    if (btn) btn.disabled = false;
  }
}

/**
 * @param {*} conn
 * @param {{ state: string, county: string, fips: string }} row
 */
export async function openDcTavilyModal(conn, row) {
  ensureDcTavilyModalWired();
  const modal = document.getElementById('dc-tavily-modal');
  const titleEl = document.getElementById('dc-tavily-modal-title');
  const subEl = document.getElementById('dc-tavily-modal-sub');
  const scoresEl = document.getElementById('dc-tavily-modal-scores');
  const digestEl = document.getElementById('dc-tavily-modal-digest');
  const sourcesEl = document.getElementById('dc-tavily-modal-sources');
  if (!modal || !conn || !titleEl || !subEl || !scoresEl || !digestEl || !sourcesEl) return;

  const st = String(row.state || '').trim();
  if (!st) return;

  if (typeof window !== 'undefined') {
    window.__dcTavilyModalContext = {
      state: st,
      county: String(row.county || '').trim(),
      fips: String(row.fips || '').trim(),
    };
  }
  const liveStatus = document.getElementById('dc-tavily-live-status');
  const liveGrid = document.getElementById('dc-tavily-live-results');
  const liveSummary = document.getElementById('dc-tavily-live-summary');
  const liveBtn = document.getElementById('dc-tavily-live-btn');
  if (liveStatus) {
    liveStatus.textContent = '';
    liveStatus.removeAttribute('title');
  }
  if (liveGrid) liveGrid.innerHTML = '';
  if (liveSummary) {
    liveSummary.hidden = true;
    liveSummary.innerHTML = '';
  }
  if (liveBtn) liveBtn.disabled = false;

  modal.hidden = false;
  modal.querySelector('.dc-tavily-modal-close')?.focus({ preventScroll: true });
  titleEl.textContent = `Tavily — ${st}`;
  subEl.textContent = `${row.county || 'County'} (FIPS ${row.fips || '—'}) · Intel is shared for all counties in ${st}.`;
  scoresEl.textContent = 'Loading…';
  digestEl.textContent = '';
  sourcesEl.innerHTML = '';

  const { runQuery, sqlStringLiteral } = await dcExplorerImport('/js/explorer/duckdb.js');

  try {
    const res = await runQuery(
      conn,
      `SELECT state_abbr, tavily_political_score, tavily_penalty, tavily_snippet_digest, tavily_sources_json
       FROM dc_tavily_state WHERE state_abbr = ${sqlStringLiteral(st)} LIMIT 1`,
    );
    const rows = res.toArray();
    const tv = rows[0];
    if (!tv) {
      scoresEl.innerHTML = `<span class="dc-tavily-muted">No Tavily row for <code>${esc(st)}</code>.</span>`;
      digestEl.textContent = '';
      sourcesEl.innerHTML = '';
    } else {
      const pol = tv.tavily_political_score != null ? Number(tv.tavily_political_score).toFixed(2) : '—';
      const pen = tv.tavily_penalty != null ? Number(tv.tavily_penalty).toFixed(1) : '—';
      scoresEl.innerHTML = `<span class="dc-tavily-pill">Political score <strong>${esc(pol)}</strong></span> <span class="dc-tavily-pill">Penalty <strong>${esc(pen)}</strong></span>`;
      const digest = tv.tavily_snippet_digest != null ? String(tv.tavily_snippet_digest) : '';
      digestEl.textContent = digest.trim() || 'No digest stored for this state (neutral run or empty results).';

      /** @type {{ url?: string, title?: string }[]} */
      let src = [];
      try {
        src = JSON.parse(tv.tavily_sources_json || '[]');
      } catch {
        src = [];
      }
      if (!Array.isArray(src) || src.length === 0) {
        sourcesEl.innerHTML = '<li class="dc-tavily-muted">No source URLs recorded.</li>';
      } else {
        sourcesEl.innerHTML = src
          .filter((x) => x && x.url)
          .slice(0, 16)
          .map((x) => {
            const u = esc(x.url);
            const t = esc(x.title || x.url);
            return `<li><a class="zillow-link" href="${u}" target="_blank" rel="noopener noreferrer">${t}</a></li>`;
          })
          .join('');
      }
    }
  } catch (e) {
    scoresEl.innerHTML = `<span class="dc-tavily-err">${esc(e?.message || e)}</span>`;
    digestEl.textContent = '';
    sourcesEl.innerHTML = '';
  }

  if (_dcTavilyKeyHandler) {
    document.removeEventListener('keydown', _dcTavilyKeyHandler);
    _dcTavilyKeyHandler = null;
  }
  _dcTavilyKeyHandler = (e) => {
    if (e.key === 'Escape') closeDcTavilyModal();
  };
  document.addEventListener('keydown', _dcTavilyKeyHandler);
}

/**
 * @param {*} conn
 * @param {HTMLElement | null} tableMount
 */
function wireDcTopMarketsClicks(conn, tableMount) {
  const tbl = tableMount?.querySelector('table.dashboard-mini-table');
  if (!tbl) return;
  tbl.addEventListener('click', (ev) => {
    if (ev.target.closest('a')) return;
    const tr = ev.target.closest('tbody tr[data-dc-state]');
    if (!tr) return;
    openDcTavilyModal(conn, {
      state: tr.getAttribute('data-dc-state') || '',
      county: tr.getAttribute('data-dc-county') || '',
      fips: tr.getAttribute('data-dc-fips') || '',
    });
  });
}

/** @param {*} conn */
export async function refreshDcDashboard(conn) {
  if (!conn) return;

  const { runQuery, sqlStringLiteral } = await dcExplorerImport('/js/explorer/duckdb.js');

  const topEl = document.getElementById('dc-stat-top');
  const subEl = document.getElementById('dc-stat-top-sub');
  const nEl = document.getElementById('dc-stat-n');
  const tableMount = document.getElementById('dc-top-markets');

  try {
    const nRes = await runQuery(
      conn,
      'SELECT COUNT(*)::BIGINT AS n FROM dc_market_scores WHERE dc_eligible = true',
    );
    const topRes = await runQuery(
      conn,
      `SELECT county, state, dc_market_score FROM dc_market_scores
       WHERE dc_eligible = true
       ORDER BY dc_market_score DESC NULLS LAST LIMIT 1`,
    );
    const top30Res = await runQuery(
      conn,
      `SELECT fips, county, state, dc_market_score, s_electrical, s_political, s_pipeline, zillow_url
       FROM dc_market_scores WHERE dc_eligible = true
       ORDER BY dc_market_score DESC NULLS LAST LIMIT 25`,
    );

    const nRows = nRes.toArray();
    const topRows = topRes.toArray();
    const top30Rows = top30Res.toArray();
    const nVal = nRows[0]?.n;
    const topRow = topRows[0];

    if (nEl) nEl.textContent = nVal != null ? String(Number(nVal)) : '—';
    if (topEl) {
      topEl.textContent =
        topRow?.dc_market_score != null ? Number(topRow.dc_market_score).toFixed(1) : '—';
    }
    if (subEl) {
      subEl.textContent =
        topRow?.county && topRow?.state ? `${topRow.county}, ${topRow.state}` : '—';
    }

    if (tableMount && top30Rows.length) {
      const rows = top30Rows
        .map((r) => {
          const zl =
            r.zillow_url && String(r.zillow_url).trim()
              ? `<a class="zillow-link" href="${esc(r.zillow_url)}" target="_blank" rel="noopener">Zillow</a>`
              : '—';
          const st = escAttr(r.state);
          const co = escAttr(r.county);
          const fi = escAttr(r.fips);
          return `<tr class="dc-row-clickable" role="button" tabindex="0" data-dc-state="${st}" data-dc-county="${co}" data-dc-fips="${fi}" title="Show Tavily results for this state">
            <td>${esc(r.county)}, ${esc(r.state)}</td><td>${esc(r.fips)}</td><td class="stat-green">${esc(r.dc_market_score)}</td><td>${esc(r.s_electrical)}</td><td>${esc(r.s_political)}</td><td>${esc(r.s_pipeline)}</td><td>${zl}</td>
          </tr>`;
        })
        .join('');
      tableMount.innerHTML = `<div class="dc-markets-table-wrap"><table class="dashboard-mini-table dc-markets-table"><thead><tr><th>County</th><th>FIPS</th><th>DC score</th><th>Electrical</th><th>Political</th><th>Pipeline</th><th>Zillow</th></tr></thead><tbody>${rows}</tbody></table></div>`;
      wireDcTopMarketsClicks(conn, tableMount);

      const tbody = tableMount.querySelector('tbody');
      tbody?.addEventListener('keydown', (ev) => {
        if (ev.key !== 'Enter' && ev.key !== ' ') return;
        const tr = ev.target?.closest?.('tr[data-dc-state]');
        if (!tr) return;
        ev.preventDefault();
        openDcTavilyModal(conn, {
          state: tr.getAttribute('data-dc-state') || '',
          county: tr.getAttribute('data-dc-county') || '',
          fips: tr.getAttribute('data-dc-fips') || '',
        });
      });
    } else if (tableMount) {
      tableMount.innerHTML =
        '<p class="dc-empty-hint">No eligible counties in this dataset, or DC tables did not load. If you see a warning banner above, some files may still be syncing.</p>';
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
