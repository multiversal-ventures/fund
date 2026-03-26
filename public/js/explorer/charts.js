/**
 * Dashboard-only charts: fed by the last DuckDB query result from the dashboard query bar.
 */
/**
 * @param {*} result DuckDB query result
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
