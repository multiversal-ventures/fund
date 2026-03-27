# Data Center Adjacency Phase 1 — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship Tier 1 DC market scoring (`dc_market_scores.parquet`), optional Tavily state intel, static thesis + leaderboard pages, and a Fund Explorer “DC Markets” tab backed by the same Parquet.

**Architecture:** Python pipeline under `scripts/dc/` reads Census ACS (`data/census/acs_2023.parquet`), Census CBP NAICS 518210, bundled EIA state industrial rates (or future API), optional Tavily REST Search; writes `data/dc/*.parquet`. Explorer loads new tables via `duckdb.js`.

**Tech Stack:** Python 3.12+, pandas, pyarrow, requests, click; Firebase Storage (existing upload); Alpine.js + DuckDB WASM (existing).

**Spec:** `docs/superpowers/specs/2026-03-26-data-center-adjacency-thesis-design.md`

---

### Task 1: DC pipeline modules + weights

**Files:**
- Create: `scripts/dc/dc_weights.default.json`
- Create: `scripts/dc/data/eia_state_industrial_2023.csv`
- Create: `scripts/dc/pull_cbp_naics518.py`
- Create: `scripts/dc/load_eia_state.py`
- Create: `scripts/dc/enrich_tavily.py`
- Create: `scripts/dc/score_dc_markets.py`
- Create: `scripts/dc/run_dc_pipeline.py`

- [ ] Implement CBP pull for NAICS 518210 (2023), county FIPS.
- [ ] Load EIA industrial ¢/kWh by state from CSV; document source in code.
- [ ] Tavily: POST `https://api.tavily.com/search` per state when `TAVILY_API_KEY` set; else neutral scores.
- [ ] Score: merge on `fips`/`state`, use `normalize_signal` from `score.py`, output `dc_market_scores.parquet` + `dc_tavily_state.parquet`.
- [ ] CLI: `uv run scripts/dc/run_dc_pipeline.py --output ../../data` (path to repo `data/`).

---

### Task 2: Tests

**Files:**
- Create: `scripts/tests/test_score_dc_markets.py`

- [ ] Unit test scoring with 3–5 synthetic counties (no network).

---

### Task 3: Static HTML + index hub

**Files:**
- Create: `public/dc_thesis.html`
- Create: `public/dc_market_leaderboard.html`
- Modify: `public/index.html`

- [ ] Thesis: methodology, validation links, Tavily docs links, disclaimer.
- [ ] Leaderboard: narrative + link to Explorer DC tab.
- [ ] Index: “Research track — Data center adjacency” cards.

---

### Task 4: Fund Explorer — DC tab

**Files:**
- Modify: `public/explorer.html` (tab + panel, bump `explorer-asset-version`)
- Modify: `public/js/explorer/duckdb.js` (register `dc_market_scores`, `dc_tavily_state`)
- Modify: `public/js/explorer/app.js` (`setTab`, optional `?tab=dc`)
- Create: `public/js/explorer/dc-dashboard.js`
- Modify: `public/js/explorer/dashboard.js` or inline script in `explorer.html` to import DC dashboard refresh

- [ ] DC tab: stats + top counties table + “How we score (DC)” collapsible.
- [ ] SQL presets for `dc_market_scores` in DC panel or SQL tab note.

---

### Task 5: Verification

- [ ] `cd scripts && uv run pytest tests/test_score_dc_markets.py -v`
- [ ] `uv run scripts/dc/run_dc_pipeline.py` with local `data/census/acs_2023.parquet` present (run multifamily census pull first if missing).
- [ ] Manual: open `explorer.html` after deploy — DC tab loads without errors (tables may warn if Parquet not uploaded).

---

### Task 6: Commit

```bash
git add scripts/dc public/dc_thesis.html public/dc_market_leaderboard.html public/index.html public/explorer.html public/js/explorer/
git commit -m "feat: DC adjacency Tier 1 pipeline, thesis pages, Explorer DC tab"
```
