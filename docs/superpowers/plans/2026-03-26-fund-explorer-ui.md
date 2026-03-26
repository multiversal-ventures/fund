# Fund Explorer UI Redesign — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the SQL-first `public/explorer.html` with an Alpine.js single-page experience: **Dashboard** (default), **Map** (Leaflet + GeoJSON + Zillow + signal tooltips), **SQL Studio** (CodeMirror 6 + schema + autocomplete), plus hybrid **estimated preview** vs **Save & Refresh Pipeline** authoritative scores.

**Architecture:** Keep Firebase Auth + Storage + DuckDB WASM loading Parquet unchanged at the data layer. Add Alpine.js for tab state and reactive UI. Split monolithic HTML into `public/explorer.html` plus ES modules under `public/js/explorer/` and optional `public/css/explorer.css`. Map uses Leaflet + a US counties GeoJSON asset. SQL Studio uses **CodeMirror 6** (`@codemirror/lang-sql`, `@codemirror/autocomplete`) with schema-driven completion — **not** a hand-built textarea.

**Tech Stack:** Alpine.js 3 (CDN), Leaflet 1.x (CDN), CodeMirror 6 (esm.sh or jsdelivr), DuckDB WASM + Firebase (existing), GeoJSON counties (bundled or CDN per spec).

**Design spec:** `docs/superpowers/specs/2026-03-26-fund-explorer-ui-design.md`

---

## File map (target end state)

| Path | Responsibility |
|------|----------------|
| `public/explorer.html` | Shell: Firebase scripts, Alpine root, tab chrome, link CSS/JS |
| `public/css/explorer.css` | Layout, tabs, dashboard cards, map split, SQL Studio grid |
| `public/js/explorer/app.js` | Alpine `createApp`: tab, scenario, auth gate, orchestration |
| `public/js/explorer/scenarios.js` | `SCENARIOS`, weight helpers, Firestore `config/pipeline` save (Task 3 creates; Task 4 extends) |
| `public/js/explorer/duckdb.js` | Init DuckDB WASM, register Parquet, shared `conn` export |
| `public/js/explorer/dashboard.js` | Queries for stats, top markets, top deals, “How We Score” data |
| `public/js/explorer/map.js` | Leaflet map, GeoJSON layer, choropleth, tooltip + Zillow, side panel |
| `public/js/explorer/sql-studio.js` | CodeMirror mount, presets, run query, export, results table |
| `public/js/explorer/schema.js` | Table/column metadata + plain-English blurbs for schema sidebar + CM completion |
| `public/js/explorer/results.js` (optional) | Shared `renderResults` / table HTML used by SQL Studio (and map/dashboard if needed) |
| `public/js/explorer/zillow.js` | Extract/refactor `buildZillowUrl` + county bounds from current inline script |
| `public/data/us-counties.geojson` | County polygons (or subset + doc if too large — see Task 5) |

---

### Task 1: Scaffold Alpine shell and static assets

**Files:**
- Create: `public/css/explorer.css` (minimal: header, tabs, panels)
- Modify: `public/explorer.html` (reduce inline CSS; add Alpine `x-data`, three tabs; keep auth HTML structure)
- Create: `public/js/explorer/app.js` (Alpine component: `activeTab`, `scenario`, `user`)

- [ ] **Step 1:** Add Alpine.js 3 from CDN to `explorer.html` (`defer` + `alpine.start()` after module).
- [ ] **Step 2:** Wrap main app in `x-data="explorerApp()"` with `activeTab: 'dashboard' | 'map' | 'sql'`.
- [ ] **Step 3:** Render tab buttons that set `activeTab`; show three sections with `x-show` / `template x-if` so **one DuckDB init** runs once (script module loads duckdb init on auth success — wire in Task 2).
- [ ] **Step 4:** Move shared styles into `explorer.css`; verify auth gate + allowlist still work.
- [ ] **Step 5:** Commit: `feat(explorer): scaffold Alpine tabs shell`

---

### Task 2: Extract DuckDB initialization module

**Files:**
- Create: `public/js/explorer/duckdb.js`
- Modify: `public/explorer.html` / `app.js` — remove duplicate init

- [ ] **Step 1:** Move `initExplorer` logic from inline `<script type="module">` into `duckdb.js`: export `async function initDuckDB(user)` returning `{ db, conn }`.
- [ ] **Step 2:** Keep the **same** `files` array paths and `CREATE TABLE ... AS SELECT * FROM parquet` behavior as current `explorer.html` (lines ~403–427).
- [ ] **Step 3:** Export `runQuery(conn, sql)` and preserve `registerFileBuffer` error handling; surface user-visible warning list if tables skip (new small toast area in UI).
- [ ] **Step 4:** `app.js` calls `initDuckDB` once after auth; pass `conn` into child modules (dashboard/map/sql) via Alpine store or callbacks.
- [ ] **Step 5:** Commit: `refactor(explorer): extract DuckDB init module`

---

### Task 3: Dashboard — stats, top lists, “How We Score”

**Files:**
- Create: `public/js/explorer/dashboard.js`
- Modify: `public/css/explorer.css`, `app.js`

- [ ] **Step 1:** Implement queries: global counts (markets, deals), max market score row, last run via Storage `data/meta/last_run.json` (reuse `loadRunStatus` pattern).
- [ ] **Step 2:** Build **Top markets** list: `SELECT ... FROM market_scores ORDER BY market_score DESC LIMIT N` (align columns with existing presets).
- [ ] **Step 3:** Build **Top deals** list: `SELECT ... FROM properties ORDER BY total_score DESC LIMIT N`; show Zillow using shared `zillow.js`.
- [ ] **Step 4:** Create **`public/js/explorer/scenarios.js`** exporting `SCENARIOS` and weight keys (moved from current inline script). **“How We Score”** panel: bind labels/weights to current scenario object. Render market vs deal columns with bar widths proportional to weights; plain-English strings from spec (vacancy trend, resilience, etc.). **Footer row required:** show formula aligned with `docs/superpowers/specs/2026-03-26-v2-scoring-sensitivity-design.md` — e.g. `Total = (Market score × market%) + (Deal score × deal%)` with a **numeric example** (sample numbers from a visible row). Task 4 adds Firestore save to the same module without duplicating scenario definitions.
- [ ] **Step 5:** Wire **scenario pill clicks** to update dashboard queries and “How We Score” numbers (preview path — same data as today until Task 8).
- [ ] **Step 6:** Commit: `feat(explorer): dashboard with scoring explainer and ranked lists`

---

### Task 4: Scenario bar — customize weights + pipeline save

**Files:**
- Modify: `public/js/explorer/scenarios.js` (add Firestore save + DOM helpers)
- Modify: `public/js/explorer/app.js`
- Modify: `public/explorer.html` (collapsible panel markup)

- [ ] **Step 1:** Move weight inputs + Firestore `config/pipeline` save from current file into Alpine-driven panel **Customize weights** (sliders or number inputs with live totals). **Single source of truth:** extend **`scenarios.js` from Task 3** — do not duplicate `SCENARIOS`.
- [ ] **Step 2:** Keep validation: market weights sum 100, deal weights sum 100, split sums 100.
- [ ] **Step 3:** **Save & Refresh Pipeline** button: identical payload to current `set({ market_weights, deal_weights, split })` and status text.
- [ ] **Step 4:** Commit: `feat(explorer): scenario presets and pipeline save panel`

---

### Task 5: Map — Leaflet, GeoJSON, choropleth, Zillow, side panel

**Files:**
- Create: `public/js/explorer/map.js`, `public/js/explorer/zillow.js`
- Add: `public/data/us-counties.geojson` **or** document CDN URL in code comments + `fetch`
- Modify: `public/css/explorer.css`

- [ ] **Step 1:** Add Leaflet CSS/JS via CDN to `explorer.html`.
- [ ] **Step 2:** Load county GeoJSON; join features to DuckDB rows on `fips` / `GEOID` / `id` — **normalize** in one place (document column name in code comment once verified against file).
- [ ] **Step 3:** Implement **Color by** dropdown: map metric → column in `market_scores` or joined query; compute min/max for legend scale.
- [ ] **Step 3b:** Implement **Filter** dropdown per spec (e.g. min total/market score threshold, “has deals” / `EXISTS` properties for county). Re-filter GeoJSON style or feature visibility + update legend counts.
- [ ] **Step 4:** **Hover:** Leaflet `bindTooltip` with county name, scores, and **Zillow link** (`buildZillowUrl(fips, lat, lng, county, state)` — pass through from feature properties or joined query).
- [ ] **Step 5:** **Click:** open side panel with **Market signals** list; each row: label, value, **(i)** popover text (static copy keyed by signal id — store in `schema.js` or `signals.json`).
- [ ] **Step 6:** Panel lists top N deals in county (query `properties WHERE fips = ?`); buttons: view all, Zillow county link.
- [ ] **Step 7:** **GeoJSON load failure:** show inline error in map tab with **Retry** button; do not block DuckDB or other tabs (per design spec).
- [ ] **Step 8:** If GeoJSON is too large for repo, use **simplified** topology or **state-level** chunk loading — document in README snippet under `docs/` only if user asked (else comment in plan task completion). Prefer **topojson/us-atlas** or similar public domain source; cite license in file header comment.
- [ ] **Step 9:** Commit: `feat(explorer): Leaflet map with county choropleth and profile panel`

---

### Task 6: SQL Studio — CodeMirror 6, schema sidebar, presets

**Files:**
- Create: `public/js/explorer/sql-studio.js`, `public/js/explorer/schema.js`
- Modify: `public/explorer.html`, `public/css/explorer.css`

- [ ] **Step 1:** Import CodeMirror 6 + `@codemirror/lang-sql` + `@codemirror/autocomplete` from **esm.sh** (or jsdelivr `+esm`) in `sql-studio.js`.
- [ ] **Step 2:** Define `schema.js`: tables, row counts (optional: fill at runtime via `SELECT COUNT(*)` once), columns, types, **description** + **tooltip** strings for (i) icons.
- [ ] **Step 2b:** **Searchable schema sidebar:** filter tables/columns as user types (spec requirement). Wire search input to show/hide `schema-table` sections and matching columns.
- [ ] **Step 3:** Build **completion** source from `schema.js` (table names, column names scoped to table).
- [ ] **Step 4:** Mount editor in `#sql-editor-root`; default SQL = current `VIEW_QUERIES.combined` or equivalent.
- [ ] **Step 5:** Wire **Run** (and ⌘↵), **presets** buttons, results table renderer (reuse existing `renderResults` logic moved to shared `results.js` or `sql-studio.js`).
- [ ] **Step 6:** CSV / Parquet export — reuse existing `COPY` + download pattern.
- [ ] **Step 7:** Commit: `feat(explorer): SQL Studio with CodeMirror and schema browser`

---

### Task 7: Charts and polish

**Files:**
- Modify: `public/js/explorer/dashboard.js` or shared `charts.js`

- [ ] **Step 1:** Move score distribution + top states charts to Dashboard tab only (or shared), fed by current result set.
- [ ] **Step 2:** Remove obsolete collapsible “Available Tables” wall of text from primary flow; either delete or tuck under SQL Studio **Help** if still needed.
- [ ] **Step 3:** Commit: `feat(explorer): charts on dashboard and UI cleanup`

---

### Task 8: Hybrid estimated preview vs authoritative scores

**Files:**
- Modify: `public/js/explorer/app.js`, `dashboard.js`, `map.js`

- [ ] **Step 0 (spike):** Audit Parquet columns (`market_scores`, `properties`) for subscore / component fields available for client-side recomputation; record findings in a short comment at top of `dashboard.js` or `scenarios.js` so Step 2 does not stall on data discovery.
- [ ] **Step 1:** Add visible **“Estimated”** badge when UI is showing client-derived ordering (scenario/slider changed since last full pipeline run) — spec: adjacent to scenario bar.
- [ ] **Step 2:** **Choose preview strategy** (document in code comment):
  - **Recommended v1:** Scenario presets switch **ORDER BY** / filter queries (Investor emphasizes `market_scores` columns, Ops emphasizes deal columns) using **existing Parquet columns** — no Python port in browser.
  - **Sliders:** Either (a) debounce and run SQL expressions that approximate weighted score **if** raw subscore columns exist in `market_scores` / `properties`, or (b) show “Custom weights apply after Save & Refresh Pipeline” until pipeline exposes subscores — **pick (a) or (b)** based on column audit in one sub-task.
- [ ] **Step 3:** After successful pipeline save, clear “Estimated” until user changes weights again (track hash of weights in Alpine state).
- [ ] **Step 4:** Commit: `feat(explorer): estimated preview state and pipeline parity messaging`

---

### Task 9: Manual verification and deploy notes

- [ ] **Step 1:** Manual test matrix from design spec: auth, three tabs, map hover Zillow, SQL autocomplete, export, save pipeline.
- [ ] **Step 2:** Confirm `firebase.json` still serves `explorer.html` and new static paths (`/js/explorer/*`, `/css/explorer.css`, `/data/*`).
- [ ] **Step 3:** Commit any small fixes; tag or note for deploy.

---

## Testing commands

No automated test suite required for static UI in v1. Manual checklist in Task 9.

---

## Dependencies & risks

| Risk | Mitigation |
|------|------------|
| GeoJSON size / perf | Simplify polygons; lazy-load map tab; optional cluster |
| Preview ≠ pipeline | “Estimated” badge + Save & Refresh for real numbers |
| CodeMirror ESM+CSP | Use same CDN pattern as DuckDB ESM; test on Firebase Hosting |

---

## Plan review

Run plan reviewer (`writing-plans` skill): `plan-document-reviewer-prompt.md` against this file + spec path.
