# Fund Explorer UI Redesign — Design Spec

**Date:** 2026-03-26  
**Status:** Draft (pending human review)  
**Authors:** Kartik + Claude  
**Prior specs:** `2026-03-25-data-pipeline-property-finder-design.md`, `2026-03-26-v2-scoring-sensitivity-design.md`

## Overview

Redesign `public/explorer.html` so the Fund Explorer reads as a **fund intelligence product**, not a SQL console. Primary users are **non-technical stakeholders and “lazy tech” teammates** who must self-serve without writing queries. **SQL remains available** as a secondary “power user” surface with **schema documentation, autocomplete, and plain-English explanations** — implemented with **established editor libraries**, not custom parsing from scratch.

### Goals

1. **Answer three questions without SQL:** Where should we invest? How does this market compare? What happens if we change the thesis (weights)?
2. **Geography-first exploration:** A real **interactive map** (county polygons, choropleth) with drill-down to county profile and deals.
3. **Transparent scoring:** The **“How We Score”** block is visible on the dashboard — two-layer model, weights, and a plain-English line per signal (not column names alone).
4. **Hybrid re-scoring:** **Live client-side preview** when changing scenarios or sliders; a clear **“Save & Refresh Pipeline”** action (same Firestore write as today) when the team wants **authoritative** scores from the Python pipeline (matches Storage Parquet). Copy may add a subtitle like “Lock in these weights” — but the **primary button label** matches the existing control to avoid confusion.
5. **SQL Studio** is **not** the default tab. It uses **existing libraries** (see Tech Stack) for editing, SQL highlighting, and completion.

### Non-goals

- Replacing Firebase Hosting with a separate app server for this phase.
- Introducing a full React/Vue build pipeline unless later needed; **Alpine.js** (CDN) is sufficient for reactive UI state.
- Pixel-perfect parity with proprietary GIS products; county-level choropleth + drill-down is enough for v1.

---

## Information Architecture

### Persistent chrome

- **Header:** Product name, signed-in user, sign out (unchanged auth model).
- **Scenario bar (always visible):** Presets — **Balanced**, **Investor**, **Ops**, **Durable Tenants** — plus **Customize weights** (expands/collapses a panel with sliders).
- **Primary navigation (tabs):** **Dashboard** | **Map** | **SQL Studio**. Default tab: **Dashboard**.

### Tab: Dashboard

1. **“How We Score”** — Top section. Side-by-side **Market** vs **Deal** weights with **bar visualization**, **scenario name**, and **one-line plain-English explanation** under each weight. Footer row: **formula** `Total = (Market × split%) + (Deal × split%)` with a numeric example.
2. **Summary stat cards** — e.g. top market score, markets screened, active deals, last pipeline run (from `data/meta/last_run.json`). **Formula copy** in “How We Score” must stay aligned with the two-layer model in `2026-03-26-v2-scoring-sensitivity-design.md` (market % + deal % split, same semantics as pipeline).
3. **Top markets** — Ranked list/cards with key subtext (state, deal count, resilience snippet). Mini visual hint of market vs deal contribution optional.
4. **Top deals** — Cards with total / market / deal scores, maturity, units, **Zillow** link (reuse existing `buildZillowUrl` logic).

### Tab: Map

- **Real map:** **Leaflet** + **GeoJSON** county polygons (US county boundaries). Choropleth fill by selected metric.
- **Controls:** **Color by** (Total, Market, Deal, Resilience, Vacancy trend, Rent growth, etc. — aligned to available columns). **Filter** (e.g. min score, “has deals”).
- **Hover:** Tooltip with county name, key scores, and a **“View listings on Zillow”** (or similar) link so users can **jump to Zillow for that county** to browse other on-market inventory (same URL strategy as today: bounds from lat/lng, county bounds map, or county+state search).
- **Click:** Opens **County profile** side panel: score ring or breakdown, **market signals** list — **each signal has an (i) icon** with a short tooltip/popover explaining the metric in plain English (data source + interpretation). List of top deals + actions (view all deals for county, Zillow).

### Tab: SQL Studio

- **Layout:** Left **schema browser** (searchable). Right: **SQL editor** + **results grid** + toolbar (Run, presets, CSV/Parquet export).
- **Editor:** Use **CodeMirror 6** with **`@codemirror/lang-sql`** (and **`@codemirror/autocomplete`** as needed), **or** **Monaco Editor** with SQL language — **pick one**; both are acceptable if loaded from CDN/esm.sh and wired to DuckDB. **Do not** hand-roll a textarea with regex autocomplete.
- **Schema UX:** Table list with row counts and descriptions; expandable columns; **(i)** tooltips per column with join keys and business meaning (can share copy with Map signal tooltips where concepts overlap).
- **Presets:** Same logical queries as today (Top Combined, Top Markets, Best Deals, etc.) — one click loads and runs.

---

## State & Data Flow

### Single DuckDB WASM instance

- One initialization path loads Parquet from Firebase Storage into DuckDB (same file list as today, extended only if spec elsewhere adds tables).
- **Dashboard** and **Map** query this instance for lists, aggregates, and choropleth values.
- **SQL Studio** runs arbitrary SQL against the **same** connection.

### Hybrid scoring model

| Mode | Behavior | Source of truth |
|------|----------|-----------------|
| **Preview** | Changing scenario or sliders updates **estimated** ranks/scores in the UI quickly (client-side SQL or precomputed component scores — implementation detail in plan). | UX responsiveness |
| **Locked** | **Save & Refresh Pipeline** writes `config/pipeline` to Firestore (existing behavior) and shows pipeline status. After run, user reloads and sees Parquet-backed scores. | Investor-facing numbers |

The UI must **label preview** as estimated when it can diverge from pipeline output — e.g. a small **“Estimated”** badge adjacent to the scenario bar or customize panel (exact placement in implementation plan).

**Implementation note (for planning):** The plan must choose one preview strategy — **client-side SQL recomputation** vs **precomputed per-signal components** — and document tradeoffs (accuracy vs effort). The spec does not mandate which.

---

## Tech Stack (UI)

| Concern | Choice |
|---------|--------|
| Reactivity / tabs | **Alpine.js** 3.x via CDN (no bundler required) |
| Map | **Leaflet** + GeoJSON county layer (public domain or Census-derived GeoJSON; store under `public/` or fetch from a stable CDN; document license in plan) |
| SQL editing | **CodeMirror 6** + `@codemirror/lang-sql` **or** **Monaco** — **implementation plan picks one** (bundle size, CDN loading, autocomplete from schema metadata). **Do not** hand-roll a textarea with regex-only completion. |
| Data | **DuckDB WASM** (existing), **Firebase** Auth + Storage + Firestore (existing) |
| Styling | Extend existing slate/dark tokens; keep visual continuity with `index.html` / current explorer |

---

## Error Handling & Edge Cases

- **Missing Parquet / table:** Graceful skip with user-visible message (current `console.warn` behavior improved for SQL Studio).
- **GeoJSON load failure:** Map tab shows error state + link to retry; Dashboard/SQL still work.
- **Zillow:** When URL cannot be built, hide link or show “Open county search” fallback (existing logic).

---

## Testing

- **Manual:** Auth gate, tab switching without reload, scenario change updates dashboard + map colors, SQL preset runs, export CSV/Parquet.
- **Automated (if feasible in repo):** None required for static HTML in v1; optional Playwright smoke later (out of scope unless requested).

---

## Files & Migration

- **Primary deliverable:** Refactor/replace `public/explorer.html` into a maintainable structure:
  - Option A: Single HTML with Alpine + modular `<script type="module">` imports from `public/js/explorer/*.js` (new files).
  - Option B: `explorer.html` + `public/css/explorer.css` + split JS modules.
- **GeoJSON:** Add `public/data/us-counties.geojson` (or split by state if size is an issue) — exact asset strategy in implementation plan.
- **Do not** break Firebase Hosting paths or `firebase.json` rewrites without updating deploy docs.

---

## Open Points (resolved in brainstorming)

- **Self-serve for all team members:** Yes.
- **Dashboard + Map primary; SQL secondary:** Yes.
- **Zillow on map hover/click:** Yes — jump to county-relevant search.
- **(i) on market signals:** Yes — in Map side panel (and align SQL schema tooltips where applicable).
- **Approach C (Alpine SPA):** Yes.
- **Hybrid client preview + pipeline lock-in:** Yes.

---

## Mockups (reference)

Brainstorm HTML mockups (local session, not shipped to production). **Locate the newest session directory** under `docs/../.superpowers/brainstorm/` (or project root `.superpowers/brainstorm/<session-id>/`). As of this spec’s authoring, files lived alongside:

- `layout-navigation.html`
- `dashboard-detail.html`
- `map-view.html`
- `sql-studio.html`

If multiple session folders exist, use the **most recently modified** directory containing these filenames.

---

## Approval

- [ ] Product / author review of this document
- [ ] Then: implementation plan (`writing-plans` skill) in `docs/superpowers/plans/`
