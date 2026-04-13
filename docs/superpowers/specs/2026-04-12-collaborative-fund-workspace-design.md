# Collaborative Fund Workspace — Design Spec

> Replace the current index.html + explorer.html with a unified React SPA where strategy, operations, and data/AI ops converge on shared data, thesis-driven layers, and partner-ready presentations.

**Date:** 2026-04-12
**Authors:** Kartik, Akshay, Holly
**Status:** Draft

---

## Problem

The fund has three workstreams (strategy, operations, data/AI ops) with overlapping data and complementary theses. Today, each workstream's artifacts are siloed:

- Strategy docs (PDFs, Excel scoring models) live in `theses/strategy/`
- Operations docs (cost models, market comparisons) live in `theses/operations/` and `theses/cost-model/`
- Pipeline data (Parquet, DuckDB, Explorer dashboard) lives in `scripts/` + `public/`

There is no shared surface where team members can layer their theses on top of the same data, find county-level intersections, or present a unified story to fund partners. The current Explorer is a single-user data tool, not a collaboration platform.

## Goal

A single workspace where:

1. All three team members can upload their own data (XLSX/CSV) and it becomes instantly queryable alongside pipeline Parquet
2. Theses are first-class objects — layers on a map, tags on analyses, not people's names
3. Natural language querying (Gemini) lowers the barrier so non-SQL users can explore
4. Saved analyses accumulate institutional knowledge organized by thesis
5. County-level map views with toggleable thesis layers surface intersections
6. Curated presentations with one-time links let partners see the story without needing accounts

## Non-Goals

- Real-time collaboration (Google Docs style) — team is 3 people, async is fine
- Mobile-first design — this is a desktop research tool
- Replacing the Python pipeline — pipeline continues to produce Parquet; the SPA consumes it
- Property management or portfolio tracking post-acquisition

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Framework | React 18+ (Vite) |
| Styling | Tailwind CSS |
| Routing | React Router |
| In-browser SQL | DuckDB-WASM |
| Maps | Leaflet (react-leaflet) |
| SQL editor | CodeMirror |
| Spreadsheet export | SheetJS |
| NL→SQL | Firebase AI SDK (Gemini) |
| Auth | Firebase Auth (Google OAuth, @multiversal.ventures allowlist) |
| Storage | Firebase Storage (Parquet files — pipeline + uploads) |
| Database | Firestore (analyses, layers, uploads metadata, shares, decks, config) |
| Functions | Cloud Functions v2 (Node.js 22) |
| Hosting | Firebase Hosting |
| CI/CD | GitHub Actions |

---

## Architecture

```
Team uploads (XLSX/CSV)
  │
  ▼
Firebase Storage (gs://mvv-fund/data/)
  ├── census/      Parquet (pipeline)
  ├── bls/         Parquet (pipeline)
  ├── hud/         Parquet (pipeline)
  ├── scored/      Parquet (pipeline)
  ├── dc/          Parquet (pipeline)
  └── uploads/     team XLSX/CSV → converted to Parquet in-browser
  │
  ▼
DuckDB-WASM (in-browser)
  Loads ALL Parquet tables (pipeline + uploads) into SQL engine
  Team members query across everything with SQL or Gemini NL
  │
  ▼
React SPA (Vite + Tailwind)
  ├── Map        Leaflet choropleth, toggle thesis layers, county drill-down
  ├── Data       Spreadsheet view (SheetJS), filter/sort/export
  ├── Query      NL input (Gemini) + SQL editor, saved analyses
  ├── County     Drill-down: all scores, properties, news
  ├── SQL Studio Full SQL editor with schema browser
  ├── Sources    Pipeline tables + team uploads management
  └── Present    Curate decks, generate one-time partner links
  │
  ▼
Firestore
  ├── analyses/  saved queries (name, SQL, thesis tag, owner)
  ├── layers/    scoring layer configs (source table, score column, color)
  ├── uploads/   upload metadata (table name, columns, uploader)
  ├── shares/    one-time partner links (token, expiry, view count)
  ├── decks/     presentation slide lists
  └── config/    pipeline config (existing)
  │
  ▼
Firebase AI (Gemini)
  NL → SQL generation using table schemas as context
  Runs client-side via Firebase AI SDK

Cloud Functions (existing + new)
  ├── triggerPipelineRefresh   (existing)
  ├── dcLocalNews              (existing)
  └── createShareLink          (new: generates one-time tokens)
```

---

## Navigation

App shell: left sidebar + main content area.

```
┌─────────────────┬────────────────────────────────────────────┐
│  MVV Fund       │                                            │
│  user@mv.vc     │          Main Content Area                 │
│                 │                                            │
│  EXPLORE        │          (Map / Data / County)             │
│    🗺 Map       │                                            │
│    📊 Data      │                                            │
│    🔍 County    │                                            │
│                 │                                            │
│  ANALYZE        │                                            │
│    💬 Query     │                                            │
│    📁 Analyses  │                                            │
│    ⚡ SQL Studio│                                            │
│                 │                                            │
│  COLLABORATE    │                                            │
│    📤 Sources   │                                            │
│    🎯 Present   │                                            │
└─────────────────┴────────────────────────────────────────────┘
```

Three sections:
- **Explore** — visual, spatial, county-centric views
- **Analyze** — query, save, build institutional knowledge
- **Collaborate** — manage data sources, build partner presentations

---

## Views

### Map (primary workspace)

Three-panel layout: layer controls (left), Leaflet choropleth (center), selected county summary (right).

**Layer panel (left):**
- Each thesis is a toggleable layer with a color, on/off switch, and optional score threshold slider
- Layers come from two sources: pipeline tables (market_scores, dc_market_scores) and team uploads that have a FIPS column + score column
- "Add Layer from Data Source" button lets you turn any uploaded table into a map layer by selecting the FIPS and score columns
- Intersection counter: shows how many counties pass all active layers above their thresholds

**Map (center):**
- Leaflet choropleth of US counties
- Single layer active: counties colored by that layer's score (0–100 gradient)
- Multiple layers active: counties colored by minimum score across active layers; gold highlight on counties passing all thresholds
- Click county → popup showing all layer scores for that county
- Legend adapts to active layer(s)

**County summary (right):**
- Populates when a county is clicked
- Shows key metrics (pop, vacancy, rent, growth, tax rate)
- Shows thesis alignment (which theses this county scores well on)
- "Full Detail" navigates to County page; "Add to Deck" adds to a presentation

### Data

Spreadsheet-like view of any table (pipeline or uploaded). Powered by DuckDB query results rendered in a sortable/filterable table.

- Table selector dropdown (all available tables)
- Column visibility toggles
- Sort by any column, filter rows
- Export to XLSX via SheetJS
- Click a row with FIPS → navigate to County detail

### County (/county/:fips)

Everything known about one county, across all theses and data sources.

- Thesis scores: bar chart showing score per thesis, with "no data" for theses without coverage
- Demographics: population, growth, employment, HHI
- Housing: vacancy, renter households, MF stock, permits/stock ratio
- Economics: median rent, rent growth, rent-to-cost ratio, effective tax rate
- Properties (HUD FHA): table of properties in this county with deal scores, maturity dates, Section 8 flags
- Local news (Tavily): recent news via existing dcLocalNews Cloud Function
- "Add to Deck" button for presentations

### Query

Natural language + SQL querying with saved analyses.

**Left sidebar:** saved analyses organized by thesis tag. Click to load.

**Main area:**
1. NL input bar — type a question, click "Generate SQL"
2. Gemini (Firebase AI SDK) receives the question + all table schemas as context, returns DuckDB-compatible SQL
3. Generated SQL shown in an editable CodeMirror block
4. "Run" executes against DuckDB-WASM
5. Results rendered as sortable table
6. Action buttons: Pin to Map (creates a layer from results), Export XLSX, Save Analysis

**Save Analysis dialog:** name, thesis tag (dropdown), description. Stored in Firestore `analyses/` collection.

### SQL Studio

Power-user SQL editor — carries forward from today's Explorer.

- Full CodeMirror editor with DuckDB SQL syntax highlighting
- Schema browser panel: all tables with column names and types
- Query presets dropdown (Top 40 Markets, DC Top 40, etc.)
- Results as sortable table, exportable
- "Save as Analysis" to promote an ad-hoc query

### Data Sources

Manage all data feeding the system.

**Pipeline section:** read-only list of pipeline-generated tables with record counts, last updated dates, and source descriptions.

**Team uploads section:** list of uploaded tables with record counts, uploader, and upload date. Delete/re-upload actions.

**Upload flow:**
1. Drag XLSX or CSV onto drop zone (or click to browse)
2. Preview: first 10 rows, column names and inferred types
3. Name the table (auto-suggested from filename)
4. Confirm → DuckDB-WASM converts to Parquet in-browser
5. Parquet uploaded to Firebase Storage (`data/uploads/{tableName}.parquet`)
6. Metadata record created in Firestore `uploads/` collection
7. Table immediately available to all team members on page refresh

### Present

Curate saved analyses and county views into shareable decks for partners.

**Deck list:** existing decks with slide count, creation date, share count.

**Deck builder:**
- Add slides: choose from saved analyses (renders results table), county detail snapshots, or free-form text/markdown
- Reorder slides via drag-and-drop
- Preview: renders the deck as partners will see it

**Generate link:**
- Calls `createShareLink` Cloud Function
- Creates a Firestore `shares/` record with a unique token, optional recipient label, and expiry (default 7 days)
- Returns URL: `mvv-fund.web.app/s/{token}`
- Partners open the link — no auth required, token validated against Firestore
- View count tracked

**Share view (`/s/:token`):**
- Public route (no Firebase Auth required)
- Token validated: if expired or invalid, shows "Link expired" message
- Renders deck slides in a clean, read-only presentation layout
- No navigation sidebar, no SQL, no upload — just the curated content

---

## Firestore Schema

### analyses/{id}
```
name: string                    # "Top 40 Supply-Constrained"
sql: string                     # DuckDB SQL
nlPrompt?: string               # original NL question (if Gemini-generated)
thesis: string                  # "invisible-supply-wall" | "broken-capital-stack" |
                                # "hidden-cost-moat" | "dc-adjacency" |
                                # "ops-cost-model" | "ops-market-comparison" |
                                # "cross-thesis"
owner: string                   # email
pinToMap: boolean               # whether to show as a map layer
createdAt: timestamp
updatedAt: timestamp
```

### layers/{id}
```
name: string                    # "Invisible Supply Wall"
type: "pipeline" | "upload"     # data source type
sourceTable: string             # DuckDB table name
scoreColumn: string             # column for choropleth color
fipsColumn: string              # column with FIPS code
color: string                   # hex color for map rendering
thesis: string                  # thesis tag
createdAt: timestamp
```

### uploads/{id}
```
tableName: string               # DuckDB table name
originalFilename: string        # "thesis_scoring_model_akshay.xlsx"
storagePath: string             # gs://mvv-fund/data/uploads/...
uploadedBy: string              # email
columns: string[]               # column names
rowCount: number
createdAt: timestamp
```

### shares/{token}
```
deckId: string                  # reference to decks/{id}
recipient?: string              # optional label ("LP Alpha")
expiresAt: timestamp            # default: createdAt + 7 days
viewCount: number               # incremented on each access
createdAt: timestamp
```

### decks/{id}
```
name: string                    # "Huntsville Deal Package"
slides: [{
  type: "analysis" | "county" | "text"
  analysisId?: string           # ref to analyses/{id}
  fips?: string                 # county FIPS for county slides
  text?: string                 # markdown for text slides
  order: number
}]
owner: string                   # email
createdAt: timestamp
updatedAt: timestamp
```

---

## React Component Architecture

```
src/
├── main.tsx                     # Vite entry, React root
├── App.tsx                      # Auth gate + app shell (sidebar + router)
├── lib/
│   ├── duckdb.ts               # DuckDB-WASM init, Parquet loading, query()
│   ├── firebase.ts             # Firebase app init (auth, firestore, storage)
│   ├── gemini.ts               # Firebase AI SDK — NL→SQL with schema context
│   └── share.ts                # One-time link generation (calls Cloud Function)
├── components/
│   ├── Sidebar.tsx             # Navigation (Explore / Analyze / Collaborate)
│   ├── LayerPanel.tsx          # Map layer toggles + threshold sliders + intersection count
│   ├── CountyPopup.tsx         # Map popup with multi-thesis scores
│   ├── CountyDetail.tsx        # Full county drill-down (used in CountyPage)
│   ├── DataTable.tsx           # Sortable/filterable table (reused across views)
│   ├── SqlEditor.tsx           # CodeMirror SQL editor (reused in Query + SQL Studio)
│   ├── QueryBar.tsx            # NL input + Gemini generate + SQL preview
│   ├── AnalysisCard.tsx        # Saved analysis in sidebar list
│   ├── UploadDialog.tsx        # Drag-drop XLSX/CSV → preview → name → confirm
│   ├── DeckBuilder.tsx         # Drag-reorder slides for presentations
│   └── ShareLinkDialog.tsx     # Generate link with expiry + recipient label
├── pages/
│   ├── MapPage.tsx             # Leaflet + LayerPanel + CountyPopup + county summary
│   ├── DataPage.tsx            # Table selector + DataTable spreadsheet view
│   ├── CountyPage.tsx          # CountyDetail (route: /county/:fips)
│   ├── QueryPage.tsx           # QueryBar + results + saved analyses sidebar
│   ├── SqlStudioPage.tsx       # Full SQL editor + schema browser + presets
│   ├── DataSourcesPage.tsx     # Pipeline tables + uploads list + UploadDialog
│   ├── PresentPage.tsx         # Deck list + DeckBuilder
│   └── ShareView.tsx           # Public route /s/:token — read-only deck
└── hooks/
    ├── useDuckDB.ts            # query() wrapper with loading/error state
    ├── useAnalyses.ts          # Firestore CRUD for analyses collection
    ├── useLayers.ts            # Layer state (active toggles, thresholds, colors)
    ├── useUpload.ts            # Upload flow state machine
    └── useCounty.ts            # Aggregate all data for a single FIPS
```

---

## Cloud Functions

### Existing (unchanged)
- `triggerPipelineRefresh` — Firestore config write → dispatches GitHub Actions refresh.yml
- `dcLocalNews` — HTTP POST, Tavily news search by state/county

### New
- `createShareLink` — HTTP POST (auth'd). Creates a `shares/{token}` Firestore doc with a crypto-random token, expiry, and deckId. Returns the share URL.

---

## Migration Path

The existing Explorer and index.html continue working during development. The new React SPA is built alongside them:

1. New Vite project in `workspace/` (or `app/`) directory
2. Firebase Hosting configured to serve the React build at root, with existing pages moved to `/legacy/` routes
3. DuckDB init logic extracted from `public/js/explorer/duckdb.js` into `src/lib/duckdb.ts` (same Parquet loading, same Firebase Storage paths)
4. Firestore rules updated to allow `analyses/`, `layers/`, `uploads/`, `shares/`, `decks/` collections
5. Once the React SPA covers all Explorer functionality, legacy pages are retired

---

## Investment Theses (Layer Names)

These are the canonical thesis names used as layer labels and analysis tags throughout the app:

| Key | Display Name | Source |
|-----|-------------|--------|
| `invisible-supply-wall` | Invisible Supply Wall | Pipeline: market_scores |
| `broken-capital-stack` | Broken Capital Stack | Pipeline: market_scores + properties |
| `hidden-cost-moat` | Hidden Cost Moat | Pipeline: market_scores |
| `dc-adjacency` | DC Adjacency | Pipeline: dc_market_scores |
| `ops-cost-model` | Ops Cost Model | Upload: RE_Fund_Automation_Cost_Model.xlsx |
| `ops-market-comparison` | Ops Market Comparison | Upload: RE_Fund_Market_Comparison_HLy.xlsx |
| `cross-thesis` | Cross-Thesis | Analyses spanning multiple theses |

---

## Success Criteria

1. All three team members can upload their data and query across the combined dataset without help
2. Map view shows county-level intersections across 2+ thesis layers with adjustable thresholds
3. Non-SQL users can ask natural language questions and get useful results via Gemini
4. Saved analyses accumulate and are browsable by thesis
5. Partners can view curated presentations via one-time links without needing accounts
6. Existing pipeline (Python scripts, Parquet generation, Firebase upload) continues unchanged
