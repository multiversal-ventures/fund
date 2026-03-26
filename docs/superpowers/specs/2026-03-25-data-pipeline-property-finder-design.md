# Data Pipeline & Property Finder — Design Spec

**Date:** 2026-03-25
**Status:** Approved
**Authors:** Kartik + Claude

## Overview

Two systems in one repo: (1) an automated data refresh pipeline that pulls Census/BLS/HUD data into per-year Parquet files, and (2) a property finder that scores multifamily acquisition targets from free public data sources. Both feed a DuckDB WASM-powered browser explorer with configurable parameters, Zillow deep-links, and Firebase auth gating.

## Architecture

```
fund/
├── scripts/                    # Python data pipeline (managed with uv)
│   ├── pyproject.toml          # uv project config + dependencies
│   ├── run.py                  # CLI entry point
│   ├── config_loader.py        # Load from local YAML or Firestore
│   ├── pull_census.py          # ACS 5-Year tables → per-year parquet
│   ├── pull_bls.py             # OEWS data → per-year parquet
│   ├── pull_hud.py             # FHA multifamily DB + USPS vacancy
│   ├── score.py                # Acquisition signal scoring
│   ├── zillow.py               # Build Zillow deep-link URLs
│   ├── upload.py               # Push parquet → Firebase Storage
│   └── config.default.yaml     # Default markets + weights
├── functions/
│   └── index.js                # Cloud Function: Firestore onWrite
│                               # triggers GitHub workflow_dispatch
├── public/
│   ├── index.html              # Existing auth-gated dashboard
│   ├── explorer.html           # NEW: DuckDB WASM query UI
│   └── *.html                  # Existing 6 reports
├── .github/workflows/
│   ├── refresh.yml             # Monthly cron + workflow_dispatch
│   └── deploy.yml              # Firebase deploy on push to main
├── firebase.json
├── firestore.rules
└── storage.rules
```

## CLI Usage

```bash
# Full refresh locally
uv run scripts/run.py --all

# Individual steps
uv run scripts/run.py --census
uv run scripts/run.py --bls
uv run scripts/run.py --hud
uv run scripts/run.py --score
uv run scripts/run.py --upload

# Config source
uv run scripts/run.py --all --config scripts/config.default.yaml   # local
uv run scripts/run.py --all --config firestore                     # remote

# Output locally without uploading
uv run scripts/run.py --all --local-only --output ./data/
```
