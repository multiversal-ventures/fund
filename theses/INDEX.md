# Multiversal Ventures — Fund Thesis Index

> Three angles on the same fund. Each folder has its own README with full context.

Last updated: April 12, 2026

---

## The Three Workstreams

| Folder | Owner | Focus | README |
|--------|-------|-------|--------|
| `strategy/` | Akshay | Fund thesis, legal structure, investor memo, scored datasets, deal pipeline | [strategy/README.md](strategy/README.md) |
| `operations/` | Holly | Deal structuring playbook, kickoff workflow, automation cost model, market comparison | [operations/README.md](operations/README.md) |
| `data-ai-ops/` | Kartik | Data pipeline, Parquet catalog, scoring model, web dashboard, DuckDB Explorer | [data-ai-ops/README.md](data-ai-ops/README.md) |

---

## How They Connect

```
Strategy (why)          Operations (how)         Data/AI Ops (what)
─────────────           ────────────────         ──────────────────
3 investment theses  →  Deal playbook         →  Census/BLS/HUD pipeline
LP pitch & legal     →  Kickoff workflow      →  Scoring engine (market + deal)
Scored ZIP targets   →  Market comparison     →  Parquet → Firebase → DuckDB
                        Automation cost model  →  Explorer dashboard (live)
                                                 DC adjacency scoring
                                                 Cloud Functions (Tavily news)
                                                 CI/CD (GitHub Actions)
```

**Strategy** defines the thesis. **Operations** defines execution. **Data/AI Ops** makes it queryable, falsifiable, and live.

---

## The Three Investment Theses

1. **The Invisible Supply Wall** — Midwest/NE secondary markets, frozen construction, 95%+ occupancy, 6.5–9.5% cap rates
2. **The Broken Capital Stack** — $310B+ matured debt, 7.15% CMBS delinquency, Sun Belt 15–30% discounts, assumable GSE loans
3. **The Hidden Cost Moat** — 150–430 bps structural OpEx advantage in low-insurance/tax inland markets

Cross-thesis overlap: Huntsville AL (all three), Boise / OKC / Little Rock / Birmingham (two each).

---

## Quick Links

| Resource | Location |
|----------|----------|
| Live dashboard | `mvv-fund.web.app` (auth-gated) |
| Fund Explorer | `mvv-fund.web.app/explorer.html` |
| DC Markets | `mvv-fund.web.app/explorer.html?tab=dc` |
| Pipeline CLI | `cd scripts && uv run python run.py --help` |
| Tests | `cd scripts && uv run pytest tests/ -v` |
| Deploy | Push to `main` triggers `deploy.yml` |
| Data refresh | Monthly cron or Firestore config change triggers `refresh.yml` |
