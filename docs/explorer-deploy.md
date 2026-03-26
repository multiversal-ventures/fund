# Fund Explorer — deploy & verification (Task 9)

## Hosting (Firebase)

- **Static files win:** Anything that exists under `public/` is served as a file **before** the catch-all rewrite to `index.html` in `firebase.json`. No change required for the explorer shell.
- **URLs:** Use **`/explorer.html`** (see `public/index.html` link). Paths such as `/css/explorer.css`, `/js/explorer/*.js` resolve to files on disk.
- **County GeoJSON:** Loaded from jsDelivr in `map.js` (not `public/data/`). DuckDB Parquet still comes from Firebase Storage per `duckdb.js`.
- **ESM / CDN:** CodeMirror and DuckDB load from CDNs; ensure ad blockers or CSP in production do not block `esm.sh`, `cdn.jsdelivr.net`, etc., if you add strict CSP later.

## Manual smoke test (before / after deploy)

| Area | Check |
|------|--------|
| Auth | Allowlisted Google user reaches app; others signed out with message. |
| Tabs | Dashboard / Map / SQL switch without full reload. |
| Dashboard | Scenario pills, weights panel, query bar, results, charts, exports. |
| Estimated | Badge after changing weights; clears after **Save & Refresh Pipeline** (if Firestore works). |
| Map | Counties draw; hover tooltip + Zillow link; click panel; filters / color by. |
| SQL Studio | Editor runs; presets; schema search; double-click insert; CSV/Parquet export. |
| Pipeline status | `last_run.json` line updates when Storage path is valid. |

## Deploy command (typical)

```bash
firebase deploy --only hosting
```

(Adjust if you deploy functions / rules in the same project.)

## Tag (optional)

After verification on staging/production:

```bash
git tag explorer-ui-2026-03-26
```

Replace date as appropriate.
