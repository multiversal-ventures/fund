/**
 * DuckDB WASM init and query helpers for Fund Explorer.
 */
import * as duckdb from 'https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.29.0/+esm';

let _db = null;
let _conn = null;

export function getDb() {
  return _db;
}

export function getConn() {
  return _conn;
}

/**
 * SQL string literal for DuckDB (use single quotes; double quotes are identifiers).
 * @param {string|null|undefined} value
 */
export function sqlStringLiteral(value) {
  if (value == null) return "''";
  return `'${String(value).replace(/'/g, "''")}'`;
}

/** @param {*} user Firebase compat user */
export async function initDuckDB(user) {
  const warnings = [];

  await user.getIdToken();

  const DUCKDB_BUNDLES = duckdb.getJsDelivrBundles();
  const bundle = await duckdb.selectBundle(DUCKDB_BUNDLES);
  const workerResp = await fetch(bundle.mainWorker);
  const workerBlob = new Blob([await workerResp.text()], { type: 'application/javascript' });
  const workerUrl = URL.createObjectURL(workerBlob);
  const worker = new Worker(workerUrl);
  const logger = new duckdb.ConsoleLogger();
  _db = new duckdb.AsyncDuckDB(logger, worker);
  await _db.instantiate(bundle.mainModule, bundle.pthreadWorker);
  _conn = await _db.connect();

  const storageRef = firebase.storage().ref();

  const files = [
    { path: 'data/dc/dc_market_scores.parquet', table: 'dc_market_scores' },
    { path: 'data/dc/dc_tavily_state.parquet', table: 'dc_tavily_state' },
    { path: 'data/scored/properties.parquet', table: 'properties' },
    { path: 'data/census/acs_2023.parquet', table: 'census_2023' },
    { path: 'data/census/acs_2022.parquet', table: 'census_2022' },
    { path: 'data/census/acs_2021.parquet', table: 'census_2021' },
    { path: 'data/hud/fha_multifamily.parquet', table: 'hud_fha' },
    { path: 'data/hud/usps_vacancy.parquet', table: 'usps_vacancy' },
    { path: 'data/census/occupations_2023.parquet', table: 'occupations_2023' },
    { path: 'data/census/occupations_2022.parquet', table: 'occupations_2022' },
    { path: 'data/census/occupations_2021.parquet', table: 'occupations_2021' },
    { path: 'data/scored/market_scores.parquet', table: 'market_scores' },
    { path: 'data/cbp/employment_2023.parquet', table: 'cbp_2023' },
    { path: 'data/permits/permits_2023.parquet', table: 'permits_2023' },
  ];

  for (const f of files) {
    try {
      const url = await storageRef.child(f.path).getDownloadURL();
      const resp = await fetch(url);
      if (!resp.ok) {
        throw new Error(`HTTP ${resp.status} ${resp.statusText || ''}`.trim());
      }
      const buffer = await resp.arrayBuffer();
      await _db.registerFileBuffer(`${f.table}.parquet`, new Uint8Array(buffer));
      await _conn.query(`CREATE TABLE ${f.table} AS SELECT * FROM '${f.table}.parquet'`);
    } catch (e) {
      const detail = e?.code ? `${e.code}: ${e.message}` : e?.message || String(e);
      const msg = `Skipping ${f.path}: ${detail}`;
      console.warn(msg);
      warnings.push(msg);
    }
  }

  return { db: _db, conn: _conn, warnings };
}

/** @param {*} conn DuckDB WASM connection */
export async function runQuery(conn, sql) {
  if (!conn) {
    throw new Error('No DuckDB connection');
  }
  return conn.query(sql);
}
