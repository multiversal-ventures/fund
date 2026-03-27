/**
 * SQL Studio — CodeMirror 6, schema browser, DuckDB run/export.
 */
import { basicSetup } from 'https://esm.sh/codemirror@6.0.1';
import { EditorView, keymap } from 'https://esm.sh/@codemirror/view@6.36.0';
import { EditorState, Prec } from 'https://esm.sh/@codemirror/state@6.5.0';
import { sql } from 'https://esm.sh/@codemirror/lang-sql@6.6.3';
import { oneDark } from 'https://esm.sh/@codemirror/theme-one-dark@6.1.2';

import { runQuery as duckRunQuery } from '/js/explorer/duckdb.js?v=20260327-10';
import { getConn, getDb } from '/js/explorer/duckdb.js?v=20260327-10';
import { VIEW_QUERIES } from '/js/explorer/scenarios.js?v=20260327-10';
import { getCodemirrorSqlSchema, renderSchemaSidebarHtml } from '/js/explorer/schema.js?v=20260327-10';
import { renderResultsTable, renderQueryError, exportLastResultCsv, exportLastResultParquet } from '/js/explorer/results.js?v=20260327-10';

/** @type {EditorView | null} */
let _editor = null;
let _mounted = false;

function getSqlEls() {
  return {
    countEl: document.getElementById('sql-result-count'),
    thead: document.querySelector('#sql-results-table thead'),
    tbody: document.querySelector('#sql-results-table tbody'),
  };
}

async function runSqlFromStudio() {
  const c = getConn();
  const sqlText = _editor?.state.doc.toString().trim() ?? '';
  const els = getSqlEls();
  if (!sqlText) {
    renderQueryError('Enter a SQL query.', els);
    return;
  }
  if (!c) {
    renderQueryError('Database not ready yet.', els);
    return;
  }
  try {
    const result = await duckRunQuery(c, sqlText);
    renderResultsTable(result, { sql: sqlText, countEl: els.countEl, thead: els.thead, tbody: els.tbody });
  } catch (err) {
    renderQueryError(err.message || String(err), els);
  }
}

function insertText(text) {
  if (!_editor) return;
  const pos = _editor.state.selection.main.head;
  const needsSpace = text && !/^\s/.test(text);
  const insert = needsSpace && pos > 0 ? ` ${text}` : text;
  _editor.dispatch({
    changes: { from: pos, to: pos, insert },
    selection: { anchor: pos + insert.length },
  });
  _editor.focus();
}

function wireSchemaInteractions(host) {
  host.querySelectorAll('.schema-col-row').forEach((row) => {
    row.addEventListener('dblclick', () => {
      const name = row.querySelector('.schema-col-name')?.textContent?.trim();
      if (name) insertText(name);
    });
  });
  host.querySelectorAll('.schema-table-block').forEach((block) => {
    const hdr = block.querySelector('.schema-table-header');
    const tableName = block.getAttribute('data-table');
    hdr?.addEventListener('dblclick', (e) => {
      e.preventDefault();
      if (tableName) insertText(tableName);
    });
  });
}

function mountSchemaSidebar() {
  const mount = document.getElementById('schema-browser-mount');
  const search = document.getElementById('schema-search');
  if (!mount) return;

  const render = (q) => {
    mount.innerHTML = renderSchemaSidebarHtml(q);
    wireSchemaInteractions(mount);
  };
  render('');
  search?.addEventListener('input', () => render(search.value));
}

/**
 * @returns {EditorView | null}
 */
export function initSqlStudioIfNeeded() {
  const root = document.getElementById('sql-editor-root');
  if (!root) return _editor;
  if (_mounted) {
    _editor?.requestMeasure();
    return _editor;
  }

  const cmSchema = getCodemirrorSqlSchema();
  const extensions = [
    basicSetup,
    oneDark,
    sql({ schema: cmSchema, upperCaseKeywords: true }),
    Prec.highest(
      keymap.of([
        {
          key: 'Mod-Enter',
          run: () => {
            runSqlFromStudio();
            return true;
          },
        },
      ]),
    ),
    EditorView.theme({
      '&': { height: '100%' },
      '.cm-scroller': { fontFamily: "ui-monospace, 'SF Mono', Menlo, monospace", fontSize: '13px' },
    }),
    EditorView.lineWrapping,
  ];

  const state = EditorState.create({
    doc: VIEW_QUERIES.combined,
    extensions,
  });

  _editor = new EditorView({ state, parent: root });
  _mounted = true;

  mountSchemaSidebar();

  document.getElementById('sql-run-btn')?.addEventListener('click', () => runSqlFromStudio());

  document.querySelectorAll('.sql-preset').forEach((btn) => {
    btn.addEventListener('click', () => {
      const q = btn.getAttribute('data-sql');
      if (!q || !_editor) return;
      _editor.dispatch({
        changes: { from: 0, to: _editor.state.doc.length, insert: q },
      });
      runSqlFromStudio();
    });
  });

  document.getElementById('sql-export-csv')?.addEventListener('click', () => exportLastResultCsv());
  document.getElementById('sql-export-parquet')?.addEventListener('click', async () => {
    const c = getConn();
    const db = getDb();
    await exportLastResultParquet(c, db);
  });

  return _editor;
}

export function notifySqlTabVisible() {
  _editor?.requestMeasure();
}
