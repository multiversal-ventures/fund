/**
 * Fund Explorer — Alpine.js root state + DuckDB store (Task 2+).
 */
document.addEventListener('alpine:init', () => {
  Alpine.store('duck', {
    db: null,
    conn: null,
    warnings: [],
  });

  Alpine.data('explorerApp', () => ({
    activeTab: 'dashboard',
    weightsOpen: false,

    setTab(tab) {
      this.activeTab = tab;
      if (tab === 'map') {
        queueMicrotask(() => {
          if (typeof window.__explorerInitMap === 'function') window.__explorerInitMap();
          if (typeof window.__explorerMapInvalidate === 'function') window.__explorerMapInvalidate();
        });
      }
      if (tab === 'sql') {
        queueMicrotask(() => {
          if (typeof window.__explorerInitSql === 'function') window.__explorerInitSql();
          if (typeof window.__explorerSqlInvalidate === 'function') window.__explorerSqlInvalidate();
        });
      }
    },

    async savePipelineToFirestore() {
      const { savePipelineConfig } = await import('/js/explorer/scenarios.js');
      await savePipelineConfig({
        loadRunStatus: typeof window.__explorerLoadRunStatus === 'function' ? window.__explorerLoadRunStatus : undefined,
      });
    },

    dismissDuckWarnings() {
      Alpine.store('duck').warnings = [];
      if (window.__explorerDuckdb) window.__explorerDuckdb.warnings = [];
    },
  }));
});
