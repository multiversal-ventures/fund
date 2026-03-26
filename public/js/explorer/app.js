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
