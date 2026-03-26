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

    setTab(tab) {
      this.activeTab = tab;
    },

    dismissDuckWarnings() {
      Alpine.store('duck').warnings = [];
      if (window.__explorerDuckdb) window.__explorerDuckdb.warnings = [];
    },
  }));
});
