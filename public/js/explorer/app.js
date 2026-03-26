/**
 * Fund Explorer — Alpine.js root state (Task 1 shell).
 * DuckDB init remains in explorer.html until Task 2 extracts it.
 */
document.addEventListener('alpine:init', () => {
  Alpine.data('explorerApp', () => ({
    activeTab: 'dashboard',

    setTab(tab) {
      this.activeTab = tab;
    },
  }));
});
