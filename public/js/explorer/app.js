/**
 * Fund Explorer — Alpine.js root state + DuckDB store (Task 2+).
 */
function explorerImport(url) {
  const v =
    typeof window !== 'undefined' && window.__explorerAssetV
      ? String(window.__explorerAssetV)
      : '1';
  return import(`${url}?v=${encodeURIComponent(v)}`);
}

document.addEventListener('alpine:init', () => {
  Alpine.store('duck', {
    db: null,
    conn: null,
    warnings: [],
  });

  Alpine.data('explorerApp', () => ({
    activeTab: 'dashboard',
    weightsOpen: false,
    /** True when DOM weights differ from last baseline (initial load or successful pipeline save). */
    estimatedPreview: false,
    weightsBaselineHash: '',

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
      if (tab === 'dc') {
        queueMicrotask(() => {
          if (typeof window.__explorerRefreshDc === 'function') window.__explorerRefreshDc();
        });
      }
    },

    async savePipelineToFirestore() {
      const { savePipelineConfig, hashPipelineWeightsFromDom } = await explorerImport('/js/explorer/scenarios.js');
      const ok = await savePipelineConfig({
        loadRunStatus: typeof window.__explorerLoadRunStatus === 'function' ? window.__explorerLoadRunStatus : undefined,
      });
      if (ok) {
        this.weightsBaselineHash = hashPipelineWeightsFromDom();
        this.estimatedPreview = false;
        window.__explorerEstimatedPreview = false;
        explorerImport('/js/explorer/map.js')
          .then((m) => m.refreshMapLegendIfAny?.())
          .catch(() => {});
      }
    },

    async setWeightsBaselineFromCurrentDom() {
      const { hashPipelineWeightsFromDom } = await explorerImport('/js/explorer/scenarios.js');
      this.weightsBaselineHash = hashPipelineWeightsFromDom();
      this.estimatedPreview = false;
      window.__explorerEstimatedPreview = false;
      explorerImport('/js/explorer/map.js')
        .then((m) => m.refreshMapLegendIfAny?.())
        .catch(() => {});
    },

    async syncEstimatedFromDom() {
      const { hashPipelineWeightsFromDom } = await explorerImport('/js/explorer/scenarios.js');
      const h = hashPipelineWeightsFromDom();
      if (this.weightsBaselineHash === '') {
        this.estimatedPreview = false;
      } else {
        this.estimatedPreview = h !== this.weightsBaselineHash;
      }
      window.__explorerEstimatedPreview = this.estimatedPreview;
      explorerImport('/js/explorer/map.js')
        .then((m) => m.refreshMapLegendIfAny?.())
        .catch(() => {});
    },

    dismissDuckWarnings() {
      Alpine.store('duck').warnings = [];
      if (window.__explorerDuckdb) window.__explorerDuckdb.warnings = [];
    },
  }));

  window.__explorerSyncEstimated = () => {
    const root = document.querySelector('.explorer-root');
    if (!root || typeof Alpine === 'undefined' || !Alpine.$data) return;
    const d = Alpine.$data(root);
    if (d && typeof d.syncEstimatedFromDom === 'function') d.syncEstimatedFromDom();
  };
});
