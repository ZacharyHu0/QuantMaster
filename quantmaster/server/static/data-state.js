/* ---------- 统一数据状态层 ---------- */

window.DataState = (() => {
  'use strict';

  const STATES = {
    LOADING: 'loading',
    READY: 'ready',
    STALE: 'stale',
    PARTIAL: 'partial',
    DEGRADED: 'degraded',
    UNAVAILABLE: 'unavailable',
    ERROR: 'error',
  };

  /** 解析服务端 snapshot 响应为统一状态。 */
  function parseSnapshot(snapshot) {
    if (!snapshot) {
      return { state: STATES.UNAVAILABLE, asOf: null, coverage: 0,
        quality: {}, formalEligible: false, provenance: '', source: '' };
    }
    const state = snapshot.state || STATES.READY;
    const asOf = snapshot.as_of || null;
    const coverage = Number(snapshot.coverage ?? snapshot.data_quality?.observed_count ?? 0);
    const quality = snapshot.data_quality || {};
    const formalEligible = Boolean(snapshot.formal_eligible ?? snapshot.state === 'ready');
    const provenance = snapshot.provenance || '';
    const source = snapshot.source || snapshot.data_quality?.source || '';
    return { state, asOf, coverage, quality, formalEligible, provenance, source };
  }

  /** 生成统一的状态描述文案。 */
  function formatLabel(state, { asOf = null, coverage = 0, formalEligible = false } = {}) {
    const labels = {
      [STATES.LOADING]: '正在加载…',
      [STATES.READY]: formalEligible ? '正式数据' : '预览数据',
      [STATES.STALE]: '缓存数据（可能已过期）',
      [STATES.PARTIAL]: '部分覆盖',
      [STATES.DEGRADED]: '降级数据',
      [STATES.UNAVAILABLE]: '暂不可用',
      [STATES.ERROR]: '读取失败',
    };
    let label = labels[state] || state;
    if (asOf) {
      const d = typeof asOf === 'string' ? asOf.slice(0, 10) : asOf;
      label += ` · 截至 ${d}`;
    }
    if (coverage) {
      label += ` · 覆盖 ${coverage} 只`;
    }
    return label;
  }

  /** 请求治理：相同 key 的并发请求合并为一次。 */
  const inflight = new Map();

  async function fetchWithState(path, options = {}) {
    const key = options.dedupKey || path;
    if (inflight.has(key)) return inflight.get(key);
    const promise = (async () => {
      const controller = new AbortController();
      const timeout = options.timeout || 15000;
      const timer = setTimeout(() => controller.abort(), timeout);
      try {
        const response = await fetch(path, {
          signal: options.signal || controller.signal,
          headers: { Accept: 'application/json' },
        });
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        const data = await response.json();
        const snapshot = data?.snapshot || data?.meta || null;
        const state = parseSnapshot(snapshot);
        return { data, snapshot, state, asOf: state.asOf };
      } finally {
        clearTimeout(timer);
        inflight.delete(key);
      }
    })();
    inflight.set(key, promise);
    return promise;
  }

  return { STATES, parseSnapshot, formatLabel, fetchWithState };
})();