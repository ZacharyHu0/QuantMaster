let context;
const features = new Map();

const FEATURE_RESOURCES = {
  temperature: ['/static/rotation.css', '../rotation.js'],
  style: ['/static/rotation.css', '../rotation.js'],
  rotation: ['/static/rotation.css', '../rotation.js'],
  industry: ['/static/rotation.css', '../rotation.js'],
  themes: ['/static/rotation.css', '../rotation.js'],
  etfs: ['/static/rotation.css', '../rotation.js'],
  news: ['/static/news.css', '../news.js'],
  'after-close': ['/static/after-close.css', '../after-close.js'],
  candidates: ['/static/candidates.css', '../candidates.js'],
  'stock-analysis': ['/static/stock-analysis.css', '../stock-analysis.js'],
};

async function feature(page) {
  const resources = FEATURE_RESOURCES[page];
  if (!resources) return null;
  const [, modulePath] = resources;
  if (!features.has(modulePath)) {
    features.set(modulePath, Promise.all([
      context.loadStyle(resources[0]),
      import(modulePath),
    ]).then(([, module]) => module));
  }
  return features.get(modulePath);
}

async function loadPage(page) {
  if (page === 'quotes') {
    document.querySelectorAll('[data-market-view]').forEach(view => {
      view.hidden = view.dataset.marketView !== page;
    });
    await Promise.all([loadAssetLists(false), loadMarket()]);
    return;
  }
  if (['temperature', 'style', 'rotation', 'industry', 'themes', 'etfs', 'news'].includes(page)) {
    const {loadAdvancedCharts} = await import('../advanced-charts.js');
    const [, module] = await Promise.all([loadAdvancedCharts(), feature(page)]);
    await module?.mount?.(page);
    return;
  }
  if (page === 'decision') {
    const {loadAdvancedCharts} = await import('../advanced-charts.js');
    await loadAdvancedCharts();
  }
  const module = await feature(page);
  await module?.mount?.(page);
  if (page === 'decision' && !decisionLoaded && !decisionLoading) await loadDecisionHistory();
}

export async function mount(next) {
  context = next;
  await loadPage(context.page);
}

export async function unmount() {
  const module = await feature(context?.page);
  await module?.unmount?.();
  const charts = await import('../today-charts.js');
  charts.disposeTodayCharts(document.getElementById('tab-market'));
  window.QuantCharts?.activateTab('');
}

export async function refresh() {
  if (!context) return;
  const module = await feature(context.page);
  if (module?.refresh) await module.refresh();
  else await loadPage(context.page);
}
