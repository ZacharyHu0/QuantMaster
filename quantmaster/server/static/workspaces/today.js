let context;
const features = new Map();
const featureRetries = new Map();
let mountedFeature = null;

const FEATURE_RESOURCES = {
  market: ['/static/market-workbench.css', '../market-workbench.js'],
  quotes: ['/static/market-workbench.css', '../market-workbench.js'],
  temperature: ['/static/market-workbench.css', '../market-workbench.js'],
  style: ['/static/market-workbench.css', '../market-workbench.js'],
  rotation: ['/static/market-workbench.css', '../market-workbench.js'],
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
    const retry = featureRetries.get(modulePath) || 0;
    const pending = Promise.all([
      context.shell.loadStyle(resources[0]),
      import(`${modulePath}${retry ? `?retry=${retry}` : ''}`),
    ]).then(([, module]) => module).catch(error => {
      if (features.get(modulePath) === pending) features.delete(modulePath);
      featureRetries.set(modulePath, retry + 1);
      throw error;
    });
    features.set(modulePath, pending);
  }
  return features.get(modulePath);
}

async function loadPage(page) {
  if (['market', 'quotes', 'temperature', 'style', 'rotation', 'industry', 'themes', 'etfs', 'news'].includes(page)) {
    const {loadAdvancedCharts} = await import('../advanced-charts.js');
    const [, module] = await Promise.all([loadAdvancedCharts(), feature(page)]);
    await module?.mount?.(page, context);
    mountedFeature = module;
    return;
  }
  if (page === 'decision') {
    const {loadAdvancedCharts} = await import('../advanced-charts.js');
    await loadAdvancedCharts();
  }
  const module = await feature(page);
  await module?.mount?.(page);
  mountedFeature = module;
  if (page === 'decision') await context.shell.loadDecisionHistory();
}

export async function mount(next) {
  context = next;
  mountedFeature = null;
  await loadPage(context.page);
}

export async function unmount() {
  await mountedFeature?.unmount?.();
  mountedFeature = null;
  await context?.shell.disposeToday();
  context?.shell.deactivateCharts();
}

export async function refresh() {
  if (!context) return;
  const module = await feature(context.page);
  if (module?.refresh) await module.refresh();
  else await loadPage(context.page);
}
