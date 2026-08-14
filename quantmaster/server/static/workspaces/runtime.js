let context;
let feature;

const RESOURCES = {
  automation: ['/static/automation.css', '../automation.js'],
  help: ['/static/help.css', '../help.js'],
  settings: ['/static/settings.css', '../settings.js'],
};

async function loadFeature(page) {
  const [style, module] = RESOURCES[page];
  await context.loadStyle(style);
  return import(module);
}

export async function mount(next) {
  context = next;
  feature = await loadFeature(context.page);
  await feature.mount?.();
}

export async function unmount() {
  await feature?.unmount?.();
  window.QuantCharts?.activateTab('');
}

export async function refresh() {
  if (!context) return;
  await feature?.refresh?.();
}
