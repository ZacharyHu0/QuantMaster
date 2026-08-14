let context;
let feature;

export async function mount(next) {
  context = next;
  if (context.page === 'lab') {
    await context.shell.loadStyle('/static/lab.css');
    const [{loadAdvancedCharts}, lab] = await Promise.all([
      import('../advanced-charts.js'), import('../lab.js'),
    ]);
    await loadAdvancedCharts();
    feature = lab;
  } else {
    await context.shell.loadStyle('/static/trading.css');
    const [{loadAdvancedCharts}, trading] = await Promise.all([
      import('../advanced-charts.js'), import('../trading.js'),
    ]);
    await loadAdvancedCharts();
    feature = trading;
  }
  await feature.mount?.(context.page);
}

export async function unmount() {
  await feature?.unmount?.();
  context?.shell.deactivateCharts();
}

export async function refresh() {
  if (!context) return;
  await feature?.refresh?.(context.page);
}
