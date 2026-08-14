let context;
let feature;

export async function mount(next) {
  context = next;
  if (context.page === 'ledger') {
    const [{loadAdvancedCharts}, ledger] = await Promise.all([
      import('../advanced-charts.js'), import('../ledger-import.js'),
    ]);
    feature = ledger;
    await Promise.all([loadAdvancedCharts(), feature.mount()]);
    await loadLedger();
    return;
  }
  await context.loadStyle('/static/trading.css');
  const [{loadAdvancedCharts}, trading] = await Promise.all([
    import('../advanced-charts.js'), import('../trading.js'),
  ]);
  await loadAdvancedCharts();
  feature = trading;
  await feature.mount?.(context.page);
}

export async function unmount() {
  await feature?.unmount?.();
  window.QuantCharts?.activateTab('');
}

export async function refresh() {
  if (!context) return;
  if (context.page === 'ledger') await loadLedger();
  else await feature?.refresh?.(context.page);
}
