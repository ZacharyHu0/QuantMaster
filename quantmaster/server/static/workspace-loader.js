const WORKSPACES = {
  today: retry => import(`./workspaces/today.js${retry ? `?retry=${retry}` : ''}`),
  research: retry => import(`./workspaces/research.js${retry ? `?retry=${retry}` : ''}`),
  account: retry => import(`./workspaces/account.js${retry ? `?retry=${retry}` : ''}`),
  runtime: retry => import(`./workspaces/runtime.js${retry ? `?retry=${retry}` : ''}`),
};

const PAGES = {
  today: {
    market: 'market', quotes: 'market', temperature: 'market', style: 'market', rotation: 'rotation',
    industry: 'rotation', themes: 'rotation', etfs: 'rotation', news: 'news',
    'after-close': 'after-close', candidates: 'candidates',
    'stock-analysis': 'stock-analysis', decision: 'decision',
  },
  research: {lab: 'lab', backtest: 'backtest'},
  account: {paper: 'paper', ledger: 'ledger'},
  runtime: {automation: 'automation', operations: 'operations', help: 'help', settings: 'settings'},
};

const DEFAULT_PAGE = {today: 'market', research: 'lab', account: 'paper', runtime: 'automation'};
const PAGE_KEY = 'quantmaster.workspacePage.v2';
const modulePromises = new Map();
const moduleRetries = new Map();
const stylePromises = new Map();
const scriptPromises = new Map();
let active = null;
let activation = 0;
let transitions = Promise.resolve();

function loadWorkspace(name) {
  if (!Object.hasOwn(WORKSPACES, name)) throw new Error(`未知工作区：${name}`);
  if (!modulePromises.has(name)) {
    const retry = moduleRetries.get(name) || 0;
    const pending = WORKSPACES[name](retry).catch(error => {
      if (modulePromises.get(name) === pending) modulePromises.delete(name);
      moduleRetries.set(name, retry + 1);
      throw error;
    });
    modulePromises.set(name, pending);
  }
  return modulePromises.get(name);
}

export function loadStyle(path) {
  if (!stylePromises.has(path)) {
    const pending = new Promise((resolve, reject) => {
      const link = document.createElement('link');
      link.rel = 'stylesheet';
      link.href = path;
      link.onload = () => resolve(link);
      link.onerror = () => {
        link.remove();
        if (stylePromises.get(path) === pending) stylePromises.delete(path);
        reject(new Error(`样式加载失败：${path}`));
      };
      document.head.appendChild(link);
    });
    stylePromises.set(path, pending);
  }
  return stylePromises.get(path);
}

export function loadScript(path) {
  if (!scriptPromises.has(path)) {
    const pending = new Promise((resolve, reject) => {
      const script = document.createElement('script');
      script.src = path;
      script.onload = () => resolve(script);
      script.onerror = () => {
        script.remove();
        if (scriptPromises.get(path) === pending) scriptPromises.delete(path);
        reject(new Error(`脚本加载失败：${path}`));
      };
      document.head.appendChild(script);
    });
    scriptPromises.set(path, pending);
  }
  return scriptPromises.get(path);
}

const shell = Object.freeze({
  ...window.QuantMasterShell,
  loadStyle,
  loadScript,
  deactivateCharts: () => window.QuantCharts?.activateTab(''),
});

function readPages() {
  try { return JSON.parse(sessionStorage.getItem(PAGE_KEY) || '{}'); }
  catch (_) { return {}; }
}

function rememberPage(workspace, page) {
  try {
    const pages = readPages();
    pages[workspace] = page;
    sessionStorage.setItem(PAGE_KEY, JSON.stringify(pages));
  } catch (_) { /* private browsing may disable session storage */ }
}

function routeFromHash() {
  const [path, rawQuery = ''] = location.hash.split('?');
  const match = path.match(/^#(today|research|account|runtime)\/([a-z-]+)$/);
  return match && PAGES[match[1]][match[2]]
    ? {workspace: match[1], page: match[2], query: rawQuery ? `?${rawQuery}` : ''}
    : null;
}

/* #30: 从路由参数中提取深链接上下文（板块、快照时间等）。 */
function routeContext(query = '') {
  const params = new URLSearchParams(String(query).replace(/^\?/, ''));
  const board = params.get('board') || '';
  const asOf = params.get('as-of') || '';
  return {
    board, asOf,
    category: params.get('category') || '',
    code: params.get('code') || '',
    method: params.get('method') || '',
    window: params.get('window') || '',
  };
}

function pageControl(workspace, page) {
  const tab = PAGES[workspace][page];
  return Array.from(document.querySelectorAll(`[data-workspace-pages="${workspace}"] [data-tab]`))
    .find(control => control.dataset.workspacePage === page && control.dataset.tab === tab);
}

function showRoute(workspace, page) {
  const tab = PAGES[workspace][page];
  const control = pageControl(workspace, page);
  document.querySelectorAll('[data-workspace]').forEach(button => {
    const selected = button.dataset.workspace === workspace;
    button.classList.toggle('active', selected);
    button.setAttribute('aria-current', selected ? 'page' : 'false');
  });
  document.querySelectorAll('[data-workspace-pages]').forEach(group => {
    group.hidden = group.dataset.workspacePages !== workspace;
  });
  document.querySelectorAll('header [role="tab"]').forEach(button => {
    const selected = button === control;
    button.classList.toggle('active', selected);
    button.setAttribute('aria-selected', String(selected));
    button.tabIndex = selected ? 0 : -1;
  });
  document.querySelectorAll('.tab').forEach(section => {
    section.classList.toggle('active', section.id === `tab-${tab}`);
  });
  control?.scrollIntoView({block: 'nearest', inline: 'nearest'});
  window.scrollTo({top: 0, left: 0, behavior: 'auto'});
}

function showError(error) {
  let output = document.getElementById('workspace-load-error');
  if (!output) {
    output = document.createElement('div');
    output.id = 'workspace-load-error';
    output.className = 'err';
    output.setAttribute('role', 'alert');
    document.querySelector('main')?.prepend(output);
  }
  output.textContent = `工作区加载失败：${error?.message || error}`;
  output.hidden = false;
}

async function transitionTo(workspace, page, query, replace, token) {
  if (token !== activation) return false;
  let previous = active;
  const currentRoute = routeFromHash();
  if (
    previous && currentRoute?.workspace === previous.workspace
    && currentRoute.page === previous.page
  ) {
    previous = {
      ...previous,
      query:currentRoute.query,
      context:{...previous.context, route:routeContext(currentRoute.query)},
    };
  }
  let previousUnmounted = false;
  let adapter = null;
  try {
    adapter = await loadWorkspace(workspace);
    if (token !== activation) return false;
    if (previous?.adapter) {
      await previous.adapter.unmount();
      previousUnmounted = true;
    }
    if (token !== activation) return false;
    const context = {workspace, page, shell, route: routeContext(query)};
    await adapter.mount(context);
    if (token !== activation) {
      await adapter.unmount();
      if (token !== activation) return false;
      return false;
    }
    showRoute(workspace, page);
    rememberPage(workspace, page);
    const mountedRoute = routeFromHash();
    const effectiveQuery = mountedRoute?.workspace === workspace && mountedRoute.page === page
      ? mountedRoute.query : query;
    const target = `#${workspace}/${page}${effectiveQuery}`;
    if (location.hash !== target) history[replace ? 'replaceState' : 'pushState'](null, '', target);
    active = {workspace, page, query:effectiveQuery, adapter, context};
    document.dispatchEvent(new CustomEvent('quantmaster:workspace-mounted', {
      detail:{workspace, page},
    }));
    document.getElementById('workspace-load-error')?.setAttribute('hidden', '');
    return true;
  } catch (error) {
    if (adapter) {
      try { await adapter.unmount(); }
      catch (_) { /* failed mounts may have nothing to dispose */ }
      if (token !== activation) return false;
    }
    if (previousUnmounted) {
      await previous.adapter.mount(previous.context);
      if (token !== activation) return false;
      showRoute(previous.workspace, previous.page);
      const target = `#${previous.workspace}/${previous.page}${previous.query || ''}`;
      if (location.hash !== target) history.replaceState(null, '', target);
      active = previous;
    }
    showError(error);
    console.error(error);
    return false;
  }
}

function activate(workspace, page, {replace = false, query = ''} = {}) {
  if (!PAGES[workspace]?.[page]) return Promise.resolve(false);
  const token = ++activation;
  const pending = transitions.then(() => transitionTo(workspace, page, query, replace, token));
  transitions = pending.catch(() => false);
  return pending;
}

document.querySelector('header')?.addEventListener('click', event => {
  const workspaceButton = event.target.closest('[data-workspace]');
  if (workspaceButton) {
    const workspace = workspaceButton.dataset.workspace;
    const remembered = readPages()[workspace];
    void activate(workspace, PAGES[workspace]?.[remembered] ? remembered : DEFAULT_PAGE[workspace]);
    return;
  }
  const pageButton = event.target.closest('[data-workspace-page]');
  if (!pageButton) {
    const utility = event.target.closest('[data-tab="help"], [data-tab="settings"]');
    if (utility) void activate('runtime', utility.dataset.tab);
    return;
  }
  const workspace = Object.keys(PAGES).find(name => PAGES[name][pageButton.dataset.workspacePage]);
  if (workspace) void activate(workspace, pageButton.dataset.workspacePage);
});

document.addEventListener('quantmaster:navigate', event => {
  const target = Object.entries(PAGES).find(([, pages]) =>
    Object.values(pages).includes(event.detail?.tab));
  if (!target) return;
  const [workspace, pages] = target;
  const page = Object.keys(pages).find(name => pages[name] === event.detail.tab);
  if (page) void activate(workspace, page).then(mounted => {
    if (mounted && event.detail?.section) {
      document.querySelector(`[data-settings-section="${CSS.escape(event.detail.section)}"]`)?.click();
    }
  });
});

window.addEventListener('hashchange', () => {
  const route = routeFromHash();
  if (route && (
    route.workspace !== active?.workspace || route.page !== active?.page
    || route.query !== active?.query
  )) {
    void activate(route.workspace, route.page, {replace: true, query: route.query});
  }
});

const initial = routeFromHash() || {workspace: 'today', page: 'market', query: ''};
void activate(initial.workspace, initial.page, {replace: !routeFromHash(), query: initial.query});
