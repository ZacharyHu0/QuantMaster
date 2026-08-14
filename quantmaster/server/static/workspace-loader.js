const WORKSPACES = {
  today: () => import('./workspaces/today.js'),
  research: () => import('./workspaces/research.js'),
  account: () => import('./workspaces/account.js'),
  runtime: () => import('./workspaces/runtime.js'),
};

const PAGES = {
  today: {
    quotes: 'market', temperature: 'market', style: 'market', rotation: 'rotation',
    industry: 'rotation', themes: 'rotation', etfs: 'rotation', news: 'news',
    'after-close': 'after-close', candidates: 'candidates',
    'stock-analysis': 'stock-analysis', decision: 'decision',
  },
  research: {lab: 'lab', backtest: 'backtest'},
  account: {paper: 'paper', ledger: 'ledger'},
  runtime: {automation: 'automation', help: 'help', settings: 'settings'},
};

const DEFAULT_PAGE = {today: 'quotes', research: 'lab', account: 'paper', runtime: 'automation'};
const PAGE_KEY = 'quantmaster.workspacePage.v2';
const modulePromises = new Map();
const stylePromises = new Map();
const scriptPromises = new Map();
let active = null;
let activation = 0;

function loadWorkspace(name) {
  if (!modulePromises.has(name)) modulePromises.set(name, WORKSPACES[name]());
  return modulePromises.get(name);
}

export function loadStyle(path) {
  if (!stylePromises.has(path)) {
    stylePromises.set(path, new Promise((resolve, reject) => {
      const link = document.createElement('link');
      link.rel = 'stylesheet';
      link.href = path;
      link.onload = () => resolve(link);
      link.onerror = () => reject(new Error(`样式加载失败：${path}`));
      document.head.appendChild(link);
    }));
  }
  return stylePromises.get(path);
}

export function loadScript(path) {
  if (!scriptPromises.has(path)) {
    scriptPromises.set(path, new Promise((resolve, reject) => {
      const script = document.createElement('script');
      script.src = path;
      script.onload = () => resolve(script);
      script.onerror = () => reject(new Error(`脚本加载失败：${path}`));
      document.head.appendChild(script);
    }));
  }
  return scriptPromises.get(path);
}

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
  const match = location.hash.match(/^#(today|research|account|runtime)\/([a-z-]+)$/);
  return match && PAGES[match[1]][match[2]] ? {workspace: match[1], page: match[2]} : null;
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

async function activate(workspace, page, {replace = false} = {}) {
  if (!PAGES[workspace]?.[page]) return false;
  const token = ++activation;
  const previous = active;
  try {
    const adapter = await loadWorkspace(workspace);
    if (token !== activation) return false;
    if (previous?.adapter) await previous.adapter.unmount();
    showRoute(workspace, page);
    rememberPage(workspace, page);
    const target = `#${workspace}/${page}`;
    if (location.hash !== target) history[replace ? 'replaceState' : 'pushState'](null, '', target);
    active = {workspace, page, adapter};
    await adapter.mount({workspace, page, loadStyle, loadScript});
    document.dispatchEvent(new CustomEvent('quantmaster:workspace-mounted', {
      detail:{workspace, page},
    }));
    document.getElementById('workspace-load-error')?.setAttribute('hidden', '');
    return true;
  } catch (error) {
    showError(error);
    console.error(error);
    return false;
  }
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
  if (route && (route.workspace !== active?.workspace || route.page !== active?.page)) {
    void activate(route.workspace, route.page, {replace: true});
  }
});

const initial = routeFromHash() || {workspace: 'today', page: 'quotes'};
void activate(initial.workspace, initial.page, {replace: !routeFromHash()});
