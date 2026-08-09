class QuantApiError extends Error {
  constructor(message, options = {}) {
    super(message);
    this.name = 'QuantApiError';
    Object.assign(this, options);
  }
}
function apiRoute(path) {
  try { return new URL(path, window.location.origin).pathname; }
  catch (_) { return String(path || ''); }
}
let browserCsrfToken = '';
let csrfRefreshPromise = null;
function csrfCookie() {
  const item = document.cookie.split(';').map(value => value.trim())
    .find(value => value.startsWith('qm_csrf='));
  return item ? decodeURIComponent(item.slice('qm_csrf='.length)) : '';
}
async function ensureCsrfToken(forceRefresh = false) {
  const cookie = csrfCookie();
  if (!forceRefresh && cookie) {
    browserCsrfToken = cookie;
    return cookie;
  }
  if (!csrfRefreshPromise) {
    csrfRefreshPromise = (async () => {
      const response = await fetch('/api/v1/session', {
        headers:{'Accept':'application/json'}, credentials:'same-origin', cache:'no-store',
      });
      if (!response.ok) throw new Error('无法建立本机安全会话');
      const data = await response.json();
      browserCsrfToken = String(data.csrf_token || '');
      if (!browserCsrfToken) throw new Error('本机安全会话未返回操作令牌');
      return browserCsrfToken;
    })();
  }
  try { return await csrfRefreshPromise; }
  finally { csrfRefreshPromise = null; }
}
async function protectedOptions(options = {}) {
  const method = String(options.method || 'GET').toUpperCase();
  const headers = new Headers(options.headers || {});
  if (['POST','PUT','PATCH','DELETE'].includes(method) && !headers.has('X-CSRF-Token')) {
    headers.set('X-CSRF-Token', await ensureCsrfToken());
  }
  return {...options, method, headers, credentials:'same-origin'};
}
async function protectedFetch(path, options = {}) {
  const prepared = await protectedOptions(options);
  let response = await fetch(path, prepared);
  const method = String(prepared.method || 'GET').toUpperCase();
  if (['POST','PUT','PATCH','DELETE'].includes(method) && response.status === 403) {
    const rejection = await response.clone().json().catch(() => ({}));
    const csrfCodes = new Set(['csrf_missing','csrf_mismatch','csrf_expired','csrf_invalid']);
    if (csrfCodes.has(String(rejection?.problem?.code || ''))) {
      const headers = new Headers(prepared.headers || {});
      headers.set('X-CSRF-Token', await ensureCsrfToken(true));
      response = await fetch(path, {...prepared, headers});
    }
  }
  return response;
}
function sourceForPath(path) {
  const route = apiRoute(path);
  const sources = [
    ['/api/v1/market/stock-analyses', '个股分析'], ['/api/v1/market', '市场行情'],
    ['/api/v1/research/decision', '每日决策'],
    ['/api/v1/research/factors', '因子研究'], ['/api/v1/backtests', '策略回测'],
    ['/api/v1/research/mining', '因子挖掘'], ['/api/v1/paper', '模拟交易'],
    ['/api/v1/news', '资讯分析'], ['/api/v1/portfolio/ledger', '实盘账本'],
    ['/api/v1/rotation', '板块联动'],
    ['/api/v1/after-close', '盘后扫描'],
    ['/api/v1/jobs', '后台任务'],
    ['/api/v1/portfolio', '我的标的'], ['/api/v1/settings', '设置中心'],
    ['/api/v1/automation', '自动任务'], ['/api/v1/lab', 'Quant Lab'],
  ];
  return sources.find(([prefix]) => route.startsWith(prefix))?.[1] || '本地服务';
}
function readableDetail(detail) {
  if (typeof detail === 'string') return detail.trim();
  if (Array.isArray(detail)) return detail.map(item => {
    const field = Array.isArray(item?.loc) ? item.loc.filter(x => x !== 'body').join('.') : '';
    return [field, item?.msg || item?.message].filter(Boolean).join('：');
  }).filter(Boolean).join('；');
  if (detail && typeof detail === 'object') {
    if (detail.message) return String(detail.message);
    try { return JSON.stringify(detail); } catch (_) { return ''; }
  }
  return '';
}
function friendlyHttpMessage(status) {
  if (status === 400) return '请求未通过检查';
  if (status === 401) return '身份验证未通过';
  if (status === 403) return '当前操作没有权限';
  if (status === 404) return '没有找到所需数据';
  if (status === 409) return '当前数据状态有冲突';
  if (status === 413) return '提交的数据超过大小限制';
  if (status === 422) return '有些输入需要修改';
  if (status === 423) return '数据目录正在迁移';
  if (status === 429) return '数据源请求过于频繁';
  if (status >= 500) return '服务端处理失败';
  return '请求未能完成';
}
function recoveryForStatus(status) {
  if ([400, 422].includes(status)) return '按页面提示修改输入后重试。';
  if (status === 401) return '检查设置中的账号或密钥，然后重试。';
  if (status === 403) return '检查对应服务的权限或应用授权，然后重试。';
  if (status === 404) return '确认所需数据或配置仍然存在，然后重试。';
  if (status === 409) return '刷新当前数据，确认最新状态后重试。';
  if (status === 413) return '减少本次提交的数据量后重试。';
  if (status === 423) return '等待数据迁移完成后再试。';
  if (status === 429) return '稍候再试；如频繁出现，请降低刷新频率。';
  return '重试一次；如仍失败，请复制诊断信息排查后端日志。';
}
function optionalCount(value) {
  if (value === null || value === undefined || value === '') return null;
  const number = Number(value);
  return Number.isFinite(number) ? Math.max(0, Math.trunc(number)) : null;
}
function normalizeProblem(value, fallback = {}) {
  const raw = value && typeof value === 'object' ? value : {};
  const message = typeof value === 'string' ? value : (raw.message || fallback.message || '操作未能完成');
  const severity = ['info','warning','error'].includes(raw.severity) ? raw.severity : (fallback.severity || 'error');
  return {
    id:String(raw.id || fallback.id || raw.code || `problem:${message}`),
    revision:String(raw.revision || ''),
    code:String(raw.code || fallback.code || 'operation_problem'),
    severity,
    source:String(raw.source || fallback.source || '本地服务'),
    title:String(raw.title || fallback.title || (severity === 'warning' ? '操作需要确认' : '操作未能完成')),
    message:String(message),
    action:String(raw.action || fallback.action || '检查提示后重试。'),
    blocking:Boolean(raw.blocking ?? fallback.blocking),
    can_continue:Boolean(raw.can_continue ?? fallback.can_continue),
    items:Array.isArray(raw.items) ? raw.items.map(String) : [],
    remote_failures:optionalCount(raw.remote_failures ?? fallback.remote_failures),
    local_blocks:optionalCount(raw.local_blocks ?? fallback.local_blocks),
  };
}
function ingestResponseProblems(data, scope = 'operation') {
  if (!data || typeof data !== 'object') return [];
  const values = [
    ...(Array.isArray(data.issues) ? data.issues : []),
    ...(Array.isArray(data.warnings) ? data.warnings : []),
  ];
  const problems = values.map((value, index) => normalizeProblem(value, {
    id:`${scope}:warning:${index}`, severity:'warning', source:'本地服务',
    title:'结果包含注意事项', action:'查看页面结果中的数据质量说明。',
  }));
  problems.forEach(problem => runtimeInfo.add(
    problem.severity === 'error' ? 'error' : problem.severity === 'info' ? 'info' : 'warning',
    problem.source, problem.title, {
      detail:problem.message, action:problem.action,
      key:`${scope}:problem:${problem.id}`, revision:problem.revision,
    },
  ));
  return problems;
}
function responseError(response, data, path, method, key = '') {
  const fallbackTitle = friendlyHttpMessage(response.status);
  const detail = readableDetail(data.detail);
  const requestId = data.error_id || response.headers.get('X-Request-ID') || '';
  const route = apiRoute(path);
  const problem = normalizeProblem(data.problem, {
    id:`request:${method}:${route}`, source:sourceForPath(path), title:fallbackTitle,
    message:detail || fallbackTitle, action:recoveryForStatus(response.status),
    blocking:true, severity:response.status === 409 ? 'warning' : 'error',
  });
  const error = new QuantApiError(`${problem.title}：${problem.message}`, {
    status:response.status, detail:data.detail, detailText:detail,
    requestId, path:route, method, logged:true, problem,
    dataQuality:data.data_quality || null,
  });
  runtimeInfo.add(problem.severity === 'warning' ? 'warning' : 'error', problem.source, problem.title, {
    detail:problem.message || `HTTP ${response.status}`, action:problem.action,
    requestId, path:`${method} ${route}`, key, revision:problem.revision,
  });
  return error;
}
async function api(path, opts = {}) {
  const method = String(opts.method || 'GET').toUpperCase();
  const route = apiRoute(path), requestKey = `request:${method}:${route}`;
  let res;
  try {
    res = await protectedFetch(path, opts);
  } catch (cause) {
    const cancelled = cause?.name === 'AbortError';
    const message = cancelled ? '请求已取消' : '无法连接本地服务';
    const error = new QuantApiError(message, {
      cause, path:route, method, logged:true,
    });
    runtimeInfo.add(cancelled ? 'warning' : 'error', sourceForPath(path), message, {
      detail:cause?.message || '',
      action:cancelled ? '无需处理；需要时可重新执行。' : '确认 QuantMaster 服务仍在运行，然后重试。',
      path:`${method} ${route}`, key:requestKey,
    });
    throw error;
  }
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw responseError(res, data, path, method, requestKey);
  if (method !== 'GET') {
    runtimeInfo.add('success', sourceForPath(path), '操作已完成', {
      requestId:res.headers.get('X-Request-ID') || '', path:`${method} ${route}`,
      key:requestKey,
    });
  } else runtimeInfo.resolve(requestKey);
  if (route !== '/api/v1/diagnostics') ingestResponseProblems(data, requestKey);
  return data;
}
function post(path, body) {
  return api(path, { method: 'POST', headers: {'Content-Type':'application/json'}, body: JSON.stringify(body) });
}
function busy(form, on, label = '') {
  const b = form.querySelector('button.primary');
  if (!b) return;
  if (on) {
    b.dataset.idleText = b.textContent;
    if (label) b.textContent = label;
  } else if (b.dataset.idleText) {
    b.textContent = b.dataset.idleText;
    delete b.dataset.idleText;
  }
  b.disabled = on;
}
function pct(v) { return v == null ? '—' : (v * 100).toFixed(2) + '%'; }
function cls(v) { return v > 0 ? 'up' : v < 0 ? 'down' : ''; }
function esc(s) { return String(s ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c])); }

const runtimeInfo = (() => {
  const root = document.getElementById('runtime-info');
  const summary = document.getElementById('runtime-summary');
  const drawer = document.getElementById('runtime-drawer-frame');
  const latest = document.getElementById('runtime-latest');
  const count = document.getElementById('runtime-count');
  const errorCount = document.getElementById('runtime-error-count');
  const list = document.getElementById('runtime-list');
  const entries = [];
  const levelLabels = {info:'进行中', success:'完成', warning:'需留意', error:'失败'};
  const emptyLabels = {
    all:'暂无后台记录。', problem:'没有需要处理的问题。',
    running:'当前没有进行中的任务。', completed:'暂无已完成任务。',
  };
  let activeFilter = 'all', expanded = false, sequence = 0, operationSequence = 0;

  function compactText(value, limit = 360) {
    const text = String(value || '').replace(/\s+/g, ' ').trim();
    return text.length > limit ? `${text.slice(0, limit - 1)}…` : text;
  }
  function visible(entry) {
    if (activeFilter === 'all') return true;
    if (activeFilter === 'problem') return ['warning', 'error'].includes(entry.level);
    if (activeFilter === 'running') return entry.level === 'info';
    if (activeFilter === 'completed') return entry.level === 'success';
    return true;
  }
  function syncSummary() {
    const problems = entries.filter(entry => ['warning', 'error'].includes(entry.level));
    const errors = problems.filter(entry => entry.level === 'error');
    const focus = problems.at(-1) || entries.at(-1);
    count.hidden = entries.length === 0;
    count.textContent = entries.length ? `${entries.length} 项` : '';
    errorCount.hidden = problems.length === 0;
    errorCount.textContent = problems.length ? `${problems.length} 个问题` : '';
    errorCount.setAttribute('aria-label', problems.length ? `${problems.length} 个后台问题` : '没有后台问题');
    latest.textContent = focus ? `${focus.source} · ${focus.message}` : '后台正常';
    root.dataset.level = errors.length ? 'error' : problems.length ? 'warning' : 'success';
  }
  function diagnostics(entry) {
    const rows = [];
    if (entry.detail) rows.push(`<dt>原因</dt><dd>${esc(entry.detail)}</dd>`);
    if (entry.path) rows.push(`<dt>接口</dt><dd><code>${esc(entry.path)}</code></dd>`);
    if (entry.requestId) rows.push(`<dt>请求编号</dt><dd><span class="runtime-request"><code>${esc(entry.requestId)}</code><button class="runtime-copy" type="button" data-copy-request="${esc(entry.requestId)}" aria-label="复制请求编号 ${esc(entry.requestId)}">复制</button></span></dd>`);
    if (entry.remoteFailures !== null) rows.push(`<dt>远端失败</dt><dd>${entry.remoteFailures} 次</dd>`);
    if (entry.localBlocks !== null) rows.push(`<dt>本地拦截</dt><dd>${entry.localBlocks} 次</dd>`);
    return rows.length ? `<details class="runtime-diagnostics"><summary>诊断信息</summary><dl class="runtime-diagnostics-grid">${rows.join('')}</dl></details>` : '';
  }
  function render() {
    syncSummary();
    const shown = entries.filter(visible).slice().reverse();
    list.innerHTML = shown.length ? shown.map(entry => {
      const secondary = ['warning', 'error'].includes(entry.level)
        ? (entry.action ? `<div class="runtime-advice">${esc(entry.action)}</div>` : '') + diagnostics(entry)
        : (entry.detail ? `<div class="runtime-note">${esc(entry.detail)}</div>` : '');
      return `<article class="runtime-entry" data-level="${entry.level}" data-runtime-id="${entry.id}">
        <time class="runtime-time" datetime="${esc(entry.iso)}">${esc(entry.time)}</time>
        <div class="runtime-entry-body"><div class="runtime-entry-title"><span class="runtime-source">${esc(entry.source)}</span><span class="runtime-message">${esc(entry.message)}</span></div>${secondary}</div>
        <span class="runtime-level">${levelLabels[entry.level]}</span></article>`;
    }).join('') : `<div class="runtime-empty">${emptyLabels[activeFilter] || emptyLabels.all}</div>`;
  }
  function setExpanded(next) {
    expanded = Boolean(next);
    root.classList.toggle('expanded', expanded);
    summary.setAttribute('aria-expanded', String(expanded));
    drawer.setAttribute('aria-hidden', String(!expanded));
    drawer.inert = !expanded;
    if (expanded) queueMicrotask(() => list.scrollTop = 0);
  }
  function add(level, source, message, meta = {}) {
    const safeLevel = levelLabels[level] ? level : 'info';
    const now = new Date(), key = String(meta.key || '');
    const existingIndex = key ? entries.findIndex(entry => entry.key === key) : -1;
    const previous = existingIndex >= 0 ? entries[existingIndex] : null;
    const revision = compactText(meta.revision, 80);
    if (previous && revision && previous.revision === revision) return previous;
    const entry = {
      id:previous?.id || ++sequence, key, level:safeLevel,
      source:compactText(source || '系统', 40), message:compactText(message || '状态已更新', 160),
      detail:compactText(meta.detail), action:compactText(meta.action, 220),
      path:compactText(meta.path, 220), requestId:compactText(meta.requestId, 80),
      remoteFailures:optionalCount(meta.remoteFailures), localBlocks:optionalCount(meta.localBlocks),
      revision, scope:compactText(meta.scope, 80), persistent:Boolean(meta.persistent),
      iso:now.toISOString(),
      time:now.toLocaleTimeString('zh-CN', {hour:'2-digit', minute:'2-digit', second:'2-digit'}),
    };
    if (existingIndex >= 0) entries.splice(existingIndex, 1);
    entries.push(entry);
    while (entries.length > 40) {
      const transientIndex = entries.findIndex(item => !item.persistent);
      entries.splice(transientIndex >= 0 ? transientIndex : 0, 1);
    }
    render();
    return entry;
  }
  function sync(scope, problems = []) {
    const normalized = problems.map((problem, index) => normalizeProblem(problem, {
      id:`${scope}:${index}`, source:'后台状态', severity:'warning',
    }));
    const activeKeys = new Set(normalized.map(problem => `persistent:${scope}:${problem.id}`));
    for (let index = entries.length - 1; index >= 0; index -= 1) {
      const entry = entries[index];
      if (entry.persistent && entry.scope === scope && !activeKeys.has(entry.key)) entries.splice(index, 1);
    }
    normalized.forEach(problem => add(
      problem.severity === 'error' ? 'error' : problem.severity === 'warning' ? 'warning' : 'info',
      problem.source, problem.title, {
        detail:problem.message, action:problem.action,
        key:`persistent:${scope}:${problem.id}`, scope, persistent:true,
        revision:problem.revision, remoteFailures:problem.remote_failures,
        localBlocks:problem.local_blocks,
      },
    ));
    render();
  }
  function begin(source, message, meta = {}) {
    const key = `operation:${Date.now()}:${++operationSequence}`;
    add('info', source, message, {...meta, key});
    return key;
  }
  function resolve(key) {
    const index = entries.findIndex(entry => entry.key === key);
    if (index < 0) return;
    entries.splice(index, 1);
    render();
  }
  function phase(source, event, path = '', key = '') {
    const requestId = event.request_id || '';
    if (event.level === 'warning') {
      const warningKey = `${key || source}:warning:${event.phase || event.detail || 'partial'}`;
      add('warning', source, event.phase || '部分数据未完成', {
        detail:event.detail, action:'已完成的结果仍可使用；需要完整数据时请稍后重试。',
        requestId, path, key:warningKey,
      });
      return;
    }
    add(event.level === 'success' ? 'success' : 'info', source, event.phase || '正在处理', {
      detail:event.detail, requestId, path, key,
    });
  }
  summary.addEventListener('click', () => setExpanded(!expanded));
  document.getElementById('runtime-collapse').addEventListener('click', () => {
    setExpanded(false); summary.focus();
  });
  document.getElementById('runtime-clear').addEventListener('click', () => {
    for (let index = entries.length - 1; index >= 0; index -= 1) {
      if (!entries[index].persistent) entries.splice(index, 1);
    }
    render();
  });
  document.querySelector('.runtime-filters').addEventListener('click', event => {
    const button = event.target.closest('[data-runtime-filter]');
    if (!button) return;
    activeFilter = button.dataset.runtimeFilter;
    document.querySelectorAll('[data-runtime-filter]').forEach(item => item.classList.toggle('active', item === button));
    render();
  });
  list.addEventListener('click', async event => {
    const button = event.target.closest('[data-copy-request]');
    if (!button) return;
    try {
      await navigator.clipboard.writeText(button.dataset.copyRequest);
      button.textContent = '已复制';
      setTimeout(() => { if (button.isConnected) button.textContent = '复制'; }, 1200);
    } catch (_) { button.previousElementSibling?.focus?.(); }
  });
  document.addEventListener('keydown', event => {
    if (event.key === 'Escape' && expanded) setExpanded(false);
  });
  render();
  return {add, begin, resolve, phase, sync, open:() => setExpanded(true), close:() => setExpanded(false)};
})();
window.QuantMasterRunInfo = runtimeInfo;
window.QuantMasterAPI = api;

const operationProblemDialog = (() => {
  const dialog = document.getElementById('operation-problem-dialog');
  const kicker = document.getElementById('operation-problem-kicker');
  const title = document.getElementById('operation-problem-title');
  const message = document.getElementById('operation-problem-message');
  const action = document.getElementById('operation-problem-action');
  const items = document.getElementById('operation-problem-items');
  const quality = document.getElementById('operation-problem-quality');
  const cancelButton = document.getElementById('operation-problem-cancel');
  const continueButton = document.getElementById('operation-problem-continue');
  const closeButton = document.getElementById('operation-problem-close');
  const statusButton = document.getElementById('operation-problem-status');
  let resolver = null, previousFocus = null;

  function settle(value) {
    if (!resolver) return;
    const resolve = resolver;
    resolver = null;
    if (dialog.open) dialog.close();
    previousFocus?.focus?.();
    resolve(Boolean(value));
  }
  function qualityMarkup(data) {
    if (!data || typeof data !== 'object') return '';
    const range = data.actual_start && data.actual_end ? `${data.actual_start} — ${data.actual_end}` : '—';
    return [
      ['可用标的', `${data.usable_symbol_count ?? 0} / ${data.requested_symbol_count ?? 0}`],
      ['有效区间', range],
      ['可成交信号', `${data.executable_signals ?? 0} / ${data.selected_signals ?? 0}`],
    ].map(([label, value]) => `<div><span>${esc(label)}</span><strong title="${esc(value)}">${esc(value)}</strong></div>`).join('');
  }
  function open(problemValue, dataQuality = null) {
    const problem = normalizeProblem(problemValue);
    if (resolver) settle(false);
    previousFocus = document.activeElement;
    dialog.dataset.severity = problem.severity;
    kicker.textContent = problem.can_continue ? '数据不完整 · 需要确认' : '操作已暂停';
    title.textContent = problem.title;
    message.textContent = problem.message;
    action.textContent = problem.action;
    action.hidden = !problem.action;
    items.innerHTML = problem.items.map(item => `<li>${esc(item)}</li>`).join('');
    items.hidden = problem.items.length === 0;
    quality.innerHTML = qualityMarkup(dataQuality);
    quality.hidden = !quality.innerHTML;
    continueButton.hidden = !problem.can_continue;
    cancelButton.textContent = problem.can_continue ? '取消' : '关闭';
    dialog.showModal();
    queueMicrotask(() => cancelButton.focus());
    return new Promise(resolve => { resolver = resolve; });
  }
  cancelButton.addEventListener('click', () => settle(false));
  closeButton.addEventListener('click', () => settle(false));
  continueButton.addEventListener('click', () => settle(true));
  statusButton.addEventListener('click', () => { settle(false); runtimeInfo.open(); });
  dialog.addEventListener('cancel', event => { event.preventDefault(); settle(false); });
  dialog.addEventListener('close', () => {
    if (resolver) settle(false);
  });
  return {open};
})();
window.QuantMasterProblemDialog = operationProblemDialog;

async function refreshBackendHealth() {
  if (document.visibilityState === 'hidden') return;
  try {
    const data = await api('/api/v1/diagnostics', {cache:'no-store'});
    runtimeInfo.sync('health', Array.isArray(data.issues) ? data.issues : []);
  } catch (error) {
    const problem = error?.problem || normalizeProblem(null, {
      id:'health-unreachable', severity:'error', source:'后台状态',
      title:'无法读取后台状态', message:error?.message || '本地服务未响应',
      action:'确认 QuantMaster 服务仍在运行，然后重试。',
    });
    runtimeInfo.resolve('request:GET:/api/v1/diagnostics');
    runtimeInfo.sync('health', [problem]);
  }
}
queueMicrotask(refreshBackendHealth);
setInterval(refreshBackendHealth, 30_000);
document.addEventListener('visibilitychange', () => {
  if (document.visibilityState === 'visible') refreshBackendHealth();
});
window.addEventListener('online', refreshBackendHealth);

function reportLocalError(source, message, error) {
  if (error?.logged) return;
  runtimeInfo.add('error', source, message, {
    detail:error?.message || String(error || '请重试'),
    action:'重试刚才的操作；如仍失败，请刷新页面后再试。',
    key:`local:${source}:${message}`,
  });
  if (error && typeof error === 'object') error.logged = true;
}
window.addEventListener('error', event => {
  if (event.error?.logged) return;
  if (event.target && event.target !== window) {
    const url = event.target.src || event.target.href || event.target.tagName;
    runtimeInfo.add('error', '前端资源', '界面资源加载失败', {
      detail:String(url || ''), action:'刷新页面；如仍失败，请检查本地服务和网络连接。',
      key:`resource:${url}`,
    });
    return;
  }
  runtimeInfo.add('error', '前端界面', '页面脚本运行异常', {
    detail:[event.message, event.filename ? `${event.filename.split('/').pop()}:${event.lineno || 0}:${event.colno || 0}` : ''].filter(Boolean).join(' · '),
    action:'刷新页面；若问题再次出现，请展开诊断信息进行排查。',
    key:`script:${event.message}:${event.filename}:${event.lineno}`,
  });
  if (event.error && typeof event.error === 'object') event.error.logged = true;
}, true);
window.addEventListener('unhandledrejection', event => {
  if (event.reason?.logged) return;
  runtimeInfo.add('error', '前端界面', '异步操作意外中断', {
    detail:event.reason?.message || String(event.reason || '未知原因'),
    action:'重试刚才的操作；若问题再次出现，请展开诊断信息进行排查。',
    key:`promise:${event.reason?.message || String(event.reason || '')}`,
  });
  if (event.reason && typeof event.reason === 'object') event.reason.logged = true;
});

function skeletonMarkup(kind) {
  if (kind === 'decision') return `<div class="load-skeleton skeleton-decision" aria-hidden="true">
    <div class="skeleton-block tall"></div><div class="skeleton-metrics">${'<div class="skeleton-block"></div>'.repeat(8)}</div></div>`;
  return `<div class="load-skeleton skeleton-market" aria-hidden="true">${'<div class="skeleton-block"></div>'.repeat(6)}</div>`;
}

function createLoadProgress(container, title, kind = 'market') {
  const started = Date.now();
  container.innerHTML = `<div class="load-state" role="status" aria-live="polite" aria-atomic="false">
    <div class="load-head"><div><div class="load-title"><span class="load-dot"></span><span data-load-phase>${esc(title)}</span></div>
      <div class="load-detail" data-load-detail>正在检查本地缓存…</div></div><div class="load-percent" data-load-percent>0%</div></div>
    <div class="load-track" role="progressbar" aria-label="数据加载进度" aria-valuemin="0" aria-valuemax="100" aria-valuenow="0">
      <div class="load-fill"></div></div>
    <div class="load-meta"><span data-load-elapsed>已用时 0 秒</span><span>可继续浏览其他页面</span></div>
    ${skeletonMarkup(kind)}</div><div class="stream-results" data-stream-results aria-live="polite"></div>`;
  const root = container.querySelector('.load-state');
  const results = container.querySelector('[data-stream-results]');
  const phase = root.querySelector('[data-load-phase]');
  const detail = root.querySelector('[data-load-detail]');
  const percent = root.querySelector('[data-load-percent]');
  const track = root.querySelector('[role=progressbar]');
  const elapsed = root.querySelector('[data-load-elapsed]');
  let currentPhase = title, hasResults = false;
  const timer = setInterval(() => {
    elapsed.textContent = `已用时 ${Math.floor((Date.now() - started) / 1000)} 秒`;
  }, 1000);
  return {
    update(event) {
      const value = Math.max(0, Math.min(100, Number(event.progress) || 0));
      root.style.setProperty('--progress', value / 100);
      track.setAttribute('aria-valuenow', String(Math.round(value)));
      percent.textContent = `${Math.round(value)}%`;
      detail.textContent = event.detail || '';
      if (event.phase && event.phase !== currentPhase) {
        currentPhase = event.phase;
        phase.textContent = event.phase;
        if (!REDUCED_MOTION) phase.animate(
          [{opacity:.45, transform:'translateY(2px)'}, {opacity:1, transform:'translateY(0)'}],
          {duration:180, easing:'cubic-bezier(.25,1,.5,1)'});
      }
    },
    reveal() {
      if (!hasResults) {
        hasResults = true;
        root.classList.add('has-results');
      }
      return results;
    },
    finish(detailText = '全部数据已加载，可继续操作') {
      clearInterval(timer);
      root.classList.add('has-results', 'complete');
      root.style.setProperty('--progress', 1);
      track.setAttribute('aria-valuenow', '100');
      percent.textContent = '100%';
      phase.textContent = '加载完成';
      detail.textContent = detailText;
    },
    fail(message) {
      clearInterval(timer);
      root.classList.add('has-results', 'failed');
      phase.textContent = hasResults ? '部分数据已保留' : '加载失败';
      detail.textContent = message;
    },
    stop() { clearInterval(timer); },
    get results() { return results; },
    get hasResults() { return hasResults; },
  };
}

async function readNdjsonEvents(path, opts = {}, onEvent) {
  const method = String(opts?.method || 'GET').toUpperCase();
  const source = sourceForPath(path), route = apiRoute(path);
  const operationKey = runtimeInfo.begin(source, '后台任务正在处理', {path:`${method} ${route}`});
  let requestId = '';
  try {
    const response = await protectedFetch(path, opts);
    requestId = response.headers.get('X-Request-ID') || '';
    if (!response.ok) {
      const data = await response.json().catch(() => ({}));
      throw responseError(response, data, path, method, operationKey);
    }
    if (!response.body) throw new Error('当前浏览器不支持流式响应');
    const reader = response.body.getReader();
    const decoder = new TextDecoder();
    let buffer = '';
    const consume = line => {
      if (!line.trim()) return;
      const event = JSON.parse(line);
      requestId = event.request_id || event.error_id || requestId;
      if (event.type === 'error') {
        const problem = normalizeProblem(event.problem, {
          id:`stream:${method}:${route}`, source, title:'后台任务未完成',
          message:event.message || '数据流异常中断',
          action:'重试一次；如仍失败，请复制请求编号排查后端日志。',
          blocking:true,
        });
        runtimeInfo.add(problem.severity === 'warning' ? 'warning' : 'error', problem.source, problem.title, {
          detail:problem.message, action:problem.action, requestId,
          path:`${method} ${route}`, key:operationKey, revision:problem.revision,
        });
        throw new QuantApiError(`${problem.title}：${problem.message}`, {
          problem, dataQuality:event.data_quality || null, requestId,
          path:route, method, logged:true,
        });
      }
      if (event.type === 'progress') runtimeInfo.phase(source, event, `${method} ${route}`, operationKey);
      onEvent?.(event);
    };
    while (true) {
      const {value, done} = await reader.read();
      buffer += decoder.decode(value || new Uint8Array(), {stream:!done});
      const lines = buffer.split('\n');
      buffer = lines.pop() || '';
      lines.forEach(consume);
      if (done) break;
    }
    consume(buffer);
    runtimeInfo.add('success', source, '后台任务已完成', {
      requestId, path:`${method} ${route}`, key:operationKey,
    });
  } catch (error) {
    if (!error?.logged) {
      const network = error instanceof TypeError && /fetch/i.test(error.message || '');
      runtimeInfo.add('error', source, network ? '无法连接本地服务' : '数据流意外中断', {
        detail:network ? '请确认 QuantMaster 服务仍在运行，然后重试。' : error?.message,
        action:network ? '确认 QuantMaster 服务仍在运行，然后重试。' : '重试一次；如仍失败，请复制诊断信息。',
        requestId, path:`${method} ${route}`, key:operationKey,
      });
      if (error && typeof error === 'object') error.logged = true;
    }
    throw error;
  }
}
window.QuantMasterNDJSON = readNdjsonEvents;

async function streamJson(path, opts, onProgress) {
  const method = String(opts?.method || 'GET').toUpperCase();
  const source = sourceForPath(path), route = apiRoute(path);
  const operationKey = runtimeInfo.begin(source, '正在加载数据', {path:`${method} ${route}`});
  let requestId = '';
  try {
    const response = await protectedFetch(path, opts);
    requestId = response.headers.get('X-Request-ID') || '';
    if (!response.ok) {
      const data = await response.json().catch(() => ({}));
      throw responseError(response, data, path, method, operationKey);
    }
    if (!response.body) throw new Error('当前浏览器不支持流式进度');
    const reader = response.body.getReader();
    const decoder = new TextDecoder();
    let buffer = '', result, streamError;
    const consume = line => {
      if (!line.trim()) return;
      const event = JSON.parse(line);
      requestId = event.request_id || event.error_id || requestId;
      if (event.type === 'progress') {
        runtimeInfo.phase(source, event, `${method} ${route}`, operationKey);
        onProgress?.(event);
      }
      if (event.type === 'result') result = event.data;
      if (event.type === 'error') streamError = {
        message:event.message || '数据任务失败', requestId:event.error_id || requestId,
        problem:event.problem || null, dataQuality:event.data_quality || null,
      };
    };
    while (true) {
      const {value, done} = await reader.read();
      buffer += decoder.decode(value || new Uint8Array(), {stream:!done});
      const lines = buffer.split('\n');
      buffer = lines.pop() || '';
      lines.forEach(consume);
      if (done) break;
    }
    consume(buffer);
    if (streamError) {
      const problem = normalizeProblem(streamError.problem, {
        id:`stream:${method}:${route}`, source, title:'数据任务未完成',
        message:streamError.message,
        action:'已完成的结果会保留；重试一次，如仍失败请复制诊断信息。',
        blocking:true,
      });
      const error = new QuantApiError(`${problem.title}：${problem.message}`, {
        requestId:streamError.requestId, path:route, method, logged:true, problem,
        dataQuality:streamError.dataQuality,
      });
      runtimeInfo.add(problem.severity === 'warning' ? 'warning' : 'error', problem.source, problem.title, {
        detail:problem.message, requestId:streamError.requestId,
        path:`${method} ${route}`, key:operationKey,
        action:problem.action, revision:problem.revision,
      });
      throw error;
    }
    if (result === undefined) throw new Error('数据流提前结束，请重试');
    runtimeInfo.add('success', source, '数据加载完成', {
      requestId, path:`${method} ${route}`, key:operationKey,
    });
    ingestResponseProblems(result, operationKey);
    return result;
  } catch (error) {
    if (!error?.logged) {
      const network = error instanceof TypeError && /fetch/i.test(error.message || '');
      runtimeInfo.add('error', source, network ? '无法连接本地服务' : '数据流意外中断', {
        detail:network ? '请确认 QuantMaster 服务仍在运行，然后重试。' : error?.message,
        action:network ? '确认 QuantMaster 服务仍在运行，然后重试。' : '重试一次；如仍失败，请复制诊断信息。',
        requestId, path:`${method} ${route}`, key:operationKey,
      });
      if (error && typeof error === 'object') error.logged = true;
    }
    throw error;
  }
}

/* ---------- 版本与 stockdb 数据状态 ---------- */
(() => {
  const trigger = document.getElementById('release-trigger');
  const panel = document.getElementById('release-popover');
  const list = document.getElementById('release-list');
  const dateElement = document.getElementById('release-date');
  const reloadButton = document.getElementById('release-reload-button');
  const reloadStatus = document.getElementById('release-reload-status');
  const stockdbTrigger = document.getElementById('stockdb-update-trigger');
  const stockdbPanel = document.getElementById('stockdb-update-popover');
  const stockdbDataDate = document.getElementById('stockdb-data-date');
  const stockdbPopoverSession = document.getElementById('stockdb-popover-session');
  const stockdbPopoverUpdatedAt = document.getElementById('stockdb-popover-updated-at');
  const stockdbPopoverState = document.getElementById('stockdb-popover-state');
  const vendorPanel = document.getElementById('free-stockdb-release');
  const vendorSummary = document.getElementById('free-stockdb-release-summary');
  const vendorState = document.getElementById('free-stockdb-release-state');
  const vendorLink = document.getElementById('free-stockdb-release-link');
  const vendorUnread = document.getElementById('free-stockdb-release-unread');
  let vendorFingerprint = '';

  function renderSummary(data) {
    if (!data?.version) return;
    document.getElementById('release-version').textContent = `v${data.version}`;
    const releaseDate = data.release_date || '';
    const [year, month, day] = String(releaseDate).split('-');
    document.querySelector('#release-date .release-year').textContent = year ? `${year}.` : '';
    document.querySelector('#release-date .release-day').textContent = [month, day].filter(Boolean).join('.');
    dateElement.dateTime = releaseDate;
    dateElement.hidden = !releaseDate;
    trigger.title = [`v${data.version}`, releaseDate, '查看更新日志'].filter(Boolean).join(' · ');
    trigger.setAttribute('aria-label', releaseDate
      ? `版本 ${data.version}，发布于 ${releaseDate}，查看更新日志`
      : `版本 ${data.version}，查看更新日志`);
  }

  function renderReleases(releases) {
    list.innerHTML = (releases || []).map(release => `
      <section class="release-entry">
        <div class="release-entry-heading"><strong>v${esc(release.version)}</strong><time datetime="${esc(release.date)}">${esc(release.date)}</time></div>
        ${(release.sections || []).map(section => `
          <div class="release-section"><h3>${esc(section.title)}</h3><ul>
            ${(section.items || []).map(item => `<li>${esc(item)}</li>`).join('')}
          </ul></div>`).join('')}
      </section>`).join('') || '<div class="msg">暂无更新日志。</div>';
  }

  function compactSession(value) {
    const session = String(value || '').slice(0, 10);
    return /^\d{4}-\d{2}-\d{2}$/.test(session) ? session.replaceAll('-', '.') : '';
  }

  function formatTimestamp(value, includeYear = false) {
    const timestamp = new Date(value || '');
    if (Number.isNaN(timestamp.getTime())) return '';
    const formatted = timestamp.toLocaleString('zh-CN', {
      ...(includeYear ? {year:'numeric'} : {}),
      month:'2-digit', day:'2-digit', hour:'2-digit', minute:'2-digit', hour12:false,
    });
    return formatted.replaceAll('/', '.');
  }

  function renderStockdbStatus(status) {
    const session = String(status?.validated_session || '');
    const sessionLabel = compactSession(session);
    const updatedValue = session ? String(status?.last_update_at || '') : '';
    const updatedFull = formatTimestamp(updatedValue, true);
    const stateLabel = String(status?.message || status?.state || '状态未知');
    stockdbDataDate.textContent = sessionLabel || '待验证';
    stockdbDataDate.dateTime = session;
    stockdbPopoverSession.textContent = sessionLabel || '尚无可信验收记录';
    stockdbPopoverUpdatedAt.textContent = updatedFull || '尚未完成真实数据验收';
    stockdbPopoverState.textContent = stateLabel;
    stockdbTrigger.title = sessionLabel
      ? `stockdb 已验证至 ${session}；最近验收 ${updatedFull || '时间未知'}`
      : 'stockdb 尚无可信验收记录';
    stockdbTrigger.setAttribute('aria-label', `${stockdbTrigger.title}，查看更新状态`);
  }

  function renderVendorNotice(notice) {
    if (!notice || (!notice.data_date && !notice.version)) return;
    vendorFingerprint = String(notice.fingerprint || `${notice.data_date || ''}|${notice.version || ''}|${notice.announcement || ''}`);
    const details = [];
    if (notice.announcement) details.push(notice.announcement);
    if (notice.data_date) details.push(`数据更新至 ${notice.data_date}`);
    if (notice.version) details.push(`最新版本 ${notice.version}`);
    vendorSummary.textContent = details.join(' · ');
    vendorState.textContent = notice.status === 'stale' ? '最近一次动态' : '最新动态';
    vendorLink.href = String(notice.url || '').startsWith('https://a.123128.xyz/')
      ? notice.url : 'https://a.123128.xyz/';
    vendorPanel.hidden = false;
    try {
      vendorUnread.hidden = localStorage.getItem('qm-free-stockdb-release-seen') === vendorFingerprint;
    } catch (_) {
      vendorUnread.hidden = false;
    }
  }

  function setReleaseOpen(open) {
    panel.hidden = !open;
    trigger.setAttribute('aria-expanded', String(open));
    if (open) setStockdbOpen(false);
  }

  function setStockdbOpen(open) {
    stockdbPanel.hidden = !open;
    stockdbTrigger.setAttribute('aria-expanded', String(open));
    if (open) {
      panel.hidden = true;
      trigger.setAttribute('aria-expanded', 'false');
    }
    if (open && vendorFingerprint) {
      try { localStorage.setItem('qm-free-stockdb-release-seen', vendorFingerprint); } catch (_) {}
      vendorUnread.hidden = true;
    }
  }

  trigger.addEventListener('click', event => {
    event.stopPropagation();
    setReleaseOpen(panel.hidden);
  });
  reloadButton.addEventListener('click', async () => {
    reloadButton.disabled = true;
    reloadStatus.textContent = '正在提交热更新请求…';
    try {
      await api('/api/v1/system/reload', {method:'POST'});
      reloadStatus.textContent = '主站正在安全重载，页面即将刷新…';
      window.setTimeout(() => window.location.reload(), 3000);
    } catch (error) {
      reloadStatus.textContent = error?.message || '热更新请求失败，请查看运行日志';
      reloadButton.disabled = false;
    }
  });
  stockdbTrigger.addEventListener('click', event => {
    event.stopPropagation();
    setStockdbOpen(stockdbPanel.hidden);
  });
  panel.addEventListener('click', event => event.stopPropagation());
  stockdbPanel.addEventListener('click', event => event.stopPropagation());
  document.addEventListener('click', () => {
    setReleaseOpen(false);
    setStockdbOpen(false);
  });
  document.addEventListener('keydown', event => {
    if (event.key === 'Escape' && !panel.hidden) {
      setReleaseOpen(false);
      trigger.focus();
    } else if (event.key === 'Escape' && !stockdbPanel.hidden) {
      setStockdbOpen(false);
      stockdbTrigger.focus();
    }
  });

  const embeddedVersion = trigger.dataset.version;
  const embeddedDate = trigger.dataset.releaseDate;
  const hasEmbeddedSummary = embeddedVersion && !embeddedVersion.includes('%%QM_');
  if (hasEmbeddedSummary) renderSummary({version:embeddedVersion, release_date:embeddedDate});
  else api('/api/v1/release').then(renderSummary).catch(() => {});

  api('/api/v1/release').then(data => {
    renderSummary(data);
    renderReleases(data.releases);
  }).catch(() => {
    list.innerHTML = '<div class="msg">更新日志暂不可用，版本信息仍可正常查看。</div>';
  });
  function refreshStockdbStatus() {
    api('/api/v1/settings/free-stockdb').then(renderStockdbStatus).catch(() => {
      stockdbPopoverState.textContent = '暂时无法读取本地库状态';
    });
  }

  refreshStockdbStatus();
  api('/api/v1/settings/free-stockdb/vendor-notice').then(renderVendorNotice).catch(() => {
    vendorState.textContent = '暂不可用';
    vendorSummary.textContent = '暂时无法读取 free-stockdb 官方动态，可前往官网查看。';
  });
  setInterval(refreshStockdbStatus, 60_000);
  document.addEventListener('visibilitychange', () => {
    if (!document.hidden) refreshStockdbStatus();
  });
})();

/* ---------- 导航 ---------- */
const ACTIVE_TAB_STORAGE_KEY = 'quantmaster.activeTab';
const ACTIVE_WORKSPACE_PAGE_KEY = 'quantmaster.workspacePage.v1';
const TAB_WORKSPACE = {
  market:'observe', rotation:'observe', news:'observe',
  'after-close':'select', candidates:'select', 'stock-analysis':'select', decision:'select',
  lab:'research', backtest:'research', paper:'trade', ledger:'trade', automation:'automation',
};
const DEFAULT_WORKSPACE_PAGE = {
  observe:'quotes', select:'after-close', research:'lab', trade:'paper', automation:'automation',
};
const ROUTE_PAGE = {
  observe:{quotes:'market', temperature:'market', style:'market', rotation:'rotation', industry:'rotation', themes:'rotation', 'etf-flows':'rotation', news:'news'},
  select:{'after-close':'after-close', candidates:'candidates', 'stock-analysis':'stock-analysis', decision:'decision'},
  research:{lab:'lab', backtest:'backtest'}, trade:{paper:'paper', ledger:'ledger'}, automation:{automation:'automation'},
};

function storedActiveTab() {
  try { return sessionStorage.getItem(ACTIVE_TAB_STORAGE_KEY) || ''; }
  catch (_) { return ''; }
}

function rememberActiveTab(tab) {
  try { sessionStorage.setItem(ACTIVE_TAB_STORAGE_KEY, tab); }
  catch (_) { /* 禁用会话存储时仍可正常导航 */ }
}

function forgetActiveTab() {
  try { sessionStorage.removeItem(ACTIVE_TAB_STORAGE_KEY); }
  catch (_) { /* 禁用会话存储时无需清理 */ }
}

function storedWorkspacePage(workspace) {
  try {
    const value = JSON.parse(sessionStorage.getItem(ACTIVE_WORKSPACE_PAGE_KEY) || '{}');
    return typeof value?.[workspace] === 'string' ? value[workspace] : '';
  } catch (_) { return ''; }
}

function rememberWorkspacePage(workspace, page) {
  if (!workspace || !page) return;
  try {
    const value = JSON.parse(sessionStorage.getItem(ACTIVE_WORKSPACE_PAGE_KEY) || '{}');
    value[workspace] = page;
    sessionStorage.setItem(ACTIVE_WORKSPACE_PAGE_KEY, JSON.stringify(value));
  } catch (_) { /* 会话存储不可用时使用默认页 */ }
}

function workspaceForTab(tab) { return TAB_WORKSPACE[tab] || ''; }

function routeForControl(control) {
  const workspace = workspaceForTab(control?.dataset.tab || '');
  const page = control?.dataset.workspacePage || '';
  if (!workspace || !page) return '';
  return workspace === 'automation' ? '#automation' : `#${workspace}/${page}`;
}

function updateRoute(control) {
  const route = routeForControl(control);
  if (route && location.hash !== route) history.replaceState(null, '', route);
}

function pageControl(workspace, page) {
  if (workspace === 'automation' && page === 'automation') {
    return document.querySelector('header [data-workspace="automation"][data-tab="automation"]');
  }
  const expectedTab = ROUTE_PAGE[workspace]?.[page];
  return Array.from(document.querySelectorAll(`header [data-workspace-pages="${workspace}"] [data-tab]`)).find(control =>
    control.dataset.workspacePage === page && (!expectedTab || control.dataset.tab === expectedTab),
  ) || null;
}

function setWorkspace(workspace) {
  document.querySelectorAll('[data-workspace]').forEach(button => {
    button.classList.toggle('active', button.dataset.workspace === workspace);
    button.setAttribute('aria-current', button.dataset.workspace === workspace ? 'page' : 'false');
  });
  document.querySelectorAll('[data-workspace-pages]').forEach(group => {
    group.hidden = group.dataset.workspacePages !== workspace;
  });
  const context = document.querySelector('.workspace-context');
  if (context) context.hidden = !document.querySelector(`[data-workspace-pages="${workspace}"]`);
}

function tabControl(tab) {
  return Array.from(document.querySelectorAll('header [data-tab]')).find(control =>
    control.dataset.tab === tab && !control.hidden && !control.disabled
    && document.getElementById('tab-' + tab));
}

function loadActiveTab(tab) {
  if ((tab === 'market' || tab === 'rotation')
      && typeof window.loadRotationFeature === 'function') window.loadRotationFeature(tab);
  if (tab === 'ledger') loadLedger();
  if (tab === 'news') loadNews();
  if (tab === 'stock-analysis' && typeof window.loadStockAnalysis === 'function') {
    window.loadStockAnalysis();
  }
  if (tab === 'paper') loadPaper();
  if (tab === 'help' && typeof window.loadHelp === 'function') window.loadHelp();
  if (tab === 'candidates' && typeof window.loadCandidates === 'function') window.loadCandidates();
  if (tab === 'after-close' && typeof window.loadAfterClose === 'function') window.loadAfterClose();
  if (tab === 'automation' && typeof window.loadAutomation === 'function') window.loadAutomation();
  if (tab === 'lab' && typeof window.loadQuantLab === 'function') window.loadQuantLab();
  if (tab === 'decision' && !decisionLoaded && !decisionLoading) void loadDecisionHistory();
}

function activateTab(control, {persist = true, load = true, route = true} = {}) {
  const tab = control?.dataset.tab;
  if (!tab || !document.getElementById('tab-' + tab)) return false;
  const replayFearGreed = tab === 'market' && control.dataset.marketPage === 'quotes'
    && control.getAttribute('aria-selected') !== 'true';
  document.querySelectorAll('header [data-tab]').forEach(b => b.classList.toggle('active', b === control));
  document.querySelectorAll('header [role="tab"]').forEach(button => {
    button.setAttribute('aria-selected', String(button === control));
    button.tabIndex = button === control ? 0 : -1;
  });
  document.querySelectorAll('.tab').forEach(s => s.classList.toggle('active', s.id === 'tab-' + tab));
  if (persist) rememberActiveTab(tab);
  const workspace = workspaceForTab(tab);
  if (workspace) {
    setWorkspace(workspace);
    rememberWorkspacePage(workspace, control.dataset.workspacePage || DEFAULT_WORKSPACE_PAGE[workspace]);
    if (route) updateRoute(control);
  } else {
    document.querySelectorAll('[data-workspace]').forEach(button => button.classList.remove('active'));
    document.querySelectorAll('[data-workspace-pages]').forEach(group => { group.hidden = true; });
    const context = document.querySelector('.workspace-context');
    if (context) context.hidden = true;
  }
  if (tab !== 'help' && location.hash.startsWith('#help')) {
    history.replaceState(null, '', location.pathname + location.search);
  }
  control.scrollIntoView({block:'nearest', inline:'nearest'});
  Object.values(charts).forEach(c => c.resize());
  if (load) loadActiveTab(tab);
  if (window.QuantCharts) window.QuantCharts.activateTab(tab);
  if (replayFearGreed) requestAnimationFrame(() => {
    const view = document.getElementById('market-quotes-view');
    if (view && !view.hidden && document.getElementById('tab-market')?.classList.contains('active')) {
      replayFearGreedGaugeAnimation(view);
    }
  });
  return true;
}

document.querySelector('header').addEventListener('click', e => {
  const workspaceControl = e.target.closest('[data-workspace]');
  if (workspaceControl) {
    const workspace = workspaceControl.dataset.workspace;
    if (workspace === 'automation') {
      activateTab(workspaceControl);
      return;
    }
    const page = storedWorkspacePage(workspace) || DEFAULT_WORKSPACE_PAGE[workspace];
    pageControl(workspace, page)?.click();
    return;
  }
  const control = e.target.closest('[data-tab]');
  if (!control) return;
  activateTab(control);
});

function routeFromHash() {
  const match = location.hash.match(/^#(observe|select|research|trade)\/([a-z-]+)$/);
  if (match && ROUTE_PAGE[match[1]]?.[match[2]]) return {workspace:match[1], page:match[2]};
  if (location.hash === '#automation') return {workspace:'automation', page:'automation'};
  return null;
}

const initialRoute = routeFromHash();
const restoredTab = location.hash.startsWith('#help') ? 'help' : (initialRoute ? ROUTE_PAGE[initialRoute.workspace][initialRoute.page] : storedActiveTab());
const restoredControl = initialRoute ? pageControl(initialRoute.workspace, initialRoute.page) : tabControl(restoredTab);
if (restoredControl) {
  activateTab(restoredControl, {persist:false, load:false});
  window.addEventListener('load', () => loadActiveTab(restoredTab), {once:true});
} else if (restoredTab) {
  forgetActiveTab();
}

window.addEventListener('hashchange', () => {
  const route = routeFromHash();
  // 市场与轮动的同级页还要切换本页视图，由 rotation.js 统一接管。
  if (!route || route.workspace === 'observe') return;
  const control = pageControl(route.workspace, route.page);
  if (control) activateTab(control, {persist:true, load:true, route:false});
});

/* ---------- 我的标的 ---------- */
let assetListsData = { favorites:[], following:[], holdings:[] };
let assetListsLoaded = false;
let assetListsError = '';
let assetListsLoading = null;
let activeAssetList = 'favorites';
const assetListEmpty = {
  favorites:'暂无自选标的，可从右上方添加。',
  following:'暂无重点关注标的。',
  holdings:'实盘账本中暂无持仓。',
};

function renderAssetList() {
  const items = assetListsData[activeAssetList] || [];
  document.querySelectorAll('[data-asset-count]').forEach(el => {
    el.textContent = (assetListsData[el.dataset.assetCount] || []).length;
  });
  document.querySelectorAll('#asset-tabs [data-asset-list]').forEach(button => {
    const active = button.dataset.assetList === activeAssetList;
    button.classList.toggle('active', active);
    button.setAttribute('aria-pressed', String(active));
  });
  const out = document.getElementById('asset-list');
  if (!items.length) {
    out.innerHTML = `<div class="msg">${assetListEmpty[activeAssetList]}</div>`;
    return;
  }
  if (activeAssetList === 'holdings') {
    out.innerHTML = `<table class="asset-table"><thead><tr><th>代码</th><th>持有</th><th>成本</th><th>缓存现价</th><th>浮动盈亏</th><th>收益率</th><th>市值</th></tr></thead><tbody>
      ${items.map(item => `<tr><td><button class="symbol-action" data-show-asset="${esc(item.symbol)}">${esc(item.symbol)}</button></td>
        <td>${item.shares}</td><td>${fixed(item.avg_cost,2)}</td><td>${fixed(item.last,2)}</td>
        <td class="${cls(item.unrealized_pnl)}">${fixed(item.unrealized_pnl,2)}</td><td class="${cls(item.pnl_pct)}">${pct(item.pnl_pct)}</td>
        <td>${Number(item.market_value || 0).toLocaleString()}</td></tr>`).join('')}</tbody></table>`;
    return;
  }
  out.innerHTML = `<table class="asset-table"><thead><tr><th>代码 / 名称</th><th>缓存现价</th><th>日变动</th><th>行情日期</th><th></th></tr></thead><tbody>
    ${items.map(item => `<tr><td><button class="symbol-action" data-show-asset="${esc(item.symbol)}" data-asset-name="${esc(item.name || item.symbol)}">${esc(item.symbol)}</button>
      <span class="reason">${esc(item.name || '')}</span></td><td>${fixed(item.last,2)}</td>
      <td class="${cls(item.change_pct)}">${item.change_pct == null ? '—' : `${item.change_pct > 0 ? '+' : ''}${fixed(item.change_pct,2)}%`}</td>
      <td>${esc(item.as_of || '暂无缓存')}</td><td><button class="text-action" data-remove-asset="${activeAssetList}" data-symbol="${esc(item.symbol)}">移除</button></td></tr>`).join('')}</tbody></table>`;
}

function acceptAssetLists(data) {
  assetListsData = data || {favorites:[], following:[], holdings:[]};
  assetListsLoaded = true;
  assetListsError = '';
  renderAssetList();
  if (typeof updateDecisionAssetButtons === 'function') updateDecisionAssetButtons();
}

async function loadAssetLists(showError = true) {
  if (assetListsLoading) return assetListsLoading;
  assetListsError = '';
  if (typeof updateDecisionAssetButtons === 'function') updateDecisionAssetButtons();
  assetListsLoading = api('/api/v1/portfolio/lists');
  try {
    acceptAssetLists(await assetListsLoading);
    return assetListsData;
  } catch (error) {
    assetListsLoaded = false;
    assetListsError = error.message || '无法读取列表状态';
    if (showError) document.getElementById('asset-list').innerHTML = `<div class="err">${esc(error.message)}</div>`;
    if (typeof updateDecisionAssetButtons === 'function') updateDecisionAssetButtons();
    return null;
  } finally {
    assetListsLoading = null;
  }
}

document.getElementById('asset-tabs').addEventListener('click', event => {
  const button = event.target.closest('[data-asset-list]');
  if (!button) return;
  activeAssetList = button.dataset.assetList;
  renderAssetList();
});
document.getElementById('asset-add-form').addEventListener('submit', async event => {
  event.preventDefault();
  const form = event.currentTarget, button = form.querySelector('button[type=submit]');
  const values = new FormData(form), listName = values.get('list_name');
  button.disabled = true;
  try {
    acceptAssetLists(await post(`/api/v1/portfolio/lists/${encodeURIComponent(listName)}`, {
      symbol:values.get('symbol'), name:values.get('name'),
    }));
    activeAssetList = listName;
    const selected = listName;
    form.reset();
    form.elements.list_name.value = selected;
    renderAssetList();
    queueMarketReload();
  } catch (error) { reportLocalError('我的标的', '添加标的失败', error); }
  button.disabled = false;
});
document.getElementById('asset-list').addEventListener('click', async event => {
  const remove = event.target.closest('[data-remove-asset]');
  if (remove) {
    remove.disabled = true;
    try {
      acceptAssetLists(await api(`/api/v1/portfolio/lists/${remove.dataset.removeAsset}/${encodeURIComponent(remove.dataset.symbol)}`, {method:'DELETE'}));
      queueMarketReload();
    } catch (error) { remove.disabled = false; reportLocalError('我的标的', '移除标的失败', error); }
    return;
  }
  const symbol = event.target.closest('[data-show-asset]');
  if (symbol) showKline(symbol.dataset.showAsset, symbol.dataset.assetName || symbol.dataset.showAsset, '1d');
});
loadAssetLists();

/* ---------- 市场 ---------- */
let marketLoading = false;
let marketReloadPending = false;
let marketStreamCycle = 0;
let marketFearGreed = null;
const PERSONAL_MARKET_GROUP = '我的股票';
const marketGroupMeta = {
  [PERSONAL_MARKET_GROUP]: {order:0, description:'自选、关注与实盘持有'},
  'A股指数': {order:0, description:'宽基与科技成长基准'},
  '全球市场': {order:2, description:'港股、美股与亚太指数'},
  '商品与汇率': {order:3, description:'主要商品期货与货币'},
};
const assetMembershipLabels = {favorites:'自选', following:'关注', holdings:'持有'};

function marketOpportunity(rsi) {
  const rsiValue = rsi == null || rsi === '' ? Number.NaN : Number(rsi);
  const fearValue = marketFearGreed?.score == null
    ? Number.NaN : Number(marketFearGreed.score);
  const rsiLimit = Number(marketFearGreed?.thresholds?.rsi_add ?? 22);
  const fearLimit = Number(marketFearGreed?.thresholds?.fear_greed_rare ?? 10);
  if (!Number.isFinite(rsiValue)) return {code:'unavailable', label:'RSI 暂缺'};
  if (rsiValue < rsiLimit && Number.isFinite(fearValue) && fearValue < fearLimit) {
    return {code:'rare-bottom', label:'罕见大底机会'};
  }
  if (rsiValue < rsiLimit) return {code:'rsi-oversold', label:'加仓抄底观察'};
  return {code:'neutral', label:'暂无极端信号'};
}

function indicatorNumber(value) {
  if (value == null || value === '') return Number.NaN;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : Number.NaN;
}

function rsiVisualClass(value) {
  const threshold = Number(marketFearGreed?.thresholds?.rsi_add ?? 22);
  const parsed = indicatorNumber(value);
  return Number.isFinite(parsed) && parsed < threshold ? 'oversold' : '';
}

function fearGreedGaugeGeometry(width, height) {
  return {
    centerX:width * .5,
    centerY:height * .54,
    dialRadius:Math.min(width,height) * .36,
  };
}

function fearGreedGaugeLabels(width, height) {
  const {centerX,centerY,dialRadius} = fearGreedGaugeGeometry(width,height);
  const radius = dialRadius + 17;
  return [0,25,45,55,75,100].map(value => {
    const angle = (210 - value / 100 * 240) * Math.PI / 180;
    return {
      type:'text',silent:true,x:centerX + Math.cos(angle) * radius,
      y:centerY - Math.sin(angle) * radius,
      style:{text:String(value),fill:MUTED,font:'600 13px -apple-system, BlinkMacSystemFont, "PingFang SC", sans-serif',
        align:'center',verticalAlign:'middle'},
    };
  });
}

function fearGreedGaugeRotation(value) {
  return (210 - Math.max(0,Math.min(100,value)) * 2.4) * Math.PI / 180;
}

function fearGreedGaugeNeedle(value, width, height, tone, animatePointer = false) {
  const {centerX,centerY,dialRadius} = fearGreedGaugeGeometry(width,height);
  const rotation = fearGreedGaugeRotation(value);
  const needle = {
    id:'fear-greed-needle',type:'group',silent:true,x:centerX,y:centerY,rotation,
    transition:['rotation'],
    children:[
      {type:'line',shape:{x1:-7,y1:0,x2:dialRadius * .56,y2:0},
        style:{stroke:tone,lineWidth:3,lineCap:'round'}},
      {type:'circle',shape:{cx:0,cy:0,r:5},
        style:{fill:CHART_COLORS.surface,stroke:tone,lineWidth:2}},
      {type:'circle',shape:{cx:0,cy:0,r:1.5},style:{fill:tone}},
    ],
  };
  if (animatePointer && !REDUCED_MOTION) {
    needle.keyframeAnimation = {
      duration:640,easing:'cubicInOut',
      keyframes:[
        {percent:0,rotation:fearGreedGaugeRotation(0)},
        {percent:1,rotation},
      ],
    };
  }
  return needle;
}

function fearGreedGaugeOption(data, width = 240, height = 190, displayScore, animatePointer = false) {
  const targetScore = indicatorNumber(data?.score);
  const available = Number.isFinite(targetScore);
  const shownScore = displayScore === undefined ? targetScore : indicatorNumber(displayScore);
  const value = available && Number.isFinite(shownScore)
    ? Math.max(0,Math.min(100,shownScore)) : 0;
  const tone = targetScore < 25 ? CHART_COLORS.danger : targetScore < 45 ? CHART_COLORS.warning
    : targetScore < 55 ? CHART_COLORS.neutral : targetScore < 75 ? CHART_COLORS.primary : CHART_COLORS.down;
  return {
    __qmMotion:true, backgroundColor:'transparent', animation:!REDUCED_MOTION,
    animationDuration:REDUCED_MOTION ? 0 : 640, animationEasing:'cubicInOut',
    animationDurationUpdate:REDUCED_MOTION ? 0 : 320, animationEasingUpdate:'cubicInOut',
    graphic:[...fearGreedGaugeLabels(width,height),
      fearGreedGaugeNeedle(value,width,height,tone,animatePointer)],
    series:[{
      id:'fear-greed-dial',type:'gauge',min:0,max:100,startAngle:210,endAngle:-30,
      center:['50%','54%'], radius:'72%', splitNumber:20,
      axisLine:{roundCap:true,lineStyle:{width:7,color:[
        [.25,CHART_COLORS.danger],[.45,CHART_COLORS.warning],
        [.55,CHART_COLORS.neutral],[.75,CHART_COLORS.primary],[1,CHART_COLORS.down],
      ]}},
      pointer:{show:false},
      anchor:{show:false},
      axisTick:{show:false},
      splitLine:{show:false},
      axisLabel:{show:false},
      title:{show:true,offsetCenter:[0,'94%'],color:CHART_COLORS.ink2,
        fontSize:14,fontWeight:600,lineHeight:16},
      detail:{offsetCenter:[0,'42%'],color:available ? CHART_COLORS.ink : MUTED,
        fontSize:30,fontWeight:720,lineHeight:32,valueAnimation:!REDUCED_MOTION,
        formatter:animatedValue => available ? Number(animatedValue).toFixed(1) : '—'},
      data:[{value,name:data?.rating_label || '暂不可用'}],
    }],
  };
}

function fearGreedAsOf(value) {
  if (!value) return '';
  const parsed = new Date(value);
  if (!Number.isFinite(parsed.getTime())) return '';
  const options = {month:'numeric',day:'numeric',hour:'2-digit',minute:'2-digit',hourCycle:'h23'};
  if (parsed.getFullYear() !== new Date().getFullYear()) options.year = 'numeric';
  const parts = Object.fromEntries(new Intl.DateTimeFormat('zh-CN',options)
    .formatToParts(parsed).map(part => [part.type,part.value]));
  const year = parts.year ? `${parts.year}年` : '';
  return `${year}${Number(parts.month)}月${Number(parts.day)}日 ${parts.hour}:${parts.minute}`;
}

function fearGreedHistoryOption(data) {
  const history = Array.isArray(data?.history) ? data.history.filter(point =>
    point?.date && Number.isFinite(Number(point?.score))) : [];
  return baseOpt({
    animation:!REDUCED_MOTION,
    grid:{left:38,right:14,top:18,bottom:28},
    tooltip:{trigger:'axis',confine:true,formatter:params => {
      const point = params[0];
      return `${marketSparkDate(point?.value?.[0])}<br>恐贪指数&nbsp;&nbsp;<b>${Number(point?.value?.[1]).toFixed(1)}</b>`;
    }},
    xAxis:timeAxis(),
    yAxis:{...valAxis(),min:0,max:100,interval:25},
    graphic:history.length ? [] : [{type:'text',left:'center',top:'middle',
      style:{text:'历史数据暂缺',fill:MUTED,fontSize:11}}],
    series:[{
      name:'CNN 恐贪',type:'line',showSymbol:false,sampling:'lttb',smooth:.12,
      data:history.map(point => [point.date,Number(point.score)]),
      lineStyle:{width:2,color:CHART_COLORS.primary},
      areaStyle:{color:CHART_COLORS.primary,opacity:.08},
      markLine:{silent:true,symbol:'none',label:{show:true,formatter:'≤10 · 罕见恐惧',
        position:'insideStartTop',distance:6,color:CHART_COLORS.warning,fontSize:10,
        backgroundColor:CHART_COLORS.surface,borderRadius:3,padding:[2,5]},
        lineStyle:{color:CHART_COLORS.warning,width:1,type:'dashed'},data:[{yAxis:10}]},
    }],
  });
}

function renderFearGreedGauge(element, replay = false) {
  const chart = mkChart(element.id, false);
  if (!chart) return;
  if (replay) chart.__qmFearGreedEntered = false;
  const animationRevision = (chart.__qmFearGreedAnimationRevision || 0) + 1;
  chart.__qmFearGreedAnimationRevision = animationRevision;
  const width = element.clientWidth, height = element.clientHeight;
  const score = indicatorNumber(marketFearGreed?.score);
  const shouldEnter = Number.isFinite(score) && !REDUCED_MOTION && !chart.__qmFearGreedEntered;
  if (shouldEnter) {
    chart.__qmFearGreedEntered = true;
    chart.setOption(fearGreedGaugeOption(marketFearGreed,width,height,0),{notMerge:true});
    requestAnimationFrame(() => {
      if (chart.isDisposed() || chart.getDom() !== element
          || chart.__qmFearGreedAnimationRevision !== animationRevision) return;
      const target = fearGreedGaugeOption(marketFearGreed,width,height,undefined,true);
      target.animationDurationUpdate = 640;
      target.animationEasingUpdate = 'cubicInOut';
      chart.setOption(target,{notMerge:false});
    });
    return;
  }
  if (Number.isFinite(score)) chart.__qmFearGreedEntered = true;
  chart.setOption(fearGreedGaugeOption(marketFearGreed,width,height),{
    notMerge:!chart.__qmFearGreedEntered,
  });
}

function replayFearGreedGaugeAnimation(root = document) {
  root.querySelectorAll('[data-fear-greed-gauge]').forEach(element => {
    renderFearGreedGauge(element,true);
  });
}

function renderFearGreedVisuals(root = document) {
  root.querySelectorAll('[data-fear-greed-gauge]').forEach(element => {
    renderFearGreedGauge(element);
  });
  root.querySelectorAll('[data-fear-greed-history]').forEach(element => {
    const chart = mkChart(element.id, false);
    if (chart) chart.setOption(fearGreedHistoryOption(marketFearGreed),{notMerge:true});
  });
}

function rsiSparkPoints(history) {
  const points = (Array.isArray(history) ? history : []).map(point => {
    const date = marketSparkParsedDate(point?.[0]);
    const value = point?.[1] == null || point?.[1] === '' ? Number.NaN : Number(point[1]);
    return {date:point?.[0], timestamp:date.getTime(), value};
  }).filter(point => Number.isFinite(point.timestamp) && Number.isFinite(point.value))
    .sort((left,right) => left.timestamp - right.timestamp);
  if (!points.length) return [];
  const latest = new Date(points.at(-1).timestamp);
  const cutoff = new Date(Date.UTC(latest.getUTCFullYear(),latest.getUTCMonth() - 3,1));
  const lastCutoffDay = new Date(Date.UTC(
    cutoff.getUTCFullYear(),cutoff.getUTCMonth() + 1,0)).getUTCDate();
  cutoff.setUTCDate(Math.min(latest.getUTCDate(),lastCutoffDay));
  return points.filter(point => point.timestamp >= cutoff.getTime());
}

function rsiSparkMarkup(points, current) {
  if (points.length < 2) return '<span class="hint">RSI 曲线暂缺</span>';
  const width = 160, chartHeight = 42, left = 2, right = 2, top = 2, plotBottom = 36;
  const firstTime = points[0].timestamp;
  const timeSpan = Math.max(1,points.at(-1).timestamp - firstTime);
  const xPosition = point => left + (point.timestamp - firstTime) * (width - left - right) / timeSpan;
  const path = points.map((point,index) => {
    const x = xPosition(point);
    const y = top + (100 - Math.max(0,Math.min(100,point.value))) * (plotBottom - top) / 100;
    return `${index ? 'L' : 'M'}${x.toFixed(2)},${y.toFixed(2)}`;
  }).join(' ');
  const thresholdY = top + (100 - 22) * (plotBottom - top) / 100;
  const lastIndex = points.length - 1;
  const tickIndexes = [...new Set([0,Math.floor(lastIndex / 2),lastIndex])];
  const ticks = tickIndexes.map(index => {
    const point = points[index];
    const x = xPosition(point).toFixed(2);
    return `<line class="mkt-rsi-axis-tick" x1="${x}" x2="${x}" y1="38" y2="41"></line>`;
  }).join('');
  const dateLabels = tickIndexes.map(index => {
    const point = points[index];
    const position = (xPosition(point) / width * 100).toFixed(2);
    const edge = index === 0 ? ' first' : index === lastIndex ? ' last' : '';
    return `<span class="mkt-rsi-date-label${edge}" style="left:${position}%">${marketSparkDate(point.date)}</span>`;
  }).join('');
  return `<svg viewBox="0 0 ${width} ${chartHeight}" preserveAspectRatio="none" aria-hidden="true">
    <line class="mkt-rsi-threshold" x1="${left}" x2="${width-right}" y1="${thresholdY.toFixed(2)}" y2="${thresholdY.toFixed(2)}"></line>
    <path class="mkt-rsi-path ${rsiVisualClass(current)}" d="${path}"></path>
    <line class="mkt-rsi-axis" x1="${left}" x2="${width-right}" y1="38" y2="38"></line>${ticks}
    <line class="mkt-rsi-hover-line" x1="0" x2="0" y1="${top}" y2="${plotBottom}" visibility="hidden"></line>
    <ellipse class="mkt-rsi-hover-dot" cx="0" cy="0" rx="0" ry="0" visibility="hidden"></ellipse></svg>
    ${dateLabels}<span class="mkt-rsi-tooltip" hidden aria-hidden="true"><span data-rsi-hover-date></span><strong data-rsi-hover-value></strong></span>`;
}

function bindRsiSparkInteraction(root, points) {
  const svg = root.querySelector('svg');
  const guide = root.querySelector('.mkt-rsi-hover-line');
  const dot = root.querySelector('.mkt-rsi-hover-dot');
  const tooltip = root.querySelector('.mkt-rsi-tooltip');
  if (!svg || !guide || !dot || !tooltip || points.length < 2) return;
  const width = 160, chartHeight = 42, left = 2, right = 2, top = 2, plotBottom = 36;
  const dotRadius = 1.4;
  const firstTime = points[0].timestamp;
  const timeSpan = Math.max(1,points.at(-1).timestamp - firstTime);
  const xPosition = point => left + (point.timestamp - firstTime) * (width - left - right) / timeSpan;
  const yPosition = point => top
    + (100 - Math.max(0,Math.min(100,point.value))) * (plotBottom - top) / 100;
  const hide = () => {
    guide.setAttribute('visibility','hidden');
    dot.setAttribute('visibility','hidden');
    tooltip.hidden = true;
  };
  const show = (point,bounds) => {
    const x = xPosition(point), y = yPosition(point), position = x / width * 100;
    guide.setAttribute('x1',x.toFixed(2)); guide.setAttribute('x2',x.toFixed(2));
    guide.setAttribute('visibility','visible');
    dot.setAttribute('cx',x.toFixed(2)); dot.setAttribute('cy',y.toFixed(2));
    dot.setAttribute('rx',(dotRadius * width / bounds.width).toFixed(2));
    dot.setAttribute('ry',(dotRadius * chartHeight / bounds.height).toFixed(2));
    dot.setAttribute('visibility','visible');
    tooltip.querySelector('[data-rsi-hover-date]').textContent = String(point.date).slice(0,10);
    tooltip.querySelector('[data-rsi-hover-value]').textContent = `RSI ${point.value.toFixed(1)}`;
    tooltip.style.left = `${position.toFixed(2)}%`;
    tooltip.classList.toggle('edge-start',position < 25);
    tooltip.classList.toggle('edge-end',position > 75);
    tooltip.hidden = false;
  };
  root.onpointermove = event => {
    const bounds = svg.getBoundingClientRect();
    if (!bounds.width) return;
    const svgX = Math.max(left,Math.min(width - right,
      (event.clientX - bounds.left) / bounds.width * width));
    const targetTime = firstTime + (svgX - left) / (width - left - right) * timeSpan;
    let closest = points[0];
    for (const point of points.slice(1)) {
      if (Math.abs(point.timestamp - targetTime) < Math.abs(closest.timestamp - targetTime)) {
        closest = point;
      }
    }
    show(closest,bounds);
  };
  root.onpointerleave = hide;
}

function refreshSentimentBindings(root = document) {
  const score = marketFearGreed?.score == null
    ? Number.NaN : Number(marketFearGreed.score);
  const scoreText = Number.isFinite(score) ? score.toFixed(1) : '—';
  const label = marketFearGreed?.rating_label || '暂不可用';
  root.querySelectorAll('[data-fear-greed-score]').forEach(node => { node.textContent = scoreText; });
  root.querySelectorAll('[data-fear-greed-label]').forEach(node => {
    node.textContent = `${node.dataset.fearGreedPrefix || ''}${label}`;
  });
  root.querySelectorAll('[data-fear-greed-gauge]').forEach(node => {
    node.setAttribute('aria-label',`CNN 当日恐贪指数 ${scoreText}，${label}`);
  });
  root.querySelectorAll('[data-opportunity-rsi]').forEach(node => {
    const signal = marketOpportunity(node.dataset.opportunityRsi);
    node.classList.remove('rare-bottom','rsi-oversold','neutral','unavailable');
    node.classList.add(signal.code);
    node.textContent = signal.label;
  });
  renderFearGreedVisuals(root);
}

function acceptMarketFearGreed(data) {
  marketFearGreed = data || {status:'unavailable', score:null, rating_label:'暂不可用'};
  const status = document.getElementById('market-fear-greed-status');
  const timestamp = document.getElementById('market-fear-greed-time');
  const note = document.getElementById('market-fear-greed-note');
  if (status) status.textContent = marketFearGreed.status === 'stale'
    ? '本地缓存 · CNN 刷新失败' : marketFearGreed.status === 'ready'
      ? '全球风险背景' : 'CNN 指数暂不可用';
  if (timestamp) {
    const formatted = fearGreedAsOf(marketFearGreed.as_of);
    timestamp.textContent = formatted
      ? `${marketFearGreed.status === 'stale' ? '缓存于' : '更新于'} ${formatted}`
      : marketFearGreed.status === 'ready' ? '刚刚更新' : '等待更新';
    if (marketFearGreed.as_of) timestamp.dateTime = marketFearGreed.as_of;
    else timestamp.removeAttribute('datetime');
  }
  if (note) note.textContent = marketFearGreed.warning ||
    'CNN 指数是美国市场风险情绪参考；每个大盘与板块使用自己的日线 RSI(14)。';
  refreshSentimentBindings();
}

async function loadMarketFearGreed(force = false) {
  try {
    acceptMarketFearGreed(await api(`/api/v1/market/fear-greed?refresh=${force ? 'true' : 'false'}`));
  } catch (error) {
    acceptMarketFearGreed({status:'unavailable', score:null, rating_label:'暂不可用',
      warning:`CNN 指数读取失败：${error.message}；RSI 仍可独立使用。`});
  }
}

let marketSparkObserver = null;
const marketSparkRenderers = new WeakMap();
const marketSparkTasks = new Map();

function cancelMarketSparkTask(element) {
  const task = marketSparkTasks.get(element);
  if (!task) return;
  if (task.idle) window.cancelIdleCallback(task.handle);
  else window.clearTimeout(task.handle);
  marketSparkTasks.delete(element);
}

function scheduleMarketSpark(element) {
  cancelMarketSparkTask(element);
  const invoke = () => {
    marketSparkTasks.delete(element);
    if (!element.isConnected) return;
    marketSparkRenderers.get(element)?.();
  };
  const idle = typeof window.requestIdleCallback === 'function';
  const handle = idle
    ? window.requestIdleCallback(invoke,{timeout:250})
    : window.setTimeout(invoke,0);
  marketSparkTasks.set(element,{idle,handle});
}

function queueMarketSpark(element, render) {
  marketSparkRenderers.set(element,render);
  if (!('IntersectionObserver' in window)) {
    scheduleMarketSpark(element);
    return;
  }
  if (!marketSparkObserver) {
    marketSparkObserver = new IntersectionObserver(entries => {
      entries.forEach(entry => {
        if (!entry.isIntersecting) return;
        entry.target.dataset.marketSparkVisible = 'true';
        marketSparkObserver?.unobserve(entry.target);
        scheduleMarketSpark(entry.target);
      });
    },{rootMargin:'320px 0px'});
  }
  if (element.dataset.marketSparkVisible === 'true') scheduleMarketSpark(element);
  else marketSparkObserver.observe(element);
}

function disposeMarketSparks() {
  marketSparkObserver?.disconnect();
  marketSparkObserver = null;
  for (const element of marketSparkTasks.keys()) cancelMarketSparkTask(element);
  for (const [id, chart] of Object.entries(charts)) {
    if (id.startsWith('spark-stream-')) {
      chart.dispose();
      delete charts[id];
    }
  }
}

function marketChangeSeries(nav) {
  return (nav || []).map(point => [point[0], +((Number(point[1]) - 1) * 100).toFixed(4)]);
}

function marketSparkSummary(series) {
  const values = (series || []).map(point => Number(point[1])).filter(Number.isFinite);
  const minimum = values.length ? Math.min(...values, 0) : 0;
  const maximum = values.length ? Math.max(...values, 0) : 0;
  const span = Math.max(0.2, maximum - minimum);
  const padding = Math.max(0.12, span * 0.16);
  return {
    first:series?.[0]?.[0] ?? null,
    lastDate:series?.at(-1)?.[0] ?? null,
    last:values.at(-1) || 0,
    min:minimum - padding,
    max:maximum + padding,
  };
}

function marketSparkParsedDate(value) {
  const normalized = typeof value === 'string' && /^\d+$/.test(value) ? Number(value) : value;
  return new Date(normalized);
}

function marketSparkDate(value) {
  const parsed = marketSparkParsedDate(value);
  if (Number.isNaN(parsed.getTime())) return '—';
  return parsed.toLocaleDateString('zh-CN',{month:'2-digit',day:'2-digit'}).replaceAll('/','.');
}

function marketSparkMonth(value, includeYear = true) {
  const parsed = marketSparkParsedDate(value);
  if (Number.isNaN(parsed.getTime())) return '—';
  const month = String(parsed.getMonth() + 1).padStart(2,'0');
  return includeYear ? `${parsed.getFullYear()}.${month}` : `${month}月`;
}

function marketSparkOption(item, changeSeries) {
  const summary = marketSparkSummary(changeSeries);
  const categories = changeSeries.map(point => point[0]);
  const values = changeSeries.map(point => Number(point[1]));
  const lastIndex = categories.length - 1;
  const tickIndexes = new Set(categories.length < 3
    ? categories.map((_,index) => index)
    : [0,Math.round(lastIndex / 2),lastIndex]);
  const firstYear = categories.length ? marketSparkParsedDate(categories[0]).getFullYear() : null;
  const tone = summary.last > 0 ? CHART_COLORS.up
    : summary.last < 0 ? CHART_COLORS.down : CHART_COLORS.neutral;
  const signed = value => `${value > 0 ? '+' : ''}${Number(value).toFixed(2)}%`;
  const dailyChanges = changeSeries.map((point, index) => {
    if (!index) return null;
    const previous = 1 + Number(changeSeries[index - 1]?.[1] || 0) / 100;
    const current = 1 + Number(point?.[1] || 0) / 100;
    return previous ? (current / previous - 1) * 100 : null;
  });
  return {
    backgroundColor:'transparent',
    animation:!REDUCED_MOTION,
    grid:{left:22,right:22,top:8,bottom:19},
    xAxis:{
      type:'category',data:categories,show:true,boundaryGap:false,
      axisLine:{show:false},axisTick:{show:false},splitLine:{show:false},
      axisLabel:{
        show:true,color:MUTED,fontSize:9,margin:5,hideOverlap:true,
        showMinLabel:true,showMaxLabel:true,
        interval:index => tickIndexes.has(index),
        formatter:(value,index) => {
          if (index === 0 || index === lastIndex) return marketSparkMonth(value,true);
          const year = marketSparkParsedDate(value).getFullYear();
          return marketSparkMonth(value,year !== firstYear);
        },
      },
    },
    yAxis:{type:'value',show:false,scale:true,min:summary.min,max:summary.max},
    tooltip:{
      show:true,trigger:'axis',confine:true,transitionDuration:REDUCED_MOTION ? 0 : .18,
      axisPointer:{type:'line',snap:true,lineStyle:{color:AXIS,width:1,type:'dashed'}},
      formatter:params => {
        const point = params.find(value => value.seriesId === 'market-spark-trend') || params[0];
        const dataIndex = Number(point?.dataIndex);
        const rawValue = Array.isArray(point?.value) ? point.value[1] : point?.value;
        const parsedValue = Number(rawValue);
        const value = Number.isFinite(parsedValue) ? parsedValue : Number(values[dataIndex] || 0);
        const date = Array.isArray(point?.value) ? point.value[0] : categories[dataIndex];
        const daily = dailyChanges[dataIndex];
        const dailyText = Number.isFinite(daily) ? signed(daily) : '—';
        return `${marketSparkDate(date)}<br><span style="color:${tone}">●</span> 区间涨跌&nbsp;&nbsp;<b>${signed(value)}</b><br><span style="color:${tone}">●</span> 当日涨跌&nbsp;&nbsp;<b>${dailyText}</b>`;
      },
    },
    aria:{enabled:true,label:{description:`${item.name}区间走势图，最新区间涨跌${signed(summary.last)}`}},
    series:[
      {
        id:'market-spark-trend',name:'区间走势',type:'line',data:values,
        showSymbol:false,symbol:'none',sampling:'lttb',silent:false,
        lineStyle:{width:2,color:tone,cap:'round',join:'round'},
        areaStyle:{color:tone,opacity:.10,origin:'auto'},
        emphasis:{disabled:true},
        markLine:{silent:true,symbol:'none',label:{show:false},
          lineStyle:{color:'rgba(195,194,183,.32)',width:1,type:'dashed'},data:[{yAxis:0}]},
      },
      {
        id:'market-spark-latest',name:'最新位置',type:'scatter',silent:true,z:4,
        data:changeSeries.length ? [[categories.at(-1),values.at(-1)]] : [],symbolSize:7,
        itemStyle:{color:tone,borderColor:CHART_COLORS.surface,borderWidth:2},
        tooltip:{show:false},
      },
    ],
  };
}

function createMarketStreamRenderer(root, pinnedGroups = {}) {
  const cycle = ++marketStreamCycle, groups = new Map(), entries = new Map();
  let index = 0, count = 0;
  root.classList.add('market-groups');
  function ensureGroup(group) {
    if (groups.has(group)) return groups.get(group);
    const meta = marketGroupMeta[group] || {order:99, description:''};
    let section = pinnedGroups[group] || null;
    if (!section) {
      section = document.createElement('section');
      section.className = 'market-section';
      section.dataset.marketGroup = group;
      section.dataset.marketOrder = String(meta.order);
      section.style.order = String(meta.order);
      section.innerHTML = `<div class="market-section-head"><div class="market-section-heading">
        <h2 class="market-section-title">${esc(group)}</h2><span class="market-section-count">0 只</span></div>
        ${meta.description ? `<p class="market-section-description">${esc(meta.description)}</p>` : ''}</div>
        <div class="mkt-grid"></div><div class="market-section-empty" hidden></div>`;
      root.appendChild(section);
    }
    const entry = {section, grid:section.querySelector('.mkt-grid'),
      count:section.querySelector('.market-section-count'),
      empty:section.querySelector('.market-section-empty'), size:0};
    groups.set(group, entry);
    return entry;
  }
  function draw(entry, item) {
    entry.item = item;
    const changeSeries = marketChangeSeries(item.nav);
    const sparkSummary = marketSparkSummary(changeSeries);
    const sparkTone = sparkSummary.last > 0 ? CHART_COLORS.up
      : sparkSummary.last < 0 ? CHART_COLORS.down : CHART_COLORS.neutral;
    const periodTone = sparkSummary.last > 0 ? 'up' : sparkSummary.last < 0 ? 'down' : '';
    const periodReturn = `${sparkSummary.last > 0 ? '+' : ''}${sparkSummary.last.toFixed(2)}%`;
    entry.element.style.setProperty('--market-tone',sparkTone);
    entry.element.querySelector('.nm').innerHTML =
      `${esc(item.name)} <span class="badge">${esc(item.symbol)}</span>`;
    entry.element.querySelector('.mkt-window').textContent = `${changeSeries.length}D`;
    entry.element.querySelector('.px').className = `px ${cls(item.change_pct)}`;
    entry.element.querySelector('.px').innerHTML =
      `${item.last} <small>${item.change_pct > 0 ? '+' : ''}${item.change_pct}%</small>`;
    const memberships = (item.memberships || []).map(value => assetMembershipLabels[value]).filter(Boolean);
    const membership = entry.element.querySelector('.mkt-memberships');
    membership.textContent = memberships.join(' · ');
    membership.hidden = memberships.length === 0;
    const status = item.cache_status === 'stale' || item.cache_status === 'refresh_failed'
      ? '本地缓存 · 数据源暂不可用' : '数据截至';
    entry.element.querySelector('.mkt-meta').textContent =
      `${status}${item.as_of ? ` ${item.as_of}` : ''}${item.source ? ` · ${item.source}` : ''}`;
    const rsi = entry.element.querySelector('.mkt-rsi');
    rsi.textContent = fixed(item.rsi_14, 1);
    rsi.classList.toggle('oversold',rsiVisualClass(item.rsi_14) === 'oversold');
    const rsiSpark = entry.element.querySelector('.mkt-rsi-spark');
    const recentRsi = rsiSparkPoints(item.rsi_history);
    rsiSpark.innerHTML = rsiSparkMarkup(recentRsi,item.rsi_14);
    bindRsiSparkInteraction(rsiSpark,recentRsi);
    rsiSpark.setAttribute('aria-label',`${item.name} 最近三个月日线 RSI 曲线，共 ${recentRsi.length} 个交易日，当前 ${fixed(item.rsi_14,1)}，参考线 22；鼠标悬停可查看日期和数值`);
    const opportunity = entry.element.querySelector('[data-opportunity-rsi]');
    opportunity.dataset.opportunityRsi = item.rsi_14 == null ? '' : String(item.rsi_14);
    refreshSentimentBindings(entry.element);
    entry.element.querySelector('.mkt-spark-period').textContent =
      `${marketSparkDate(sparkSummary.first)}—${marketSparkDate(sparkSummary.lastDate)}`;
    const period = entry.element.querySelector('.mkt-period-return');
    period.className = `mkt-period-return ${periodTone}`;
    period.textContent = `区间 ${periodReturn}`;
    entry.element.setAttribute('aria-label',
      `${item.name} ${item.symbol}，现价 ${item.last}，日涨跌 ${item.change_pct > 0 ? '+' : ''}${item.change_pct}%，日线 RSI ${fixed(item.rsi_14,1)}，区间涨跌 ${periodReturn}，点击查看 K 线`);
    entry.element.onclick = () => showKline(item.symbol, item.name);
    queueMarketSpark(entry.element,() => {
      if (!document.getElementById(entry.sparkId)) return;
      const chart = mkChart(entry.sparkId);
      chart.setOption(marketSparkOption(item,changeSeries),{notMerge:true});
    });
  }
  return {
    add(group, item) {
      if (!item) return false;
      const key = `${group}\u0000${item.symbol}`;
      if (entries.has(key)) {
        draw(entries.get(key), item);
        return false;
      }
      const groupEntry = ensureGroup(group);
      const order = Math.min(index,8);
      const sparkId = `spark-stream-${cycle}-${index++}`;
      const element = document.createElement('button');
      element.type = 'button';
      element.className = 'mkt-item stream-enter';
      element.style.setProperty('--market-order',String(order));
      element.innerHTML = `<span class="mkt-item-head"><span class="nm"></span><span class="mkt-window"></span></span><span class="px"></span><span class="mkt-memberships" hidden></span><span class="mkt-meta"></span>
        <span class="mkt-indicators"><span class="mkt-rsi-reading"><span class="mkt-rsi-label"><span>RSI(14)</span><small>日线</small></span><b class="mkt-rsi">—</b></span><span class="state-pill opportunity-signal" data-opportunity-rsi=""></span></span>
        <span class="mkt-rsi-spark" role="img"></span>
        <span class="mkt-spark-shell"><span class="spark" id="${sparkId}"></span></span>
        <span class="mkt-spark-foot"><span class="mkt-spark-period"></span><span class="mkt-period-return"></span></span>`;
      groupEntry.grid.appendChild(element);
      groupEntry.size += 1;
      groupEntry.count.textContent = `${groupEntry.size} 只`;
      groupEntry.empty.hidden = true;
      count += 1;
      const entry = {element, sparkId, item}; entries.set(key, entry); draw(entry, item);
      return true;
    },
    addAll(data) {
      const unavailableByGroup = new Map();
      for (const item of data.unavailable_items || []) {
        if (!unavailableByGroup.has(item.group)) unavailableByGroup.set(item.group, []);
        unavailableByGroup.get(item.group).push(item);
      }
      for (const [group, items] of Object.entries(data.groups || {})) {
        for (const item of items) this.add(group, item);
        const entry = ensureGroup(group);
        const configured = Number(data.group_counts?.[group] ?? items.length);
        if (configured > entry.size) {
          entry.count.textContent = `${entry.size} / ${configured} 只`;
          const subject = group === PERSONAL_MARKET_GROUP ? '股票' : '标的';
          const unavailable = unavailableByGroup.get(group) || [];
          const examples = unavailable.slice(0, 4).map(item => item.name).join('、');
          const reason = unavailable[0]?.message ? `；${unavailable[0].message}` : '';
          entry.empty.textContent = entry.size
            ? `另有 ${configured - entry.size} 只${subject}暂无可绘制行情${examples ? `（${examples}）` : ''}${reason}。`
            : `${configured} 只${subject}暂时都没有可绘制行情${examples ? `（${examples}）` : ''}${reason}；区块会保留，可同步重试。`;
          entry.empty.hidden = false;
        } else if (!configured && group === PERSONAL_MARKET_GROUP) {
          entry.empty.textContent = '暂无自选、关注或持有股票；在上方添加后，这里会显示对应走势图。';
          entry.empty.hidden = false;
        } else {
          entry.count.textContent = `${entry.size} 只`;
          entry.empty.hidden = true;
        }
      }
    },
    failPinned(message) {
      for (const [group, section] of Object.entries(pinnedGroups)) {
        const entry = groups.get(group) || {
          count:section.querySelector('.market-section-count'),
          empty:section.querySelector('.market-section-empty'), size:0,
        };
        if (entry.size) continue;
        entry.count.textContent = '暂不可用';
        entry.empty.textContent = `${message}；主要指数区块已保留，可点击“同步最新行情”重试。`;
        entry.empty.hidden = false;
      }
    },
    get count() { return count; },
  };
}

function queueMarketReload() {
  if (marketLoading) {
    marketReloadPending = true;
    return;
  }
  void loadMarket('auto');
}

async function loadMarket(refresh = 'auto') {
  if (marketLoading) return;
  marketLoading = true;
  disposeMarketSparks();
  const majorIndexes = document.getElementById('major-indexes');
  majorIndexes.querySelector('.mkt-grid').replaceChildren();
  majorIndexes.querySelector('.market-section-count').textContent = '正在读取';
  majorIndexes.querySelector('.market-section-empty').textContent = '正在读取本地指数行情…';
  majorIndexes.querySelector('.market-section-empty').hidden = false;
  const container = document.getElementById('mkt-groups');
  const tracker = createLoadProgress(container, '准备市场数据', 'market');
  const renderer = createMarketStreamRenderer(tracker.results, {'A股指数':majorIndexes});
  void loadMarketFearGreed(refresh === 'incremental');
  try {
    const data = await streamJson(`/api/v1/market/overview/stream?refresh=${encodeURIComponent(refresh)}`, {}, event => {
      tracker.update(event);
      const partial = event.partial;
      if (partial?.kind === 'market_item') {
        tracker.reveal();
        renderer.add(partial.group, partial.item);
      }
    });
    renderer.addAll(data);
    if (renderer.count) tracker.reveal();
    tracker.finish(`已加载 ${renderer.count} 个行情标的，可点击查看 K 线`);
    document.getElementById('mkt-stamp').textContent =
      '检查于 ' + new Date().toLocaleTimeString('zh-CN', {hour: '2-digit', minute: '2-digit'});
  } catch (e) {
    renderer.failPinned('指数行情加载失败');
    tracker.fail(e.message);
    tracker.reveal().insertAdjacentHTML('beforeend',
      `<div class="err">市场数据加载失败：${esc(e.message)}\n已完成的卡片仍可继续使用。</div>`);
  } finally {
    marketLoading = false;
    if (marketReloadPending) {
      marketReloadPending = false;
      queueMicrotask(() => loadMarket('auto'));
    }
  }
}
document.getElementById('mkt-refresh').onsubmit = async e => {
  e.preventDefault(); busy(e.target, true, '同步中…');
  invalidateKlineSeriesCache();
  await loadMarket('incremental');
  busy(e.target, false);
};
function klineFrequencyName(frequency) {
  return frequency === '1d' ? '日线' : frequency.replace('m', ' 分钟');
}

function klineStartDate(frequency) {
  if (frequency !== '1d') {
    return new Date(Date.now() - 12 * 86400000).toISOString().slice(0, 10);
  }
  const start = new Date();
  start.setHours(0,0,0,0);
  start.setFullYear(start.getFullYear() - 3);
  return [start.getFullYear(),String(start.getMonth() + 1).padStart(2,'0'),
    String(start.getDate()).padStart(2,'0')].join('-');
}

const KLINE_CACHE_LIMIT = 64;
const KLINE_DAILY_TTL_MS = 5 * 60 * 1000;
const KLINE_INTRADAY_TTL_MS = 30 * 1000;
const klineSeriesCache = new Map();
const klineSeriesInflight = new Map();
let klineCacheGeneration = 0;

function invalidateKlineSeriesCache() {
  klineSeriesCache.clear();
  klineSeriesInflight.clear();
  klineCacheGeneration += 1;
}

function cachedKlineSeries(key) {
  const cached = klineSeriesCache.get(key);
  if (!cached) return null;
  if (cached.expiresAt <= Date.now()) {
    klineSeriesCache.delete(key);
    return null;
  }
  klineSeriesCache.delete(key);
  klineSeriesCache.set(key,cached);
  return cached.data;
}

function storeKlineSeries(key, data, ttl) {
  klineSeriesCache.delete(key);
  klineSeriesCache.set(key,{data,expiresAt:Date.now() + ttl});
  while (klineSeriesCache.size > KLINE_CACHE_LIMIT) {
    klineSeriesCache.delete(klineSeriesCache.keys().next().value);
  }
}

function consumeKlineRequest(entry, signal) {
  entry.consumers += 1;
  return new Promise((resolve,reject) => {
    let released = false;
    const release = () => {
      if (released) return;
      released = true;
      signal?.removeEventListener('abort',onAbort);
      entry.consumers -= 1;
      if (!entry.consumers && !entry.settled) entry.controller.abort();
    };
    const onAbort = () => {
      release();
      reject(new DOMException('请求已取消','AbortError'));
    };
    if (signal?.aborted) { onAbort(); return; }
    signal?.addEventListener('abort',onAbort,{once:true});
    entry.promise.then(data => {
      if (released) return;
      release(); resolve(data);
    },error => {
      if (released) return;
      release(); reject(error);
    });
  });
}

async function loadKlineSeries(symbol, frequency, {signal} = {}) {
  const start = klineStartDate(frequency);
  const key = `${symbol}\u0000${frequency}\u0000${start}`;
  const cached = cachedKlineSeries(key);
  if (cached) return cached;
  let entry = klineSeriesInflight.get(key);
  if (!entry) {
    const controller = new AbortController();
    const generation = klineCacheGeneration;
    const path = '/api/v1/market/history/' + encodeURIComponent(symbol)
      + `?frequency=${encodeURIComponent(frequency)}&start=${start}`;
    entry = {controller,consumers:0,settled:false,promise:null};
    entry.promise = api(path,{signal:controller.signal,cache:'no-store'}).then(data => {
      if (!Array.isArray(data.kline) || !data.kline.length) {
        throw new Error('所选周期暂无本地或远端数据');
      }
      if (generation === klineCacheGeneration) {
        storeKlineSeries(key,data,frequency === '1d' ? KLINE_DAILY_TTL_MS : KLINE_INTRADAY_TTL_MS);
      }
      return data;
    }).finally(() => {
      entry.settled = true;
      if (klineSeriesInflight.get(key) === entry) klineSeriesInflight.delete(key);
    });
    klineSeriesInflight.set(key,entry);
  }
  return consumeKlineRequest(entry,signal);
}

function rollingMean(values, windowSize) {
  const result = Array(values.length).fill(null);
  let total = 0, valid = 0;
  values.forEach((raw,index) => {
    const value = Number(raw);
    if (Number.isFinite(value)) { total += value; valid += 1; }
    if (index >= windowSize) {
      const expired = Number(values[index - windowSize]);
      if (Number.isFinite(expired)) { total -= expired; valid -= 1; }
    }
    if (index + 1 >= windowSize && valid === windowSize) {
      result[index] = +(total / windowSize).toFixed(3);
    }
  });
  return result;
}

function renderKlineSeries(chart, data) {
  chart.__quantmasterKlineData = data;
  const compact = chart.getDom().clientWidth < 520;
  const closes = data.kline.map(k => k[2]);
  const categories = data.kline.map(k => k[0]);
  const ma5 = rollingMean(closes,5), ma20 = rollingMean(closes,20);
  const priceAxis = valAxis();
  priceAxis.axisLabel = {...priceAxis.axisLabel, fontSize:compact ? 10 : 12};
  const volumeAxis = valAxis(v => v >= 1e8
    ? (v / 1e8).toFixed(1) + '亿' : (v / 1e4).toFixed(0) + '万');
  Object.assign(volumeAxis, {gridIndex:1, splitNumber:compact ? 1 : 2});
  volumeAxis.axisLabel = {...volumeAxis.axisLabel, show:!compact, fontSize:compact ? 9 : 12};
  chart.setOption(baseOpt({
    animationDuration:260, animationDurationUpdate:340,
    animationEasing:'cubicOut', animationEasingUpdate:'cubicOut',
    legend:{textStyle:{color:INK2},top:0,data:['MA5','MA20']},
    grid:[{left:compact ? 46 : 60,right:compact ? 8 : 20,top:38,height:compact ? '50%' : '58%'},
          {left:compact ? 46 : 60,right:compact ? 8 : 20,top:compact ? '69%' : '76%',height:compact ? '13%' : '12%'}],
    xAxis:[
      {type:'category',data:categories,axisLabel:{color:MUTED,fontSize:compact ? 9 : 12,hideOverlap:true,...(compact ? {interval:2} : {})},axisLine:{lineStyle:{color:AXIS}}},
      {type:'category',gridIndex:1,data:categories,axisLabel:{show:false},axisLine:{lineStyle:{color:AXIS}},axisTick:{show:false}},
    ],
    yAxis:[priceAxis,volumeAxis],
    dataZoom:[
      {id:'market-kline-x-wheel',type:'inside',xAxisIndex:[0,1],filterMode:'none'},
      {id:'market-kline-x-slider',type:'slider',xAxisIndex:[0,1],filterMode:'none',
        height:16,bottom:2,borderColor:AXIS,textStyle:{color:MUTED}},
    ],
    series:[
      {type:'candlestick',data:data.kline.map(k => k.slice(1,5)),
        itemStyle:{color:CHART_COLORS.up,color0:CHART_COLORS.down,borderColor:CHART_COLORS.up,borderColor0:CHART_COLORS.down}},
      {name:'MA5',type:'line',data:ma5,showSymbol:false,lineStyle:{width:1.5}},
      {name:'MA20',type:'line',data:ma20,showSymbol:false,lineStyle:{width:1.5}},
      {name:'成交量',type:'bar',xAxisIndex:1,yAxisIndex:1,
        data:data.kline.map(k => ({value:k[5],itemStyle:{color:k[2] >= k[1] ? 'rgba(230,103,103,.52)' : 'rgba(36,160,107,.52)'}})),
        barMaxWidth:8,silent:true},
    ],
  }), {notMerge:true});
}

let activeKline = {symbol:'', name:'', frequency:'1d', request:0, controller:null};
async function showKline(symbol, name, frequency = '1d') {
  const previousController = activeKline.controller;
  const request = activeKline.request + 1;
  const controller = new AbortController();
  activeKline = {symbol, name, frequency, request, controller};
  const panel = document.getElementById('kline-panel');
  panel.style.display = '';
  document.querySelectorAll('#kline-frequency button').forEach(button => {
    const active = button.dataset.frequency === frequency;
    button.classList.toggle('active', active);
    button.setAttribute('aria-pressed', String(active));
  });
  document.getElementById('kline-title').textContent =
    `${name}（${symbol}）· ${klineFrequencyName(frequency)}`;
  const chart = mkChart('kline', false);
  chart.showLoading({textColor:INK2,maskColor:'rgba(13,13,13,0.6)'});
  panel.scrollIntoView({behavior:'auto',block:'start'});
  try {
    const pending = loadKlineSeries(symbol, frequency, {signal:controller.signal});
    previousController?.abort();
    const data = await pending;
    if (request !== activeKline.request) return;
    chart.hideLoading();
    renderKlineSeries(chart, data);
  } catch (error) {
    if (request !== activeKline.request || error?.name === 'AbortError') return;
    chart.hideLoading();
    reportLocalError('K 线', '行情加载失败', error);
  }
}
document.getElementById('kline-frequency').addEventListener('click', e => {
  const frequency = e.target.dataset.frequency;
  if (!frequency || !activeKline.symbol || frequency === activeKline.frequency) return;
  showKline(activeKline.symbol, activeKline.name, frequency);
});
loadMarket();

/* ---------- 决策 ---------- */
let decisionLoaded = false, decisionLoading = false, decisionHistoryLoading = false;
let decisionHistoryKey = '', decisionViewRequest = 0;
function fixed(v, digits = 2) { return v == null || !Number.isFinite(+v) ? '—' : (+v).toFixed(digits); }
function directionLabel(value) { return value === 'up' ? '上行' : value === 'down' ? '下行' : '震荡'; }
function actionLabel(value) { return value === 'buy' ? '目标持仓' : value === 'watch' ? '观察候选' : '回避'; }
function decisionPositionStateLabel(value) {
  return ({invested:'已配置',reduced:'降仓',flat:'主动空仓',degraded:'数据不足'})[value] || '仓位待定';
}
function decisionPositionReasonText(reasons = []) {
  const labels = {allocated:'仓位已分配',opportunity_limited:'合格机会不足，组合仓位已缩放',
    market_risk_off:'市场基础仓位低于 20%',no_qualified_candidates:'没有合格标的',
    insufficient_signal_data:'评分、市场或 20 日波动率数据不足',rebalance_buffer:'变动位于交易缓冲带内'};
  return reasons.map(reason => labels[reason] || reason).join('；');
}
const DECISION_PROFILES = {
  risk_adjusted:'扣费风险收益', short_term:'短期命中收益', stable:'稳定可解释', legacy:'历史模型',
};
const DECISION_COMPONENTS = {rule:'规则', factor:'因子', ml:'ML'};
const DECISION_PROFILE_STORAGE_KEY = 'quantmaster.decision.profile.v2';
function decisionProfileLabel(value) { return DECISION_PROFILES[value] || value || '扣费风险收益'; }
function decisionComponentLabel(value) { return DECISION_COMPONENTS[value] || String(value || '').toUpperCase(); }
function decisionComponentState(component) {
  if (component?.status === 'fallback') return '已回退';
  if (component?.role === 'ml' && component?.status !== 'production') return '影子';
  return '生效';
}
function decisionModelEvidenceMarkup(snapshot, selection = {}) {
  if (!snapshot) return '';
  const components = snapshot.components || [];
  const fallback = Boolean(snapshot.fallback_active || components.some(item => item.status === 'fallback'));
  const effective = snapshot.effective_weights || {};
  const componentMarkup = components.map(component => {
    const weight = component.effective_weight ?? effective[component.role] ?? component.weight;
    const version = component.version_id || component.kind || 'built-in';
    const status = decisionComponentState(component);
    const note = component.fallback_reason || `${version} · ${component.scope || 'exact'}`;
    return `<div class="decision-component" data-status="${esc(component.status || 'active')}">
      <div class="decision-component-head"><strong title="${esc(component.name || version)}">${esc(decisionComponentLabel(component.role))} · ${esc(component.name || version)}</strong><span>${weight == null ? status : pct(weight)}</span></div>
      <small>${esc(status)} · ${esc(note)}</small></div>`;
  }).join('');
  const warnings = [...new Set([...(snapshot.warnings || []), ...(selection.warnings || [])])];
  const validation = selection.validation_summary || {};
  const hash = snapshot.policy_hash || selection.policy_hash || '';
  return `<details class="decision-model-evidence">
    <summary><strong>模型依据与不可变快照</strong><span>${esc(snapshot.profile_label || decisionProfileLabel(snapshot.profile))} · ${components.length} 个组件 · ${fallback ? '自动回退中' : '运行正常'}</span></summary>
    <div class="decision-model-body">
      <div class="decision-component-grid">${componentMarkup || '<div class="decision-component"><div class="decision-component-head"><strong>规则基线</strong><span>100.00%</span></div><small>未匹配到 Quant Lab Champion</small></div>'}</div>
      <div class="decision-policy-meta"><span>引擎 <code>${esc(snapshot.engine_version || 'hybrid-v2')}</code></span><span>快照 <code>${esc(hash ? hash.slice(0,16) : '—')}</code></span><span>校准样本 <code>${esc(validation.samples ?? '—')}</code></span><span>Brier <code>${fixed(validation.brier_score,3)}</code></span></div>
      ${warnings.length ? `<ul class="decision-policy-warnings">${warnings.map(item => `<li>${esc(item)}</li>`).join('')}</ul>` : ''}
    </div></details>`;
}
function decisionPickEvidenceMarkup(pick) {
  const scores = Object.entries(pick.component_scores || {}).filter(([, value]) => value != null);
  const rows = scores.map(([role, value]) => `<span class="pick-contribution"><b>${esc(decisionComponentLabel(role))}</b><em>${fixed(value,1)}</em><i style="--component-score:${Math.max(0,Math.min(100,+value || 0))}%"></i></span>`).join('');
  const reasons = (pick.reasons || []).map(esc).join(' · ');
  return `<details class="pick-evidence"><summary>查看贡献</summary><div class="pick-contributions">${rows || '<span>仅规则基线</span>'}</div><div class="pick-reasons">${reasons || '暂无补充说明'}</div></details>`;
}

const DECISION_KLINE_CHART_ID = 'decision-kline';
const DECISION_KLINE_FREQUENCIES = [
  ['1d','日线'], ['60m','60分'], ['15m','15分'], ['5m','5分'], ['1m','1分'],
];
const decisionKlineState = {
  symbol:'', name:'', frequency:'1d', request:0, loading:false,
  data:null, error:'', assetMessage:'', assetError:'',
};
const decisionAssetBusy = new Set();
const decisionAssetLabels = {favorites:'自选', following:'关注'};

function assetListContains(listName, symbol) {
  return (assetListsData[listName] || []).some(item => item.symbol === symbol);
}

function decisionKlineBodyMarkup() {
  if (decisionKlineState.loading) {
    return `<div class="decision-kline-loading" role="status"><span>正在加载${esc(klineFrequencyName(decisionKlineState.frequency))}行情…</span><i></i><i></i><i></i></div>`;
  }
  if (decisionKlineState.error) {
    return `<div class="decision-kline-error" role="alert"><div><strong>行情图未加载</strong><p>${esc(decisionKlineState.error)}。可切换周期或稍后重试。</p></div><button class="decision-inline-retry" type="button" data-decision-kline-retry>重新加载行情</button></div>`;
  }
  return `<div class="decision-kline-canvas" id="${DECISION_KLINE_CHART_ID}" role="img" aria-label="${esc(decisionKlineState.name)} ${esc(klineFrequencyName(decisionKlineState.frequency))} K 线与成交量"></div>`;
}

function decisionKlineDetailMarkup() {
  const frequencies = DECISION_KLINE_FREQUENCIES.map(([value, label]) => {
    const active = decisionKlineState.frequency === value;
    return `<button type="button" data-decision-frequency="${value}" class="${active ? 'active' : ''}" aria-pressed="${active}">${label}</button>`;
  }).join('');
  return `<tr class="decision-detail-row" data-decision-detail="${esc(decisionKlineState.symbol)}"><td colspan="10">
    <div class="decision-detail-shell" id="decision-kline-detail">
      <div class="decision-detail-toolbar">
        <div class="decision-detail-title"><span class="eyebrow">行情核查</span><strong>${esc(decisionKlineState.name)}</strong><span class="badge">${esc(decisionKlineState.symbol)}</span></div>
        <div class="decision-detail-tools">
          <div class="segmented decision-frequency" aria-label="行情周期">${frequencies}</div>
          <div class="decision-list-actions" aria-label="自选与关注">
            <button class="decision-list-toggle" type="button" data-decision-asset-toggle="favorites" aria-pressed="false">读取状态…</button>
            <button class="decision-list-toggle" type="button" data-decision-asset-toggle="following" aria-pressed="false">读取状态…</button>
          </div>
          <button class="decision-detail-close" type="button" data-decision-kline-close aria-label="收起 ${esc(decisionKlineState.name)} 行情">收起</button>
        </div>
      </div>
      <div class="decision-detail-feedback" data-decision-asset-feedback role="status" aria-live="polite"><span></span><button class="decision-inline-retry" type="button" data-decision-assets-retry hidden>重试列表状态</button></div>
      ${decisionKlineBodyMarkup()}
    </div></td></tr>`;
}

function updateDecisionAssetButtons() {
  const root = document.querySelector('.decision-detail-row');
  if (!root || !decisionKlineState.symbol) return;
  const pending = decisionAssetBusy.size > 0;
  root.querySelectorAll('[data-decision-asset-toggle]').forEach(button => {
    const listName = button.dataset.decisionAssetToggle;
    const label = decisionAssetLabels[listName];
    const active = assetListsLoaded && assetListContains(listName, decisionKlineState.symbol);
    button.disabled = pending || !assetListsLoaded;
    button.classList.toggle('loading', pending);
    button.setAttribute('aria-pressed', String(active));
    button.textContent = pending && decisionAssetBusy.has(listName)
      ? '处理中…' : !assetListsLoaded ? '读取状态…' : active ? `已${label}` : `加入${label}`;
    button.setAttribute('aria-label', !assetListsLoaded
      ? `${label}状态尚未读取`
      : `${active ? '移出' : '加入'}${label}：${decisionKlineState.name}`);
    button.title = active ? `再次点击移出${label}` : `加入${label}`;
  });
  const feedback = root.querySelector('[data-decision-asset-feedback]');
  const text = feedback?.querySelector('span');
  const retry = feedback?.querySelector('[data-decision-assets-retry]');
  if (!feedback || !text || !retry) return;
  feedback.classList.remove('success','error');
  retry.hidden = true;
  if (decisionKlineState.assetError) {
    feedback.classList.add('error');
    text.textContent = decisionKlineState.assetError;
  } else if (assetListsError) {
    feedback.classList.add('error');
    text.textContent = `自选与关注状态读取失败：${assetListsError}`;
    retry.hidden = false;
  } else if (!assetListsLoaded) {
    text.textContent = '正在读取自选与关注状态…';
  } else if (decisionKlineState.assetMessage) {
    feedback.classList.add('success');
    text.textContent = decisionKlineState.assetMessage;
  } else {
    text.textContent = '快捷操作会同步到市场页“我的股票”。';
  }
}

function syncDecisionDetailWidth(shell = document.querySelector('#decision-out .decision-detail-shell')) {
  const scroller = shell?.closest('.decision-table-scroll');
  if (!shell || !scroller) return;
  shell.style.setProperty('--decision-detail-inline-size', `${scroller.clientWidth}px`);
}

function scrollDecisionDetailVertically(shell) {
  if (!shell) return;
  const rect = shell.getBoundingClientRect();
  const headerBottom = document.querySelector('header')?.getBoundingClientRect().bottom || 0;
  const safeTop = Math.max(12, headerBottom + 12);
  const safeBottom = window.innerHeight - 16;
  if (rect.top >= safeTop && rect.top <= safeBottom) return;
  window.scrollTo({
    top:Math.max(0, window.scrollY + rect.top - safeTop),
    behavior:REDUCED_MOTION ? 'auto' : 'smooth',
  });
}

function mountDecisionKline({scroll = false} = {}) {
  disposeChart(DECISION_KLINE_CHART_ID);
  document.querySelectorAll('#decision-out .decision-detail-row').forEach(row => row.remove());
  const triggers = Array.from(document.querySelectorAll('#decision-out [data-decision-kline-trigger]'));
  triggers.forEach(trigger => {
    const active = Boolean(decisionKlineState.symbol)
      && trigger.dataset.decisionKlineTrigger === decisionKlineState.symbol;
    trigger.setAttribute('aria-expanded', String(active));
    trigger.closest('tr')?.classList.toggle('is-expanded', active);
  });
  if (!decisionKlineState.symbol) return;
  const trigger = triggers.find(item => item.dataset.decisionKlineTrigger === decisionKlineState.symbol);
  if (!trigger) return;
  const row = trigger.closest('tr');
  row.insertAdjacentHTML('afterend', decisionKlineDetailMarkup());
  const detailShell = row.nextElementSibling?.querySelector('.decision-detail-shell');
  syncDecisionDetailWidth(detailShell);
  updateDecisionAssetButtons();
  if (decisionKlineState.data && !decisionKlineState.loading && !decisionKlineState.error) {
    const chart = mkChart(DECISION_KLINE_CHART_ID, false);
    if (chart) {
      renderKlineSeries(chart, decisionKlineState.data);
      queueMicrotask(() => chart.resize());
    }
  }
  if (scroll) scrollDecisionDetailVertically(detailShell);
}

function closeDecisionKline() {
  decisionKlineState.request += 1;
  Object.assign(decisionKlineState, {
    symbol:'', name:'', frequency:'1d', loading:false, data:null,
    error:'', assetMessage:'', assetError:'',
  });
  mountDecisionKline();
}

async function loadDecisionKline({scroll = false} = {}) {
  if (!decisionKlineState.symbol) return;
  const symbol = decisionKlineState.symbol, frequency = decisionKlineState.frequency;
  const request = decisionKlineState.request + 1;
  Object.assign(decisionKlineState, {request, loading:true, data:null, error:''});
  mountDecisionKline({scroll});
  try {
    const data = await loadKlineSeries(symbol, frequency);
    if (request !== decisionKlineState.request
        || symbol !== decisionKlineState.symbol || frequency !== decisionKlineState.frequency) return;
    Object.assign(decisionKlineState, {loading:false, data, error:''});
  } catch (error) {
    if (request !== decisionKlineState.request
        || symbol !== decisionKlineState.symbol || frequency !== decisionKlineState.frequency) return;
    Object.assign(decisionKlineState, {
      loading:false, data:null, error:error.message || '无法读取行情',
    });
  }
  mountDecisionKline();
}

function openDecisionKline(row) {
  const symbol = row?.dataset.symbol;
  if (!symbol) return;
  if (decisionKlineState.symbol === symbol) {
    closeDecisionKline();
    return;
  }
  decisionKlineState.request += 1;
  Object.assign(decisionKlineState, {
    symbol, name:row.dataset.name || symbol, frequency:'1d', loading:false,
    data:null, error:'', assetMessage:'', assetError:'',
  });
  if (!assetListsLoaded && !assetListsLoading) void loadAssetLists(false);
  void loadDecisionKline({scroll:true});
}

async function toggleDecisionAsset(listName) {
  if (!decisionKlineState.symbol || !assetListsLoaded || decisionAssetBusy.size) return;
  if (!Object.prototype.hasOwnProperty.call(decisionAssetLabels, listName)) return;
  const symbol = decisionKlineState.symbol, name = decisionKlineState.name;
  const label = decisionAssetLabels[listName];
  const active = assetListContains(listName, symbol);
  const previous = assetListsData;
  const items = active
    ? (assetListsData[listName] || []).filter(item => item.symbol !== symbol)
    : [{symbol, name}, ...(assetListsData[listName] || [])];
  assetListsData = {...assetListsData, [listName]:items};
  decisionAssetBusy.add(listName);
  decisionKlineState.assetError = '';
  decisionKlineState.assetMessage = active ? `已移出${label}` : `已加入${label}`;
  renderAssetList();
  updateDecisionAssetButtons();
  try {
    const data = active
      ? await api(`/api/v1/portfolio/lists/${listName}/${encodeURIComponent(symbol)}`, {method:'DELETE'})
      : await post(`/api/v1/portfolio/lists/${listName}`, {symbol, name});
    acceptAssetLists(data);
    queueMarketReload();
  } catch (error) {
    assetListsData = previous;
    renderAssetList();
    if (decisionKlineState.symbol === symbol) {
      decisionKlineState.assetMessage = '';
      decisionKlineState.assetError = `${active ? '移出' : '加入'}${label}未完成：${error.message || '请稍后重试'}`;
    }
  } finally {
    decisionAssetBusy.delete(listName);
    updateDecisionAssetButtons();
  }
}

const REGIME_WINDOWS = {
  '7d':{label:'7D', days:7}, '14d':{label:'14D', days:14},
  '1m':{label:'1M', months:1}, '3m':{label:'3M', months:3}, '6m':{label:'6M', months:6},
  '1y':{label:'1Y', years:1}, '3y':{label:'3Y', years:3}, '5y':{label:'5Y', years:5}, '10y':{label:'10Y', years:10},
};
let regimeHistory = [];
let activeRegimeWindow = '3m';

function renderRegimeChart(windowKey = activeRegimeWindow) {
  const spec = REGIME_WINDOWS[windowKey] || REGIME_WINDOWS['3m'];
  activeRegimeWindow = windowKey;
  const lastDate = regimeHistory.length
    ? new Date(`${regimeHistory[regimeHistory.length - 1].date}T00:00:00Z`) : new Date();
  const cutoff = new Date(lastDate);
  if (spec.days) cutoff.setUTCDate(cutoff.getUTCDate() - spec.days);
  if (spec.months) cutoff.setUTCMonth(cutoff.getUTCMonth() - spec.months);
  if (spec.years) cutoff.setUTCFullYear(cutoff.getUTCFullYear() - spec.years);
  const history = regimeHistory.filter(row => new Date(`${row.date}T00:00:00Z`) >= cutoff);
  document.querySelectorAll('[data-regime-window]').forEach(button => {
    const active = button.dataset.regimeWindow === windowKey;
    button.classList.toggle('active', active);
    button.setAttribute('aria-pressed', String(active));
  });
  const count = document.getElementById('regime-window-count');
  if (count) count.textContent = `${spec.label} · ${history.length} 个交易日`;
  const chart = mkChart('regime-chart', false);
  if (!chart) return;
  chart.setOption(baseOpt({
    animationDuration:280, animationDurationUpdate:360,
    animationEasing:'cubicOut', animationEasingUpdate:'cubicOut',
    grid:{left:42,right:45,top:44,bottom:36},
    legend:{textStyle:{color:INK2},top:0},
    xAxis:timeAxis(), yAxis:[valAxis(),{...valAxis(v => (v * 100).toFixed(0) + '%'),min:0,max:1}],
    series:[
      {name:'牛熊分',type:'line',data:history.map(r => [r.date,r.bull_score]),showSymbol:false,smooth:.16,lineStyle:{width:2}},
      {name:'上涨宽度',type:'line',yAxisIndex:1,data:history.map(r => [r.date,r.advance_ratio]),showSymbol:false,lineStyle:{width:1.5,color:CHART_COLORS.up}},
      {name:'站上MA20',type:'line',yAxisIndex:1,data:history.map(r => [r.date,r.above_ma20_ratio]),showSymbol:false,lineStyle:{width:1.5,color:CHART_COLORS.compare}},
    ],
  }), {notMerge:true});
}

function renderDecisionRsiChart(market, sectors) {
  const chart = mkChart('decision-rsi-chart', false);
  if (!chart) return;
  const marketData = (market?.past || []).filter(row =>
    row?.date && Number.isFinite(Number(row?.rsi_14))).map(row => [row.date,Number(row.rsi_14)]);
  const sectorSeries = (sectors || []).map((sector,index) => ({
    name:String(sector.sector || `板块 ${index + 1}`), type:'line', showSymbol:false,
    data:(sector.rsi_history || []).filter(point => point?.[0] && Number.isFinite(Number(point?.[1])))
      .map(point => [point[0],Number(point[1])]),
    lineStyle:{width:1.2,color:PALETTE[index % PALETTE.length],opacity:.72},
    emphasis:{focus:'series',lineStyle:{width:2.4,opacity:1}},
  }));
  chart.setOption(baseOpt({
    animation:!REDUCED_MOTION,
    grid:{left:38,right:16,top:42,bottom:30},
    legend:{type:'scroll',top:0,left:0,right:0,textStyle:{color:MUTED,fontSize:10},pageTextStyle:{color:MUTED}},
    tooltip:{trigger:'axis',confine:true,valueFormatter:value => Number(value).toFixed(1)},
    xAxis:timeAxis(),
    yAxis:{...valAxis(),min:0,max:100,interval:25},
    graphic:marketData.length ? [] : [{type:'text',left:'center',top:'middle',
      style:{text:'RSI 历史数据暂缺',fill:MUTED,fontSize:11}}],
    series:[{
      name:'候选等权大盘',type:'line',showSymbol:false,sampling:'lttb',data:marketData,
      lineStyle:{width:2.8,color:CHART_COLORS.ink},z:4,
      markLine:{silent:true,symbol:'none',label:{show:true,formatter:'抄底观察 22',color:CHART_COLORS.warning,fontSize:9},
        lineStyle:{color:CHART_COLORS.warning,width:1,type:'dashed'},data:[{yAxis:22}]},
    },...sectorSeries],
  }),{notMerge:true});
}

function decisionSignedPercent(value) {
  if (value == null || !Number.isFinite(+value)) return '—';
  return `${+value > 0 ? '+' : ''}${pct(+value)}`;
}

function decisionPositionPicks(snapshot) {
  const picks = snapshot.picks || [];
  return picks.some(pick => Object.prototype.hasOwnProperty.call(pick,'target_weight'))
    ? picks.filter(pick => Number(pick.target_weight || 0) > 0)
    : picks;
}

function decisionSnapshotPicksMarkup(snapshot) {
  const picks = decisionPositionPicks(snapshot).slice(0,3);
  return `<div class="snapshot-pick-list">${picks.map(pick => `<div class="snapshot-pick"><span class="snapshot-pick-name" title="${esc(pick.name || '名称待同步')}">${esc(pick.name || '名称待同步')}</span><span class="snapshot-pick-symbol">${esc(pick.symbol)}${pick.target_weight == null ? '' : ` · ${pct(pick.target_weight)}`}</span></div>`).join('') || '<span class="snapshot-pick-symbol">空仓，无持仓验证</span>'}</div>`;
}

function decisionSnapshotSummaryMarkup(snapshot) {
  const picks = decisionPositionPicks(snapshot).slice(0,3);
  return `<div class="snapshot-pick-summary">${picks.map(pick => `<span title="${esc(`${pick.name || '名称待同步'} · ${pick.symbol || ''}`)}">${esc(pick.name || pick.symbol || '名称待同步')}</span>`).join('<i aria-hidden="true">·</i>') || '<span>空仓</span>'}</div>`;
}

function decisionFollowUpSummaryMarkup(snapshot) {
  const validation = snapshot.follow_up_validation || {};
  const horizon = Math.max(1,Number(validation.horizon_days || snapshot.holding_horizon_days || 1));
  const completed = Math.max(0,Math.min(horizon,Number(validation.completed_sessions || 0)));
  const status = validation.status || 'unavailable';
  const statusLabel = ({completed:'周期已到',in_progress:'验证中',pending:'等待 T+1',flat:'空仓期',
    unavailable:'行情待更新'})[status] || '行情待更新';
  const average = validation.average_return;
  const averageLabel = average == null || !Number.isFinite(+average)
    ? (status === 'flat' ? '无持仓' : '—') : decisionSignedPercent(average);
  const progressLabel = `${statusLabel} ${completed}/${horizon}`;
  return `<div class="snapshot-summary-validation" data-status="${esc(status)}">
    <span>${esc(statusLabel)}</span><progress max="${horizon}" value="${completed}" aria-label="${esc(progressLabel)}"></progress><small>${completed}/${horizon}</small><strong class="${cls(average)}">${esc(averageLabel)}</strong>
  </div>`;
}

function decisionFollowUpMarkup(snapshot) {
  const validation = snapshot.follow_up_validation || {};
  const picks = decisionPositionPicks(snapshot).slice(0,3);
  const outcomes = new Map((validation.picks || []).map(item => [item.symbol,item]));
  const horizon = Math.max(1,Number(validation.horizon_days || snapshot.holding_horizon_days || 1));
  const completed = Math.max(0,Math.min(horizon,Number(validation.completed_sessions || 0)));
  const status = validation.status || 'unavailable';
  const statusLabel = ({completed:'周期已到',in_progress:'验证中',pending:'等待 T+1',flat:'空仓期',
    unavailable:'行情待更新'})[status] || '行情待更新';
  if (status === 'flat') return '<div class="snapshot-validation" data-status="flat"><div class="snapshot-validation-head"><span>主动空仓</span><strong>无持仓验证</strong><span class="snapshot-validation-meta">现金期不计入标的收益</span></div></div>';
  const available = Number(validation.available_picks || 0);
  const average = validation.average_return;
  const cohort = picks.length === 3 && available === 3
    ? '前三目标持仓' : available ? `可比 ${available} 只` : '暂无可比收益';
  const summary = average == null || !Number.isFinite(+average)
    ? cohort : `${cohort} ${decisionSignedPercent(average)}`;
  const pickRows = picks.map((pick,index) => {
    const outcome = outcomes.get(pick.symbol) || {};
    const ready = outcome.status === 'ready' && Number.isFinite(+outcome.return);
    const resultLabel = ({pending:'待入场',missing_entry:'无 T+1 开盘价',
      missing_price:'收盘价暂缺',unavailable:'本地行情待更新'})[outcome.status] || '本地行情待更新';
    const detail = ready
      ? `<span class="snapshot-pick-result ${cls(outcome.return)}" title="${esc(`T+1 开盘 ${fixed(outcome.entry_price,2)}，${status === 'completed' ? '周期' : '最新'}收盘 ${fixed(outcome.price,2)} · ${outcome.price_date || ''}`)}"><strong>${decisionSignedPercent(outcome.return)}</strong><small>${fixed(outcome.entry_price,2)} → ${fixed(outcome.price,2)}</small></span>`
      : `<span class="snapshot-pick-result pending"><strong>—</strong><small>${esc(resultLabel)}</small></span>`;
    return `<div class="snapshot-result-row"><span class="snapshot-result-rank">#${Number(pick.rank || index + 1)}</span>${detail}</div>`;
  }).join('') || '<span class="snapshot-pick-symbol">该快照没有入选标的</span>';
  const progressLabel = `${statusLabel} ${completed}/${horizon}`;
  const fullPeriodLabel = validation.entry_date && validation.evaluation_date
    ? `T+1 ${validation.entry_date} 开盘 → ${status === 'completed' ? '周期' : '最新'} ${validation.evaluation_date} 收盘`
    : '等待本地日线出现 T+1 开盘与收盘';
  const periodLabel = validation.entry_date && validation.evaluation_date
    ? `${String(validation.entry_date).slice(5)} 开 → ${String(validation.evaluation_date).slice(5)} 收`
    : '等待 T+1 行情';
  return `<div class="snapshot-validation" data-status="${esc(status)}">
    <div class="snapshot-validation-head" title="${esc(fullPeriodLabel)}"><span>${esc(progressLabel)}</span><strong class="${cls(average)}">${esc(summary)}</strong><span class="snapshot-validation-meta">${esc(periodLabel)}</span></div>
    <progress class="snapshot-progress" max="${horizon}" value="${completed}" aria-label="${esc(progressLabel)}"></progress>
    <div class="snapshot-result-list">${pickRows}</div>
  </div>`;
}

function toggleDecisionSnapshotRow(row) {
  const detail = document.getElementById(row?.dataset.snapshotToggle || '');
  if (!row || !detail) return;
  const expanded = row.getAttribute('aria-expanded') === 'true';
  row.setAttribute('aria-expanded',String(!expanded));
  detail.hidden = expanded;
  row.querySelector('[data-snapshot-toggle-button]')?.setAttribute('aria-expanded',String(!expanded));
}

function decisionHistoryTableMarkup(snapshots, emptyText = '生成后会自动保存快照') {
  const rows = snapshots.map((snapshot,index) => {
    const detailId = `decision-snapshot-detail-${index}`;
    return `<tr class="snapshot-record-row" data-snapshot-toggle="${detailId}" aria-expanded="false" title="点击展开该日完整验证">
    <td class="snapshot-date"><button class="snapshot-row-toggle" type="button" data-snapshot-toggle-button aria-expanded="false" aria-controls="${detailId}">${esc(snapshot.signal_date)}</button></td>
    <td class="snapshot-period">${snapshot.holding_horizon_days} 日<div class="reason">${esc(decisionProfileLabel(snapshot.profile))}</div></td>
    <td class="snapshot-exposure">${pct(snapshot.recommended_exposure)}</td>
    <td class="snapshot-picks">${decisionSnapshotSummaryMarkup(snapshot)}</td>
    <td class="snapshot-verification">${decisionFollowUpSummaryMarkup(snapshot)}</td>
  </tr><tr class="snapshot-detail-row" id="${detailId}" hidden><td colspan="5"><div class="snapshot-detail-grid">
    <div class="snapshot-detail-section"><div class="snapshot-detail-label">前三目标持仓</div>${decisionSnapshotPicksMarkup(snapshot)}</div>
    <div class="snapshot-detail-section"><div class="snapshot-detail-label">股价变动验证</div>${decisionFollowUpMarkup(snapshot)}</div>
  </div></td></tr>`;
  }).join('') || `<tr><td colspan="5" class="msg">${esc(emptyText)}</td></tr>`;
  return `<div class="table-scroll snapshot-table-scroll"><table class="snapshot-table"><thead><tr><th class="snapshot-date">日期</th><th class="snapshot-period">周期</th><th class="snapshot-exposure">仓位</th><th class="snapshot-picks">前三目标持仓</th><th class="snapshot-verification">股价变动验证</th></tr></thead><tbody>${rows}</tbody></table></div>`;
}

function renderDecision(data, target = document.getElementById('decision-out')) {
  const out = target;
  const market = data.market, current = market.current;
  const selectionReady = Boolean(data.selection), selection = data.selection || {};
  const sectorsReady = Array.isArray(market.sectors), historyReady = Array.isArray(data.history);
  // A complete render into the canonical output owns the view.  Fence any
  // slower history/candidate bootstrap so it cannot replace this DOM later.
  if (selectionReady && historyReady && out === document.getElementById('decision-out')) {
    decisionLoaded = true;
    decisionViewRequest += 1;
  }
  const validation = Object.fromEntries((market.forecast_validation || []).map(v => [v.horizon_days, v]));
  const tone = current.trend_score > .2 ? 'up' : current.trend_score < -.2 ? 'down' : '';
  const picks = selection.picks || [];
  const snapshot = selection.model_snapshot || data.model_snapshot || data.policy || null;
  const profile = selection.profile || snapshot?.profile || 'risk_adjusted';
  const modelFallback = Boolean(snapshot?.fallback_active || (snapshot?.components || []).some(item => item.status === 'fallback'));
  const activeComponents = (snapshot?.components || []).filter(item => item.status !== 'fallback').map(item => decisionComponentLabel(item.role));
  const validationSummary = selection.validation_summary || {};
  out.innerHTML = `
    <div class="decision-summary reveal">
      <div class="regime-block">
        <div><div class="eyebrow">${esc(current.as_of)} · ${current.universe_size} 只标的</div>
          <div class="regime-name ${tone}">${esc(current.state_label)}</div>
          <div class="decision-strategy-line"><span class="state-pill">${esc(selection.profile_label || snapshot?.profile_label || decisionProfileLabel(profile))}</span><span class="state-pill model-health ${modelFallback ? 'fallback' : ''}">${modelFallback ? '模型已回退' : '模型运行正常'}</span><span class="state-pill opportunity-signal" data-opportunity-rsi="${fixed(current.rsi_14,2)}"></span></div></div>
        <div class="regime-score"><strong class="${tone}">${fixed(current.bull_score, 1)}</strong><span class="hint">牛熊分 / 100</span></div>
      </div>
      <div class="metric-strip">
        <div class="metric-cell"><div class="k">目标总仓位</div><div class="v">${pct(selection.recommended_exposure)}</div></div>
        <div class="metric-cell"><div class="k">现金比例</div><div class="v">${pct(selection.cash_weight)}</div></div>
        <div class="metric-cell"><div class="k">市场基础仓位</div><div class="v">${pct(selection.market_base_exposure)}</div></div>
        <div class="metric-cell"><div class="k">机会系数</div><div class="v">${pct(selection.opportunity_scale)}</div></div>
        <div class="metric-cell"><div class="k">合格标的</div><div class="v">${Number(selection.qualified_count || 0)}</div></div>
        <div class="metric-cell"><div class="k">有效组件</div><div class="v">${esc(activeComponents.join(' + ') || '规则')}</div></div>
        <div class="metric-cell"><div class="k">上涨宽度</div><div class="v">${pct(current.advance_ratio)}</div></div>
        <div class="metric-cell"><div class="k">站上 MA20</div><div class="v">${pct(current.above_ma20_ratio)}</div></div>
        <div class="metric-cell"><div class="k">校准 Brier</div><div class="v">${fixed(validationSummary.brier_score, 3)}</div></div>
        <div class="metric-cell rsi-primary ${rsiVisualClass(current.rsi_14)}"><div class="k">日线 RSI(14)</div><div class="v">${fixed(current.rsi_14, 1)}</div></div>
        <div class="metric-cell"><div class="k">CNN 恐贪</div><div class="v"><span data-fear-greed-score>—</span> <small data-fear-greed-label>读取中</small></div></div>
        <div class="metric-cell"><div class="k">决策周期</div><div class="v">${selectionReady ? `${selection.holding_horizon_days} 日` : '计算中'}</div></div>
      </div>
    </div>
    ${decisionModelEvidenceMarkup(snapshot, selection)}
    <div class="indicator-dashboard reveal reveal-delay">
      <section class="panel">
        <div class="panel-heading"><h3>CNN 恐贪指数</h3><span class="state-pill" data-fear-greed-label data-fear-greed-prefix="全球背景 · ">读取中</span></div>
        <div class="decision-fear-greed-visuals">
          <div class="fear-greed-gauge" id="fear-greed-gauge-decision" data-fear-greed-gauge role="img" aria-label="CNN 当日恐贪指数仪表盘"></div>
          <div class="fear-greed-history-wrap"><div class="indicator-chart-head"><strong>历史曲线</strong><span>黄色虚线：CNN ≤10，属于罕见恐惧区间；分数越低越恐惧</span></div>
            <div class="fear-greed-history" id="fear-greed-history-decision" data-fear-greed-history role="img" aria-label="CNN 恐贪指数历史曲线；黄色虚线表示 10 分罕见恐惧参考阈值"></div></div>
        </div>
        <div class="hint">美国市场风险情绪，仅作全球背景；不代表 A 股或具体板块。</div>
      </section>
      <section class="panel">
        <div class="panel-heading"><h3>大盘与板块 RSI(14)</h3><span class="state-pill">日线 · 22 抄底观察线</span></div>
        <div class="rsi-current-callout ${rsiVisualClass(current.rsi_14)}"><strong>${fixed(current.rsi_14,1)}</strong><span>候选等权大盘当前 RSI</span></div>
        <div class="decision-rsi-chart" id="decision-rsi-chart" role="img" aria-label="大盘与板块日线 RSI 历史曲线"></div>
      </section>
    </div>
    <div class="decision-grid reveal reveal-delay">
      <div class="panel"><div class="panel-heading regime-heading">
        <div class="regime-heading-title"><h3>牛熊分与市场宽度</h3><span class="state-pill" id="regime-window-count">3M</span></div>
        <div class="segmented regime-window" aria-label="牛熊观察窗口">
          <button type="button" data-regime-window="7d" aria-pressed="false">7D</button><button type="button" data-regime-window="14d" aria-pressed="false">14D</button>
          <button type="button" data-regime-window="1m" aria-pressed="false">1M</button><button type="button" data-regime-window="3m" class="active" aria-pressed="true">3M</button>
          <button type="button" data-regime-window="6m" aria-pressed="false">6M</button><button type="button" data-regime-window="1y" aria-pressed="false">1Y</button>
          <button type="button" data-regime-window="3y" aria-pressed="false">3Y</button><button type="button" data-regime-window="5y" aria-pressed="false">5Y</button>
          <button type="button" data-regime-window="10y" aria-pressed="false">10Y</button>
        </div></div><div class="chart" id="regime-chart"></div></div>
      <div class="panel"><div class="panel-heading"><h3>未来概率与历史验证</h3><span class="state-pill">非确定预测</span></div>
        <table><thead><tr><th>周期</th><th>方向</th><th>上涨概率</th><th>期望</th><th>准确率</th><th>Brier</th></tr></thead>
        <tbody>${(market.future || []).map(f => { const v = validation[f.horizon_days] || {}; return `<tr>
          <td>${f.horizon_days} 日</td><td class="${cls(f.probability_up - .5)}">${directionLabel(f.direction)}</td>
          <td>${pct(f.probability_up)}</td><td class="${cls(f.expected_return)}">${pct(f.expected_return)}</td>
          <td>${v.direction_accuracy == null ? '—' : pct(v.direction_accuracy)}</td><td>${fixed(v.brier_score, 3)}</td></tr>`; }).join('')}</tbody></table>
        <div class="hint">Brier 越低越好；准确率和误差只评价历史已揭晓样本。</div>
      </div>
    </div>
    <div class="panel reveal reveal-delay">
      <div class="panel-heading"><h3>今日目标持仓与观察候选</h3><span class="state-pill">${selectionReady ? `${esc(selection.signal_date)} · ${esc(decisionPositionStateLabel(selection.position_state))} · T+1 执行` : '正在计算'}</span></div>
      <div class="table-scroll decision-table-scroll"><table class="decision-table"><thead><tr><th>#</th><th>名称 / 代码 / 板块</th><th>综合分</th><th>结论</th><th>目标仓位</th><th>上涨概率</th><th>扣费后预期</th><th>置信 / 一致</th><th>止损 / 止盈</th><th>模型依据</th></tr></thead>
      <tbody>${picks.map(p => `<tr data-symbol="${esc(p.symbol)}" data-name="${esc(p.name || p.symbol)}" title="点击展开行情">
        <td>${p.rank}</td><td><button class="decision-symbol-trigger" type="button" data-decision-kline-trigger="${esc(p.symbol)}" aria-expanded="false" aria-controls="decision-kline-detail"><strong>${esc(p.name || '名称待同步')}</strong><span class="reason">${esc(p.symbol)} · ${esc(p.industry)} · ${fixed(p.last_close,2)}</span></button></td>
        <td>${fixed(p.score, 1)}<div class="score-track"><div class="score-fill" style="--score:${Math.max(0,Math.min(1,p.score/100))}"></div></div></td>
        <td><span class="state-pill ${esc(p.action)}">${actionLabel(p.action)}</span></td><td><strong>${pct(p.target_weight)}</strong><div class="reason">强度 ${fixed(p.allocation_strength,3)}</div></td><td>${pct(p.probability_up)}</td>
        <td class="${cls(p.expected_return_net ?? p.expected_return)}">${pct(p.expected_return_net ?? p.expected_return)}</td><td>${pct(p.confidence)}<div class="reason">一致 ${pct(p.model_agreement)}</div></td>
        <td><span class="down">-${pct(p.stop_loss)}</span> / <span class="up">+${pct(p.take_profit)}</span></td>
        <td>${decisionPickEvidenceMarkup(p)}</td></tr>`).join('') || `<tr><td colspan="10" class="msg">${selectionReady ? '当前条件下没有目标持仓或观察候选' : '市场状态已可查看，决策结果仍在计算…'}</td></tr>`}</tbody></table></div>
      <div class="hint">${esc(decisionPositionReasonText(selection.position_reasons) || '')}${selection.position_reasons?.length ? ' · ' : ''}${esc(selection.risk_note || '决策结果生成后将在此显示依据与风控位。')}</div>
    </div>
    <div class="decision-bottom-stack reveal reveal-delay">
      <div class="panel"><div class="panel-heading"><h3>板块强弱</h3><span class="state-pill" data-fear-greed-label data-fear-greed-prefix="CNN 恐贪 · ">CNN 恐贪读取中</span></div>
        <div class="table-scroll"><table><thead><tr><th>板块</th><th>成分</th><th>状态</th><th>牛熊分</th><th>RSI14</th><th>机会提示</th><th>上涨宽度</th></tr></thead><tbody>
        ${(market.sectors || []).map(s => `<tr><td>${esc(s.sector)}</td><td>${s.members}</td><td class="${cls(s.trend_score)}">${esc(s.state_label)}</td><td>${fixed(s.bull_score,1)}</td><td><span class="rsi-badge ${rsiVisualClass(s.rsi_14)}">${fixed(s.rsi_14,1)}</span></td><td><span class="state-pill opportunity-signal" data-opportunity-rsi="${fixed(s.rsi_14,2)}"></span></td><td>${pct(s.advance_ratio)}</td></tr>`).join('') || `<tr><td colspan="7" class="msg">${sectorsReady ? '暂无行业映射' : '板块状态聚合中…'}</td></tr>`}
        </tbody></table></div></div>
      <div class="panel decision-history-panel"><div class="panel-heading"><h3>历史决策快照</h3><span class="state-pill">T+1 后续验证 · 本地行情</span></div>
        ${decisionHistoryTableMarkup(data.history || [], historyReady ? '生成后会自动保存快照' : '正在读取本地快照…')}</div>
    </div>`;

  regimeHistory = market.past || [];
  refreshSentimentBindings(out);
  renderRegimeChart(activeRegimeWindow);
  renderDecisionRsiChart(market,market.sectors || []);
  mountDecisionKline();
}

function renderDecisionHistory(snapshots, target = document.getElementById('decision-out')) {
  disposeChart('regime-chart');
  closeDecisionKline();
  if (!snapshots.length) {
    target.innerHTML = `<div class="panel ready-preview"><div class="ready-preview-head"><strong>尚无匹配的历史决策</strong><span class="state-pill">未计算</span></div>
      <div class="hint">当前候选、策略画像与持有周期没有已保存快照。确认参数后点击“生成今日决策”。</div></div>`;
    return;
  }
  const latest = snapshots[0], picks = latest.picks || [], heldPicks = decisionPositionPicks(latest);
  const snapshot = latest.model_snapshot || null;
  const validation = latest.validation_summary || {};
  const regime = latest.market_regime || {};
  target.innerHTML = `
    <div class="panel ready-preview reveal">
      <div class="ready-preview-head"><strong>上一次已保存的决策</strong><span class="state-pill">只读 · 未重新计算</span></div>
      <div class="decision-summary">
        <div class="regime-block"><div><div class="eyebrow">${esc(latest.signal_date)} · 历史快照</div><div class="regime-name">${esc(regime.state_label || latest.profile_label || decisionProfileLabel(latest.profile))}</div></div>
          <div class="regime-score"><strong>${heldPicks.length}</strong><span class="hint">目标持仓</span></div></div>
        <div class="metric-strip">
          <div class="metric-cell"><div class="k">建议仓位</div><div class="v">${pct(latest.recommended_exposure)}</div></div>
          <div class="metric-cell"><div class="k">决策周期</div><div class="v">${latest.holding_horizon_days} 日</div></div>
          <div class="metric-cell"><div class="k">策略画像</div><div class="v">${esc(latest.profile_label || decisionProfileLabel(latest.profile))}</div></div>
          <div class="metric-cell"><div class="k">校准 Brier</div><div class="v">${fixed(validation.brier_score, 3)}</div></div>
        </div>
      </div>
      <div class="hint">这是本地已保存结果。只有点击“生成今日决策”才会读取候选行情、重新计算并写入新快照。</div>
    </div>
    ${decisionModelEvidenceMarkup(snapshot, latest)}
    <div class="panel reveal reveal-delay">
      <div class="panel-heading"><h3>${esc(latest.signal_date)} 目标持仓与观察候选</h3><span class="state-pill">${esc(decisionPositionStateLabel(latest.position_state))} · T+1 执行口径</span></div>
      <div class="table-scroll decision-table-scroll"><table class="decision-table"><thead><tr><th>#</th><th>名称 / 代码 / 板块</th><th>综合分</th><th>结论</th><th>目标仓位</th><th>上涨概率</th><th>扣费后预期</th><th>置信 / 一致</th><th>止损 / 止盈</th><th>模型依据</th></tr></thead><tbody>
        ${picks.map(p => `<tr data-symbol="${esc(p.symbol)}" data-name="${esc(p.name || p.symbol)}" title="点击展开行情">
          <td>${p.rank}</td><td><button class="decision-symbol-trigger" type="button" data-decision-kline-trigger="${esc(p.symbol)}" aria-expanded="false" aria-controls="decision-kline-detail"><strong>${esc(p.name || '名称待同步')}</strong><span class="reason">${esc(p.symbol)} · ${esc(p.industry)} · ${fixed(p.last_close,2)}</span></button></td>
          <td>${fixed(p.score, 1)}</td><td><span class="state-pill ${esc(p.action)}">${actionLabel(p.action)}</span></td><td><strong>${pct(p.target_weight)}</strong><div class="reason">强度 ${fixed(p.allocation_strength,3)}</div></td><td>${pct(p.probability_up)}</td>
          <td class="${cls(p.expected_return_net ?? p.expected_return)}">${pct(p.expected_return_net ?? p.expected_return)}</td><td>${pct(p.confidence)}<div class="reason">一致 ${pct(p.model_agreement)}</div></td>
          <td><span class="down">-${pct(p.stop_loss)}</span> / <span class="up">+${pct(p.take_profit)}</span></td><td>${decisionPickEvidenceMarkup(p)}</td></tr>`).join('') || '<tr><td colspan="10" class="msg">该历史快照没有目标持仓或观察候选</td></tr>'}
      </tbody></table></div><div class="hint">${esc(latest.risk_note || '历史决策风险说明未记录。')}</div>
    </div>
    <div class="panel reveal reveal-delay decision-history-panel"><div class="panel-heading"><h3>历史决策快照</h3><span class="state-pill">T+1 后续验证 · 相同候选与参数</span></div>
      ${decisionHistoryTableMarkup(snapshots)}</div>`;
  mountDecisionKline();
}

async function loadDecisionHistory({force = false} = {}) {
  if (decisionLoading || decisionHistoryLoading) return;
  const form = document.getElementById('decision-form');
  const fd = new FormData(form);
  const params = new URLSearchParams({
    universe:String(fd.get('universe') || ''), profile:String(fd.get('profile') || ''),
    horizon:String(fd.get('horizon') || '3'), limit:'10',
  });
  const key = params.toString();
  if (!force && key === decisionHistoryKey) return;
  decisionHistoryLoading = true;
  const request = ++decisionViewRequest;
  const out = document.getElementById('decision-out');
  out.innerHTML = '<div class="trading-skeleton" aria-label="正在读取历史决策"></div>';
  try {
    const data = await api(`/api/v1/research/selection/history?${key}`, {cache:'no-store'});
    if (request !== decisionViewRequest) return;
    decisionHistoryKey = key;
    renderDecisionHistory(data.snapshots || [], out);
  } catch (error) {
    if (request === decisionViewRequest) out.innerHTML = `<div class="err">历史决策读取失败：${esc(error.message)}</div>`;
  } finally {
    decisionHistoryLoading = false;
    if (request !== decisionViewRequest && !decisionLoaded && !decisionLoading
        && document.getElementById('tab-decision').classList.contains('active')) {
      queueMicrotask(() => loadDecisionHistory({force:true}));
    }
  }
}

function createDecisionStreamRenderer(root) {
  const state = {market:null, sectors:null, policy:null, selection:null, history:null};
  let completed = 0, total = 0, successful = 0, preview = null, symbols = null, count = null;
  const ensurePreview = () => {
    if (preview) return;
    root.innerHTML = `<div class="panel ready-preview stream-enter">
      <div class="ready-preview-head"><strong>已就绪标的</strong><span class="state-pill" data-ready-count>0 / 0</span></div>
      <div class="ready-symbols" data-ready-symbols></div>
      <div class="hint">已完成的标的可先打开 K 线；跨标的牛熊与选股需等待候选数据同步完成。</div></div>`;
    preview = root.querySelector('.ready-preview');
    symbols = root.querySelector('[data-ready-symbols]');
    count = root.querySelector('[data-ready-count]');
  };
  const render = () => {
    if (!state.market) return;
    if (state.sectors) state.market.sectors = state.sectors;
    renderDecision({market:state.market, policy:state.policy, model_snapshot:state.policy, selection:state.selection, history:state.history}, root);
  };
  return {
    partial(partial) {
      if (!partial) return;
      if (partial.kind === 'decision_symbol') {
        completed = partial.completed; total = partial.total;
        if (partial.success) successful += 1;
        ensurePreview();
        count.textContent = `${completed} / ${total} · ${successful} 成功`;
        if (partial.success && symbols.children.length < 24) {
          const button = document.createElement('button');
          button.type = 'button'; button.className = 'ready-symbol stream-enter';
          button.dataset.previewSymbol = partial.symbol; button.textContent = partial.symbol;
          symbols.appendChild(button);
        }
        return;
      }
      if (partial.kind === 'decision_market') state.market = partial.market;
      if (partial.kind === 'decision_sectors') state.sectors = partial.sectors;
      if (partial.kind === 'decision_policy') state.policy = partial.policy;
      if (partial.kind === 'decision_selection') state.selection = partial.selection;
      if (partial.kind === 'decision_history') state.history = partial.history;
      render();
    },
    finish(data) {
      state.market = data.market; state.selection = data.selection; state.history = data.history;
      state.sectors = data.market?.sectors || [];
      renderDecision(data, root);
    },
  };
}

async function loadDecision(form) {
  if (decisionLoading) return;
  decisionLoading = true;
  decisionViewRequest += 1;
  closeDecisionKline();
  const out = document.getElementById('decision-out');
  busy(form, true, '生成中…');
  const tracker = createLoadProgress(out, '准备每日决策', 'decision');
  const renderer = createDecisionStreamRenderer(tracker.results);
  try {
    const fd = new FormData(form);
    const body = {
      universe:fd.get('universe'), start:fd.get('start'), horizon:+fd.get('horizon'),
      profile:fd.get('profile') || 'risk_adjusted', top_n:+fd.get('top_n'), sector_top:10, history:2600, save:true,
    };
    const data = await streamJson('/api/v1/research/decision/dashboard/stream', {
      method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify(body),
    }, event => {
      tracker.update(event);
      if (event.partial) {
        tracker.reveal();
        renderer.partial(event.partial);
      }
    });
    renderer.finish(data);
    tracker.reveal();
    tracker.finish(`决策已完成 · ${decisionProfileLabel(data.selection?.profile)} · ${data.selection?.picks?.length || 0} 只入选`);
    decisionLoaded = true;
  } catch (err) {
    tracker.fail(err.message);
    if (!tracker.hasResults) {
      tracker.reveal().innerHTML = `<div class="err">${esc(err.message)}</div>`;
    } else {
      tracker.results.insertAdjacentHTML('beforeend',
        `<div class="err">后续阶段加载失败：${esc(err.message)}；上方已完成结果仍可使用。</div>`);
    }
  }
  decisionLoading = false;
  busy(form, false);
}
const decisionForm = document.getElementById('decision-form');
const storedDecisionProfile = localStorage.getItem(DECISION_PROFILE_STORAGE_KEY);
if (storedDecisionProfile && DECISION_PROFILES[storedDecisionProfile]) {
  const storedProfileInput = decisionForm.querySelector(`input[name="profile"][value="${storedDecisionProfile}"]`);
  if (storedProfileInput) storedProfileInput.checked = true;
}
decisionForm.addEventListener('change', event => {
  if (event.target.name === 'profile') localStorage.setItem(DECISION_PROFILE_STORAGE_KEY, event.target.value);
  if (['universe', 'profile', 'horizon'].includes(event.target.name)) {
    decisionViewRequest += 1;
    decisionLoaded = false;
    decisionHistoryKey = '';
    if (document.getElementById('tab-decision').classList.contains('active')) {
      void loadDecisionHistory({force:true});
    }
  }
});
document.addEventListener('quantmaster:candidates-updated', () => {
  if (!decisionLoaded && document.getElementById('tab-decision').classList.contains('active')) {
    decisionViewRequest += 1;
    decisionHistoryKey = '';
    void loadDecisionHistory({force:true});
  }
});
decisionForm.onsubmit = e => { e.preventDefault(); loadDecision(e.target); };
document.getElementById('decision-out').addEventListener('click', e => {
  const snapshotRow = e.target.closest('[data-snapshot-toggle]');
  if (snapshotRow) { toggleDecisionSnapshotRow(snapshotRow); return; }
  const preview = e.target.closest('[data-preview-symbol]');
  if (preview) {
    document.querySelector('nav button[data-tab="market"]').click();
    showKline(preview.dataset.previewSymbol, preview.dataset.previewSymbol, '1d');
    return;
  }
  const windowButton = e.target.closest('[data-regime-window]');
  if (windowButton) { renderRegimeChart(windowButton.dataset.regimeWindow); return; }
  const assetToggle = e.target.closest('[data-decision-asset-toggle]');
  if (assetToggle) { void toggleDecisionAsset(assetToggle.dataset.decisionAssetToggle); return; }
  if (e.target.closest('[data-decision-assets-retry]')) {
    decisionKlineState.assetError = '';
    void loadAssetLists(false);
    return;
  }
  if (e.target.closest('[data-decision-kline-retry]')) {
    void loadDecisionKline();
    return;
  }
  if (e.target.closest('[data-decision-kline-close]')) { closeDecisionKline(); return; }
  const frequencyButton = e.target.closest('[data-decision-frequency]');
  if (frequencyButton) {
    const frequency = frequencyButton.dataset.decisionFrequency;
    if (frequency !== decisionKlineState.frequency) {
      decisionKlineState.frequency = frequency;
      void loadDecisionKline();
    }
    return;
  }
  const trigger = e.target.closest('[data-decision-kline-trigger]');
  if (trigger) { openDecisionKline(trigger.closest('tr[data-symbol]')); return; }
  const row = e.target.closest('tr[data-symbol]');
  if (!row) return;
  if (!e.target.closest('button,a,input,select,textarea,details,summary')) openDecisionKline(row);
});

/* ---------- 因子 ---------- */
async function loadFactorList() {
  try {
    const data = await api('/api/v1/research/factors');
    document.getElementById('factor-list').innerHTML =
      data.factors.map(f => `<option value="${esc(f.name)}">${esc(f.description)}</option>`).join('');
    window.QuantMasterFactorCatalog = data.factors;
    document.dispatchEvent(new CustomEvent('quantmaster:factor-catalog', {detail:data.factors}));
  } catch (e) { /* 非致命 */ }
}
loadFactorList();
document.addEventListener('quantmaster:factors-changed', loadFactorList);

document.getElementById('factor-form').onsubmit = async e => {
  e.preventDefault(); const form = e.target; busy(form, true);
  const out = document.getElementById('factor-out');
  out.innerHTML = '<div class="msg">计算中…（首次需拉取行情）</div>';
  try {
    const fd = new FormData(form);
    const data = await post('/api/v1/research/factors/test', {
      expression: fd.get('expression'), universe: fd.get('universe'),
      start: fd.get('start'), quantiles: +fd.get('quantiles'),
      neutralize: form.querySelector('[name=neutralize]').checked });
    const s = data.summary;
    const neutralNote = form.querySelector('[name=neutralize]').checked && !data.neutralized
      ? '<div class="hint" style="color:var(--s4)">⚠️ 行业映射为空（首次需联网抓取，约1-2分钟），本次未做中性化</div>' : '';
    out.innerHTML = `
      ${neutralNote}
      <div class="cards">
        <div class="card"><div class="k">RankIC 均值${data.neutralized ? '（行业中性）' : ''}</div><div class="v ${cls(s.ic_mean)}">${s.ic_mean}</div></div>
        <div class="card"><div class="k">ICIR</div><div class="v">${s.icir}</div></div>
        <div class="card"><div class="k">IC&gt;0 占比</div><div class="v">${pct(s.ic_positive_ratio)}</div></div>
        <div class="card"><div class="k">多空年化</div><div class="v ${cls(s.long_short_annual)}">${pct(s.long_short_annual)}</div></div>
        <div class="card"><div class="k">单调性</div><div class="v">${s.monotonicity}</div></div>
        <div class="card"><div class="k">Top组换手/日</div><div class="v">${pct(s.top_quantile_turnover)}</div></div>
      </div>
      <div class="row">
        <div class="panel"><h3>RankIC（20日滚动均值）</h3><div class="chart-sm" id="ic-chart"></div></div>
        <div class="panel"><h3>分层净值（Q1 最低分 → Q${Object.keys(data.quantile_nav).length} 最高分）</h3><div class="chart-sm" id="q-chart"></div></div>
      </div>
      <div class="hint">经验参考：|IC均值| &gt; 0.03 值得关注；单调性接近 ±1 说明分层有序；换手过高会被交易成本侵蚀。</div>`;
    mkChart('ic-chart').setOption(baseOpt({
      xAxis: timeAxis(), yAxis: valAxis(),
      series: [{ name: 'RankIC(20d)', type: 'line', data: data.ic_series, showSymbol: false, lineStyle: { width: 2 } },
               { name: '0', type: 'line', data: data.ic_series.map(p => [p[0], 0]), showSymbol: false, lineStyle: { width: 1, color: AXIS }, tooltip: { show: false }, silent: true }],
    }));
    const qNames = Object.keys(data.quantile_nav);
    mkChart('q-chart').setOption(baseOpt({
      legend: { textStyle: { color: INK2 }, top: 0 },
      xAxis: timeAxis(), yAxis: valAxis(),
      series: qNames.map(q => ({ name: q, type: 'line', data: data.quantile_nav[q], showSymbol: false, lineStyle: { width: 2 } })),
    }));
  } catch (err) { out.innerHTML = `<div class="err">${esc(err.message)}</div>`; }
  busy(form, false);
};

document.getElementById('validate-form').onsubmit = async e => {
  e.preventDefault(); const form = e.target; busy(form, true);
  const out = document.getElementById('validate-out');
  out.innerHTML = '<div class="msg">验证中…</div>';
  try {
    const ff = new FormData(document.getElementById('factor-form'));
    const fd = new FormData(form);
    const r = await post('/api/v1/research/factors/validate', {
      expression: ff.get('expression'), universe: ff.get('universe'), start: ff.get('start'),
      split: fd.get('split'), n_splits: +fd.get('n_splits') });
    const verdictColor = r.verdict === '稳健' ? 'up' : (r.verdict === '衰减' ? '' : 'down');
    out.innerHTML = `
      <div class="cards">
        <div class="card"><div class="k">训练期 RankIC（${r.is_days}天）</div><div class="v">${r.is_ic}</div></div>
        <div class="card"><div class="k">验证期 RankIC（${r.oos_days}天）</div><div class="v">${r.oos_ic}</div></div>
        <div class="card"><div class="k">训练期 ICIR</div><div class="v">${r.is_icir}</div></div>
        <div class="card"><div class="k">验证期 ICIR</div><div class="v">${r.oos_icir}</div></div>
        <div class="card"><div class="k">衰减度</div><div class="v">${r.degradation == null ? '—' : pct(r.degradation)}</div></div>
        <div class="card"><div class="k">结论</div><div class="v ${verdictColor}">${esc(r.verdict)}</div></div>
      </div>
      <table><thead><tr><th>分段</th><th>起止</th><th>天数</th><th>RankIC</th><th>ICIR</th></tr></thead>
      <tbody>${(r.segments || []).map((s, i) => `<tr><td>${i + 1}</td><td>${esc(s.start)} ~ ${esc(s.end)}</td>
        <td>${s.days}</td><td class="${cls(s.ic_mean)}">${s.ic_mean == null ? '—' : (+s.ic_mean).toFixed(4)}</td>
        <td>${s.icir == null ? '—' : (+s.icir).toFixed(3)}</td></tr>`).join('')}</tbody></table>`;
  } catch (err) { out.innerHTML = `<div class="err">${esc(err.message)}</div>`; }
  busy(form, false);
};

/* ---------- 挖掘 ---------- */
function renderMined(list, extraCols) {
  const rows = list.map(f => `<tr><td><code class="mined-expr" style="cursor:pointer" title="点击填入因子页验证">${esc(f.expression)}</code></td>
    <td class="${cls(f.ic_mean)}">${(+f.ic_mean).toFixed(4)}</td><td>${(+f.icir).toFixed(3)}</td>${extraCols(f)}</tr>`).join('');
  return `<div class="panel"><h3>挖掘结果（点击表达式 → 跳转因子页体检）</h3>
    <table><thead><tr><th>表达式</th><th>RankIC</th><th>ICIR</th><th></th></tr></thead><tbody>${rows}</tbody></table></div>`;
}
document.getElementById('mine-out').addEventListener('click', e => {
  const code = e.target.closest('code.mined-expr');
  if (!code) return;
  document.querySelector('#factor-form [name=expression]').value = code.textContent;
  document.querySelector('nav button[data-tab="lab"]').click();
  if (window.quantLabOpenExpression) window.quantLabOpenExpression(code.textContent);
});
document.getElementById('gp-form').onsubmit = async e => {
  e.preventDefault(); const form = e.target; busy(form, true);
  const out = document.getElementById('mine-out');
  out.innerHTML = '<div class="msg">遗传规划进化中，视参数可能需要几分钟…</div>';
  try {
    const fd = new FormData(form);
    const data = await post('/api/v1/research/mining/genetic', {
      universe: fd.get('universe'), start: fd.get('start'),
      generations: +fd.get('generations'), population: +fd.get('population') });
    out.innerHTML = renderMined(data.factors, f => `<td>fitness=${(+f.fitness).toFixed(4)}</td>`);
  } catch (err) { out.innerHTML = `<div class="err">${esc(err.message)}</div>`; }
  busy(form, false);
};
document.getElementById('llm-form').onsubmit = async e => {
  e.preventDefault(); const form = e.target; busy(form, true);
  const out = document.getElementById('mine-out');
  out.innerHTML = '<div class="msg">LLM 生成并验证中…</div>';
  try {
    const fd = new FormData(form);
    const data = await post('/api/v1/research/mining/llm', {
      universe: fd.get('universe'), start: fd.get('start'), n: +fd.get('n'), rounds: +fd.get('rounds') });
    out.innerHTML = renderMined(data.factors, f =>
      `<td>${f.valid ? '✅ 达标' : '—'} ${esc(f.rationale || '')}</td>`);
  } catch (err) { out.innerHTML = `<div class="err">${esc(err.message)}</div>`; }
  busy(form, false);
};

/* ---------- 资讯 ---------- */
async function loadNews() {
  const out = document.getElementById('news-out');
  try {
    const data = await api('/api/v1/news?limit=80');
    if (!data.items.length) { out.innerHTML = '<div class="msg">尚无数据，点击上方抓取。</div>'; return; }
    out.innerHTML = `<table><thead><tr><th>时间</th><th>摘要/内容</th><th>类型</th><th>情绪</th><th>相关标的</th></tr></thead>
      <tbody>${data.items.map(n => `<tr>
        <td style="white-space:nowrap">${esc((n.published_at || '').slice(0, 16))}</td>
        <td>${esc(n.summary || n.content.slice(0, 80))}</td>
        <td>${n.event_type ? `<span class="badge">${esc(n.event_type)}</span>` : ''}</td>
        <td class="${cls(n.sentiment)}">${n.sentiment ? (+n.sentiment).toFixed(2) : ''}</td>
        <td>${(n.symbols || []).map(s => `<span class="badge">${esc(s)}</span>`).join(' ')}</td></tr>`).join('')}
      </tbody></table>`;
  } catch (e) { out.innerHTML = `<div class="err">${esc(e.message)}</div>`; }
}
document.getElementById('news-form').onsubmit = async e => {
  e.preventDefault(); const form = e.target; busy(form, true);
  try {
    const skip = form.querySelector('[name=skip_llm]').checked;
    await post('/api/v1/news/crawl?skip_llm=' + skip, {});
    await loadNews();
  } catch (err) { document.getElementById('news-out').innerHTML = `<div class="err">${esc(err.message)}</div>`; }
  busy(form, false);
};

/* ---------- 实盘 ---------- */
async function loadLedgerNav() {
  try {
    const r = await api('/api/v1/portfolio/ledger/nav');
    if (!r.dates || !r.dates.length) return;
    document.getElementById('ledger-nav-panel').style.display = '';
    const twr = r.dates.map((d, i) => [d, r.twr[i]]);
    const series = [{ name: '我的组合(TWR)', type: 'line', data: twr, showSymbol: false, lineStyle: { width: 2 } }];
    if (r.benchmark && r.benchmark.length) {
      series.push({ name: '基准', type: 'line', showSymbol: false, lineStyle: { width: 2 },
        data: r.dates.map((d, i) => [d, r.benchmark[i]]) });
    }
    mkChart('ledger-nav-chart').setOption(baseOpt({
      legend: { textStyle: { color: INK2 }, top: 0 },
      xAxis: timeAxis(), yAxis: valAxis(),
      series,
    }));
  } catch (e) { /* 无行情缓存时静默跳过 */ }
}

async function loadLedger() {
  loadLedgerNav();
  const out = document.getElementById('ledger-out');
  try {
    const r = await api('/api/v1/portfolio/ledger/report');
    out.innerHTML = `
      <div class="cards">
        <div class="card"><div class="k">总资产</div><div class="v">${r.total_assets.toLocaleString()}</div></div>
        <div class="card"><div class="k">总盈亏</div><div class="v ${cls(r.total_pnl)}">${r.total_pnl.toLocaleString()}</div></div>
        <div class="card"><div class="k">累计收益率</div><div class="v ${cls(r.total_return)}">${pct(r.total_return)}</div></div>
        <div class="card"><div class="k">XIRR 年化</div><div class="v ${cls(r.xirr)}">${pct(r.xirr)}</div></div>
        <div class="card"><div class="k">已实现盈亏</div><div class="v ${cls(r.realized_pnl)}">${r.realized_pnl.toLocaleString()}</div></div>
        <div class="card"><div class="k">浮动盈亏</div><div class="v ${cls(r.unrealized_pnl)}">${r.unrealized_pnl.toLocaleString()}</div></div>
        <div class="card"><div class="k">现金</div><div class="v">${r.cash.toLocaleString()}</div></div>
        <div class="card"><div class="k">累计费用</div><div class="v">${r.fees.toLocaleString()}</div></div>
      </div>
      ${(r.warnings || []).map(w => `<div class="hint" style="color:var(--s4)">⚠️ ${esc(w)}</div>`).join('')}
      ${r.missing_price.length ? `<div class="hint">⚠️ 以下持仓未获取到行情，按成本价估值：${r.missing_price.map(esc).join('、')}</div>` : ''}
      <div class="panel"><h3>当前持仓</h3>
        <table><thead><tr><th>代码</th><th>数量</th><th>成本</th><th>现价</th><th>市值</th><th>浮盈</th><th>已实现</th></tr></thead>
        <tbody>${r.positions.filter(p => p.shares > 0).map(p => `<tr>
          <td>${esc(p.symbol)}</td><td>${p.shares}</td><td>${p.avg_cost}</td><td>${p.price}</td>
          <td>${p.market_value.toLocaleString()}</td>
          <td class="${cls(p.unrealized_pnl)}">${p.unrealized_pnl.toLocaleString()}</td>
          <td class="${cls(p.realized_pnl)}">${p.realized_pnl.toLocaleString()}</td></tr>`).join('')
          || '<tr><td colspan="7" class="msg">暂无持仓</td></tr>'}
        </tbody></table></div>`;
  } catch (e) { out.innerHTML = `<div class="err">${esc(e.message)}\n提示：先入金（右上表单）再录成交。</div>`; }
}
document.getElementById('trade-form').onsubmit = async e => {
  e.preventDefault(); const form = e.target; busy(form, true);
  try {
    const fd = new FormData(form);
    await post('/api/v1/portfolio/ledger/trade', { date: fd.get('date'), symbol: fd.get('symbol'),
      side: fd.get('side'), price: +fd.get('price'), shares: +fd.get('shares'), fee: +fd.get('fee') });
    await loadLedger();
    await loadAssetLists(false);
  } catch (err) { reportLocalError('实盘账本', '成交记录未能保存', err); }
  busy(form, false);
};
document.getElementById('cash-form').onsubmit = async e => {
  e.preventDefault(); const form = e.target; busy(form, true);
  try {
    const fd = new FormData(form);
    await post('/api/v1/portfolio/ledger/cashflow', { date: fd.get('date'), amount: +fd.get('amount'), kind: fd.get('kind') });
    await loadLedger();
  } catch (err) { reportLocalError('实盘账本', '资金流水未能保存', err); }
  busy(form, false);
};
