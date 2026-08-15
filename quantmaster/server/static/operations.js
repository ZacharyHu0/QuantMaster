let context;
let refreshTimer = 0;
let pollDeadline = 0;
let mounted = false;
let bound = false;
let snapshot = null;

const TERMINAL_OPERATIONS = new Set(['activated', 'already_active', 'rolled_back', 'blocked']);

function text(value) {
  return String(value ?? '').trim();
}

function identityValue(identity) {
  return text(identity?.build_sha) || '—';
}

function statusLabel(value) {
  return ({
    empty: '尚未初始化', stable: '稳定运行', pending: '切换中',
    rolled_back: '已回滚', blocked: '已阻断', accepted: '已提交', running: '执行中',
    activated: '已激活', already_active: '当前已是该版本',
  })[text(value)] || text(value) || '未知';
}

function clearRefreshTimer() {
  if (refreshTimer) window.clearTimeout(refreshTimer);
  refreshTimer = 0;
}

function schedulePoll() {
  clearRefreshTimer();
  if (!mounted || !pollDeadline || Date.now() >= pollDeadline) return;
  refreshTimer = window.setTimeout(() => { void load(true); }, 750);
}

function operationState(data) {
  return text(data?.operation?.status || data?.status);
}

function renderIdentity(id, value) {
  id.textContent = identityValue(value);
  id.title = identityValue(value);
}

function renderBlockers(data) {
  const panel = document.getElementById('operations-blockers-panel');
  const list = document.getElementById('operations-blockers-list');
  const blockers = Array.isArray(data?.blockers) ? data.blockers : [];
  list.replaceChildren(...blockers.map(blocker => {
    const item = document.createElement('li');
    const code = document.createElement('code');
    code.textContent = text(blocker?.code) || 'blocked';
    item.append(code, document.createTextNode(` ${text(blocker?.message) || '候选槽不可激活'}`));
    return item;
  }));
  panel.hidden = blockers.length === 0;
}

function renderCandidates(data) {
  const list = document.getElementById('operations-staged-list');
  const candidates = Array.isArray(data?.staged) ? data.staged : [];
  if (!candidates.length) {
    list.innerHTML = '<div class="msg">没有可检查的 staged 槽。</div>';
    return;
  }
  list.replaceChildren(...candidates.map(candidate => {
    const item = document.createElement('article');
    item.className = 'operations-candidate';
    const heading = document.createElement('div');
    heading.className = 'operations-candidate-heading';
    const sha = document.createElement('code');
    sha.textContent = text(candidate?.build_sha) || '未知 SHA';
    heading.appendChild(sha);
    const state = document.createElement('span');
    state.textContent = candidate?.current ? '当前 active'
      : candidate?.eligible ? '可激活' : '不可激活';
    heading.appendChild(state);
    const detail = document.createElement('p');
    const reasons = Array.isArray(candidate?.blockers) ? candidate.blockers : [];
    detail.textContent = reasons.length
      ? reasons.map(reason => `${text(reason?.code) || 'blocked'}：${text(reason?.message) || '证据不足'}`).join('；')
      : candidate?.current ? '当前稳定槽，无需重复激活。' : '完整 local-main package/smoke 证据已通过。';
    const button = document.createElement('button');
    button.className = 'primary operations-activate';
    button.type = 'button';
    button.dataset.operationActivate = text(candidate?.build_sha);
    button.textContent = candidate?.current ? '当前版本' : '激活此槽';
    button.disabled = !candidate?.eligible || !text(candidate?.build_sha);
    item.append(heading, detail, button);
    return item;
  }));
}

function render(data) {
  snapshot = data || {};
  renderIdentity(document.getElementById('operations-active-build'), data?.active);
  renderIdentity(document.getElementById('operations-previous-build'), data?.previous);
  renderIdentity(document.getElementById('operations-pending-build'), data?.pending);
  renderCandidates(data);
  renderBlockers(data);
  const eligibility = data?.eligibility || {};
  document.getElementById('operations-eligibility').textContent =
    `${statusLabel(eligibility.status)}${eligibility.eligible_count ? ` · ${eligibility.eligible_count} 个可激活` : ''}`;
  const operation = data?.operation;
  const result = operation?.result;
  document.getElementById('operations-result').textContent = result
    ? `${statusLabel(result.status)} · active ${identityValue(result.active ? {build_sha:result.active} : data?.active)}${result.last_error ? ` · ${text(result.last_error)}` : ''}`
    : '尚无切换记录。';
  const progress = document.getElementById('operations-progress');
  const currentStatus = operationState(data);
  progress.textContent = operation && !TERMINAL_OPERATIONS.has(currentStatus)
    ? `正在执行稳定切换：${statusLabel(currentStatus)}。服务重启期间页面会自动重连。`
    : data?.status === 'blocked' ? '稳定更新被阻断，请先处理下方证据或指针问题。'
      : `当前状态：${statusLabel(data?.status)}`;
}

async function load(isPoll = false) {
  if (!mounted) return;
  try {
    const data = await window.QuantMasterAPI('/api/v1/system/update', {cache:'no-store'});
    if (!mounted) return;
    render(data);
    const status = operationState(data);
    if (isPoll && !TERMINAL_OPERATIONS.has(status)) schedulePoll();
    else if (status === 'accepted' || status === 'running') {
      pollDeadline ||= Date.now() + 20_000;
      schedulePoll();
    } else {
      pollDeadline = 0;
      clearRefreshTimer();
    }
  } catch (error) {
    if (!mounted) return;
    document.getElementById('operations-progress').textContent = isPoll
      ? '服务正在重启，等待重新连接…'
      : text(error?.message) || '稳定槽状态暂不可用。';
    if (isPoll) schedulePoll();
  }
}

async function activate(buildSha) {
  const candidate = snapshot?.staged?.find(item => item?.build_sha === buildSha && item?.eligible);
  if (!candidate) return;
  const buttons = [...document.querySelectorAll('[data-operation-activate]')];
  buttons.forEach(button => { button.disabled = true; });
  document.getElementById('operations-progress').textContent = '已提交激活请求，正在切换并检查 B 的身份…';
  pollDeadline = Date.now() + 20_000;
  try {
    await window.QuantMasterAPI('/api/v1/system/update/activate', {
      method:'POST', headers:{'Content-Type':'application/json'},
      body:JSON.stringify({build_sha:buildSha}),
    });
  } catch (error) {
    // The old Web process may disappear before the response reaches the
    // browser. The durable operation file remains the source of truth.
    document.getElementById('operations-progress').textContent =
      '旧服务连接已断开，正在等待新版本或回滚结果…';
  }
  schedulePoll();
}

export async function mount(next) {
  context = next;
  mounted = true;
  if (!bound) {
    document.getElementById('operations-refresh').addEventListener('click', () => { void load(); });
    document.getElementById('operations-staged-list').addEventListener('click', event => {
      const button = event.target.closest('[data-operation-activate]');
      if (button) void activate(button.dataset.operationActivate);
    });
    bound = true;
  }
  await load();
}

export async function unmount() {
  mounted = false;
  clearRefreshTimer();
  pollDeadline = 0;
  context = null;
}

export async function refresh() {
  await load();
}
