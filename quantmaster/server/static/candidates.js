(() => {
  'use strict';

  const PAGE_SIZE = 100;
  const today = new Date().toISOString().slice(0, 10);
  const workspace = document.getElementById('candidate-workspace');
  const listRoot = document.getElementById('candidate-list');
  const mobileSelect = document.getElementById('candidate-mobile-select');
  const state = {
    loaded: false,
    loading: false,
    catalogPromise: null,
    catalog: [],
    conflicts: [],
    currentName: null,
    lastExistingName: null,
    pendingName: null,
    detail: null,
    draft: [],
    originalSymbols: [],
    draftName: '',
    newMode: false,
    dirty: false,
    memberQuery: '',
    listQuery: '',
    page: 1,
    mode: null,
    bulkText: '',
    indexSymbol: '000300.SH',
    validationErrors: [],
    importData: null,
    guard: null,
    originTab: null,
    notice: null,
  };

  function html(value) {
    return String(value ?? '').replace(/[&<>"']/g, char => ({
      '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;',
    }[char]));
  }

  function cloneMembers(members) {
    return (members || []).map(item => ({symbol: item.symbol, name: item.name || null}));
  }

  function sourceLabel(value) {
    return ({
      built_in: '内置示例', custom: '本地自定义', 'tushare:index_weight': 'Tushare 历史成分',
    })[value] || value || '—';
  }

  function qualityLabel(value) {
    return value === 'production' ? '生产研究' : value === 'sandbox' ? '沙盒研究' : '—';
  }

  function typeLabel(item) {
    if (item?.kind === 'dynamic') return '动态候选';
    if (item?.readonly) return '系统固定候选';
    return '自定义候选';
  }

  function tabLabel(tab) {
    return ({market:'市场', news:'资讯', decision:'决策', lab:'Quant Lab', backtest:'回测',
      paper:'模拟盘', ledger:'实盘', automation:'自动化', settings:'设置'})[tab] || '原页面';
  }

  async function request(path, options = {}) {
    const method = String(options.method || 'GET').toUpperCase();
    if (method === 'GET') return window.QuantMasterAPI(path, options);
    await window.QuantMasterManagement.ensureSettings();
    return window.QuantMasterManagement.request(path, options);
  }

  function catalogItem(name) {
    return state.catalog.find(item => item.name === name);
  }

  function allowedCatalog(scope) {
    return state.catalog.filter(item => scope === 'all' || item.kind === 'fixed');
  }

  function syncCandidateSelects(mapping = null) {
    document.querySelectorAll('[data-candidate-select]').forEach(select => {
      const scope = select.dataset.candidateSelect || 'fixed';
      const choices = allowedCatalog(scope);
      let current = select.value || select.dataset.candidateValue || '';
      if (mapping && current === mapping.from) current = mapping.to;
      select.innerHTML = choices.map(item =>
        `<option value="${html(item.name)}">${html(item.name)}${item.kind === 'dynamic' ? ' · 动态' : ''}</option>`
      ).join('');
      if (choices.some(item => item.name === current)) select.value = current;
      else if (choices.some(item => item.name === 'demo')) select.value = 'demo';
      else if (choices.length) select.value = choices[0].name;
      select.dataset.candidateValue = select.value;
    });
    document.dispatchEvent(new CustomEvent('quantmaster:candidates-updated', {
      detail: {catalog: state.catalog, mapping},
    }));
  }

  function renderConflicts() {
    const root = document.getElementById('candidate-conflict');
    root.hidden = !state.conflicts.length;
    root.innerHTML = state.conflicts.map(item =>
      `<strong>${html(item.message)}</strong><br><span>${html(item.path)}</span>`).join('<br>');
  }

  function renderCatalog() {
    const query = state.listQuery.trim().toLowerCase();
    const visible = state.catalog.filter(item => item.name.toLowerCase().includes(query));
    listRoot.innerHTML = visible.length ? visible.map(item => {
      const references = (item.references || []).map(reference => reference.label).join('、');
      const count = item.count == null ? '按日期' : `${item.count} 只`;
      return `<button class="candidate-item${item.name === state.currentName ? ' active' : ''}" type="button"
        role="option" aria-selected="${item.name === state.currentName}" data-candidate-name="${html(item.name)}">
        <strong>${html(item.name)}</strong><small>${html(typeLabel(item))}${references ? ` · ${html(references)}` : ''}</small>
        <span class="candidate-item-count">${count}</span></button>`;
    }).join('') : '<div class="candidate-list-state">没有匹配的候选。换个名称试试。</div>';
    mobileSelect.innerHTML = state.catalog.map(item =>
      `<option value="${html(item.name)}">${html(item.name)} · ${html(typeLabel(item))}</option>`).join('');
    if (state.currentName && catalogItem(state.currentName)) mobileSelect.value = state.currentName;
  }

  async function refreshCatalog({mapping = null, select = null, loadDetail = false} = {}) {
    if (state.catalogPromise) return state.catalogPromise;
    state.loading = true;
    state.catalogPromise = (async () => {
      try {
        const data = await request('/api/settings/universes');
        state.catalog = data.universes || [];
        state.conflicts = data.conflicts || [];
        state.loaded = true;
        syncCandidateSelects(mapping);
        renderConflicts();
        renderCatalog();
        const target = select || state.pendingName || state.currentName || state.catalog[0]?.name;
        state.pendingName = null;
        if (loadDetail && target) await selectCandidate(target, {force:true});
      } catch (error) {
        listRoot.innerHTML = `<div class="candidate-list-state err">${html(error.message)}</div>`;
        workspace.innerHTML = renderErrorState(error.message);
      } finally {
        state.loading = false;
      }
    })();
    try { return await state.catalogPromise; }
    finally { state.catalogPromise = null; }
  }

  function renderErrorState(message, dynamic = false) {
    return `<div class="candidate-empty"><strong>候选详情暂时不可用</strong>
      <span>${html(message)}</span>${dynamic ? '<button class="ghost" type="button" data-candidate-settings="data">检查数据设置</button>' : ''}</div>`;
  }

  function renderLoading(label = '正在读取候选详情…') {
    workspace.innerHTML = `<div class="candidate-feedback">${html(label)}</div><div class="candidate-skeleton" aria-label="${html(label)}">
      <span></span><span></span><span></span></div>`;
  }

  async function selectCandidate(name, {force = false, asOf = null} = {}) {
    if (!name || !catalogItem(name)) return;
    if (!force && state.dirty && name !== state.currentName) {
      showGuard({type:'candidate', value:name});
      return;
    }
    state.lastExistingName = name;
    state.currentName = name;
    state.newMode = false;
    state.mode = null;
    state.guard = null;
    state.notice = null;
    state.memberQuery = '';
    state.page = 1;
    renderCatalog();
    renderLoading(name === 'csi800' ? '正在读取目标日的动态成分…' : '正在读取候选详情…');
    try {
      const suffix = name === 'csi800' ? `?as_of=${encodeURIComponent(asOf || state.detail?.as_of || today)}` : '';
      const detail = await request(`/api/settings/universes/${encodeURIComponent(name)}${suffix}`);
      hydrateDetail(detail);
    } catch (error) {
      state.detail = null;
      state.draft = [];
      state.dirty = false;
      workspace.innerHTML = renderErrorState(error.message, name === 'csi800');
    }
  }

  function hydrateDetail(detail) {
    state.detail = detail;
    state.draft = cloneMembers(detail.members);
    state.originalSymbols = detail.symbols.slice();
    state.draftName = detail.name;
    state.newMode = false;
    state.dirty = false;
    state.validationErrors = [];
    state.mode = null;
    state.notice = null;
    renderDetail();
  }

  function startNew({name = '', members = [], errors = []} = {}) {
    if (state.currentName) state.lastExistingName = state.currentName;
    state.currentName = null;
    state.detail = {
      name: '', kind: 'fixed', readonly: false, source: 'custom', research_quality: 'sandbox',
      references: [], members: [], symbols: [], count: 0,
    };
    state.draft = cloneMembers(members);
    state.originalSymbols = [];
    state.draftName = name;
    state.newMode = true;
    state.mode = null;
    state.memberQuery = '';
    state.page = 1;
    state.validationErrors = errors;
    state.notice = errors.length ? {kind:'error', message:`有 ${errors.length} 个代码未加入，请在批量编辑中修正。`} : null;
    updateDirty();
    renderCatalog();
    renderDetail();
  }

  function updateDirty() {
    const symbols = state.draft.map(item => item.symbol);
    state.dirty = state.newMode
      ? Boolean(state.draftName.trim() || symbols.length)
      : JSON.stringify(symbols) !== JSON.stringify(state.originalSymbols);
    updateSaveDock();
  }

  function updateSaveDock() {
    const dock = document.querySelector('.candidate-save-dock');
    if (!dock) return;
    dock.classList.toggle('dirty', state.dirty);
    const label = dock.querySelector('span');
    if (label) label.textContent = state.dirty ? '有尚未生效的更改' : '当前候选与已保存版本一致';
    const save = dock.querySelector('[data-candidate-action="save"]');
    if (save) save.disabled = !state.dirty || !state.draftName.trim() || !state.draft.length;
  }

  function referenceText(references) {
    return (references || []).length
      ? references.map(item => item.label).join('、')
      : '未被默认流程引用';
  }

  function renderNewConfig() {
    if (!state.newMode) return '';
    const errors = state.validationErrors.length ? `<div class="candidate-tool-errors"><strong>未加入的代码</strong><ul>${
      state.validationErrors.slice(0, 8).map(item => `<li>${html(item.value)}：${html(item.message)}</li>`).join('')
    }</ul>${state.validationErrors.length > 8 ? `<span>另有 ${state.validationErrors.length - 8} 项</span>` : ''}</div>` : '';
    return `<section class="candidate-tool-panel" aria-label="新候选设置">
      <div class="candidate-tool-head"><div><h4>新建候选</h4><p>先命名，再逐只添加、批量粘贴，或读取某个指数的当前成分。</p></div></div>
      <label>名称<input id="candidate-new-name" value="${html(state.draftName)}" maxlength="40" placeholder="例如 核心观察"></label>
      <div class="candidate-date-row"><label>指数代码<input id="candidate-index-symbol" value="${html(state.indexSymbol)}" placeholder="000300.SH"></label>
        <button class="ghost" type="button" data-candidate-action="index-preview">读取指数成分</button></div>
      ${errors}</section>`;
  }

  function renderGuard() {
    if (!state.guard) return '';
    return `<section class="candidate-tool-panel" aria-label="未保存更改">
      <div class="candidate-tool-head"><div><h4>先处理尚未生效的更改</h4><p>保存后继续，或放弃当前草稿。留在这里不会丢失内容。</p></div></div>
      <div class="candidate-tool-actions"><button class="primary" type="button" data-candidate-action="guard-save">保存并继续</button>
        <button class="ghost candidate-danger" type="button" data-candidate-action="guard-discard">放弃并继续</button>
        <button class="ghost" type="button" data-candidate-action="guard-keep">继续编辑</button></div></section>`;
  }

  function renderModePanel() {
    if (state.guard) return renderGuard();
    if (state.mode === 'add') return `<section class="candidate-tool-panel">
      <div class="candidate-tool-head"><div><h4>添加一只标的</h4><p>支持 600519、600519.SH、SZ000001 等常见 A 股写法。</p></div>
        <button class="candidate-origin" type="button" data-candidate-action="close-tool">收起</button></div>
      <label>证券代码<input id="candidate-add-symbol" autocomplete="off" placeholder="600519"></label>
      <div class="candidate-tool-actions"><button class="primary" type="button" data-candidate-action="add-symbol">添加到草稿</button></div></section>`;
    if (state.mode === 'bulk') return `<section class="candidate-tool-panel">
      <div class="candidate-tool-head"><div><h4>批量编辑</h4><p>每行或逗号分隔；应用后只更新草稿，仍需保存才会生效。</p></div>
        <button class="candidate-origin" type="button" data-candidate-action="close-tool">收起</button></div>
      <label>候选代码<textarea id="candidate-bulk-text" rows="12" placeholder="600519&#10;000858.SZ">${html(state.bulkText)}</textarea></label>
      <div id="candidate-bulk-errors" class="candidate-tool-errors"></div>
      <div class="candidate-tool-actions"><button class="primary" type="button" data-candidate-action="apply-bulk">应用到草稿</button></div></section>`;
    if (state.mode === 'import') {
      const lists = state.importData || {};
      const sources = [
        ['favorites', '自选', lists.favorites?.length || 0],
        ['following', '关注', lists.following?.length || 0],
        ['holdings', '持有', lists.holdings?.length || 0],
      ];
      return `<section class="candidate-tool-panel">
        <div class="candidate-tool-head"><div><h4>从我的标的复制</h4><p>来源列表保持不变；复制后先形成新候选草稿。</p></div>
          <button class="candidate-origin" type="button" data-candidate-action="close-tool">收起</button></div>
        <div class="candidate-import-sources">${sources.map(([key,label,count]) =>
          `<button class="ghost" type="button" data-candidate-import-source="${key}" ${count ? '' : 'disabled'}>${label}<br><small>${count} 只</small></button>`).join('')}</div></section>`;
    }
    if (state.mode === 'rename') return `<section class="candidate-tool-panel">
      <div class="candidate-tool-head"><div><h4>重命名候选</h4><p>自动化和 Quant Lab 的活动引用会同步更新；历史记录保留原名称。</p></div>
        <button class="candidate-origin" type="button" data-candidate-action="close-tool">收起</button></div>
      <label>新名称<input id="candidate-rename-name" value="${html(state.currentName)}" maxlength="40"></label>
      <div class="candidate-tool-actions"><button class="primary" type="button" data-candidate-action="confirm-rename">保存新名称</button></div></section>`;
    if (state.mode === 'delete') {
      const references = state.detail?.references || [];
      const needsFixed = references.some(item => item.key === 'automation.primary_universe');
      const replacements = state.catalog.filter(item => item.name !== state.currentName && (!needsFixed || item.kind === 'fixed'));
      return `<section class="candidate-tool-panel">
        <div class="candidate-tool-head"><div><h4>删除 ${html(state.currentName)}</h4><p>候选文件将永久删除。${references.length ? '请选择替代候选，活动流程会先安全切换。' : '它当前未被默认流程引用。'}</p></div>
          <button class="candidate-origin" type="button" data-candidate-action="close-tool">保留候选</button></div>
        ${references.length ? `<label>替代候选<select id="candidate-delete-replacement">${replacements.map(item =>
          `<option value="${html(item.name)}">${html(item.name)} · ${html(typeLabel(item))}</option>`).join('')}</select></label>` : ''}
        <div class="candidate-tool-actions"><button class="ghost candidate-danger" type="button" data-candidate-action="confirm-delete">删除 ${html(state.currentName)}</button></div></section>`;
    }
    return '';
  }

  function renderDetail() {
    if (!state.detail) return;
    const detail = state.detail;
    const editable = !detail.readonly;
    const name = state.newMode ? (state.draftName || '尚未命名') : detail.name;
    const references = referenceText(detail.references);
    const origin = state.originTab ? `<button class="candidate-origin" type="button" data-candidate-action="return-origin">← 返回${html(tabLabel(state.originTab))}</button>` : '';
    const actions = state.newMode ? '' : `<button class="ghost" type="button" data-candidate-action="clone">复制为自定义候选</button>${editable ?
      '<button class="ghost" type="button" data-candidate-action="rename">重命名</button><button class="ghost candidate-danger" type="button" data-candidate-action="delete">删除</button>' : ''}`;
    const dynamic = detail.kind === 'dynamic' ? `<div class="candidate-date-row"><label>查看日期<input id="candidate-as-of" type="date" max="${today}" value="${html(detail.as_of || today)}"></label>
      <button class="ghost" type="button" data-candidate-action="load-date">查看该日成分</button></div>
      <div class="candidate-snapshots">${Object.entries(detail.snapshot_dates || {}).map(([code,value]) =>
        `<span>${html(code)} 快照 · ${html(value)}</span>`).join('')}</div>` : '';
    const notice = state.notice ? `<div class="candidate-feedback ${html(state.notice.kind)}">${html(state.notice.message)}</div>` : '<div class="candidate-feedback" id="candidate-feedback"></div>';
    const saveDock = editable ? `<div class="candidate-save-dock${state.dirty ? ' dirty' : ''}"><span>${state.dirty ? '有尚未生效的更改' : '当前候选与已保存版本一致'}</span>
      <button class="ghost" type="button" data-candidate-action="discard" ${state.dirty ? '' : 'disabled'}>放弃更改</button>
      <button class="primary" type="button" data-candidate-action="save" ${state.dirty && state.draftName.trim() && state.draft.length ? '' : 'disabled'}>${state.newMode ? '创建候选' : '保存更改'}</button></div>` : '';
    workspace.innerHTML = `<article class="candidate-detail">
      <header class="candidate-detail-head"><div class="candidate-title-block">${origin}<div class="candidate-title-line"><h3>${html(name)}</h3>
        <span class="candidate-type${detail.kind === 'dynamic' ? ' dynamic' : ''}">${html(typeLabel(detail))}</span></div>
        <p class="candidate-detail-copy">${detail.kind === 'dynamic' ? '成分随历史日期变化，页面不会把它改写为固定列表。' : editable ? '编辑先进入本地草稿，保存后才影响后续研究。' : '系统候选只读，可复制后继续编辑。'}</p></div>
        <div class="candidate-detail-actions">${actions}</div></header>
      <dl class="candidate-meta"><div><dt>成分</dt><dd><strong>${state.draft.length}</strong> 只</dd></div>
        <div><dt>来源</dt><dd>${html(sourceLabel(detail.source))}</dd></div>
        <div><dt>研究质量</dt><dd>${html(qualityLabel(detail.research_quality))}</dd></div>
        <div><dt>使用位置</dt><dd>${html(references)}</dd></div></dl>
      ${dynamic}${renderNewConfig()}${renderModePanel()}
      <div class="candidate-toolbar"><div class="candidate-search-add"><input id="candidate-member-search" type="search" value="${html(state.memberQuery)}" placeholder="按代码或名称筛选" aria-label="筛选候选成分">
        <button class="ghost" type="button" data-candidate-action="clear-search">清除</button></div>
        <div class="candidate-toolbar-actions">${editable ? '<button class="ghost" type="button" data-candidate-action="add">添加代码</button><button class="ghost" type="button" data-candidate-action="bulk">批量编辑</button>' : ''}
          <button class="ghost" type="button" data-candidate-action="refresh-names" ${state.draft.length ? '' : 'disabled'}>同步名称</button></div></div>
      ${notice}<div id="candidate-member-section"></div>${saveDock}</article>`;
    renderMembers();
  }

  function filteredMembers() {
    const query = state.memberQuery.trim().toLowerCase();
    return query ? state.draft.filter(item =>
      item.symbol.toLowerCase().includes(query) || (item.name || '').toLowerCase().includes(query)) : state.draft;
  }

  function renderMembers() {
    const root = document.getElementById('candidate-member-section');
    if (!root) return;
    const editable = !state.detail?.readonly;
    const members = filteredMembers();
    const pages = Math.max(1, Math.ceil(members.length / PAGE_SIZE));
    state.page = Math.min(state.page, pages);
    const start = (state.page - 1) * PAGE_SIZE;
    const rows = members.slice(start, start + PAGE_SIZE);
    if (!members.length) {
      root.innerHTML = `<div class="candidate-empty"><strong>${state.memberQuery ? '没有匹配的成分' : '候选还是空的'}</strong>
        <span>${state.memberQuery ? '换个代码或名称试试。' : editable ? '添加代码或切换到批量编辑，建立第一版候选。' : '当前日期没有可显示的成分。'}</span></div>`;
      return;
    }
    root.innerHTML = `<div class="candidate-table-wrap"><table class="candidate-table"><thead><tr><th>证券名称</th><th>代码</th><th>操作</th></tr></thead><tbody>${rows.map(item =>
      `<tr><td class="candidate-member-name${item.name ? '' : ' pending'}">${html(item.name || '名称待同步')}</td>
        <td class="candidate-member-symbol">${html(item.symbol)}</td><td class="candidate-member-action">${editable ?
          `<button type="button" data-candidate-remove="${html(item.symbol)}" aria-label="从候选移除 ${html(item.symbol)}">移除</button>` : ''}</td></tr>`).join('')}</tbody></table></div>
      <div class="candidate-pagination"><span>第 ${state.page} / ${pages} 页 · ${members.length} 只</span><div>
        <button class="ghost" type="button" data-candidate-page="${state.page - 1}" ${state.page <= 1 ? 'disabled' : ''}>上一页</button>
        <button class="ghost" type="button" data-candidate-page="${state.page + 1}" ${state.page >= pages ? 'disabled' : ''}>下一页</button></div></div>`;
  }

  function setNotice(kind, message) {
    state.notice = {kind, message};
    const root = document.getElementById('candidate-feedback');
    if (root) {
      root.className = `candidate-feedback ${kind}`;
      root.textContent = message;
    } else renderDetail();
  }

  function showGuard(pending) {
    state.guard = {pending};
    renderDetail();
    workspace.scrollIntoView({block:'start'});
  }

  async function continuePending() {
    const pending = state.guard?.pending;
    state.guard = null;
    if (!pending) return;
    if (pending.type === 'candidate') await selectCandidate(pending.value, {force:true});
    else if (pending.type === 'tab') document.querySelector(`header [data-tab="${CSS.escape(pending.value)}"]`)?.click();
    else if (pending.type === 'action' && pending.value === 'new') startNew();
    else if (pending.type === 'action' && pending.value === 'import') await openImport();
  }

  async function previewSymbols(symbols) {
    return request('/api/settings/universes/preview', {
      method:'POST', body:{kind:'manual', symbols},
    });
  }

  async function addSymbol() {
    const input = document.getElementById('candidate-add-symbol');
    const value = input?.value.trim();
    if (!value) return setNotice('error', '请输入要添加的证券代码。');
    try {
      const result = await previewSymbols([value]);
      if (result.errors.length) return setNotice('error', result.errors[0].message);
      const member = result.members[0];
      if (state.draft.some(item => item.symbol === member.symbol)) return setNotice('error', `${member.symbol} 已经在当前候选中。`);
      state.draft.push(member);
      state.mode = null;
      state.page = Math.ceil(state.draft.length / PAGE_SIZE);
      updateDirty();
      state.notice = {kind:'success', message:`已把 ${member.symbol} 加入草稿。`};
      renderDetail();
    } catch (error) { setNotice('error', error.message); }
  }

  async function applyBulk() {
    const values = state.bulkText.split(/[\s,，;；]+/).map(item => item.trim()).filter(Boolean);
    const errorsRoot = document.getElementById('candidate-bulk-errors');
    try {
      const result = await previewSymbols(values);
      if (result.errors.length) {
        errorsRoot.innerHTML = `<strong>请修正以下代码后再应用</strong><ul>${result.errors.slice(0, 12).map(item =>
          `<li>${html(item.value)}：${html(item.message)}</li>`).join('')}</ul>`;
        return;
      }
      state.draft = cloneMembers(result.members);
      state.validationErrors = [];
      state.mode = null;
      state.page = 1;
      updateDirty();
      state.notice = {kind:'success', message:`已规范化 ${result.count} 只标的${result.duplicates.length ? `，忽略 ${result.duplicates.length} 个重复项` : ''}。`};
      renderDetail();
    } catch (error) {
      if (errorsRoot) errorsRoot.textContent = error.message;
    }
  }

  async function previewIndex(button) {
    const name = document.getElementById('candidate-new-name')?.value.trim();
    const indexSymbol = document.getElementById('candidate-index-symbol')?.value.trim();
    if (!indexSymbol) return setNotice('error', '请输入指数代码，例如 000300.SH。');
    button.disabled = true;
    button.textContent = '正在读取…';
    try {
      const result = await request('/api/settings/universes/preview', {
        method:'POST', body:{kind:'index', index_symbol:indexSymbol},
      });
      state.draftName = name || state.draftName;
      state.indexSymbol = indexSymbol;
      state.draft = cloneMembers(result.members);
      state.validationErrors = result.errors || [];
      updateDirty();
      state.notice = {kind:'success', message:`已读取 ${result.count} 只指数成分，保存前仍可编辑。`};
      renderDetail();
    } catch (error) {
      setNotice('error', error.message);
      button.disabled = false;
      button.textContent = '读取指数成分';
    }
  }

  async function refreshNames(button) {
    if (!state.draft.length) return;
    button.disabled = true;
    button.textContent = '正在同步…';
    try {
      const result = await request('/api/settings/universes/names/refresh', {
        method:'POST', body:{symbols:state.draft.map(item => item.symbol)},
      });
      state.draft.forEach(item => { item.name = result.names[item.symbol] || item.name || null; });
      if (state.detail?.members) state.detail.members = cloneMembers(state.draft);
      state.notice = {kind:result.missing.length ? 'error' : 'success', message:result.missing.length
        ? `已同步可用名称，仍有 ${result.missing.length} 只待补全。`
        : `已同步 ${state.draft.length} 只证券名称。`};
      renderDetail();
    } catch (error) {
      setNotice('error', error.message);
      button.disabled = false;
      button.textContent = '同步名称';
    }
  }

  async function saveCandidate(button = null) {
    const name = state.draftName.trim();
    if (!name) { setNotice('error', '请先填写候选名称。'); return false; }
    if (!state.draft.length) { setNotice('error', '候选至少需要一只有效标的。'); return false; }
    if (button) { button.disabled = true; button.textContent = state.newMode ? '正在创建…' : '正在保存…'; }
    const body = {name, symbols:state.draft.map(item => item.symbol)};
    try {
      if (state.newMode) await request('/api/settings/universes', {method:'POST', body});
      else await request(`/api/settings/universes/${encodeURIComponent(state.currentName)}`, {method:'PUT', body});
      await refreshCatalog({select:name, loadDetail:true});
      return true;
    } catch (error) {
      setNotice('error', error.message);
      if (button) { button.disabled = false; button.textContent = state.newMode ? '创建候选' : '保存更改'; }
      return false;
    }
  }

  function discardChanges() {
    if (state.newMode) {
      const target = state.lastExistingName || state.catalog[0]?.name;
      if (target) selectCandidate(target, {force:true});
      return;
    }
    state.draft = cloneMembers(state.detail.members);
    state.draftName = state.detail.name;
    state.mode = null;
    state.notice = null;
    state.dirty = false;
    renderDetail();
  }

  async function openImport() {
    if (state.dirty) return showGuard({type:'action', value:'import'});
    state.mode = 'import';
    state.notice = null;
    renderDetail();
    try {
      state.importData = await window.QuantMasterAPI('/api/assets/lists');
      renderDetail();
    } catch (error) {
      state.mode = null;
      setNotice('error', `无法读取我的标的：${error.message}`);
    }
  }

  async function importSource(source) {
    const labels = {favorites:'自选', following:'关注', holdings:'持有'};
    const items = state.importData?.[source] || [];
    if (!items.length) return;
    try {
      const result = await previewSymbols(items.map(item => item.symbol));
      const sourceNames = new Map(items.map(item => [item.symbol, item.name || null]));
      const members = result.members.map(item => ({
        ...item, name:item.name || sourceNames.get(item.symbol) || null,
      }));
      startNew({
        name:`${labels[source]}_${today.replaceAll('-', '')}`,
        members, errors:result.errors,
      });
    } catch (error) { setNotice('error', error.message); }
  }

  async function renameCandidate(button) {
    const next = document.getElementById('candidate-rename-name')?.value.trim();
    if (!next) return setNotice('error', '请填写新的候选名称。');
    if (next === state.currentName) return setNotice('error', '新名称与当前名称相同。');
    button.disabled = true;
    button.textContent = '正在重命名…';
    const previous = state.currentName;
    try {
      await request(`/api/settings/universes/${encodeURIComponent(previous)}/rename`, {
        method:'POST', body:{new_name:next},
      });
      await refreshCatalog({mapping:{from:previous,to:next}, select:next, loadDetail:true});
      await window.QuantMasterManagement.ensureSettings(true);
    } catch (error) {
      setNotice('error', error.message);
      button.disabled = false;
      button.textContent = '保存新名称';
    }
  }

  async function deleteCandidate(button) {
    const previous = state.currentName;
    const replacement = document.getElementById('candidate-delete-replacement')?.value || '';
    button.disabled = true;
    button.textContent = '正在删除…';
    try {
      const suffix = replacement ? `?replacement=${encodeURIComponent(replacement)}` : '';
      await request(`/api/settings/universes/${encodeURIComponent(previous)}${suffix}`, {method:'DELETE'});
      await refreshCatalog({mapping:{from:previous,to:replacement || 'demo'}, select:replacement || 'demo', loadDetail:true});
      await window.QuantMasterManagement.ensureSettings(true);
    } catch (error) {
      setNotice('error', error.message);
      button.disabled = false;
      button.textContent = `删除 ${previous}`;
    }
  }

  function cloneCurrent() {
    const base = state.detail?.name || '候选';
    startNew({name:`${base}_${today.replaceAll('-', '')}`, members:state.draft});
  }

  function openCandidate(name, origin = null) {
    const active = document.querySelector('.tab.active')?.id.replace('tab-', '');
    if (origin || (active && active !== 'candidates')) state.originTab = origin || active;
    state.pendingName = name || state.currentName || 'demo';
    document.querySelector('header [data-tab="candidates"]')?.click();
    loadCandidates();
  }

  async function loadCandidates() {
    if (!state.loaded) {
      await refreshCatalog();
    }
    const target = state.pendingName || state.currentName || state.catalog[0]?.name;
    state.pendingName = null;
    if (target && (!state.detail || state.currentName !== target)) await selectCandidate(target, {force:true});
    else if (state.detail) renderDetail();
  }

  document.getElementById('candidate-list-search').addEventListener('input', event => {
    state.listQuery = event.target.value;
    renderCatalog();
  });

  listRoot.addEventListener('click', event => {
    const button = event.target.closest('[data-candidate-name]');
    if (button) selectCandidate(button.dataset.candidateName);
  });

  listRoot.addEventListener('keydown', event => {
    if (!['ArrowDown', 'ArrowUp'].includes(event.key)) return;
    const items = Array.from(listRoot.querySelectorAll('[data-candidate-name]'));
    const index = Math.max(0, items.indexOf(document.activeElement));
    const next = event.key === 'ArrowDown' ? Math.min(items.length - 1, index + 1) : Math.max(0, index - 1);
    items[next]?.focus();
    event.preventDefault();
  });

  mobileSelect.addEventListener('change', event => selectCandidate(event.target.value));

  document.getElementById('candidate-new').addEventListener('click', async () => {
    await loadCandidates();
    if (state.dirty) showGuard({type:'action', value:'new'});
    else startNew();
  });
  document.getElementById('candidate-import').addEventListener('click', async () => {
    await loadCandidates();
    await openImport();
  });

  document.addEventListener('click', event => {
    const trigger = event.target.closest('[data-candidate-view]');
    if (!trigger) return;
    const select = trigger.closest('label')?.querySelector('[data-candidate-select]');
    if (select?.value) openCandidate(select.value);
  });

  document.addEventListener('change', event => {
    if (event.target.matches('[data-candidate-select]')) event.target.dataset.candidateValue = event.target.value;
  });

  workspace.addEventListener('input', event => {
    if (event.target.id === 'candidate-member-search') {
      state.memberQuery = event.target.value;
      state.page = 1;
      renderMembers();
    } else if (event.target.id === 'candidate-bulk-text') state.bulkText = event.target.value;
    else if (event.target.id === 'candidate-new-name') {
      state.draftName = event.target.value;
      updateDirty();
      const title = workspace.querySelector('.candidate-title-line h3');
      if (title) title.textContent = state.draftName || '尚未命名';
    } else if (event.target.id === 'candidate-index-symbol') state.indexSymbol = event.target.value;
  });

  workspace.addEventListener('click', async event => {
    const page = event.target.closest('[data-candidate-page]');
    if (page && !page.disabled) {
      state.page = Number(page.dataset.candidatePage);
      renderMembers();
      return;
    }
    const remove = event.target.closest('[data-candidate-remove]');
    if (remove) {
      state.draft = state.draft.filter(item => item.symbol !== remove.dataset.candidateRemove);
      updateDirty();
      renderMembers();
      return;
    }
    const source = event.target.closest('[data-candidate-import-source]');
    if (source && !source.disabled) return importSource(source.dataset.candidateImportSource);
    const button = event.target.closest('[data-candidate-action]');
    if (!button || button.disabled) return;
    const action = button.dataset.candidateAction;
    if (action === 'return-origin') {
      const target = state.originTab;
      if (state.dirty) showGuard({type:'tab', value:target});
      else document.querySelector(`header [data-tab="${CSS.escape(target)}"]`)?.click();
    } else if (action === 'clone') cloneCurrent();
    else if (action === 'rename') { state.mode = 'rename'; renderDetail(); }
    else if (action === 'delete') { state.mode = 'delete'; renderDetail(); }
    else if (action === 'add') { state.mode = 'add'; renderDetail(); document.getElementById('candidate-add-symbol')?.focus(); }
    else if (action === 'bulk') { state.mode = 'bulk'; state.bulkText = state.draft.map(item => item.symbol).join('\n'); renderDetail(); }
    else if (action === 'close-tool') { state.mode = null; renderDetail(); }
    else if (action === 'clear-search') { state.memberQuery = ''; state.page = 1; renderDetail(); }
    else if (action === 'add-symbol') await addSymbol();
    else if (action === 'apply-bulk') await applyBulk();
    else if (action === 'index-preview') await previewIndex(button);
    else if (action === 'refresh-names') await refreshNames(button);
    else if (action === 'save') await saveCandidate(button);
    else if (action === 'discard') discardChanges();
    else if (action === 'confirm-rename') await renameCandidate(button);
    else if (action === 'confirm-delete') await deleteCandidate(button);
    else if (action === 'load-date') {
      const asOf = document.getElementById('candidate-as-of')?.value;
      await selectCandidate('csi800', {force:true, asOf});
    } else if (action === 'guard-save') {
      const pending = state.guard?.pending;
      if (await saveCandidate(button)) {
        state.guard = {pending};
        await continuePending();
      }
    } else if (action === 'guard-discard') {
      const pending = state.guard?.pending;
      state.guard = {pending};
      state.dirty = false;
      await continuePending();
    } else if (action === 'guard-keep') { state.guard = null; renderDetail(); }
  });

  workspace.addEventListener('click', event => {
    const settings = event.target.closest('[data-candidate-settings]');
    if (settings) window.QuantMasterManagement.open(settings.dataset.candidateSettings);
  });

  document.querySelector('header').addEventListener('click', event => {
    const target = event.target.closest('[data-tab]');
    const active = document.getElementById('tab-candidates')?.classList.contains('active');
    if (!active || !target || target.dataset.tab === 'candidates' || !state.dirty) return;
    event.preventDefault();
    event.stopImmediatePropagation();
    showGuard({type:'tab', value:target.dataset.tab});
  }, true);

  window.addEventListener('beforeunload', event => {
    if (!state.dirty) return;
    event.preventDefault();
    event.returnValue = '';
  });

  window.loadCandidates = loadCandidates;
  window.QuantMasterCandidates = {
    open: openCandidate,
    refresh: refreshCatalog,
    get catalog() { return state.catalog.slice(); },
  };

  refreshCatalog();
})();
