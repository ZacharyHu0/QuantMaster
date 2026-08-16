const settingsFeature = (() => {
  'use strict';

  const state = {
    loaded: false,
    config: null,
    secretActions: { llm: 'keep', tushare: 'keep' },
    migrationTimer: null,
    migrationId: '',
    dataRefreshTimer: null,
    dataRefreshId: '',
    dataRefreshPreview: null,
    researchTimer: null,
    researchId: '',
    researchPreview: null,
    researchCatalog: null,
    researchControlsLoaded: false,
    modelCheckSignature: '',
    modelCheckTimer: null,
    autoSaveTimer: null,
    retryTimer: null,
    saveInFlight: false,
    saveQueued: false,
    editRevision: 0,
    savedRevision: 0,
    lastSavedFingerprint: '',
    fillingForm: false,
    weixinLoginTimer: null,
    weixinLoginId: '',
    weixinLoginQr: '',
    weixinLoginCreateSequence: 0,
    weixinLoginCreatePending: false,
    lastRuntime: null,
    diagnosticTasks: {},
    contractMigrationTimer: null,
    contractMigrationFailures: 0,
    contractMigrationId: '',
    persistedRevision: 0,
    latestGeneration: 0,
    runtimeEpoch: 0,
    applyTask: null,
  };
  const form = document.getElementById('settings-form');
  let freeStockDbPollTimer = null;
  let freeStockDbPollFailures = 0;
  let freeStockDbActive = false;
  let mounted = false;
  let lifecycleGeneration = 0;

  function isFreeStockDbActive(stockdb) {
    return ['queued', 'updating', 'restarting'].includes(stockdb?.state)
      || ['queued', 'stopping', 'syncing', 'restarting', 'validating'].includes(stockdb?.phase);
  }

  function scheduleFreeStockDbPoll(delay = 1000, generation = lifecycleGeneration) {
    if (!mounted || generation !== lifecycleGeneration || freeStockDbPollTimer !== null) return;
    freeStockDbPollTimer = setTimeout(() => pollFreeStockDbSidecar(generation), delay);
  }

  function html(value) {
    return String(value ?? '').replace(/[&<>"']/g, char => ({
      '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;',
    }[char]));
  }

  async function request(path, options = {}) {
    const headers = new Headers(options.headers || {});
    if (options.body && !(options.body instanceof FormData) && !headers.has('Content-Type')) {
      headers.set('Content-Type', 'application/json');
      options.body = JSON.stringify(options.body);
    }
    return window.QuantMasterAPI(path, {...options, headers});
  }

  function setPath(object, path, value) {
    const parts = path.split('.');
    let current = object;
    parts.slice(0, -1).forEach(part => {
      current[part] ||= {};
      current = current[part];
    });
    current[parts.at(-1)] = value;
  }

  function getPath(object, path) {
    return path.split('.').reduce((value, key) => value?.[key], object);
  }

  function parseSymbols(value) {
    return String(value).split(/[\s,，;；]+/).map(item => item.trim()).filter(Boolean);
  }

  function markDirty(message = '有未保存改动') {
    const el = document.getElementById('settings-save-state');
    el.className = 'dirty';
    el.querySelector('span:last-child').textContent = message;
    const draft = document.getElementById('settings-draft-revision');
    if (draft) draft.textContent = '草稿有未保存变化';
  }

  function setSaveState(kind, message) {
    const el = document.getElementById('settings-save-state');
    el.className = kind;
    el.querySelector('span:last-child').textContent = message;
  }

  function switchSection(name) {
    document.querySelectorAll('[data-settings-section]').forEach(button => {
      const active = button.dataset.settingsSection === name;
      button.classList.toggle('active', active);
      button.setAttribute('aria-current', active ? 'page' : 'false');
    });
    document.querySelectorAll('[data-settings-panel]').forEach(panel => {
      panel.classList.toggle('active', panel.dataset.settingsPanel === name);
    });
    const mobileSelect = document.getElementById('settings-section-select');
    if (mobileSelect && mobileSelect.value !== name) mobileSelect.value = name;
    form.hidden = ['sources', 'backup'].includes(name);
    if (name === 'backup') loadSnapshots();
    if (name === 'local-data') loadContractMigrationStatus();
    if (['automation', 'lab'].includes(name)) loadAutomationOverview();
  }

  function syncReasoningEffortOptions(normalize = false) {
    const provider = form.elements['llm.provider'].value;
    const select = form.elements['llm.reasoning_effort'];
    const newsSelect = form.elements['news.annotation_reasoning_effort'];
    const unsupported = provider === 'anthropic' ? new Set(['none', 'minimal']) : new Set();
    [...select.options].forEach(option => {
      const unavailable = unsupported.has(option.value);
      option.hidden = unavailable;
      option.disabled = unavailable;
    });
    if (normalize && unsupported.has(select.value)) select.value = 'medium';
    [...newsSelect.options].forEach(option => {
      option.hidden = false;
      option.disabled = false;
    });
    document.getElementById('reasoning-effort-hint').textContent = provider === 'anthropic'
      ? 'Anthropic 从低到最大；具体级别取决于所选模型'
      : '支持范围取决于所选模型和兼容网关';
    document.getElementById('news-reasoning-effort-hint').textContent =
      '完整七档使用 API 原文；实际支持范围取决于资讯模型和网关。';
  }

  document.getElementById('settings-nav').addEventListener('click', event => {
    const button = event.target.closest('[data-settings-section]');
    if (button) switchSection(button.dataset.settingsSection);
  });
  document.getElementById('settings-section-select').addEventListener('change', event => {
    switchSection(event.target.value);
  });

  document.querySelector('header').addEventListener('click', event => {
    if (event.target.closest('[data-tab="settings"]')) loadSettings();
  });

  function fillForm(config) {
    state.fillingForm = true;
    form.querySelectorAll('[name]').forEach(input => {
      const value = getPath(config, input.name);
      if (value === undefined) return;
      if (input.dataset.listCheckbox !== undefined) {
        input.checked = (value || []).map(String).includes(String(input.value));
      } else if (input.type === 'checkbox') input.checked = Boolean(value);
      else if (input.dataset.valueType === 'list') input.value = (value || []).join('\n');
      else {
        if (input.matches('[data-candidate-select]')) {
          input.dataset.candidateValue = value;
          if (value && ![...input.options].some(option => option.value === value)) {
            input.add(new Option(value, value));
          }
        }
        input.value = value;
      }
      input.removeAttribute('aria-invalid');
    });
    syncReasoningEffortOptions(true);
    document.getElementById('settings-config-path').textContent = config.config_path;
    renderFieldSources(config.field_sources || {});
    updateSecretStates(config);
    for (const name of ['llm', 'tushare']) {
      document.getElementById(`${name}-secret`).value = '';
      state.secretActions[name] = 'keep';
    }
    state.lastSavedFingerprint = JSON.stringify(documentPayload(false));
    state.savedRevision = state.editRevision;
    state.fillingForm = false;
    setSaveState('', config.managed_by_gui ? '自动保存已开启' : '填写后将自动启用 GUI 配置管理');
    renderRuntime(config.runtime || state.lastRuntime);
    renderSavedChecks(config.checks || {});
    scheduleAutomaticModelCheck();
  }

  function renderRuntime(runtime) {
    if (!runtime) return;
    const incomingRevision = Number(runtime.persisted_revision ?? runtime.config_revision ?? 0);
    const incomingGeneration = Number(runtime.latest_generation ?? 0);
    if (incomingRevision < state.persistedRevision) return;
    if (incomingRevision === state.persistedRevision && incomingGeneration < state.latestGeneration) return;
    state.persistedRevision = incomingRevision;
    state.latestGeneration = incomingGeneration;
    state.lastRuntime = runtime;
    const persisted = document.getElementById('settings-persisted-revision');
    if (persisted) persisted.textContent = `已保存 revision ${incomingRevision}`;
    const draft = document.getElementById('settings-draft-revision');
    if (draft && state.editRevision === state.savedRevision) draft.textContent = '草稿无变化';
    const components = Object.values(runtime.components || {});
    const drift = runtime.drift || components.filter(item =>
      Number(item.effective_revision || 0) !== incomingRevision);
    const summary = document.getElementById('settings-effective-summary');
    if (summary) summary.textContent = drift.length ? `${drift.length} 个组件待确认或存在漂移` : '所有组件已确认当前 revision';
    const list = document.getElementById('settings-component-list');
    if (list) {
      const labels = {
        effective:'已生效', pending:'待应用', rebuilding:'正在重建', restart_required:'需要重启',
        failed:'应用失败', unconfirmed:'未确认', superseded:'已被新版本取代',
      };
      list.innerHTML = components.map(item => {
        const status = String(item.status || 'pending');
        const appliedAt = item.last_applied_at
          ? ` · 最近 ${new Date(Number(item.last_applied_at) * 1000).toLocaleString('zh-CN', {hour12:false})}` : '';
        const detail = item.error || item.recommendation ||
          `${item.apply_strategy || 'immediate'} · generation ${Number(item.generation || 0)}`;
        return `<div class="settings-component-row"><span class="settings-component-name">${html(item.component)}</span><span class="settings-component-status ${html(status)}">${html(labels[status] || status)}</span><span class="settings-component-revision">effective ${Number(item.effective_revision || 0)} → target ${Number(item.target_revision || incomingRevision)} · generation ${Number(item.generation || 0)}</span><span class="settings-component-detail">${html(detail)}${item.diagnostic_code ? ` · ${html(item.diagnostic_code)}` : ''}${html(appliedAt)}</span></div>`;
      }).join('');
    }
    const note = document.getElementById('settings-runtime-note');
    const restart = runtime.server?.restart_required || [];
    note.classList.toggle('restart-required', Boolean(restart.length));
    note.textContent = restart.length
      ? `${restart.join(' / ')} 已保存，重启服务后生效`
      : drift.length ? '设置已保存，等待各组件确认应用' : '当前 revision 已由所有组件确认';
    const stockdb = runtime.free_stockdb;
    const stockdbStatus = document.getElementById('free-stockdb-sidecar-status');
    if (stockdb && stockdbStatus) {
      const elapsed = Number.isFinite(stockdb.elapsed_seconds) ? ` · 已用 ${stockdb.elapsed_seconds} 秒` : '';
      const engine = stockdb.sdk_engine ? ` · ${stockdb.sdk_engine}` : '';
      const sessions = stockdb.target_session
        ? ` · 目标 ${stockdb.target_session} / 实际 ${stockdb.actual_session || '待验收'}`
        : stockdb.validated_session ? ` · 已验证 ${stockdb.validated_session}` : '';
      const attempts = stockdb.attempt
        ? ` · 第 ${stockdb.attempt}/${stockdb.max_attempts || 1} 次` : '';
      const retry = stockdb.next_retry_at
        ? ` · 下次 ${new Date(stockdb.next_retry_at).toLocaleTimeString('zh-CN', {hour:'2-digit',minute:'2-digit',hour12:false})}` : '';
      stockdbStatus.textContent = `${stockdb.message || stockdb.state}${sessions}${attempts}${retry}${elapsed}${engine}${stockdb.updated_at ? ` · ${new Date(stockdb.updated_at).toLocaleString('zh-CN', {hour12: false})}` : ''}`;
      const failed = ['error', 'degraded'].includes(stockdb.state)
        || ['failed', 'manual_required'].includes(stockdb.update_result);
      const healthy = stockdb.state === 'running'
        && !['failed', 'manual_required', 'retry_wait'].includes(stockdb.update_result);
      stockdbStatus.className = `field-wide check-result ${failed ? 'error' : healthy ? 'success' : ''}`;
      const active = isFreeStockDbActive(stockdb);
      freeStockDbActive = active;
      const updateButton = document.getElementById('free-stockdb-update-now');
      if (updateButton) updateButton.disabled = active;
      if (mounted && active) scheduleFreeStockDbPoll();
    }
    const labels = {
      running: '运行中', standby: '等待调度租约', disabled: '已停用',
      draining: '停止领取新任务', degraded: '运行异常', applied: '已应用',
    };
    const automation = document.getElementById('automation-runtime-state');
    if (automation && runtime.automation) {
      automation.textContent = labels[runtime.automation.status] || runtime.automation.status;
      automation.className = `state-pill ${runtime.automation.status === 'running' ? 'buy' : ''}`;
    }
    const lab = document.getElementById('lab-runtime-state');
    if (lab && runtime.lab) {
      lab.textContent = labels[runtime.lab.status] || runtime.lab.status;
      lab.className = `state-pill ${runtime.lab.status === 'running' ? 'buy' : ''}`;
    }
  }

  function updateSecretStates(config) {
    for (const name of ['llm', 'tushare']) {
      const secret = config.secrets[name];
      const suffix = secret.tail ? ` · 末尾 ${secret.tail}` : '';
      const label = `${secret.present ? '已配置' : '未配置'} · ${secret.source || secret.state}${suffix}`;
      document.getElementById(`${name}-secret-state`).textContent = label;
    }
  }

  function renderFieldSources(sources) {
    form.querySelectorAll('[name]').forEach(input => {
      const meta = sources[input.name];
      if (!meta) return;
      const label = input.closest('label');
      if (!label) return;
      let note = label.querySelector('.settings-source-note');
      if (!note) {
        note = document.createElement('small');
        note.className = 'settings-source-note';
        label.append(note);
      }
      if (meta.override) {
        note.textContent = `当前由 ${meta.environment || '环境变量'} 覆盖；保存值不会立即生效`;
        note.classList.add('override');
      } else {
        note.textContent = `来源：${meta.source || 'default'}`;
        note.classList.remove('override');
      }
    });
  }

  async function loadSettings(force = false) {
    if (state.loaded && !force) return;
    try {
      const data = await request('/api/v1/settings');
      state.config = data;
      state.loaded = true;
      fillForm(data);
      await loadDataRefreshControls();
    } catch (error) {
      document.getElementById('settings-config-path').textContent = `设置不可用：${error.message}`;
      form.querySelectorAll('input, select, textarea, button').forEach(item => { item.disabled = true; });
      const entry = document.querySelector('.header-settings');
      entry.disabled = true;
      entry.title = '远程监听时设置中心不可用';
    }
  }

  function scheduleAutosave(delay = 750, message = '等待自动保存…') {
    if (!state.loaded || state.fillingForm) return;
    clearTimeout(state.autoSaveTimer);
    clearTimeout(state.retryTimer);
    state.editRevision += 1;
    state.saveQueued = state.saveInFlight;
    markDirty(message);
    state.autoSaveTimer = setTimeout(flushAutosave, delay);
  }

  function markCheckStale(kind) {
    const result = document.querySelector(`[data-check-result="${kind}"]`);
    if (!result || result.classList.contains('stale')) return;
    if (result.classList.contains('checking')) {
      result.dataset.stalePending = 'true';
      return;
    }
    if (!result.dataset.checked) return;
    result.classList.add('stale');
    const badge = result.querySelector('.check-stale');
    if (badge) badge.hidden = false;
  }

  function markDependentChecksStale(input) {
    const name = input.name || '';
    if (name.startsWith('llm.') || input.id === 'llm-secret') {
      markCheckStale('llm-models');
      markCheckStale('llm-web-search');
    }
    if (name === 'llm.timeout') markCheckStale('data-sources');
    if (name === 'data.root') {
      markCheckStale('storage');
      markCheckStale('data-sources');
      markCheckStale('lab');
    }
    if (['data.free_stockdb_url', 'data.free_stockdb_timeout',
      'data.free_stockdb_sdk_path', 'data.free_stockdb_online_enabled',
      'data.free_stockdb_online_url', 'data.free_stockdb_online_timeout',
      'data.akshare_enabled', 'data.tushare_enabled',
      'data.yfinance_enabled'].includes(name)) markCheckStale('data-sources');
    if (input.id === 'tushare-secret') {
      markCheckStale('tushare');
      markCheckStale('lab');
    }
    if (['lab.universe', 'lab.device'].includes(name)) markCheckStale('lab');
    if (name.startsWith('server.')) markCheckStale('server');
  }

  form.addEventListener('input', event => {
    const input = event.target;
    if (input.id === 'plaintext-confirm' || input.id === 'feishu-app-secret' ||
        input.id === 'weixin-verify-code') return;
    if (!input.name && !['llm-secret', 'tushare-secret'].includes(input.id)) return;
    markDependentChecksStale(input);
    if (input.id === 'llm-secret' || input.id === 'tushare-secret') {
      const name = input.id.replace('-secret', '');
      state.secretActions[name] = input.value ? 'replace' : 'keep';
      if (input.id === 'llm-secret') scheduleAutomaticModelCheck();
      markDirty('凭据填写完成后自动保存…');
      return;
    }
    if (input.name === 'llm.provider') syncReasoningEffortOptions(true);
    if (['llm.provider', 'llm.base_url'].includes(input.name) || input.id === 'llm-secret') {
      scheduleAutomaticModelCheck();
    }
    scheduleAutosave(input.type === 'checkbox' ? 0 : 750);
  });

  form.addEventListener('change', event => {
    const input = event.target;
    if (input.id === 'plaintext-confirm') {
      if (Object.values(state.secretActions).some(action => action !== 'keep')) {
        scheduleAutosave(0, '正在重试凭据保存…');
      }
      return;
    }
    if (input.id === 'feishu-app-secret' || input.id === 'weixin-verify-code') return;
    markDependentChecksStale(input);
    if (input.dataset.confirmCloud !== undefined && input.checked) {
      const confirmed = window.confirm('打开后，云端样本会直接发送给当前模型服务，不再逐次询问。是否允许自动发送？');
      if (!confirmed) {
        input.checked = false;
        return;
      }
    }
    if (input.name || ['llm-secret', 'tushare-secret'].includes(input.id)) scheduleAutosave(0);
  });

  form.addEventListener('focusout', event => {
    const input = event.target;
    if (input.id === 'feishu-app-secret' || input.id === 'weixin-verify-code') return;
    if (input.name || ['llm-secret', 'tushare-secret'].includes(input.id)) scheduleAutosave(0);
  });

  form.addEventListener('keydown', event => {
    if (event.key !== 'Enter' || !['llm-secret', 'tushare-secret'].includes(event.target.id)) return;
    event.preventDefault();
    event.target.blur();
  });

  function scheduleAutomaticModelCheck() {
    clearTimeout(state.modelCheckTimer);
    state.modelCheckTimer = setTimeout(() => {
      const result = document.querySelector('[data-check-result="llm-models"]');
      if (result?.dataset.checked) return;
      const provider = form.elements['llm.provider'].value;
      const base = form.elements['llm.base_url'].value.trim();
      const sameTarget = provider === state.config?.llm?.provider &&
        base.replace(/\/$/, '') === String(state.config?.llm?.base_url || '').replace(/\/$/, '');
      const configured = provider === 'openai-compatible'
        ? Boolean(base)
        : Boolean(document.getElementById('llm-secret').value ||
          (sameTarget && state.config?.secrets?.llm?.configured));
      const signature = `${provider}|${base}|${configured}`;
      if (configured && signature !== state.modelCheckSignature) {
        state.modelCheckSignature = signature;
        document.querySelector('[data-check="llm-models"]').click();
      }
    }, 500);
  }

  document.querySelectorAll('[data-clear-secret]').forEach(button => {
    button.addEventListener('click', () => {
      const name = button.dataset.clearSecret;
      state.secretActions[name] = 'clear';
      document.getElementById(`${name}-secret`).value = '';
      document.getElementById(`${name}-secret-state`).textContent = '正在显式清除…';
      markDependentChecksStale(document.getElementById(`${name}-secret`));
      scheduleAutosave(0, '正在清除凭据…');
    });
  });

  function documentPayload(includeSecrets = false) {
    const payload = {config_version: 1, llm: {}, data: {}, trade: {}, server: {},
      automation: structuredClone(state.config?.automation || {}),
      news: structuredClone(state.config?.news || {}),
      lab: structuredClone(state.config?.lab || {})};
    const handledLists = new Set();
    form.querySelectorAll('[name]').forEach(input => {
      if (input.dataset.listCheckbox !== undefined) {
        if (handledLists.has(input.name)) return;
        handledLists.add(input.name);
        const value = [...form.querySelectorAll(`[name="${input.name}"][data-list-checkbox]:checked`)]
          .map(item => Number(item.value));
        setPath(payload, input.name, value);
        return;
      }
      let value = input.value;
      if (input.type === 'checkbox') value = input.checked;
      else if (input.dataset.valueType === 'list') value = parseSymbols(value);
      else if (input.type === 'number') value = value === '' ? null : Number(value);
      setPath(payload, input.name, value);
    });
    if (includeSecrets) {
      payload.secrets = {};
      for (const name of ['llm', 'tushare']) {
        const action = state.secretActions[name];
        payload.secrets[name] = {action};
        if (action === 'replace') payload.secrets[name].value = document.getElementById(`${name}-secret`).value;
      }
      payload.allow_plaintext_secrets = document.getElementById('plaintext-confirm').checked;
    }
    return payload;
  }

  function diagnosticGroups(kind, data) {
    const details = data.details || {};
    if (kind === 'data-sources') {
      const sources = Object.entries(details.sources || {}).map(([name, item]) => ({
        label: name, value: item.message || '无返回信息', status: item.status,
      }));
      const circuits = Object.entries(details.circuits || {})
        .filter(([, item]) => item.state !== 'closed')
        .map(([name, item]) => ({label: name, value: item.state, status: 'warning'}));
      const master = details.security_master;
      const coverage = master?.coverage?.map(item =>
        `${item.market}/${item.asset_type} ${item.count}`).join('、');
      const masterRows = master ? [{
        label: '证券主数据',
        value: `${master.record_count || 0} 条${coverage ? ` · ${coverage}` : ''}`,
        status: master.status,
      }] : [];
      const governance = master?.governance || {};
      if (master && governance.phase) masterRows.push(
        {label: '身份治理阶段', value: governance.phase, status: 'success'},
        {label: 'StockDB 确认', value: governance.stockdb_confirmation || '待检查', status: governance.stockdb_confirmation === 'confirmed' ? 'success' : 'warning'},
        {label: '交叉验证', value: governance.cross_validation || '待检查', status: governance.cross_validation === 'confirmed' ? 'success' : 'warning'},
        {label: '歧义 / 冲突', value: `${governance.ambiguous_aliases || 0} / ${governance.conflicts || 0}`, status: (governance.ambiguous_aliases || governance.conflicts) ? 'warning' : 'success'},
        {label: '历史 alias 缺口', value: governance.historical_alias_gaps || 0, status: governance.historical_alias_gaps ? 'warning' : 'success'},
        {label: 'Provider 覆盖', value: (governance.provider_coverage || []).map(item => `${item.provider}/${item.verification_status} ${item.count}`).join('、') || '暂无已确认 alias'},
        {label: '诊断码', value: (governance.diagnostic_codes || []).join('、') || '无'},
      );
      const proxies = Object.entries(details.proxies || {}).map(([name, value]) => ({
        label: name, value,
      }));
      return [
        {title: '依赖与端点', rows: sources},
        {title: '熔断状态', rows: circuits.length ? circuits : [{label: '全部通道', value: 'closed'}]},
        {title: '证券主数据', rows: masterRows},
        {title: '代理', rows: proxies.length ? proxies : [{label: '环境代理', value: '未配置'}]},
      ].filter(group => group.rows.length);
    }
    if (kind === 'lab') {
      return [{title: '环境检查', rows: Object.entries(details.checks || {}).map(([name, item]) => ({
        label: name, value: item.message || '无返回信息', status: item.status,
      }))}];
    }
    const rows = [];
    if (Array.isArray(details.models)) rows.push({label: '模型数量', value: details.models.length});
    if (details.endpoint) rows.push({label: '检测端点', value: details.endpoint});
    if (details.path) rows.push({label: '目录', value: details.path});
    if (details.free_bytes != null) rows.push({
      label: '剩余空间', value: `${(details.free_bytes / (1024 ** 3)).toFixed(1)} GB`,
    });
    if (details.host) rows.push({label: '监听地址', value: `${details.host}:${details.port}`});
    if (details.category) rows.push({label: '分类', value: details.category});
    if (details.supported != null) rows.push({
      label: '联网搜索', value: details.supported ? '支持' : '未确认支持',
      status: details.supported ? 'success' : 'warning',
    });
    if (Array.isArray(details.sources) && details.sources.length) {
      rows.push(...details.sources.slice(0, 5).map(item => ({
        label: item.title || '来源', value: item.url || '',
      })));
    }
    return rows.length ? [{title: '检测详情', rows}] : [];
  }

  function diagnosticIssueCount(kind, data) {
    if (kind === 'data-sources') {
      const sourceIssues = Object.values(data.details?.sources || {})
        .filter(item => item.status !== 'success').length;
      const circuitIssues = Object.values(data.details?.circuits || {})
        .filter(item => item.state !== 'closed').length;
      return sourceIssues + circuitIssues;
    }
    if (kind === 'lab') {
      return Object.values(data.details?.checks || {})
        .filter(item => item.status !== 'success').length;
    }
    return data.status === 'success' ? 0 : 1;
  }

  function renderSavedChecks(checks) {
    Object.entries(checks).forEach(([kind, data]) => renderCheck(kind, data));
  }

  function formatCheckTimestamp(value) {
    if (!value) return '';
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) return '';
    return date.toLocaleString('zh-CN', {hour12: false});
  }

  function renderCheck(kind, data) {
    const el = document.querySelector(`[data-check-result="${kind}"]`);
    if (!el) return;
    const status = data.status || 'warning';
    const time = formatCheckTimestamp(data.checked_at);
    const stale = Boolean(data.stale || el.dataset.stalePending === 'true');
    const issueCount = diagnosticIssueCount(kind, data);
    const meta = [
      state.editRevision > state.savedRevision
        ? '检测草稿（未保存）' : `检测已保存 revision ${state.persistedRevision}`,
      data.diagnostic_id ? `诊断 ${data.diagnostic_id}` : '',
      data.latency_ms != null ? `${data.latency_ms}ms` : '',
      time,
      issueCount ? `${issueCount} 项需关注` : '未发现异常',
    ].filter(Boolean);
    const groups = diagnosticGroups(kind, data);
    const detailHtml = groups.length ? `<details class="check-details"${status === 'success' ? '' : ' open'}>
      <summary>查看检测详情</summary>
      <div class="check-detail-groups">${groups.map(group => `<section>
        <h5>${html(group.title)}</h5>
        ${group.rows.map(row => `<div class="check-detail-row ${html(row.status || '')}">
          <strong>${html(row.label)}</strong><span>${html(row.value)}</span>
        </div>`).join('')}
      </section>`).join('')}</div>
    </details>` : '';
    el.className = `check-result ${status}${stale ? ' stale' : ''}`;
    el.dataset.checked = 'true';
    delete el.dataset.stalePending;
    el.innerHTML = `<div class="check-summary">
      <div><strong>${html(data.message || '检测完成')}</strong><span class="check-stale"${stale ? '' : ' hidden'}>配置已修改，需要重新检测</span></div>
      <small>${meta.map(html).join(' · ')}</small>
    </div>${detailHtml}`;
    if (kind === 'llm-models' && Array.isArray(data.details?.models)) {
      const list = document.getElementById('settings-model-list');
      list.innerHTML = data.details.models.map(model => `<option value="${html(model)}"></option>`).join('');
      const selected = form.elements['llm.model'].value;
      document.getElementById('model-hint').textContent = data.details.models.includes(selected)
        ? '当前模型由 API 列表返回'
        : '当前手填值不在返回列表中，仍会保留';
    }
  }

  function renderDiagnosticTask(kind, task) {
    const el = document.querySelector(`[data-check-result="${kind}"]`);
    if (!el) return;
    const active = ['queued', 'running', 'cancelling'].includes(task.status);
    const action = active && task.links?.cancel
      ? `<button type="button" class="check-task-cancel" data-settings-task-cancel="${html(task.id)}" aria-label="取消当前检测">取消检测</button>`
      : '';
    el.className = `check-result checking ${task.status === 'cancelling' ? 'stale' : ''}`;
    el.removeAttribute('data-checked');
    el.innerHTML = `<div class="check-summary"><div><strong>${html(task.phase || 'LLM 检测任务')}</strong><span>${html(task.detail || task.status)}</span></div><small>任务 ${html(String(task.id || '').slice(-8))} · ${Number(task.progress || 0)}%</small></div>${action}`;
  }

  function delay(ms) {
    return new Promise(resolve => window.setTimeout(resolve, ms));
  }

  async function watchDiagnosticTask(kind, initial) {
    let task = initial;
    state.diagnosticTasks[kind] = task.id;
    while (state.diagnosticTasks[kind] === task.id) {
      renderDiagnosticTask(kind, task);
      if (!['queued', 'running', 'cancelling'].includes(task.status)) break;
      await delay(500);
      try {
        task = await request(task.links?.self || `/api/v1/jobs/${encodeURIComponent(task.id)}`);
      } catch (error) {
        renderCheck(kind, {status: 'warning', stale: true, message: `任务状态暂不可读取：${error.message}`});
        return;
      }
    }
    if (state.diagnosticTasks[kind] !== task.id) return;
    delete state.diagnosticTasks[kind];
    if (task.status === 'completed') {
      try {
        // Diagnostic results live only in the dedicated redacted settings
        // projection; they never become a general-purpose job artifact.
        const settings = await request('/api/v1/settings');
        const result = settings.checks?.[kind];
        if (result) {
          renderCheck(kind, result);
          return;
        }
      } catch (error) {
        renderCheck(kind, {
          status: 'warning', stale: true,
          message: `检测完成，但结果暂不可读取：${error.message}`,
        });
        return;
      }
    }
    renderCheck(kind, {
      status: 'warning', stale: true,
      message: task.status === 'cancelled' || task.status === 'interrupted'
        ? '检测已取消；请按当前设置重新检测。'
        : `检测未完成：${task.detail || task.phase || task.status}`,
    });
  }

  async function watchRuntimeApply(initial) {
    let task = initial;
    const identity = `${state.persistedRevision}:${state.latestGeneration}:${task.id || ''}`;
    state.applyTask = identity;
    while (['queued', 'running', 'cancelling'].includes(task.status)) {
      await delay(500);
      try {
        task = await request(task.links?.self || `/api/v1/jobs/${encodeURIComponent(task.id)}`);
      } catch (error) {
        if (state.applyTask === identity) setSaveState('error', `已保存 revision ${state.persistedRevision}；应用状态暂不可读取：${error.message}`);
        return;
      }
    }
    if (state.applyTask !== identity) return;
    if (task.status !== 'completed' || !task.result) {
      setSaveState('error', `已保存 revision ${state.persistedRevision}；后台应用${task.status === 'cancelled' ? '已取消' : '失败'}`);
      return;
    }
    const applied = task.result.result || task.result;
    if (applied.runtime) renderRuntime(applied.runtime);
  }

  document.getElementById('settings-retry-apply')?.addEventListener('click', async event => {
    const button = event.currentTarget;
    button.disabled = true;
    setSaveState('saving', `正在重试应用 revision ${state.persistedRevision}…`);
    try {
      const result = await request('/api/v1/settings/apply', {method: 'POST'});
      state.latestGeneration = Number(result.generation || state.latestGeneration);
      renderRuntime(result.runtime);
      if (result.runtime_apply?.id) void watchRuntimeApply(result.runtime_apply);
      setSaveState('saved', `revision ${state.persistedRevision} 已重新排队；等待组件确认`);
    } catch (error) {
      setSaveState('error', `revision ${state.persistedRevision} 重试应用失败：${error.message}`);
    } finally {
      button.disabled = false;
    }
  });

  document.querySelectorAll('.check-button').forEach(button => {
    button.addEventListener('click', async () => {
      const kind = button.dataset.check;
      const result = document.querySelector(`[data-check-result="${kind}"]`);
      if (result) {
        result.className = 'check-result checking';
        result.removeAttribute('data-checked');
        delete result.dataset.stalePending;
        result.innerHTML = '<div class="check-summary"><strong>检测中…</strong><small>正在等待服务返回</small></div>';
      }
      button.disabled = true;
      try {
        const data = await request(`/api/v1/settings/check/${kind}`, {
          method: 'POST', body: {...documentPayload(false), secrets: documentPayload(true).secrets},
        });
        if (data.type === 'settings.diagnostic' && data.id) {
          renderDiagnosticTask(kind, data);
          void watchDiagnosticTask(kind, data);
        } else {
          renderCheck(kind, data);
        }
      } catch (error) {
        renderCheck(kind, {status: 'error', message: error.message});
      } finally {
        button.disabled = false;
      }
    });
  });

  document.addEventListener('click', async event => {
    const button = event.target.closest('[data-settings-task-cancel]');
    if (!button) return;
    const taskId = button.dataset.settingsTaskCancel;
    const kind = Object.entries(state.diagnosticTasks)
      .find(([, id]) => id === taskId)?.[0];
    if (!taskId || !kind) return;
    button.disabled = true;
    try {
      const task = await request(`/api/v1/jobs/${encodeURIComponent(taskId)}/cancel`, {method: 'POST'});
      void watchDiagnosticTask(kind, task);
    } catch (error) {
      renderCheck(kind, {status: 'warning', stale: true, message: error.message});
    }
  });

  function markInvalidFields() {
    let count = 0;
    form.querySelectorAll('[name]').forEach(input => {
      const invalid = !input.checkValidity();
      input.toggleAttribute('aria-invalid', invalid);
      count += Number(invalid);
    });
    for (const name of ['lab.horizons', 'lab.weekly_days']) {
      const inputs = [...form.querySelectorAll(`[name="${name}"][data-list-checkbox]`)];
      const fieldset = inputs[0]?.closest('fieldset');
      const invalid = inputs.length > 0 && !inputs.some(input => input.checked);
      fieldset?.toggleAttribute('aria-invalid', invalid);
      count += Number(invalid);
    }
    return count;
  }

  async function flushAutosave() {
    clearTimeout(state.autoSaveTimer);
    if (!state.loaded) return;
    if (state.saveInFlight) {
      state.saveQueued = true;
      return;
    }
    const invalidCount = markInvalidFields();
    if (invalidCount) {
      setSaveState('dirty', `等待补全 ${invalidCount} 个字段…`);
      return;
    }

    const plain = documentPayload(false);
    const fingerprint = JSON.stringify(plain);
    const hasSecretChange = Object.values(state.secretActions).some(action => action !== 'keep');
    if (fingerprint === state.lastSavedFingerprint && !hasSecretChange) {
      state.savedRevision = state.editRevision;
      setSaveState('saved', '已是最新设置');
      return;
    }

    const revision = state.editRevision;
    const secretPayload = documentPayload(true);
    state.saveInFlight = true;
    state.saveQueued = false;
    setSaveState('saving', '正在校验…');
    try {
      const validated = await request('/api/v1/settings/validate', {method: 'POST', body: plain});
      setSaveState('saving', '正在保存设置…');
      const update = {...validated.normalized, secrets: secretPayload.secrets,
        allow_plaintext_secrets: secretPayload.allow_plaintext_secrets};
      const result = await request('/api/v1/settings', {method: 'PUT', body: update});

      if (result.settings) {
        state.config = {...result.settings, runtime: result.runtime};
        updateSecretStates(state.config);
      }
      for (const name of ['llm', 'tushare']) {
        const sent = secretPayload.secrets[name];
        const input = document.getElementById(`${name}-secret`);
        const unchangedReplace = sent.action === 'replace' &&
          state.secretActions[name] === 'replace' && input.value === sent.value;
        const unchangedClear = sent.action === 'clear' && state.secretActions[name] === 'clear';
        if (unchangedReplace || unchangedClear) {
          input.value = '';
          state.secretActions[name] = 'keep';
        }
      }

      state.lastSavedFingerprint = JSON.stringify(validated.normalized);
      state.savedRevision = revision;
      state.persistedRevision = Number(result.persisted_revision ?? result.config_revision ?? state.persistedRevision);
      state.latestGeneration = Number(result.generation ?? result.runtime?.latest_generation ?? state.latestGeneration);
      if (state.editRevision === revision && result.settings) fillForm(state.config);
      renderRuntime(result.runtime);
      if (result.runtime_apply?.id) void watchRuntimeApply(result.runtime_apply);
      const suffix = (result.restart_required || []).length
        ? `；${result.restart_required.join(' / ')} 重启后生效` : '';
      const degraded = Object.entries(result.apply_status || {})
        .filter(([, value]) => value?.status === 'degraded')
        .map(([name]) => name === 'automation' ? '自动化运行态异常' : '研究 Worker 运行态异常');
      const cancellation = result.llm_cancellation?.scopes?.length
        ? ['旧模型任务已逻辑取消；已发出的网络请求会在 timeout 内结束'] : [];
      const allWarnings = [...(result.warnings || []), ...degraded, ...cancellation];
      const warnings = allWarnings.length ? `；${allWarnings.join('；')}` : '';
      const time = new Date().toLocaleTimeString('zh-CN', {hour12: false, hour: '2-digit', minute: '2-digit'});
      setSaveState('saved', `已保存 revision ${state.persistedRevision} · ${time}；应用状态待组件确认${suffix}${warnings}`);
      document.dispatchEvent(new CustomEvent('quantmaster:settings-persisted', {detail: result}));
      if (document.querySelector('[data-settings-section="automation"].active') ||
          document.querySelector('[data-settings-section="lab"].active')) loadAutomationOverview();
    } catch (error) {
      setSaveState('error', `自动保存失败：${error.message}`);
      if (mounted && error.status === 423) {
        state.retryTimer = setTimeout(flushAutosave, 1200);
      }
    } finally {
      state.saveInFlight = false;
      if (mounted && (state.saveQueued || state.editRevision > revision)) {
        state.autoSaveTimer = setTimeout(flushAutosave, 0);
      }
    }
  }

  form.addEventListener('submit', event => {
    event.preventDefault();
    if (form.reportValidity()) flushAutosave();
  });

  document.addEventListener('visibilitychange', () => {
    if (document.visibilityState === 'hidden' && state.editRevision > state.savedRevision) flushAutosave();
  });

  window.addEventListener('beforeunload', event => {
    if (!state.saveInFlight && state.editRevision <= state.savedRevision) return;
    event.preventDefault();
    event.returnValue = '';
  });

  function setChannelState(channel, configured, detail = '') {
    const el = document.getElementById(`${channel}-channel-state`);
    el.classList.toggle('configured', configured);
    el.textContent = detail || (configured ? '已配置' : '未配置');
  }

  async function loadAutomationOverview(generation = lifecycleGeneration) {
    if (!state.loaded || !mounted || generation !== lifecycleGeneration) return;
    const runtimeState = document.getElementById('automation-runtime-state');
    try {
      const data = await request('/api/v1/automation/overview');
      if (!mounted || generation !== lifecycleGeneration) return;
      const runtimeLabels = {running: '运行中', standby: '等待调度租约', disabled: '已停用', degraded: '运行异常'};
      runtimeState.textContent = runtimeLabels[data.runtime] || data.runtime;
      runtimeState.className = `state-pill ${data.runtime === 'running' ? 'buy' : ''}`;
      for (const channel of ['weixin', 'feishu']) {
        const account = (data.bot_accounts || []).find(item => item.channel === channel);
        const configured = Boolean(data.channels?.[channel]?.configured);
        const feishuStates = {
          not_configured: '未配置', invalid_credentials: '凭据无效',
          tls_error: 'TLS 失败', network_error: '网络不可达',
          rate_limited: '连接受限', connected: '已连接', connecting: '连接中',
        };
        setChannelState(channel, configured,
          channel === 'feishu' ? (feishuStates[account?.status] || account?.status || '')
            : (account?.status || ''));
      }
      const feishu = (data.bot_accounts || []).find(item => item.channel === 'feishu');
      const appId = document.getElementById('feishu-app-id');
      if (feishu && document.activeElement !== appId) appId.value = feishu.account_id || '';
      document.getElementById('automation-enable-connect').hidden = !(feishu && !data.enabled);
      document.getElementById('feishu-remove').disabled = !feishu;
      const runtime = await request('/api/v1/settings/runtime');
      if (!mounted || generation !== lifecycleGeneration) return;
      renderRuntime(runtime);
    } catch (error) {
      if (!mounted || generation !== lifecycleGeneration) return;
      runtimeState.textContent = `状态不可用：${error.message}`;
    }
  }

  function scheduleWeixinPoll(sessionId, delay = 700, generation = lifecycleGeneration) {
    if (!mounted || generation !== lifecycleGeneration || !sessionId) return;
    clearTimeout(state.weixinLoginTimer);
    state.weixinLoginId = String(sessionId);
    state.weixinLoginTimer = setTimeout(() => pollWeixinLogin(sessionId, generation), delay);
  }

  function renderWeixinLoginSession() {
    if (!state.weixinLoginId) return;
    const image = document.getElementById('weixin-login-qr');
    image.src = state.weixinLoginQr;
    image.hidden = !image.src;
    document.getElementById('weixin-login-panel').hidden = false;
    document.getElementById('weixin-login-status').textContent = '请使用微信扫码并确认';
    document.getElementById('weixin-login-start').disabled = true;
  }

  async function pollWeixinLogin(sessionId, generation = lifecycleGeneration) {
    if (!mounted || generation !== lifecycleGeneration || state.weixinLoginId !== String(sessionId)) return;
    const status = document.getElementById('weixin-login-status');
    const verifyCode = document.getElementById('weixin-verify-code').value.trim();
    try {
      const data = await request(
        `/api/v1/automation/channels/weixin/login/${encodeURIComponent(sessionId)}` +
        `?verify_code=${encodeURIComponent(verifyCode)}`
      );
      if (!mounted || generation !== lifecycleGeneration || state.weixinLoginId !== String(sessionId)) return;
      const labels = {
        wait: '等待扫码确认…', scanned: '已扫码，请在微信中确认',
        confirmed: '接入成功，请先给机器人发一条消息', expired: '二维码已失效，请重新生成',
      };
      status.textContent = labels[data.status] || `微信状态：${data.status}`;
      if (data.status === 'confirmed') {
        clearTimeout(state.weixinLoginTimer);
        state.weixinLoginTimer = null;
        state.weixinLoginId = '';
        state.weixinLoginQr = '';
        document.getElementById('weixin-login-start').disabled = false;
        await loadAutomationOverview(generation);
      } else if (data.status === 'expired') {
        state.weixinLoginId = '';
        state.weixinLoginQr = '';
        document.getElementById('weixin-login-start').disabled = false;
      } else {
        scheduleWeixinPoll(sessionId, 700, generation);
      }
    } catch (error) {
      if (!mounted || generation !== lifecycleGeneration || state.weixinLoginId !== String(sessionId)) return;
      state.weixinLoginId = '';
      state.weixinLoginQr = '';
      status.textContent = `登录状态读取失败：${error.message}`;
      document.getElementById('weixin-login-start').disabled = false;
    }
  }

  document.getElementById('weixin-login-start').addEventListener('click', async event => {
    if (state.weixinLoginCreatePending) return;
    const button = event.currentTarget;
    const panel = document.getElementById('weixin-login-panel');
    const status = document.getElementById('weixin-login-status');
    clearTimeout(state.weixinLoginTimer);
    const requestSequence = ++state.weixinLoginCreateSequence;
    state.weixinLoginCreatePending = true;
    state.weixinLoginId = '';
    state.weixinLoginQr = '';
    button.disabled = true;
    status.textContent = '正在申请二维码…';
    panel.hidden = false;
    try {
      const data = await request('/api/v1/automation/channels/weixin/login', {method: 'POST'});
      if (requestSequence !== state.weixinLoginCreateSequence) return;
      state.weixinLoginCreatePending = false;
      state.weixinLoginId = String(data.session_id || '');
      state.weixinLoginQr = data.qrcode_svg || data.qrcode_url || '';
      if (!state.weixinLoginId) throw new Error('微信登录没有返回可跟踪的会话');
      if (!mounted) return;
      renderWeixinLoginSession();
      scheduleWeixinPoll(state.weixinLoginId);
    } catch (error) {
      if (requestSequence !== state.weixinLoginCreateSequence) return;
      state.weixinLoginCreatePending = false;
      state.weixinLoginId = '';
      state.weixinLoginQr = '';
      if (!mounted) return;
      status.textContent = `二维码生成失败：${error.message}`;
      button.disabled = false;
    }
  });

  document.getElementById('feishu-connect').addEventListener('click', async event => {
    const button = event.currentTarget;
    const appId = document.getElementById('feishu-app-id').value.trim();
    const secretInput = document.getElementById('feishu-app-secret');
    const status = document.getElementById('feishu-connect-status');
    if (appId.length < 3 || !secretInput.value.trim()) {
      status.textContent = '请完整填写 App ID 与 App Secret。';
      return;
    }
    button.disabled = true;
    status.textContent = '正在验证凭据并启动长连接…';
    try {
      const data = await request('/api/v1/automation/channels/feishu/config', {
        method: 'POST', body: {app_id: appId, app_secret: secretInput.value},
      });
      secretInput.value = '';
      status.textContent = data.runtime_status === 'disabled'
        ? '凭据验证通过并已安全保存；自动化尚未启用。'
        : data.verification?.message || '飞书凭据已安全保存，正在建立长连接。';
      await loadAutomationOverview();
      await checkFeishu();
    } catch (error) {
      status.textContent = `飞书接入失败：${error.message}`;
    } finally {
      button.disabled = false;
    }
  });

  const feishuStageLabels = {
    credential: '应用凭据', runtime: '自动化运行时', websocket: '长连接',
    event: '消息事件', binding: '会话绑定',
  };

  function renderFeishuDiagnostic(data) {
    const root = document.getElementById('feishu-diagnostic');
    root.hidden = false;
    root.innerHTML = Object.entries(data.stages || {}).map(([name, stage]) => `
      <div class="channel-diagnostic-item">
        <strong>${html(feishuStageLabels[name] || name)}</strong>
        <span class="${html(stage.status)}">${html({success: '通过', warning: '待处理', error: '失败'}[stage.status] || stage.status)}</span>
        <span>${html(stage.message)}</span>
      </div>`).join('');
  }

  async function checkFeishu() {
    const button = document.getElementById('feishu-check');
    const status = document.getElementById('feishu-connect-status');
    button.disabled = true;
    status.textContent = '正在逐项检测飞书接入…';
    try {
      const data = await request('/api/v1/automation/channels/feishu/check', {method: 'POST'});
      renderFeishuDiagnostic(data);
      status.textContent = data.status === 'success'
        ? '飞书接入链路全部通过。'
        : '检测完成；请按下方未通过阶段逐项处理。';
      await loadAutomationOverview();
      return data;
    } catch (error) {
      status.textContent = `飞书检测失败：${error.message}`;
      return null;
    } finally {
      button.disabled = false;
    }
  }

  document.getElementById('feishu-check').addEventListener('click', checkFeishu);

  document.getElementById('feishu-remove').addEventListener('click', async event => {
    if (!window.confirm('移除飞书接入？凭据会从系统凭据库删除，已有会话将标记为需要重新绑定。')) return;
    event.currentTarget.disabled = true;
    const status = document.getElementById('feishu-connect-status');
    try {
      const data = await request('/api/v1/automation/channels/feishu/config', {method: 'DELETE'});
      document.getElementById('feishu-app-id').value = '';
      document.getElementById('feishu-diagnostic').hidden = true;
      status.textContent = (data.warnings || []).length
        ? `接入已移除；${data.warnings.join('；')}` : '飞书凭据和接入记录已移除。';
      await loadAutomationOverview();
    } catch (error) {
      status.textContent = `移除失败：${error.message}`;
    } finally {
      event.currentTarget.disabled = !document.getElementById('feishu-app-id').value.trim();
    }
  });

  document.getElementById('automation-enable-connect').addEventListener('click', async event => {
    const toggle = form.elements['automation.enabled'];
    toggle.checked = true;
    event.currentTarget.disabled = true;
    scheduleAutosave(0, '正在启用自动化并连接飞书…');
    await flushAutosave();
    event.currentTarget.disabled = false;
    await checkFeishu();
  });

  async function loadSnapshots() {
    const container = document.getElementById('snapshot-list');
    try {
      const data = await request('/api/v1/settings/snapshots');
      if (!data.snapshots.length) {
        container.innerHTML = '<div class="msg">尚无快照。成功保存设置后会自动创建。</div>';
        return;
      }
      container.innerHTML = data.snapshots.map(item => `<div class="snapshot-row">
        <div><strong>${html(item.name || ({automatic: '自动保存', initial: '初始配置'}[item.kind] || item.kind))}</strong>
        <small>${html(new Date(item.created_at).toLocaleString('zh-CN'))} · ${html(item.kind)}</small></div>
        <div class="snapshot-actions"><button class="ghost" type="button" data-snapshot-diff="${html(item.id)}">查看差异</button>
        ${item.kind === 'manual' ? `<button class="ghost danger" type="button" data-snapshot-delete="${html(item.id)}">删除</button>` : ''}</div></div>`).join('');
    } catch (error) {
      container.innerHTML = `<div class="err">${html(error.message)}</div>`;
    }
  }

  document.getElementById('snapshot-form').addEventListener('submit', async event => {
    event.preventDefault();
    const input = event.target.elements.name;
    try {
      await request('/api/v1/settings/snapshots', {method: 'POST', body: {name: input.value}});
      input.value = '';
      await loadSnapshots();
    } catch (error) { input.setCustomValidity(error.message); input.reportValidity(); input.setCustomValidity(''); }
  });

  document.getElementById('snapshot-list').addEventListener('click', async event => {
    const diffButton = event.target.closest('[data-snapshot-diff]');
    const deleteButton = event.target.closest('[data-snapshot-delete]');
    if (deleteButton) {
      if (!window.confirm('永久删除这个手动快照？')) return;
      await request(`/api/v1/settings/snapshots/${deleteButton.dataset.snapshotDelete}`, {method: 'DELETE'});
      await loadSnapshots();
      return;
    }
    if (!diffButton) return;
    const id = diffButton.dataset.snapshotDiff;
    const panel = document.getElementById('snapshot-diff');
    try {
      const data = await request(`/api/v1/settings/snapshots/${id}/diff`);
      panel.hidden = false;
      panel.innerHTML = `<div class="group-heading"><div><h4>回滚前差异</h4><p>${data.diff.length} 个字段会改变，凭据与业务数据不会参与。</p></div>
        <button class="primary" type="button" data-snapshot-rollback="${html(id)}">确认回滚</button></div>
        <div class="table-scroll"><table><thead><tr><th>字段</th><th>当前</th><th>目标</th></tr></thead><tbody>
        ${data.diff.map(row => `<tr><td>${html(row.field)}</td><td>${html(JSON.stringify(row.current))}</td><td>${html(JSON.stringify(row.target))}</td></tr>`).join('') || '<tr><td colspan="3">无差异</td></tr>'}
        </tbody></table></div>`;
    } catch (error) { panel.hidden = false; panel.innerHTML = `<div class="err">${html(error.message)}</div>`; }
  });

  document.getElementById('snapshot-diff').addEventListener('click', async event => {
    const button = event.target.closest('[data-snapshot-rollback]');
    if (!button) return;
    button.disabled = true;
    try {
      await request(`/api/v1/settings/snapshots/${button.dataset.snapshotRollback}/rollback`, {method: 'POST'});
      state.loaded = false;
      await loadSettings(true);
      await loadSnapshots();
      document.getElementById('snapshot-diff').hidden = true;
    } catch (error) { button.textContent = `回滚失败：${error.message}`; button.disabled = false; }
  });

  function dataRefreshPayload() {
    const scope = document.getElementById('data-refresh-scope').value;
    return {
      scope,
      universe: scope === 'universe' ? document.getElementById('data-refresh-universe').value : '',
      start: scope === 'universe' ? document.getElementById('data-refresh-start').value : '',
    };
  }

  function resetDataRefreshPreview() {
    state.dataRefreshPreview = null;
    document.getElementById('data-refresh-confirm').hidden = true;
    const scope = document.getElementById('data-refresh-scope').value;
    document.getElementById('data-refresh-universe').disabled = scope !== 'universe';
    document.getElementById('data-refresh-start').disabled = scope !== 'universe';
  }

  async function loadDataRefreshControls() {
    const start = document.getElementById('data-refresh-start');
    if (!start.value) start.value = state.config?.lab?.start || '';
    try {
      const data = await request('/api/v1/settings/universes');
      const select = document.getElementById('data-refresh-universe');
      const current = select.value || state.config?.lab?.universe || '';
      select.innerHTML = (data.universes || []).map(item =>
        `<option value="${html(item.name)}">${html(item.name)}${item.count == null ? '' : ` · ${item.count} 只`}</option>`
      ).join('');
      if ([...select.options].some(option => option.value === current)) select.value = current;
    } catch (_) {
      // 候选管理区域仍可独立报告加载错误；增量同步保留市场页与已缓存范围。
    }
    resetDataRefreshPreview();
    try {
      const latest = await request('/api/v1/jobs?domain=data&limit=1');
      const job = latest.items?.[0];
      if (job) {
        renderDataRefresh(job);
        if (mounted && ['running', 'cancelling'].includes(job.status)) pollDataRefresh(job.id);
      }
    } catch (_) { /* 首次使用时没有任务是正常状态。 */ }
  }

  function renderDataRefresh(task) {
    const root = document.getElementById('data-refresh-progress');
    root.hidden = false;
    root.style.setProperty('--data-refresh-progress', (task.progress || 0) / 100);
    const labels = {
      running: '增量同步中', cancelling: '正在完成当前标的', cancelled: '已取消，可继续',
      interrupted: '服务重启中断，可继续', completed: '增量同步完成',
      completed_with_errors: '刷新完成，部分标的失败',
    };
    const current = task.current_symbol ? ` · ${task.current_symbol}` : '';
    root.querySelector('[data-refresh-phase]').textContent =
      `${labels[task.status] || task.status} · ${task.next_index}/${task.total}${current}`;
    root.querySelector('[data-refresh-percent]').textContent = `${task.progress || 0}%`;
    const failures = task.failures || [];
    root.querySelector('[data-refresh-failures]').textContent = failures.length
      ? `${task.failed} 个失败：${failures.slice(-3).map(item => `${item.symbol} ${item.error}`).join('；')}`
      : `${task.succeeded || 0} 个标的已成功同步`;
    const cancel = document.getElementById('data-refresh-cancel');
    cancel.hidden = !['running', 'cancelling'].includes(task.status);
    cancel.disabled = task.status === 'cancelling';
    cancel.dataset.jobId = task.id;
    const resume = document.getElementById('data-refresh-resume');
    resume.hidden = !['cancelled', 'interrupted', 'completed_with_errors'].includes(task.status);
    resume.dataset.jobId = task.id;
    state.dataRefreshId = ['running', 'cancelling'].includes(task.status) ? String(task.id || '') : '';
    const runtimeKey = `persistent:health:refresh:${task.id}`;
    if (['running', 'cancelling'].includes(task.status)) {
      window.QuantMasterRunInfo.add('info', '数据同步', '行情尾部正在增量同步', {
        detail:`进度 ${task.progress || 0}%，当前 ${task.current_symbol || '准备中'}`,
        action:'任务会在后台继续，可正常浏览其他页面。',
        key:runtimeKey, scope:'health', persistent:true,
        revision:`${task.status}:${task.progress || 0}:${task.current_symbol || ''}`,
      });
    } else if (['interrupted', 'completed_with_errors'].includes(task.status)) {
      window.QuantMasterRunInfo.add('warning', '数据刷新', '最近的数据刷新未完整完成', {
        detail:`${task.failed || failures.length} 个标的失败或任务被中断。`,
        action:'在设置中心查看失败项并重试。',
        key:runtimeKey, scope:'health', persistent:true,
        revision:`${task.status}:${task.failed || failures.length}`,
      });
    } else {
      window.QuantMasterRunInfo.resolve(runtimeKey);
    }
  }

  async function pollDataRefresh(id, generation = lifecycleGeneration) {
    if (!mounted || generation !== lifecycleGeneration) return;
    clearTimeout(state.dataRefreshTimer);
    try {
      const task = await request(`/api/v1/jobs/${encodeURIComponent(id)}`);
      if (!mounted || generation !== lifecycleGeneration) return;
      renderDataRefresh(task);
      if (['running', 'cancelling'].includes(task.status)) {
        state.dataRefreshTimer = setTimeout(() => pollDataRefresh(id, generation), 800);
      }
    } catch (error) {
      if (!mounted || generation !== lifecycleGeneration) return;
      const root = document.getElementById('data-refresh-progress');
      root.hidden = false;
      root.querySelector('[data-refresh-phase]').textContent = error.message;
    }
  }

  document.getElementById('data-refresh-scope').addEventListener('change', resetDataRefreshPreview);
  document.getElementById('data-refresh-universe').addEventListener('change', resetDataRefreshPreview);
  document.getElementById('data-refresh-start').addEventListener('change', resetDataRefreshPreview);

  document.getElementById('data-refresh-preview').addEventListener('click', async event => {
    event.target.disabled = true;
    try {
      const preview = await request('/api/v1/data/refresh/preview', {
        method: 'POST', body: dataRefreshPayload(),
      });
      state.dataRefreshPreview = preview;
      const warning = preview.unhealthy_sources?.length
        ? `；当前冷却：${preview.unhealthy_sources.join('、')}` : '';
      document.querySelector('[data-refresh-preview-text]').textContent =
        `${preview.message}（${preview.start} 至 ${preview.end}）${warning}`;
      document.getElementById('data-refresh-confirm').hidden = false;
    } catch (error) {
      state.dataRefreshPreview = null;
      document.querySelector('[data-refresh-preview-text]').textContent = error.message;
      document.getElementById('data-refresh-confirm').hidden = false;
      document.getElementById('data-refresh-start-button').disabled = true;
    } finally {
      event.target.disabled = false;
      if (state.dataRefreshPreview) document.getElementById('data-refresh-start-button').disabled = false;
    }
  });

  document.getElementById('data-refresh-start-button').addEventListener('click', async event => {
    if (!state.dataRefreshPreview) return;
    if (!window.confirm(`确认增量同步 ${state.dataRefreshPreview.total} 个标的？已有缓存只请求尾部重叠区间。`)) return;
    event.target.disabled = true;
    try {
      const task = await request('/api/v1/data/refresh', {
        method: 'POST', body: dataRefreshPayload(),
      });
      document.getElementById('data-refresh-confirm').hidden = true;
      renderDataRefresh(task);
      pollDataRefresh(task.id);
    } catch (error) {
      event.target.disabled = false;
      document.querySelector('[data-refresh-preview-text]').textContent = error.message;
    }
  });

  document.getElementById('data-refresh-cancel').addEventListener('click', async event => {
    const id = event.target.dataset.jobId;
    if (!id) return;
    const task = await request(`/api/v1/jobs/${encodeURIComponent(id)}/cancel`, {method: 'POST'});
    renderDataRefresh(task);
    pollDataRefresh(id);
  });

  document.getElementById('data-refresh-resume').addEventListener('click', async event => {
    const id = event.target.dataset.jobId;
    if (!id) return;
    event.target.disabled = true;
    try {
      const task = await request(`/api/v1/jobs/${encodeURIComponent(id)}/retry`, {method: 'POST'});
      renderDataRefresh(task);
      pollDataRefresh(id);
    } finally { event.target.disabled = false; }
  });

  function researchPayload() {
    return {
      assets: [...document.querySelectorAll('#research-assets input:checked')].map(item => item.value),
      datasets: [...document.getElementById('research-datasets').selectedOptions].map(item => item.value),
      specs: [...document.getElementById('research-specs').selectedOptions].map(item => item.value),
      start: document.getElementById('research-start').value,
      end: document.getElementById('research-end').value,
      mode: document.getElementById('research-mode').value,
      backend: document.getElementById('research-backend').value,
    };
  }

  function resetResearchPreview() {
    state.researchPreview = null;
    document.getElementById('research-confirm').hidden = true;
  }

  function renderResearchCapabilities(data) {
    const root = document.getElementById('research-capabilities');
    const daily = (data.data || []).filter(item => !item.premium);
    const unique = [];
    daily.forEach(item => {
      if (!unique.some(value => value.endpoint === item.endpoint)) unique.push(item);
    });
    const kernel = data.kernel || {};
    root.innerHTML = unique.map(item =>
      `<span class="research-capability ${html(item.state)}" title="${html(item.detail)}">${html(item.endpoint)} · ${html(item.state)}</span>`
    ).join('') + `<span class="research-capability ${kernel.backend === 'rust' ? 'available' : ''}" title="${html(kernel.fallback_reason || '原生内核可用')}">kernel · ${html(kernel.backend || 'python')}</span>`;
  }

  async function loadResearchControls() {
    const start = document.getElementById('research-start');
    const end = document.getElementById('research-end');
    if (!start.value) start.value = state.config?.lab?.start || '2022-01-01';
    if (!end.value) end.value = new Date().toISOString().slice(0, 10);
    try {
      const [catalog, capabilities, jobs] = await Promise.all([
        request('/api/v1/research/data/catalog'),
        request('/api/v1/research/data/capabilities'),
        request('/api/v1/research/data/jobs?limit=1'),
      ]);
      state.researchCatalog = catalog;
      state.researchControlsLoaded = true;
      const datasetSelect = document.getElementById('research-datasets');
      const selectedDatasets = new Set([...datasetSelect.selectedOptions].map(item => item.value));
      datasetSelect.innerHTML = (catalog.datasets || []).filter(item => !item.premium).map(item =>
        `<option value="${html(item.id)}"${selectedDatasets.has(item.id) ? ' selected' : ''}>${html(item.asset_class)} · ${html(item.name)}</option>`
      ).join('');
      const specSelect = document.getElementById('research-specs');
      const selectedSpecs = new Set([...specSelect.selectedOptions].map(item => item.value));
      specSelect.innerHTML = (catalog.specs || []).filter(item =>
        item.tags?.includes('cross-asset') || item.tags?.includes('label') || item.tags?.includes('qm-style-v1')
      ).map(item =>
        `<option value="${html(item.id)}"${selectedSpecs.has(item.id) ? ' selected' : ''}>${html(item.kind)} · ${html(item.name || item.id)}</option>`
      ).join('');
      renderResearchCapabilities(capabilities);
      const latest = (jobs.items || [])[0];
      if (latest) {
        renderResearchJob(latest);
        if (mounted && ['running', 'cancelling'].includes(latest.status)) pollResearchJob(latest.id);
      }
    } catch (error) {
      document.getElementById('research-capabilities').innerHTML = `<span class="err">研究目录不可用：${html(error.message)}</span>`;
    }
    resetResearchPreview();
  }

  function renderResearchJob(task) {
    const root = document.getElementById('research-progress');
    root.hidden = false;
    root.style.setProperty('--research-progress', (task.progress || 0) / 100);
    const labels = {
      running: '研究生产中', cancelling: '正在完成当前分区', cancelled: '已取消，可继续',
      interrupted: '服务重启中断，可继续', completed: '研究生产完成',
      completed_with_errors: '任务完成，部分分区失败', failed: '研究任务失败',
    };
    const current = task.current_task ? ` · ${task.current_task}` : '';
    root.querySelector('[data-research-phase]').textContent =
      `${labels[task.status] || task.status} · ${task.next_index}/${task.total}${current}`;
    root.querySelector('[data-research-percent]').textContent = `${task.progress || 0}%`;
    const failures = task.failures || [];
    root.querySelector('[data-research-detail]').textContent = failures.length
      ? `${failures.length} 个失败：${failures.slice(-2).map(item => item.error).join('；')}`
      : `${task.succeeded || 0} 个分区或计算批次已完成`;
    const cancel = document.getElementById('research-cancel');
    cancel.hidden = !['running', 'cancelling'].includes(task.status);
    cancel.disabled = task.status === 'cancelling';
    cancel.dataset.jobId = task.id;
    const resume = document.getElementById('research-resume');
    resume.hidden = !['cancelled', 'interrupted', 'completed_with_errors'].includes(task.status);
    resume.dataset.jobId = task.id;
    state.researchId = ['running', 'cancelling'].includes(task.status) ? String(task.id || '') : '';
  }

  async function pollResearchJob(id, generation = lifecycleGeneration) {
    if (!mounted || generation !== lifecycleGeneration) return;
    clearTimeout(state.researchTimer);
    try {
      const task = await request(`/api/v1/research/data/jobs/${id}`);
      if (!mounted || generation !== lifecycleGeneration) return;
      renderResearchJob(task);
      if (['running', 'cancelling'].includes(task.status)) {
        state.researchTimer = setTimeout(() => pollResearchJob(id, generation), 2000);
      }
    } catch (error) {
      if (!mounted || generation !== lifecycleGeneration) return;
      const root = document.getElementById('research-progress');
      root.hidden = false;
      root.querySelector('[data-research-detail]').textContent = error.message;
    }
  }

  document.querySelectorAll('#research-assets input, #research-mode, #research-backend, #research-start, #research-end, #research-datasets, #research-specs').forEach(item => {
    item.addEventListener('change', resetResearchPreview);
  });
  document.getElementById('research-data-reload').addEventListener('click', async event => {
    event.target.disabled = true;
    try { await loadResearchControls(); } finally { event.target.disabled = false; }
  });
  document.getElementById('research-artifacts').addEventListener('toggle', async event => {
    if (!event.target.open || state.researchControlsLoaded) return;
    await loadResearchControls();
  });
  document.getElementById('research-preview').addEventListener('click', async event => {
    event.target.disabled = true;
    try {
      const payload = researchPayload();
      if (!payload.assets.length) throw new Error('至少选择一种资产');
      const plan = await request('/api/v1/research/data/plans', {method: 'POST', body: payload});
      state.researchPreview = plan;
      const size = plan.estimated_bytes >= 1048576
        ? `${(plan.estimated_bytes / 1048576).toFixed(1)} MiB` : `${plan.estimated_bytes || 0} B`;
      const blockers = plan.capability_blocks || [];
      document.querySelector('[data-research-preview-text]').textContent = blockers.length
        ? `能力阻塞：${blockers.map(item => `${item.dataset_id} ${item.detail}`).join('；')}`
        : `${plan.tasks.length} 个批次，约 ${plan.estimated_rows} 行 / ${size}。${(plan.warnings || []).join('；')}`;
      document.getElementById('research-start-button').disabled = Boolean(blockers.length);
      document.getElementById('research-confirm').hidden = false;
    } catch (error) {
      state.researchPreview = null;
      document.querySelector('[data-research-preview-text]').textContent = error.message;
      document.getElementById('research-start-button').disabled = true;
      document.getElementById('research-confirm').hidden = false;
    } finally { event.target.disabled = false; }
  });
  document.getElementById('research-start-button').addEventListener('click', async event => {
    if (!state.researchPreview) return;
    event.target.disabled = true;
    try {
      const task = await request('/api/v1/research/data/jobs', {method: 'POST', body: researchPayload()});
      document.getElementById('research-confirm').hidden = true;
      renderResearchJob(task);
      pollResearchJob(task.id);
    } catch (error) {
      document.querySelector('[data-research-preview-text]').textContent = error.message;
    } finally { event.target.disabled = false; }
  });
  document.getElementById('research-cancel').addEventListener('click', async event => {
    const id = event.target.dataset.jobId;
    if (!id) return;
    renderResearchJob(await request(`/api/v1/research/data/jobs/${id}/cancel`, {method: 'POST'}));
    pollResearchJob(id);
  });
  document.getElementById('research-resume').addEventListener('click', async event => {
    const id = event.target.dataset.jobId;
    if (!id) return;
    event.target.disabled = true;
    try {
      renderResearchJob(await request(`/api/v1/research/data/jobs/${id}/resume`, {method: 'POST'}));
      pollResearchJob(id);
    } finally { event.target.disabled = false; }
  });

  async function pollMigration(id, generation = lifecycleGeneration) {
    if (!mounted || generation !== lifecycleGeneration) return;
    clearTimeout(state.migrationTimer);
    try {
      const task = await request(`/api/v1/data/migrations/${id}`);
      if (!mounted || generation !== lifecycleGeneration) return;
      const root = document.getElementById('migration-progress');
      root.hidden = false;
      root.style.setProperty('--migration-progress', task.progress / 100);
      root.querySelector('[data-migration-phase]').textContent = task.error || task.phase;
      root.querySelector('[data-migration-percent]').textContent = `${task.progress}%`;
      state.migrationId = ['pending', 'running', 'cancelling'].includes(task.status)
        ? String(task.id || id) : '';
      if (state.migrationId) {
        state.migrationTimer = setTimeout(() => pollMigration(id, generation), 600);
      } else {
        document.getElementById('migration-cancel').hidden = true;
        if (task.status === 'completed') {
          state.loaded = false;
          await loadSettings(true);
          if (!mounted || generation !== lifecycleGeneration) return;
        }
      }
    } catch (error) {
      if (!mounted || generation !== lifecycleGeneration) return;
      document.querySelector('[data-migration-phase]').textContent = error.message;
    }
  }

  document.getElementById('migration-start').addEventListener('click', async () => {
    const target = document.getElementById('migration-target').value.trim();
    const mode = document.getElementById('migration-mode').value;
    if (!target) return;
    if (mode === 'switch' && !window.confirm('仅切换不会复制任何数据。确认目标已包含完整数据？')) return;
    try {
      const task = await request('/api/v1/data/migrations', {method: 'POST', body: {target, mode}});
      document.getElementById('migration-cancel').hidden = false;
      document.getElementById('migration-cancel').dataset.taskId = task.id;
      pollMigration(task.id);
    } catch (error) {
      const root = document.getElementById('migration-progress');
      root.hidden = false;
      root.querySelector('[data-migration-phase]').textContent = error.message;
    }
  });

  document.getElementById('migration-cancel').addEventListener('click', async event => {
    const id = event.target.dataset.taskId;
    if (id) await request(`/api/v1/data/migrations/${id}/cancel`, {method: 'POST'});
  });

  const contractMigrationLabels = {
    'market-data-legacy': '行情与证券主数据', decision: '决策快照', paper: '模拟盘账本',
    news: '资讯记录', automation: '自动化设置', 'after-close': '盘后快照', rotation: '轮动快照',
  };

  function contractDomainLabel(domain) {
    return contractMigrationLabels[domain] || String(domain || '').replaceAll('-', ' ');
  }

  function renderContractMigration(task) {
    const empty = document.getElementById('contract-migration-empty');
    if (!task) {
      state.contractMigrationId = '';
      empty.hidden = false;
      return;
    }
    empty.hidden = true;
    const total = Number(task.total || 0);
    const checked = Number(task.checked || 0);
    const ratio = total > 0 ? Math.min(1, checked / total) : 0;
    const root = document.getElementById('contract-migration-status');
    root.style.setProperty('--contract-migration-progress', ratio);
    root.querySelector('[data-contract-phase]').textContent =
      `${contractDomainLabel(task.domain)} · ${task.phase || task.status}`;
    root.querySelector('[data-contract-progress]').textContent = `${checked} / ${total}`;
    ['total', 'checked', 'converted', 'blank', 'review', 'conflicts'].forEach(name => {
      root.querySelector(`[data-contract-count="${name}"]`).textContent = Number(task[name] || 0).toLocaleString('zh-CN');
    });
    root.querySelector('[data-contract-batch]').textContent = `最近批次：${task.last_batch || '—'}${task.last_key ? ` · ${task.last_key}` : ''}`;
    root.querySelector('[data-contract-eta]').textContent = task.estimated_remaining_seconds == null
      ? '预计剩余：—' : `预计剩余：${Math.max(0, Number(task.estimated_remaining_seconds))} 秒`;
    const diagnostic = root.querySelector('[data-contract-diagnostic]');
    diagnostic.hidden = !task.diagnostic_code;
    diagnostic.textContent = task.diagnostic_code ? `诊断码 ${task.diagnostic_code}` : '';
    const writeState = document.getElementById('contract-migration-write-state');
    writeState.textContent = task.write_paused
      ? '离线停写已验证' : '未处于离线停写窗口';
    writeState.className = `state-pill ${task.write_paused ? 'sell' : ''}`;
    const active = ['queued', 'backing_up', 'running', 'pausing'].includes(task.status);
    state.contractMigrationId = active ? String(task.id || '') : '';
    document.getElementById('contract-migration-pause').hidden = !active || task.status === 'pausing';
    document.getElementById('contract-migration-resume').hidden = true;
    document.getElementById('contract-migration-rollback').hidden = true;
    const investigation = document.getElementById('contract-migration-investigation');
    const list = investigation.querySelector('ol');
    const unknown = Array.isArray(task.unknown_results) ? task.unknown_results : [];
    investigation.hidden = unknown.length === 0;
    list.innerHTML = unknown.map(item => `<li><strong>${html(item.record_key)}</strong><code>${html(item.diagnostic_code || 'needs_review')}</code><span>${html((item.unknown_fields || []).join('、') || '未确认可选字段')}</span><small>${html(item.detail || '原记录证据不足，当前字段保持为空。')}</small></li>`).join('');
    if (mounted && active) scheduleContractMigrationPoll(task.id);
  }

  function scheduleContractMigrationPoll(id, delay = 800, generation = lifecycleGeneration) {
    if (!mounted || generation !== lifecycleGeneration) return;
    clearTimeout(state.contractMigrationTimer);
    state.contractMigrationTimer = setTimeout(() => pollContractMigration(id, generation), delay);
  }

  async function pollContractMigration(id, generation = lifecycleGeneration) {
    if (!mounted || generation !== lifecycleGeneration) return;
    try {
      const task = await request(`/api/v1/data/contract-migrations/${id}`);
      if (!mounted || generation !== lifecycleGeneration) return;
      state.contractMigrationFailures = 0;
      renderContractMigration(task);
    } catch (error) {
      if (!mounted || generation !== lifecycleGeneration) return;
      state.contractMigrationFailures += 1;
      if (state.contractMigrationFailures === 1) {
        document.querySelector('[data-contract-phase]').textContent = `状态暂时不可用：${error.message}`;
      }
      if (mounted && state.contractMigrationFailures < 5) {
        scheduleContractMigrationPoll(
          id, Math.min(8000, 600 * (2 ** state.contractMigrationFailures)), generation,
        );
      }
    }
  }

  async function loadContractMigrationStatus() {
    try {
      const data = await request('/api/v1/data/contract-migrations');
      const select = document.getElementById('contract-migration-domain');
      const current = select.value;
      select.innerHTML = (data.available_types || []).length
        ? data.available_types.map(name => `<option value="${html(name)}">${html(contractDomainLabel(name))}</option>`).join('')
        : '<option value="">暂无可用迁移</option>';
      if ([...select.options].some(option => option.value === current)) select.value = current;
      document.getElementById('contract-migration-start').disabled = !select.value;
      renderContractMigration(data.latest);
    } catch (error) {
      document.querySelector('[data-contract-phase]').textContent = `迁移状态不可用：${error.message}`;
    }
  }

  document.getElementById('contract-migration-start').addEventListener('click', async event => {
    const domain = document.getElementById('contract-migration-domain').value;
    const mode = document.getElementById('contract-migration-mode').value;
    if (!domain) return;
    event.target.disabled = true;
    try {
      renderContractMigration(await request('/api/v1/data/contract-migrations', {
        method: 'POST', body: {domain, mode, batch_size: 250},
      }));
    } finally { event.target.disabled = false; }
  });
  document.getElementById('contract-migration-pause').addEventListener('click', async () => {
    if (state.contractMigrationId) renderContractMigration(await request(`/api/v1/data/contract-migrations/${state.contractMigrationId}/pause`, {method: 'POST'}));
  });
  document.getElementById('contract-migration-resume').addEventListener('click', async () => {
    if (state.contractMigrationId) renderContractMigration(await request(`/api/v1/data/contract-migrations/${state.contractMigrationId}/resume`, {method: 'POST'}));
  });
  document.getElementById('contract-migration-rollback').addEventListener('click', async () => {
    if (!state.contractMigrationId || !window.confirm('确认从迁移前备份回滚？')) return;
    renderContractMigration(await request(`/api/v1/data/contract-migrations/${state.contractMigrationId}/rollback`, {method: 'POST'}));
  });

  async function pollFreeStockDbSidecar(generation = lifecycleGeneration) {
    if (!mounted || generation !== lifecycleGeneration) return;
    if (freeStockDbPollTimer !== null) clearTimeout(freeStockDbPollTimer);
    freeStockDbPollTimer = null;
    try {
      const status = await request('/api/v1/settings/free-stockdb');
      if (!mounted || generation !== lifecycleGeneration) return;
      freeStockDbPollFailures = 0;
      renderRuntime({...state.lastRuntime, free_stockdb: status});
      const active = isFreeStockDbActive(status);
      freeStockDbActive = active;
      document.getElementById('free-stockdb-update-now').disabled = active;
      if (active) scheduleFreeStockDbPoll(1000, generation);
    } catch (error) {
      if (!mounted || generation !== lifecycleGeneration) return;
      freeStockDbPollFailures += 1;
      document.getElementById('free-stockdb-sidecar-status').textContent = error.message;
      if (freeStockDbPollFailures < 5) {
        scheduleFreeStockDbPoll(
          Math.min(8000, 500 * (2 ** freeStockDbPollFailures)), generation,
        );
      } else {
        document.getElementById('free-stockdb-update-now').disabled = false;
      }
    }
  }

  document.getElementById('free-stockdb-update-now').addEventListener('click', async event => {
    const generation = lifecycleGeneration;
    freeStockDbActive = true;
    event.target.disabled = true;
    try {
      const status = await request('/api/v1/settings/free-stockdb/update', {method: 'POST'});
      if (!mounted || generation !== lifecycleGeneration) return;
      renderRuntime({...state.lastRuntime, free_stockdb: status});
      scheduleFreeStockDbPoll(250, generation);
    } catch (error) {
      if (!mounted || generation !== lifecycleGeneration) return;
      freeStockDbActive = false;
      document.getElementById('free-stockdb-sidecar-status').textContent = error.message;
      event.target.disabled = false;
    }
  });

  function resumeActiveWork() {
    document.getElementById('weixin-login-start').disabled = Boolean(
      state.weixinLoginId || state.weixinLoginCreatePending,
    );
    if (state.editRevision > state.savedRevision && !state.saveInFlight) {
      state.autoSaveTimer = setTimeout(flushAutosave, 0);
    }
    if (state.dataRefreshId) void pollDataRefresh(state.dataRefreshId);
    if (state.researchId) void pollResearchJob(state.researchId);
    if (state.migrationId) void pollMigration(state.migrationId);
    if (state.contractMigrationId) scheduleContractMigrationPoll(state.contractMigrationId, 0);
    if (freeStockDbActive) scheduleFreeStockDbPoll(0);
    if (state.weixinLoginId) {
      renderWeixinLoginSession();
      scheduleWeixinPoll(state.weixinLoginId, 0);
    }
  }

  async function mountSettings() {
    const resume = state.loaded;
    const generation = ++lifecycleGeneration;
    mounted = true;
    const initialSection = document.querySelector('[data-settings-section].active')
      ?.dataset.settingsSection || 'llm';
    switchSection(initialSection);
    await loadSettings();
    if (resume && mounted && generation === lifecycleGeneration) resumeActiveWork();
  }

  const management = {
    request, ensureSettings: loadSettings, state,
  };
  function unmount() {
    mounted = false;
    lifecycleGeneration += 1;
    [
      'migrationTimer', 'dataRefreshTimer', 'researchTimer', 'modelCheckTimer',
      'autoSaveTimer', 'retryTimer', 'weixinLoginTimer', 'contractMigrationTimer',
    ].forEach(key => {
      clearTimeout(state[key]);
      state[key] = null;
    });
    clearTimeout(freeStockDbPollTimer);
    freeStockDbPollTimer = null;
    document.getElementById('weixin-login-start').disabled = Boolean(
      state.weixinLoginId || state.weixinLoginCreatePending,
    );
  }

  return {mount:mountSettings, unmount, refresh:() => loadSettings(true), ...management};
})();

export const {mount, unmount, refresh, request, ensureSettings, state} = settingsFeature;
