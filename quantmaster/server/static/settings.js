(() => {
  'use strict';

  const state = {
    loaded: false,
    csrf: '',
    config: null,
    secretActions: { llm: 'keep', tushare: 'keep' },
    migrationTimer: null,
    dataRefreshTimer: null,
    dataRefreshPreview: null,
    researchTimer: null,
    researchPreview: null,
    researchCatalog: null,
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
    lastRuntime: null,
  };
  const form = document.getElementById('settings-form');

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
    if (state.csrf) headers.set('X-CSRF-Token', state.csrf);
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
    form.hidden = ['sources', 'backup'].includes(name);
    if (name === 'backup') loadSnapshots();
    if (['automation', 'lab'].includes(name)) loadAutomationOverview();
  }

  document.getElementById('settings-nav').addEventListener('click', event => {
    const button = event.target.closest('[data-settings-section]');
    if (button) switchSection(button.dataset.settingsSection);
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
        if (input.matches('[data-candidate-select]')) input.dataset.candidateValue = value;
        input.value = value;
      }
      input.removeAttribute('aria-invalid');
    });
    document.getElementById('settings-config-path').textContent = config.config_path;
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
    scheduleAutomaticModelCheck();
  }

  function renderRuntime(runtime) {
    if (!runtime) return;
    state.lastRuntime = runtime;
    const note = document.getElementById('settings-runtime-note');
    const restart = runtime.server?.restart_required || [];
    note.classList.toggle('restart-required', Boolean(restart.length));
    note.textContent = restart.length
      ? `${restart.join(' / ')} 已保存，重启服务后生效`
      : '停顿片刻或离开字段后自动生效';
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
      const label = secret.configured ? `已配置 · ${secret.state}` : `未配置 · ${secret.state}`;
      document.getElementById(`${name}-secret-state`).textContent = label;
    }
  }

  async function loadSettings(force = false) {
    if (state.loaded && !force) return;
    try {
      const data = await request('/api/settings');
      state.csrf = data.csrf_token;
      state.config = data;
      state.loaded = true;
      fillForm(data);
      await loadDataRefreshControls();
      await loadResearchControls();
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

  form.addEventListener('input', event => {
    const input = event.target;
    if (input.id === 'plaintext-confirm' || input.id === 'feishu-app-secret' ||
        input.id === 'weixin-verify-code') return;
    if (!input.name && !['llm-secret', 'tushare-secret'].includes(input.id)) return;
    if (input.id === 'llm-secret' || input.id === 'tushare-secret') {
      const name = input.id.replace('-secret', '');
      state.secretActions[name] = input.value ? 'replace' : 'keep';
      if (input.id === 'llm-secret') scheduleAutomaticModelCheck();
      markDirty('凭据填写完成后自动保存…');
      return;
    }
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
    if (input.dataset.confirmCloud !== undefined && input.checked) {
      const confirmed = window.confirm('允许匿名云端样本会扩大数据出站范围。每次实际发送仍需确认，是否继续？');
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

  function renderCheck(kind, data) {
    const el = document.querySelector(`[data-check-result="${kind}"]`);
    if (!el) return;
    el.className = `check-result ${data.status || ''}`;
    const time = data.checked_at ? new Date(data.checked_at).toLocaleTimeString('zh-CN', {hour12: false}) : '';
    const labChecks = kind === 'lab' ? Object.values(data.details?.checks || {})
      .map(item => item.message).filter(Boolean) : [];
    const sourceChecks = kind === 'data-sources' ? Object.entries(data.details?.sources || {})
      .map(([name, item]) => `${name}: ${item.message}`) : [];
    const openCircuits = kind === 'data-sources' ? Object.entries(data.details?.circuits || {})
      .filter(([, item]) => item.state !== 'closed')
      .map(([name, item]) => `${name} ${item.state}`) : [];
    const proxies = kind === 'data-sources' ? Object.entries(data.details?.proxies || {})
      .map(([name, value]) => `${name}=${value}`) : [];
    const master = kind === 'data-sources' ? data.details?.security_master : null;
    const coverage = master?.coverage?.map(item => `${item.market}/${item.asset_type} ${item.count}`).join('、');
    const details = [...labChecks, ...sourceChecks,
      ...(master ? [`证券主数据：${master.record_count || 0} 条${coverage ? `（${coverage}）` : ''}`] : []),
      ...(openCircuits.length ? [`熔断：${openCircuits.join('、')}`] : []),
      ...(proxies.length ? [`代理：${proxies.join('、')}`] : [])];
    el.textContent = `${data.message}${details.length ? ` · ${details.join('；')}` : ''}` +
      `${data.latency_ms != null ? ` · ${data.latency_ms}ms` : ''}${time ? ` · ${time}` : ''}`;
    if (kind === 'llm-models' && Array.isArray(data.details?.models)) {
      const list = document.getElementById('settings-model-list');
      list.innerHTML = data.details.models.map(model => `<option value="${html(model)}"></option>`).join('');
      const selected = form.elements['llm.model'].value;
      document.getElementById('model-hint').textContent = data.details.models.includes(selected)
        ? '当前模型由 API 列表返回'
        : '当前手填值不在返回列表中，仍会保留';
    }
  }

  document.querySelectorAll('.check-button').forEach(button => {
    button.addEventListener('click', async () => {
      const kind = button.dataset.check;
      const result = document.querySelector(`[data-check-result="${kind}"]`);
      if (result) { result.className = 'check-result'; result.textContent = '检测中…'; }
      button.disabled = true;
      try {
        const data = await request(`/api/settings/check/${kind}`, {
          method: 'POST', body: {...documentPayload(false), secrets: documentPayload(true).secrets},
        });
        renderCheck(kind, data);
      } catch (error) {
        renderCheck(kind, {status: 'error', message: error.message});
      } finally {
        button.disabled = false;
      }
    });
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
      const validated = await request('/api/settings/validate', {method: 'POST', body: plain});
      setSaveState('saving', '正在自动保存并应用…');
      const update = {...validated.normalized, secrets: secretPayload.secrets,
        allow_plaintext_secrets: secretPayload.allow_plaintext_secrets};
      const result = await request('/api/settings', {method: 'PUT', body: update});

      if (result.settings) {
        state.config = {...result.settings, csrf_token: state.csrf, runtime: result.runtime};
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
      if (state.editRevision === revision && result.settings) fillForm(state.config);
      renderRuntime(result.runtime);
      const suffix = (result.restart_required || []).length
        ? `；${result.restart_required.join(' / ')} 重启后生效` : '';
      const degraded = Object.entries(result.apply_status || {})
        .filter(([, value]) => value?.status === 'degraded')
        .map(([name]) => name === 'automation' ? '自动化运行态异常' : '研究 Worker 运行态异常');
      const allWarnings = [...(result.warnings || []), ...degraded];
      const warnings = allWarnings.length ? `；${allWarnings.join('；')}` : '';
      const time = new Date().toLocaleTimeString('zh-CN', {hour12: false, hour: '2-digit', minute: '2-digit'});
      setSaveState('saved', `已自动保存 ${time}${suffix}${warnings}`);
      document.dispatchEvent(new CustomEvent('quantmaster:settings-applied', {detail: result}));
      if (document.querySelector('[data-settings-section="automation"].active') ||
          document.querySelector('[data-settings-section="lab"].active')) loadAutomationOverview();
    } catch (error) {
      setSaveState('error', `自动保存失败：${error.message}`);
      if (error.status === 423) {
        state.retryTimer = setTimeout(flushAutosave, 1200);
      }
    } finally {
      state.saveInFlight = false;
      if (state.saveQueued || state.editRevision > revision) {
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

  async function loadAutomationOverview() {
    if (!state.loaded) return;
    const runtimeState = document.getElementById('automation-runtime-state');
    try {
      const data = await request('/api/automation/overview');
      const runtimeLabels = {running: '运行中', standby: '等待调度租约', disabled: '已停用', degraded: '运行异常'};
      runtimeState.textContent = runtimeLabels[data.runtime] || data.runtime;
      runtimeState.className = `state-pill ${data.runtime === 'running' ? 'buy' : ''}`;
      for (const channel of ['weixin', 'feishu']) {
        const account = (data.bot_accounts || []).find(item => item.channel === channel);
        const configured = Boolean(data.channels?.[channel]?.configured);
        setChannelState(channel, configured, account?.status || '');
      }
      const feishu = (data.bot_accounts || []).find(item => item.channel === 'feishu');
      const appId = document.getElementById('feishu-app-id');
      if (feishu && document.activeElement !== appId) appId.value = feishu.account_id || '';
      document.getElementById('automation-enable-connect').hidden = !(feishu && !data.enabled);
      document.getElementById('feishu-remove').disabled = !feishu;
      const runtime = await request('/api/settings/runtime');
      renderRuntime(runtime);
    } catch (error) {
      runtimeState.textContent = `状态不可用：${error.message}`;
    }
  }

  function scheduleWeixinPoll(sessionId, delay = 700) {
    clearTimeout(state.weixinLoginTimer);
    state.weixinLoginTimer = setTimeout(() => pollWeixinLogin(sessionId), delay);
  }

  async function pollWeixinLogin(sessionId) {
    const status = document.getElementById('weixin-login-status');
    const verifyCode = document.getElementById('weixin-verify-code').value.trim();
    try {
      const data = await request(
        `/api/automation/channels/weixin/login/${encodeURIComponent(sessionId)}` +
        `?verify_code=${encodeURIComponent(verifyCode)}`
      );
      const labels = {
        wait: '等待扫码确认…', scanned: '已扫码，请在微信中确认',
        confirmed: '接入成功，请先给机器人发一条消息', expired: '二维码已失效，请重新生成',
      };
      status.textContent = labels[data.status] || `微信状态：${data.status}`;
      if (data.status === 'confirmed') {
        clearTimeout(state.weixinLoginTimer);
        document.getElementById('weixin-login-start').disabled = false;
        await loadAutomationOverview();
      } else if (data.status === 'expired') {
        document.getElementById('weixin-login-start').disabled = false;
      } else {
        scheduleWeixinPoll(sessionId);
      }
    } catch (error) {
      status.textContent = `登录状态读取失败：${error.message}`;
      document.getElementById('weixin-login-start').disabled = false;
    }
  }

  document.getElementById('weixin-login-start').addEventListener('click', async event => {
    const button = event.currentTarget;
    const panel = document.getElementById('weixin-login-panel');
    const status = document.getElementById('weixin-login-status');
    clearTimeout(state.weixinLoginTimer);
    button.disabled = true;
    status.textContent = '正在申请二维码…';
    panel.hidden = false;
    try {
      const data = await request('/api/automation/channels/weixin/login', {method: 'POST'});
      const image = document.getElementById('weixin-login-qr');
      image.src = data.qrcode_svg || data.qrcode_url;
      image.hidden = !image.src;
      status.textContent = '请使用微信扫码并确认';
      scheduleWeixinPoll(data.session_id);
    } catch (error) {
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
      const data = await request('/api/automation/channels/feishu/config', {
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
      const data = await request('/api/automation/channels/feishu/check', {method: 'POST'});
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
      const data = await request('/api/automation/channels/feishu/config', {method: 'DELETE'});
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
      const data = await request('/api/settings/snapshots');
      if (!data.snapshots.length) {
        container.innerHTML = '<div class="msg">尚无快照。成功保存设置后会自动创建。</div>';
        return;
      }
      container.innerHTML = data.snapshots.map(item => `<div class="snapshot-row">
        <div><strong>${html(item.name || ({automatic: '自动保存', initial: '初始配置'}[item.kind] || item.kind))}</strong>
        <small>${html(new Date(item.created_at).toLocaleString('zh-CN'))} · ${html(item.kind)} · ${html((item.config_hash || '').slice(0, 10))}</small></div>
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
      await request('/api/settings/snapshots', {method: 'POST', body: {name: input.value}});
      input.value = '';
      await loadSnapshots();
    } catch (error) { input.setCustomValidity(error.message); input.reportValidity(); input.setCustomValidity(''); }
  });

  document.getElementById('snapshot-list').addEventListener('click', async event => {
    const diffButton = event.target.closest('[data-snapshot-diff]');
    const deleteButton = event.target.closest('[data-snapshot-delete]');
    if (deleteButton) {
      if (!window.confirm('永久删除这个手动快照？')) return;
      await request(`/api/settings/snapshots/${deleteButton.dataset.snapshotDelete}`, {method: 'DELETE'});
      await loadSnapshots();
      return;
    }
    if (!diffButton) return;
    const id = diffButton.dataset.snapshotDiff;
    const panel = document.getElementById('snapshot-diff');
    try {
      const data = await request(`/api/settings/snapshots/${id}/diff`);
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
      await request(`/api/settings/snapshots/${button.dataset.snapshotRollback}/rollback`, {method: 'POST'});
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
      const data = await request('/api/settings/universes');
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
      const latest = await request('/api/settings/data-refresh/latest');
      if (latest.job) {
        renderDataRefresh(latest.job);
        if (['running', 'cancelling'].includes(latest.job.status)) pollDataRefresh(latest.job.id);
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

  async function pollDataRefresh(id) {
    clearTimeout(state.dataRefreshTimer);
    try {
      const task = await request(`/api/settings/data-refresh/${id}`);
      renderDataRefresh(task);
      if (['running', 'cancelling'].includes(task.status)) {
        state.dataRefreshTimer = setTimeout(() => pollDataRefresh(id), 800);
      }
    } catch (error) {
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
      const preview = await request('/api/settings/data-refresh/preview', {
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
      const task = await request('/api/settings/data-refresh', {
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
    const task = await request(`/api/settings/data-refresh/${id}/cancel`, {method: 'POST'});
    renderDataRefresh(task);
    pollDataRefresh(id);
  });

  document.getElementById('data-refresh-resume').addEventListener('click', async event => {
    const id = event.target.dataset.jobId;
    if (!id) return;
    event.target.disabled = true;
    try {
      const task = await request(`/api/settings/data-refresh/${id}/resume`, {method: 'POST'});
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
        request('/api/research/data/catalog'),
        request('/api/research/data/capabilities'),
        request('/api/research/data/jobs?limit=1'),
      ]);
      state.researchCatalog = catalog;
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
        if (['running', 'cancelling'].includes(latest.status)) pollResearchJob(latest.id);
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
  }

  async function pollResearchJob(id) {
    clearTimeout(state.researchTimer);
    try {
      const task = await request(`/api/research/data/jobs/${id}`);
      renderResearchJob(task);
      if (['running', 'cancelling'].includes(task.status)) {
        state.researchTimer = setTimeout(() => pollResearchJob(id), 2000);
      }
    } catch (error) {
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
  document.getElementById('research-preview').addEventListener('click', async event => {
    event.target.disabled = true;
    try {
      const payload = researchPayload();
      if (!payload.assets.length) throw new Error('至少选择一种资产');
      const plan = await request('/api/research/data/plans', {method: 'POST', body: payload});
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
      const task = await request('/api/research/data/jobs', {method: 'POST', body: researchPayload()});
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
    renderResearchJob(await request(`/api/research/data/jobs/${id}/cancel`, {method: 'POST'}));
    pollResearchJob(id);
  });
  document.getElementById('research-resume').addEventListener('click', async event => {
    const id = event.target.dataset.jobId;
    if (!id) return;
    event.target.disabled = true;
    try {
      renderResearchJob(await request(`/api/research/data/jobs/${id}/resume`, {method: 'POST'}));
      pollResearchJob(id);
    } finally { event.target.disabled = false; }
  });

  async function pollMigration(id) {
    clearTimeout(state.migrationTimer);
    try {
      const task = await request(`/api/settings/migration/${id}`);
      const root = document.getElementById('migration-progress');
      root.hidden = false;
      root.style.setProperty('--migration-progress', task.progress / 100);
      root.querySelector('[data-migration-phase]').textContent = task.error || task.phase;
      root.querySelector('[data-migration-percent]').textContent = `${task.progress}%`;
      if (['pending', 'running', 'cancelling'].includes(task.status)) {
        state.migrationTimer = setTimeout(() => pollMigration(id), 600);
      } else {
        document.getElementById('migration-cancel').hidden = true;
        if (task.status === 'completed') {
          state.loaded = false;
          await loadSettings(true);
        }
      }
    } catch (error) {
      document.querySelector('[data-migration-phase]').textContent = error.message;
    }
  }

  document.getElementById('migration-start').addEventListener('click', async () => {
    const target = document.getElementById('migration-target').value.trim();
    const mode = document.getElementById('migration-mode').value;
    if (!target) return;
    if (mode === 'switch' && !window.confirm('仅切换不会复制任何数据。确认目标已包含完整数据？')) return;
    try {
      const task = await request('/api/settings/migration', {method: 'POST', body: {target, mode}});
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
    if (id) await request(`/api/settings/migration/${id}/cancel`, {method: 'POST'});
  });

  async function openSettings(section = 'llm') {
    const entry = document.querySelector('[data-tab="settings"]');
    entry?.click();
    await loadSettings();
    switchSection(section);
  }

  window.QuantMasterManagement = {
    request, ensureSettings: loadSettings, state, open: openSettings,
  };
})();
