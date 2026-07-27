(() => {
  'use strict';

  const state = {loaded:false, loading:false, data:null, expanded:new Set(), bindings:new Map(), qrSession:'', qrPolling:false};
  const presets = {
    conservative:{regime_threshold:80, confirmation_bars:3, cooldown_minutes:60,
      news_thresholds:{holding:75, watchlist:85, market:90}, hourly_cap:3},
    balanced:{regime_threshold:65, confirmation_bars:2, cooldown_minutes:30,
      news_thresholds:{holding:65, watchlist:75, market:80}, hourly_cap:6},
    sensitive:{regime_threshold:50, confirmation_bars:1, cooldown_minutes:15,
      news_thresholds:{holding:50, watchlist:60, market:70}, hourly_cap:12},
  };
  const presetLabels = {conservative:'保守', balanced:'均衡', sensitive:'敏感'};
  const jobLabels = {
    intraday_monitor:'盘中变盘监控', fast_news_scan:'财经快讯扫描',
    official_news_scan:'官方公告扫描', periodic_news_scan:'定期资讯扫描',
    daily_close_pipeline:'收盘决策流程',
    news_digest:'重要消息摘要', paper_rebalance_proposal:'模拟调仓建议',
  };

  async function secureApi(path, options = {}) {
    await window.QuantMasterManagement.ensureSettings();
    const headers = new Headers(options.headers || {});
    headers.set('X-CSRF-Token', window.QuantMasterManagement.state.csrf);
    return api(path, {...options, headers});
  }

  function statusText(value) {
    return ({healthy:'正常', listening:'监听中', configured:'已配置', connecting:'连接中',
      waiting_message:'等待首次消息', degraded:'异常', needs_rebind:'需重新绑定',
      unbound:'未绑定', paused:'已暂停'}[value] || value || '未配置');
  }

  function dateText(value) {
    if (!value) return '—';
    const date = new Date(value);
    return Number.isNaN(date.getTime()) ? esc(value) : date.toLocaleString('zh-CN', {hour12:false});
  }

  function scheduleText(schedule) {
    if (!schedule) return '—';
    const base = schedule.type === 'interval'
      ? `每 ${schedule.minutes} 分钟`
      : `每日 ${schedule.times.join(' / ')}`;
    const windows = schedule.windows || (schedule.window ? [schedule.window] : []);
    return `${base}${windows.length ? ` · ${windows.join('、')}` : ''}${schedule.weekdays ? ' · 工作日' : ''}`;
  }

  function publicAccount(channel) {
    return (state.data?.bot_accounts || []).find(item => item.channel === channel);
  }

  function renderChannels() {
    const root = document.getElementById('automation-channels');
    const wx = publicAccount('weixin');
    const fs = publicAccount('feishu');
    const fsInbound = state.data?.inbound?.feishu || {total:0,last_received_at:''};
    const fsDiagnostic = fs?.status === 'listening' && !fsInbound.total
      ? `<div class="channel-diagnostic warn"><strong>长连接正常，但尚未收到消息事件</strong><span>请在飞书开放平台确认：事件配置选择“使用长连接接收事件”、已添加 im.message.receive_v1、已开通单聊消息权限，并发布了包含这些变更的应用版本。</span></div>`
      : fsInbound.total
        ? `<div class="channel-diagnostic ok">已收到 ${fsInbound.total} 条消息事件 · 最近 ${dateText(fsInbound.last_received_at)}</div>`
        : '';
    root.innerHTML = `
      <div class="bot-row">
        <div class="bot-title"><strong>飞书应用 Bot <span class="badge">主通道</span></strong><span class="status-label ${esc(fs?.status || 'unbound')}">${statusText(fs?.status)}</span></div>
        <div class="bot-meta">${fs ? `App ID ${esc(fs.account_id)}${fs.last_error ? ` · ${esc(fs.last_error)}` : ''}` : '企业自建应用：长连接收命令，消息卡片推送告警'}</div>
        <div class="bot-actions"><button class="ghost" type="button" data-manage-channel="automation">${fs ? '管理飞书接入' : '前往设置接入'}</button><button class="ghost" type="button" data-feishu-diagnose ${fs ? '' : 'disabled'}>运行五阶段诊断</button></div>
        <div class="hint">需开通：以应用身份发消息、读取机器人单聊、获取群组中所有消息（im:message.group_msg）；订阅 im.message.receive_v1 并发布应用版本。普通群消息只用于话题记忆，不触发回复。</div>
        <div data-feishu-diagnostic></div>
        ${fsDiagnostic}
      </div>
      <div class="bot-row">
        <div class="bot-title"><strong>腾讯微信 ClawBot <span class="badge">轻量提醒</span></strong><span class="status-label ${esc(wx?.status || 'unbound')}">${statusText(wx?.status)}</span></div>
        <div class="bot-meta">${wx ? `Bot ${esc(wx.account_id)}${wx.last_error ? ` · ${esc(wx.last_error)}` : ''}` : 'iLink 能力有限，仅作为文本提醒与简单命令补充'}</div>
        <div class="bot-actions"><button class="ghost" type="button" data-manage-channel="automation">${wx ? '管理微信接入' : '前往设置接入'}</button></div>
      </div>`;
  }

  function mergedPolicy(target) {
    const base = structuredClone(presets[target.preset] || presets.balanced);
    const override = target.overrides || {};
    Object.assign(base, override);
    base.news_thresholds = {...(presets[target.preset] || presets.balanced).news_thresholds,
      ...(override.news_thresholds || {})};
    return base;
  }

  function bindingMarkup(target, session) {
    if (!session) return '';
    const command = `绑定 QuantMaster ${session.code}`;
    const noEvent = target.chat_type === 'group'
      ? '未收到群聊 @机器人 事件。请确认消息中真正 @QuantMaster，并检查 im:message.group_at_msg:readonly 已开通、审批且随新版本发布。'
      : '尚未收到私聊消息事件，请检查 im.message.receive_v1、单聊权限、应用发布状态和可用范围。';
    const status = {
      waiting:'等待飞书消息事件…', event_seen:'已收到消息，正在完成绑定…',
      no_event:noEvent,
      bound:'绑定成功，可以发送测试消息。', expired:'绑定码已过期，请重新开始绑定。',
      error:`绑定状态检查失败：${session.error || '未知错误'}`,
    }[session.status] || '等待绑定';
    const instruction = target.chat_type === 'group'
      ? '在目标群输入并选择 @QuantMaster，然后粘贴下面的绑定命令并发送。必须真正 @ 机器人。'
      : '打开 QuantMaster 机器人私聊，粘贴下面的完整命令并发送。';
    return `<div class="binding-wizard ${esc(session.status)}">
      <ol><li>${esc(instruction)}</li><li>保持本页打开，收到事件后会自动完成并刷新状态。</li></ol>
      <div class="binding-command"><code>${esc(command)}</code><button class="ghost" type="button" data-copy-binding="${esc(command)}">复制命令</button></div>
      <div class="binding-progress"><span class="status-label ${session.status === 'bound' ? 'listening' : session.status === 'no_event' || session.status === 'error' ? 'degraded' : 'connecting'}">${esc(status)}</span><small>10 分钟内有效</small></div>
    </div>`;
  }

  function renderTargets() {
    const root = document.getElementById('automation-targets');
    const targets = state.data?.targets || [];
    const owner = targets.find(target => target.id === 'feishu_owner');
    const ownerBound = Boolean(owner?.target && owner?.account_id && owner?.owner_actor);
    root.innerHTML = targets.map(target => {
      const policy = mergedPolicy(target);
      const expanded = state.expanded.has(target.id);
      const bound = Boolean(target.target && target.account_id);
      const binding = state.bindings.get(target.id);
      const groupBlocked = target.id === 'feishu_group' && !ownerBound;
      const bindLabel = bound ? '重新绑定' : groupBlocked ? '需先绑定管理员' : target.id === 'feishu_owner' ? '开始绑定管理员' : '开始绑定群聊';
      return `<article class="target-card" data-target-card="${esc(target.id)}">
        <div class="target-head"><div><h4>${esc(target.label)}</h4><p>${bound ? `${esc(target.channel)} · ${esc(target.target)}` : `${esc(target.channel)} · 尚未绑定会话`}</p></div>
          <span class="status-label ${esc(target.status)}">${statusText(target.status)}</span></div>
        <div class="target-controls">
          <div class="segmented" aria-label="${esc(target.label)}推送强度">
            ${Object.entries(presetLabels).map(([key,label]) => `<button type="button" data-policy="${key}" data-target="${esc(target.id)}" class="${target.preset === key ? 'active' : ''}" aria-pressed="${target.preset === key}">${label}</button>`).join('')}
          </div>
          <div class="target-tools">
            <button class="ghost" type="button" data-policy-more="${esc(target.id)}">${expanded ? '收起高级' : '高级设置'}</button>
            ${target.channel === 'feishu' ? `<button class="ghost" type="button" data-bind-target="${esc(target.id)}" data-bind-blocked="${groupBlocked}">${bindLabel}</button>` : ''}
            <button class="ghost" type="button" data-test-target="${esc(target.id)}" data-bound="${bound}">${bound ? '测试' : '测试（先绑定）'}</button>
            <button class="ghost" type="button" data-toggle-target="${esc(target.id)}">${target.enabled ? '关闭推送' : '开启推送'}</button>
          </div>
        </div>
        <div class="policy-details" ${expanded ? '' : 'hidden'}>
          <label>变盘阈值<input data-policy-field="regime_threshold" type="number" min="0" max="100" value="${policy.regime_threshold}"></label>
          <label>确认 K 线<input data-policy-field="confirmation_bars" type="number" min="1" max="3" value="${policy.confirmation_bars}"></label>
          <label>冷却分钟<input data-policy-field="cooldown_minutes" type="number" min="15" max="120" value="${policy.cooldown_minutes}"></label>
          <label>重要消息阈值<input data-policy-field="news_market" type="number" min="0" max="100" value="${policy.news_thresholds.market}"></label>
          <label>每小时上限<input data-policy-field="hourly_cap" type="number" min="1" max="30" value="${policy.hourly_cap}"></label>
          <div class="policy-save"><button class="primary" type="button" data-save-policy="${esc(target.id)}">保存高级设置</button></div>
        </div>
        <div data-binding-result="${esc(target.id)}">${bindingMarkup(target, binding)}</div>
      </article>`;
    }).join('') || '<div class="automation-empty">暂无推送目标</div>';
  }

  function renderJobs() {
    const root = document.getElementById('automation-jobs');
    root.innerHTML = (state.data?.jobs || []).map(job => `<div class="job-item">
      <div><strong>${esc(jobLabels[job.name] || job.name)}</strong><small>${job.enabled ? (state.data.runtime === 'running' ? '运行中' : '等待全局启用') : '已暂停'}${job.next_run ? ` · 下次 ${dateText(job.next_run)}` : ''}</small></div>
      <small>${esc(scheduleText(job.schedule))}</small>
      <div class="job-actions"><button class="ghost" type="button" data-job-toggle="${esc(job.name)}" data-enabled="${job.enabled}">${job.enabled ? '暂停' : '恢复'}</button><button class="ghost" type="button" data-job-run="${esc(job.name)}">立即运行</button></div>
    </div>`).join('') || '<div class="automation-empty">暂无任务</div>';
  }

  function renderEvents() {
    const root = document.getElementById('automation-events');
    root.innerHTML = (state.data?.recent_events || []).slice(0, 6).map(item => `<div class="bot-row">
      <div class="bot-title"><strong>${esc(item.payload?.title || item.kind)}</strong><span class="badge">${Number(item.score).toFixed(0)}</span></div>
      <div class="bot-meta">${dateText(item.occurred_at)} · ${esc(item.direction || 'neutral')}</div>
    </div>`).join('') || '<div class="automation-empty">暂无事件</div>';
  }

  async function renderAudit() {
    const root = document.getElementById('automation-audit');
    try {
      const data = await api('/api/automation/audit?limit=30');
      root.innerHTML = `<table class="automation-table"><thead><tr><th>时间</th><th>操作者</th><th>动作</th><th>对象</th><th>结果</th></tr></thead><tbody>
        ${data.items.map(item => `<tr><td class="muted">${dateText(item.created_at)}</td><td>${esc(item.actor)}</td><td>${esc(item.action)}</td><td>${esc(item.object_type)} / ${esc(item.object_id)}</td><td>${esc(item.result)}</td></tr>`).join('') || '<tr><td colspan="5" class="muted">暂无记录</td></tr>'}
      </tbody></table>`;
    } catch (error) { root.innerHTML = `<div class="err">${esc(error.message)}</div>`; }
  }

  function render() {
    const runtime = document.getElementById('automation-runtime');
    runtime.className = `automation-status ${state.data?.runtime === 'running' ? 'running' : ''}`;
    runtime.textContent = state.data?.runtime === 'running'
      ? `调度器运行中 · ${state.data.timezone}`
      : state.data?.runtime === 'standby'
        ? `自动化已启用 · 等待调度租约 · ${state.data.timezone}`
        : '自动化未开启 · 可在设置中心即时启用';
    renderChannels(); renderJobs(); renderTargets(); renderEvents(); renderAudit();
  }

  window.loadAutomation = async function(force = false) {
    if (state.loading || (state.loaded && !force)) return;
    state.loading = true;
    try {
      state.data = await api('/api/automation/overview');
      state.loaded = true;
      render();
    } catch (error) {
      document.getElementById('automation-runtime').textContent = `读取失败：${error.message}`;
    } finally { state.loading = false; }
  };

  async function updateTarget(targetId, body) {
    await secureApi(`/api/automation/targets/${encodeURIComponent(targetId)}/policy`, {
      method:'PATCH', headers:{'Content-Type':'application/json'}, body:JSON.stringify(body),
    });
    state.loaded = false; await loadAutomation(true);
  }

  async function pollBinding(targetId, session) {
    while (state.bindings.get(targetId) === session && Date.now() < session.expiresAt) {
      await new Promise(resolve => setTimeout(resolve, 2000));
      try {
        const result = await secureApi(`/api/automation/bindings/${encodeURIComponent(session.id)}`);
        if (result.status === 'bound') {
          session.status = 'bound';
          state.loaded = false;
          await loadAutomation(true);
          return;
        }
        if (result.status === 'expired') {
          session.status = 'expired'; renderTargets(); return;
        }
        if (Number(result.inbound?.total || 0) > session.initialInbound) {
          session.status = 'event_seen';
        } else if (Date.now() - session.startedAt > 15000) {
          session.status = 'no_event';
        }
        renderTargets();
      } catch (error) {
        session.status = 'error'; session.error = error.message; renderTargets(); return;
      }
    }
    if (state.bindings.get(targetId) === session) {
      session.status = 'expired'; renderTargets();
    }
  }

  async function startWeixinLogin(button) {
    button.disabled = true;
    const box = document.getElementById('weixin-login-box');
    box.innerHTML = '<div class="hint">正在向微信申请二维码…</div>';
    try {
      const result = await secureApi('/api/automation/channels/weixin/login', {method:'POST'});
      state.qrSession = result.session_id;
      box.innerHTML = `<div class="weixin-login"><img src="${esc(result.qrcode_svg || result.qrcode_url)}" alt="微信 ClawBot 登录二维码"><div><p>请使用绑定机器人的微信扫码，并在手机上确认。</p><div class="hint" data-qr-status>等待扫码，二维码约 5 分钟有效。</div></div></div>`;
      pollWeixinLogin();
    } catch (error) { box.innerHTML = `<div class="err">${esc(error.message)}</div>`; }
    finally { button.disabled = false; }
  }

  async function pollWeixinLogin(verifyCode = '') {
    if (state.qrPolling || !state.qrSession) return;
    state.qrPolling = true;
    const started = Date.now();
    try {
      while (state.qrSession && Date.now() - started < 305000) {
        const query = verifyCode ? `?verify_code=${encodeURIComponent(verifyCode)}` : '';
        const result = await secureApi(`/api/automation/channels/weixin/login/${encodeURIComponent(state.qrSession)}${query}`);
        verifyCode = '';
        const status = document.querySelector('[data-qr-status]');
        if (result.status === 'confirmed') {
          if (status) status.textContent = '授权成功。现在给 ClawBot 发送任意消息，以建立可回复会话。';
          state.qrSession = ''; state.loaded = false; await loadAutomation(true); break;
        }
        if (['expired','canceled'].includes(result.status)) {
          if (status) status.textContent = '二维码已失效，请重新扫码。';
          state.qrSession = ''; break;
        }
        if (result.needs_verify_code) {
          if (status) status.innerHTML = `微信要求安全校验。<div class="bot-actions" style="margin-top:8px"><input data-weixin-verify-input inputmode="numeric" placeholder="手机显示的数字"><button class="ghost" type="button" data-weixin-verify>提交校验</button></div>`;
          break;
        }
        if (status) status.textContent = result.status === 'scaned' ? '已扫码，请在手机上确认。' : '等待扫码…';
        await new Promise(resolve => setTimeout(resolve, 1200));
      }
    } catch (error) {
      const status = document.querySelector('[data-qr-status]');
      if (status) status.textContent = `轮询中断：${error.message}`;
    } finally { state.qrPolling = false; }
  }

  document.getElementById('automation-refresh').addEventListener('click', () => loadAutomation(true));
  document.getElementById('automation-channels').addEventListener('click', event => {
    const manage = event.target.closest('[data-manage-channel]');
    if (manage) {
      window.QuantMasterManagement.open(manage.dataset.manageChannel || 'automation');
      return;
    }
    const diagnose = event.target.closest('[data-feishu-diagnose]');
    if (!diagnose) return;
    diagnose.disabled = true;
    const root = document.querySelector('[data-feishu-diagnostic]');
    root.innerHTML = '<div class="hint">正在检查凭据、运行时、长连接、消息事件和会话绑定…</div>';
    secureApi('/api/automation/channels/feishu/check', {method:'POST'})
      .then(data => {
        const labels = {credential:'凭据', runtime:'运行时', websocket:'长连接', event:'消息事件', binding:'会话绑定'};
        root.innerHTML = Object.entries(data.stages || {}).map(([key, value]) =>
          `<div class="channel-diagnostic ${value.status === 'success' ? 'ok' : 'warn'}"><strong>${esc(labels[key] || key)} · ${value.status === 'success' ? '通过' : value.status === 'error' ? '失败' : '待处理'}</strong><span>${esc(value.message)}</span></div>`
        ).join('');
      })
      .catch(error => { root.innerHTML = `<div class="err">${esc(error.message)}</div>`; })
      .finally(() => { diagnose.disabled = false; });
  });

  document.getElementById('automation-targets').addEventListener('click', async event => {
    const policy = event.target.closest('[data-policy]');
    const more = event.target.closest('[data-policy-more]');
    const bind = event.target.closest('[data-bind-target]');
    const test = event.target.closest('[data-test-target]');
    const toggle = event.target.closest('[data-toggle-target]');
    const save = event.target.closest('[data-save-policy]');
    const copy = event.target.closest('[data-copy-binding]');
    try {
      if (copy) {
        await navigator.clipboard.writeText(copy.dataset.copyBinding);
        copy.textContent = '已复制';
        setTimeout(() => { copy.textContent = '复制命令'; }, 1500);
        return;
      }
      if (policy) return await updateTarget(policy.dataset.target, {preset:policy.dataset.policy, overrides:{}});
      if (more) {
        state.expanded.has(more.dataset.policyMore) ? state.expanded.delete(more.dataset.policyMore) : state.expanded.add(more.dataset.policyMore);
        renderTargets(); return;
      }
      if (bind) {
        if (bind.dataset.bindBlocked === 'true') {
          alert('请先绑定“飞书管理员私聊”。群绑定必须由已绑定管理员在目标群内完成。');
          return;
        }
        const data = await secureApi(`/api/automation/bindings/code?target_id=${encodeURIComponent(bind.dataset.bindTarget)}`, {method:'POST'});
        const target = state.data.targets.find(item => item.id === bind.dataset.bindTarget);
        const inbound = state.data?.inbound?.feishu?.[target.chat_type] || {total:0};
        const session = {
          id:data.id, code:data.code, status:'waiting', startedAt:Date.now(),
          expiresAt:Number(data.expires_at) * 1000,
          initialInbound:Number(inbound.total || 0), error:'',
        };
        state.bindings.set(bind.dataset.bindTarget, session);
        renderTargets();
        pollBinding(bind.dataset.bindTarget, session);
        return;
      }
      if (test) {
        if (test.dataset.bound !== 'true') {
          alert('当前目标尚未绑定会话。请先生成绑定码，并在对应飞书私聊或群聊中发送绑定命令。');
          return;
        }
        test.disabled = true;
        await secureApi(`/api/automation/targets/${encodeURIComponent(test.dataset.testTarget)}/test`, {method:'POST'});
        alert('测试消息已提交。'); return;
      }
      if (toggle) {
        const target = state.data.targets.find(item => item.id === toggle.dataset.toggleTarget);
        return await updateTarget(target.id, {preset:target.preset, overrides:target.overrides, enabled:!target.enabled});
      }
      if (save) {
        const target = state.data.targets.find(item => item.id === save.dataset.savePolicy);
        const card = save.closest('[data-target-card]');
        const number = name => Number(card.querySelector(`[data-policy-field="${name}"]`).value);
        const overrides = {regime_threshold:number('regime_threshold'), confirmation_bars:number('confirmation_bars'),
          cooldown_minutes:number('cooldown_minutes'), hourly_cap:number('hourly_cap'),
          news_thresholds:{market:number('news_market')}};
        return await updateTarget(target.id, {preset:target.preset, overrides});
      }
    } catch (error) { alert(error.message); }
    finally { if (test) test.disabled = false; }
  });

  document.getElementById('automation-jobs').addEventListener('click', async event => {
    const toggle = event.target.closest('[data-job-toggle]');
    const run = event.target.closest('[data-job-run]');
    if (!toggle && !run) return;
    const button = toggle || run; button.disabled = true;
    try {
      if (toggle) await secureApi(`/api/automation/jobs/${encodeURIComponent(toggle.dataset.jobToggle)}`, {
        method:'PATCH', headers:{'Content-Type':'application/json'},
        body:JSON.stringify({action:toggle.dataset.enabled === 'true' ? 'pause' : 'resume'}),
      });
      if (run) await secureApi(`/api/automation/jobs/${encodeURIComponent(run.dataset.jobRun)}/run`, {method:'POST'});
      state.loaded = false; await loadAutomation(true);
    } catch (error) { alert(error.message); }
    finally { button.disabled = false; }
  });
})();
