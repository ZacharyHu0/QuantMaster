const automationFeature = (() => {
  'use strict';

  const state = {
    loaded:false, loading:false, data:null, activeView:'overview', activeRecord:'runs',
    expandedJobId:'', expandedTargetId:'', advancedTargetId:'', channelDetails:new Map(),
    bindings:new Map(), targetFeedback:new Map(), targetSaving:new Set(),
    jobFeedback:new Map(), jobSaving:new Set(), diagnostics:null,
    recordCache:{runs:null, events:null, audit:null},
    recordErrors:{runs:'', events:'', audit:''}, recordLoading:new Set(),
    lastLoadedAt:null, pageFeedbackTimer:0, operationalRevision:'',
  };

  const viewNames = ['overview', 'jobs', 'messaging', 'records'];
  const recordViewNames = ['runs', 'events', 'audit'];
  const presets = {
    conservative:{regime_threshold:80, confirmation_bars:3, cooldown_minutes:60,
      news_thresholds:{holding:75, watchlist:85, market:90}, hourly_cap:3},
    balanced:{regime_threshold:65, confirmation_bars:2, cooldown_minutes:30,
      news_thresholds:{holding:65, watchlist:75, market:80}, hourly_cap:6},
    sensitive:{regime_threshold:50, confirmation_bars:1, cooldown_minutes:15,
      news_thresholds:{holding:50, watchlist:60, market:70}, hourly_cap:12},
  };
  const presetLabels = {conservative:'保守', balanced:'均衡', sensitive:'敏感'};
  const eventTypeLabels = {
    important_news:'重要资讯', market_turn:'盘中变盘', market_close:'收盘状态',
    task_report:'任务结果', task_failure:'任务失败',
  };
  const allEventTypes = Object.keys(eventTypeLabels);
  const jobLabels = {
    intraday_monitor:'盘中变盘监控', fast_news_scan:'财经快讯扫描',
    official_news_scan:'官方公告扫描', periodic_news_scan:'定期资讯扫描',
    daily_close_pipeline:'收盘决策流程', news_digest:'重要消息摘要',
    news_dead_letter_recovery:'资讯暂停项恢复', paper_rebalance_proposal:'模拟调仓建议',
  };
  const runStatusLabels = {
    queued:'排队中', pending:'等待中', running:'运行中', retrying:'等待重试',
    completed:'已完成', succeeded:'已完成', success:'已完成', failed:'运行失败',
    error:'运行失败', cancelled:'已取消', canceled:'已取消', paused:'已暂停',
  };
  const failingRunStatuses = new Set(['failed', 'error', 'dead', 'timed_out', 'timeout']);
  const jobKindLabels = {
    high_frequency_poll:'高频轮询', daily:'每日业务', time_window:'时间窗摄取', manual:'手工任务',
  };

  function openSettings(section) {
    document.dispatchEvent(new CustomEvent('quantmaster:navigate', {
      detail:{tab:'settings', section},
    }));
  }

  async function secureApi(path, options = {}) {
    return api(path, options);
  }

  function domId(value) {
    return String(value || '').replace(/[^a-zA-Z0-9_-]/g, '-');
  }

  function cleanJobName(value) {
    return String(value || '').replace(/^automation\./, '');
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

  function timeText(value) {
    if (!value) return '尚未刷新';
    const date = value instanceof Date ? value : new Date(value);
    return Number.isNaN(date.getTime()) ? '尚未刷新' : `上次刷新 ${date.toLocaleTimeString('zh-CN', {
      hour12:false, hour:'2-digit', minute:'2-digit', second:'2-digit',
    })}`;
  }

  function scheduleText(schedule) {
    if (!schedule) return '未设置计划';
    const base = schedule.type === 'interval'
      ? `每 ${schedule.minutes} 分钟`
      : `每日 ${(schedule.times || []).join(' / ')}`;
    const windows = schedule.windows || (schedule.window ? [schedule.window] : []);
    return `${base}${windows.length ? ` · ${windows.join('、')}` : ''}${schedule.weekdays ? ' · 工作日' : ''}`;
  }

  function publicAccount(channel) {
    return (state.data?.bot_accounts || []).find(item => item.channel === channel);
  }

  function latestRun(jobName) {
    const job = (state.data?.jobs || []).find(item => item.name === jobName);
    return job?.execution?.id ? job.execution : null;
  }

  function runtimeStatus() {
    return String(state.data?.runtime?.status || 'disabled');
  }

  function progressValue(value) {
    const number = Number(value);
    return Number.isFinite(number) ? Math.max(0, Math.min(100, Math.round(number))) : null;
  }

  function durationText(value) {
    const seconds = Math.max(0, Math.round(Number(value) || 0));
    if (seconds < 60) return `${seconds} 秒`;
    if (seconds < 3600) return `${Math.floor(seconds / 60)} 分 ${seconds % 60} 秒`;
    return `${Math.floor(seconds / 3600)} 小时 ${Math.floor((seconds % 3600) / 60)} 分`;
  }

  function queueText(queue = {}) {
    return `待处理 ${Number(queue.pending || 0)} · 运行 ${Number(queue.running || 0)} · 重试 ${Number(queue.retry_wait || 0)} · 死信 ${Number(queue.dead_letter || 0)}`;
  }

  function runStatus(value) {
    return String(value || '').toLowerCase();
  }

  function runTone(value) {
    const status = runStatus(value);
    if (failingRunStatuses.has(status)) return 'error';
    if (['cancelled', 'canceled', 'paused'].includes(status)) return 'notice';
    if (['queued', 'pending', 'running', 'retrying'].includes(status)) return 'warning';
    if (['completed', 'succeeded', 'success'].includes(status)) return 'healthy';
    return 'neutral';
  }

  function jobHealth(job) {
    const recent = latestRun(job.name);
    const recentStatus = runStatus(recent?.status);
    if (recent?.stalled?.is_stalled) {
      return {tone:'error', label:'任务停滞', order:0, recent};
    }
    if (failingRunStatuses.has(recentStatus)) {
      return {tone:'error', label:'最近失败', order:0, recent};
    }
    if (!job.enabled) return {tone:'notice', label:'已暂停', order:1, recent};
    if (runtimeStatus() !== 'running') {
      return {tone:'warning', label:'等待运行时', order:2, recent};
    }
    if (recent?.backoff?.active || ['retry', 'retrying', 'retry_wait'].includes(recentStatus)) {
      return {tone:'neutral', label:'合法退避', order:3, recent};
    }
    if (['queued', 'pending', 'running', 'cancelling', 'interrupted'].includes(recentStatus)) {
      return {tone:'warning', label:runStatusLabels[recentStatus] || '处理中', order:3, recent};
    }
    return {tone:'healthy', label:'按计划运行', order:3, recent};
  }

  function channelHealth(channel) {
    const account = publicAccount(channel);
    if (!account) {
      return channel === 'feishu'
        ? {tone:'warning', label:'主通道未配置', detail:'飞书是主要告警与命令通道，请先完成应用接入。'}
        : {tone:'neutral', label:'可选 · 未配置', detail:'微信仅作为补充提醒，不影响主自动化流程。'};
    }
    const accountStatus = String(account.status || 'configured');
    if (['degraded', 'needs_rebind'].includes(accountStatus)) {
      return {tone:'error', label:statusText(accountStatus),
        detail:account.last_error || '通道连接异常，请运行诊断后重新接入。'};
    }
    if (accountStatus === 'listening') {
      const inbound = state.data?.inbound?.[channel] || {total:0};
      if (channel === 'feishu' && !Number(inbound.total || 0)) {
        return {tone:'warning', label:'等待消息事件',
          detail:'长连接正常，但尚未收到消息事件，请检查订阅、权限和应用发布状态。'};
      }
      return {tone:'healthy', label:'监听正常',
        detail:`最近消息 ${dateText(inbound.last_received_at)}`};
    }
    if (['connecting', 'configured', 'waiting_message'].includes(accountStatus)) {
      return {tone:'warning', label:statusText(accountStatus),
        detail:runtimeStatus() === 'running' ? '通道仍在建立连接。' : '启用自动化运行时后才会建立连接。'};
    }
    return {tone:'warning', label:statusText(accountStatus), detail:'通道尚未进入稳定监听状态。'};
  }

  function targetHealth(target) {
    if (!target.enabled) return {tone:'neutral', label:'主动推送已关闭'};
    if (!target.target || !target.account_id) return {tone:'warning', label:'启用但尚未绑定'};
    if (['degraded', 'needs_rebind'].includes(String(target.status || ''))) {
      return {tone:'error', label:statusText(target.status)};
    }
    return {tone:'healthy', label:statusText(target.status || 'healthy')};
  }

  function statusMarkup(label, tone = 'neutral') {
    return `<span class="status-label" data-tone="${esc(tone)}">${esc(label)}</span>`;
  }

  function showPageFeedback(kind, message, {sticky = false} = {}) {
    const root = document.getElementById('automation-page-feedback');
    if (!root) return;
    clearTimeout(state.pageFeedbackTimer);
    root.className = `automation-page-feedback ${kind || 'info'}`;
    root.textContent = message || '';
    root.hidden = !message;
    if (message && !sticky && ['success', 'info'].includes(kind)) {
      state.pageFeedbackTimer = window.setTimeout(() => {
        root.hidden = true;
        root.textContent = '';
      }, 4200);
    }
  }

  function renderRuntimeHeader() {
    const runtime = document.getElementById('automation-runtime');
    const updated = document.getElementById('automation-updated-at');
    const current = runtimeStatus();
    const tone = current === 'running' ? 'healthy' : current === 'standby' ? 'warning' : 'error';
    runtime.className = `automation-status ${tone}`;
    runtime.textContent = current === 'running'
      ? `调度器运行中 · ${state.data.timezone}`
      : current === 'standby'
        ? `自动化已启用 · 等待调度租约 · ${state.data.timezone}`
        : '自动化未开启 · 请在设置中心启用';
    updated.textContent = timeText(state.lastLoadedAt);
  }

  function buildAttentionItems() {
    const items = [];
    const runtime = runtimeStatus();
    if (runtime !== 'running') {
      items.push({tone:runtime === 'standby' ? 'warning' : 'error',
        title:runtime === 'standby' ? '调度器正在等待运行租约' : '自动化运行时尚未启用',
        detail:runtime === 'standby'
          ? '当前进程不会重复执行任务；租约释放后会自动接管。'
          : '计划任务与消息监听均不会运行，请检查自动化总开关。',
        action:'打开运行时设置', settings:'automation'});
    }
    for (const job of state.data?.jobs || []) {
      const health = jobHealth(job);
      if (!['error', 'notice'].includes(health.tone)) continue;
      items.push({tone:health.tone,
        title:`${jobLabels[job.name] || job.name}${health.tone === 'error' ? '最近运行失败' : '已暂停'}`,
        detail:health.tone === 'error'
          ? `${health.recent?.error || health.recent?.result || '最近执行未完成，请查看运行记录。'}`
          : '该任务不会按计划运行；若为临时停用，可在任务调度中恢复。',
        action:'检查任务', view:'jobs', focus:`automation-job-${domId(job.name)}`, job:job.name});
    }
    const feishu = channelHealth('feishu');
    if (['error', 'warning'].includes(feishu.tone)) {
      items.push({tone:feishu.tone, title:`飞书主通道：${feishu.label}`, detail:feishu.detail,
        action:'检查飞书通道', view:'messaging', focus:'automation-channel-feishu', channel:'feishu'});
    }
    for (const target of state.data?.targets || []) {
      const health = targetHealth(target);
      if (!['error', 'warning'].includes(health.tone)) continue;
      items.push({tone:health.tone, title:`${target.label}：${health.label}`,
        detail:target.last_error || '该目标已启用，但当前无法可靠接收主动推送。',
        action:'检查推送目标', view:'messaging', focus:`automation-target-${domId(target.id)}`, target:target.id});
    }
    const order = {error:0, warning:1, notice:2};
    return items.sort((a, b) => (order[a.tone] ?? 3) - (order[b.tone] ?? 3));
  }

  function renderOverview() {
    const root = document.getElementById('automation-overview');
    const jobs = state.data?.jobs || [];
    const targets = state.data?.targets || [];
    const enabledJobs = jobs.filter(job => job.enabled).length;
    const failedJobs = jobs.filter(job => jobHealth(job).tone === 'error').length;
    const enabledTargets = targets.filter(target => target.enabled);
    const healthyTargets = enabledTargets.filter(target => targetHealth(target).tone === 'healthy').length;
    const feishu = channelHealth('feishu');
    const attention = buildAttentionItems();
    const upcoming = jobs.filter(job => job.enabled && job.next_run)
      .sort((a, b) => new Date(a.next_run) - new Date(b.next_run)).slice(0, 4);
    const runtime = runtimeStatus();
    const runtimeTone = runtime === 'running' ? 'healthy'
      : runtime === 'standby' ? 'warning' : 'error';
    const queue = state.data.queue_summary || {};
    const outbox = state.data.outbox || {};
    const queueRisk = Number(queue.failed || 0) + Number(queue.dead_letter || 0);
    const outboxRisk = Number(outbox.dead_letter || 0);
    const operationalRevision = JSON.stringify([
      runtime, queueRisk, queue.running, queue.retry_wait, outbox.dispatcher_status,
      outbox.pending, outbox.retry_wait, outbox.dead_letter,
      ...jobs.map(job => [job.name, jobHealth(job).label, job.execution?.stalled?.diagnostic_code || '']),
    ]);
    root.setAttribute('aria-live', operationalRevision === state.operationalRevision ? 'off' : 'polite');
    state.operationalRevision = operationalRevision;

    root.innerHTML = `
      <dl class="automation-health-rail" aria-label="自动化健康状态">
        <div><dt>运行时</dt><dd>${statusMarkup(runtime === 'running' ? '运行正常' : runtime === 'standby' ? '等待租约' : '已关闭', runtimeTone)}<small>${esc(state.data.timezone || '—')}</small></dd></div>
        <div><dt>计划任务</dt><dd>${statusMarkup(failedJobs ? `${failedJobs} 项失败` : `${enabledJobs}/${jobs.length} 已启用`, failedJobs ? 'error' : enabledJobs === jobs.length ? 'healthy' : 'notice')}<small>${jobs.length ? '按异常与暂停状态排序' : '尚未配置任务'}</small></dd></div>
        <div><dt>飞书主通道</dt><dd>${statusMarkup(feishu.label, feishu.tone)}<small>${esc(feishu.detail)}</small></dd></div>
        <div><dt>推送目标</dt><dd>${statusMarkup(enabledTargets.length ? `${healthyTargets}/${enabledTargets.length} 可用` : '未启用', healthyTargets === enabledTargets.length && enabledTargets.length ? 'healthy' : enabledTargets.length ? 'warning' : 'neutral')}<small>${targets.length} 个已登记会话</small></dd></div>
        <div><dt>Durable 队列</dt><dd>${statusMarkup(queueRisk ? `${queueRisk} 项需处理` : `${Number(queue.running || 0)} 运行 · ${Number(queue.queued || 0)} 排队`, queueRisk ? 'error' : Number(queue.retry_wait || 0) ? 'neutral' : 'healthy')}<small>退避 ${Number(queue.retry_wait || 0)} · 合并触发 ${Number(queue.coalesced_count || 0)}</small></dd></div>
        <div><dt>Outbox</dt><dd>${statusMarkup(outboxRisk ? `${outboxRisk} 项死信` : outbox.dispatcher_status === 'running' ? `${Number(outbox.pending || 0)} 待发送` : outbox.dispatcher_status === 'disabled' ? '已停用' : '未配置', outboxRisk ? 'error' : outbox.dispatcher_status === 'running' ? 'healthy' : 'neutral')}<small>重试 ${Number(outbox.retry_wait || 0)}${outbox.next_retry_at ? ` · ${dateText(outbox.next_retry_at)}` : ''}</small></dd></div>
      </dl>
      <div class="automation-overview-grid">
        <section class="automation-overview-section automation-attention" aria-labelledby="automation-attention-title">
          <header><div><span class="eyebrow">ATTENTION</span><h3 id="automation-attention-title">待处理项</h3></div><span class="automation-section-count">${attention.length ? `${attention.length} 项` : '全部正常'}</span></header>
          ${attention.length ? `<div class="automation-attention-list">${attention.map((item, index) => `
            <article class="automation-attention-item" data-tone="${esc(item.tone)}">
              <span class="automation-attention-index" aria-hidden="true">${String(index + 1).padStart(2, '0')}</span>
              <div><strong>${esc(item.title)}</strong><p>${esc(item.detail)}</p></div>
              <button class="automation-text-action" type="button" data-attention-view="${esc(item.view || '')}" data-attention-focus="${esc(item.focus || '')}" data-attention-job="${esc(item.job || '')}" data-attention-target="${esc(item.target || '')}" data-attention-channel="${esc(item.channel || '')}" data-attention-settings="${esc(item.settings || '')}">${esc(item.action)}</button>
            </article>`).join('')}</div>` : `
            <div class="automation-all-clear">
              <span class="automation-clear-mark" aria-hidden="true"></span>
              <div><strong>当前没有需要处理的自动化问题</strong><p>调度、飞书主通道和已启用推送目标均处于可用状态。</p></div>
              <button class="automation-text-action" type="button" data-attention-view="jobs">查看全部任务</button>
            </div>`}
        </section>
        <aside class="automation-overview-section automation-upcoming" aria-labelledby="automation-upcoming-title">
          <header><div><span class="eyebrow">NEXT RUNS</span><h3 id="automation-upcoming-title">接下来运行</h3></div><button class="automation-text-action" type="button" data-attention-view="jobs">管理任务</button></header>
          ${upcoming.length ? `<ol>${upcoming.map(job => `<li><div><strong>${esc(jobLabels[job.name] || job.name)}</strong><small>${esc(scheduleText(job.schedule))}</small></div><time>${dateText(job.next_run)}</time></li>`).join('')}</ol>` : `<div class="automation-empty">暂无即将运行的任务。启用任务后，这里会显示下一次执行时间。</div>`}
        </aside>
      </div>`;
    root.setAttribute('aria-busy', 'false');
  }

  function diagnosticsMarkup() {
    if (!state.diagnostics) return '';
    if (state.diagnostics.loading) {
      return '<div class="channel-diagnostic loading">正在检查凭据、运行时、长连接、消息事件和会话绑定…</div>';
    }
    if (state.diagnostics.error) {
      return `<div class="channel-diagnostic error"><strong>诊断未完成</strong><span>${esc(state.diagnostics.error)} 请稍后重试。</span></div>`;
    }
    const labels = {credential:'凭据', runtime:'运行时', websocket:'长连接', event:'消息事件', binding:'会话绑定'};
    return `<div class="channel-diagnostic-stages">${Object.entries(state.diagnostics.stages || {}).map(([key, value]) => {
      const tone = value.status === 'success' ? 'healthy' : value.status === 'error' ? 'error' : 'warning';
      return `<div class="channel-diagnostic" data-tone="${tone}"><strong>${esc(labels[key] || key)} · ${value.status === 'success' ? '通过' : value.status === 'error' ? '失败' : '待处理'}</strong><span>${esc(value.message)}</span></div>`;
    }).join('')}</div>`;
  }

  function renderChannels() {
    const root = document.getElementById('automation-channels');
    const channels = [
      {id:'feishu', title:'飞书应用 Bot', role:'主通道',
        summary:'长连接接收命令，并用结构化消息卡片推送告警。'},
      {id:'weixin', title:'腾讯微信 ClawBot', role:'可选提醒',
        summary:'iLink 能力有限，仅用于文本提醒与简单命令补充。'},
    ];
    root.innerHTML = channels.map(channel => {
      const account = publicAccount(channel.id);
      const health = channelHealth(channel.id);
      const explicit = state.channelDetails.get(channel.id);
      const expanded = explicit === undefined ? ['error', 'warning'].includes(health.tone) : explicit;
      const inbound = state.data?.inbound?.[channel.id] || {total:0, last_received_at:''};
      const meta = account
        ? `${channel.id === 'feishu' ? 'App ID' : 'Bot'} ${esc(account.account_id || '—')}`
        : channel.summary;
      const detail = channel.id === 'feishu'
        ? `<p>需开通以应用身份发消息、读取机器人单聊与群聊消息；订阅 <code>im.message.receive_v1</code> 并发布包含权限变更的应用版本。普通群消息只用于话题记忆。</p>
          ${Number(inbound.total || 0) ? `<p class="channel-received">已收到 ${Number(inbound.total)} 条消息事件 · 最近 ${dateText(inbound.last_received_at)}</p>` : ''}
          ${diagnosticsMarkup()}`
        : '<p>微信通道不参与主通道健康判断。需要补充文本提醒时，可在设置中心扫码接入。</p>';
      return `<article class="automation-channel-row ${expanded ? 'expanded' : ''}" id="automation-channel-${channel.id}" data-channel-row="${channel.id}" tabindex="-1">
        <div class="automation-channel-summary">
          <div class="automation-channel-title"><strong>${channel.title}</strong><span class="automation-role-label">${channel.role}</span></div>
          ${statusMarkup(health.label, health.tone)}
          <p>${meta}${account?.last_error ? ` · ${esc(account.last_error)}` : ''}</p>
          <div class="automation-channel-actions">
            <button class="ghost" type="button" data-manage-channel="automation">${account ? `管理${channel.id === 'feishu' ? '飞书' : '微信'}接入` : `接入${channel.id === 'feishu' ? '飞书' : '微信'}`}</button>
            ${channel.id === 'feishu' ? `<button class="ghost" type="button" data-feishu-diagnose ${account ? '' : 'disabled'}>运行五阶段诊断</button>` : ''}
            <button class="automation-disclosure" type="button" data-channel-expand="${channel.id}" aria-expanded="${expanded}" aria-controls="automation-channel-detail-${channel.id}">${expanded ? '收起接入详情' : '查看接入详情'}</button>
          </div>
        </div>
        <div class="automation-disclosure-shell" id="automation-channel-detail-${channel.id}"><div class="automation-channel-detail">${detail}</div></div>
      </article>`;
    }).join('');
  }

  function mergedPolicy(target) {
    const base = structuredClone(presets[target.preset] || presets.balanced);
    const override = target.overrides || {};
    Object.assign(base, override);
    base.news_thresholds = {...(presets[target.preset] || presets.balanced).news_thresholds,
      ...(override.news_thresholds || {})};
    return base;
  }

  function selectedEventTypes(target) {
    const configured = target.overrides?.event_types;
    return Array.isArray(configured)
      ? allEventTypes.filter(kind => configured.includes(kind))
      : [...allEventTypes];
  }

  function subscriptionSummary(target) {
    const selected = selectedEventTypes(target);
    if (!selected.length) return '不接收主动推送';
    if (selected.length === allEventTypes.length) return `订阅全部 ${selected.length} 类内容`;
    const names = selected.slice(0, 2).map(kind => eventTypeLabels[kind]);
    return `订阅 ${selected.length} 类 · ${names.join('、')}${selected.length > 2 ? '等' : ''}`;
  }

  function feedbackMarkup(targetId) {
    const feedback = state.targetFeedback.get(targetId);
    return `<div class="target-feedback ${esc(feedback?.kind || '')}" role="status" aria-live="polite">${esc(feedback?.message || '')}</div>`;
  }

  function bindingMarkup(target, session) {
    if (!session) return '';
    const command = `绑定 QuantMaster ${session.code}`;
    const noEvent = target.chat_type === 'group'
      ? '未收到群聊 @机器人 事件。请确认消息中真正 @QuantMaster，并检查群消息权限已经审批并随新版本发布。'
      : '尚未收到私聊消息事件，请检查消息订阅、单聊权限、应用发布状态和可用范围。';
    const status = {
      waiting:'等待飞书消息事件…', event_seen:'已收到消息，正在完成绑定…', no_event:noEvent,
      bound:'绑定成功，可以发送测试消息。', expired:'绑定码已过期，请重新生成。',
      error:`绑定状态检查失败：${session.error || '未知错误'}。请重新开始绑定。`,
    }[session.status] || '等待绑定';
    const instruction = target.chat_type === 'group'
      ? '在目标群真正 @QuantMaster，并发送下面的完整绑定命令。'
      : '打开 QuantMaster 机器人私聊，发送下面的完整绑定命令。';
    const tone = session.status === 'bound' ? 'healthy'
      : ['no_event', 'error', 'expired'].includes(session.status) ? 'error' : 'warning';
    return `<div class="binding-wizard" data-tone="${tone}">
      <ol><li>${esc(instruction)}</li><li>保持本页打开；收到事件后会自动完成并刷新状态。</li></ol>
      <div class="binding-command"><code>${esc(command)}</code><button class="ghost" type="button" data-copy-binding="${esc(command)}">复制绑定命令</button></div>
      <div class="binding-progress">${statusMarkup(status, tone)}<small>绑定码 10 分钟内有效</small></div>
    </div>`;
  }

  function renderTargets() {
    const root = document.getElementById('automation-targets');
    const targets = state.data?.targets || [];
    const owner = targets.find(target => target.id === 'feishu_owner');
    const ownerBound = Boolean(owner?.target && owner?.account_id && owner?.owner_actor);
    if (!targets.length) {
      root.innerHTML = '<div class="automation-empty"><strong>尚无推送目标</strong><p>完成 Bot 接入并绑定会话后，可在这里为每个目标设置独立策略。</p><button class="automation-text-action" type="button" data-manage-channel="automation">前往设置接入</button></div>';
      return;
    }
    root.innerHTML = targets.map(target => {
      const policy = mergedPolicy(target);
      const expanded = state.expandedTargetId === target.id;
      const advanced = expanded && state.advancedTargetId === target.id;
      const bound = Boolean(target.target && target.account_id);
      const binding = state.bindings.get(target.id);
      const eventTypes = selectedEventTypes(target);
      const saving = state.targetSaving.has(target.id);
      const health = targetHealth(target);
      const groupBlocked = target.id === 'feishu_group' && !ownerBound;
      const bindLabel = bound ? '重新绑定会话' : groupBlocked ? '绑定群聊（需先绑定管理员）'
        : target.id === 'feishu_owner' ? '绑定管理员私聊' : '绑定群聊';
      return `<article class="automation-target-row ${expanded ? 'expanded' : ''}" id="automation-target-${domId(target.id)}" data-target-card="${esc(target.id)}" tabindex="-1">
        <div class="automation-target-summary">
          <button class="automation-target-disclosure" type="button" data-target-expand="${esc(target.id)}" aria-expanded="${expanded}" aria-controls="automation-target-detail-${domId(target.id)}">
            <span class="automation-target-identity"><strong>${esc(target.label)}</strong><small>${bound ? `${esc(target.channel)} · ${esc(target.target)}` : `${esc(target.channel)} · 尚未绑定会话`}</small></span>
            <span class="automation-target-subscription">${esc(subscriptionSummary(target))}</span>
            <span class="automation-target-toggle-label">${expanded ? '收起策略' : '编辑策略'}</span>
          </button>
          <div class="automation-target-state">${statusMarkup(health.label, health.tone)}</div>
          <div class="segmented automation-preset" aria-label="${esc(target.label)}推送强度">
            ${Object.entries(presetLabels).map(([key, label]) => `<button type="button" data-policy="${key}" data-target="${esc(target.id)}" class="${target.preset === key ? 'active' : ''}" aria-pressed="${target.preset === key}" ${saving ? 'disabled' : ''}>${label}</button>`).join('')}
          </div>
        </div>
        ${feedbackMarkup(target.id)}
        <div class="automation-disclosure-shell" id="automation-target-detail-${domId(target.id)}"><div class="automation-target-detail">
          <div class="target-tools">
            <button class="ghost" type="button" data-policy-more="${esc(target.id)}" aria-expanded="${advanced}">${advanced ? '收起高级阈值' : '编辑高级阈值'}</button>
            ${target.channel === 'feishu' ? `<button class="ghost" type="button" data-bind-target="${esc(target.id)}" data-bind-blocked="${groupBlocked}">${bindLabel}</button>` : ''}
            <button class="ghost" type="button" data-test-target="${esc(target.id)}" data-bound="${bound}">发送测试消息</button>
            <button class="ghost" type="button" data-toggle-target="${esc(target.id)}" ${saving ? 'disabled' : ''}>${target.enabled ? '关闭主动推送' : '开启主动推送'}</button>
          </div>
          <fieldset class="target-content" ${saving ? 'disabled' : ''}>
            <legend>主动推送内容</legend>
            <div class="target-content-options">
              ${Object.entries(eventTypeLabels).map(([key, label]) => `<label><input type="checkbox" data-event-type="${key}" data-target="${esc(target.id)}" ${eventTypes.includes(key) ? 'checked' : ''}><span>${label}</span></label>`).join('')}
            </div>
            <p class="target-content-note ${eventTypes.length ? '' : 'muted-warning'}">${eventTypes.length ? '各会话独立订阅；取消某类后，高优先级事件也不会绕过。' : '未订阅任何内容；自动化与 Bot 监听仍会继续运行。'}</p>
          </fieldset>
          <div class="policy-details ${advanced ? 'expanded' : ''}" aria-hidden="${!advanced}">
            <div class="policy-details-inner">
              <label>变盘阈值<input data-policy-field="regime_threshold" type="number" min="0" max="100" value="${policy.regime_threshold}"></label>
              <label>确认 K 线<input data-policy-field="confirmation_bars" type="number" min="1" max="3" value="${policy.confirmation_bars}"></label>
              <label>冷却分钟<input data-policy-field="cooldown_minutes" type="number" min="15" max="120" value="${policy.cooldown_minutes}"></label>
              <label>重要消息阈值<input data-policy-field="news_market" type="number" min="0" max="100" value="${policy.news_thresholds.market}"></label>
              <label>每小时上限<input data-policy-field="hourly_cap" type="number" min="1" max="30" value="${policy.hourly_cap}"></label>
              <div class="policy-save"><button class="primary" type="button" data-save-policy="${esc(target.id)}" ${saving ? 'disabled' : ''}>${saving ? '正在保存高级阈值…' : '保存高级阈值'}</button></div>
            </div>
          </div>
          <div data-binding-result="${esc(target.id)}">${bindingMarkup(target, binding)}</div>
        </div></div>
      </article>`;
    }).join('');
  }

  function renderJobs() {
    const root = document.getElementById('automation-jobs');
    const jobs = [...(state.data?.jobs || [])].sort((a, b) => {
      const order = jobHealth(a).order - jobHealth(b).order;
      if (order) return order;
      return new Date(a.next_run || '9999-12-31') - new Date(b.next_run || '9999-12-31');
    });
    if (!jobs.length) {
      root.innerHTML = '<div class="automation-empty"><strong>尚无定时任务</strong><p>启用自动化任务后，这里会显示计划、下次运行和最近结果。</p></div>';
      return;
    }
    root.innerHTML = jobs.map(job => {
      const health = jobHealth(job);
      const expanded = state.expandedJobId === job.name;
      const saving = state.jobSaving.has(job.name);
      const feedback = state.jobFeedback.get(job.name);
      const recent = health.recent;
      const execution = job.execution || recent || {};
      const progress = progressValue(execution.progress);
      const queue = execution.queue || {};
      const backoff = execution.backoff || {};
      const stalled = execution.stalled || {};
      const recentLabel = recent
        ? `${runStatusLabels[runStatus(recent.status)] || recent.status} · ${dateText(recent.finished_at || recent.started_at)}`
        : '尚无最近运行记录';
      const recentDetail = recent?.error || (typeof recent?.result === 'string' ? recent.result : '');
      return `<article class="automation-job-row ${expanded ? 'expanded' : ''}" id="automation-job-${domId(job.name)}" data-job-row="${esc(job.name)}" tabindex="-1">
        <div class="automation-job-primary">
          <div><span><strong>${esc(jobLabels[job.name] || job.name)}</strong><small class="automation-job-kind">${esc(jobKindLabels[job.job_kind] || job.job_kind || '计划任务')}</small></span>${statusMarkup(health.label, health.tone)}</div>
          <small>${job.next_run ? `下次触发 ${dateText(job.next_run)}` : job.enabled ? '等待调度器计算下次时间' : '恢复后重新计算下次时间'} · ${Number(execution.running_instances || 0)} 个运行实例</small>
          ${progress === null || !execution.id ? '' : `<div class="automation-job-progress"><progress max="100" value="${progress}" aria-label="${esc(jobLabels[job.name] || job.name)}进度 ${progress}%"></progress><span>${progress}%</span></div>`}
        </div>
        <button class="automation-job-expand" type="button" data-job-expand="${esc(job.name)}" aria-expanded="${expanded}" aria-controls="automation-job-detail-${domId(job.name)}">${expanded ? '收起任务详情' : '展开任务详情'}</button>
        <div class="automation-job-detail-shell" id="automation-job-detail-${domId(job.name)}"><div class="automation-job-detail">
          <div class="automation-job-field"><span>运行计划</span><strong>${esc(scheduleText(job.schedule))}</strong></div>
          <div class="automation-job-field"><span>最近结果</span><strong>${esc(recentLabel)}</strong>${recentDetail ? `<small>${esc(recentDetail)}</small>` : ''}</div>
          <dl class="automation-job-facts">
            <div><dt>Durable Job</dt><dd>${execution.id ? `<a href="${esc(execution.links?.self || `/api/v1/jobs/${execution.id}`)}">${esc(execution.id)}</a>` : '—'}</dd></div>
            <div><dt>原子队列</dt><dd>${esc(queueText(queue))}</dd></div>
            <div><dt>合并触发</dt><dd>${Number(execution.coalesced_count || 0)} 次</dd></div>
            <div><dt>上次开始</dt><dd>${dateText(execution.started_at)}</dd></div>
            <div><dt>上次结束</dt><dd>${dateText(execution.finished_at)}</dd></div>
            <div><dt>当前耗时</dt><dd>${execution.started_at ? durationText(execution.elapsed_seconds) : '—'}</dd></div>
            <div><dt>心跳 / 最近原子项</dt><dd>${dateText(execution.heartbeat_at)} / ${dateText(execution.last_completed_unit_at)}</dd></div>
          </dl>
          ${backoff.active ? `<div class="automation-job-diagnostic" data-tone="neutral"><strong>合法退避${backoff.next_retry_at ? ` · 下次重试 ${dateText(backoff.next_retry_at)}` : ''}</strong><span>${esc(backoff.waiting_on || '等待重试条件满足')}</span></div>` : ''}
          ${stalled.is_stalled ? `<div class="automation-job-diagnostic" data-tone="error" role="status"><strong>任务停滞 · ${esc(stalled.diagnostic_code || 'stalled')}</strong><span>${esc(stalled.reason || '进度心跳已超出预期')} · 当前等待 ${esc(stalled.waiting_on || '未知对象')} · 观察于 ${dateText(stalled.observed_at)}</span></div>` : ''}
          <div class="job-actions">
            <button class="ghost" type="button" data-job-toggle="${esc(job.name)}" data-enabled="${job.enabled}" ${saving ? 'disabled' : ''}>${job.enabled ? '暂停任务' : '恢复任务'}</button>
            <button class="ghost" type="button" data-job-run="${esc(job.name)}" ${saving ? 'disabled' : ''}>立即运行任务</button>
          </div>
        </div></div>
        <div class="automation-row-feedback ${esc(feedback?.kind || '')}" role="status" aria-live="polite">${esc(feedback?.message || '')}</div>
      </article>`;
    }).join('');
  }

  function recordRoot(view) {
    return document.getElementById({runs:'automation-runs', events:'automation-events', audit:'automation-audit'}[view]);
  }

  function recordLoadingMarkup(label) {
    return `<div class="automation-record-loading" aria-label="正在读取${esc(label)}"><i></i><i></i><i></i></div>`;
  }

  function recordEmptyMarkup(title, detail) {
    return `<div class="automation-empty"><strong>${esc(title)}</strong><p>${esc(detail)}</p></div>`;
  }

  function renderRunRecords(items) {
    if (!items.length) return recordEmptyMarkup('暂无任务运行记录', '任务提交后会在这里显示排队、执行和完成状态。');
    return `<div class="automation-record-table-wrap"><table class="automation-record-table"><thead><tr><th>任务</th><th>状态</th><th>进度</th><th>阶段与详情</th><th>更新时间</th></tr></thead><tbody>${items.map(item => {
      const name = cleanJobName(item.type);
      const status = runStatus(item.status);
      const value = progressValue(item.progress);
      const progress = value === null ? '—' : `${value}%`;
      return `<tr><td data-label="任务"><strong>${esc(jobLabels[name] || name)}</strong><small>${esc(item.id || '')}</small></td><td data-label="状态">${statusMarkup(runStatusLabels[status] || status || '未知', runTone(status))}</td><td data-label="进度">${esc(progress)}</td><td data-label="阶段与详情"><strong>${esc(item.phase || '—')}</strong><small>${esc(item.detail || '')}</small></td><td data-label="更新时间"><time>${dateText(item.updated_at || item.created_at)}</time></td></tr>`;
    }).join('')}</tbody></table></div>`;
  }

  function renderEventRecords(items) {
    if (!items.length) return recordEmptyMarkup('暂无事件', '市场、资讯或任务产生事件后，会在这里显示入库时间和方向。');
    const directionLabels = {up:'偏多', down:'偏空', neutral:'中性'};
    return `<div class="automation-record-table-wrap"><table class="automation-record-table automation-event-table"><thead><tr><th>事件</th><th>类型</th><th>方向</th><th>评分</th><th>发生时间</th></tr></thead><tbody>${items.map(item => `<tr><td data-label="事件"><strong>${esc(item.payload?.title || item.kind || '未命名事件')}</strong></td><td data-label="类型">${esc(eventTypeLabels[item.kind] || item.kind || '—')}</td><td data-label="方向">${esc(directionLabels[item.direction] || item.direction || '中性')}</td><td data-label="评分"><span class="automation-score">${Number.isFinite(Number(item.score)) ? Number(item.score).toFixed(0) : '—'}</span></td><td data-label="发生时间"><time>${dateText(item.occurred_at)}</time></td></tr>`).join('')}</tbody></table></div>`;
  }

  function renderAuditRecords(items) {
    if (!items.length) return recordEmptyMarkup('暂无操作审计', '绑定、策略调整和任务变更发生后，会在这里留下记录。');
    return `<div class="automation-record-table-wrap"><table class="automation-record-table"><thead><tr><th>时间</th><th>操作者</th><th>动作</th><th>对象</th><th>结果</th></tr></thead><tbody>${items.map(item => `<tr><td data-label="时间"><time>${dateText(item.created_at)}</time></td><td data-label="操作者">${esc(item.actor)}</td><td data-label="动作"><strong>${esc(item.action)}</strong></td><td data-label="对象">${esc(item.object_type)} / ${esc(item.object_id)}</td><td data-label="结果">${esc(item.result)}</td></tr>`).join('')}</tbody></table></div>`;
  }

  function renderRecordView(view) {
    const root = recordRoot(view);
    if (!root) return;
    if (state.recordLoading.has(view)) {
      root.innerHTML = recordLoadingMarkup({runs:'任务运行记录', events:'事件流', audit:'操作审计'}[view]);
      return;
    }
    if (state.recordErrors[view]) {
      root.innerHTML = `<div class="automation-error-state"><strong>${esc({runs:'任务运行记录', events:'事件流', audit:'操作审计'}[view])}读取失败</strong><p>${esc(state.recordErrors[view])} 请检查本地服务后重试。</p><button class="ghost" type="button" data-record-retry="${view}">重新读取</button></div>`;
      return;
    }
    const items = state.recordCache[view];
    if (items === null) {
      root.innerHTML = `<div class="automation-empty">打开后读取${esc({runs:'任务运行记录', events:'事件流', audit:'操作审计'}[view])}</div>`;
      return;
    }
    root.innerHTML = view === 'runs' ? renderRunRecords(items)
      : view === 'events' ? renderEventRecords(items) : renderAuditRecords(items);
  }

  async function loadRecordView(view, force = false) {
    if (!recordViewNames.includes(view) || state.recordLoading.has(view)) return;
    if (!force && state.recordCache[view] !== null) {
      renderRecordView(view);
      return;
    }
    const paths = {
      runs:'/api/v1/automation/jobs', events:'/api/v1/automation/events?limit=50',
      audit:'/api/v1/automation/audit?limit=50',
    };
    state.recordLoading.add(view);
    state.recordErrors[view] = '';
    renderRecordView(view);
    try {
      const data = await api(paths[view]);
      state.recordCache[view] = view === 'runs' ? (data.runs || []).slice(0, 50) : (data.items || []).slice(0, 50);
    } catch (error) {
      state.recordErrors[view] = error.message || '本地接口暂时不可用。';
    } finally {
      state.recordLoading.delete(view);
      renderRecordView(view);
    }
  }

  function setActiveRecord(view, {focusTab = false} = {}) {
    if (!recordViewNames.includes(view)) return;
    state.activeRecord = view;
    document.querySelectorAll('[data-record-view]').forEach(button => {
      const active = button.dataset.recordView === view;
      button.setAttribute('aria-selected', String(active));
      button.classList.toggle('active', active);
      button.tabIndex = active ? 0 : -1;
      if (active && focusTab) button.focus();
    });
    document.querySelectorAll('[data-record-panel]').forEach(panel => {
      panel.hidden = panel.dataset.recordPanel !== view;
    });
    void loadRecordView(view);
  }

  function focusAutomationItem(id) {
    if (!id) return;
    window.requestAnimationFrame(() => {
      const target = document.getElementById(id);
      if (!target) return;
      target.focus({preventScroll:true});
      target.scrollIntoView({block:'center', behavior:window.matchMedia('(prefers-reduced-motion: reduce)').matches ? 'auto' : 'smooth'});
    });
  }

  function setActiveView(view, {focusTab = false, focusId = ''} = {}) {
    if (!viewNames.includes(view)) return;
    state.activeView = view;
    document.querySelectorAll('[data-automation-view]').forEach(button => {
      const active = button.dataset.automationView === view;
      button.setAttribute('aria-selected', String(active));
      button.classList.toggle('active', active);
      button.tabIndex = active ? 0 : -1;
      if (active && focusTab) button.focus();
    });
    document.querySelectorAll('[data-automation-panel]').forEach(panel => {
      panel.hidden = panel.dataset.automationPanel !== view;
    });
    if (view === 'records') setActiveRecord(state.activeRecord);
    focusAutomationItem(focusId);
  }

  function invalidateRecordCache() {
    state.recordCache = {runs:null, events:null, audit:null};
    state.recordErrors = {runs:'', events:'', audit:''};
  }

  function render() {
    renderRuntimeHeader();
    renderOverview();
    renderJobs();
    renderChannels();
    renderTargets();
    setActiveView(state.activeView);
  }

  function renderLoadError(error) {
    const message = error?.message || '本地自动化接口暂时不可用。';
    const runtime = document.getElementById('automation-runtime');
    runtime.className = 'automation-status error';
    runtime.textContent = '自动化状态读取失败';
    document.getElementById('automation-updated-at').textContent = '尚未刷新';
    const markup = `<div class="automation-error-state"><strong>无法读取自动化运营状态</strong><p>${esc(message)} 请确认本地服务正在运行后重试。</p><button class="ghost" type="button" data-automation-retry>重新读取状态</button></div>`;
    document.getElementById('automation-overview').innerHTML = markup;
    document.getElementById('automation-jobs').innerHTML = markup;
    document.getElementById('automation-channels').innerHTML = markup;
    document.getElementById('automation-targets').innerHTML = markup;
    showPageFeedback('error', `自动化状态读取失败：${message}`, {sticky:true});
  }

  async function loadAutomation(force = false, options = {}) {
    if (state.loading || (state.loaded && !force)) return;
    const refresh = document.getElementById('automation-refresh');
    state.loading = true;
    refresh.disabled = true;
    refresh.textContent = force ? '正在刷新…' : '正在读取…';
    if (force && options.invalidateRecords !== false) invalidateRecordCache();
    try {
      state.data = await api('/api/v1/automation/overview');
      state.loaded = true;
      state.lastLoadedAt = new Date();
      render();
      if (force && !options.silent) showPageFeedback('success', '自动化状态已刷新。');
      if (state.activeView === 'records' && state.recordCache[state.activeRecord] === null) {
        await loadRecordView(state.activeRecord, true);
      }
    } catch (error) {
      renderLoadError(error);
    } finally {
      state.loading = false;
      refresh.disabled = false;
      refresh.textContent = '刷新状态';
    }
  }

  async function updateTarget(targetId, body, successMessage = '推送设置已保存') {
    if (state.targetSaving.has(targetId)) return null;
    state.targetSaving.add(targetId);
    state.targetFeedback.set(targetId, {kind:'saving', message:'正在保存推送设置…'});
    renderTargets();
    try {
      const saved = await secureApi(`/api/v1/automation/targets/${encodeURIComponent(targetId)}/policy`, {
        method:'PATCH', headers:{'Content-Type':'application/json'}, body:JSON.stringify(body),
      });
      const index = state.data.targets.findIndex(item => item.id === targetId);
      if (index >= 0) state.data.targets[index] = saved;
      const time = new Date().toLocaleTimeString('zh-CN', {hour12:false, hour:'2-digit', minute:'2-digit'});
      state.targetFeedback.set(targetId, {kind:'success', message:`${successMessage} · ${time}`});
      invalidateRecordCache();
      renderOverview();
      return saved;
    } catch (error) {
      state.targetFeedback.set(targetId, {kind:'error',
        message:`推送设置保存失败：${error.message} 请检查输入或本地服务后重试。`});
      return null;
    } finally {
      state.targetSaving.delete(targetId);
      renderTargets();
    }
  }

  async function pollBinding(targetId, session) {
    while (state.bindings.get(targetId) === session && Date.now() < session.expiresAt) {
      await new Promise(resolve => setTimeout(resolve, 2000));
      try {
        const result = await secureApi(`/api/v1/automation/bindings/${encodeURIComponent(session.id)}`);
        if (result.status === 'bound') {
          session.status = 'bound';
          state.targetFeedback.set(targetId, {kind:'success', message:'会话绑定成功，可以发送测试消息。'});
          await loadAutomation(true, {silent:true});
          return;
        }
        if (result.status === 'expired') {
          session.status = 'expired';
          state.targetFeedback.set(targetId, {kind:'error', message:'绑定码已过期，请重新生成绑定命令。'});
          renderTargets();
          return;
        }
        if (Number(result.inbound?.total || 0) > session.initialInbound) {
          session.status = 'event_seen';
        } else if (Date.now() - session.startedAt > 15000) {
          session.status = 'no_event';
        }
        renderTargets();
      } catch (error) {
        session.status = 'error';
        session.error = error.message;
        state.targetFeedback.set(targetId, {kind:'error',
          message:`绑定状态检查失败：${error.message} 请重新生成绑定命令。`});
        renderTargets();
        return;
      }
    }
    if (state.bindings.get(targetId) === session) {
      session.status = 'expired';
      state.targetFeedback.set(targetId, {kind:'error', message:'绑定码已过期，请重新生成绑定命令。'});
      renderTargets();
    }
  }

  function moveTabFocus(event, selector, dataKey, activate) {
    if (!['ArrowLeft', 'ArrowRight', 'Home', 'End'].includes(event.key)) return;
    const buttons = [...event.currentTarget.querySelectorAll(selector)];
    const current = buttons.indexOf(event.target);
    if (current < 0) return;
    event.preventDefault();
    const next = event.key === 'Home' ? 0 : event.key === 'End' ? buttons.length - 1
      : (current + (event.key === 'ArrowRight' ? 1 : -1) + buttons.length) % buttons.length;
    activate(buttons[next].dataset[dataKey], {focusTab:true});
  }

  document.querySelector('.automation-view-tabs').addEventListener('click', event => {
    const button = event.target.closest('[data-automation-view]');
    if (button) setActiveView(button.dataset.automationView);
  });
  document.querySelector('.automation-view-tabs').addEventListener('keydown', event => {
    moveTabFocus(event, '[data-automation-view]', 'automationView', setActiveView);
  });
  document.querySelector('.automation-record-tabs').addEventListener('click', event => {
    const button = event.target.closest('[data-record-view]');
    if (button) setActiveRecord(button.dataset.recordView);
  });
  document.querySelector('.automation-record-tabs').addEventListener('keydown', event => {
    moveTabFocus(event, '[data-record-view]', 'recordView', setActiveRecord);
  });

  document.getElementById('automation-refresh').addEventListener('click', () => {
    void loadAutomation(true);
  });

  document.getElementById('tab-automation').addEventListener('click', event => {
    const retry = event.target.closest('[data-automation-retry]');
    if (retry) {
      void loadAutomation(true);
      return;
    }
    const recordRetry = event.target.closest('[data-record-retry]');
    if (recordRetry) {
      void loadRecordView(recordRetry.dataset.recordRetry, true);
      return;
    }
    const attention = event.target.closest('[data-attention-view], [data-attention-settings]');
    if (!attention) return;
    if (attention.dataset.attentionSettings) {
      openSettings(attention.dataset.attentionSettings);
      return;
    }
    if (attention.dataset.attentionJob) state.expandedJobId = attention.dataset.attentionJob;
    if (attention.dataset.attentionTarget) state.expandedTargetId = attention.dataset.attentionTarget;
    if (attention.dataset.attentionChannel) state.channelDetails.set(attention.dataset.attentionChannel, true);
    renderJobs(); renderChannels(); renderTargets();
    setActiveView(attention.dataset.attentionView || 'overview', {focusId:attention.dataset.attentionFocus || ''});
  });

  document.getElementById('automation-channels').addEventListener('click', event => {
    const manage = event.target.closest('[data-manage-channel]');
    if (manage) {
      openSettings(manage.dataset.manageChannel || 'automation');
      return;
    }
    const expand = event.target.closest('[data-channel-expand]');
    if (expand) {
      const channel = expand.dataset.channelExpand;
      const current = expand.getAttribute('aria-expanded') === 'true';
      state.channelDetails.set(channel, !current);
      renderChannels();
      return;
    }
    const diagnose = event.target.closest('[data-feishu-diagnose]');
    if (!diagnose) return;
    state.channelDetails.set('feishu', true);
    state.diagnostics = {loading:true, error:'', stages:null};
    renderChannels();
    secureApi('/api/v1/automation/channels/feishu/check', {method:'POST'})
      .then(data => {
        state.diagnostics = {loading:false, error:'', stages:data.stages || {}};
        showPageFeedback(data.status === 'success' ? 'success' : 'info',
          data.status === 'success' ? '飞书五阶段诊断全部通过。' : '飞书诊断完成，仍有项目需要处理。');
      })
      .catch(error => {
        state.diagnostics = {loading:false, error:error.message, stages:null};
        showPageFeedback('error', `飞书诊断未完成：${error.message} 请稍后重试。`, {sticky:true});
      })
      .finally(() => renderChannels());
  });

  document.getElementById('automation-targets').addEventListener('click', async event => {
    const manage = event.target.closest('[data-manage-channel]');
    const expand = event.target.closest('[data-target-expand]');
    const policy = event.target.closest('[data-policy]');
    const more = event.target.closest('[data-policy-more]');
    const bind = event.target.closest('[data-bind-target]');
    const test = event.target.closest('[data-test-target]');
    const toggle = event.target.closest('[data-toggle-target]');
    const save = event.target.closest('[data-save-policy]');
    const copy = event.target.closest('[data-copy-binding]');
    if (manage) {
      openSettings(manage.dataset.manageChannel || 'automation');
      return;
    }
    if (expand) {
      const targetId = expand.dataset.targetExpand;
      state.expandedTargetId = state.expandedTargetId === targetId ? '' : targetId;
      if (state.expandedTargetId !== targetId) state.advancedTargetId = '';
      renderTargets();
      return;
    }
    try {
      if (copy) {
        await navigator.clipboard.writeText(copy.dataset.copyBinding);
        const card = copy.closest('[data-target-card]');
        const targetId = card?.dataset.targetCard || '';
        if (targetId) state.targetFeedback.set(targetId, {kind:'success', message:'绑定命令已复制。'});
        renderTargets();
        return;
      }
      if (policy) {
        const target = state.data.targets.find(item => item.id === policy.dataset.target);
        const overrides = {...target.overrides, event_types:selectedEventTypes(target)};
        await updateTarget(target.id, {preset:policy.dataset.policy, overrides},
          `推送强度已设为${presetLabels[policy.dataset.policy]}`);
        return;
      }
      if (more) {
        const targetId = more.dataset.policyMore;
        state.expandedTargetId = targetId;
        state.advancedTargetId = state.advancedTargetId === targetId ? '' : targetId;
        renderTargets();
        return;
      }
      if (bind) {
        const targetId = bind.dataset.bindTarget;
        state.expandedTargetId = targetId;
        if (bind.dataset.bindBlocked === 'true') {
          state.targetFeedback.set(targetId, {kind:'error',
            message:'暂时不能绑定群聊：请先完成“飞书管理员私聊”绑定，再由该管理员在目标群发送绑定命令。'});
          renderTargets();
          return;
        }
        state.targetFeedback.set(targetId, {kind:'saving', message:'正在生成绑定命令…'});
        renderTargets();
        const data = await secureApi(`/api/v1/automation/bindings/code?target_id=${encodeURIComponent(targetId)}`, {method:'POST'});
        const target = state.data.targets.find(item => item.id === targetId);
        const inbound = state.data?.inbound?.feishu?.[target.chat_type] || {total:0};
        const session = {id:data.id, code:data.code, status:'waiting', startedAt:Date.now(),
          expiresAt:Number(data.expires_at) * 1000, initialInbound:Number(inbound.total || 0), error:''};
        state.bindings.set(targetId, session);
        state.targetFeedback.set(targetId, {kind:'success', message:'绑定命令已生成，请在 10 分钟内发送。'});
        renderTargets();
        void pollBinding(targetId, session);
        return;
      }
      if (test) {
        const targetId = test.dataset.testTarget;
        state.expandedTargetId = targetId;
        if (test.dataset.bound !== 'true') {
          state.targetFeedback.set(targetId, {kind:'error',
            message:'测试消息未发送：当前目标尚未绑定会话。请先生成并发送绑定命令。'});
          renderTargets();
          return;
        }
        state.targetFeedback.set(targetId, {kind:'saving', message:'正在提交测试消息…'});
        renderTargets();
        await secureApi(`/api/v1/automation/targets/${encodeURIComponent(targetId)}/test`, {method:'POST'});
        state.targetFeedback.set(targetId, {kind:'success', message:'测试消息已提交，请在目标会话中确认。'});
        renderTargets();
        invalidateRecordCache();
        return;
      }
      if (toggle) {
        const target = state.data.targets.find(item => item.id === toggle.dataset.toggleTarget);
        await updateTarget(target.id, {
          preset:target.preset, overrides:target.overrides, enabled:!target.enabled,
        }, target.enabled ? '主动推送已关闭' : '主动推送已开启');
        return;
      }
      if (save) {
        const target = state.data.targets.find(item => item.id === save.dataset.savePolicy);
        const card = save.closest('[data-target-card]');
        const fields = [...card.querySelectorAll('[data-policy-field]')];
        const invalid = fields.find(field => !field.checkValidity());
        if (invalid) {
          state.targetFeedback.set(target.id, {kind:'error',
            message:'高级阈值未保存：请检查标红字段是否处于允许范围。'});
          renderTargets();
          const replacement = document.querySelector(`[data-target-card="${CSS.escape(target.id)}"] [data-policy-field="${CSS.escape(invalid.dataset.policyField)}"]`);
          replacement?.focus();
          return;
        }
        const number = name => Number(card.querySelector(`[data-policy-field="${name}"]`).value);
        const overrides = {regime_threshold:number('regime_threshold'),
          confirmation_bars:number('confirmation_bars'), cooldown_minutes:number('cooldown_minutes'),
          hourly_cap:number('hourly_cap'), news_thresholds:{market:number('news_market')},
          event_types:selectedEventTypes(target)};
        await updateTarget(target.id, {preset:target.preset, overrides}, '高级阈值已保存');
      }
    } catch (error) {
      const card = event.target.closest('[data-target-card]');
      const targetId = card?.dataset.targetCard || '';
      if (targetId) {
        state.targetFeedback.set(targetId, {kind:'error',
          message:`操作未完成：${error.message} 请检查通道状态后重试。`});
        renderTargets();
      } else {
        showPageFeedback('error', `操作未完成：${error.message} 请稍后重试。`, {sticky:true});
      }
    }
  });

  document.getElementById('automation-targets').addEventListener('change', async event => {
    const input = event.target.closest('[data-event-type]');
    if (!input) return;
    const target = state.data.targets.find(item => item.id === input.dataset.target);
    const card = input.closest('[data-target-card]');
    const eventTypes = [...card.querySelectorAll('[data-event-type]:checked')].map(item => item.dataset.eventType);
    const overrides = {...target.overrides, event_types:eventTypes};
    const message = eventTypes.length
      ? `推送内容已保存（${eventTypes.length} 类）`
      : '已保存：当前目标不接收主动推送';
    await updateTarget(target.id, {preset:target.preset, overrides}, message);
  });

  document.getElementById('automation-jobs').addEventListener('click', async event => {
    const expand = event.target.closest('[data-job-expand]');
    if (expand) {
      const name = expand.dataset.jobExpand;
      state.expandedJobId = state.expandedJobId === name ? '' : name;
      renderJobs();
      return;
    }
    const toggle = event.target.closest('[data-job-toggle]');
    const run = event.target.closest('[data-job-run]');
    if (!toggle && !run) return;
    const name = (toggle || run).dataset.jobToggle || (toggle || run).dataset.jobRun;
    if (state.jobSaving.has(name)) return;
    state.jobSaving.add(name);
    state.jobFeedback.set(name, {kind:'saving',
      message:toggle ? '正在更新任务状态…' : '正在提交立即运行请求…'});
    renderJobs();
    try {
      if (toggle) {
        const pausing = toggle.dataset.enabled === 'true';
        await secureApi(`/api/v1/automation/jobs/${encodeURIComponent(name)}`, {
          method:'PATCH', headers:{'Content-Type':'application/json'},
          body:JSON.stringify({action:pausing ? 'pause' : 'resume'}),
        });
        state.jobFeedback.set(name, {kind:'success', message:pausing ? '任务已暂停。' : '任务已恢复并重新加入调度。'});
      } else {
        const result = await secureApi(`/api/v1/automation/jobs/${encodeURIComponent(name)}/run`, {method:'POST'});
        const progress = progressValue(result.progress);
        state.jobFeedback.set(name, {kind:'success',
          message:result.created === false
            ? `已连接同一任务 ${result.job_id || result.run_id || ''}${progress === null ? '' : ` · 当前 ${progress}%`}，继续显示其原有进度。`
            : `任务 ${result.job_id || result.run_id || ''} 已提交，正在后台排队执行。`});
      }
      invalidateRecordCache();
      await loadAutomation(true, {silent:true, invalidateRecords:false});
    } catch (error) {
      state.jobFeedback.set(name, {kind:'error',
        message:`任务操作未完成：${error.message} 请检查运行时状态后重试。`});
    } finally {
      state.jobSaving.delete(name);
      renderJobs();
      renderOverview();
    }
  });

  function unmount() {
    clearTimeout(state.pageFeedbackTimer);
    state.pageFeedbackTimer = 0;
  }

  return {
    mount: () => loadAutomation(false),
    unmount,
    refresh: () => loadAutomation(true),
  };
})();

export const {mount, unmount, refresh} = automationFeature;
