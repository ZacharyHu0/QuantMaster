(() => {
  'use strict';

  const state = {
    loaded: false,
    loading: false,
    items: [],
    nextCursor: null,
    hasMore: false,
    sources: [],
    selectedSource: null,
    clearToken: false,
    updatedIds: new Set(),
    annotationTimer: null,
    annotationHideTimer: null,
    annotationStatsTimer: null,
    annotationStartedAt: 0,
    annotationBusy: false,
    queue: null,
    eventFocusDays: 7,
    eventFocusLoadedDays: null,
    eventFocusRequest: 0,
    eventFocusRetryDays: 7,
    tasks: new Map(),
  };

  const feed = document.getElementById('news-out');
  const filterForm = document.getElementById('news-form');
  const sourceForm = document.getElementById('news-source-editor');

  function html(value) {
    return String(value ?? '').replace(/[&<>"']/g, char => ({
      '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;',
    }[char]));
  }

  function safeUrl(value) {
    try {
      const url = new URL(value, window.location.origin);
      return ['http:', 'https:'].includes(url.protocol) ? html(url.href) : '';
    } catch (_) { return ''; }
  }

  function report(message, error, level = 'error') {
    const detail = error?.message || String(error || '');
    if (window.reportLocalError && level === 'error') {
      window.reportLocalError('资讯工作台', message, error);
      return;
    }
    const live = document.getElementById('news-live-state');
    live.className = `news-live-state ${level === 'error' ? 'degraded' : 'ready'}`;
    live.innerHTML = `<i></i>${html(detail ? `${message}：${detail}` : message)}`;
  }

  function sourceFeedback(kind = '', message = '') {
    const target = document.getElementById('news-source-feedback');
    target.className = `source-feedback ${kind}`;
    target.textContent = message;
  }

  async function api(path, options = {}) {
    return window.QuantMasterAPI(path, options);
  }

  async function secure(path, options = {}) {
    await window.QuantMasterManagement.ensureSettings();
    return window.QuantMasterManagement.request(path, options);
  }

  async function submitAnnotationTask(mode, ids) {
    const accepted = await secure('/api/v1/news/reanalyze', {
      method: 'POST', body: {mode, ids: ids?.length ? ids : undefined},
    });
    if (!(accepted.job?.id || accepted.id)) throw new Error('服务未返回资讯任务编号');
    return accepted.job || accepted;
  }

  function taskIsActive(task) {
    return ['queued', 'running', 'cancelling'].includes(task.status)
      || (task.status === 'interrupted' && !task.manual_retry_required);
  }

  function retainRecentNewsTasks() {
    const terminal = [...state.tasks.values()]
      .filter(task => !taskIsActive(task))
      .sort((left, right) => Number(right._finishedAt || 0) - Number(left._finishedAt || 0));
    for (const task of terminal.slice(4)) state.tasks.delete(task.id);
  }

  function renderNewsTasks() {
    retainRecentNewsTasks();
    const tasks = [...state.tasks.values()];
    if (!tasks.length) return false;
    const live = document.getElementById('news-live-state');
    const failed = tasks.some(task => !taskIsActive(task) &&
      !['completed', 'completed_with_errors'].includes(task.status));
    live.className = `news-live-state ${failed ? 'degraded' : 'ready'}`;
    live.innerHTML = tasks.map(task => {
      const label = task._label || '资讯任务';
      const message = `${label}：${task.phase || task.status}${task.detail ? ` · ${task.detail}` : ''}`;
      const cancel = taskIsActive(task) && task.status !== 'cancelling'
        ? ` <button type="button" data-news-task-cancel="${html(task.id)}">取消</button>` : '';
      const retry = task.manual_retry_required && task.can_retry
        ? ` <button type="button" data-news-task-retry="${html(task.id)}">按当前配置重试</button>` : '';
      return `<span class="news-task-state"><i></i>${html(message)}${cancel}${retry}</span>`;
    }).join(' ');
    live.onclick = async event => {
      const button = event.target.closest('[data-news-task-cancel]');
      const task = button && state.tasks.get(button.dataset.newsTaskCancel);
      if (task) {
        await secure(task.links?.cancel || `/api/v1/jobs/${encodeURIComponent(task.id)}/cancel`, {method: 'POST'});
        state.tasks.set(task.id, {...task, status: 'cancelling', phase: '正在取消'});
        renderNewsTasks();
        return;
      }
      const retryButton = event.target.closest('[data-news-task-retry]');
      const retryTask = retryButton && state.tasks.get(retryButton.dataset.newsTaskRetry);
      if (!retryTask) return;
      const retried = await secure(
        retryTask.links?.retry || `/api/v1/jobs/${encodeURIComponent(retryTask.id)}/retry`,
        {method: 'POST'},
      );
      void watchNewsTask({...retried, _label: retryTask._label}, retryTask._label).catch(error =>
        report(`${retryTask._label || '资讯任务'}未完成`, error));
    };
    return true;
  }

  async function watchNewsTask(task, label, onProgress = null) {
    const id = task?.id;
    if (!id) throw new Error('服务未返回资讯任务编号');
    let current = {...task, _label: label};
    state.tasks.set(id, current);
    renderNewsTasks();
    if (onProgress) onProgress(current);
    while (taskIsActive(current)) {
      const job = await api(current.links?.self || `/api/v1/jobs/${encodeURIComponent(id)}`);
      current = {...job, _label: label};
      if (!taskIsActive(current)) current._finishedAt = Date.now();
      state.tasks.set(id, current);
      renderNewsTasks();
      if (onProgress) onProgress(current);
      if (taskIsActive(current)) await new Promise(resolve => setTimeout(resolve, 500));
    }
    if (current.manual_retry_required) return current;
    if (current.status !== 'completed' && current.status !== 'completed_with_errors') {
      const finishedAt = current._finishedAt;
      window.setTimeout(() => {
        if (state.tasks.get(id)?._finishedAt !== finishedAt) return;
        state.tasks.delete(id);
        if (!renderNewsTasks()) renderSourceHealth();
      }, 15000);
      throw new Error(current.detail || `${label}未完成`);
    }
    await Promise.all([loadFeed(), loadStats(), loadEventFocus(), loadSources()]);
    const finishedAt = current._finishedAt;
    window.setTimeout(() => {
      if (state.tasks.get(id)?._finishedAt !== finishedAt) return;
      state.tasks.delete(id);
      if (!renderNewsTasks()) renderSourceHealth();
    }, 15000);
    return current;
  }

  function elapsedText() {
    const seconds = Math.max(0, Math.floor((performance.now() - state.annotationStartedAt) / 1000));
    if (seconds < 60) return `已用时 ${seconds} 秒`;
    return `已用时 ${Math.floor(seconds / 60)} 分 ${seconds % 60} 秒`;
  }

  function setAnnotationProgress({percent = 0, phase, detail, count} = {}) {
    const panel = document.getElementById('news-annotation-progress');
    const value = Math.max(0, Math.min(100, Math.round(percent)));
    panel.style.setProperty('--annotation-progress', value / 100);
    document.getElementById('news-annotation-percent').textContent = `${value}%`;
    document.getElementById('news-annotation-track').setAttribute('aria-valuenow', String(value));
    if (phase) document.getElementById('news-annotation-phase').textContent = phase;
    if (detail) document.getElementById('news-annotation-detail').textContent = detail;
    if (count) document.getElementById('news-annotation-count').textContent = count;
  }

  function startAnnotationProgress({phase = '准备标注队列', detail = '正在读取可处理的待标注资讯…'} = {}) {
    const panel = document.getElementById('news-annotation-progress');
    clearInterval(state.annotationTimer);
    clearTimeout(state.annotationHideTimer);
    state.annotationStartedAt = performance.now();
    panel.hidden = false;
    panel.className = 'news-annotation-progress running';
    document.getElementById('news-annotation-elapsed').textContent = '已用时 0 秒';
    setAnnotationProgress({
      percent: 0, phase, detail, count: '0 / 0',
    });
    state.annotationTimer = setInterval(() => {
      document.getElementById('news-annotation-elapsed').textContent = elapsedText();
    }, 1000);
  }

  function finishAnnotationProgress(kind, phase, detail, percent = 100) {
    const panel = document.getElementById('news-annotation-progress');
    clearInterval(state.annotationTimer);
    panel.className = `news-annotation-progress ${kind}`;
    document.getElementById('news-annotation-elapsed').textContent = elapsedText();
    setAnnotationProgress({percent, phase, detail});
    clearTimeout(state.annotationHideTimer);
    state.annotationHideTimer = setTimeout(() => { panel.hidden = true; }, 7000);
  }

  function localDate(value) {
    if (!value) return {day: '时间未知', time: ''};
    const parsed = new Date(value);
    if (Number.isNaN(parsed.getTime())) return {day: String(value).slice(0, 10), time: ''};
    const parts = new Intl.DateTimeFormat('zh-CN', {
      timeZone: 'Asia/Shanghai', month: '2-digit', day: '2-digit',
      hour: '2-digit', minute: '2-digit', hour12: false,
    }).formatToParts(parsed).reduce((result, part) => ({...result, [part.type]: part.value}), {});
    return {day: `${parts.month}-${parts.day}`, time: `${parts.hour}:${parts.minute}`};
  }

  function sourceName(item) {
    return item.source_name || state.sources.find(source => source.id === item.source_id)?.name || item.source_id;
  }

  function retryWindow(value) {
    const epoch = Number(value || 0);
    if (!epoch) return {relative: '未安排', absolute: ''};
    const delta = epoch - Date.now() / 1000;
    let relative = '现在可以执行';
    if (delta > 0) {
      const minutes = Math.ceil(delta / 60);
      relative = minutes < 60 ? `${minutes} 分钟后` :
        minutes < 1440 ? `${Math.ceil(minutes / 60)} 小时后` : `${Math.ceil(minutes / 1440)} 天后`;
    }
    const absolute = new Intl.DateTimeFormat('zh-CN', {
      timeZone: 'Asia/Shanghai', month: '2-digit', day: '2-digit',
      hour: '2-digit', minute: '2-digit', hour12: false,
    }).format(new Date(epoch * 1000));
    return {relative, absolute};
  }

  function statusLabel(status) {
    return {
      complete: '已标注', pending: '待标注', failed: '退避重试',
      recovery: '恢复中', dead_letter: '已暂停',
    }[status] || status || '待标注';
  }

  function failureTemplate(item) {
    const status = String(item.analysis_status || '');
    if (!['failed', 'dead_letter'].includes(status)) return '';
    const dead = status === 'dead_letter';
    const attempts = Number(item.analysis_attempts || 0);
    const recoveryCount = Number(item.analysis_recovery_count || 0);
    const window = retryWindow(item.next_retry_at);
    const action = dead ? '恢复此项' : '立即重试此项';
    const code = String(item.last_failure_code || 'unknown').toLocaleUpperCase('en-US');
    const scheduleLabel = dead ? '自动恢复窗口' : '自动重试';
    const reason = item.analysis_error || '未记录具体错误信息';
    return `<section class="news-failure-panel${dead ? ' dead' : ''}" aria-label="${dead ? '暂停项诊断' : '分析失败诊断'}">
      <div class="news-failure-copy">
        <div class="news-failure-head"><strong>${dead ? '自动重试已暂停' : '本次分析未完成'}</strong><span>${dead ? '可手动恢复' : '仍在自动退避队列'}</span></div>
        <p>原因：${html(reason)}</p>
        <div class="news-failure-meta"><span>错误码 ${html(code)}</span><span>连续尝试 ${attempts} 次</span><span>${scheduleLabel} ${html(window.relative)}${window.absolute ? ` · ${html(window.absolute)}` : ''}</span>${dead ? `<span>已手动恢复 ${recoveryCount} 次</span>` : ''}</div>
      </div>
      <button class="news-item-retry" type="button" data-news-retry="${dead ? 'dead_letter' : 'failed'}" data-news-id="${Number(item.id)}" data-retry-ready="true" aria-label="${html(action)}：${html(item.title || '')}">
        <svg viewBox="0 0 20 20" aria-hidden="true"><path d="M15.6 7.25A6 6 0 1 0 15.3 13M15.6 3.8v3.45h-3.45"/></svg><span data-action-label>${html(action)}</span>
      </button>
    </section>`;
  }

  function eventTemplate(item) {
    const timestamp = localDate(item.first_seen_at || item.published_at);
    const sentiment = Number(item.sentiment || 0);
    const sentimentClass = sentiment > .15 ? 'positive' : sentiment < -.15 ? 'negative' : 'neutral';
    const score = Math.round(Number(item.importance_score || 0));
    const sectors = Array.isArray(item.sectors) ? item.sectors : [];
    const tags = [
      item.is_official ? '<span class="news-tag official">官方</span>' : '',
      item.event_type ? `<span class="news-tag">${html(item.event_type)}</span>` : '',
      `<span class="news-tag ${html(item.analysis_status)}">${html(statusLabel(item.analysis_status))}</span>`,
      ...sectors.slice(0, 3).map(sector => `<span class="news-tag sector">${html(sector)}</span>`),
      sectors.length > 3 ? `<span class="news-tag sector">+${sectors.length - 3}</span>` : '',
      ...(item.symbols || []).slice(0, 4).map(symbol => `<span class="news-tag symbol">${html(symbol)}</span>`),
    ].join('');
    const link = safeUrl(item.url);
    const updated = state.updatedIds.has(Number(item.id)) ? ' stream-updated' : '';
    return `<article class="news-event${updated}" data-news-id="${Number(item.id)}" data-content-truncated="${Boolean(item.content_truncated)}">
      <button class="news-event-main" type="button" aria-expanded="false">
        <span class="news-event-time"><strong>${html(timestamp.time)}</strong>${html(timestamp.day)}</span>
        <span><span class="news-event-title">${html(item.title)}</span>
          <span class="news-event-summary">${html(item.summary || item.content || '等待结构化摘要')}</span>
          <span class="news-event-meta"><span class="news-tag">${html(sourceName(item))}</span>${tags}</span>
        </span>
        <span class="news-event-score ${sentimentClass}"><strong>${sentiment > 0 ? '+' : ''}${sentiment.toFixed(2)}</strong><span>${score} IMP</span></span>
      </button>
      <div class="news-event-detail">
        <div class="news-detail-copy">${html(item.content || item.summary || '暂无正文')}</div>
        ${failureTemplate(item)}
        <div class="news-detail-metric"><span>置信度</span><strong>${Math.round(Number(item.confidence || 0) * 100)}%</strong></div>
        <div class="news-detail-metric"><span>影响范围</span><strong>${html(item.scope || '待判断')}</strong></div>
        <div class="news-detail-metric"><span>相关板块</span><strong>${html(sectors.join('、') || '未映射')}</strong></div>
        <div class="news-detail-metric"><span>首次获取</span><strong>${html(item.first_seen_at || '—')}</strong></div>
        ${link ? `<a class="news-detail-link" href="${link}" target="_blank" rel="noopener noreferrer">查看原始来源 ↗</a>` : ''}
      </div>
    </article>`;
  }

  function renderFeed(append = false) {
    if (!state.items.length) {
      feed.innerHTML = '<div class="news-empty"><div><strong>当前筛选下没有资讯</strong>调整筛选条件，或立即同步已启用的来源。</div></div>';
    } else {
      const markup = state.items.map(eventTemplate).join('');
      if (append) feed.insertAdjacentHTML('beforeend', markup);
      else feed.innerHTML = markup;
    }
    const caption = document.getElementById('news-result-caption');
    caption.textContent = `当前显示 ${feed.querySelectorAll('.news-event').length} 条 · 点击展开证据`;
    const more = document.getElementById('news-load-more');
    more.hidden = !state.hasMore;
    more.disabled = false;
    state.updatedIds.clear();
    refreshAnnotationAvailability();
  }

  function matchesCurrentFilters(item) {
    const filters = Object.fromEntries(new FormData(filterForm));
    const query = String(filters.q || '').trim().toLocaleLowerCase('zh-CN');
    const searchable = `${item.title || ''} ${item.summary || ''} ${item.content || ''}`
      .toLocaleLowerCase('zh-CN');
    if (query && !searchable.includes(query)) return false;
    if (filters.group && item.source_group !== filters.group) return false;
    if (filters.source && item.source_id !== filters.source) return false;
    if (filters.status && item.analysis_status !== filters.status) return false;
    const sentiment = Number(item.sentiment || 0);
    if (filters.sentiment === 'positive' && sentiment <= .15) return false;
    if (filters.sentiment === 'negative' && sentiment >= -.15) return false;
    if (filters.sentiment === 'neutral' && (sentiment < -.15 || sentiment > .15)) return false;
    return true;
  }

  function mergeAnnotationItems(items) {
    const byId = new Map(state.items.map(item => [Number(item.id), item]));
    const updated = new Set();
    for (const item of items || []) {
      const id = Number(item.id);
      byId.set(id, {...(byId.get(id) || {}), ...item});
      updated.add(id);
    }
    state.items = [...byId.values()].filter(matchesCurrentFilters);
    const importance = filterForm.elements.sort.value === 'importance';
    state.items.sort(importance
      ? (left, right) => Number(right.importance_score || 0) -
        Number(left.importance_score || 0) || Number(right.id) - Number(left.id)
      : (left, right) => Number(right.id) - Number(left.id));
    state.items = state.items.slice(0, 40);
    state.updatedIds = updated;
    renderFeed();
  }

  function queryString(cursor = null) {
    const formData = new FormData(filterForm);
    const query = new URLSearchParams({limit: '40'});
    for (const [key, value] of formData) if (String(value).trim()) query.set(key, String(value).trim());
    if (cursor) query.set('cursor', String(cursor));
    return query.toString();
  }

  async function loadFeed({append = false} = {}) {
    if (state.loading) return;
    state.loading = true;
    if (!append) feed.innerHTML = '<div class="news-skeleton" aria-label="加载中"><i></i><i></i><i></i></div>';
    try {
      const data = await api(`/api/v1/news?${queryString(append ? state.nextCursor : null)}`);
      state.items = data.items || [];
      state.nextCursor = data.next_cursor;
      state.hasMore = Boolean(data.has_more);
      renderFeed(append);
    } catch (error) {
      feed.innerHTML = `<div class="news-empty"><div><strong>资讯库暂不可用</strong>${html(error.message)}</div></div>`;
      report('读取资讯失败', error);
    } finally { state.loading = false; }
  }

  function factorChart(series, scaleMax) {
    const container = document.getElementById('news-factor-chart');
    if (!series?.length) {
      if (typeof disposeChart === 'function') disposeChart('news-factor-chart');
      container.innerHTML = '<span class="news-chart-empty">暂无足够的已标注资讯</span>';
      return;
    }
    const maxAbs = Math.max(10, Number(scaleMax) || 20);
    if (!window.echarts?.getInstanceByDom(container)) container.replaceChildren();
    const chart = mkChart('news-factor-chart');
    chart.setOption(baseOpt({
      grid: {left: 28, right: 8, top: 10, bottom: 18},
      tooltip: {trigger: 'axis', valueFormatter: value => `${Number(value).toFixed(1)}`},
      xAxis: {
        type: 'category', boundaryGap: false, data: series.map(item => item[0]),
        axisLabel: {show: false}, axisLine: {lineStyle: {color: AXIS}},
      },
      yAxis: {
        type: 'value', min: -maxAbs, max: maxAbs, interval: maxAbs,
        axisLabel: {color: MUTED, fontSize: 9, formatter: value => value > 0 ? `+${value}` : String(value)},
        splitLine: {lineStyle: {color: GRID}},
      },
      series: [{
        id: 'market-sentiment', name: '大盘情绪', type: 'line', showSymbol: false,
        data: series.map(item => Math.max(-maxAbs, Math.min(maxAbs, Number(item[1] || 0) * 100))),
        lineStyle: {width: 2, color: CHART_COLORS.primary},
        markLine: {silent: true, symbol: 'none', label: {show: false},
          lineStyle: {color: AXIS, width: 1}, data: [{yAxis: 0}]},
      }],
    }), {notMerge: true});
  }

  function refreshAnnotationAvailability() {
    const queue = state.queue;
    const actions = [
      {id: 'news-reanalyze', count: Number(queue?.pending || 0), label: '处理待标注'},
      {id: 'news-retry-failed', count: Number(queue?.failed || 0), label: '重试失败项'},
      {id: 'news-recover-dead', count: Number(queue?.manual_recoverable_dead_letter || 0), label: '恢复暂停项'},
    ];
    for (const action of actions) {
      const button = document.getElementById(action.id);
      button.disabled = state.annotationBusy || !queue || action.count <= 0;
      button.setAttribute('aria-label', `${action.label}，${action.count} 条`);
      if (action.id === 'news-reanalyze') {
        button.title = action.count ? `分析 ${action.count} 条尚未尝试的资讯` : '没有待标注资讯';
      } else if (action.id === 'news-retry-failed') {
        button.title = action.count ? `立即重试 ${action.count} 条退避失败资讯` : '没有退避失败资讯';
      }
    }
    document.querySelectorAll('[data-news-retry]').forEach(button => {
      button.disabled = state.annotationBusy || button.dataset.retryReady !== 'true';
    });
  }

  function renderQueueState(value) {
    state.queue = {
      pending: Number(value?.pending || 0),
      failed: Number(value?.failed || 0),
      recovery: Number(value?.recovery || 0),
      dead_letter: Number(value?.dead_letter || 0),
      manual_recoverable_dead_letter: Number(
        value?.manual_recoverable_dead_letter ?? value?.recoverable_dead_letter ?? 0),
      recoverable_dead_letter: Number(value?.recoverable_dead_letter || 0),
    };
    const queue = state.queue;
    document.getElementById('news-pending-action-count').textContent = queue.pending.toLocaleString();
    document.getElementById('news-failed-action-count').textContent = queue.failed.toLocaleString();
    document.getElementById('news-dead-action-count').textContent =
      queue.manual_recoverable_dead_letter.toLocaleString();
    document.getElementById('news-dead-action-hint').textContent = queue.dead_letter
      ? `可手动恢复 ${queue.manual_recoverable_dead_letter} / 共 ${queue.dead_letter} 个暂停项` : '没有暂停项';
    const parts = [];
    if (queue.pending) parts.push(`待标注 ${queue.pending}`);
    if (queue.failed) parts.push(`退避重试 ${queue.failed}`);
    if (queue.dead_letter) parts.push(`已暂停 ${queue.dead_letter}`);
    document.getElementById('news-queue-summary').textContent = parts.length
      ? parts.join(' · ') : '队列已清空，没有等待处理的资讯';
    const deadButton = document.getElementById('news-recover-dead');
    deadButton.title = queue.manual_recoverable_dead_letter
      ? `${queue.manual_recoverable_dead_letter} 个暂停项可立即手动恢复；${queue.recoverable_dead_letter} 个已到自动恢复时间`
      : '没有暂停项';
    refreshAnnotationAvailability();
  }

  function renderStats(data) {
    document.getElementById('news-stat-total').textContent = Number(data.total || 0).toLocaleString();
    document.getElementById('news-stat-coverage').textContent = `${Math.round(Number(data.coverage || 0) * 100)}%`;
    document.getElementById('news-stat-pending').textContent =
      `${Number(data.pending || 0)} / ${Number(data.failed || 0)} / ${Number(data.dead_letter || 0)}`;
    document.getElementById('news-stat-important').textContent = Number(data.important || 0).toLocaleString();
    document.getElementById('news-halflife-days').textContent = Number(data.halflife_days || 3).toLocaleString();
    renderQueueState(data.queue || {
      pending: data.pending, failed: data.failed, dead_letter: data.dead_letter,
      recoverable_dead_letter: data.dead_letter,
    });
    const series = data.sentiment_series || [];
    const scale = data.display_scale || {};
    const marketScale = Math.max(10,Number(scale.market_abs_max) || 20);
    const sectorScale = Math.max(10,Number(scale.sector_abs_max) || 20);
    const market = data.market_sentiment || {};
    const hasMarket = Number(market.event_count || 0) > 0;
    const current = hasMarket ? Number(market.score || 0) : null;
    const number = document.getElementById('news-factor-value');
    number.textContent = current === null ? '—' : `${current > 0 ? '+' : ''}${current.toFixed(1)}`;
    number.className = `sentiment-number ${current > 5 ? 'positive' : current < -5 ? 'negative' : ''}`;
    document.getElementById('news-market-label').textContent = hasMarket ? market.label : '暂无数据';
    document.getElementById('news-market-meta').textContent = hasMarket
      ? `${Number(market.event_count).toLocaleString()} 条有效事件 · 自适应显示 · 理论范围 ±100`
      : '等待达到置信度门槛的事件';
    document.getElementById('news-axis-low').textContent = `-${marketScale} 偏空`;
    document.getElementById('news-axis-high').textContent = `偏多 +${marketScale}`;
    const marker = document.getElementById('news-factor-marker');
    marker.style.left = `${current === null ? 50 : Math.max(0, Math.min(100, (current + marketScale) / (2 * marketScale) * 100))}%`;
    factorChart(series,marketScale);
    const sectors = data.sector_scores || [];
    document.getElementById('news-sector-scale').textContent = `自适应 ±${sectorScale}`;
    document.getElementById('news-sector-scores').innerHTML = sectors.length ? sectors.map(item => {
      const score = Number(item.score || 0);
      const direction = score > 5 ? 'positive' : score < -5 ? 'negative' : 'neutral';
      const signed = `${score > 0 ? '+' : ''}${score.toFixed(1)}`;
      const magnitude = Math.min(1, Math.abs(score) / sectorScale).toFixed(4);
      return `<div class="news-sector-row" title="利好 ${Number(item.positive || 0)} 条 · 利空 ${Number(item.negative || 0)} 条">
        <span><strong>${html(item.sector)}</strong><small>${Number(item.event_count || 0)} 条 · ${html(item.label)}</small></span>
        <i><b class="${direction}" style="--sector-magnitude:${magnitude}"></b></i>
        <em class="${direction}">${signed}</em>
      </div>`;
    }).join('') : '<span class="news-muted">暂无达到质量门槛的板块标注</span>';
  }

  function setEventFocusSelection(days) {
    document.querySelectorAll('[data-news-focus-days]').forEach(button => {
      button.setAttribute('aria-pressed', String(Number(button.dataset.newsFocusDays) === Number(days)));
    });
  }

  function renderEventFocus(data) {
    const days = Number(data.days || state.eventFocusDays);
    const symbols = data.top_symbols || [];
    const max = Math.max(1, ...symbols.map(item => item.count));
    document.getElementById('news-top-symbols').innerHTML = symbols.length ? symbols.map(item =>
      `<div class="news-symbol-row" title="${html(item.name || '名称待同步')} · ${html(item.symbol)} · 提及 ${Number(item.count || 0)} 次"><span class="news-symbol-identity"><strong>${html(item.name || '名称待同步')}</strong><small>${html(item.symbol)}</small></span><i><b style="transform:scaleX(${item.count / max})"></b></i><span>${item.count}</span></div>`
    ).join('') : `<span class="news-muted">过去 ${days} 日暂无达到质量门槛的标的提及</span>`;
  }

  function eventFocusFeedback(message = '', retryDays = null) {
    const feedback = document.getElementById('news-focus-feedback');
    feedback.hidden = !message;
    feedback.querySelector('span').textContent = message;
    feedback.querySelector('button').hidden = retryDays === null;
    if (retryDays !== null) state.eventFocusRetryDays = retryDays;
  }

  async function loadEventFocus(days = state.eventFocusDays) {
    const requestedDays = Number(days);
    if (![1, 3, 7, 30].includes(requestedDays)) return null;
    const requestId = ++state.eventFocusRequest;
    const symbols = document.getElementById('news-top-symbols');
    state.eventFocusDays = requestedDays;
    setEventFocusSelection(requestedDays);
    symbols.setAttribute('aria-busy', 'true');
    eventFocusFeedback();
    try {
      const data = await api(`/api/v1/news/event-focus?days=${requestedDays}`);
      if (requestId !== state.eventFocusRequest) return null;
      if (Number(data.days) !== requestedDays) throw new Error('事件聚焦返回了错误的时间窗口');
      renderEventFocus(data);
      state.eventFocusLoadedDays = requestedDays;
      return data;
    } catch (error) {
      if (requestId !== state.eventFocusRequest) return null;
      const previousDays = state.eventFocusLoadedDays;
      state.eventFocusDays = previousDays || 7;
      setEventFocusSelection(state.eventFocusDays);
      eventFocusFeedback(
        previousDays
          ? `${requestedDays} 日数据加载失败，仍显示 ${previousDays} 日结果。`
          : `${requestedDays} 日数据加载失败，请重试。`,
        requestedDays,
      );
      return null;
    } finally {
      if (requestId === state.eventFocusRequest) symbols.removeAttribute('aria-busy');
    }
  }

  async function loadStats() {
    try {
      const data = await api('/api/v1/news/stats?days=30');
      renderStats(data);
      return data;
    } catch (error) {
      report('量化摘要读取失败', error);
      return null;
    }
  }

  function renderSourceHealth() {
    const enabled = state.sources.filter(source => source.enabled);
    const target = document.getElementById('news-source-health');
    target.innerHTML = enabled.length ? enabled.slice(0, 7).map(source => {
      const status = source.last_status || 'idle';
      const stamp = source.last_run ? localDate(source.last_run) : null;
      return `<div class="news-source-health-row" title="${html(source.last_error || '')}"><i class="${html(status)}"></i><span>${html(source.name)}</span><time>${stamp ? `${stamp.day} ${stamp.time}` : '未运行'}</time></div>`;
    }).join('') : '<span class="news-muted">没有已启用的来源</span>';
    const failed = enabled.filter(source => source.last_status === 'failed').length;
    if (renderNewsTasks()) return;
    const live = document.getElementById('news-live-state');
    live.className = `news-live-state ${failed ? 'degraded' : 'ready'}`;
    live.innerHTML = `<i></i>${failed ? `${failed} 个来源异常` : `${enabled.length} 个来源已启用`}`;
  }

  function sourceStage(source) {
    if (!source.enabled) return '已停用';
    if (source.last_status === 'running') return '抓取中';
    if (Number(source.last_pending || 0)) return '待处理';
    if (source.last_status === 'failed' || source.health === 'failed') return '等待恢复';
    return source.last_run ? '空闲' : '未运行';
  }

  function sourceDiagnostic(source) {
    const parts = [
      `阶段 ${sourceStage(source)}`,
      `发现 ${Number(source.last_fetched || 0).toLocaleString()}`,
      `已落盘 ${Number(source.last_saved || 0).toLocaleString()}`,
      `待处理 ${Number(source.last_pending || 0).toLocaleString()}`,
    ];
    if (source.last_success_at) parts.push(`最近成功 ${source.last_success_at}`);
    if (source.last_error_code) parts.push(`诊断码 ${source.last_error_code}`);
    if (source.last_error) parts.push(`原因 ${source.last_error}`);
    return parts.join('\n');
  }

  function sourceProgress(source) {
    const found = Number(source.last_fetched || 0);
    const saved = Number(source.last_saved || 0);
    const pending = Number(source.last_pending || 0);
    const total = Math.max(found, saved + pending);
    return total ? Math.max(0, Math.min(100, Math.round(saved / total * 100))) : 0;
  }

  function updateSourceFilter() {
    const select = document.getElementById('news-source-filter');
    const current = select.value;
    select.innerHTML = '<option value="">全部来源</option>' + state.sources.map(source =>
      `<option value="${html(source.id)}">${html(source.name)}</option>`
    ).join('');
    if (state.sources.some(source => source.id === current)) select.value = current;
  }

  function groupLabel(group) {
    return {fast: '快讯组 · 10 MIN', official: '官方组 · 15 MIN', periodic: '定期组 · 60 MIN'}[group] || group;
  }

  function renderSourceList() {
    const target = document.getElementById('news-source-list');
    const groups = ['fast', 'official', 'periodic'];
    target.innerHTML = groups.map(group => {
      const items = state.sources.filter(source => source.group_name === group);
      if (!items.length) return '';
      return `<div class="source-list-group">${html(groupLabel(group))}</div>${items.map(source =>
        `<button class="source-list-item ${state.selectedSource?.id === source.id ? 'active' : ''}" data-source-id="${html(source.id)}" type="button" aria-label="${html(`${source.name}，${sourceStage(source)}`)}"><i class="${source.enabled ? 'enabled' : ''}"></i><span><strong>${html(source.name)}</strong><small>${html(`${sourceStage(source)} · ${source.last_error_code || source.last_error || (source.last_run ? `最近运行 ${source.last_run}` : '尚未运行')}`)}</small><b class="source-progress" aria-label="已落盘进度 ${sourceProgress(source)}%"><i style="width:${sourceProgress(source)}%"></i></b></span><span>${html(source.kind)}</span></button>`
      ).join('')}`;
    }).join('') || '<div class="msg">暂无来源</div>';
  }

  const parserFields = [
    'items_path', 'title_path', 'content_path', 'url_path', 'published_at_path',
    'item_selector', 'title_selector', 'content_selector', 'url_selector',
    'published_at_selector', 'detail_content_selector',
  ];

  function showSourceFields() {
    const kind = sourceForm.elements.kind.value;
    document.querySelectorAll('[data-source-parser]').forEach(fieldset => {
      fieldset.hidden = fieldset.dataset.sourceParser !== kind;
    });
    sourceForm.querySelector('[data-auth-header]').hidden = sourceForm.elements.auth_type.value !== 'header';
  }

  function fillSource(source = null) {
    state.selectedSource = source;
    state.clearToken = false;
    sourceForm.reset();
    sourceForm.elements.enabled.checked = true;
    sourceForm.elements.item_limit.value = 30;
    sourceForm.elements.factor_weight.value = 1;
    sourceForm.elements.group_name.value = 'periodic';
    sourceForm.elements.kind.value = 'rss';
    if (source) {
      for (const name of ['id', 'name', 'kind', 'group_name', 'url', 'item_limit',
        'factor_weight', 'auth_type', 'auth_header']) {
        if (sourceForm.elements[name]) sourceForm.elements[name].value = source[name] ?? '';
      }
      sourceForm.elements.enabled.checked = Boolean(source.enabled);
      sourceForm.elements.is_official.checked = Boolean(source.is_official);
      parserFields.forEach(name => { sourceForm.elements[name].value = source.parser?.[name] || ''; });
    }
    sourceForm.elements.token.value = '';
    sourceForm.classList.toggle('is-builtin', Boolean(source?.built_in));
    document.getElementById('source-editor-kind').textContent = source?.built_in ? 'BUILT-IN ADAPTER' : 'CUSTOM SOURCE';
    document.getElementById('source-editor-title').textContent = source?.name || '添加声明式资讯来源';
    document.getElementById('source-secret-state').textContent = source?.auth_configured ? '凭据已配置' : '无鉴权凭据';
    const run = source ? ` · ${sourceStage(source)} · 发现 ${Number(source.last_fetched || 0)} / 已落盘 ${Number(source.last_saved || 0)} / 待处理 ${Number(source.last_pending || 0)}` : '';
    sourceFeedback(source?.last_error ? 'error' : '', source ? `${run.slice(3)}${source.last_error_code ? ` · 诊断码 ${source.last_error_code}` : ''}` : '');
    sourceForm.querySelector('[data-source-delete]').hidden = !source || source.built_in;
    sourceForm.querySelector('[data-source-clear-token]').hidden = !source?.auth_configured;
    sourceForm.querySelector('[data-source-run]').hidden = !source;
    showSourceFields();
    renderSourceList();
    document.getElementById('news-source-preview').innerHTML = '<span class="news-muted">保存前可先测试解析结果。</span>';
    sourceFeedback();
  }

  async function loadSources(selectFirst = false) {
    try {
      const data = await api('/api/v1/news/sources');
      state.sources = data.items || [];
      updateSourceFilter();
      renderSourceHealth();
      if (state.selectedSource) {
        state.selectedSource = state.sources.find(source => source.id === state.selectedSource.id) || null;
      }
      if (selectFirst && !state.selectedSource && state.sources.length) fillSource(state.sources[0]);
      else renderSourceList();
    } catch (error) {
      document.getElementById('news-source-list').innerHTML = `<div class="msg">${html(error.message)}</div>`;
      report('来源配置读取失败', error);
    }
  }

  function sourcePayload() {
    const formData = new FormData(sourceForm);
    const kind = String(formData.get('kind'));
    const parser = {};
    parserFields.forEach(name => { parser[name] = String(formData.get(name) || '').trim(); });
    return {
      name: String(formData.get('name') || '').trim(), kind,
      enabled: sourceForm.elements.enabled.checked,
      group_name: String(formData.get('group_name')),
      url: String(formData.get('url') || '').trim(),
      item_limit: Number(formData.get('item_limit')),
      factor_weight: Number(formData.get('factor_weight')),
      is_official: sourceForm.elements.is_official.checked,
      parser,
      auth_type: String(formData.get('auth_type')),
      auth_header: String(formData.get('auth_header') || '').trim(),
    };
  }

  function previewMarkup(items) {
    return items?.length ? items.map(item => `<div class="source-preview-item"><strong>${html(item.title)}</strong><p>${html(item.content || '')}</p></div>`).join('') : '<span class="news-muted">请求成功，但解析规则没有提取到条目。</span>';
  }

  sourceForm.addEventListener('change', event => {
    if (['kind', 'auth_type'].includes(event.target.name)) showSourceFields();
  });

  sourceForm.onsubmit = async event => {
    event.preventDefault();
    const submit = sourceForm.querySelector('[type="submit"]');
    submit.disabled = true;
    submit.textContent = '保存中…';
    sourceFeedback('saving', '正在校验并保存来源设置…');
    try {
      const token = sourceForm.elements.token.value;
      let saved;
      if (state.selectedSource) {
        const source = state.selectedSource.built_in ? {
          enabled: sourceForm.elements.enabled.checked,
          group_name: sourceForm.elements.group_name.value,
          item_limit: Number(sourceForm.elements.item_limit.value),
          factor_weight: Number(sourceForm.elements.factor_weight.value),
        } : sourcePayload();
        saved = await secure(`/api/v1/news/sources/${encodeURIComponent(state.selectedSource.id)}`, {
          method: 'PUT', body: {source, token_action: state.clearToken ? 'clear' : token ? 'replace' : 'keep', token},
        });
      } else {
        const source = sourcePayload();
        if (source.kind === 'builtin') throw new Error('不能创建内置适配器');
        saved = await secure('/api/v1/news/sources', {method: 'POST', body: {source, token}});
      }
      report(`来源“${saved.name}”已保存`, null, 'success');
      state.selectedSource = saved;
      await loadSources();
      fillSource(state.sources.find(source => source.id === saved.id));
      const time = new Date().toLocaleTimeString('zh-CN', {
        hour12:false, hour:'2-digit', minute:'2-digit',
      });
      sourceFeedback('success', `来源“${saved.name}”已保存 · ${time}`);
    } catch (error) {
      report('保存来源失败', error);
      sourceFeedback('error', `保存失败：${error.message}`);
    } finally {
      submit.disabled = false;
      submit.textContent = '保存来源';
    }
  };

  sourceForm.querySelector('[data-source-test]').onclick = async () => {
    const target = document.getElementById('news-source-preview');
    target.innerHTML = '<span class="news-muted">正在请求并解析前 3 条…</span>';
    try {
      const data = state.selectedSource ? await secure(
        `/api/v1/news/sources/${encodeURIComponent(state.selectedSource.id)}/test`, {method: 'POST'},
      ) : await secure('/api/v1/news/sources/preview', {
        method: 'POST', body: {source: sourcePayload(), token: sourceForm.elements.token.value},
      });
      target.innerHTML = previewMarkup(data.items);
    } catch (error) {
      target.innerHTML = `<span class="err">${html(error.message)}</span>`;
    }
  };

  sourceForm.querySelector('[data-source-run]').onclick = async () => {
    if (!state.selectedSource) return;
    try {
      const result = await secure(`/api/v1/news/sources/${encodeURIComponent(state.selectedSource.id)}/run`, {method: 'POST'});
      void watchNewsTask(result, '来源采集任务').catch(() => renderNewsTasks());
    } catch (error) { report('来源采集失败', error); }
  };

  sourceForm.querySelector('[data-source-clear-token]').onclick = () => {
    state.clearToken = true;
    sourceForm.elements.token.value = '';
    document.getElementById('source-secret-state').textContent = '保存后清除凭据';
  };

  sourceForm.querySelector('[data-source-delete]').onclick = async () => {
    const source = state.selectedSource;
    if (!source || source.built_in || !window.confirm(`删除资讯来源“${source.name}”？历史资讯仍会保留。`)) return;
    try {
      await secure(`/api/v1/news/sources/${encodeURIComponent(source.id)}`, {method: 'DELETE'});
      state.selectedSource = null;
      await loadSources(true);
      report(`来源“${source.name}”已删除，历史资讯已保留`, null, 'success');
    } catch (error) { report('删除来源失败', error); }
  };

  document.getElementById('news-source-list').onclick = event => {
    const button = event.target.closest('[data-source-id]');
    if (!button) return;
    fillSource(state.sources.find(source => source.id === button.dataset.sourceId));
  };

  document.getElementById('news-source-new').onclick = () => fillSource(null);

  feed.onclick = async event => {
    const retryButton = event.target.closest('[data-news-retry]');
    if (retryButton) {
      event.preventDefault();
      event.stopPropagation();
      await runAnnotation(
        retryButton, retryButton.dataset.newsRetry,
        [Number(retryButton.dataset.newsId)],
      );
      return;
    }
    const button = event.target.closest('.news-event-main');
    if (!button) return;
    const article = button.closest('.news-event');
    const expanded = article.classList.toggle('expanded');
    button.setAttribute('aria-expanded', String(expanded));
    if (expanded && article.dataset.contentTruncated === 'true' && article.dataset.loaded !== 'true') {
      const copy = article.querySelector('.news-detail-copy');
      copy.textContent = '正在读取完整正文…';
      try {
        const detail = await api(`/api/v1/news/${article.dataset.newsId}`);
        copy.textContent = detail.content || detail.summary || '暂无正文';
        article.dataset.loaded = 'true';
      } catch (error) {
        copy.textContent = `完整正文读取失败：${error.message}`;
      }
    }
  };

  filterForm.onsubmit = event => { event.preventDefault(); loadFeed(); };
  document.getElementById('news-reset').onclick = () => { filterForm.reset(); loadFeed(); };
  document.getElementById('news-focus-window').onclick = event => {
    const button = event.target.closest('[data-news-focus-days]');
    if (button) loadEventFocus(Number(button.dataset.newsFocusDays));
  };
  document.querySelector('#news-focus-feedback button').onclick = () =>
    loadEventFocus(state.eventFocusRetryDays);
  document.getElementById('news-load-more').onclick = async event => {
    event.currentTarget.disabled = true;
    await loadFeed({append: true});
  };

  document.getElementById('news-sync').onclick = async event => {
    const button = event.currentTarget;
    button.disabled = true;
    button.textContent = '同步中…';
    try {
      const result = await secure('/api/v1/news/crawl', {method: 'POST', body: {limit: 30}});
      void watchNewsTask(result, '资讯同步任务').catch(() => renderNewsTasks());
    } catch (error) { report('同步失败', error); }
    finally { button.disabled = false; button.textContent = '立即同步'; }
  };

  const annotationModes = {
    pending: {
      idle: '处理待标注', active: '标注中', preparing: '准备标注队列',
      reading: '正在读取尚未尝试的待标注资讯…', empty: '没有待处理资讯',
      emptyDetail: '当前没有尚未尝试的待标注资讯。', complete: '本轮标注已完成',
      report: '标注处理完成',
    },
    failed: {
      idle: '重试失败项', active: '重试中', preparing: '准备失败重试队列',
      reading: '正在重新排队之前失败的资讯…', empty: '没有失败项',
      emptyDetail: '当前没有可立即重试的失败资讯。', complete: '本轮重试已完成',
      report: '失败项重试完成',
    },
    dead_letter: {
      idle: '恢复暂停项', active: '恢复中', preparing: '准备暂停项恢复队列',
      reading: '正在认领可手动恢复的暂停项…', empty: '没有可手动恢复的暂停项',
      emptyDetail: '当前没有等待手动恢复的暂停项。', complete: '本轮暂停项恢复已完成',
      report: '暂停项恢复完成',
    },
  };

  function setAnnotationActionsDisabled(disabled) {
    state.annotationBusy = disabled;
    refreshAnnotationAvailability();
  }

  function setActionLabel(button, value) {
    const label = button.querySelector('[data-action-label]');
    if (label) label.textContent = value;
    else button.textContent = value;
  }

  async function runAnnotation(button, mode, ids = null) {
    const copy = annotationModes[mode];
    const idleLabel = button.querySelector('[data-action-label]')?.textContent || copy.idle;
    setAnnotationActionsDisabled(true);
    button.setAttribute('aria-busy', 'true');
    setActionLabel(button, copy.active);
    startAnnotationProgress({phase: copy.preparing, detail: copy.reading});
    try {
      const task = await submitAnnotationTask(mode, ids);
      setAnnotationProgress({
        percent: 0, phase: '任务已入队',
        detail: '可继续处理其他资讯；完成后会自动刷新。', count: '等待执行',
      });
      const job = await watchNewsTask(task, copy.report, current => {
        const result = current.result || {};
        const count = Number(result.processed || 0)
          ? `成功 ${Number(result.completed || 0)} · 失败 ${Number(result.failed || 0)}`
          : undefined;
        setAnnotationProgress({
          percent: Number(current.progress || 0),
          phase: current.phase || undefined,
          detail: current.detail || undefined,
          count,
        });
      });
        const partial = job.status === 'completed_with_errors';
        finishAnnotationProgress(
          partial ? 'warning' : 'success', partial ? '标注部分完成' : copy.complete,
          job.detail || (partial ? '部分资讯需要重试。' : '结果已写入并刷新。'),
          Number(job.progress || 100),
        );
        report(partial ? '标注任务部分完成' : copy.report, null, partial ? 'warning' : 'success');
    } catch (error) {
      const current = Number(document.getElementById('news-annotation-track').getAttribute('aria-valuenow'));
      finishAnnotationProgress('failed', '标注任务中断', error.message, current);
      report('标注任务失败', error);
    } finally {
      setAnnotationActionsDisabled(false);
      button.removeAttribute('aria-busy');
      setActionLabel(button, idleLabel);
    }
  }

  document.getElementById('news-reanalyze').onclick = event =>
    runAnnotation(event.currentTarget, 'pending');

  document.getElementById('news-retry-failed').onclick = event =>
    runAnnotation(event.currentTarget, 'failed');

  document.getElementById('news-recover-dead').onclick = event =>
    runAnnotation(event.currentTarget, 'dead_letter');

  function openSourceSettings() {
    document.querySelector('header [data-tab="settings"]').click();
    document.querySelector('[data-settings-section="sources"]').click();
  }

  document.getElementById('news-open-sources').onclick = openSourceSettings;
  document.getElementById('news-source-feedback').addEventListener('dblclick', async () => {
    const source = state.selectedSource;
    if (!source || !navigator.clipboard) return;
    await navigator.clipboard.writeText(sourceDiagnostic(source));
    sourceFeedback('success', '已复制脱敏诊断摘要');
  });
  document.getElementById('settings-nav').addEventListener('click', event => {
    if (event.target.closest('[data-settings-section="sources"]')) loadSources(true);
  });

  async function loadNews() {
    await Promise.all([loadFeed(), loadStats(), loadEventFocus(), loadSources()]);
    state.loaded = true;
  }

  window.loadNews = loadNews;
})();
