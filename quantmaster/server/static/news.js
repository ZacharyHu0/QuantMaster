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

  async function api(path, options = {}) {
    return window.QuantMasterAPI(path, options);
  }

  async function secure(path, options = {}) {
    await window.QuantMasterManagement.ensureSettings();
    return window.QuantMasterManagement.request(path, options);
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

  function statusLabel(status) {
    return {complete: '已标注', pending: '待标注', failed: '标注失败'}[status] || status || '待标注';
  }

  function eventTemplate(item) {
    const timestamp = localDate(item.first_seen_at || item.published_at);
    const sentiment = Number(item.sentiment || 0);
    const sentimentClass = sentiment > .15 ? 'positive' : sentiment < -.15 ? 'negative' : 'neutral';
    const score = Math.round(Number(item.importance_score || 0));
    const tags = [
      item.is_official ? '<span class="news-tag official">官方</span>' : '',
      item.event_type ? `<span class="news-tag">${html(item.event_type)}</span>` : '',
      `<span class="news-tag ${html(item.analysis_status)}">${html(statusLabel(item.analysis_status))}</span>`,
      ...(item.symbols || []).slice(0, 4).map(symbol => `<span class="news-tag symbol">${html(symbol)}</span>`),
    ].join('');
    const link = safeUrl(item.url);
    return `<article class="news-event" data-news-id="${Number(item.id)}" data-content-truncated="${Boolean(item.content_truncated)}">
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
        <div class="news-detail-metric"><span>置信度</span><strong>${Math.round(Number(item.confidence || 0) * 100)}%</strong></div>
        <div class="news-detail-metric"><span>影响范围</span><strong>${html(item.scope || '待判断')}</strong></div>
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
      const data = await api(`/api/news?${queryString(append ? state.nextCursor : null)}`);
      state.items = data.items || [];
      state.nextCursor = data.next_cursor;
      state.hasMore = Boolean(data.has_more);
      renderFeed(append);
    } catch (error) {
      feed.innerHTML = `<div class="news-empty"><div><strong>资讯库暂不可用</strong>${html(error.message)}</div></div>`;
      report('读取资讯失败', error);
    } finally { state.loading = false; }
  }

  function factorChart(series) {
    const svg = document.getElementById('news-factor-chart');
    if (!series?.length) {
      svg.innerHTML = '<text x="0" y="48" fill="var(--muted)" font-size="11">暂无足够的已标注资讯</text>';
      return;
    }
    const width = 320;
    const height = 92;
    const values = series.map(item => Number(item[1] || 0));
    const maxAbs = Math.max(.2, ...values.map(Math.abs));
    const points = values.map((value, index) => {
      const x = values.length === 1 ? width / 2 : index / (values.length - 1) * width;
      const y = height / 2 - value / maxAbs * (height / 2 - 7);
      return `${x.toFixed(1)},${y.toFixed(1)}`;
    }).join(' ');
    svg.innerHTML = `<line x1="0" y1="46" x2="320" y2="46" stroke="var(--axis)" stroke-width="1"/>
      <polyline points="${points}" fill="none" stroke="var(--s1)" stroke-width="1.75" vector-effect="non-scaling-stroke"/>
      <circle cx="${points.split(' ').at(-1).split(',')[0]}" cy="${points.split(' ').at(-1).split(',')[1]}" r="3" fill="var(--s1)"/>`;
  }

  function renderStats(data) {
    document.getElementById('news-stat-total').textContent = Number(data.total || 0).toLocaleString();
    document.getElementById('news-stat-coverage').textContent = `${Math.round(Number(data.coverage || 0) * 100)}%`;
    document.getElementById('news-stat-pending').textContent = `${Number(data.pending || 0)} / ${Number(data.failed || 0)}`;
    document.getElementById('news-stat-important').textContent = Number(data.important || 0).toLocaleString();
    const series = data.sentiment_series || [];
    const current = series.length ? Number(series.at(-1)[1] || 0) : null;
    const number = document.getElementById('news-factor-value');
    number.textContent = current === null ? '—' : `${current > 0 ? '+' : ''}${current.toFixed(3)}`;
    number.className = `sentiment-number ${current > .05 ? 'positive' : current < -.05 ? 'negative' : ''}`;
    const marker = document.getElementById('news-factor-marker');
    marker.style.left = `${current === null ? 50 : Math.max(0, Math.min(100, (current + 1) * 50))}%`;
    factorChart(series);
    const symbols = data.top_symbols || [];
    const max = Math.max(1, ...symbols.map(item => item.count));
    document.getElementById('news-top-symbols').innerHTML = symbols.length ? symbols.map(item =>
      `<div class="news-symbol-row"><strong>${html(item.symbol)}</strong><i><b style="transform:scaleX(${item.count / max})"></b></i><span>${item.count}</span></div>`
    ).join('') : '<span class="news-muted">暂无个股映射</span>';
  }

  async function loadStats() {
    try { renderStats(await api('/api/news/stats?days=30')); }
    catch (error) { report('量化摘要读取失败', error); }
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
    const live = document.getElementById('news-live-state');
    live.className = `news-live-state ${failed ? 'degraded' : 'ready'}`;
    live.innerHTML = `<i></i>${failed ? `${failed} 个来源异常` : `${enabled.length} 个来源已启用`}`;
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
        `<button class="source-list-item ${state.selectedSource?.id === source.id ? 'active' : ''}" data-source-id="${html(source.id)}" type="button"><i class="${source.enabled ? 'enabled' : ''}"></i><span><strong>${html(source.name)}</strong><small>${html(source.last_error || (source.last_run ? `最近运行 ${source.last_run}` : '尚未运行'))}</small></span><span>${html(source.kind)}</span></button>`
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
    sourceForm.querySelector('[data-source-delete]').hidden = !source || source.built_in;
    sourceForm.querySelector('[data-source-clear-token]').hidden = !source?.auth_configured;
    sourceForm.querySelector('[data-source-run]').hidden = !source;
    showSourceFields();
    renderSourceList();
    document.getElementById('news-source-preview').innerHTML = '<span class="news-muted">保存前可先测试解析结果。</span>';
  }

  async function loadSources(selectFirst = false) {
    try {
      const data = await api('/api/news/sources');
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
        saved = await secure(`/api/news/sources/${encodeURIComponent(state.selectedSource.id)}`, {
          method: 'PUT', body: {source, token_action: state.clearToken ? 'clear' : token ? 'replace' : 'keep', token},
        });
      } else {
        const source = sourcePayload();
        if (source.kind === 'builtin') throw new Error('不能创建内置适配器');
        saved = await secure('/api/news/sources', {method: 'POST', body: {source, token}});
      }
      report(`来源“${saved.name}”已保存`, null, 'success');
      state.selectedSource = saved;
      await loadSources();
      fillSource(state.sources.find(source => source.id === saved.id));
    } catch (error) { report('保存来源失败', error); }
    finally { submit.disabled = false; }
  };

  sourceForm.querySelector('[data-source-test]').onclick = async () => {
    const target = document.getElementById('news-source-preview');
    target.innerHTML = '<span class="news-muted">正在请求并解析前 3 条…</span>';
    try {
      const data = state.selectedSource ? await secure(
        `/api/news/sources/${encodeURIComponent(state.selectedSource.id)}/test`, {method: 'POST'},
      ) : await secure('/api/news/sources/preview', {
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
      const result = await secure(`/api/news/sources/${encodeURIComponent(state.selectedSource.id)}/run`, {method: 'POST'});
      report(`采集完成：新增 ${result.saved || 0} 条`, null, 'success');
      await Promise.all([loadSources(), loadFeed(), loadStats()]);
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
      await secure(`/api/news/sources/${encodeURIComponent(source.id)}`, {method: 'DELETE'});
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
    const button = event.target.closest('.news-event-main');
    if (!button) return;
    const article = button.closest('.news-event');
    const expanded = article.classList.toggle('expanded');
    button.setAttribute('aria-expanded', String(expanded));
    if (expanded && article.dataset.contentTruncated === 'true' && article.dataset.loaded !== 'true') {
      const copy = article.querySelector('.news-detail-copy');
      copy.textContent = '正在读取完整正文…';
      try {
        const detail = await api(`/api/news/${article.dataset.newsId}`);
        copy.textContent = detail.content || detail.summary || '暂无正文';
        article.dataset.loaded = 'true';
      } catch (error) {
        copy.textContent = `完整正文读取失败：${error.message}`;
      }
    }
  };

  filterForm.onsubmit = event => { event.preventDefault(); loadFeed(); };
  document.getElementById('news-reset').onclick = () => { filterForm.reset(); loadFeed(); };
  document.getElementById('news-load-more').onclick = async event => {
    event.currentTarget.disabled = true;
    await loadFeed({append: true});
  };

  document.getElementById('news-sync').onclick = async event => {
    const button = event.currentTarget;
    button.disabled = true;
    button.textContent = '同步中…';
    try {
      const result = await secure('/api/news/crawl', {method: 'POST', body: {limit: 30}});
      report(`同步完成：抓取 ${result.fetched || 0} 条，新增 ${result.saved || 0} 条`, null, 'success');
      await Promise.all([loadFeed(), loadStats(), loadSources()]);
    } catch (error) { report('同步失败', error); }
    finally { button.disabled = false; button.textContent = '立即同步'; }
  };

  document.getElementById('news-reanalyze').onclick = async event => {
    const button = event.currentTarget;
    button.disabled = true;
    try {
      const result = await secure('/api/news/reanalyze', {method: 'POST', body: {limit: 100}});
      report(`标注处理完成：${result.completed || 0}/${result.processed || 0}`, null, 'success');
      await Promise.all([loadFeed(), loadStats()]);
    } catch (error) { report('标注任务失败', error); }
    finally { button.disabled = false; }
  };

  function openSourceSettings() {
    document.querySelector('header [data-tab="settings"]').click();
    document.querySelector('[data-settings-section="sources"]').click();
  }

  document.getElementById('news-open-sources').onclick = openSourceSettings;
  document.getElementById('settings-nav').addEventListener('click', event => {
    if (event.target.closest('[data-settings-section="sources"]')) loadSources(true);
  });

  async function loadNews() {
    await Promise.all([loadFeed(), loadStats(), loadSources()]);
    state.loaded = true;
  }

  window.loadNews = loadNews;
})();
