const marketWorkbench = (() => {
  'use strict';

  const METHODS = {
    equal:'等权', float_mv:'流通市值', amount:'成交额', volume:'成交量', total_mv:'总市值',
  };
  const CATEGORIES = {sw1:'申万一级', sw2:'申万二级', theme:'题材'};
  const WINDOWS = [1,3,5,20];
  const responseCache = new Map();
  const controllers = new Map();
  let mounted = false;
  let generation = 0;
  let searchTimer = 0;
  let context = null;
  let state = {
    category:'sw1', code:'', method:'equal', window:5, query:'', page:1,
    list:null, detail:null, constituents:null, selectedStock:'',
  };

  const finite = value => {
    if (value === null || value === undefined || value === '') return null;
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  };
  const number = (value, digits = 1) => finite(value) == null ? '—' : finite(value).toFixed(digits);
  const percent = (value, digits = 2) => {
    const parsed = finite(value);
    return parsed == null ? '—' : `${parsed > 0 ? '+' : ''}${parsed.toFixed(digits)}%`;
  };
  const ratio = value => finite(value) == null ? '—' : `${(finite(value) * 100).toFixed(0)}%`;
  const tone = value => finite(value) > 0 ? 'up' : finite(value) < 0 ? 'down' : '';
  const statusLabel = value => ({
    complete:'完整', partial:'部分可用', ready:'可用', verified:'已验证', stale:'陈旧',
    unavailable:'不可用', cold:'等待快照', degraded:'证据待补', loading:'计算中',
  })[String(value || '').toLowerCase()] || String(value || '未知');

  function abort(owner) {
    controllers.get(owner)?.abort();
    controllers.delete(owner);
  }

  function nextSignal(owner) {
    abort(owner);
    const controller = new AbortController();
    controllers.set(owner, controller);
    return controller.signal;
  }

  async function readJson(path, {owner = '', force = false} = {}) {
    const cached = responseCache.get(path);
    const headers = {'Accept':'application/json'};
    if (!force && cached?.etag) headers['If-None-Match'] = cached.etag;
    const signal = owner ? nextSignal(owner) : undefined;
    const response = await fetch(path, {headers, signal, credentials:'same-origin'});
    if (response.status === 304 && cached) return cached.value;
    const value = await response.json().catch(() => ({}));
    if (!response.ok) {
      const message = value?.problem?.message || value?.detail || `读取失败（HTTP ${response.status}）`;
      throw new Error(typeof message === 'string' ? message : '页面数据未能读取');
    }
    responseCache.set(path, {etag:response.headers.get('ETag') || '', value});
    return value;
  }

  function updateRoute() {
    if (!mounted) return;
    const params = new URLSearchParams({
      category:state.category, method:state.method, window:String(state.window),
    });
    if (state.code) params.set('code', state.code);
    history.replaceState(null, '', `#today/market?${params}`);
  }

  function restoreRoute(route = {}) {
    state.category = Object.hasOwn(CATEGORIES, route.category) ? route.category : 'sw1';
    state.method = Object.hasOwn(METHODS, route.method) ? route.method : 'equal';
    const selectedWindow = Number(route.window);
    state.window = WINDOWS.includes(selectedWindow) ? selectedWindow : 5;
    state.code = String(route.code || '');
    state.page = 1;
  }

  function shellMarkup() {
    return `<div class="market-workbench">
      <header class="market-workbench-head">
        <div><span class="market-kicker">MARKET PANORAMA</span><h1>市场全景</h1><p>从市场状态进入板块，再下钻到成分股；历史曲线仅在选中后读取。</p></div>
        <div class="market-head-actions"><span id="market-refresh-progress" role="status">本地快照</span><button type="button" data-market-refresh>刷新指数</button></div>
      </header>
      <section class="market-decision-strip" id="market-decision-strip" aria-label="今日市场决策条">
        <div class="market-loading-line"><i></i><span>正在读取本地决策快照…</span></div>
      </section>
      <div class="market-three-column">
        <aside class="market-directory" aria-labelledby="market-directory-title">
          <div class="market-pane-head"><div><span>01 / DIRECTORY</span><h2 id="market-directory-title">板块排名</h2></div><span id="market-directory-count">—</span></div>
          <div class="market-category-tabs" role="tablist" aria-label="板块分类">
            ${Object.entries(CATEGORIES).map(([value,label], index) => `<button type="button" role="tab" data-market-category="${value}" aria-selected="${String(value === state.category)}" tabindex="${value === state.category ? 0 : -1}">${label}</button>`).join('')}
          </div>
          <label class="market-search"><span class="sr-only">搜索板块</span><input type="search" data-market-query maxlength="80" placeholder="搜索代码或名称" autocomplete="off"></label>
          <div class="market-window-tabs" role="group" aria-label="排名周期">${WINDOWS.map(value => `<button type="button" data-market-window="${value}" aria-pressed="${String(value === state.window)}">${value}日</button>`).join('')}</div>
          <div class="market-board-list" id="market-board-list" role="listbox" aria-label="板块排名"><div class="market-loading-line"><i></i><span>读取排名…</span></div></div>
          <div class="market-list-pages" id="market-list-pages"></div>
        </aside>
        <section class="market-focus" aria-labelledby="market-focus-title">
          <div class="market-focus-head" id="market-focus-head"><div><span>02 / BOARD INDEX</span><h2 id="market-focus-title">选择一个板块</h2></div></div>
          <div class="market-method-tabs" role="tablist" aria-label="板块指数算法">
            ${Object.entries(METHODS).map(([value,label]) => `<button type="button" role="tab" data-market-method="${value}" aria-selected="${String(value === state.method)}" tabindex="${value === state.method ? 0 : -1}">${label}</button>`).join('')}
          </div>
          <div class="market-chart-wrap"><div class="market-board-chart" id="market-board-chart" role="img" aria-label="选中板块指数日线"></div><div class="market-chart-empty" id="market-chart-empty">等待板块详情</div></div>
          <section class="market-method-compare" id="market-method-compare" aria-label="五算法阶段收益"></section>
        </section>
        <aside class="market-evidence" aria-labelledby="market-evidence-title">
          <div class="market-pane-head"><div><span>03 / EVIDENCE</span><h2 id="market-evidence-title">状态与成分</h2></div></div>
          <div id="market-board-evidence"><div class="market-loading-line"><i></i><span>等待板块选择…</span></div></div>
          <section class="market-stock-history" id="market-stock-history" hidden>
            <div class="market-stock-head"><div><span>ON DEMAND HISTORY</span><h3 id="market-stock-title">个股历史</h3></div><button type="button" data-market-stock-close aria-label="关闭个股历史">×</button></div>
            <div class="market-stock-chart" id="market-stock-chart"></div>
          </section>
        </aside>
      </div>
    </div>`;
  }

  function errorMarkup(message, retry = '') {
    return `<div class="market-state" data-state="error"><strong>数据未能读取</strong><p>${esc(message || '请稍后重试')}</p>${retry ? `<button type="button" ${retry}>重试</button>` : ''}</div>`;
  }

  function metricMarkup(label, value, note = '', valueTone = '', href = '') {
    const content = `<span>${esc(label)}</span><strong class="${valueTone}">${esc(String(value ?? '—'))}</strong><small>${esc(note)}</small>`;
    return href ? `<a class="market-decision-metric" href="${href}">${content}</a>` : `<article>${content}</article>`;
  }

  function renderDecisionStrip(results) {
    const target = document.getElementById('market-decision-strip');
    if (!target || !mounted) return;
    const marketResult = results.market?.status === 'fulfilled' ? results.market.value : null;
    const market = marketResult?.data || marketResult || {};
    const snapshot = marketResult?.snapshot || {};
    const globalFear = results.globalFear?.status === 'fulfilled' ? results.globalFear.value : {};
    const ashareFear = results.ashareFear?.status === 'fulfilled' ? results.ashareFear.value : {};
    const rotation = results.rotation?.status === 'fulfilled' ? results.rotation.value : {};
    const temperature = rotation?.data?.market?.temperature?.temperature
      ?? (results.temperature?.status === 'fulfilled' ? results.temperature.value?.data?.current?.temperature : null);
    const groups = market.groups || {};
    const indexes = (groups['A股指数'] || []).filter(item => finite(item.last) != null).slice(0,4);
    const quality = market.data_quality || {};
    const coverage = Number(quality.requested_count) > 0
      ? `${quality.observed_count || 0}/${quality.requested_count}` : '—';
    const date = snapshot.as_of || market.meta?.as_of || rotation?.meta?.as_of || '—';
    target.dataset.state = snapshot.state || quality.status || 'ready';
    target.innerHTML = [
      ...indexes.map(item => metricMarkup(item.name || item.symbol, number(item.last,2), percent(item.change_pct), tone(item.change_pct))),
      metricMarkup('A股恐贪', number(ashareFear.score,0), ashareFear.rating_label || statusLabel(ashareFear.status), '', '#today/quotes?focus=ashare-fear-greed'),
      metricMarkup('美股恐贪', number(globalFear.score,0), globalFear.rating_label || statusLabel(globalFear.status), '', '#today/quotes?focus=fear-greed'),
      metricMarkup('市场温度', number(temperature,0), temperature == null ? '等待快照' : '0–100', '', '#today/temperature'),
      metricMarkup('数据覆盖', coverage, `${date} · ${statusLabel(quality.status || snapshot.state)}`),
    ].join('');
  }

  async function loadDecisionStrip(token) {
    const paths = {
      market:'/api/v1/market/overview',
      globalFear:'/api/v1/market/fear-greed',
      ashareFear:'/api/v1/market/ashare-fear-greed?symbol=%E4%B8%8A%E8%AF%81%E6%8C%87%E6%95%B0',
      temperature:'/api/v1/market/temperature',
      rotation:`/api/v1/rotation/overview?window=${state.window}`,
    };
    const entries = await Promise.all(Object.entries(paths).map(async ([key,path]) => {
      try { return [key,{status:'fulfilled',value:await readJson(path)}]; }
      catch (error) { return [key,{status:'rejected',reason:error}]; }
    }));
    if (token !== generation || !mounted) return;
    renderDecisionStrip(Object.fromEntries(entries));
  }

  function boardListPath() {
    const params = new URLSearchParams({
      category:state.category, method:state.method, window:String(state.window),
      page:String(state.page), page_size:'25', sort:'change', order:'desc',
    });
    if (state.query.trim()) params.set('query', state.query.trim());
    return `/api/v1/rotation/board-indexes?${params}`;
  }

  function renderBoardList(payload) {
    const target = document.getElementById('market-board-list');
    const counter = document.getElementById('market-directory-count');
    const pages = document.getElementById('market-list-pages');
    if (!target || !pages) return;
    const data = payload?.data || {};
    const items = data.items || [];
    const pagination = data.pagination || {};
    if (counter) counter.textContent = `${pagination.total ?? items.length} 个`;
    if (!items.length) {
      target.innerHTML = '<div class="market-state"><strong>没有匹配板块</strong><p>调整分类或搜索词后重试。</p></div>';
    } else {
      target.innerHTML = items.map((item,index) => `<button type="button" role="option" data-market-board="${esc(item.board_code || item.code)}" data-category="${esc(item.category || state.category)}" aria-selected="${String((item.board_code || item.code) === state.code)}" tabindex="${(item.board_code || item.code) === state.code ? 0 : -1}"><span>${String((pagination.page - 1) * pagination.page_size + index + 1).padStart(2,'0')}</span><div><strong>${esc(item.name || item.board_code)}</strong><small>${esc(item.board_code || '')} · 覆盖 ${ratio(item.coverage)}</small></div><output class="${tone(item.change)}">${percent(item.change)}</output></button>`).join('');
    }
    pages.innerHTML = `<button type="button" data-market-page="${Math.max(1,Number(pagination.page || 1)-1)}" ${pagination.has_previous ? '' : 'disabled'}>上一页</button><span>${pagination.page || 1} / ${pagination.pages || 1}</span><button type="button" data-market-page="${Number(pagination.page || 1)+1}" ${pagination.has_next ? '' : 'disabled'}>下一页</button>`;
  }

  async function loadBoardList({keepCode = true} = {}) {
    const token = generation;
    const target = document.getElementById('market-board-list');
    target?.setAttribute('aria-busy','true');
    try {
      const payload = await readJson(boardListPath(), {owner:'list'});
      if (token !== generation || !mounted) return;
      state.list = payload;
      const items = payload?.data?.items || [];
      const containsSelected = items.some(item => (item.board_code || item.code) === state.code);
      if ((!keepCode || !state.code || (!containsSelected && !context?.route?.code)) && items.length) {
        state.code = items[0].board_code || items[0].code;
      }
      renderBoardList(payload);
      updateRoute();
      if (state.code) await loadBoardDetail();
      else renderEmptyDetail();
    } catch (error) {
      if (error?.name === 'AbortError' || token !== generation) return;
      if (target) target.innerHTML = errorMarkup(error.message, 'data-market-list-retry');
    } finally {
      target?.removeAttribute('aria-busy');
    }
  }

  function renderEmptyDetail() {
    document.getElementById('market-focus-title').textContent = '选择一个板块';
    document.getElementById('market-chart-empty').hidden = false;
    document.getElementById('market-method-compare').replaceChildren();
    document.getElementById('market-board-evidence').innerHTML = '<div class="market-state"><strong>等待板块选择</strong><p>从左侧目录选择板块查看五算法指数与当前成分。</p></div>';
    window.QuantCharts?.dispose('market-board-chart');
  }

  function methodChange(method, detail) {
    const changes = detail?.comparison?.[method]?.changes || {};
    return changes[String(state.window)];
  }

  function renderBoardChart(detail) {
    const empty = document.getElementById('market-chart-empty');
    const values = detail.series || [];
    if (!values.length || detail.method_status?.status === 'unavailable') {
      window.QuantCharts?.dispose('market-board-chart');
      empty.hidden = false;
      empty.textContent = detail.method_status?.reason || '当前算法暂无可用日线';
      return;
    }
    empty.hidden = true;
    const chart = mkChart('market-board-chart');
    if (!chart) return;
    chart.setOption(baseOpt({
      animationDuration:window.REDUCED_MOTION ? 0 : 260,
      grid:{left:52,right:20,top:28,bottom:45},
      tooltip:{trigger:'axis',valueFormatter:value => number(value,2)},
      xAxis:timeAxis(),
      yAxis:{type:'value',scale:true,axisLabel:{color:MUTED,fontSize:10},splitLine:{lineStyle:{color:GRID}}},
      dataZoom:[{type:'inside',filterMode:'none'},{type:'slider',height:12,bottom:8,showDetail:false,borderColor:AXIS}],
      series:[{name:METHODS[state.method],type:'line',showSymbol:false,smooth:false,data:values.map(row => [row.date,row.close]),lineStyle:{width:2,color:CHART_COLORS.primary},areaStyle:{opacity:.05,color:CHART_COLORS.primary}}],
    }), {notMerge:true});
  }

  function renderComparison(detail) {
    const target = document.getElementById('market-method-compare');
    target.innerHTML = Object.entries(METHODS).map(([method,label]) => {
      const current = detail.comparison?.[method] || {};
      const change = methodChange(method, detail);
      const unavailable = current.status === 'unavailable';
      return `<button type="button" data-market-method="${method}" data-active="${String(method === state.method)}" ${unavailable ? 'disabled' : ''}><span>${label}</span><strong class="${tone(change)}">${unavailable ? '不可用' : percent(change)}</strong><small>${unavailable ? esc(current.reason || '证据不足') : `${state.window}日 · ${number(current.last,2)}`}</small></button>`;
    }).join('');
  }

  function renderEvidence(detail, constituentsPayload) {
    const target = document.getElementById('market-board-evidence');
    const meta = state.list?.meta || {};
    const quality = meta.quality || {};
    const constituents = constituentsPayload?.data?.items || [];
    const coverage = detail.coverage;
    const status = detail.method_status?.status || quality.status;
    target.innerHTML = `<div class="market-board-status" data-state="${esc(status || 'unknown')}">
      <div><span>指数状态</span><strong>${esc(statusLabel(status))}</strong></div><div><span>当前成分</span><strong>${detail.constituent_count ?? detail.member_count ?? '—'}</strong></div><div><span>行情覆盖</span><strong>${ratio(coverage)}</strong></div>
    </div>
    <div class="market-constituent-head"><div><h3>当前成分排行</h3><p>点击个股后才读取历史行情</p></div><span>${constituentsPayload?.data?.pagination?.total ?? constituents.length} 只</span></div>
    <div class="market-constituents">${constituents.map(item => `<button type="button" data-market-stock="${esc(item.symbol)}" data-stock-name="${esc(item.name || item.symbol)}"><div><strong>${esc(item.name || item.symbol)}</strong><small>${esc(item.symbol)} · ${esc(item.as_of || '')}</small></div><span>${number(item.last,2)}</span><output class="${tone(item.change_pct)}">${percent(item.change_pct)}</output></button>`).join('') || '<div class="market-state"><strong>暂无成分报价</strong><p>指数曲线仍可独立使用。</p></div>'}</div>
    <details class="market-quality"><summary>数据质量与口径</summary><dl><div><dt>成分口径</dt><dd>当前成分股回溯</dd></div><div><dt>频率 / 基准</dt><dd>${esc(detail.frequency || '1d')} / ${number(detail.base,0)}</dd></div><div><dt>算法版本</dt><dd>${esc(meta.board_index_algorithm_version || meta.algorithm_version || '—')}</dd></div><div><dt>数据日期</dt><dd>${esc(meta.as_of || '—')}</dd></div><div><dt>来源</dt><dd>${esc((meta.sources || ['本地 StockDB']).join(' · '))}</dd></div></dl><p>历史曲线采用 current_constituents_backcast，不代表历史时点成分。</p>${(quality.issues || []).map(item => `<p>${esc(item)}</p>`).join('')}</details>`;
  }

  async function loadBoardDetail() {
    const token = generation;
    abort('history');
    closeStockHistory();
    document.getElementById('market-focus-title').textContent = '读取板块详情…';
    document.getElementById('market-board-evidence').innerHTML = '<div class="market-loading-line"><i></i><span>读取成分与质量…</span></div>';
    const code = encodeURIComponent(state.code);
    const detailPath = `/api/v1/rotation/board-indexes/${state.category}/${code}?method=${state.method}`;
    const constituentPath = `/api/v1/rotation/board-indexes/${state.category}/${code}/constituents?page=1&page_size=25&sort=change&order=desc`;
    try {
      const detailSignal = nextSignal('detail');
      const constituentSignal = nextSignal('constituents');
      const [detailPayload,constituentsPayload] = await Promise.all([
        readJsonWithSignal(detailPath, detailSignal), readJsonWithSignal(constituentPath, constituentSignal),
      ]);
      if (token !== generation || !mounted) return;
      state.detail = detailPayload;
      state.constituents = constituentsPayload;
      const detail = detailPayload.data || {};
      document.getElementById('market-focus-title').textContent = detail.name || state.code;
      const head = document.getElementById('market-focus-head');
      head.querySelector('span').textContent = `${CATEGORIES[state.category]} · ${detail.board_code || state.code}`;
      renderBoardChart(detail);
      renderComparison(detail);
      renderEvidence(detail,constituentsPayload);
      renderBoardList(state.list);
      updateRoute();
    } catch (error) {
      if (error?.name === 'AbortError' || token !== generation) return;
      document.getElementById('market-focus-title').textContent = state.code;
      document.getElementById('market-chart-empty').hidden = false;
      document.getElementById('market-chart-empty').textContent = error.message;
      document.getElementById('market-board-evidence').innerHTML = errorMarkup(error.message, 'data-market-detail-retry');
      window.QuantCharts?.dispose('market-board-chart');
    }
  }

  async function readJsonWithSignal(path, signal) {
    const cached = responseCache.get(path);
    const headers = {'Accept':'application/json'};
    if (cached?.etag) headers['If-None-Match'] = cached.etag;
    const response = await fetch(path,{headers,signal,credentials:'same-origin'});
    if (response.status === 304 && cached) return cached.value;
    const value = await response.json().catch(() => ({}));
    if (!response.ok) throw new Error(value?.problem?.message || value?.detail || `读取失败（HTTP ${response.status}）`);
    responseCache.set(path,{etag:response.headers.get('ETag') || '',value});
    return value;
  }

  function closeStockHistory() {
    abort('history');
    state.selectedStock = '';
    const panel = document.getElementById('market-stock-history');
    if (panel) panel.hidden = true;
    window.QuantCharts?.dispose('market-stock-chart');
  }

  async function loadStockHistory(symbol, name) {
    const panel = document.getElementById('market-stock-history');
    if (!panel) return;
    state.selectedStock = symbol;
    panel.hidden = false;
    document.getElementById('market-stock-title').textContent = `${name} · ${symbol}`;
    const chartTarget = document.getElementById('market-stock-chart');
    chartTarget.innerHTML = '<div class="market-loading-line"><i></i><span>读取个股历史…</span></div>';
    try {
      const payload = await readJson(`/api/v1/market/history/${encodeURIComponent(symbol)}?frequency=1d`,{owner:'history'});
      if (!mounted || state.selectedStock !== symbol) return;
      chartTarget.replaceChildren();
      const chart = mkChart('market-stock-chart');
      const rows = payload.kline || [];
      if (!chart || !rows.length) throw new Error('个股历史暂不可用');
      chart.setOption(baseOpt({
        animationDuration:window.REDUCED_MOTION ? 0 : 220,
        grid:{left:45,right:12,top:18,bottom:36},
        xAxis:{type:'category',data:rows.map(row => row[0]),axisLabel:{color:MUTED,fontSize:9,hideOverlap:true},axisLine:{lineStyle:{color:AXIS}}},
        yAxis:{type:'value',scale:true,axisLabel:{color:MUTED,fontSize:9},splitLine:{lineStyle:{color:GRID}}},
        dataZoom:[{type:'inside',filterMode:'none'},{type:'slider',height:10,bottom:6,showDetail:false}],
        series:[{type:'candlestick',data:rows.map(row => [row[1],row[2],row[3],row[4]]),itemStyle:{color:CHART_COLORS.up,color0:CHART_COLORS.down,borderColor:CHART_COLORS.up,borderColor0:CHART_COLORS.down}}],
      }),{notMerge:true});
    } catch (error) {
      if (error?.name === 'AbortError') return;
      chartTarget.innerHTML = errorMarkup(error.message);
    }
  }

  function selectCategory(value) {
    if (!Object.hasOwn(CATEGORIES,value) || value === state.category) return;
    state.category = value; state.code = ''; state.page = 1;
    document.querySelectorAll('[data-market-category]').forEach(button => {
      const selected = button.dataset.marketCategory === value;
      button.setAttribute('aria-selected',String(selected)); button.tabIndex = selected ? 0 : -1;
    });
    context.route.code = '';
    loadBoardList({keepCode:false});
  }

  function selectMethod(value) {
    if (!Object.hasOwn(METHODS,value) || value === state.method) return;
    state.method = value; state.page = 1;
    document.querySelectorAll('.market-method-tabs [data-market-method]').forEach(button => {
      const selected = button.dataset.marketMethod === value;
      button.setAttribute('aria-selected',String(selected)); button.tabIndex = selected ? 0 : -1;
    });
    loadBoardList();
  }

  function selectWindow(value) {
    if (!WINDOWS.includes(value) || value === state.window) return;
    state.window = value; state.page = 1;
    document.querySelectorAll('[data-market-window]').forEach(button => button.setAttribute('aria-pressed',String(Number(button.dataset.marketWindow) === value)));
    void loadDecisionStrip(generation);
    loadBoardList();
  }

  async function refreshIndexes(button) {
    if (button.disabled) return;
    button.disabled = true;
    const progress = document.getElementById('market-refresh-progress');
    try {
      let job = await post('/api/v1/market/analytics/refresh',{scope:'indexes',mode:'incremental',source:'auto',purpose:'current_analysis'});
      while (['queued','running','cancelling'].includes(String(job.status || ''))) {
        progress.textContent = `${job.phase || '刷新指数'} · ${Math.round(Number(job.progress || 0))}%`;
        await new Promise(resolve => setTimeout(resolve,1000));
        job = await api(`/api/v1/jobs/${encodeURIComponent(job.id)}`);
        if (!mounted) return;
      }
      if (!String(job.status || '').startsWith('completed')) throw new Error(job.message || job.detail || '指数刷新未完成');
      progress.textContent = '指数快照已更新';
      responseCache.clear();
      await Promise.all([loadDecisionStrip(generation),loadBoardList()]);
    } catch (error) {
      progress.textContent = `刷新失败 · ${error.message}`;
    } finally {
      button.disabled = false;
    }
  }

  function activateAdjacent(target, selector, direction) {
    const values = [...target.closest('[role="tablist"]').querySelectorAll(selector)];
    const index = values.indexOf(target);
    const next = values[(index + direction + values.length) % values.length];
    next.focus(); next.click();
  }

  function bindEvents(root) {
    root.onclick = event => {
      const category = event.target.closest('[data-market-category]');
      if (category) { selectCategory(category.dataset.marketCategory); return; }
      const method = event.target.closest('[data-market-method]');
      if (method) { selectMethod(method.dataset.marketMethod); return; }
      const windowButton = event.target.closest('[data-market-window]');
      if (windowButton) { selectWindow(Number(windowButton.dataset.marketWindow)); return; }
      const board = event.target.closest('[data-market-board]');
      if (board) {
        state.category = board.dataset.category || state.category;
        state.code = board.dataset.marketBoard;
        context.route.code = '';
        loadBoardDetail(); return;
      }
      const page = event.target.closest('[data-market-page]');
      if (page) { state.page = Number(page.dataset.marketPage) || 1; loadBoardList(); return; }
      const stock = event.target.closest('[data-market-stock]');
      if (stock) { loadStockHistory(stock.dataset.marketStock,stock.dataset.stockName || stock.dataset.marketStock); return; }
      if (event.target.closest('[data-market-stock-close]')) { closeStockHistory(); return; }
      if (event.target.closest('[data-market-list-retry]')) { loadBoardList(); return; }
      if (event.target.closest('[data-market-detail-retry]')) { loadBoardDetail(); return; }
      const refreshButton = event.target.closest('[data-market-refresh]');
      if (refreshButton) refreshIndexes(refreshButton);
    };
    root.oninput = event => {
      if (!event.target.matches('[data-market-query]')) return;
      state.query = event.target.value; state.page = 1;
      clearTimeout(searchTimer); searchTimer = setTimeout(() => loadBoardList({keepCode:false}),180);
    };
    root.onkeydown = event => {
      const tab = event.target.closest('[role="tab"]');
      if (tab && ['ArrowLeft','ArrowRight'].includes(event.key)) {
        event.preventDefault(); activateAdjacent(tab,'[role="tab"]',event.key === 'ArrowRight' ? 1 : -1); return;
      }
      const board = event.target.closest('[data-market-board]');
      if (board && ['ArrowUp','ArrowDown','Home','End'].includes(event.key)) {
        const items = [...board.closest('[role="listbox"]').querySelectorAll('[data-market-board]')];
        const index = items.indexOf(board);
        const nextIndex = event.key === 'Home' ? 0 : event.key === 'End' ? items.length - 1
          : Math.max(0,Math.min(items.length - 1,index + (event.key === 'ArrowDown' ? 1 : -1)));
        event.preventDefault(); items[nextIndex].focus(); items[nextIndex].click();
      }
    };
  }

  function consumePendingStock() {
    try {
      const pending = JSON.parse(sessionStorage.getItem('quantmaster.market.pending-stock.v1') || 'null');
      sessionStorage.removeItem('quantmaster.market.pending-stock.v1');
      if (pending?.symbol) setTimeout(() => loadStockHistory(pending.symbol,pending.name || pending.symbol),0);
    } catch (_) {}
  }

  async function mount(page, nextContext = {}) {
    context = nextContext;
    const root = document.getElementById('market-workbench-root');
    const workbench = document.getElementById('market-workbench-view');
    mounted = true; generation += 1;
    document.querySelectorAll('[data-market-view]').forEach(view => { view.hidden = true; });
    workbench.hidden = false;
    restoreRoute(nextContext.route || {});
    root.innerHTML = shellMarkup();
    bindEvents(root);
    updateRoute();
    const token = generation;
    await Promise.all([loadDecisionStrip(token),loadBoardList()]);
    if (token === generation) consumePendingStock();
  }

  function unmount() {
    mounted = false; generation += 1;
    clearTimeout(searchTimer); searchTimer = 0;
    for (const owner of [...controllers.keys()]) abort(owner);
    window.QuantCharts?.dispose('market-board-chart');
    window.QuantCharts?.dispose('market-stock-chart');
    const root = document.getElementById('market-workbench-root');
    if (root) { root.onclick = null; root.oninput = null; root.onkeydown = null; }
  }

  return {mount,unmount,refresh:() => Promise.all([loadDecisionStrip(generation),loadBoardList()])};
})();

export const {mount,unmount,refresh} = marketWorkbench;
