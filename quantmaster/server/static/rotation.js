(() => {
  'use strict';

  const STATE_LABELS = {
    strong_up:'强势加速', up:'趋势延续', range:'中位整理', weak:'低位偏弱',
  };
  const QUALITY_LABELS = {
    complete:'覆盖完整', partial:'部分覆盖', limited:'样本有限', cold:'等待快照',
    stale:'快照过期', corrupt:'数据损坏', loading:'正在计算', empty:'暂无结果',
  };
  const STYLE_LABELS = {
    strong_dominant:'强势样本占优', weak_rebound:'低位样本修复', balanced:'强弱均衡',
    pending:'等待连续确认', unavailable:'样本不足',
  };
  const cache = new Map();
  let activeMarketPage = 'quotes';
  let activeRotationPage = 'overview';
  let activeJob = null;
  const ACTIVE_JOB_KEY = 'quantmaster.rotation.active-job.v1';
  const WINDOW_KEY = 'quantmaster.rotation.window.v2';
  const WINDOWS = [1,3,5,20];
  let themeCatalog = [];
  let etfCatalog = [];
  let activeWindow = 5;
  let themePage = 1;
  let etfPage = 1;
  let industrySort = 'change';
  let themeSort = 'change';
  let etfSort = 'flow';
  let themeQuery = '';
  let themeStage = '';
  let etfQuery = '';
  let etfCategory = '';
  try {
    const savedWindow = Number(localStorage.getItem(WINDOW_KEY));
    if (WINDOWS.includes(savedWindow)) activeWindow = savedWindow;
  } catch (_) {}

  const number = (value, digits = 1) => {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed.toFixed(digits) : '—';
  };
  const percent = (value, digits = 1) => {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? `${parsed.toFixed(digits)}%` : '—';
  };
  const returnPct = (value, digits = 2) => {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? `${parsed >= 0 ? '+' : ''}${(parsed * 100).toFixed(digits)}%` : '—';
  };
  const money = value => {
    const parsed = Number(value);
    if (!Number.isFinite(parsed)) return '—';
    const sign = parsed > 0 ? '+' : '';
    if (Math.abs(parsed) >= 1e8) return `${sign}${(parsed / 1e8).toFixed(2)} 亿`;
    if (Math.abs(parsed) >= 1e4) return `${sign}${(parsed / 1e4).toFixed(1)} 万`;
    return `${sign}${parsed.toFixed(0)}`;
  };
  const tone = value => Number(value) > 0 ? 'up' : Number(value) < 0 ? 'down' : '';
  const signed = (value, digits = 1, suffix = '') => {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? `${parsed > 0 ? '+' : ''}${parsed.toFixed(digits)}${suffix}` : '—';
  };
  const signal = (item, window = activeWindow) => item?.signals?.[String(window)] || {};
  const pp = value => signed(value,1,' pp');
  const windowControl = label => `<div class="rotation-window-control" aria-label="${esc(label)}">${WINDOWS.map(window => `<button type="button" data-rotation-window="${window}" aria-pressed="${String(window === activeWindow)}">${window} 日</button>`).join('')}</div>`;

  function qualityMarkup(meta) {
    const quality = meta?.quality || {status:'cold', issues:[]};
    const status = quality.status || 'cold';
    const coverage = quality.scope_coverage ?? quality.coverage ?? quality.price_coverage;
    const availableDimensions = Number(quality.available_dimensions);
    const totalDimensions = Number(quality.total_dimensions);
    const dimensionText = Number.isFinite(availableDimensions) && Number.isFinite(totalDimensions) && totalDimensions > 0
      ? ` · ${availableDimensions}/${totalDimensions} 维度`
      : '';
    const hasCoverage = coverage !== null && coverage !== undefined && coverage !== '';
    const hasCoverageBasis = quality.scope_coverage !== null && quality.scope_coverage !== undefined
      || quality.price_coverage !== null && quality.price_coverage !== undefined
      || Number(quality.expected_count) > 0;
    const coverageText = !dimensionText && hasCoverage && hasCoverageBasis && Number.isFinite(Number(coverage))
      ? ` · ${(Number(coverage) * 100).toFixed(0)}%`
      : '';
    const title = (quality.issues || []).join('；');
    return `<span class="rotation-quality" data-status="${esc(status)}" title="${esc(title)}">${esc(QUALITY_LABELS[status] || status)}${dimensionText}${coverageText}</span>`;
  }

  function updateMeta(kind, meta) {
    const target = document.querySelector(`[data-rotation-asof="${kind}"]`);
    if (target) {
      const values = target.querySelectorAll('dd');
      if (values[0]) values[0].textContent = meta?.as_of || '尚无快照';
      if (values[1] && kind === 'temperature') values[1].textContent = meta?.algorithm_version || '等待快照';
      if (values[1] && kind === 'overview') values[1].textContent = meta?.algorithm_version || '等待快照';
      if (values[1] && kind === 'themes') {
        const source = (meta?.sources || []).find(value => String(value).includes('concept'));
        const labels = {'eastmoney-concept':'东方财富概念','tushare:dc-concept':'Tushare DC 概念','ths:concept':'同花顺概念'};
        values[1].textContent = labels[source] || source || '等待目录';
      }
    }
    const group = kind === 'temperature' || kind === 'structure' ? 'market' : 'rotation';
    const line = document.querySelector(`[data-rotation-meta="${group}"] .rotation-meta-line`);
    if (line && meta) {
      const sources = [...(meta.sources || [])];
      if (kind === 'themes') sources.sort((left, right) => (
        Number(String(right).includes('concept')) - Number(String(left).includes('concept'))
      ));
      const source = sources.slice(0, 2).join(' · ') || '本地缓存';
      line.innerHTML = `${qualityMarkup(meta)}<span>${esc(meta.as_of || '尚无日期')}</span><span>${esc(source)}</span>`;
    }
  }

  function issuesMarkup(meta) {
    const issues = meta?.quality?.issues || [];
    if (!issues.length) return '';
    const toneValue = meta.quality.status === 'corrupt' ? 'error' : 'warning';
    return `<aside class="rotation-callout" data-tone="${toneValue}"><strong>数据说明</strong><ul>${issues.map(item => `<li>${esc(item)}</li>`).join('')}</ul></aside>`;
  }

  function emptyMarkup(meta, fallback, scope = 'all') {
    const message = meta?.quality?.issues?.[0] || fallback;
    return `<div class="rotation-empty"><strong>${esc(QUALITY_LABELS[meta?.quality?.status] || '暂无可展示结果')}</strong><p>${esc(message)}</p><button class="rotation-refresh" type="button" data-rotation-refresh="${esc(scope)}">生成联动快照</button></div>`;
  }

  function errorMarkup(error) {
    return `<div class="rotation-callout" data-tone="error"><strong>页面数据未能读取</strong><span>${esc(error?.message || '请稍后重试')}</span></div>`;
  }

  async function fetchView(key, path, force = false) {
    if (!force && cache.has(key)) return cache.get(key);
    const task = api(path);
    cache.set(key, task);
    try {
      const value = await task;
      cache.set(key, value);
      return value;
    } catch (error) {
      cache.delete(key);
      throw error;
    }
  }

  function temperatureChart(history) {
    const chart = mkChart('rotation-temperature-chart');
    if (!chart) return;
    const series = [
      ['市场温度','temperature',CHART_COLORS.primary,2], ['MA5','ma5',CHART_COLORS.neutral,1.2],
      ['MA10','ma10',CHART_COLORS.warning,1.2], ['MA20','ma20',CHART_COLORS.compare,1.2],
    ].map(([name, field, color, width], index) => ({
      name, type:'line', showSymbol:false, smooth:index > 0, connectNulls:false,
      lineStyle:{width,color}, itemStyle:{color},
      data:history.map(row => [row.date, row[field]]),
      markLine:index === 0 ? {
        silent:true, symbol:'none', label:{color:MUTED,fontSize:9,formatter:'{b}'},
        lineStyle:{color:AXIS,type:'dashed',width:1},
        data:[{name:'冰点 10',yAxis:10},{name:'收缩 25',yAxis:25},{name:'过热 50',yAxis:50}],
      } : undefined,
    }));
    chart.setOption(baseOpt({
      legend:{top:0,textStyle:{color:INK2,fontSize:10}},
      grid:{left:46,right:18,top:38,bottom:34}, xAxis:timeAxis(),
      yAxis:{type:'value',min:0,max:100,axisLabel:{color:MUTED,formatter:'{value}%'},splitLine:{lineStyle:{color:GRID}}},
      tooltip:{trigger:'axis',backgroundColor:'#1a1a19',borderColor:AXIS,textStyle:{color:'#fff',fontSize:11},valueFormatter:value => `${number(value,1)}%`},
      series,
    }));
  }

  function renderTemperature(payload) {
    const meta = payload.meta || {};
    const data = payload.data || {};
    const out = document.getElementById('market-temperature-content');
    updateMeta('temperature', meta);
    if (!data.current) {
      out.innerHTML = emptyMarkup(meta, data.message || '请先生成市场温度快照。', 'market');
      return;
    }
    const current = data.current;
    const ratios = current.ratios || {};
    out.innerHTML = `
      <div class="rotation-kpis">
        <div class="rotation-kpi"><span>市场温度</span><strong class="${Number(current.temperature) >= 50 ? 'up' : ''}">${percent(current.temperature)}</strong><small>趋势向上样本占比</small></div>
        <div class="rotation-kpi"><span>温度区间</span><strong>${esc(current.regime_label || '—')}</strong><small>${esc(current.regime || '')}</small></div>
        <div class="rotation-kpi"><span>强势加速</span><strong>${percent(ratios.strong_up)}</strong><small>${Number(current.counts?.strong_up || 0).toLocaleString()} 只</small></div>
        <div class="rotation-kpi"><span>有效样本</span><strong>${Number(current.eligible_count || 0).toLocaleString()}</strong><small>停牌与缺失不进分母</small></div>
      </div>
      <div class="rotation-layout two">
        <section class="rotation-section"><div class="rotation-section-head"><div><h3>温度序列</h3><p>市场温度及 5 / 10 / 20 日均线</p></div><output>${esc(data.as_of || '')}</output></div><div class="rotation-chart tall" id="rotation-temperature-chart"></div></section>
        <section class="rotation-section"><div class="rotation-section-head"><div><h3>四档分布</h3><p>同一有效样本互斥归类，家数严格守恒</p></div></div>
          <div class="rotation-state-list">${Object.keys(STATE_LABELS).map(state => `<div class="rotation-state-row"><strong>${STATE_LABELS[state]}</strong><div class="rotation-meter"><i style="--ratio:${Math.max(0,Math.min(1,Number(ratios[state] || 0)/100))}"></i></div><output>${percent(ratios[state])} · ${Number(current.counts?.[state] || 0).toLocaleString()}</output></div>`).join('')}</div>
        </section>
      </div>
      <section class="rotation-section"><div class="rotation-section-head"><div><h3>证据分解</h3><p>缺失维度从有效权重中剔除，不按零分处理</p></div><output>有效权重 ${data.evidence?.available_weight || 0}/100 · 综合 ${number(data.evidence?.score,1)}</output></div>
        <div class="rotation-evidence-list">${(data.evidence?.items || []).map(item => `<div class="rotation-evidence-row" data-available="${item.available}"><strong>${esc(item.label)}</strong><div><div class="rotation-meter"><i style="--ratio:${item.available ? Math.max(0,Math.min(1,Number(item.score)/100)) : 0}"></i></div><span>${esc(item.note || '')}</span></div><output>${item.available ? number(item.score,1) : '待补'} · ${item.weight}</output></div>`).join('')}</div>
      </section>${issuesMarkup(meta)}`;
    temperatureChart(data.history || []);
  }

  function structureChart(history) {
    const chart = mkChart('rotation-structure-chart');
    if (!chart) return;
    chart.setOption(baseOpt({
      legend:{top:0,textStyle:{color:INK2,fontSize:10}},
      grid:{left:52,right:18,top:38,bottom:34}, xAxis:timeAxis(),
      yAxis:{type:'value',axisLabel:{color:MUTED,formatter:value => `${(value * 100).toFixed(1)}%`},splitLine:{lineStyle:{color:GRID}}},
      series:[
        {name:'强势样本',type:'line',showSymbol:false,data:history.map(row => [row.date,row.strong_return]),lineStyle:{color:CHART_COLORS.up,width:1.5}},
        {name:'低位样本',type:'line',showSymbol:false,data:history.map(row => [row.date,row.weak_return]),lineStyle:{color:CHART_COLORS.down,width:1.5}},
        {name:'强弱差',type:'bar',barMaxWidth:6,data:history.map(row => [row.date,row.spread]),itemStyle:{color:CHART_COLORS.primary}},
      ],
    }));
  }

  function renderStructure(payload) {
    const meta = payload.meta || {}, data = payload.data || {}, out = document.getElementById('market-style-content');
    updateMeta('structure', meta);
    if (!data.current) {
      out.innerHTML = emptyMarkup(meta, data.message || '请先生成市场风格快照。', 'market');
      return;
    }
    const current = data.current;
    const label = current.confirmed === 'pending' ? `${STYLE_LABELS[current.candidate] || current.candidate} · 待确认` : STYLE_LABELS[current.confirmed] || current.confirmed;
    out.innerHTML = `
      <div class="rotation-kpis">
        <div class="rotation-kpi"><span>当前结构</span><strong>${esc(label)}</strong><small>三日连续才确认</small></div>
        <div class="rotation-kpi"><span>当日强弱差</span><strong class="${tone(current.spread_1d)}">${returnPct(current.spread_1d)}</strong><small>强势中位数 − 低位中位数</small></div>
        <div class="rotation-kpi"><span>三日均值</span><strong class="${tone(current.spread_3d)}">${returnPct(current.spread_3d)}</strong><small>过滤单日跳变</small></div>
        <div class="rotation-kpi"><span>判断死区</span><strong>±0.25 pp</strong><small>区间内记为均衡</small></div>
      </div>
      <div class="rotation-layout two">
        <section class="rotation-section"><div class="rotation-section-head"><div><h3>强弱样本收益</h3><p>柱为强弱差，折线为两组当日收益中位数</p></div></div><div class="rotation-chart tall" id="rotation-structure-chart"></div></section>
        <section class="rotation-section"><div class="rotation-section-head"><div><h3>当前分布</h3><p>上涨比例与收益中位数同时核查</p></div></div>
          <div class="rotation-state-list">${(data.distribution || []).map(row => `<div class="rotation-state-row"><strong>${esc(row.label)}</strong><span>${row.count} 只 · 上涨 ${row.positive_ratio == null ? '—' : percent(row.positive_ratio * 100)}</span><output class="${tone(row.median_return)}">${returnPct(row.median_return)}</output></div>`).join('')}</div>
        </section>
      </div>
      <div class="rotation-layout equal">
        <section class="rotation-section"><div class="rotation-section-head"><div><h3>强势样本前列</h3><p>仅用于解释结构，不构成候选清单</p></div></div>${cohortTable(data.leaders || [])}</section>
        <section class="rotation-section"><div class="rotation-section-head"><div><h3>低位样本前列</h3><p>按趋势分数从低到高</p></div></div>${cohortTable(data.laggards || [])}</section>
      </div>${issuesMarkup(meta)}`;
    structureChart(data.history || []);
  }

  function cohortTable(items) {
    if (!items.length) return '<div class="rotation-empty"><p>当前组没有足够样本。</p></div>';
    return `<div class="rotation-table-wrap"><table class="rotation-table" style="min-width:420px"><thead><tr><th>名称</th><th>代码</th><th class="numeric">趋势</th><th class="numeric">日收益</th></tr></thead><tbody>${items.map(item => `<tr><td>${esc(item.name)}</td><td>${esc(item.symbol)}</td><td class="numeric">${number(item.trend_score,3)}</td><td class="numeric ${tone(item.return_1d)}">${returnPct(item.return_1d)}</td></tr>`).join('')}</tbody></table></div>`;
  }

  function scatterOption(items) {
    const values = [];
    items.forEach(item => {
      const current = [Number(item.strong_ratio),Number(item.weak_ratio)];
      const currentSignal = signal(item);
      const previous = [current[0] - Number(currentSignal.strong_change_pp || 0),current[1] - Number(currentSignal.weak_change_pp || 0)];
      values.push(...current,...previous);
    });
    const axisMax = (offset) => {
      const maximum = Math.max(0,...values.filter((_,index) => index % 2 === offset).filter(Number.isFinite));
      return [5,10,20,40,60,80,100].find(value => value >= maximum * 1.15) || 100;
    };
    const labels = new Set([...items].sort((left,right) => Math.abs(Number(signal(right).rotation_change_pp || 0)) - Math.abs(Number(signal(left).rotation_change_pp || 0))).slice(0,8).map(item => item.code));
    const trails = items.map(item => {
      const currentSignal = signal(item);
      return {coords:[
        [Number(item.strong_ratio) - Number(currentSignal.strong_change_pp || 0),Number(item.weak_ratio) - Number(currentSignal.weak_change_pp || 0)],
        [Number(item.strong_ratio),Number(item.weak_ratio)],
      ],lineStyle:{color:Number(currentSignal.rotation_change_pp) >= 0 ? CHART_COLORS.up : CHART_COLORS.down}};
    });
    return baseOpt({
      grid:{left:50,right:22,top:24,bottom:44},
      tooltip:{trigger:'item',backgroundColor:'#1a1a19',borderColor:AXIS,textStyle:{color:'#fff',fontSize:11},formatter:params => {
        const item = params.data?.item;
        if (!item) return '';
        const currentSignal = signal(item);
        return `${esc(item.name)}<br>${activeWindow}日变化 ${pp(currentSignal.rotation_change_pp)}<br>超额 ${returnPct(currentSignal.excess_return)}<br>上涨宽度 ${percent(Number(currentSignal.advance_ratio || 0)*100)}<br>${esc(item.stage_label)}（固定3日）`;
      }},
      xAxis:{type:'value',name:'强势加速占比',nameLocation:'middle',nameGap:28,min:0,max:axisMax(0),axisLabel:{color:MUTED,formatter:'{value}%'},nameTextStyle:{color:MUTED,fontSize:10},splitLine:{lineStyle:{color:GRID}}},
      yAxis:{type:'value',name:'低位偏弱占比',nameTextStyle:{color:MUTED,fontSize:10},min:0,max:axisMax(1),axisLabel:{color:MUTED,formatter:'{value}%'},splitLine:{lineStyle:{color:GRID}}},
      series:[
        {type:'lines',coordinateSystem:'cartesian2d',silent:true,symbol:['none','arrow'],symbolSize:5,lineStyle:{width:1,opacity:.34},data:trails},
        {name:'行业',type:'scatter',data:items.map(item => ({value:[item.strong_ratio,item.weak_ratio,Math.max(7,Math.sqrt(item.eligible_count || 1)*2.2)],item,itemStyle:{color:Number(signal(item).rotation_change_pp) > 0 ? CHART_COLORS.up : Number(signal(item).rotation_change_pp) < 0 ? CHART_COLORS.down : CHART_COLORS.primary}})),symbolSize:value => value[2],label:{show:true,position:'top',color:INK2,fontSize:9,formatter:params => labels.has(params.data.item.code) ? params.data.item.name : ''}},
      ],
    });
  }

  function dimensionStrip(dimensions = {}) {
    const labels = {market:'市场',industries:'行业',themes:'题材',etf:'ETF'};
    return `<div class="rotation-dimensions" aria-label="四维快照状态">${Object.entries(labels).map(([key,label]) => {
      const value = dimensions[key] || {};
      return `<div><span>${label}</span><strong data-status="${esc(value.status || 'cold')}">${esc(QUALITY_LABELS[value.status] || value.status || '等待快照')}</strong><small>${esc(value.as_of || '尚无日期')} · ${value.eligible_count ?? '—'}/${value.expected_count ?? '—'}</small></div>`;
    }).join('')}</div>`;
  }

  function stageDistribution(title, summary = {}) {
    const stages = summary.stages || {};
    const total = Number(summary.group_count || Object.values(stages).reduce((sum,value) => sum + Number(value || 0),0));
    return `<div class="rotation-stage-summary"><div><strong>${esc(title)}</strong><span>${total} 个可计算</span></div><div class="rotation-stage-track" aria-label="${esc(title)}阶段分布">${Object.entries(STATE_LABELS).length ? Object.entries({repair_spread:'修复扩散',low_repair:'低位修复',extreme_weak:'极弱钝化',unclear:'方向未明',retreat_watch:'退潮观察',clear_retreat:'明确退潮'}).map(([key,label]) => `<i data-stage="${key}" style="--share:${total ? Number(stages[key] || 0)/total : 0}" title="${label} ${Number(stages[key] || 0)}"></i>`).join('') : ''}</div><div class="rotation-stage-legend">${Object.entries({repair_spread:'修复',low_repair:'低位修复',extreme_weak:'钝化',unclear:'未明',retreat_watch:'观察',clear_retreat:'退潮'}).map(([key,label]) => `<span><i data-stage="${key}"></i>${label} ${Number(stages[key] || 0)}</span>`).join('')}</div></div>`;
  }

  function rankingTable(rows, kind) {
    if (!rows?.length) return '<div class="rotation-empty compact"><p>当前窗口没有足够样本。</p></div>';
    return `<div class="rotation-table-wrap"><table class="rotation-table rotation-ranking-table"><thead><tr><th>板块</th><th>阶段</th><th class="numeric">变化</th><th class="numeric">超额</th><th class="numeric">宽度</th><th class="numeric">量能</th></tr></thead><tbody>${rows.map(row => `<tr><td><button type="button" data-rotation-jump="${kind}" data-code="${esc(row.code)}">${esc(row.name)}</button></td><td><span class="rotation-stage" data-stage="${esc(row.stage)}">${esc(row.stage_label)}</span></td><td class="numeric ${tone(row.signal?.rotation_change_pp)}">${pp(row.signal?.rotation_change_pp)}</td><td class="numeric ${tone(row.signal?.excess_return)}">${returnPct(row.signal?.excess_return)}</td><td class="numeric">${row.signal?.advance_ratio == null ? '—' : percent(Number(row.signal.advance_ratio)*100)}</td><td class="numeric ${tone(row.signal?.amount_activity)}">${returnPct(row.signal?.amount_activity)}</td></tr>`).join('')}</tbody></table></div>`;
  }

  function renderOverview(payload) {
    const meta = payload.meta || {}, data = payload.data || {}, out = document.getElementById('rotation-overview-content');
    updateMeta('overview',meta);
    const ranking = data.rankings?.[String(activeWindow)];
    if (!ranking) {
      out.innerHTML = emptyMarkup(meta,'当前快照尚未包含多周期信号，请刷新升级。');
      return;
    }
    const temp = data.market?.temperature;
    const tempChange = data.market?.temperature_changes?.[String(activeWindow)];
    const industryRank = ranking.industries || {}, themeRank = ranking.themes || {};
    const etfWindow = data.etf_context?.summary?.windows?.[String(activeWindow)] || {};
    const resonance = data.resonance?.[String(activeWindow)] || [];
    const benchmarks = [...(data.etf_context?.benchmarks || [])].sort((left,right) => Math.abs(Number(right.flows?.[String(activeWindow)] || 0)) - Math.abs(Number(left.flows?.[String(activeWindow)] || 0))).slice(0,12);
    out.innerHTML = `
      <div class="rotation-commandbar"><div><strong>观察窗口</strong><span>阶段固定 3 日；窗口影响变化、收益、宽度与资金统计</span></div>${windowControl('轮动总览观察窗口')}</div>
      ${dimensionStrip(data.dimensions)}
      <div class="rotation-kpis">
        <div class="rotation-kpi"><span>市场温度</span><strong>${temp ? percent(temp.temperature) : '—'}</strong><small>${activeWindow} 日 ${pp(tempChange)} · ${esc(temp?.regime_label || '等待')}</small></div>
        <div class="rotation-kpi"><span>改善行业</span><strong>${industryRank.improving_count ?? 0}/${industryRank.available ?? 0}</strong><small>强势变化 − 弱势变化 &gt; 0</small></div>
        <div class="rotation-kpi"><span>改善题材</span><strong>${themeRank.improving_count ?? 0}/${themeRank.available ?? 0}</strong><small>完整可用目录，不限 Top 16</small></div>
        <div class="rotation-kpi"><span>${activeWindow} 日 ETF 净流</span><strong class="${tone(etfWindow.net_flow)}">${money(etfWindow.net_flow)}</strong><small>${etfWindow.sessions ?? 0} 个交易日 · ${etfWindow.inflow_count ?? 0} 增 / ${etfWindow.outflow_count ?? 0} 减</small></div>
      </div>
      <section class="rotation-section"><div class="rotation-section-head"><div><h3>全量阶段分布</h3><p>阶段描述当前结构，变化榜描述所选窗口的方向与速度</p></div></div><div class="rotation-distributions">${stageDistribution('申万一级行业',data.distributions?.industries)}${stageDistribution('细分题材',data.distributions?.themes)}</div></section>
      <div class="rotation-overview-ranks">
        <section class="rotation-section"><div class="rotation-section-head"><div><h3>行业变化前列</h3><p>${activeWindow} 日净改善最快</p></div><output>全量 ${industryRank.available ?? 0}</output></div>${rankingTable(industryRank.leaders,'industry')}</section>
        <section class="rotation-section"><div class="rotation-section-head"><div><h3>行业变化末位</h3><p>末位不等于负值，以实际符号为准</p></div></div>${rankingTable(industryRank.laggards,'industry')}</section>
        <section class="rotation-section"><div class="rotation-section-head"><div><h3>题材变化前列</h3><p>${activeWindow} 日净改善最快</p></div><output>全量 ${themeRank.available ?? 0}</output></div>${rankingTable(themeRank.leaders,'theme')}</section>
        <section class="rotation-section"><div class="rotation-section-head"><div><h3>题材变化末位</h3><p>关联行业来自真实成分交集</p></div></div>${rankingTable(themeRank.laggards,'theme')}</section>
      </div>
      <section class="rotation-section"><div class="rotation-section-head"><div><h3>行业—题材共振</h3><p>至少两个题材映射到同一一级行业才判定；不合成总分</p></div><output>${resonance.filter(row => row.status !== 'insufficient').length}/${resonance.length} 可判定</output></div>
        <div class="rotation-table-wrap"><table class="rotation-table rotation-resonance-table"><thead><tr><th>行业</th><th>一致性</th><th class="numeric">行业变化</th><th class="numeric">行业超额</th><th class="numeric">题材中位</th><th>关联题材</th></tr></thead><tbody>${resonance.map(row => `<tr><td><button type="button" data-rotation-jump="industry" data-code="${esc(row.code)}">${esc(row.name)}</button></td><td><span class="rotation-resonance" data-status="${esc(row.status)}">${{improving:'同步改善',retreating:'同步转弱',diverging:'证据分歧',insufficient:'题材不足'}[row.status] || '待核查'}</span></td><td class="numeric ${tone(row.industry_change_pp)}">${pp(row.industry_change_pp)}</td><td class="numeric ${tone(row.industry_excess_return)}">${returnPct(row.industry_excess_return)}</td><td class="numeric ${tone(row.theme_median_change_pp)}">${pp(row.theme_median_change_pp)}</td><td>${row.themes?.map(theme => `<button type="button" class="rotation-inline-link" data-rotation-jump="theme" data-code="${esc(theme.code)}">${esc(theme.name)}</button>`).join('') || '<span class="hint">不足 2 个</span>'}<div class="hint">${row.improving_theme_count || 0} 改善 · ${row.retreating_theme_count || 0} 转弱</div></td></tr>`).join('')}</tbody></table></div>
      </section>
      <section class="rotation-section"><div class="rotation-section-head"><div><h3>ETF 跟踪基准背景</h3><p>只解释整体风险偏好，不作为行业资金流</p></div><output>${data.etf_context?.benchmarks?.length || 0} 个基准</output></div>
        <div class="rotation-benchmark-grid">${benchmarks.map(item => `<div><strong>${esc(item.benchmark)}</strong><span>${item.fund_count} 只 · ${esc(item.category)}</span><output class="${tone(item.flows?.[String(activeWindow)])}">${money(item.flows?.[String(activeWindow)])}</output></div>`).join('') || '<div class="rotation-empty compact"><p>等待 ETF 跟踪基准快照。</p></div>'}</div>
      </section>${issuesMarkup(meta)}`;
  }

  function sortedIndustryItems(items) {
    return [...items].sort((left,right) => {
      const a = signal(left), b = signal(right);
      if (industrySort === 'excess') return Number(b.excess_return ?? -Infinity) - Number(a.excess_return ?? -Infinity);
      if (industrySort === 'amount') return Number(b.amount_activity ?? -Infinity) - Number(a.amount_activity ?? -Infinity);
      if (industrySort === 'weak') return Number(b.weak_ratio ?? -Infinity) - Number(a.weak_ratio ?? -Infinity);
      return Number(b.rotation_change_pp ?? -Infinity) - Number(a.rotation_change_pp ?? -Infinity);
    });
  }

  function renderIndustries(payload) {
    const meta = payload.meta || {}, data = payload.data || {}, out = document.getElementById('rotation-industry-content');
    updateMeta('industries',meta);
    const items = data.items || [];
    if (!items.length) { out.innerHTML = emptyMarkup(meta,data.message || '行业成分尚未达到计算门槛。','industries'); return; }
    if (!items[0]?.signals) { out.innerHTML = emptyMarkup(meta,'行业快照需要升级后才能展示多周期信号。','industries'); return; }
    const l1 = items.filter(item => item.level === 'L1');
    const rows = sortedIndustryItems(items);
    const best = sortedIndustryItems(l1)[0], worst = sortedIndustryItems(l1).at(-1);
    out.innerHTML = `
      <div class="rotation-commandbar"><div><strong>行业观察窗口</strong><span>坐标位置是当前值，箭头起点为 ${activeWindow} 个交易日前</span></div>${windowControl('行业周期观察窗口')}</div>
      <div class="rotation-kpis"><div class="rotation-kpi"><span>有效一级行业</span><strong>${l1.length}</strong><small>申万 2021 共 31 个</small></div><div class="rotation-kpi"><span>变化最快</span><strong>${esc(best?.name || '—')}</strong><small class="${tone(signal(best).rotation_change_pp)}">${pp(signal(best).rotation_change_pp)}</small></div><div class="rotation-kpi"><span>变化末位</span><strong>${esc(worst?.name || '—')}</strong><small class="${tone(signal(worst).rotation_change_pp)}">${pp(signal(worst).rotation_change_pp)}</small></div><div class="rotation-kpi"><span>覆盖门槛</span><strong>8 · 70%</strong><small>最少成分 · 行情覆盖</small></div></div>
      <section class="rotation-section"><div class="rotation-section-head"><div><h3>周期坐标与 ${activeWindow} 日轨迹</h3><p>横轴强势加速、纵轴低位偏弱；仅标记变化绝对值最大的行业</p></div><output>${l1.length} 个一级行业</output></div><div class="rotation-chart tall" id="rotation-industry-scatter"></div></section>
      <section class="rotation-section"><div class="rotation-section-head"><div><h3>行业信号矩阵</h3><p>阶段固定 3 日；表格按独立证据排序，不合成综合分</p></div><label class="rotation-compact-field">排序<select data-rotation-industry-sort><option value="change" ${industrySort === 'change' ? 'selected' : ''}>轮动变化</option><option value="excess" ${industrySort === 'excess' ? 'selected' : ''}>相对收益</option><option value="amount" ${industrySort === 'amount' ? 'selected' : ''}>量能活跃</option><option value="weak" ${industrySort === 'weak' ? 'selected' : ''}>低位占比</option></select></label></div>
        <div class="rotation-table-wrap"><table class="rotation-table rotation-signal-table"><thead><tr><th>行业</th><th>阶段（3日）</th><th class="numeric">${activeWindow}日变化</th><th class="numeric">成员收益</th><th class="numeric">超额</th><th class="numeric">上涨宽度</th><th class="numeric">量能</th><th class="numeric">强势 / 低位</th><th class="numeric">覆盖</th></tr></thead><tbody>${rows.map(item => { const current = signal(item); return `<tr><td><button type="button" data-rotation-detail="industry" data-code="${esc(item.code)}">${esc(item.name)}</button><div class="hint">${esc(item.code)} · ${esc(item.level)}</div></td><td><span class="rotation-stage" data-stage="${esc(item.stage)}">${esc(item.stage_label)}</span></td><td class="numeric ${tone(current.rotation_change_pp)}">${pp(current.rotation_change_pp)}</td><td class="numeric ${tone(current.member_return)}">${returnPct(current.member_return)}</td><td class="numeric ${tone(current.excess_return)}">${returnPct(current.excess_return)}</td><td class="numeric">${current.advance_ratio == null ? '—' : percent(Number(current.advance_ratio)*100)}</td><td class="numeric ${tone(current.amount_activity)}">${returnPct(current.amount_activity)}</td><td class="numeric">${percent(item.strong_ratio)} / ${percent(item.weak_ratio)}</td><td class="numeric">${item.eligible_count}/${item.member_count}</td></tr>`; }).join('')}</tbody></table></div>
      </section><section class="rotation-detail" id="rotation-industry-detail" hidden></section><details class="rotation-l2"><summary><span>二级行业关注区 <small class="rotation-l2-copy">最多 30 个，不改变一级行业汇总</small></span></summary><div id="rotation-l2-options"><div class="rotation-skeleton"><span></span></div></div></details>${issuesMarkup(meta)}`;
    const chart = mkChart('rotation-industry-scatter');
    if (chart) chart.setOption(scatterOption(l1));
    loadL2Options();
  }

  async function loadL2Options() {
    const target = document.getElementById('rotation-l2-options');
    if (!target) return;
    try {
      const [taxonomy, preferences] = await Promise.all([
        fetchView('taxonomy','/api/v1/rotation/taxonomy/industries'),
        fetchView('preferences','/api/v1/rotation/preferences'),
      ]);
      const nodes = taxonomy.data?.l2 || [], selected = new Set(preferences.data?.l2_codes || []);
      if (!nodes.length) {
        target.innerHTML = '<div class="rotation-empty"><strong>二级目录尚未同步</strong><p>一级行业分析不受影响；二级目录可在完整申万分类同步后单独选择。</p></div>';
        return;
      }
      target.innerHTML = `<div class="rotation-l2-grid">${nodes.map(item => `<label class="rotation-l2-option"><input type="checkbox" value="${esc(item.code)}" ${selected.has(item.code) ? 'checked' : ''}><span>${esc(item.name)} · ${item.member_count} 只</span></label>`).join('')}</div><div class="rotation-l2-actions"><output id="rotation-l2-count">已选 ${selected.size}/30</output><button type="button" class="rotation-refresh" id="rotation-l2-save">保存关注区</button></div>`;
      const checks = Array.from(target.querySelectorAll('input[type=checkbox]'));
      const update = changed => {
        const active = checks.filter(input => input.checked);
        if (active.length > 30 && changed) changed.checked = false;
        const count = checks.filter(input => input.checked).length;
        target.querySelector('#rotation-l2-count').textContent = `已选 ${count}/30`;
      };
      checks.forEach(input => input.addEventListener('change', () => update(input)));
      target.querySelector('#rotation-l2-save').addEventListener('click', async event => {
        const button = event.currentTarget; button.disabled = true; button.textContent = '正在保存…';
        try {
          const body = {l2_codes:checks.filter(input => input.checked).map(input => input.value),theme_limit:preferences.data?.theme_limit || 16};
          const saved = await api('/api/v1/rotation/preferences',{method:'PUT',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
          cache.set('preferences',saved); cache.delete('industries'); cache.delete('overview');
          button.textContent = '已保存'; await loadCurrent(true);
        } catch (error) { button.textContent = '保存失败'; reportLocalError('板块联动','二级行业关注未能保存',error); }
        finally { setTimeout(() => { button.disabled = false; if (button.textContent !== '保存关注区') button.textContent = '保存关注区'; },1200); }
      });
    } catch (error) { target.innerHTML = errorMarkup(error); }
  }

  function renderThemes(payload) {
    const meta = payload.meta || {}, data = payload.data || {}, out = document.getElementById('rotation-themes-content');
    updateMeta('themes',meta); themeCatalog = data.items || [];
    if (!themeCatalog.length) { out.innerHTML = emptyMarkup(meta,data.message || '尚未建立细分题材成分目录。','themes'); return; }
    if (!themeCatalog[0]?.signals) { out.innerHTML = emptyMarkup(meta,'题材快照需要升级后才能展示多周期信号。','themes'); return; }
    const stages = [...new Set(themeCatalog.map(item => item.stage))].sort();
    out.innerHTML = `<div class="rotation-commandbar"><div><strong>题材信号矩阵</strong><span>${themeCatalog.length}/${data.total || themeCatalog.length} 个可计算题材 · 每页 50 条</span></div>${windowControl('题材观察窗口')}</div><div class="rotation-filterbar"><label>搜索<input data-rotation-theme-query type="search" value="${esc(themeQuery)}" placeholder="题材名称、代码或关联行业"></label><label>阶段<select data-rotation-theme-stage><option value="">全部阶段</option>${stages.map(value => `<option value="${esc(value)}" ${themeStage === value ? 'selected' : ''}>${esc(themeCatalog.find(item => item.stage === value)?.stage_label || value)}</option>`).join('')}</select></label><label>排序<select data-rotation-theme-sort><option value="change" ${themeSort === 'change' ? 'selected' : ''}>轮动变化</option><option value="excess" ${themeSort === 'excess' ? 'selected' : ''}>相对收益</option><option value="amount" ${themeSort === 'amount' ? 'selected' : ''}>量能活跃</option><option value="coverage" ${themeSort === 'coverage' ? 'selected' : ''}>样本覆盖</option></select></label></div><div id="rotation-theme-results"></div><section class="rotation-detail" id="rotation-theme-detail" hidden></section>${issuesMarkup(meta)}`;
    drawThemeTable();
  }

  function drawThemeTable() {
    const target = document.getElementById('rotation-theme-results'); if (!target) return;
    const needle = themeQuery.trim().toLowerCase();
    const filtered = themeCatalog.filter(item => {
      const industry = item.primary_industry?.name || '';
      return (!needle || `${item.name} ${item.code} ${industry}`.toLowerCase().includes(needle)) && (!themeStage || item.stage === themeStage);
    }).sort((left,right) => {
      const a = signal(left), b = signal(right);
      if (themeSort === 'excess') return Number(b.excess_return ?? -Infinity) - Number(a.excess_return ?? -Infinity);
      if (themeSort === 'amount') return Number(b.amount_activity ?? -Infinity) - Number(a.amount_activity ?? -Infinity);
      if (themeSort === 'coverage') return Number(right.coverage || 0) - Number(left.coverage || 0);
      return Number(b.rotation_change_pp ?? -Infinity) - Number(a.rotation_change_pp ?? -Infinity);
    });
    const pages = Math.max(1,Math.ceil(filtered.length / 50)); themePage = Math.min(themePage,pages);
    const visible = filtered.slice((themePage - 1)*50,themePage*50);
    target.innerHTML = visible.length ? `<div class="rotation-table-wrap"><table class="rotation-table rotation-signal-table"><thead><tr><th>题材</th><th>关联一级行业</th><th>阶段（3日）</th><th class="numeric">${activeWindow}日变化</th><th class="numeric">成员收益</th><th class="numeric">超额</th><th class="numeric">上涨宽度</th><th class="numeric">量能</th><th class="numeric">覆盖</th></tr></thead><tbody>${visible.map(item => { const current = signal(item); return `<tr><td><button type="button" data-rotation-detail="theme" data-code="${esc(item.code)}">${esc(item.name)}</button><div class="hint">${esc(item.code)}</div></td><td>${item.primary_industry ? `${esc(item.primary_industry.name)}<div class="hint">${percent(Number(item.primary_industry.theme_share || 0)*100)} · ${item.primary_industry.overlap_count} 只</div>` : '<span class="hint">跨行业 / 证据不足</span>'}</td><td><span class="rotation-stage" data-stage="${esc(item.stage)}">${esc(item.stage_label)}</span></td><td class="numeric ${tone(current.rotation_change_pp)}">${pp(current.rotation_change_pp)}</td><td class="numeric ${tone(current.member_return)}">${returnPct(current.member_return)}</td><td class="numeric ${tone(current.excess_return)}">${returnPct(current.excess_return)}</td><td class="numeric">${current.advance_ratio == null ? '—' : percent(Number(current.advance_ratio)*100)}</td><td class="numeric ${tone(current.amount_activity)}">${returnPct(current.amount_activity)}</td><td class="numeric">${item.eligible_count}/${item.member_count}</td></tr>`; }).join('')}</tbody></table></div><div class="rotation-pagination"><span>第 ${themePage}/${pages} 页 · ${filtered.length} 条</span><div><button type="button" data-rotation-page-step="theme:-1" ${themePage <= 1 ? 'disabled' : ''}>上一页</button><button type="button" data-rotation-page-step="theme:1" ${themePage >= pages ? 'disabled' : ''}>下一页</button></div></div>` : '<div class="rotation-empty"><strong>没有匹配题材</strong><p>可缩短关键词或清除阶段筛选。</p></div>';
  }

  function renderEtf(payload) {
    const meta = payload.meta || {}, data = payload.data || {}, out = document.getElementById('rotation-etf-content');
    updateMeta('etf_flows',meta); etfCatalog = data.items || [];
    if (!etfCatalog.length) { out.innerHTML = emptyMarkup(meta,data.summary?.message || data.message || '等待 ETF 份额快照。','etf'); return; }
    const currentWindow = data.summary?.windows?.[String(activeWindow)] || {};
    const categories = [...new Set(etfCatalog.map(item => item.category))].sort();
    out.innerHTML = `<div class="rotation-commandbar"><div><strong>ETF 资金窗口</strong><span>累计资金按最近完整交易日求和；跟踪基准缺失时不猜测</span></div>${windowControl('ETF资金观察窗口')}</div><div class="rotation-kpis"><div class="rotation-kpi"><span>${activeWindow} 日净流</span><strong class="${tone(currentWindow.net_flow)}">${money(currentWindow.net_flow)}</strong><small>${currentWindow.sessions || 0} 个交易日</small></div><div class="rotation-kpi"><span>净申购 ETF</span><strong>${currentWindow.inflow_count || 0}</strong><small>窗口累计为正</small></div><div class="rotation-kpi"><span>净赎回 ETF</span><strong>${currentWindow.outflow_count || 0}</strong><small>窗口累计为负</small></div><div class="rotation-kpi"><span>收盘价降级</span><strong>${data.summary?.close_fallback_count || 0}</strong><small>净值缺失时使用</small></div></div><section class="rotation-section"><div class="rotation-section-head"><div><h3>每日与累计资金</h3><p>柱为当日净流，折线为当前本地历史累计</p></div><output>${data.daily?.length || 0} 日</output></div><div class="rotation-chart tall" id="rotation-etf-chart"></div></section><section class="rotation-section"><div class="rotation-section-head"><div><h3>跟踪基准聚合</h3><p>同一基准下的 ETF 合并，避免同质产品重复放大观感</p></div><output>${data.benchmarks?.length || 0} 个基准</output></div><div class="rotation-benchmark-grid">${[...(data.benchmarks || [])].sort((a,b) => Math.abs(Number(b.flows?.[String(activeWindow)] || 0)) - Math.abs(Number(a.flows?.[String(activeWindow)] || 0))).slice(0,20).map(item => `<div><strong>${esc(item.benchmark)}</strong><span>${item.fund_count} 只 · ${esc(item.category)}</span><output class="${tone(item.flows?.[String(activeWindow)])}">${money(item.flows?.[String(activeWindow)])}</output></div>`).join('')}</div></section><section class="rotation-section"><div class="rotation-section-head"><div><h3>ETF 贡献明细</h3><p>完整目录默认 50 行分页，逐只披露价格来源</p></div></div><div class="rotation-filterbar"><label>搜索<input data-rotation-etf-query type="search" value="${esc(etfQuery)}" placeholder="ETF 名称、代码或基准"></label><label>类别<select data-rotation-etf-category><option value="">全部类别</option>${categories.map(value => `<option value="${esc(value)}" ${etfCategory === value ? 'selected' : ''}>${esc(value)}</option>`).join('')}</select></label><label>排序<select data-rotation-etf-sort><option value="flow" ${etfSort === 'flow' ? 'selected' : ''}>窗口资金</option><option value="daily" ${etfSort === 'daily' ? 'selected' : ''}>当日资金</option><option value="name" ${etfSort === 'name' ? 'selected' : ''}>名称</option></select></label></div><div id="rotation-etf-results"></div></section>${issuesMarkup(meta)}`;
    drawEtfTable();
    const chart = mkChart('rotation-etf-chart'); if (chart) chart.setOption(baseOpt({legend:{top:0,textStyle:{color:INK2,fontSize:10}},grid:{left:64,right:64,top:38,bottom:34},xAxis:timeAxis(),yAxis:[{type:'value',axisLabel:{color:MUTED,formatter:value => money(value)},splitLine:{lineStyle:{color:GRID}}},{type:'value',axisLabel:{color:MUTED,formatter:value => money(value)},splitLine:{show:false}}],series:[{name:'当日净流',type:'bar',barMaxWidth:8,data:(data.daily || []).map(row => [row.date,row.flow]),itemStyle:{color:params => Number(params.value[1]) >= 0 ? CHART_COLORS.up : CHART_COLORS.down}},{name:'累计净流',type:'line',yAxisIndex:1,showSymbol:false,data:(data.daily || []).map(row => [row.date,row.cumulative]),lineStyle:{color:CHART_COLORS.primary,width:1.7}},{name:'累计 MA5',type:'line',yAxisIndex:1,showSymbol:false,connectNulls:false,data:(data.daily || []).map(row => [row.date,row.cumulative_ma5]),lineStyle:{color:CHART_COLORS.warning,width:1.2}},{name:'累计 MA20',type:'line',yAxisIndex:1,showSymbol:false,connectNulls:false,data:(data.daily || []).map(row => [row.date,row.cumulative_ma20]),lineStyle:{color:CHART_COLORS.compare,width:1.2}}]}));
  }

  function drawEtfTable() {
    const target = document.getElementById('rotation-etf-results'); if (!target) return;
    const needle = etfQuery.trim().toLowerCase();
    const filtered = etfCatalog.filter(item => (!needle || `${item.name} ${item.symbol} ${item.benchmark}`.toLowerCase().includes(needle)) && (!etfCategory || item.category === etfCategory)).sort((left,right) => {
      if (etfSort === 'name') return String(left.name).localeCompare(String(right.name),'zh-CN');
      if (etfSort === 'daily') return Number(right.flow || 0) - Number(left.flow || 0);
      return Number(right.flows?.[String(activeWindow)] || 0) - Number(left.flows?.[String(activeWindow)] || 0);
    });
    const pages = Math.max(1,Math.ceil(filtered.length / 50)); etfPage = Math.min(etfPage,pages);
    const visible = filtered.slice((etfPage - 1)*50,etfPage*50);
    target.innerHTML = visible.length ? `<div class="rotation-table-wrap"><table class="rotation-table"><thead><tr><th>ETF</th><th>跟踪基准</th><th>类别</th><th class="numeric">${activeWindow}日资金</th><th class="numeric">当日资金</th><th class="numeric">份额变化</th><th class="numeric">估算价格</th><th>价格来源</th></tr></thead><tbody>${visible.map(item => `<tr><td>${esc(item.name)}<div class="hint">${esc(item.symbol)}</div></td><td>${esc(item.benchmark || '未披露')}</td><td>${esc(item.category)}</td><td class="numeric ${tone(item.flows?.[String(activeWindow)])}">${money(item.flows?.[String(activeWindow)])}</td><td class="numeric ${tone(item.flow)}">${money(item.flow)}</td><td class="numeric ${tone(item.share_change)}">${number(item.share_change,0)}</td><td class="numeric">${number(item.price,4)}</td><td>${item.price_source === 'nav' ? '净值' : '<span class="rotation-stage">收盘价降级</span>'}</td></tr>`).join('')}</tbody></table></div><div class="rotation-pagination"><span>第 ${etfPage}/${pages} 页 · ${filtered.length} 条</span><div><button type="button" data-rotation-page-step="etf:-1" ${etfPage <= 1 ? 'disabled' : ''}>上一页</button><button type="button" data-rotation-page-step="etf:1" ${etfPage >= pages ? 'disabled' : ''}>下一页</button></div></div>` : '<div class="rotation-empty"><strong>没有匹配 ETF</strong><p>可缩短关键词或清除类别筛选。</p></div>';
  }

  async function openGroupDetail(kind, code) {
    const isTheme = kind === 'theme';
    const target = document.getElementById(isTheme ? 'rotation-theme-detail' : 'rotation-industry-detail');
    if (!target) return;
    target.hidden = false;
    target.innerHTML = '<div class="rotation-skeleton"><span></span><span></span></div>';
    target.scrollIntoView({behavior:REDUCED_MOTION ? 'auto' : 'smooth',block:'nearest'});
    try {
      const payload = await api(`/api/v1/rotation/${isTheme ? 'themes' : 'industries'}/${encodeURIComponent(code)}`);
      const item = payload.data || {};
      const industryContext = isTheme ? (item.primary_industry
        ? ` · 主行业 ${esc(item.primary_industry.name)} ${percent(Number(item.primary_industry.theme_share || 0)*100)}`
        : ' · 跨行业或映射证据不足') : '';
      target.innerHTML = `<div class="rotation-detail-head"><div><h3>${esc(item.name)} <span class="rotation-stage" data-stage="${esc(item.stage)}">${esc(item.stage_label)}</span></h3><p>${esc(item.code)} · ${item.eligible_count}/${item.member_count} 有效成分 · 覆盖 ${percent(Number(item.coverage || 0)*100)}${industryContext}</p></div><button type="button" class="rotation-link" data-close-rotation-detail>关闭详情</button></div><div class="rotation-detail-signals">${WINDOWS.map(window => { const current = signal(item,window); return `<div><span>${window} 日变化</span><strong class="${tone(current.rotation_change_pp)}">${pp(current.rotation_change_pp)}</strong><small>超额 ${returnPct(current.excess_return)} · 宽度 ${current.advance_ratio == null ? '—' : percent(Number(current.advance_ratio)*100)} · 量能 ${returnPct(current.amount_activity)}</small></div>`; }).join('')}</div><div class="rotation-representatives">${(item.representatives || []).map(value => `<div class="rotation-representative"><strong>${esc(value.name)}</strong><span>${esc(value.symbol)}</span><span class="${tone(value.return_1d)}">趋势 ${number(value.trend_score,3)} · ${returnPct(value.return_1d)}</span></div>`).join('') || '<span class="hint">暂无满足流动性与历史门槛的代表样本</span>'}</div><div class="rotation-chart compact" id="rotation-detail-chart"></div>`;
      const chart = mkChart('rotation-detail-chart');
      if (chart) chart.setOption(baseOpt({
        legend:{top:0,textStyle:{color:INK2,fontSize:10}},grid:{left:48,right:18,top:36,bottom:30},xAxis:timeAxis(),yAxis:{type:'value',min:0,max:100,axisLabel:{color:MUTED,formatter:'{value}%'},splitLine:{lineStyle:{color:GRID}}},series:[{name:'强势加速',type:'line',showSymbol:false,data:(item.history || []).map(row => [row.date,row.strong_ratio]),lineStyle:{color:CHART_COLORS.up,width:1.5}},{name:'低位偏弱',type:'line',showSymbol:false,data:(item.history || []).map(row => [row.date,row.weak_ratio]),lineStyle:{color:CHART_COLORS.down,width:1.5}}],
      }));
    } catch (error) { target.innerHTML = errorMarkup(error); }
  }

  async function loadCurrent(force = false) {
    const marketPage = activeMarketPage;
    const rotationPage = activeRotationPage;
    const marketActive = document.getElementById('tab-market')?.classList.contains('active');
    const rotationActive = document.getElementById('tab-rotation')?.classList.contains('active');
    const stillCurrent = () => (
      (marketActive && activeMarketPage === marketPage && document.getElementById('tab-market')?.classList.contains('active'))
      || (rotationActive && activeRotationPage === rotationPage && document.getElementById('tab-rotation')?.classList.contains('active'))
    );
    try {
      let payload;
      if (marketActive && marketPage === 'temperature') {
        payload = await fetchView('temperature','/api/v1/market/temperature',force);
        if (stillCurrent()) renderTemperature(payload);
      } else if (marketActive && marketPage === 'style') {
        payload = await fetchView('structure','/api/v1/market/structure',force);
        if (stillCurrent()) renderStructure(payload);
      } else if (rotationActive && rotationPage === 'overview') {
        payload = await fetchView('overview','/api/v1/rotation/overview',force);
        if (stillCurrent()) renderOverview(payload);
      } else if (rotationActive && rotationPage === 'industry') {
        payload = await fetchView('industries','/api/v1/rotation/industries',force);
        if (stillCurrent()) renderIndustries(payload);
      } else if (rotationActive && rotationPage === 'themes') {
        payload = await fetchView('themes','/api/v1/rotation/themes?limit=500',force);
        if (stillCurrent()) renderThemes(payload);
      } else if (rotationActive && rotationPage === 'etf-flows') {
        payload = await fetchView('etf','/api/v1/rotation/etf-flows',force);
        if (stillCurrent()) renderEtf(payload);
      }
    } catch (error) {
      if (!stillCurrent()) return;
      const target = marketActive
        ? (marketPage === 'temperature' ? document.getElementById('market-temperature-content') : document.getElementById('market-style-content'))
        : document.getElementById(`rotation-${rotationPage === 'etf-flows' ? 'etf' : rotationPage}-content`);
      if (target) target.innerHTML = errorMarkup(error);
    }
  }

  function setMarketPage(page, updateHash = true) {
    if (!['quotes','temperature','style'].includes(page)) page = 'quotes';
    activeMarketPage = page;
    document.querySelectorAll('[data-market-page]').forEach(button => {
      const selected = button.dataset.marketPage === page;
      button.setAttribute('aria-selected',String(selected));
      button.tabIndex = selected ? 0 : -1;
    });
    document.querySelectorAll('[data-market-view]').forEach(view => { view.hidden = view.dataset.marketView !== page; });
    if (updateHash && location.hash !== `#market/${page}`) history.replaceState(null,'',`#market/${page}`);
    if (page !== 'quotes') loadCurrent();
    requestAnimationFrame(() => Object.values(charts).forEach(chart => chart.resize()));
  }

  function setRotationPage(page, updateHash = true) {
    if (page === 'radar') page = 'overview';
    if (!['overview','industry','themes','etf-flows'].includes(page)) page = 'overview';
    activeRotationPage = page;
    document.querySelectorAll('[data-rotation-page]').forEach(button => {
      const selected = button.dataset.rotationPage === page;
      button.setAttribute('aria-selected',String(selected));
      button.tabIndex = selected ? 0 : -1;
    });
    document.querySelectorAll('[data-rotation-view]').forEach(view => { view.hidden = view.dataset.rotationView !== page; });
    if (updateHash && location.hash !== `#rotation/${page}`) history.replaceState(null,'',`#rotation/${page}`);
    loadCurrent();
    requestAnimationFrame(() => Object.values(charts).forEach(chart => chart.resize()));
  }

  function saveActiveJob(job, scope) {
    activeJob = job;
    try { sessionStorage.setItem(ACTIVE_JOB_KEY,JSON.stringify({id:job.id,scope})); } catch (_) {}
  }

  function clearActiveJob() {
    activeJob = null;
    try { sessionStorage.removeItem(ACTIVE_JOB_KEY); } catch (_) {}
  }

  function refreshResult(scope, title, detail, resultTone = 'warning') {
    const target = scope === 'market'
      ? document.getElementById('market-temperature-content')
      : scope === 'industries' ? document.getElementById('rotation-industry-content')
      : scope === 'themes' ? document.getElementById('rotation-themes-content')
      : scope === 'etf' ? document.getElementById('rotation-etf-content')
      : document.getElementById('rotation-overview-content');
    if (!target) return;
    target.querySelector('[data-rotation-job-result]')?.remove();
    target.insertAdjacentHTML('afterbegin',`<aside class="rotation-callout" data-rotation-job-result data-tone="${esc(resultTone)}"><strong>${esc(title)}</strong><span>${esc(detail || '')}</span></aside>`);
  }

  async function monitorRefresh(job, scope, button, idleLabel = '') {
    const idle = idleLabel || button.textContent;
    button.disabled = true;
    try {
      saveActiveJob(job,scope);
      button.textContent = `${activeJob.phase || '等待执行'} · ${activeJob.progress || 0}%`;
      while (activeJob && !['completed','failed','cancelled'].includes(activeJob.status)) {
        await new Promise(resolve => setTimeout(resolve,1200));
        activeJob = await api(`/api/v1/jobs/rotation/${activeJob.id}`);
        button.textContent = `${activeJob.phase || '正在分析'} · ${activeJob.progress || 0}%`;
      }
      if (activeJob?.status === 'completed') {
        cache.clear(); themeCatalog = []; etfCatalog = [];
        const outcome = activeJob.result?.outcome || 'updated';
        const labels = {updated:'快照已更新',partial:'部分更新完成',unchanged:'数据未推进'};
        button.textContent = labels[outcome] || '任务已完成';
        await loadCurrent(true);
        const warnings = activeJob.result?.warnings || [];
        const detail = warnings.join('；') || (
          outcome === 'unchanged'
            ? `行情仍截至 ${activeJob.result?.as_of || '原日期'}，未发现可提交的新数据。`
            : `数据截至 ${activeJob.result?.as_of || '最新快照'}。`
        );
        refreshResult(scope,labels[outcome] || '任务已完成',detail,outcome === 'updated' ? 'info' : 'warning');
      } else if (activeJob) {
        throw new Error(activeJob.detail || '刷新任务未完成');
      }
    } catch (error) {
      button.textContent = '刷新失败';
      refreshResult(scope,'联动快照刷新失败',error?.message || '请稍后重试','error');
      reportLocalError('板块联动','分析快照未能更新',error);
    } finally {
      clearActiveJob();
      setTimeout(() => { button.disabled = false; button.textContent = idle; },1000);
    }
  }

  async function refresh(scope, button) {
    if (activeJob) return;
    const idle = button.textContent;
    button.disabled = true;
    button.textContent = '正在创建任务…';
    try {
      const allowed = new Set(['all','market','industries','themes','etf']);
      const selected = allowed.has(scope) ? scope : 'all';
      const job = await post('/api/v1/market/analytics/refresh',{scope:selected,mode:'incremental',source:'auto'});
      await monitorRefresh(job,selected,button,idle);
    } catch (error) {
      clearActiveJob();
      button.disabled = false;
      button.textContent = '刷新失败';
      refreshResult(scope,'刷新任务创建失败',error?.message || '请稍后重试','error');
      reportLocalError('板块联动','刷新任务未能创建',error);
    }
  }

  function recoverActiveJob() {
    let saved;
    try { saved = JSON.parse(sessionStorage.getItem(ACTIVE_JOB_KEY) || 'null'); } catch (_) { saved = null; }
    if (!saved?.id) return;
    const scope = saved.scope || 'all';
    const button = document.querySelector(`[data-rotation-refresh="${scope}"]`)
      || document.querySelector('[data-rotation-refresh]');
    if (!button) return;
    api(`/api/v1/jobs/rotation/${encodeURIComponent(saved.id)}`)
      .then(job => monitorRefresh(job,scope,button))
      .catch(() => clearActiveJob());
  }

  function applyHash() {
    const match = location.hash.match(/^#(market|rotation)\/([a-z-]+)$/);
    if (!match) return false;
    const control = tabControl(match[1]);
    if (control) activateTab(control,{persist:true,load:false});
    if (match[1] === 'market') setMarketPage(match[2],false);
    else {
      setRotationPage(match[2],false);
      if (match[2] === 'radar') history.replaceState(null,'','#rotation/overview');
    }
    return true;
  }

  async function jumpToGroup(kind, code) {
    const page = kind === 'theme' ? 'themes' : 'industry';
    setRotationPage(page);
    await loadCurrent();
    openGroupDetail(kind,code);
  }

  document.addEventListener('click', event => {
    const market = event.target.closest('[data-market-page]');
    if (market) { setMarketPage(market.dataset.marketPage); return; }
    const rotation = event.target.closest('[data-rotation-page]');
    if (rotation) { setRotationPage(rotation.dataset.rotationPage); return; }
    const windowButton = event.target.closest('[data-rotation-window]');
    if (windowButton) {
      const selected = Number(windowButton.dataset.rotationWindow);
      if (WINDOWS.includes(selected) && selected !== activeWindow) {
        activeWindow = selected; themePage = 1; etfPage = 1;
        try { localStorage.setItem(WINDOW_KEY,String(activeWindow)); } catch (_) {}
        loadCurrent();
      }
      return;
    }
    const pageStep = event.target.closest('[data-rotation-page-step]');
    if (pageStep) {
      const [kind,step] = pageStep.dataset.rotationPageStep.split(':');
      if (kind === 'theme') { themePage = Math.max(1,themePage + Number(step)); drawThemeTable(); }
      if (kind === 'etf') { etfPage = Math.max(1,etfPage + Number(step)); drawEtfTable(); }
      return;
    }
    const jump = event.target.closest('[data-rotation-jump]');
    if (jump) { jumpToGroup(jump.dataset.rotationJump,jump.dataset.code); return; }
    const refreshButton = event.target.closest('[data-rotation-refresh]');
    if (refreshButton) { refresh(refreshButton.dataset.rotationRefresh,refreshButton); return; }
    const detail = event.target.closest('[data-rotation-detail]');
    if (detail) { openGroupDetail(detail.dataset.rotationDetail,detail.dataset.code); return; }
    const close = event.target.closest('[data-close-rotation-detail]');
    if (close) close.closest('.rotation-detail').hidden = true;
  });

  document.addEventListener('keydown', event => {
    const current = event.target.closest('.rotation-local-tabs [role="tab"]');
    if (!current || !['ArrowLeft','ArrowRight','Home','End'].includes(event.key)) return;
    const tabs = [...current.closest('[role="tablist"]').querySelectorAll('[role="tab"]')];
    const index = tabs.indexOf(current);
    const nextIndex = event.key === 'Home' ? 0 : event.key === 'End' ? tabs.length - 1
      : (index + (event.key === 'ArrowRight' ? 1 : -1) + tabs.length) % tabs.length;
    const next = tabs[nextIndex];
    event.preventDefault();
    next.focus();
    if (next.dataset.rotationPage) setRotationPage(next.dataset.rotationPage);
    else if (next.dataset.marketPage) setMarketPage(next.dataset.marketPage);
  });

  document.addEventListener('input', event => {
    if (event.target.matches('[data-rotation-theme-query]')) {
      themeQuery = event.target.value; themePage = 1; drawThemeTable();
    }
    if (event.target.matches('[data-rotation-etf-query]')) {
      etfQuery = event.target.value; etfPage = 1; drawEtfTable();
    }
  });

  document.addEventListener('change', event => {
    if (event.target.matches('[data-rotation-industry-sort]')) {
      industrySort = event.target.value; loadCurrent();
    } else if (event.target.matches('[data-rotation-theme-stage]')) {
      themeStage = event.target.value; themePage = 1; drawThemeTable();
    } else if (event.target.matches('[data-rotation-theme-sort]')) {
      themeSort = event.target.value; themePage = 1; drawThemeTable();
    } else if (event.target.matches('[data-rotation-etf-category]')) {
      etfCategory = event.target.value; etfPage = 1; drawEtfTable();
    } else if (event.target.matches('[data-rotation-etf-sort]')) {
      etfSort = event.target.value; etfPage = 1; drawEtfTable();
    }
  });

  document.querySelector('header')?.addEventListener('click', event => {
    const control = event.target.closest('[data-tab]');
    if (control?.dataset.tab === 'market' && !location.hash.startsWith('#market/')) setMarketPage(activeMarketPage);
    if (control?.dataset.tab === 'rotation' && !location.hash.startsWith('#rotation/')) setRotationPage(activeRotationPage);
  });
  window.addEventListener('hashchange',applyHash);

  window.loadRotationFeature = tab => {
    if (tab === 'market') {
      const page = location.hash.startsWith('#market/') ? location.hash.slice(8) : activeMarketPage;
      setMarketPage(page,false);
    } else if (tab === 'rotation') {
      const page = location.hash.startsWith('#rotation/') ? location.hash.slice(10) : activeRotationPage;
      setRotationPage(page,false);
    }
  };

  if (!applyHash()) setMarketPage('quotes',false);
  recoverActiveJob();
})();
