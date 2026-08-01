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
  let activeRotationPage = 'radar';
  let activeJob = null;
  const ACTIVE_JOB_KEY = 'quantmaster.rotation.active-job.v1';
  let themeCatalog = [];
  let showAllThemes = false;

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
      if (values[1] && kind === 'overview') values[1].textContent = meta?.algorithm_version || 'QM_ROTATION_V1';
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

  function scatterOption(items, name = '行业') {
    const axisMax = field => {
      const maximum = Math.max(0, ...items.map(item => Number(item[field])).filter(Number.isFinite));
      const padded = maximum * 1.15;
      return [5,10,20,40,60,80,100].find(value => value >= padded) || 100;
    };
    return baseOpt({
      grid:{left:50,right:22,top:24,bottom:44},
      tooltip:{trigger:'item',backgroundColor:'#1a1a19',borderColor:AXIS,textStyle:{color:'#fff',fontSize:11},formatter:params => {
        const item = params.data.item;
        return `${esc(item.name)}<br>强势 ${percent(item.strong_ratio)}<br>低位 ${percent(item.weak_ratio)}<br>${esc(item.stage_label)} · ${number(item.rotation_score,1)}`;
      }},
      xAxis:{type:'value',name:'强势加速占比',nameLocation:'middle',nameGap:28,min:0,max:axisMax('strong_ratio'),axisLabel:{color:MUTED,formatter:'{value}%'},nameTextStyle:{color:MUTED,fontSize:10},splitLine:{lineStyle:{color:GRID}}},
      yAxis:{type:'value',name:'低位偏弱占比',nameTextStyle:{color:MUTED,fontSize:10},min:0,max:axisMax('weak_ratio'),axisLabel:{color:MUTED,formatter:'{value}%'},splitLine:{lineStyle:{color:GRID}}},
      series:[{name,type:'scatter',data:items.map(item => ({
        value:[item.strong_ratio,item.weak_ratio,Math.max(7,Math.sqrt(item.eligible_count || 1)*2.2)], item,
        itemStyle:{color:item.stage?.includes('repair') ? CHART_COLORS.up : item.stage?.includes('retreat') ? CHART_COLORS.down : CHART_COLORS.primary},
      })),symbolSize:value => value[2],label:{show:items.length <= 18,position:'top',color:INK2,fontSize:9,formatter:params => params.data.item.name}}],
    });
  }

  function groupRows(items, kind = 'industry') {
    if (!items.length) return '<tr><td colspan="8" class="msg">暂无达到覆盖门槛的板块。</td></tr>';
    return items.map(item => `<tr>
      <td><button type="button" data-rotation-detail="${kind}" data-code="${esc(item.code)}">${esc(item.name)}</button><div class="hint">${esc(item.code)}</div></td>
      <td><span class="rotation-stage" data-stage="${esc(item.stage)}">${esc(item.stage_label)}</span></td>
      <td class="numeric">${percent(item.strong_ratio)}</td><td class="numeric">${percent(item.weak_ratio)}</td>
      <td class="numeric ${tone(item.delta_strong_3d)}">${Number(item.delta_strong_3d) > 0 ? '+' : ''}${number(item.delta_strong_3d,1)} pp</td>
      <td class="numeric ${tone(-Number(item.delta_weak_3d))}">${Number(item.delta_weak_3d) > 0 ? '+' : ''}${number(item.delta_weak_3d,1)} pp</td>
      <td class="numeric">${item.eligible_count}/${item.member_count}</td>
      <td class="numeric"><span class="rotation-grade" data-grade="${esc(item.grade)}">${esc(item.grade)}</span> · ${number(item.rotation_score,1)}</td>
    </tr>`).join('');
  }

  function topGroupTable(items, kind) {
    if (!items.length) return '<div class="rotation-empty"><p>暂无达到覆盖门槛的结果。</p></div>';
    return `<div class="rotation-table-wrap"><table class="rotation-table" style="min-width:520px"><thead><tr><th>板块</th><th>阶段</th><th class="numeric">强势</th><th class="numeric">低位</th><th class="numeric">评分</th></tr></thead><tbody>${items.map(item => `<tr><td><button type="button" data-rotation-detail="${kind}" data-code="${esc(item.code)}">${esc(item.name)}</button></td><td><span class="rotation-stage" data-stage="${esc(item.stage)}">${esc(item.stage_label)}</span></td><td class="numeric">${percent(item.strong_ratio)}</td><td class="numeric">${percent(item.weak_ratio)}</td><td class="numeric">${number(item.rotation_score,1)}</td></tr>`).join('')}</tbody></table></div>`;
  }

  function renderRadar(payload) {
    const meta = payload.meta || {}, data = payload.data || {}, out = document.getElementById('rotation-radar-content');
    updateMeta('overview', meta);
    const industries = data.industries || [], themes = data.themes || [], temp = data.temperature;
    if (!temp && !industries.length && !themes.length) {
      out.innerHTML = emptyMarkup(meta, '联动总览尚无可用子快照。');
      return;
    }
    out.innerHTML = `
      <div class="rotation-kpis">
        <div class="rotation-kpi"><span>市场温度</span><strong>${temp ? percent(temp.temperature) : '—'}</strong><small>${esc(temp?.regime_label || '等待市场快照')}</small></div>
        <div class="rotation-kpi"><span>领先行业</span><strong>${esc(industries[0]?.name || '—')}</strong><small>${esc(industries[0]?.stage_label || '暂无')}</small></div>
        <div class="rotation-kpi"><span>领先题材</span><strong>${esc(themes[0]?.name || '—')}</strong><small>${themes[0] ? `评分 ${number(themes[0].rotation_score,1)}` : '等待概念目录'}</small></div>
        <div class="rotation-kpi"><span>ETF 净流</span><strong class="${tone(data.etf?.net_flow)}">${money(data.etf?.net_flow)}</strong><small>${esc(data.etf?.status === 'ready' ? '份额资金估算' : '等待份额快照')}</small></div>
      </div>
      <div class="rotation-layout two">
        <section class="rotation-section"><div class="rotation-section-head"><div><h3>行业生命周期坐标</h3><p>右下倾向扩散，左上倾向退潮；位置只描述当前结构</p></div></div><div class="rotation-chart tall" id="rotation-radar-scatter"></div></section>
        <section class="rotation-section"><div class="rotation-section-head"><div><h3>行业前列</h3><p>按透明轮动评分排序</p></div></div>${topGroupTable(industries,'industry')}</section>
      </div>
      <section class="rotation-section"><div class="rotation-section-head"><div><h3>题材前列</h3><p>生命周期 55% + 宽度 45%；分级不是交易建议</p></div></div>${topGroupTable(themes,'theme')}</section>
      ${issuesMarkup(meta)}`;
    const chart = mkChart('rotation-radar-scatter');
    if (chart) chart.setOption(scatterOption(industries));
  }

  function renderIndustries(payload) {
    const meta = payload.meta || {}, data = payload.data || {}, out = document.getElementById('rotation-industry-content');
    updateMeta('industries', meta);
    const items = data.items || [];
    if (!items.length) {
      out.innerHTML = emptyMarkup(meta, data.message || '行业成分尚未达到计算门槛。', 'industries');
      return;
    }
    const l1 = items.filter(item => item.level === 'L1');
    out.innerHTML = `
      <div class="rotation-kpis">
        <div class="rotation-kpi"><span>有效一级行业</span><strong>${l1.length}</strong><small>申万 2021 共 31 个</small></div>
        <div class="rotation-kpi"><span>修复扩散</span><strong>${items.filter(item => ['repair_spread','low_repair'].includes(item.stage)).length}</strong><small>强势升 / 低位降</small></div>
        <div class="rotation-kpi"><span>退潮观察</span><strong>${items.filter(item => ['retreat_watch','clear_retreat'].includes(item.stage)).length}</strong><small>宽度同步走弱</small></div>
        <div class="rotation-kpi"><span>覆盖门槛</span><strong>8 · 70%</strong><small>最少成分 · 行情覆盖</small></div>
      </div>
      <section class="rotation-section"><div class="rotation-section-head"><div><h3>周期坐标</h3><p>可点表格行业查看近 120 日轨迹</p></div><output>${items.length} 个可计算节点</output></div><div class="rotation-chart tall" id="rotation-industry-scatter"></div></section>
      <section class="rotation-section"><div class="rotation-section-head"><div><h3>行业明细</h3><p>三日变化单位为百分点</p></div></div>
        <div class="rotation-table-wrap"><table class="rotation-table"><thead><tr><th>行业</th><th>周期阶段</th><th class="numeric">强势</th><th class="numeric">低位</th><th class="numeric">强势 3D</th><th class="numeric">低位 3D</th><th class="numeric">覆盖</th><th class="numeric">评分</th></tr></thead><tbody>${groupRows(items,'industry')}</tbody></table></div>
      </section>
      <section class="rotation-detail" id="rotation-industry-detail" hidden></section>
      <details class="rotation-l2"><summary><span>二级行业关注区 <small class="rotation-l2-copy">最多 30 个，不改变一级行业汇总</small></span></summary><div id="rotation-l2-options"><div class="rotation-skeleton"><span></span></div></div></details>
      ${issuesMarkup(meta)}`;
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
    updateMeta('themes', meta);
    themeCatalog = data.items || [];
    if (!themeCatalog.length) {
      out.innerHTML = emptyMarkup(meta, data.message || '尚未建立细分题材成分目录。', 'themes');
      return;
    }
    out.innerHTML = `<div class="rotation-theme-toolbar"><label for="rotation-theme-search">搜索完整题材目录<input id="rotation-theme-search" type="search" placeholder="输入题材名称或板块代码" autocomplete="off"></label><div class="rotation-meta-line"><span id="rotation-theme-count"></span>${qualityMarkup(meta)}</div></div><div id="rotation-theme-results"></div><section class="rotation-detail" id="rotation-theme-detail" hidden></section>${issuesMarkup(meta)}`;
    const input = document.getElementById('rotation-theme-search');
    input.addEventListener('input', () => drawThemeCards(input.value));
    drawThemeCards('');
  }

  function drawThemeCards(query) {
    const target = document.getElementById('rotation-theme-results');
    if (!target) return;
    const needle = String(query || '').trim().toLowerCase();
    const filtered = themeCatalog.filter(item => !needle || String(item.name).toLowerCase().includes(needle) || String(item.code).toLowerCase().includes(needle));
    const visible = needle || showAllThemes ? filtered : filtered.slice(0,16);
    document.getElementById('rotation-theme-count').textContent = `${visible.length}/${filtered.length} 个题材`;
    target.innerHTML = visible.length ? `<div class="rotation-theme-grid">${visible.map(item => `<button type="button" class="rotation-theme-card" data-rotation-detail="theme" data-code="${esc(item.code)}"><h3>${esc(item.name)}</h3><span class="rotation-stage" data-stage="${esc(item.stage)}">${esc(item.stage_label)}</span><div class="rotation-theme-metrics"><span>评分<strong>${number(item.rotation_score,1)} · ${esc(item.grade)}</strong></span><span>强势<strong>${percent(item.strong_ratio)}</strong></span><span>低位<strong>${percent(item.weak_ratio)}</strong></span></div><div class="rotation-theme-footer"><span>${item.eligible_count}/${item.member_count} 有效</span><span>上涨 ${percent(Number(item.advance_ratio || 0)*100)}</span></div></button>`).join('')}</div>${!needle && filtered.length > 16 ? `<div class="rotation-catalog-actions"><button type="button" class="rotation-refresh" id="rotation-theme-toggle">${showAllThemes ? '收起到 Top 16' : `查看全部 ${filtered.length} 个`}</button></div>` : ''}` : '<div class="rotation-empty"><strong>没有匹配题材</strong><p>尝试输入更短的名称或板块代码。</p></div>';
    document.getElementById('rotation-theme-toggle')?.addEventListener('click', () => { showAllThemes = !showAllThemes; drawThemeCards(query); });
  }

  function renderEtf(payload) {
    const meta = payload.meta || {}, data = payload.data || {}, out = document.getElementById('rotation-etf-content');
    updateMeta('etf_flows', meta);
    if (!data.items?.length) {
      out.innerHTML = emptyMarkup(meta, data.summary?.message || data.message || '等待 ETF 份额快照。', 'etf');
      return;
    }
    const summary = data.summary || {};
    out.innerHTML = `
      <div class="rotation-kpis">
        <div class="rotation-kpi"><span>当日净流</span><strong class="${tone(summary.net_flow)}">${money(summary.net_flow)}</strong><small>份额变化资金估算</small></div>
        <div class="rotation-kpi"><span>净申购</span><strong>${summary.inflow_count || 0}</strong><small>份额增加 ETF</small></div>
        <div class="rotation-kpi"><span>净赎回</span><strong>${summary.outflow_count || 0}</strong><small>份额减少 ETF</small></div>
        <div class="rotation-kpi"><span>收盘价降级</span><strong>${summary.close_fallback_count || 0}</strong><small>净值缺失时使用</small></div>
      </div>
      <section class="rotation-section"><div class="rotation-section-head"><div><h3>每日与累计资金</h3><p>柱为当日净流，折线为所选历史区间累计值</p></div></div><div class="rotation-chart tall" id="rotation-etf-chart"></div></section>
      <section class="rotation-section"><div class="rotation-section-head"><div><h3>ETF 贡献明细</h3><p>逐只披露价格来源，避免把降级估算当作净值</p></div></div><div class="rotation-table-wrap"><table class="rotation-table"><thead><tr><th>ETF</th><th>类别</th><th class="numeric">资金估算</th><th class="numeric">份额变化</th><th class="numeric">估算价格</th><th>价格来源</th></tr></thead><tbody>${data.items.map(item => `<tr><td>${esc(item.name)}<div class="hint">${esc(item.symbol)}</div></td><td>${esc(item.category)}</td><td class="numeric ${tone(item.flow)}">${money(item.flow)}</td><td class="numeric ${tone(item.share_change)}">${number(item.share_change,0)}</td><td class="numeric">${number(item.price,4)}</td><td>${item.price_source === 'nav' ? '净值' : '<span class="rotation-stage">收盘价降级</span>'}</td></tr>`).join('')}</tbody></table></div></section>${issuesMarkup(meta)}`;
    const chart = mkChart('rotation-etf-chart');
    if (chart) chart.setOption(baseOpt({
      legend:{top:0,textStyle:{color:INK2,fontSize:10}},grid:{left:64,right:64,top:38,bottom:34},xAxis:timeAxis(),
      yAxis:[{type:'value',axisLabel:{color:MUTED,formatter:value => money(value)},splitLine:{lineStyle:{color:GRID}}},{type:'value',axisLabel:{color:MUTED,formatter:value => money(value)},splitLine:{show:false}}],
      series:[
        {name:'当日净流',type:'bar',barMaxWidth:8,data:(data.daily || []).map(row => [row.date,row.flow]),itemStyle:{color:params => Number(params.value[1]) >= 0 ? CHART_COLORS.up : CHART_COLORS.down}},
        {name:'累计净流',type:'line',yAxisIndex:1,showSymbol:false,data:(data.daily || []).map(row => [row.date,row.cumulative]),lineStyle:{color:CHART_COLORS.primary,width:1.7}},
        {name:'累计 MA5',type:'line',yAxisIndex:1,showSymbol:false,connectNulls:false,data:(data.daily || []).map(row => [row.date,row.cumulative_ma5]),lineStyle:{color:CHART_COLORS.warning,width:1.2}},
        {name:'累计 MA20',type:'line',yAxisIndex:1,showSymbol:false,connectNulls:false,data:(data.daily || []).map(row => [row.date,row.cumulative_ma20]),lineStyle:{color:CHART_COLORS.compare,width:1.2}},
      ],
    }));
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
      target.innerHTML = `<div class="rotation-detail-head"><div><h3>${esc(item.name)} <span class="rotation-stage" data-stage="${esc(item.stage)}">${esc(item.stage_label)}</span></h3><p>${esc(item.code)} · ${item.eligible_count}/${item.member_count} 有效成分 · 覆盖 ${percent(Number(item.coverage || 0)*100)}</p></div><button type="button" class="rotation-link" data-close-rotation-detail>关闭详情</button></div><div class="rotation-representatives">${(item.representatives || []).map(value => `<div class="rotation-representative"><strong>${esc(value.name)}</strong><span>${esc(value.symbol)}</span><span class="${tone(value.return_1d)}">趋势 ${number(value.trend_score,3)} · ${returnPct(value.return_1d)}</span></div>`).join('') || '<span class="hint">暂无满足流动性与历史门槛的代表样本</span>'}</div><div class="rotation-chart compact" id="rotation-detail-chart"></div>`;
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
      } else if (rotationActive && rotationPage === 'radar') {
        payload = await fetchView('overview','/api/v1/rotation/overview',force);
        if (stillCurrent()) renderRadar(payload);
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
    document.querySelectorAll('[data-market-page]').forEach(button => button.setAttribute('aria-selected',String(button.dataset.marketPage === page)));
    document.querySelectorAll('[data-market-view]').forEach(view => { view.hidden = view.dataset.marketView !== page; });
    if (updateHash && location.hash !== `#market/${page}`) history.replaceState(null,'',`#market/${page}`);
    if (page !== 'quotes') loadCurrent();
    requestAnimationFrame(() => Object.values(charts).forEach(chart => chart.resize()));
  }

  function setRotationPage(page, updateHash = true) {
    if (!['radar','industry','themes','etf-flows'].includes(page)) page = 'radar';
    activeRotationPage = page;
    document.querySelectorAll('[data-rotation-page]').forEach(button => button.setAttribute('aria-selected',String(button.dataset.rotationPage === page)));
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
      : document.getElementById('rotation-radar-content');
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
        cache.clear(); themeCatalog = []; showAllThemes = false;
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
    else setRotationPage(match[2],false);
    return true;
  }

  document.addEventListener('click', event => {
    const market = event.target.closest('[data-market-page]');
    if (market) { setMarketPage(market.dataset.marketPage); return; }
    const rotation = event.target.closest('[data-rotation-page]');
    if (rotation) { setRotationPage(rotation.dataset.rotationPage); return; }
    const refreshButton = event.target.closest('[data-rotation-refresh]');
    if (refreshButton) { refresh(refreshButton.dataset.rotationRefresh,refreshButton); return; }
    const detail = event.target.closest('[data-rotation-detail]');
    if (detail) { openGroupDetail(detail.dataset.rotationDetail,detail.dataset.code); return; }
    const close = event.target.closest('[data-close-rotation-detail]');
    if (close) close.closest('.rotation-detail').hidden = true;
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
