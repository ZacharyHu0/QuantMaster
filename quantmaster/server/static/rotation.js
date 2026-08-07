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
  const STAGE_LABELS = {
    repair_spread:'修复扩散', low_repair:'低位修复', extreme_weak:'极弱钝化',
    unclear:'方向未明', retreat_watch:'退潮观察', clear_retreat:'明确退潮',
  };
  const cache = new Map();
  let activeMarketPage = 'quotes';
  let activeRotationPage = 'overview';
  let activeJob = null;
  const ACTIVE_JOB_KEY = 'quantmaster.rotation.active-job.v1';
  const WINDOW_KEY = 'quantmaster.rotation.window.v2';
  const WINDOWS = [1,3,5,20];
  let themeCatalog = [];
  let themeFocus = [];
  let themeFocusDefinition = {};
  let etfCatalog = [];
  let activeWindow = 5;
  let themePage = 1;
  let etfPage = 1;
  let themePageSize = 50;
  let etfPageSize = 50;
  let themePagination = {page:1,page_size:50,total:0,pages:1,has_previous:false,has_next:false};
  let etfPagination = {page:1,page_size:50,total:0,pages:1,has_previous:false,has_next:false};
  let activeIndustryLevel = 'L1';
  let industryPayload = null;
  let industryChartFrame = 0;
  let industrySort = 'change';
  let themeSort = 'change';
  let etfSort = 'flow';
  let themeQuery = '';
  let themeStage = '';
  let themeGrade = '';
  let etfQuery = '';
  let etfCategory = '';
  let requestVersion = 0;
  let searchTimer = 0;
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
  const groupScore = item => item?.score || {};
  const pp = value => signed(value,1,' pp');
  const scoreEvidenceMarkup = score => `<div class="rotation-evidence-list">${(score?.items || []).map(item => `<div class="rotation-evidence-row" data-available="${item.available}"><strong>${esc(item.label)}</strong><div><div class="rotation-meter"><i style="--ratio:${item.available ? Math.max(0,Math.min(1,Number(item.score)/100)) : 0}"></i></div><span>${esc(item.note || '')}</span></div><output>${item.available ? number(item.score,1) : '待补'} · ${item.weight}</output></div>`).join('')}</div>`;
  const windowControl = label => `<div class="rotation-window-control" aria-label="${esc(label)}">${WINDOWS.map(window => `<button type="button" data-rotation-window="${window}" aria-pressed="${String(window === activeWindow)}">${window} 日</button>`).join('')}</div>`;
  const pageControl = (kind, pagination, pageSize) => {
    const page = Number(pagination?.page || 1), pages = Math.max(1, Number(pagination?.pages || 1));
    const numbers = Array.from({length:pages},(_, index) => index + 1).filter(value =>
      value === 1 || value === pages || Math.abs(value - page) <= 2,
    );
    const rendered = numbers.flatMap((value,index) => {
      const gap = index && value - numbers[index - 1] > 1 ? ['<span class="rotation-page-gap" aria-hidden="true">…</span>'] : [];
      return [...gap, `<button type="button" data-rotation-page-to="${kind}:${value}" ${value === page ? 'aria-current="page"' : ''}>${value}</button>`];
    }).join('');
    return `<div class="rotation-pagination"><span>第 ${page}/${pages} 页 · ${Number(pagination?.total || 0)} 条</span><div class="rotation-pagination-actions"><button type="button" data-rotation-page-to="${kind}:1" ${page <= 1 ? 'disabled' : ''}>首页</button><button type="button" data-rotation-page-step="${kind}:-1" ${page <= 1 ? 'disabled' : ''}>上一页</button><span class="rotation-page-numbers">${rendered}</span><button type="button" data-rotation-page-step="${kind}:1" ${page >= pages ? 'disabled' : ''}>下一页</button><button type="button" data-rotation-page-to="${kind}:${pages}" ${page >= pages ? 'disabled' : ''}>末页</button><label class="rotation-page-size">每页<select data-rotation-${kind}-page-size>${[25,50,100].map(value => `<option value="${value}" ${value === pageSize ? 'selected' : ''}>${value}</option>`).join('')}</select></label></div></div>`;
  };

  function chartZoom(pointCount, {yAxisIndex = 0, initialPoints = 0, initialYEnd = 100} = {}) {
    const start = initialPoints > 0 && pointCount > initialPoints
      ? Math.max(0, 100 * (pointCount - initialPoints) / pointCount)
      : 0;
    const common = {start,end:100,filterMode:'none',realtime:true};
    const slider = {
      showDataShadow:false,showDetail:false,borderColor:AXIS,
      backgroundColor:'rgba(195,194,183,.04)',fillerColor:'rgba(150,152,144,.16)',
      handleStyle:{color:INK2,borderColor:AXIS},moveHandleStyle:{color:INK2},
    };
    return [
      {id:'zoom-x-inside',type:'inside',xAxisIndex:0,...common},
      {id:'zoom-x-slider',type:'slider',xAxisIndex:0,height:12,bottom:7,...common,...slider},
      {id:'zoom-y-inside',type:'inside',yAxisIndex,start:0,end:initialYEnd,filterMode:'none'},
      {id:'zoom-y-slider',type:'slider',orient:'vertical',yAxisIndex,width:12,right:3,top:42,bottom:56,
        start:0,end:initialYEnd,filterMode:'none',...slider},
    ];
  }

  function paddedExtent(values) {
    const finite = values.map(Number).filter(Number.isFinite);
    if (!finite.length) return {min:-1,max:1};
    let min = Math.min(0,...finite), max = Math.max(0,...finite);
    const span = max - min || Math.max(Math.abs(min),Math.abs(max),1);
    const padding = span * .08;
    min = min < 0 ? min - padding : 0;
    max = max > 0 ? max + padding : 0;
    if (min === max) return {min:min - 1,max:max + 1};
    return {min,max};
  }

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
        data:[{name:'冰点 10',yAxis:10},{name:'强势扩散 25',yAxis:25},{name:'过热 >50',yAxis:50}],
      } : undefined,
    }));
    chart.setOption(baseOpt({
      legend:{top:0,textStyle:{color:INK2,fontSize:10}},
      grid:{left:46,right:80,top:38,bottom:58}, xAxis:{...timeAxis(),splitNumber:12},
      yAxis:{type:'value',min:0,max:100,axisLabel:{color:MUTED,formatter:'{value}%'},splitLine:{lineStyle:{color:GRID}}},
      tooltip:{trigger:'axis',backgroundColor:'#1a1a19',borderColor:AXIS,textStyle:{color:'#fff',fontSize:11},valueFormatter:value => `${number(value,1)}%`},
      dataZoom:chartZoom(history.length,{initialPoints:252,initialYEnd:60}),
      series,
    }));
  }

  function recentTemperatureChart(history) {
    const chart = mkChart('rotation-temperature-recent-chart');
    if (!chart) return;
    const recent = (history || []).slice(-15);
    const values = recent.map(row => Number(row.temperature)).filter(Number.isFinite);
    const minimum = values.length ? Math.min(...values) : 0;
    const maximum = values.length ? Math.max(...values) : 100;
    const padding = Math.max(3, (maximum - minimum) * .15);
    const lower = Math.max(0, Math.floor((minimum - padding) / 5) * 5);
    const upper = Math.min(100, Math.ceil((maximum + padding) / 5) * 5);
    chart.setOption(baseOpt({
      grid:{left:30,right:8,top:18,bottom:24},
      tooltip:{trigger:'axis',backgroundColor:'#1a1a19',borderColor:AXIS,textStyle:{color:'#fff',fontSize:10},valueFormatter:value => `${number(value,1)}%`},
      xAxis:{type:'category',boundaryGap:false,data:recent.map(row => row.date),axisLine:{lineStyle:{color:AXIS}},axisTick:{show:false},axisLabel:{color:MUTED,fontSize:9,interval:index => index === 0 || index === recent.length - 1 || index % 3 === 0,formatter:value => String(value || '').slice(5)}},
      yAxis:{type:'value',min:lower,max:upper > lower ? upper : Math.min(100,lower + 5),splitNumber:3,axisLine:{show:false},axisTick:{show:false},axisLabel:{color:MUTED,fontSize:9,formatter:'{value}%'},splitLine:{lineStyle:{color:GRID}}},
      series:[{name:'市场温度',type:'line',showSymbol:true,symbol:'circle',symbolSize:5,data:recent.map(row => row.temperature),lineStyle:{color:CHART_COLORS.primary,width:1.7},itemStyle:{color:CHART_COLORS.primary},label:{show:true,position:'top',distance:4,color:INK2,fontSize:9,formatter:params => `${number(params.value,1)}%`},labelLayout:{hideOverlap:true}}],
    }));
  }

  function evidenceRadarChart(items) {
    const chart = mkChart('rotation-evidence-radar');
    if (!chart) return;
    const dimensions = [
      ['trend','趋势分布'], ['breadth','涨跌宽度'], ['volume','量能确认'],
      ['etf_capital','ETF 资金'], ['sentiment','情绪代理'],
    ];
    const indexed = new Map((items || []).map(item => [item.id,item]));
    const evidence = dimensions.map(([id,label]) => ({id,label,item:indexed.get(id)}));
    const complete = evidence.every(({item}) => item?.available && Number.isFinite(Number(item.score)));
    const values = evidence.map(({item}) => Number(item?.score));
    chart.setOption(baseOpt({
      tooltip:complete ? {trigger:'item',backgroundColor:'#1a1a19',borderColor:AXIS,textStyle:{color:'#fff',fontSize:10},formatter:params => `${params.name}<br>${evidence.map(({label},index) => `${label} ${number(params.value[index],1)}`).join('<br>')}`} : {show:false},
      radar:{center:['50%','52%'],radius:'68%',startAngle:90,splitNumber:4,indicator:evidence.map(({label}) => ({name:label,max:100})),axisName:{color:INK2,fontSize:10},axisNameGap:7,axisLine:{lineStyle:{color:AXIS}},splitLine:{lineStyle:{color:GRID}},splitArea:{show:false}},
      series:complete ? [{name:'五维证据',type:'radar',symbol:'circle',symbolSize:4,lineStyle:{color:CHART_COLORS.primary,width:1.6},itemStyle:{color:CHART_COLORS.primary},areaStyle:{color:'rgba(57,135,229,.14)'},data:[{name:'五维证据',value:values}]}] : [],
      graphic:complete ? [] : [{type:'text',left:'center',top:'middle',style:{text:'等待完整五维证据',fill:MUTED,font:'10px sans-serif'}}],
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
    const recent = (data.history || []).slice(-15);
    out.innerHTML = `
      <div class="rotation-kpis">
        <div class="rotation-kpi"><span>市场温度</span><strong class="${Number(current.temperature) > 50 ? 'up' : ''}">${percent(current.temperature)}</strong><small>趋势向上样本占比</small></div>
        <div class="rotation-kpi"><span>温度区间</span><strong class="rotation-regime" data-regime="${esc(current.regime || 'unavailable')}">${esc(current.regime_label || '—')}</strong><small>${esc(current.regime || '')}</small></div>
        <div class="rotation-kpi"><span>强势加速</span><strong>${percent(ratios.strong_up)}</strong><small>${Number(current.counts?.strong_up || 0).toLocaleString()} 只</small></div>
        <div class="rotation-kpi"><span>有效样本</span><strong>${Number(current.eligible_count || 0).toLocaleString()}</strong><small>停牌与缺失不进分母</small></div>
      </div>
      <div class="rotation-layout two rotation-temperature-layout">
        <section class="rotation-section"><div class="rotation-section-head"><div><h3>温度序列</h3><p>市场温度及 5 / 10 / 20 日均线 · 拖动底部与右侧控件缩放</p></div><output>${esc(data.as_of || '')}</output></div><div class="rotation-chart tall" id="rotation-temperature-chart"></div></section>
        <div class="rotation-temperature-aside">
          <section class="rotation-section"><div class="rotation-section-head"><div><h3>四档分布</h3><p>每只股票只归入一档，四档合计等于参与计算的股票总数</p></div></div>
            <div class="rotation-state-list">${Object.keys(STATE_LABELS).map(state => `<div class="rotation-state-row"><strong>${STATE_LABELS[state]}</strong><div class="rotation-meter"><i style="--ratio:${Math.max(0,Math.min(1,Number(ratios[state] || 0)/100))}"></i></div><output>${percent(ratios[state])} · ${Number(current.counts?.[state] || 0).toLocaleString()}</output></div>`).join('')}</div>
          </section>
          <section class="rotation-section"><div class="rotation-section-head"><div><h3>近 15 日温度路径</h3><p>最近 15 个已完成交易日</p></div></div><div class="rotation-chart rotation-temperature-recent-chart" id="rotation-temperature-recent-chart"></div></section>
        </div>
      </div>
      <section class="rotation-section"><div class="rotation-section-head"><div><h3>证据分解</h3><p>缺失维度从有效权重中剔除，不按零分处理</p></div><output>有效权重 ${data.evidence?.available_weight || 0}/100 · 综合 ${number(data.evidence?.score,1)}</output></div>
        <div class="rotation-evidence-layout"><div class="rotation-chart rotation-evidence-radar" id="rotation-evidence-radar" aria-label="市场温度五维证据雷达图"></div><div class="rotation-evidence-list">${(data.evidence?.items || []).map(item => `<div class="rotation-evidence-row" data-available="${item.available}"><strong>${esc(item.label)}</strong><div><div class="rotation-meter"><i style="--ratio:${item.available ? Math.max(0,Math.min(1,Number(item.score)/100)) : 0}"></i></div><span>${esc(item.note || '')}</span></div><output>${item.available ? number(item.score,1) : '待补'} · ${item.weight}</output></div>`).join('')}</div></div>
      </section>${issuesMarkup(meta)}`;
    temperatureChart(data.history || []);
    recentTemperatureChart(recent);
    evidenceRadarChart(data.evidence?.items || []);
  }

  const structureSpreadColor = value => {
    const parsed = Number(Array.isArray(value) ? value[value.length - 1] : value);
    if (!Number.isFinite(parsed)) return CHART_COLORS.neutral;
    if (parsed > .0025) return CHART_COLORS.up;
    if (parsed < -.0025) return CHART_COLORS.down;
    return CHART_COLORS.primary;
  };

  function structureChart(history) {
    const chart = mkChart('rotation-structure-chart');
    if (!chart) return;
    chart.setOption(baseOpt({
      legend:{top:0,textStyle:{color:INK2,fontSize:10}},
      grid:{left:52,right:18,top:38,bottom:34}, xAxis:timeAxis(),
      yAxis:{type:'value',axisLabel:{color:MUTED,formatter:value => `${(value * 100).toFixed(1)}%`},splitLine:{lineStyle:{color:GRID}}},
      series:[
        {name:'强势样本',type:'line',showSymbol:false,data:history.map(row => [row.date,row.strong_return]),lineStyle:{color:CHART_COLORS.up,width:1.7,type:'solid'}},
        {name:'低位样本',type:'line',showSymbol:false,data:history.map(row => [row.date,row.weak_return]),lineStyle:{color:CHART_COLORS.down,width:1.7,type:'dashed'}},
        {
          name:'强弱差',type:'bar',barMaxWidth:6,
          data:history.map(row => [row.date,row.spread]),
          itemStyle:{color:params => structureSpreadColor(params.value)},
          markArea:{
            silent:true,label:{show:false},
            itemStyle:{color:'rgba(79,143,216,.07)'},
            data:[[{yAxis:-.0025},{yAxis:.0025}]],
          },
        },
      ],
    }));
  }

  function structurePathChart(history) {
    const chart = mkChart('rotation-style-path-chart');
    if (!chart) return;
    const recent = (history || []).slice(-10);
    const levels = {weak_rebound:-1,balanced:0,strong_dominant:1};
    const levelLabels = {'-1':'低位修复','0':'均衡','1':'强势占优'};
    const levelStyles = {'-1':'weak','0':'balanced','1':'strong'};
    const pathColor = state => state === 'strong_dominant'
      ? CHART_COLORS.up
      : state === 'weak_rebound'
        ? CHART_COLORS.down
        : state === 'balanced' ? CHART_COLORS.primary : MUTED;
    const points = recent.map(row => {
      const confirmed = row.confirmed !== 'pending' && row.confirmed !== 'unavailable';
      const state = confirmed ? row.confirmed : row.candidate;
      const value = Number(levels[state]);
      const color = pathColor(state);
      return {
        value:Number.isFinite(value) ? value : null,
        pathRow:row,
        symbol:'circle',
        symbolSize:confirmed ? 7 : 6,
        itemStyle:{
          color:confirmed ? color : CHART_COLORS.surface,
          borderColor:color,
          borderWidth:confirmed ? 0 : 2,
        },
      };
    });
    const hasPath = points.some(point => point.value !== null);
    chart.setOption(baseOpt({
      grid:{left:62,right:8,top:8,bottom:24},
      tooltip:{
        trigger:'axis',
        axisPointer:{type:'line'},
        backgroundColor:CHART_COLORS.surface,
        borderColor:AXIS,
        textStyle:{color:CHART_COLORS.ink,fontSize:10},
        formatter:params => {
          const row = (params || []).find(item => item.data?.pathRow)?.data?.pathRow;
          if (!row) return '';
          const candidate = STYLE_LABELS[row.candidate] || '待判定';
          const confirmed = row.confirmed === 'pending'
            ? '待确认'
            : STYLE_LABELS[row.confirmed] || '样本不足';
          return `${esc(row.date || '')}<br>候选 ${esc(candidate)}<br>确认 ${esc(confirmed)}`;
        },
      },
      xAxis:{
        type:'category',
        boundaryGap:false,
        data:recent.map(row => row.date),
        axisLine:{lineStyle:{color:AXIS}},
        axisTick:{show:false},
        axisLabel:{
          color:MUTED,
          fontSize:9,
          interval:recent.length > 6 ? 2 : 0,
          formatter:value => String(value || '').slice(5),
        },
      },
      yAxis:{
        type:'value',
        min:-1,
        max:1,
        interval:1,
        axisLine:{show:false},
        axisTick:{show:false},
        axisLabel:{
          color:MUTED,
          fontSize:9,
          formatter:value => {
            const key = levelStyles[String(value)];
            return key ? `{${key}|${levelLabels[String(value)]}}` : '';
          },
          rich:{
            strong:{color:CHART_COLORS.up,fontSize:9},
            balanced:{color:CHART_COLORS.primary,fontSize:9},
            weak:{color:CHART_COLORS.down,fontSize:9},
          },
        },
        splitLine:{lineStyle:{color:GRID}},
      },
      graphic:hasPath ? [] : [{
        type:'text',
        left:'center',
        top:'middle',
        style:{text:'暂无确认路径',fill:MUTED,font:'10px sans-serif'},
      }],
      series:[{
        name:'确认路径',
        type:'line',
        step:'end',
        connectNulls:false,
        data:points,
        lineStyle:{color:MUTED,width:1.4},
        markArea:{
          silent:true,
          label:{show:false},
          data:[
            [{yAxis:-1,itemStyle:{color:'rgba(36,160,107,.055)'}},{yAxis:-.5}],
            [{yAxis:-.5,itemStyle:{color:'rgba(79,143,216,.055)'}},{yAxis:.5}],
            [{yAxis:.5,itemStyle:{color:'rgba(230,103,103,.055)'}},{yAxis:1}],
          ],
        },
        emphasis:{focus:'series'},
      }],
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
    const rawStyle = current.confirmed === 'pending' ? current.candidate : current.confirmed;
    const style = ['strong_dominant','weak_rebound','balanced'].includes(rawStyle)
      ? rawStyle
      : 'unavailable';
    const styleLabel = STYLE_LABELS[style] || '样本不足';
    const confirmation = current.confirmed === 'pending'
      ? 'pending'
      : style === 'unavailable' ? 'unavailable' : 'confirmed';
    const confirmationLabel = confirmation === 'pending'
      ? '待确认'
      : confirmation === 'confirmed' ? '已确认' : '样本不足';
    out.innerHTML = `
      <div class="rotation-kpis">
        <div class="rotation-kpi rotation-style-current-kpi" data-style="${esc(style)}" data-confirmation="${confirmation}"><span>当前结构</span><strong class="rotation-style-current-value">${esc(styleLabel)}</strong><small><b class="rotation-style-confirmation">${confirmationLabel}</b> · 候选连续 ${current.candidate_sessions || 0} 日 · 确认连续 ${current.confirmed_sessions || 0} 日</small></div>
        <div class="rotation-kpi"><span>当日强弱差</span><strong class="${tone(current.spread_1d)}">${returnPct(current.spread_1d)}</strong><small>强势中位数 − 低位中位数</small></div>
        <div class="rotation-kpi"><span>三日均值</span><strong class="${tone(current.spread_3d)}">${returnPct(current.spread_3d)}</strong><small>过滤单日跳变</small></div>
        <div class="rotation-kpi"><span>判断死区</span><strong class="rotation-style-threshold">±0.25 pp</strong><small>区间内记为均衡</small></div>
      </div>
      <div class="rotation-layout two rotation-style-layout">
        <section class="rotation-section"><div class="rotation-section-head"><div><h3>强弱样本收益</h3><p>红绿柱为强弱差，蓝色带为 ±0.25 pp 死区；折线为两组收益中位数</p></div></div><div class="rotation-chart tall" id="rotation-structure-chart"></div></section>
        <div class="rotation-structure-aside">
          <section class="rotation-section"><div class="rotation-section-head"><div><h3>当前分布</h3><p>上涨比例与收益中位数同时核查</p></div></div>
            <div class="rotation-state-list rotation-style-distribution">${(data.distribution || []).map(row => `<div class="rotation-state-row" data-state="${esc(row.state || 'unavailable')}"><strong>${esc(row.label)}</strong><span>${row.count} 只 · ${row.share == null ? '—' : percent(row.share * 100)} · 上涨 ${row.positive_ratio == null ? '—' : percent(row.positive_ratio * 100)}</span><output class="${tone(row.median_return)}">${returnPct(row.median_return)}</output></div>`).join('')}</div>
          </section>
          <section class="rotation-section"><div class="rotation-section-head"><div><h3>最近 10 日确认路径</h3><p>阶梯线为候选方向；实心点已确认，空心点待确认</p></div></div><div class="rotation-chart rotation-style-path-chart" id="rotation-style-path-chart" aria-label="最近 10 日市场风格确认路径折线图"></div></section>
        </div>
      </div>
      <div class="rotation-layout equal">
        <section class="rotation-section"><div class="rotation-section-head"><div><h3 class="rotation-style-heading" data-tone="strong">强势样本前列</h3><p>仅用于解释结构，不构成候选清单</p></div></div>${cohortTable(data.leaders || [])}</section>
        <section class="rotation-section"><div class="rotation-section-head"><div><h3 class="rotation-style-heading" data-tone="weak">低位样本前列</h3><p>按趋势分数从低到高</p></div></div>${cohortTable(data.laggards || [])}</section>
      </div>${issuesMarkup(meta)}`;
    structureChart(data.history || []);
    structurePathChart(data.history || []);
  }

  function cohortTable(items) {
    if (!items.length) return '<div class="rotation-empty"><p>当前组没有足够样本。</p></div>';
    return `<div class="rotation-table-wrap"><table class="rotation-table" style="min-width:420px"><thead><tr><th>名称</th><th>代码</th><th class="numeric">趋势</th><th class="numeric">日收益</th></tr></thead><tbody>${items.map(item => `<tr><td>${esc(item.name)}</td><td>${esc(item.symbol)}</td><td class="numeric">${number(item.trend_score,3)}</td><td class="numeric ${tone(item.return_1d)}">${returnPct(item.return_1d)}</td></tr>`).join('')}</tbody></table></div>`;
  }

  function industryPlotPoints(items) {
    return items.map(item => {
      if (item?.positive_ratio === null || item?.positive_ratio === undefined
          || item?.weak_ratio === null || item?.weak_ratio === undefined) return null;
      const current = [Number(item.positive_ratio),Number(item.weak_ratio)];
      if (!current.every(Number.isFinite)) return null;
      const currentSignal = signal(item);
      const positiveChange = Number(currentSignal.positive_change_pp);
      const weakChange = Number(currentSignal.weak_change_pp);
      const previous = [
        current[0] - (Number.isFinite(positiveChange) ? positiveChange : 0),
        current[1] - (Number.isFinite(weakChange) ? weakChange : 0),
      ];
      return {item,current,previous,currentSignal};
    }).filter(Boolean);
  }

  function scatterOption(items) {
    const points = industryPlotPoints(items);
    const values = points.flatMap(point => [...point.current,...point.previous]);
    const axisRange = (offset) => {
      const axisValues = values.filter((_,index) => index % 2 === offset).filter(Number.isFinite);
      const lowest = Math.max(0,Math.min(100,...axisValues));
      const highest = Math.max(0,Math.min(100,Math.max(...axisValues)));
      const span = Math.max(0,highest - lowest);
      const padding = Math.max(3,span * .16);
      let minimum = Math.max(0,Math.floor((lowest - padding) / 5) * 5);
      let maximum = Math.min(100,Math.ceil((highest + padding) / 5) * 5);
      if (maximum - minimum < 10) {
        const center = (lowest + highest) / 2;
        minimum = Math.max(0,Math.floor((center - 5) / 5) * 5);
        maximum = Math.min(100,Math.ceil((center + 5) / 5) * 5);
      }
      if (maximum <= minimum) maximum = Math.min(100,minimum + 10);
      if (maximum <= minimum) minimum = Math.max(0,maximum - 10);
      return {min:minimum,max:maximum};
    };
    const xRange = axisRange(0), yRange = axisRange(1);
    const labels = new Set([...points].sort((left,right) => Math.abs(Number(right.currentSignal.rotation_change_pp || 0)) - Math.abs(Number(left.currentSignal.rotation_change_pp || 0))).slice(0,8).map(point => point.item.code));
    const trails = points.map(point => ({
      coords:[point.previous,point.current],
      lineStyle:{
        color:Number(point.currentSignal.rotation_change_pp) >= 0 ? CHART_COLORS.up : CHART_COLORS.down,
        width:1.2,
        opacity:.62,
      },
    }));
    return baseOpt({
      grid:{left:50,right:48,top:24,bottom:60},
      tooltip:{trigger:'item',backgroundColor:'#1a1a19',borderColor:AXIS,textStyle:{color:'#fff',fontSize:11},formatter:params => {
        const item = params.data?.item;
        if (!item) return '';
        const currentSignal = signal(item);
        return `${esc(item.name)}<br>${activeWindow}日评分 ${number(item.rotation_score,1)} · ${esc(item.grade || '待补')}<br>${activeWindow}日变化 ${pp(currentSignal.rotation_change_pp)}<br>超额 ${returnPct(currentSignal.excess_return)}<br>上涨宽度 ${percent(Number(currentSignal.advance_ratio || 0)*100)}<br>${esc(item.stage_label)}（固定3日）`;
      }},
      xAxis:{type:'value',name:'趋势向上占比',nameLocation:'middle',nameGap:28,min:xRange.min,max:xRange.max,splitNumber:5,axisLabel:{color:MUTED,formatter:'{value}%'},nameTextStyle:{color:MUTED,fontSize:10},splitLine:{lineStyle:{color:GRID}}},
      yAxis:{type:'value',name:'低位偏弱占比',nameTextStyle:{color:MUTED,fontSize:10},min:yRange.min,max:yRange.max,splitNumber:5,axisLabel:{color:MUTED,formatter:'{value}%'},splitLine:{lineStyle:{color:GRID}}},
      series:[
        {name:'轨迹',type:'lines',coordinateSystem:'cartesian2d',silent:true,symbol:['circle','arrow'],symbolSize:[3,7],lineStyle:{width:1.2,opacity:.62},data:trails,z:2},
        {name:'行业',type:'scatter',data:points.map(point => ({value:[...point.current,Math.min(22,Math.max(8,Math.sqrt(point.item.eligible_count || 1)*2.1))],item:point.item,itemStyle:{color:Number(point.currentSignal.rotation_change_pp) > 0 ? CHART_COLORS.up : Number(point.currentSignal.rotation_change_pp) < 0 ? CHART_COLORS.down : CHART_COLORS.primary,borderColor:CHART_COLORS.surface,borderWidth:1.5,opacity:.96}})),symbolSize:value => value[2],label:{show:true,position:'top',color:INK2,fontSize:9,formatter:params => labels.has(params.data.item.code) ? params.data.item.name : ''},z:3},
      ],
      dataZoom:chartZoom(points.length),
    });
  }

  function scheduleIndustryChart(items, levelLabel) {
    cancelAnimationFrame(industryChartFrame);
    const target = document.getElementById('rotation-industry-scatter');
    if (!target) return;
    const points = industryPlotPoints(items);
    if (!points.length) {
      disposeChart('rotation-industry-scatter');
      target.innerHTML = `<div class="rotation-chart-state"><strong>暂无可绘制坐标</strong><span>${esc(levelLabel)}尚未形成有效的趋势向上与低位偏弱坐标。</span></div>`;
      return;
    }
    industryChartFrame = requestAnimationFrame(() => {
      if (!target.isConnected || target.offsetParent === null) return;
      try {
        const chart = mkChart('rotation-industry-scatter');
        if (!chart) throw new Error('图表引擎尚未就绪');
        chart.setOption(scatterOption(items),{notMerge:true});
        chart.resize();
      } catch (error) {
        disposeChart('rotation-industry-scatter');
        target.innerHTML = `<div class="rotation-chart-state" data-tone="error"><strong>周期坐标暂不可用</strong><span>${esc(error?.message || '请稍后重试')}</span></div>`;
        reportLocalError('板块联动','行业周期坐标未能绘制',error);
      }
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

  function movementSummary(summary = {}, window = activeWindow) {
    const movement = summary.movements?.[String(window)] || {};
    const longest = summary.persistence?.longest?.[0];
    return `<div class="rotation-movement-strip"><div><span>${window} 日改善</span><strong class="up">${movement.improving_count ?? 0}</strong></div><div><span>${window} 日转弱</span><strong class="down">${movement.retreating_count ?? 0}</strong></div><div><span>最快变化</span><strong>${esc(movement.leader?.name || '—')}</strong><small>${pp(movement.leader?.rotation_change_pp)}</small></div><div><span>持续最长</span><strong>${esc(longest?.name || '—')}</strong><small>${longest?.sessions || 0} 日 · ${esc(longest?.stage_label || '')}</small></div></div>`;
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
        <div class="rotation-kpi"><span>改善行业</span><strong>${industryRank.improving_count ?? 0}/${industryRank.available ?? 0}</strong><small>趋势向上变化 − 低位变化 &gt; 0</small></div>
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
      if (industrySort === 'score') return Number(right.rotation_score ?? -Infinity) - Number(left.rotation_score ?? -Infinity);
      if (industrySort === 'excess') return Number(b.excess_return ?? -Infinity) - Number(a.excess_return ?? -Infinity);
      if (industrySort === 'amount') return Number(b.amount_activity ?? -Infinity) - Number(a.amount_activity ?? -Infinity);
      if (industrySort === 'weak') return Number(b.weak_ratio ?? -Infinity) - Number(a.weak_ratio ?? -Infinity);
      return Number(b.rotation_change_pp ?? -Infinity) - Number(a.rotation_change_pp ?? -Infinity);
    });
  }

  function summarizeIndustryItems(items, window = activeWindow) {
    const available = items.map(item => ({item,current:signal(item,window)})).filter(({current}) => (
      current.rotation_change_pp !== null && current.rotation_change_pp !== undefined
      && Number.isFinite(Number(current.rotation_change_pp))
    ));
    const ranked = [...available].sort((left,right) => Number(right.current.rotation_change_pp) - Number(left.current.rotation_change_pp));
    const longest = [...items].sort((left,right) => Number(right.stage_sessions || 0) - Number(left.stage_sessions || 0))[0];
    return {
      movements:{[String(window)]:{
        improving_count:available.filter(({current}) => Number(current.rotation_change_pp) > 0).length,
        retreating_count:available.filter(({current}) => Number(current.rotation_change_pp) < 0).length,
        unchanged_count:available.filter(({current}) => Number(current.rotation_change_pp) === 0).length,
        unavailable_count:items.length - available.length,
        leader:ranked[0] ? {...ranked[0].item,rotation_change_pp:ranked[0].current.rotation_change_pp} : null,
        laggard:ranked.at(-1) ? {...ranked.at(-1).item,rotation_change_pp:ranked.at(-1).current.rotation_change_pp} : null,
      }},
      persistence:{longest:longest ? [{...longest,sessions:Number(longest.stage_sessions || 0)}] : []},
    };
  }

  function industryLevelTabs() {
    return `<div class="rotation-industry-level-tabs" role="tablist" aria-label="行业层级">${[
      ['L1','一级行业'],['L2','二级行业'],
    ].map(([level,label]) => `<button id="rotation-industry-${level.toLowerCase()}-tab" type="button" role="tab" data-rotation-industry-level="${level}" aria-selected="${String(level === activeIndustryLevel)}" aria-controls="rotation-industry-level-content" tabindex="${level === activeIndustryLevel ? 0 : -1}">${label}</button>`).join('')}</div>`;
  }

  function renderIndustries(payload) {
    const meta = payload.meta || {}, data = payload.data || {}, out = document.getElementById('rotation-industry-content');
    industryPayload = payload;
    updateMeta('industries',meta);
    const items = data.items || [];
    if (!items.length) { out.innerHTML = emptyMarkup(meta,data.message || '行业成分尚未达到计算门槛。','industries'); return; }
    if (!items.some(item => item?.signals)) { out.innerHTML = emptyMarkup(meta,'行业快照需要升级后才能展示多周期信号。','industries'); return; }
    if (!items.some(item => item?.positive_ratio != null && item?.weak_ratio != null)) { out.innerHTML = emptyMarkup(meta,'行业快照正在升级趋势向上口径。','industries'); return; }
    const levelItems = items.filter(item => item.level === activeIndustryLevel);
    const rows = sortedIndustryItems(levelItems);
    const summary = summarizeIndustryItems(levelItems,activeWindow);
    const movement = summary.movements[String(activeWindow)] || {};
    const best = movement.leader, worst = movement.laggard;
    const isL2 = activeIndustryLevel === 'L2';
    const levelLabel = isL2 ? '二级行业' : '一级行业';
    const levelDescription = isL2 ? '已关注申万二级行业' : '申万 2021 一级行业';
    const managerMarkup = isL2 ? `<section class="rotation-l2-manager" id="rotation-l2-manager" aria-labelledby="rotation-l2-manager-title" hidden><div class="rotation-l2-manager-head"><div><h3 id="rotation-l2-manager-title">管理二级行业关注区</h3><p>最多选择 30 个；这里只改变二级行业工作台，不改写一级行业汇总。</p></div><button type="button" class="rotation-link" data-rotation-l2-toggle aria-expanded="true" aria-controls="rotation-l2-manager">收起</button></div><div id="rotation-l2-options"><div class="rotation-skeleton"><span></span></div></div></section>` : '';
    const matrixMarkup = rows.length ? `<div class="rotation-table-wrap"><table class="rotation-table rotation-signal-table"><thead><tr><th>行业</th><th>阶段（3日）</th><th class="numeric">评分 / 等级</th><th class="numeric">${activeWindow}日变化</th><th class="numeric">成员收益</th><th class="numeric">超额</th><th class="numeric">上涨宽度</th><th class="numeric">量能</th><th class="numeric">趋势向上 / 低位</th><th class="numeric">覆盖</th></tr></thead><tbody>${rows.map(item => { const current = signal(item); const coordinates = item.positive_ratio == null || item.weak_ratio == null ? '—' : `${percent(item.positive_ratio)} / ${percent(item.weak_ratio)}`; return `<tr><td><button type="button" data-rotation-detail="industry" data-code="${esc(item.code)}">${esc(item.name)}</button><div class="hint">${esc(item.code)}</div></td><td><span class="rotation-stage" data-stage="${esc(item.stage)}">${esc(item.stage_label)}</span></td><td class="numeric">${number(item.rotation_score,1)} · ${esc(item.grade || '—')}</td><td class="numeric ${tone(current.rotation_change_pp)}">${pp(current.rotation_change_pp)}</td><td class="numeric ${tone(current.member_return)}">${returnPct(current.member_return)}</td><td class="numeric ${tone(current.excess_return)}">${returnPct(current.excess_return)}</td><td class="numeric">${current.advance_ratio == null ? '—' : percent(Number(current.advance_ratio)*100)}</td><td class="numeric ${tone(current.amount_activity)}">${returnPct(current.amount_activity)}</td><td class="numeric">${coordinates}</td><td class="numeric">${item.eligible_count}/${item.member_count}</td></tr>`; }).join('')}</tbody></table></div>` : `<div class="rotation-empty compact"><strong>${isL2 ? '尚未关注可计算的二级行业' : '当前没有可计算的一级行业'}</strong><p>${isL2 ? '打开管理关注区，选择需要持续观察的申万二级行业。' : '请刷新行业快照并核对数据覆盖。'}</p>${isL2 ? '<button class="rotation-refresh" type="button" data-rotation-l2-toggle aria-expanded="false" aria-controls="rotation-l2-manager">管理二级行业</button>' : ''}</div>`;
    out.innerHTML = `
      <div class="rotation-industry-toolbar">${industryLevelTabs()}<span class="rotation-industry-basis">阶段固定 3 日；评分、轨迹和变化采用当前观察窗口</span>${windowControl('行业周期观察窗口')}${isL2 ? '<button type="button" class="rotation-l2-manage-toggle" data-rotation-l2-toggle aria-expanded="false" aria-controls="rotation-l2-manager">管理关注区</button>' : ''}</div>
      ${managerMarkup}
      <div class="rotation-industry-workbench" id="rotation-industry-level-content" role="tabpanel" aria-labelledby="rotation-industry-${activeIndustryLevel.toLowerCase()}-tab">
        <section class="rotation-section rotation-industry-chart-panel"><div class="rotation-section-head"><div><h3>周期坐标与 ${activeWindow} 日轨迹</h3><p>横轴趋势向上、纵轴低位偏弱；圆点为窗口起点，箭头指向当前坐标</p></div><output>${levelItems.length} 个${levelLabel}</output></div><div class="rotation-chart rotation-industry-chart" id="rotation-industry-scatter" role="img" aria-label="${esc(levelLabel)}周期坐标与 ${activeWindow} 日轨迹"></div></section>
        <aside class="rotation-industry-summary" aria-label="${esc(levelLabel)}摘要"><div class="rotation-industry-summary-head"><div><strong>${esc(levelLabel)}摘要</strong><span>${esc(levelDescription)}</span></div><output>${levelItems.length} 个可计算</output></div><div class="rotation-industry-kpis"><div><span>有效行业</span><strong>${levelItems.length}</strong></div><div><span>覆盖门槛</span><strong>8 · 70%</strong></div><div><span>变化最快</span><strong>${esc(best?.name || '—')}</strong><small class="${tone(best?.rotation_change_pp)}">${pp(best?.rotation_change_pp)}</small></div><div><span>变化末位</span><strong>${esc(worst?.name || '—')}</strong><small class="${tone(worst?.rotation_change_pp)}">${pp(worst?.rotation_change_pp)}</small></div></div><div class="rotation-industry-movement"><div class="rotation-industry-summary-label"><strong>阶段迁移</strong><span>净变化与连续阶段</span></div>${movementSummary(summary,activeWindow)}</div></aside>
      </div>
      <section class="rotation-section rotation-industry-matrix"><div class="rotation-section-head"><div><h3>${esc(levelLabel)}信号矩阵</h3><p>评分为 75% 绝对结构与 25% 同层级相对证据；不构成交易评级</p></div><label class="rotation-compact-field">排序<select data-rotation-industry-sort><option value="change" ${industrySort === 'change' ? 'selected' : ''}>轮动变化</option><option value="score" ${industrySort === 'score' ? 'selected' : ''}>周期评分</option><option value="excess" ${industrySort === 'excess' ? 'selected' : ''}>相对收益</option><option value="amount" ${industrySort === 'amount' ? 'selected' : ''}>量能活跃</option><option value="weak" ${industrySort === 'weak' ? 'selected' : ''}>低位占比</option></select></label></div>${matrixMarkup}</section>
      <section class="rotation-detail" id="rotation-industry-detail" hidden></section>${issuesMarkup(meta)}`;
    scheduleIndustryChart(levelItems,levelLabel);
    if (isL2) loadL2Options();
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
          cache.set('preferences',saved);
          Array.from(cache.keys()).filter(key => key.startsWith('industries:')).forEach(key => cache.delete(key));
          cache.delete('overview');
          button.textContent = '已保存'; await loadCurrent(true);
        } catch (error) { button.textContent = '保存失败'; reportLocalError('板块联动','二级行业关注未能保存',error); }
        finally { setTimeout(() => { button.disabled = false; if (button.textContent !== '保存关注区') button.textContent = '保存关注区'; },1200); }
      });
    } catch (error) { target.innerHTML = errorMarkup(error); }
  }

  function themeFocusMeta(item) {
    if (item?.focus) return item.focus;
    const current = signal(item);
    const reasons = [
      ['rotation','轮动改善',Number(current.rotation_change_pp) > 0],
      ['excess','相对收益为正',Number(current.excess_return) > 0],
      ['breadth','上涨宽度过半',Number(current.advance_ratio) >= .5],
      ['amount','量能活跃',Number(current.amount_activity) > 0],
      ['grade','周期结构 A/B',['A','B'].includes(item?.grade)],
    ].filter(([, , available]) => available).map(([id,label]) => ({id,label}));
    return {evidence_count:reasons.length,evidence_total:5,reasons};
  }

  function themeFocusCard(item, index) {
    const current = signal(item), focus = themeFocusMeta(item), lead = index === 0;
    const evidenceCount = Number(focus.evidence_count || 0);
    const evidenceTotal = Number(focus.evidence_total || 5);
    const reasons = (focus.reasons || []).map(reason => esc(reason.label)).join('、')
      || '当前窗口证据偏弱，仅按相对表现进入观察';
    const priority = evidenceCount >= 4 ? '优先核查' : evidenceCount >= 3 ? '持续跟踪' : '相对观察';
    const industry = item.primary_industry
      ? `${esc(item.primary_industry.name)} · 重合 ${item.primary_industry.overlap_count || 0} 只`
      : '跨行业 / 映射证据不足';
    const representatives = (item.representatives || []).slice(0,3);
    const identity = `<div class="rotation-theme-focus-identity"><span>系统筛选</span><h4 id="rotation-theme-focus-${index}">${esc(item.name)}</h4><p>${esc(item.code)} · ${industry}</p></div>`;
    const header = `<header class="rotation-theme-focus-card-head"><span class="rotation-theme-focus-rank" aria-hidden="true">${String(index + 1).padStart(2,'0')}</span>${identity}<span class="rotation-stage" data-stage="${esc(item.stage)}">${esc(item.stage_label || '待判定')}</span></header>`;
    const score = `<div class="rotation-theme-focus-score"><strong>${number(item.rotation_score,1)}</strong><span>/ 100</span><small>${activeWindow} 日周期评分 · ${esc(item.grade || '待补')}</small></div>`;
    const evidence = `<div class="rotation-theme-focus-evidence"><span>${priority}</span><strong>${evidenceCount}/${evidenceTotal} 项证据</strong></div>`;
    const footer = `<footer class="rotation-theme-focus-card-foot"><span>${item.stage_sessions || 0} 日阶段持续 · ${item.eligible_count || 0}/${item.member_count || 0} 有效成分</span><button type="button" data-rotation-detail="theme" data-code="${esc(item.code)}">查看完整证据</button></footer>`;
    if (!lead) {
      const representative = representatives[0];
      const compactFooter = `<footer class="rotation-theme-focus-card-foot"><span>${representative ? `代表 ${esc(representative.name)} ${returnPct(representative.return_1d)}` : '暂无合格代表样本'} · ${item.eligible_count || 0}/${item.member_count || 0} 有效</span><button type="button" data-rotation-detail="theme" data-code="${esc(item.code)}">查看完整证据</button></footer>`;
      return `<article class="rotation-theme-focus-card is-compact" aria-labelledby="rotation-theme-focus-${index}">${header}<div class="rotation-theme-focus-compact-row">${score}<dl><div><dt>轮动</dt><dd class="${tone(current.rotation_change_pp)}">${pp(current.rotation_change_pp)}</dd></div><div><dt>超额</dt><dd class="${tone(current.excess_return)}">${returnPct(current.excess_return)}</dd></div><div><dt>宽度</dt><dd>${current.advance_ratio == null ? '—' : percent(Number(current.advance_ratio)*100)}</dd></div></dl>${evidence}</div><p class="rotation-theme-focus-reason"><span>入选依据</span>${reasons}</p>${compactFooter}</article>`;
    }
    const metrics = [
      ['轮动变化',pp(current.rotation_change_pp),tone(current.rotation_change_pp)],
      ['成员收益',returnPct(current.member_return),tone(current.member_return)],
      ['相对收益',returnPct(current.excess_return),tone(current.excess_return)],
      ['上涨宽度',current.advance_ratio == null ? '—' : percent(Number(current.advance_ratio)*100),''],
      ['量能活跃',returnPct(current.amount_activity),tone(current.amount_activity)],
      ['样本覆盖',percent(Number(item.coverage || 0)*100),''],
    ];
    const representativeRows = representatives.map(value => `<div class="rotation-theme-focus-representative"><strong>${esc(value.name)}</strong><span>${esc(value.symbol)}</span><output class="${tone(value.return_1d)}">趋势 ${number(value.trend_score,3)} · ${returnPct(value.return_1d)}</output></div>`).join('')
      || '<p class="hint">暂无满足流动性与历史门槛的代表样本</p>';
    const windowTrace = `<div class="rotation-theme-focus-window-trace"><div class="rotation-theme-focus-subhead"><span>多窗口轮动变化</span><small>趋势向上占比变化 − 低位偏弱占比变化</small></div><dl>${WINDOWS.map(window => { const value = signal(item,window).rotation_change_pp; return `<div><dt>${window} 日</dt><dd class="${tone(value)}">${pp(value)}</dd></div>`; }).join('')}</dl></div>`;
    return `<article class="rotation-theme-focus-card is-lead" aria-labelledby="rotation-theme-focus-${index}">${header}<div class="rotation-theme-focus-lead-reading">${score}${evidence}</div><p class="rotation-theme-focus-reason"><span>自动关注依据</span>${reasons}</p><dl class="rotation-theme-focus-metrics">${metrics.map(([label,value,valueTone]) => `<div><dt>${label}</dt><dd class="${valueTone}">${value}</dd></div>`).join('')}</dl>${windowTrace}<div class="rotation-theme-focus-representatives"><div class="rotation-theme-focus-subhead"><span>代表样本</span><small>趋势分 / 当日涨跌</small></div>${representativeRows}</div>${footer}</article>`;
  }

  function renderThemes(payload) {
    const meta = payload.meta || {}, data = payload.data || {}, out = document.getElementById('rotation-themes-content');
    updateMeta('themes',meta); themeCatalog = data.items || []; themePagination = data.pagination || themePagination;
    themeFocus = (data.focus_items || themeCatalog.slice(0,4)).slice(0,4);
    themeFocusDefinition = data.focus_definition || {};
    const summary = data.summary || {};
    const total = Number(summary.group_count || themePagination.total || themeCatalog.length);
    if (!total && !themeFocus.length) { out.innerHTML = emptyMarkup(meta,data.message || '尚未建立细分题材成分目录。','themes'); return; }
    const sample = themeFocus[0] || themeCatalog[0];
    if (!sample?.signals) { out.innerHTML = emptyMarkup(meta,'题材快照需要升级后才能展示多周期信号。','themes'); return; }
    const criteria = (themeFocusDefinition.criteria || []).map(item => item.label).filter(Boolean).join('、')
      || '轮动改善、正超额、上涨宽度、量能与周期结构';
    out.innerHTML = `<div class="rotation-commandbar"><div><strong>自动关注口径</strong><span>${activeWindow} 日窗口按 ${esc(criteria)} 排序；只表达核查优先级，不构成交易评级</span></div>${windowControl('题材观察窗口')}</div><section class="rotation-theme-focus" aria-labelledby="rotation-theme-focus-heading"><div class="rotation-theme-focus-head"><div><span>FOCUS QUEUE</span><h3 id="rotation-theme-focus-heading">重点关注题材</h3><p>先看证据更完整的题材，再进入全量目录交叉核查。</p></div><output>${themeFocus.length} / ${total} 个</output></div><div class="rotation-theme-focus-board">${themeFocus.map(themeFocusCard).join('')}</div><div class="rotation-theme-market-context"><span>全量题材背景</span>${movementSummary(summary,activeWindow)}</div></section><section class="rotation-detail" id="rotation-theme-detail" hidden></section><section class="rotation-theme-catalog" aria-labelledby="rotation-theme-catalog-heading"><div class="rotation-theme-catalog-head"><div><span>FULL CATALOG</span><h3 id="rotation-theme-catalog-heading">搜索与完整列表</h3><p>按名称、代码或关联行业定位；筛选只作用于下方目录。</p></div><output>${themePagination.total || 0} / ${total} 条</output></div><div class="rotation-filterbar rotation-theme-filterbar"><label>搜索题材<input data-rotation-theme-query type="search" value="${esc(themeQuery)}" placeholder="输入题材名称、代码或关联行业"></label><label>阶段<select data-rotation-theme-stage><option value="">全部阶段</option>${Object.entries(STAGE_LABELS).map(([value,label]) => `<option value="${esc(value)}" ${themeStage === value ? 'selected' : ''}>${esc(label)}</option>`).join('')}</select></label><label>周期评分<select data-rotation-theme-grade><option value="">全部等级</option>${['A','B','C','D'].map(value => `<option value="${value}" ${themeGrade === value ? 'selected' : ''}>${value}</option>`).join('')}</select></label><label>排序<select data-rotation-theme-sort><option value="change" ${themeSort === 'change' ? 'selected' : ''}>轮动变化</option><option value="score" ${themeSort === 'score' ? 'selected' : ''}>周期评分</option><option value="excess" ${themeSort === 'excess' ? 'selected' : ''}>相对收益</option><option value="amount" ${themeSort === 'amount' ? 'selected' : ''}>量能活跃</option><option value="coverage" ${themeSort === 'coverage' ? 'selected' : ''}>样本覆盖</option></select></label></div><div id="rotation-theme-results"></div></section>${issuesMarkup(meta)}`;
    drawThemeTable();
  }

  function drawThemeTable() {
    const target = document.getElementById('rotation-theme-results'); if (!target) return;
    target.innerHTML = themeCatalog.length ? `<div class="rotation-table-wrap"><table class="rotation-table rotation-signal-table"><thead><tr><th>题材</th><th>关联一级行业</th><th>阶段（3日）</th><th class="numeric">评分 / 等级</th><th class="numeric">持续</th><th class="numeric">${activeWindow}日变化</th><th class="numeric">成员收益</th><th class="numeric">超额</th><th class="numeric">上涨宽度</th><th class="numeric">量能</th><th class="numeric">覆盖</th></tr></thead><tbody>${themeCatalog.map(item => { const current = signal(item); return `<tr><td><button type="button" data-rotation-detail="theme" data-code="${esc(item.code)}">${esc(item.name)}</button><div class="hint">${esc(item.code)}</div></td><td>${item.primary_industry ? `${esc(item.primary_industry.name)}<div class="hint">${percent(Number(item.primary_industry.theme_share || 0)*100)} · ${item.primary_industry.overlap_count} 只</div>` : '<span class="hint">跨行业 / 证据不足</span>'}</td><td><span class="rotation-stage" data-stage="${esc(item.stage)}">${esc(item.stage_label)}</span></td><td class="numeric">${number(item.rotation_score,1)} · ${esc(item.grade || '—')}</td><td class="numeric">${item.stage_sessions || 0} 日</td><td class="numeric ${tone(current.rotation_change_pp)}">${pp(current.rotation_change_pp)}</td><td class="numeric ${tone(current.member_return)}">${returnPct(current.member_return)}</td><td class="numeric ${tone(current.excess_return)}">${returnPct(current.excess_return)}</td><td class="numeric">${current.advance_ratio == null ? '—' : percent(Number(current.advance_ratio)*100)}</td><td class="numeric ${tone(current.amount_activity)}">${returnPct(current.amount_activity)}</td><td class="numeric">${item.eligible_count}/${item.member_count}</td></tr>`; }).join('')}</tbody></table></div>${pageControl('theme',themePagination,themePageSize)}` : '<div class="rotation-empty"><strong>没有匹配题材</strong><p>可缩短关键词或清除阶段、评分筛选。</p></div>';
  }

  function renderEtf(payload) {
    const meta = payload.meta || {}, data = payload.data || {}, out = document.getElementById('rotation-etf-content');
    updateMeta('etf_flows',meta); etfCatalog = data.items || []; etfPagination = data.pagination || etfPagination;
    const research = data.research || {}, researchItems = research.items || [], researchMeta = research.meta || {};
    if (!data.summary && !etfCatalog.length && !researchItems.length) { out.innerHTML = emptyMarkup(meta,data.message || '等待 ETF 研究与份额快照。','etf'); return; }
    const currentWindow = data.summary?.windows?.[String(activeWindow)] || {};
    const categories = [...new Set([...(research.categories || []),...(data.categories || [])])];
    const streaks = data.summary?.streaks || {};
    const researchRows = researchItems.map(item => { const m=item.metrics||{}, minute=item.minute_evidence||{}; return `<tr><td>${esc(item.name)}<div class="hint">${esc(item.symbol)}</div></td><td>${esc(item.category)}</td><td class="numeric">${item.category_rank == null ? '—' : `${item.category_rank} · ${number(item.score,1)}`}</td><td class="numeric ${tone(m.return_20d)}">${returnPct(m.return_20d)}</td><td class="numeric">${money(m.avg_amount_20d)}</td><td>${item.share_lag_sessions === 0 ? '当日直连' : item.share_lag_sessions === 1 ? '<span class="rotation-stage">滞后 1 日</span>' : '份额缺失'}</td><td>${minute.complete_session ? `完整 · ${number(minute.vwap_deviation,4)}` : `${minute.rows||0} 条 · 不参与评分`}</td></tr>`; }).join('');
    const researchSection = `<section class="rotation-section"><div class="rotation-section-head"><div><h3>全场分类研究</h3><p>各类资产只在同类别内排名；stockdb 与直连接口共享 Tushare 上游，不视为独立交叉验证</p></div><output>${research.pagination?.total || researchItems.length} 只 · ${esc(researchMeta.as_of || '待扫描')}</output></div>${researchRows ? `<div class="rotation-table-wrap"><table class="rotation-table"><thead><tr><th>ETF</th><th>类别</th><th class="numeric">类别排名 · 分数</th><th class="numeric">20 日收益</th><th class="numeric">日均成交额</th><th>份额时点</th><th>分钟证据 / VWAP偏离</th></tr></thead><tbody>${researchRows}</tbody></table></div>` : '<div class="rotation-empty"><strong>尚无全场 ETF 快照</strong><p>运行 ETF 扫描后显示全部可交易 ETF；原有资金流仍在下方保留。</p></div>'}</section>`;
    out.innerHTML = `<div class="rotation-commandbar"><div><strong>ETF 资金窗口</strong><span>累计资金按最近完整交易日求和；跟踪基准缺失时不猜测</span></div>${windowControl('ETF资金观察窗口')}</div><div class="rotation-kpis"><div class="rotation-kpi"><span>${activeWindow} 日净流</span><strong class="${tone(currentWindow.net_flow)}">${money(currentWindow.net_flow)}</strong><small>${currentWindow.sessions || 0} 个交易日</small></div><div class="rotation-kpi"><span>最大净申购</span><strong>${esc(currentWindow.largest_inflow?.name || '—')}</strong><small class="up">${money(currentWindow.largest_inflow?.flow)}</small></div><div class="rotation-kpi"><span>最大净赎回</span><strong>${esc(currentWindow.largest_outflow?.name || '—')}</strong><small class="down">${money(currentWindow.largest_outflow?.flow)}</small></div><div class="rotation-kpi"><span>连续申购 / 赎回</span><strong>${esc(streaks.longest_inflow?.name || '—')} / ${esc(streaks.longest_outflow?.name || '—')}</strong><small>${streaks.longest_inflow?.sessions || 0} / ${streaks.longest_outflow?.sessions || 0} 个观测日</small></div></div><section class="rotation-section"><div class="rotation-section-head"><div><h3>每日与累计资金</h3><p>柱为当日净流，折线为当前本地历史累计 · 支持横纵坐标缩放</p></div><output>${data.daily?.length || 0} 日</output></div><div class="rotation-chart tall" id="rotation-etf-chart"></div></section><section class="rotation-section"><div class="rotation-section-head"><div><h3>跟踪基准聚合</h3><p>同一基准下的 ETF 合并，避免同质产品重复放大观感</p></div><output>${data.benchmarks?.length || 0} 个基准</output></div><div class="rotation-benchmark-grid">${[...(data.benchmarks || [])].sort((a,b) => Math.abs(Number(b.flows?.[String(activeWindow)] || 0)) - Math.abs(Number(a.flows?.[String(activeWindow)] || 0))).slice(0,20).map(item => `<div><strong>${esc(item.benchmark)}</strong><span>${item.fund_count} 只 · ${esc(item.category)}</span><output class="${tone(item.flows?.[String(activeWindow)])}">${money(item.flows?.[String(activeWindow)])}</output></div>`).join('')}</div></section><section class="rotation-section"><div class="rotation-section-head"><div><h3>ETF 贡献明细</h3><p>完整目录默认 50 行分页，逐只披露价格来源与连续申赎</p></div></div><div class="rotation-filterbar"><label>搜索<input data-rotation-etf-query type="search" value="${esc(etfQuery)}" placeholder="ETF 名称、代码或基准"></label><label>类别<select data-rotation-etf-category><option value="">全部类别</option>${categories.map(value => `<option value="${esc(value)}" ${etfCategory === value ? 'selected' : ''}>${esc(value)}</option>`).join('')}</select></label><label>排序<select data-rotation-etf-sort><option value="flow" ${etfSort === 'flow' ? 'selected' : ''}>窗口资金</option><option value="daily" ${etfSort === 'daily' ? 'selected' : ''}>当日资金</option><option value="streak" ${etfSort === 'streak' ? 'selected' : ''}>连续申赎</option><option value="name" ${etfSort === 'name' ? 'selected' : ''}>名称</option></select></label></div><div id="rotation-etf-results"></div></section>${issuesMarkup(meta)}`;
    out.innerHTML = researchSection + out.innerHTML;
    drawEtfTable();
    const chart = mkChart('rotation-etf-chart');
    const daily = data.daily || [];
    const flowExtent = paddedExtent(daily.map(row => row.flow));
    const cumulativeExtent = paddedExtent(daily.flatMap(row => [row.cumulative,row.cumulative_ma5,row.cumulative_ma20]));
    if (chart) chart.setOption(baseOpt({
      legend:{top:0,textStyle:{color:INK2,fontSize:10}},grid:{left:70,right:92,top:38,bottom:58},xAxis:timeAxis(),
      yAxis:[
        {type:'value',scale:true,min:flowExtent.min,max:flowExtent.max,axisLabel:{color:MUTED,formatter:value => money(value)},splitLine:{lineStyle:{color:GRID}}},
        {type:'value',scale:true,min:cumulativeExtent.min,max:cumulativeExtent.max,axisLabel:{color:MUTED,formatter:value => money(value)},splitLine:{show:false}},
      ],
      dataZoom:chartZoom(daily.length,{yAxisIndex:[0,1],initialPoints:260}),
      series:[
        {name:'当日净流',type:'bar',barMaxWidth:8,data:daily.map(row => [row.date,row.flow]),itemStyle:{color:params => Number(params.value[1]) >= 0 ? CHART_COLORS.up : CHART_COLORS.down}},
        {name:'累计净流',type:'line',yAxisIndex:1,showSymbol:false,data:daily.map(row => [row.date,row.cumulative]),lineStyle:{color:CHART_COLORS.primary,width:1.7}},
        {name:'累计 MA5',type:'line',yAxisIndex:1,showSymbol:false,connectNulls:false,data:daily.map(row => [row.date,row.cumulative_ma5]),lineStyle:{color:CHART_COLORS.warning,width:1.2}},
        {name:'累计 MA20',type:'line',yAxisIndex:1,showSymbol:false,connectNulls:false,data:daily.map(row => [row.date,row.cumulative_ma20]),lineStyle:{color:CHART_COLORS.compare,width:1.2}},
      ],
    }));
  }

  function drawEtfTable() {
    const target = document.getElementById('rotation-etf-results'); if (!target) return;
    target.innerHTML = etfCatalog.length ? `<div class="rotation-table-wrap"><table class="rotation-table"><thead><tr><th>ETF</th><th>跟踪基准</th><th>类别</th><th class="numeric">${activeWindow}日资金</th><th class="numeric">当日资金</th><th class="numeric">连续</th><th class="numeric">份额变化</th><th class="numeric">估算价格</th><th>价格来源</th></tr></thead><tbody>${etfCatalog.map(item => `<tr><td>${esc(item.name)}<div class="hint">${esc(item.symbol)}</div></td><td>${esc(item.benchmark || '未披露')}</td><td>${esc(item.category)}</td><td class="numeric ${tone(item.flows?.[String(activeWindow)])}">${money(item.flows?.[String(activeWindow)])}</td><td class="numeric ${tone(item.flow)}">${money(item.flow)}</td><td class="numeric ${tone(item.flow_streak_sessions)}">${Number(item.flow_streak_sessions || 0) > 0 ? '+' : ''}${item.flow_streak_sessions || 0} 日</td><td class="numeric ${tone(item.share_change)}">${number(item.share_change,0)}</td><td class="numeric">${number(item.price,4)}</td><td>${item.price_source === 'nav' ? '净值' : '<span class="rotation-stage">收盘价降级</span>'}</td></tr>`).join('')}</tbody></table></div>${pageControl('etf',etfPagination,etfPageSize)}` : '<div class="rotation-empty"><strong>没有匹配 ETF</strong><p>可缩短关键词或清除类别筛选。</p></div>';
  }

  async function openGroupDetail(kind, code) {
    const isTheme = kind === 'theme';
    const target = document.getElementById(isTheme ? 'rotation-theme-detail' : 'rotation-industry-detail');
    if (!target) return;
    target.hidden = false;
    target.innerHTML = '<div class="rotation-skeleton"><span></span><span></span></div>';
    target.scrollIntoView({behavior:'auto',block:'nearest'});
    try {
      const payload = await api(`/api/v1/rotation/${isTheme ? 'themes' : 'industries'}/${encodeURIComponent(code)}?window=${activeWindow}`);
      const item = payload.data || {};
      const score = groupScore(item);
      const industryContext = isTheme ? (item.primary_industry
        ? ` · 主行业 ${esc(item.primary_industry.name)} ${percent(Number(item.primary_industry.theme_share || 0)*100)}`
        : ' · 跨行业或映射证据不足') : '';
      target.innerHTML = `<div class="rotation-detail-head"><div><h3>${esc(item.name)} <span class="rotation-stage" data-stage="${esc(item.stage)}">${esc(item.stage_label)}</span></h3><p>${esc(item.code)} · ${item.eligible_count}/${item.member_count} 有效成分 · 覆盖 ${percent(Number(item.coverage || 0)*100)}${industryContext}</p><p>${activeWindow} 日周期评分 ${number(score.score,1)} · ${esc(score.grade || '待补')} · 有效权重 ${score.available_weight ?? 0}/100</p></div><button type="button" class="rotation-link" data-close-rotation-detail>关闭详情</button></div><div class="rotation-detail-signals">${WINDOWS.map(window => { const current = signal(item,window); return `<div><span>${window} 日变化</span><strong class="${tone(current.rotation_change_pp)}">${pp(current.rotation_change_pp)}</strong><small>超额 ${returnPct(current.excess_return)} · 宽度 ${current.advance_ratio == null ? '—' : percent(Number(current.advance_ratio)*100)} · 量能 ${returnPct(current.amount_activity)}</small></div>`; }).join('')}</div><section class="rotation-section"><div class="rotation-section-head"><div><h3>评分证据</h3><p>缺失维度退出有效权重；同层级百分位只承担 25%</p></div></div>${scoreEvidenceMarkup(score)}</section><div class="rotation-representatives">${(item.representatives || []).map(value => `<div class="rotation-representative"><strong>${esc(value.name)}</strong><span>${esc(value.symbol)}</span><span class="${tone(value.return_1d)}">趋势 ${number(value.trend_score,3)} · ${returnPct(value.return_1d)}</span></div>`).join('') || '<span class="hint">暂无满足流动性与历史门槛的代表样本</span>'}</div><div class="rotation-chart compact" id="rotation-detail-chart"></div>`;
      const chart = mkChart('rotation-detail-chart');
      if (chart) chart.setOption(baseOpt({
        legend:{top:0,textStyle:{color:INK2,fontSize:10}},grid:{left:48,right:18,top:36,bottom:30},xAxis:timeAxis(),yAxis:{type:'value',min:0,max:100,axisLabel:{color:MUTED,formatter:'{value}%'},splitLine:{lineStyle:{color:GRID}}},series:[{name:'趋势向上',type:'line',showSymbol:false,data:(item.history || []).map(row => [row.date,row.positive_ratio]),lineStyle:{color:CHART_COLORS.up,width:1.5}},{name:'低位偏弱',type:'line',showSymbol:false,data:(item.history || []).map(row => [row.date,row.weak_ratio]),lineStyle:{color:CHART_COLORS.down,width:1.5}}],
      }));
    } catch (error) { target.innerHTML = errorMarkup(error); }
  }

  async function loadCurrent(force = false) {
    const thisRequest = ++requestVersion;
    const marketPage = activeMarketPage;
    const rotationPage = activeRotationPage;
    const marketActive = document.getElementById('tab-market')?.classList.contains('active');
    const rotationActive = document.getElementById('tab-rotation')?.classList.contains('active');
    const stillCurrent = () => (
      thisRequest === requestVersion && ((marketActive && activeMarketPage === marketPage && document.getElementById('tab-market')?.classList.contains('active'))
      || (rotationActive && activeRotationPage === rotationPage && document.getElementById('tab-rotation')?.classList.contains('active')))
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
        payload = await fetchView(`industries:${activeWindow}`,`/api/v1/rotation/industries?window=${activeWindow}`,force);
        if (stillCurrent()) renderIndustries(payload);
      } else if (rotationActive && rotationPage === 'themes') {
        const params = new URLSearchParams({page:String(themePage),page_size:String(themePageSize),window:String(activeWindow),sort:themeSort,order:themeSort === 'name' ? 'asc' : 'desc'});
        if (themeQuery.trim()) params.set('query',themeQuery.trim());
        if (themeStage) params.set('stage',themeStage);
        if (themeGrade) params.set('grade',themeGrade);
        payload = await fetchView(`themes:${params.toString()}`,`/api/v1/rotation/themes?${params}`,force);
        themePage = Number(payload.data?.pagination?.page || themePage);
        if (stillCurrent()) renderThemes(payload);
      } else if (rotationActive && rotationPage === 'etf-flows') {
        const params = new URLSearchParams({page:String(etfPage),page_size:String(etfPageSize),window:String(activeWindow),sort:etfSort,order:etfSort === 'name' ? 'asc' : 'desc'});
        if (etfQuery.trim()) params.set('query',etfQuery.trim());
        if (etfCategory) params.set('category',etfCategory);
        const researchParams = new URLSearchParams({page:String(etfPage),page_size:String(etfPageSize),sort:'rank',order:'asc'});
        if (etfQuery.trim()) researchParams.set('query',etfQuery.trim());
        if (etfCategory) researchParams.set('category',etfCategory);
        const [summary, items, research] = await Promise.all([
          fetchView('etf-summary','/api/v1/rotation/etf-flows?include_items=false',force),
          fetchView(`etf-items:${params.toString()}`,`/api/v1/rotation/etf-flows/items?${params}`,force),
          fetchView(`etf-research:${researchParams.toString()}`,`/api/v1/rotation/etfs?${researchParams}`,force),
        ]);
        etfPage = Number(items.data?.pagination?.page || etfPage);
        payload = {meta:summary.meta,data:{...summary.data,...items.data,research:{...research.data,meta:research.meta}}};
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
    const route = `#observe/${page}`;
    if (updateHash && location.hash !== route) history.replaceState(null,'',route);
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
    const routePage = {overview:'rotation',industry:'industry',themes:'themes','etf-flows':'etf-flows'}[page] || 'rotation';
    const route = `#observe/${routePage}`;
    if (updateHash && location.hash !== route) history.replaceState(null,'',route);
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
    const canonical = location.hash.match(/^#observe\/([a-z-]+)$/);
    const legacy = location.hash.match(/^#(market|rotation)\/([a-z-]+)$/);
    const route = canonical ? canonical[1] : legacy ? legacy[2] : '';
    const parent = canonical
      ? ({quotes:'market',temperature:'market',style:'market',rotation:'rotation',industry:'rotation',themes:'rotation','etf-flows':'rotation'}[route] || '')
      : legacy?.[1];
    if (!parent) return false;
    const workspacePage = parent === 'market'
      ? route
      : ({overview:'rotation',industry:'industry',themes:'themes','etf-flows':'etf-flows',radar:'rotation'}[route] || 'rotation');
    const control = document.querySelector(`header [data-workspace-page="${workspacePage}"][data-tab="${parent}"]`);
    if (control) activateTab(control,{persist:true,load:false,route:false});
    if (parent === 'market') setMarketPage(route,false);
    else {
      setRotationPage(route === 'radar' ? 'overview' : route,false);
    }
    if (legacy) {
      const canonicalRoute = parent === 'market'
        ? `#observe/${route}`
        : `#observe/${route === 'radar' ? 'rotation' : route}`;
      history.replaceState(null,'',canonicalRoute);
    }
    return true;
  }

  async function jumpToGroup(kind, code) {
    const page = kind === 'theme' ? 'themes' : 'industry';
    setRotationPage(page);
    await loadCurrent();
    openGroupDetail(kind,code);
  }

  async function scanEtfResearch(button) {
    const original = button.textContent;
    button.disabled = true;
    button.textContent = '正在提交…';
    try {
      let job = await api('/api/v1/rotation/etfs/scan',{method:'POST',headers:{'Content-Type':'application/json'},body:'{}'});
      while (['queued','running','cancelling','interrupted'].includes(job.status)) {
        button.textContent = `${Math.round(Number(job.progress || 0))}% ${job.phase || '读取本地库'}`;
        await new Promise(resolve => setTimeout(resolve,800));
        job = await api(`/api/v1/rotation/etfs/jobs/${encodeURIComponent(job.id)}`);
      }
      if (!['completed','completed_with_warnings'].includes(job.status)) throw new Error(job.error || job.message || 'ETF 扫描失败');
      Array.from(cache.keys()).filter(key => key.startsWith('etf-research:')).forEach(key => cache.delete(key));
      button.textContent = '扫描完成';
      await loadCurrent(true);
    } catch (error) {
      button.textContent = '扫描失败';
      reportLocalError('ETF 研究','全场 ETF 扫描未完成',error);
    } finally {
      setTimeout(() => { button.disabled = false; button.textContent = original; },1200);
    }
  }

  document.addEventListener('click', event => {
    const market = event.target.closest('[data-market-page]');
    if (market) { setMarketPage(market.dataset.marketPage); return; }
    const rotation = event.target.closest('[data-rotation-page]');
    if (rotation) { setRotationPage(rotation.dataset.rotationPage); return; }
    const industryLevel = event.target.closest('[data-rotation-industry-level]');
    if (industryLevel) {
      const selected = industryLevel.dataset.rotationIndustryLevel;
      if (['L1','L2'].includes(selected) && selected !== activeIndustryLevel) {
        activeIndustryLevel = selected;
        if (industryPayload) renderIndustries(industryPayload);
        else loadCurrent();
      }
      return;
    }
    const l2Toggle = event.target.closest('[data-rotation-l2-toggle]');
    if (l2Toggle) {
      const manager = document.getElementById('rotation-l2-manager');
      if (!manager) return;
      const opening = manager.hidden;
      manager.hidden = !opening;
      document.querySelectorAll('[data-rotation-l2-toggle]').forEach(button => button.setAttribute('aria-expanded',String(opening)));
      if (opening && l2Toggle.closest('.rotation-empty')) requestAnimationFrame(() => manager.scrollIntoView({behavior:'auto',block:'nearest'}));
      return;
    }
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
      if (kind === 'theme') { themePage = Math.max(1,themePage + Number(step)); loadCurrent(); }
      if (kind === 'etf') { etfPage = Math.max(1,etfPage + Number(step)); loadCurrent(); }
      return;
    }
    const pageTo = event.target.closest('[data-rotation-page-to]');
    if (pageTo) {
      const [kind,page] = pageTo.dataset.rotationPageTo.split(':');
      if (kind === 'theme') { themePage = Math.max(1,Number(page) || 1); loadCurrent(); }
      if (kind === 'etf') { etfPage = Math.max(1,Number(page) || 1); loadCurrent(); }
      return;
    }
    const jump = event.target.closest('[data-rotation-jump]');
    if (jump) { jumpToGroup(jump.dataset.rotationJump,jump.dataset.code); return; }
    const refreshButton = event.target.closest('[data-rotation-refresh]');
    if (refreshButton) { refresh(refreshButton.dataset.rotationRefresh,refreshButton); return; }
    const etfScan = event.target.closest('[data-etf-research-scan]');
    if (etfScan) { scanEtfResearch(etfScan); return; }
    const detail = event.target.closest('[data-rotation-detail]');
    if (detail) { openGroupDetail(detail.dataset.rotationDetail,detail.dataset.code); return; }
    const close = event.target.closest('[data-close-rotation-detail]');
    if (close) close.closest('.rotation-detail').hidden = true;
  });

  document.addEventListener('keydown', event => {
    const industryTab = event.target.closest('.rotation-industry-level-tabs [role="tab"]');
    if (industryTab && ['ArrowLeft','ArrowRight','Home','End'].includes(event.key)) {
      const tabs = [...industryTab.closest('[role="tablist"]').querySelectorAll('[role="tab"]')];
      const index = tabs.indexOf(industryTab);
      const nextIndex = event.key === 'Home' ? 0 : event.key === 'End' ? tabs.length - 1
        : (index + (event.key === 'ArrowRight' ? 1 : -1) + tabs.length) % tabs.length;
      event.preventDefault();
      tabs[nextIndex].focus();
      tabs[nextIndex].click();
      return;
    }
    const current = event.target.closest('.workspace-context [role="tab"]');
    if (!current || !['ArrowLeft','ArrowRight','Home','End'].includes(event.key)) return;
    const tabs = [...current.closest('[role="tablist"]').querySelectorAll('[role="tab"]')];
    const index = tabs.indexOf(current);
    const nextIndex = event.key === 'Home' ? 0 : event.key === 'End' ? tabs.length - 1
      : (index + (event.key === 'ArrowRight' ? 1 : -1) + tabs.length) % tabs.length;
    const next = tabs[nextIndex];
    event.preventDefault();
    next.focus();
    next.click();
  });

  document.addEventListener('input', event => {
    if (event.target.matches('[data-rotation-theme-query]')) {
      themeQuery = event.target.value; themePage = 1;
      clearTimeout(searchTimer); searchTimer = setTimeout(() => loadCurrent(),220);
    }
    if (event.target.matches('[data-rotation-etf-query]')) {
      etfQuery = event.target.value; etfPage = 1;
      clearTimeout(searchTimer); searchTimer = setTimeout(() => loadCurrent(),220);
    }
  });

  document.addEventListener('change', event => {
    if (event.target.matches('[data-rotation-industry-sort]')) {
      industrySort = event.target.value; loadCurrent();
    } else if (event.target.matches('[data-rotation-theme-stage]')) {
      themeStage = event.target.value; themePage = 1; loadCurrent();
    } else if (event.target.matches('[data-rotation-theme-grade]')) {
      themeGrade = event.target.value; themePage = 1; loadCurrent();
    } else if (event.target.matches('[data-rotation-theme-sort]')) {
      themeSort = event.target.value; themePage = 1; loadCurrent();
    } else if (event.target.matches('[data-rotation-theme-page-size]')) {
      themePageSize = Number(event.target.value) || 50; themePage = 1; loadCurrent();
    } else if (event.target.matches('[data-rotation-etf-category]')) {
      etfCategory = event.target.value; etfPage = 1; loadCurrent();
    } else if (event.target.matches('[data-rotation-etf-sort]')) {
      etfSort = event.target.value; etfPage = 1; loadCurrent();
    } else if (event.target.matches('[data-rotation-etf-page-size]')) {
      etfPageSize = Number(event.target.value) || 50; etfPage = 1; loadCurrent();
    }
  });

  document.querySelector('header')?.addEventListener('click', event => {
    const control = event.target.closest('[data-tab]');
    if (control?.dataset.tab === 'market' && !control.dataset.marketPage) setMarketPage(activeMarketPage);
    if (control?.dataset.tab === 'rotation' && !control.dataset.rotationPage) setRotationPage(activeRotationPage);
  });
  window.addEventListener('hashchange',applyHash);

  window.loadRotationFeature = tab => {
    if (tab === 'market') {
      const page = location.hash.startsWith('#observe/') ? location.hash.slice(9) : activeMarketPage;
      setMarketPage(page,false);
    } else if (tab === 'rotation') {
      const page = location.hash.startsWith('#observe/') ? location.hash.slice(9) : activeRotationPage;
      setRotationPage(page,false);
    }
  };

  if (!applyHash()) setMarketPage('quotes',false);
  recoverActiveJob();
})();
