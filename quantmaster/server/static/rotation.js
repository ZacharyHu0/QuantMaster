const rotationFeature = (() => {
  'use strict';

  const TODAY_ROUTE_PREFIX = '#today/';
  const STATE_LABELS = {
    strong_up:'强势加速', up:'趋势延续', range:'中位整理', weak:'低位偏弱',
  };
  const ETF_STATE_LABELS = {
    leading:'领涨共振', low_turn:'低位转强', improving:'趋势改善',
    weakening:'走弱', watch:'震荡观察', not_applicable:'位置不适用',
  };
  const ETF_STATE_SHAPES = {
    leading:'▲', low_turn:'◆', improving:'↗', weakening:'▼', watch:'●', not_applicable:'—', risk:'!',
  };
  const ETF_CANDIDATE_LABELS = {
    momentum_hot:'动量热门候选', stage_low_rebound:'阶段低位候选',
    stage_high_activity:'阶段高位活跃候选',
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
  const ETF_AUTO_INPUT_KEY = 'quantmaster.etf.auto-input.v2';
  const WINDOW_KEY = 'quantmaster.rotation.window.v2';
  const TEMPERATURE_WINDOW_KEY = 'quantmaster.market.temperature-window.v1';
  const WINDOWS = [1,3,5,20];
  let themeCatalog = [];
  let themeFocus = [];
  let themeFocusDefinition = {};
  let etfProductCatalog = [];
  let etfSectorCatalog = [];
  let etfOverview = {};
  let selectedEtfSnapshotId = '';
  let displayedEtfSnapshotId = '';
  let selectedEtfTier = 'production';
  let displayedEtfTier = 'production';
  let etfLastJob = null;
  let etfDrawerReturnFocus = null;
  let activeWindow = 5;
  let temperatureWindow = 5;
  let themePage = 1;
  let etfProductPage = 1;
  let etfSectorPage = 1;
  let themePageSize = 50;
  let etfProductPageSize = 50;
  let etfSectorPageSize = 25;
  let themePagination = {page:1,page_size:50,total:0,pages:1,has_previous:false,has_next:false};
  let etfProductPagination = {page:1,page_size:50,total:0,pages:1,has_previous:false,has_next:false};
  let activeIndustryLevel = 'L1';
  let industryPayload = null;
  let industryChartFrame = 0;
  let industrySort = 'change';
  let themeSort = 'change';
  let etfSort = 'trend';
  let themeQuery = '';
  let themeStage = '';
  let themeGrade = '';
  let etfQuery = '';
  let etfCategory = '';
  let etfAsset = 'equity';
  let pendingEtfAssetFocus = '';
  let etfState = '';
  let requestVersion = 0;
  let searchTimer = 0;

  document.addEventListener('pointerdown', event=>{
    if (pendingEtfAssetFocus && !event.target.closest?.('[data-etf-asset]')) pendingEtfAssetFocus='';
  }, true);

  function restorePendingEtfAssetFocus() {
    if (!pendingEtfAssetFocus) return;
    const active = document.activeElement;
    if (active !== document.body && active !== document.documentElement && active?.isConnected) {
      pendingEtfAssetFocus = '';
      return;
    }
    const asset = pendingEtfAssetFocus;
    pendingEtfAssetFocus = '';
    const target = document.querySelector(`[data-etf-asset="${asset}"]`);
    target?.focus({preventScroll:true});
  }
  try {
    const savedWindow = Number(localStorage.getItem(WINDOW_KEY));
    if (WINDOWS.includes(savedWindow)) activeWindow = savedWindow;
    const savedTemperatureWindow = Number(localStorage.getItem(TEMPERATURE_WINDOW_KEY));
    if (WINDOWS.includes(savedTemperatureWindow)) temperatureWindow = savedTemperatureWindow;
  } catch (_) {}

  const finite = value => {
    if (value === null || value === undefined || value === '') return null;
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  };
  const number = (value, digits = 1) => {
    const parsed = finite(value);
    return parsed == null ? '—' : parsed.toFixed(digits);
  };
  const percent = (value, digits = 1) => {
    const parsed = finite(value);
    return parsed == null ? '—' : `${parsed.toFixed(digits)}%`;
  };
  const returnPct = (value, digits = 2) => {
    const parsed = finite(value);
    return parsed == null ? '—' : `${parsed >= 0 ? '+' : ''}${(parsed * 100).toFixed(digits)}%`;
  };
  const money = value => {
    const parsed = finite(value);
    if (parsed == null) return '—';
    const sign = parsed > 0 ? '+' : '';
    if (Math.abs(parsed) >= 1e8) return `${sign}${(parsed / 1e8).toFixed(2)} 亿元`;
    if (Math.abs(parsed) >= 1e4) return `${sign}${(parsed / 1e4).toFixed(1)} 万元`;
    return `${sign}${parsed.toFixed(0)} 元`;
  };
  const amountMoney = value => {
    const parsed = finite(value);
    if (parsed == null) return '—';
    const abs = Math.abs(parsed);
    if (abs >= 1e8) return `${(parsed / 1e8).toFixed(2)} 亿元`;
    if (abs >= 1e4) return `${(parsed / 1e4).toFixed(1)} 万元`;
    return `${parsed.toFixed(0)} 元`;
  };
  const shares = value => {
    const parsed = finite(value);
    if (parsed == null) return '—';
    const sign = parsed > 0 ? '+' : parsed < 0 ? '−' : '';
    const abs = Math.abs(parsed);
    if (abs >= 1e8) return `${sign}${(abs / 1e8).toFixed(2)} 亿份`;
    if (abs >= 1e4) return `${sign}${(abs / 1e4).toFixed(1)} 万份`;
    return `${sign}${abs.toFixed(0)} 份`;
  };
  const tone = value => finite(value) > 0 ? 'up' : finite(value) < 0 ? 'down' : '';
  const signed = (value, digits = 1, suffix = '') => {
    const parsed = finite(value);
    return parsed == null ? '—' : `${parsed > 0 ? '+' : ''}${parsed.toFixed(digits)}${suffix}`;
  };
  const signal = (item, window = activeWindow) => item?.signals?.[String(window)] || {};
  const groupScore = item => item.score;
  const pp = value => signed(value,1,' pp');
  const scoreEvidenceMarkup = score => `<div class="rotation-evidence-list">${(score?.items || []).map(item => `<div class="rotation-evidence-row" data-available="${item.available}"><strong>${esc(item.label)}</strong><div><div class="rotation-meter"><i style="--ratio:${item.available ? Math.max(0,Math.min(1,Number(item.score)/100)) : 0}"></i></div><span>${esc(item.note || '')}</span></div><output>${item.available ? number(item.score,1) : '待补'} · ${item.weight}</output></div>`).join('')}</div>`;
  const windowControl = label => `<div class="rotation-window-control" aria-label="${esc(label)}">${WINDOWS.map(window => `<button type="button" data-rotation-window="${window}" aria-pressed="${String(window === activeWindow)}">${window} 日</button>`).join('')}</div>`;
  const temperatureWindowControl = available => `<div class="rotation-window-control" aria-label="市场温度变化窗口">${WINDOWS.map(window => `<button type="button" data-temperature-window="${window}" aria-pressed="${String(window === temperatureWindow)}" ${available ? '' : 'disabled'}>${window} 日</button>`).join('')}</div>`;
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

  function sourceLabel(value) {
    const raw = String(value || '').trim();
    const lower = raw.toLowerCase();
    if (lower.startsWith('free-stockdb')) return '本地 StockDB';
    if (lower.startsWith('local') || lower.startsWith('research_lake')) return '本地数据缓存';
    if (lower.startsWith('tushare')) return 'Tushare';
    if (lower.startsWith('akshare')) return 'AKShare';
    if (lower.startsWith('ths')) return '同花顺';
    if (lower.startsWith('eastmoney')) return '东方财富';
    return raw.replace(/[-_:]+/g, ' ');
  }

  function updateMeta(kind, meta) {
    renderSourceStatus(meta);
    const target = document.querySelector(`[data-rotation-asof="${kind}"]`);
    if (target) {
      const values = target.querySelectorAll('dd');
      if (values[0]) values[0].textContent = meta?.as_of || '尚无快照';
      if (values[1] && kind === 'temperature') values[1].textContent = meta ? '本地快照已生成' : '等待快照';
      if (values[1] && kind === 'overview') values[1].textContent = meta ? '本地快照已生成' : '等待快照';
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
      const source = sources.slice(0, 2).map(sourceLabel).join(' · ') || '本地缓存';
      line.innerHTML = `${qualityMarkup(meta)}<span>${esc(meta.as_of || '尚无日期')}</span><span>${esc(source)}</span>`;
    }
  }

  const DATA_STATUS_LABELS = {
    local_hit:'本地完整命中',local_stale_accepted:'本地旧快照可用',
    local_plus_remote_complete:'本地 + 远程补齐完整',remote_complete:'远程补齐完整',
    partial_coverage:'部分覆盖',unavailable:'数据不可用',
  };
  const PROVIDER_STATUS_LABELS = {
    available:'可用',rate_limited:'限流',auth_invalid:'认证无效',
    permission_missing:'接口权限不足',capability_missing:'能力缺失',network:'网络异常',
    '5xx':'上游 5xx',contract_changed:'接口合同变化',
  };
  function readableTime(value) {
    const seconds = Number(value || 0);
    return seconds > 0 ? new Date(seconds * 1000).toLocaleString('zh-CN',{hour12:false}) : '—';
  }
  function renderSourceStatus(meta) {
    const status = meta?.status || {}, data = status.data || {}, providers = status.providers || [];
    const coverage = data.coverage || {}, provenance = data.provenance || {}, pending = data.pending || {};
    const taxonomy = provenance.taxonomy || {};
    const identities = [taxonomy.theme?.taxonomy_id, taxonomy.industry?.taxonomy_id];
    const identity = identities.find(value => value && !String(value).startsWith('unresolved:'))
      || identities.find(Boolean) || '—';
    const complete = Number(coverage.complete || 0), total = Number(coverage.total || 0);
    const dataState = document.querySelector('[data-rotation-data-state]');
    if (dataState) {
      dataState.textContent = DATA_STATUS_LABELS[data.resolution] || '等待快照';
      dataState.dataset.state = data.state || 'unavailable';
    }
    const rows = document.querySelectorAll('[data-rotation-data-summary] dd');
    if (rows[0]) rows[0].textContent = identity;
    if (rows[1]) rows[1].textContent = data.as_of || '—';
    if (rows[2]) rows[2].textContent = total ? `${complete}/${total}` : '分母待确认';
    if (rows[3]) rows[3].textContent = `本地 ${Number(provenance.local_hits || 0)} · 远程 ${Number(provenance.remote_fills || 0)}`;
    const progress = document.querySelector('[data-rotation-data-progress]');
    if (progress) {
      progress.max = Math.max(1,total); progress.value = Math.min(complete,Math.max(1,total));
      progress.setAttribute('aria-valuetext',total ? `${complete}/${total} 完成` : '覆盖分母待确认');
    }
    const missing = coverage.missing_partitions || [], affected = data.affected_views || [];
    const detail = document.querySelector('[data-rotation-data-detail]');
    if (detail) detail.innerHTML = `<dl><div><dt>缺失分区</dt><dd>${esc(missing.join('、') || '无')}</dd></div><div><dt>Pending queue</dt><dd>${Number(pending.retryable || 0)} 待重试 / ${Number(pending.total || 0)} 总项</dd></div><div><dt>影响页面/任务</dt><dd>${esc(affected.join('、') || '无')}</dd></div></dl>`;
    const providerState = document.querySelector('[data-rotation-provider-state]');
    const failures = providers.filter(item => item.state !== 'available');
    if (providerState) providerState.textContent = failures.length ? `${failures.length} 项待恢复` : providers.length ? '全部可用' : '未涉及远程来源';
    const providerSummary = document.querySelector('[data-rotation-provider-summary]');
    if (providerSummary) providerSummary.innerHTML = failures.length
      ? `<p>当前数据状态独立；${failures.length} 项上游能力不会在本地完整时阻断页面。</p>`
      : '<p>没有影响当前数据用途的上游问题。</p>';
    const providerDetail = document.querySelector('[data-rotation-provider-detail]');
    if (providerDetail) providerDetail.innerHTML = providers.length ? providers.map(item => `<section><div><strong>${esc(item.provider)} · ${esc(item.capability)}</strong><span>${esc(PROVIDER_STATUS_LABELS[item.state] || item.state)}</span></div><dl><div><dt>恢复/探测</dt><dd>${esc(readableTime(item.retry_after_at || item.next_probe_at))}</dd></div><div><dt>最近成功/失败</dt><dd>${esc(readableTime(item.last_success_at))} / ${esc(readableTime(item.last_failure_at))}</dd></div><div><dt>诊断码</dt><dd><code>${esc(item.diagnostic_code || '—')}</code> <button type="button" class="rotation-status-copy" data-copy-provider-code="${esc(item.diagnostic_code || '')}" aria-label="复制 ${esc(item.provider)} 脱敏诊断码">复制</button></dd></div></dl></section>`).join('') : '<p>暂无上游诊断。</p>';
    const live = document.getElementById('rotation-status-live');
    if (live) live.textContent = data.resolution ? `${DATA_STATUS_LABELS[data.resolution] || data.resolution}，${complete}/${total || '未知'} 完成` : '等待页面数据';
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

  function evidenceRadarChart(items, comparison) {
    const chart = mkChart('rotation-evidence-radar');
    if (!chart) return;
    const dimensions = [
      ['trend','趋势分布'], ['breadth','涨跌宽度'], ['volume','量能确认'],
      ['etf_capital','ETF 资金'], ['sentiment','情绪代理'],
    ];
    const indexed = new Map((items || []).map(item => [item.id,item]));
    const compared = new Map((comparison?.evidence?.items || []).map(item => [item.id,item]));
    const evidence = dimensions.map(([id,label]) => ({id,label,item:indexed.get(id)}));
    const currentComplete = evidence.every(({item}) => item?.available && Number.isFinite(Number(item.score)));
    const previousComplete = evidence.every(({id}) => {
      const item = compared.get(id);
      return item?.previous_available && Number.isFinite(Number(item.previous_score));
    });
    const currentValues = evidence.map(({item}) => Number(item?.score));
    const previousValues = evidence.map(({id}) => Number(compared.get(id)?.previous_score));
    const currentLabel = `当前 · ${comparison?.current_as_of || '最新'}`;
    const previousLabel = `${temperatureWindow} 日前 · ${comparison?.reference_as_of || '历史不足'}`;
    const series = [];
    if (currentComplete) {
      series.push({
        name:currentLabel,type:'radar',symbol:'circle',symbolSize:4,
        lineStyle:{color:CHART_COLORS.primary,width:1.7},itemStyle:{color:CHART_COLORS.primary},
        areaStyle:{color:'rgba(57,135,229,.14)'},data:[{name:currentLabel,value:currentValues}],
      });
    }
    if (currentComplete && previousComplete) {
      series.push({
        name:previousLabel,type:'radar',symbol:'none',
        lineStyle:{color:CHART_COLORS.neutral,width:1.4,type:'dashed'},
        itemStyle:{color:CHART_COLORS.neutral},areaStyle:{opacity:0},
        data:[{name:previousLabel,value:previousValues}],
      });
    }
    chart.setOption(baseOpt({
      legend:currentComplete ? {top:0,data:series.map(item => item.name),textStyle:{color:INK2,fontSize:9},itemWidth:18,itemHeight:6} : undefined,
      tooltip:currentComplete ? {
        trigger:'item',backgroundColor:'#1a1a19',borderColor:AXIS,textStyle:{color:'#fff',fontSize:10},
        formatter:() => `${esc(currentLabel)}<br>${evidence.map(({id,label}) => {
          const current = indexed.get(id), prior = compared.get(id);
          return `${esc(label)} ${number(current?.score,1)} / ${number(prior?.previous_score,1)} · ${signed(prior?.change_pp,1)}`;
        }).join('<br>')}`,
      } : {show:false},
      radar:{center:['50%','57%'],radius:'61%',startAngle:90,splitNumber:4,indicator:evidence.map(({label}) => ({name:label,max:100})),axisName:{color:INK2,fontSize:10},axisNameGap:7,axisLine:{lineStyle:{color:AXIS}},splitLine:{lineStyle:{color:GRID}},splitArea:{show:false}},
      series,
      graphic:currentComplete ? [] : [{type:'text',left:'center',top:'middle',style:{text:'等待完整五维证据',fill:MUTED,font:'10px sans-serif'}}],
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
    const changePayload = data.change_windows || {};
    const comparison = changePayload.windows?.[String(temperatureWindow)] || {};
    const temperatureChange = Number(comparison.temperature?.change_pp);
    const roundedTemperatureChange = Number.isFinite(temperatureChange)
      ? Math.round(temperatureChange * 10) / 10 : Number.NaN;
    const temperatureDirection = roundedTemperatureChange > 0
      ? '升温' : roundedTemperatureChange < 0
        ? '降温' : Number.isFinite(roundedTemperatureChange) ? '持平' : '历史不足';
    const comparedItems = new Map(
      (comparison.evidence?.items || []).map(item => [item.id,item]),
    );
    const comparableCount = Number(comparison.evidence?.comparable_count || 0);
    const totalEvidence = Number(comparison.evidence?.total_count || 5);
    const changeAvailable = Boolean(changePayload.windows);
    const radarNote = comparableCount === totalEvidence
      ? `五维均可与 ${comparison.reference_as_of || `${temperatureWindow} 日前`} 比较`
      : comparison.reference_as_of
        ? `${temperatureWindow} 日前仅 ${comparableCount}/${totalEvidence} 维可比，历史轮廓未绘制`
        : '生成 V7 市场快照后可查看历史轮廓';
    out.innerHTML = `
      <div class="rotation-commandbar rotation-temperature-commandbar"><div><strong>温度变化窗口</strong><span>比较当前与 N 个交易日前；只改变本页变化参照</span></div>${temperatureWindowControl(changeAvailable)}</div>
      <div class="rotation-kpis">
        <div class="rotation-kpi"><span>市场温度</span><strong class="${Number(current.temperature) > 50 ? 'up' : ''}">${percent(current.temperature)}</strong><small>趋势向上样本占比 · <span class="rotation-temperature-change ${tone(roundedTemperatureChange)}">${temperatureWindow} 日 ${signed(roundedTemperatureChange,1)} · ${temperatureDirection}</span></small></div>
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
      <section class="rotation-section"><div class="rotation-section-head"><div><h3>证据分解</h3><p>填充条为当前分，细刻度为 ${temperatureWindow} 个交易日前；缺失维度不补零</p></div><output>有效权重 ${data.evidence?.available_weight || 0}/100 · 综合 ${number(data.evidence?.score,1)}</output></div>
        <div class="rotation-evidence-layout"><div class="rotation-evidence-radar-wrap"><div class="rotation-chart rotation-evidence-radar" id="rotation-evidence-radar" aria-label="市场温度五维证据当前与历史雷达图"></div><p>${esc(radarNote)}</p></div><div class="rotation-evidence-list">${(data.evidence?.items || []).map(item => { const compared = comparedItems.get(item.id) || {}; const previousRatio = compared.previous_available ? Math.max(0,Math.min(1,Number(compared.previous_score)/100)) : null; const comparisonTitle = compared.previous_available ? `${temperatureWindow} 日前 ${number(compared.previous_score,1)}；变化 ${signed(compared.change_pp,1)}` : (compared.previous_note || '历史证据不足'); return `<div class="rotation-evidence-row" data-available="${item.available}" data-comparable="${Boolean(compared.comparable)}" title="${esc(comparisonTitle)}"><strong>${esc(item.label)}</strong><div><div class="rotation-meter rotation-evidence-meter"><i style="--ratio:${item.available ? Math.max(0,Math.min(1,Number(item.score)/100)) : 0}"></i>${previousRatio == null ? '' : `<b class="rotation-meter-reference" style="--reference:${previousRatio}" aria-label="${temperatureWindow} 日前 ${number(compared.previous_score,1)}"></b>`}</div><span>${esc(item.note || '')}</span></div><output><strong>${item.available ? number(item.score,1) : '待补'}</strong><span class="${tone(compared.change_pp)}">${compared.comparable ? signed(compared.change_pp,1) : '不可比'}</span><small>权重 ${item.weight}</small></output></div>`; }).join('')}</div></div>
      </section>${issuesMarkup(meta)}`;
    temperatureChart(data.history || []);
    recentTemperatureChart(recent);
    evidenceRadarChart(data.evidence?.items || [], comparison);
  }

  const structureBarPoint = (row, key, direction) => {
    const candidate = row?.[key];
    const rawReturn = candidate == null ? Number.NaN : Number(candidate);
    return {
      value:[row.date,Number.isFinite(rawReturn) ? direction * Math.abs(rawReturn) : null],
      rawReturn:Number.isFinite(rawReturn) ? rawReturn : null,
    };
  };

  function structureChart(history) {
    const chart = mkChart('rotation-structure-chart');
    if (!chart) return;
    const structureExtent = Math.max(
      .0025,
      ...history.flatMap(row => [row.strong_return,row.weak_return,row.spread])
        .filter(value => value != null).map(Number).filter(Number.isFinite).map(Math.abs),
    ) * 1.12;
    chart.setOption(baseOpt({
      legend:{top:0,textStyle:{color:INK2,fontSize:10}},
      grid:{left:52,right:18,top:38,bottom:34}, xAxis:timeAxis(),
      yAxis:{type:'value',min:-structureExtent,max:structureExtent,axisLabel:{color:MUTED,formatter:value => `${(value * 100).toFixed(1)}%`},splitLine:{lineStyle:{color:GRID}}},
      tooltip:{
        trigger:'axis',
        formatter:params => {
          const items = (params || []).filter(item => item?.seriesName);
          if (!items.length) return '';
          const date = items[0].axisValueLabel || items[0].axisValue || '';
          const lines = items.map(item => {
            const hasRawReturn = Object.prototype.hasOwnProperty.call(item.data || {},'rawReturn');
            const plotted = Array.isArray(item.value) ? item.value[item.value.length - 1] : item.value;
            const value = hasRawReturn ? item.data.rawReturn : plotted;
            return `${item.marker || ''}${esc(item.seriesName)} ${value == null ? '—' : returnPct(value)}`;
          });
          return `${esc(date)}<br>${lines.join('<br>')}`;
        },
      },
      series:[
        {name:'强势样本',type:'bar',barMaxWidth:7,data:history.map(row => structureBarPoint(row,'strong_return',1)),itemStyle:{color:CHART_COLORS.up,borderRadius:[2,2,0,0]},z:2},
        {name:'低位样本',type:'bar',barMaxWidth:7,barGap:'-100%',data:history.map(row => structureBarPoint(row,'weak_return',-1)),itemStyle:{color:CHART_COLORS.down,borderRadius:[0,0,2,2]},z:2},
        {
          name:'强弱差',type:'line',showSymbol:false,
          data:history.map(row => [row.date,row.spread]),
          lineStyle:{color:CHART_COLORS.primary,width:1.8,type:'solid'},
          itemStyle:{color:CHART_COLORS.primary},
          z:4,
          markArea:{
            silent:true,label:{show:false},
            itemStyle:{color:'rgba(201,150,66,.07)'},
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
        <section class="rotation-section"><div class="rotation-section-head"><div><h3>强弱样本收益</h3><p>红绿柱按样本组上下镜像，悬停显示原始收益；蓝线为强弱差，黄色带为 ±0.25 pp 死区</p></div></div><div class="rotation-chart tall" id="rotation-structure-chart"></div></section>
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
        return `${esc(item.name)}<br>${activeWindow}日评分 ${number(item.score.score,1)} · ${esc(item.score.grade || '待补')}<br>${activeWindow}日变化 ${pp(currentSignal.rotation_change_pp)}<br>超额 ${returnPct(currentSignal.excess_return)}<br>上涨宽度 ${percent(Number(currentSignal.advance_ratio || 0)*100)}<br>${esc(item.stage_label)}（固定3日）`;
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
      if (industrySort === 'score') return Number(right.score.score ?? -Infinity) - Number(left.score.score ?? -Infinity);
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
    const matrixMarkup = rows.length ? `<div class="rotation-table-wrap"><table class="rotation-table rotation-signal-table"><thead><tr><th>行业</th><th>阶段（3日）</th><th class="numeric">评分 / 等级</th><th class="numeric">${activeWindow}日变化</th><th class="numeric">成员收益</th><th class="numeric">超额</th><th class="numeric">上涨宽度</th><th class="numeric">量能</th><th class="numeric">趋势向上 / 低位</th><th class="numeric">覆盖</th></tr></thead><tbody>${rows.map(item => { const current = signal(item); const coordinates = item.positive_ratio == null || item.weak_ratio == null ? '—' : `${percent(item.positive_ratio)} / ${percent(item.weak_ratio)}`; return `<tr><td><button type="button" data-rotation-detail="industry" data-code="${esc(item.code)}">${esc(item.name)}</button><div class="hint">${esc(item.code)}</div></td><td><span class="rotation-stage" data-stage="${esc(item.stage)}">${esc(item.stage_label)}</span></td><td class="numeric">${number(item.score.score,1)} · ${esc(item.score.grade || '—')}</td><td class="numeric ${tone(current.rotation_change_pp)}">${pp(current.rotation_change_pp)}</td><td class="numeric ${tone(current.member_return)}">${returnPct(current.member_return)}</td><td class="numeric ${tone(current.excess_return)}">${returnPct(current.excess_return)}</td><td class="numeric">${current.advance_ratio == null ? '—' : percent(Number(current.advance_ratio)*100)}</td><td class="numeric ${tone(current.amount_activity)}">${returnPct(current.amount_activity)}</td><td class="numeric">${coordinates}</td><td class="numeric">${item.eligible_count}/${item.member_count}</td></tr>`; }).join('')}</tbody></table></div>` : `<div class="rotation-empty compact"><strong>${isL2 ? '尚未关注可计算的二级行业' : '当前没有可计算的一级行业'}</strong><p>${isL2 ? '打开管理关注区，选择需要持续观察的申万二级行业。' : '请刷新行业快照并核对数据覆盖。'}</p>${isL2 ? '<button class="rotation-refresh" type="button" data-rotation-l2-toggle aria-expanded="false" aria-controls="rotation-l2-manager">管理二级行业</button>' : ''}</div>`;
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
          const body = {l2_codes:checks.filter(input => input.checked).map(input => input.value)};
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
    const score = `<div class="rotation-theme-focus-score"><strong>${number(item.score.score,1)}</strong><span>/ 100</span><small>${activeWindow} 日周期评分 · ${esc(item.score.grade || '待补')}</small></div>`;
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
    target.innerHTML = themeCatalog.length ? `<div class="rotation-table-wrap"><table class="rotation-table rotation-signal-table"><thead><tr><th>题材</th><th>关联一级行业</th><th>阶段（3日）</th><th class="numeric">评分 / 等级</th><th class="numeric">持续</th><th class="numeric">${activeWindow}日变化</th><th class="numeric">成员收益</th><th class="numeric">超额</th><th class="numeric">上涨宽度</th><th class="numeric">量能</th><th class="numeric">覆盖</th></tr></thead><tbody>${themeCatalog.map(item => { const current = signal(item); return `<tr><td><button type="button" data-rotation-detail="theme" data-code="${esc(item.code)}">${esc(item.name)}</button><div class="hint">${esc(item.code)}</div></td><td>${item.primary_industry ? `${esc(item.primary_industry.name)}<div class="hint">${percent(Number(item.primary_industry.theme_share || 0)*100)} · ${item.primary_industry.overlap_count} 只</div>` : '<span class="hint">跨行业 / 证据不足</span>'}</td><td><span class="rotation-stage" data-stage="${esc(item.stage)}">${esc(item.stage_label)}</span></td><td class="numeric">${number(item.score.score,1)} · ${esc(item.score.grade || '—')}</td><td class="numeric">${item.stage_sessions || 0} 日</td><td class="numeric ${tone(current.rotation_change_pp)}">${pp(current.rotation_change_pp)}</td><td class="numeric ${tone(current.member_return)}">${returnPct(current.member_return)}</td><td class="numeric ${tone(current.excess_return)}">${returnPct(current.excess_return)}</td><td class="numeric">${current.advance_ratio == null ? '—' : percent(Number(current.advance_ratio)*100)}</td><td class="numeric ${tone(current.amount_activity)}">${returnPct(current.amount_activity)}</td><td class="numeric">${item.eligible_count}/${item.member_count}</td></tr>`; }).join('')}</tbody></table></div>${pageControl('theme',themePagination,themePageSize)}` : '<div class="rotation-empty"><strong>没有匹配题材</strong><p>可缩短关键词或清除阶段、评分筛选。</p></div>';
  }

  const etfStateBadge = item => `<span class="etf-state" data-state="${esc(item.state || item.sector_state || 'watch')}"><b aria-hidden="true">${ETF_STATE_SHAPES[item.state || item.sector_state || 'watch'] || '●'}</b>${esc(item.state_label || item.sector_state_label || '震荡观察')}</span>`;
  const etfCandidateMarkup = item => (item.candidate_codes || []).map(code => `<span class="etf-candidate-badge">候选 · ${esc(ETF_CANDIDATE_LABELS[code] || code)}</span>`).join('');
  const etfCoverageText = coverage => {
    const entries = Object.values(coverage || {});
    return entries.length ? `${entries.filter(Boolean).length}/${entries.length}` : '—';
  };

  const fundEvidenceText = funds => {
    const value = funds || {}, status = value.status || 'missing';
    const streak = finite(value.unchanged_sessions);
    const streakText = streak != null && streak > 1 ? ` · 连续 ${streak} 个交易日` : '';
    const daily = value.consecutive !== false && value.period_kind !== 'interval';
    const periodPrefix = daily ? '' : `${value.period_label || '跨期累计变化'}：`;
    if (status === 'confirmed_zero') return daily
      ? `0 份（0.00%）· 已确认当日无净申赎${streakText}`
      : `${periodPrefix}0 份（0.00%）· 不解释为当日申赎`;
    if (status === 'confirmed_change' || status === 'confirmed') {
      const delta = finite(value.share_delta), rate = finite(value.share_change_pct), flow = finite(value.estimated_flow);
      if (delta === 0 && rate === 0) return daily
        ? `0 份（0.00%）· 已确认当日无净申赎${streakText}`
        : `${periodPrefix}0 份（0.00%）· 不解释为当日申赎`;
      const rateText = rate == null ? '—' : `${rate >= 0 ? '+' : '−'}${Math.abs(rate * 100).toFixed(2)}%`;
      const flowText = flow == null ? '估算资金 —' : `估算净${flow >= 0 ? '申购' : '赎回'}${amountMoney(Math.abs(flow))}`;
      return `${periodPrefix}${shares(delta)}（${rateText}）· ${flowText}`;
    }
    if (status === 'stale') return `— · 份额仅截至 ${value.effective_date || '前一日'}`;
    return value.message || '— · 未覆盖连续份额快照';
  };

  function etfFreshnessMarkup(freshness) {
    const labels = {market:'行情',shares:'份额',adjustment:'复权',metadata:'元数据'};
    return `<div class="etf-freshness" aria-label="研究证据新鲜度">${Object.entries(labels).map(([key,label]) => {
      const item = freshness?.[key] || {}, coverage = finite(item.coverage), official = finite(item.official_coverage), enhanced = finite(item.enhanced_coverage);
      const coverageText = key === 'metadata' && official != null
        ? `本地 ${percent((coverage || 0) * 100,0)} · 官方目录 ${percent(official * 100,0)}${enhanced != null ? ` · 指数增强 ${percent(enhanced * 100,0)}` : ''}`
        : coverage == null ? '未覆盖' : `覆盖 ${percent(coverage * 100,0)}`;
      return `<div data-status="${esc(item.status || 'missing')}"><span>${label}</span><strong>${esc(item.date || '—')}</strong><small>${coverageText} · ${esc(item.status === 'ready' ? '有效' : item.status === 'stale' ? '滞后' : '待补')}</small></div>`;
    }).join('')}</div>`;
  }

  function drawEtfSectorTable() {
    const target = document.getElementById('rotation-etf-sector-results'); if (!target) return;
    const total = etfSectorCatalog.length, pages = Math.max(1,Math.ceil(total / etfSectorPageSize));
    etfSectorPage = Math.min(pages,Math.max(1,etfSectorPage));
    const start = (etfSectorPage - 1) * etfSectorPageSize;
    const visible = etfSectorCatalog.slice(start,start + etfSectorPageSize);
    const pagination = {page:etfSectorPage,page_size:etfSectorPageSize,total,pages,has_previous:etfSectorPage>1,has_next:etfSectorPage<pages};
    const positionLabel = etfOverview.map?.label || '60 日阶段位置';
    target.innerHTML = visible.length ? `<div class="rotation-table-wrap"><table class="rotation-table rotation-etf-research-table"><thead><tr><th>板块 / 代表 ETF</th><th>状态</th><th class="numeric">5 / 20 / 60 日</th><th class="numeric">趋势</th><th class="numeric">${esc(positionLabel)}</th><th class="numeric">活跃度 / 量比</th><th>申赎证据</th></tr></thead><tbody>${visible.map(item => { const m=item.metrics||{}; return `<tr><td data-label="板块"><button type="button" data-etf-sector="${esc(item.sector_id)}">${esc(item.sector_name)}</button><div class="hint">${esc(item.representative?.name || '—')} · ${item.index_count || 0} 指数 / ${item.member_count || 0} 产品</div></td><td data-label="状态">${etfStateBadge(item)}${etfCandidateMarkup(item)}${(item.risk_badges||[]).map(badge => `<span class="etf-risk-badge">${esc(badge.label)}</span>`).join('')}</td><td data-label="收益" class="numeric ${tone(m.return_20d)}">${returnPct(m.return_5d)} / ${returnPct(m.return_20d)} / ${returnPct(m.return_60d)}</td><td data-label="趋势" class="numeric">${number(item.trend_strength,0)}</td><td data-label="位置" class="numeric">${item.display_position == null ? '— · 暂不可评估' : percent(item.display_position,0)}</td><td data-label="活跃度" class="numeric">${number(item.activity_score,0)} / ${number(m.amount_ratio_5v20,2)}×</td><td data-label="申赎证据" class="etf-fund-cell ${tone(item.funds?.estimated_flow)}">${esc(fundEvidenceText(item.funds))}<small>${esc(item.funds?.coverage_level ? `${{high:'高',medium:'中',low:'低'}[item.funds.coverage_level] || '低'}覆盖 · ${item.funds.interpretation_note || ''}` : '')}</small></td></tr>`; }).join('')}</tbody></table></div>${pageControl('etf-sector',pagination,etfSectorPageSize)}` : '<div class="rotation-empty compact"><strong>当前资产类没有板块</strong><p>切换资产标签，或更新研究证据。</p></div>';
  }

  function drawEtfProductTable() {
    const target = document.getElementById('rotation-etf-product-results'); if (!target) return;
    target.innerHTML = etfProductCatalog.length ? `<div class="rotation-table-wrap"><table class="rotation-table rotation-etf-research-table"><thead><tr><th>ETF</th><th>板块 / 指数</th><th>板块状态</th><th class="numeric">20 日收益</th><th class="numeric">日均成交额</th><th class="numeric">研究位置</th><th>份额变化</th></tr></thead><tbody>${etfProductCatalog.map(item => { const m=item.metrics||{}; return `<tr><td data-label="ETF"><button type="button" data-etf-detail="${esc(item.symbol)}">${esc(item.name)}</button><div class="hint">${esc(item.symbol)}${item.is_representative ? ' · 代表产品' : ''}</div></td><td data-label="板块 / 指数">${esc(item.sector_name)}<div class="hint">${esc(item.normalized_index || '指数待补')}</div></td><td data-label="板块状态">${etfStateBadge(item)}${etfCandidateMarkup(item)}</td><td data-label="20 日收益" class="numeric ${tone(m.return_20d)}">${returnPct(m.return_20d)}</td><td data-label="日均成交额" class="numeric">${amountMoney(m.avg_amount_20d)}</td><td data-label="研究位置" class="numeric">${m.display_position == null ? '—' : `${percent(m.display_position,0)}<small>${esc(item.position_label || '')}</small>`}</td><td data-label="份额变化" class="etf-fund-cell ${tone(item.funds?.estimated_flow)}">${esc(fundEvidenceText(item.funds))}</td></tr>`; }).join('')}</tbody></table></div>${pageControl('etf-product',etfProductPagination,etfProductPageSize)}` : '<div class="rotation-empty compact"><strong>没有匹配产品</strong><p>缩短关键词或清除状态、类别筛选。</p></div>';
  }

  function drawEtfMap() {
    const chart = mkChart('rotation-etf-map'); if (!chart) return;
    const map = etfOverview.map || {}, visibleIds = new Set(map.sector_ids || []);
    const points = etfSectorCatalog.filter(item => visibleIds.has(item.sector_id) && finite(item.trend_strength) != null && finite(item.display_position) != null).map(item => ({
      name:item.sector_name,sectorId:item.sector_id,state:item.state,
      value:[item.trend_strength,item.display_position,item.activity_score || 0,item.metrics?.return_5d],
      label:{show:['leading','low_turn','weakening'].includes(item.state) || (item.candidate_codes||[]).length > 0},
      itemStyle:{color:finite(item.metrics.return_5d) >= 0 ? CHART_COLORS.up : CHART_COLORS.down},
    }));
    chart.setOption(baseOpt({
      animation:!window.matchMedia('(prefers-reduced-motion: reduce)').matches,
      grid:{left:50,right:18,top:18,bottom:42},
      tooltip:{trigger:'item',formatter:params => `${esc(params.data.name)}<br>趋势 ${number(params.value[0],0)} · 位置 ${percent(params.value[1],0)}<br>活跃度 ${number(params.value[2],0)} · 5日 ${returnPct(params.value[3])}`},
      xAxis:{type:'value',min:0,max:100,name:'趋势强度 →',nameLocation:'middle',nameGap:28,axisLabel:{color:MUTED},splitLine:{lineStyle:{color:GRID}}},
      yAxis:{type:'value',min:0,max:100,name:map.label || '研究位置',nameLocation:'middle',nameGap:42,axisLabel:{color:MUTED,formatter:'{value}%'},splitLine:{lineStyle:{color:GRID}}},
      graphic:points.length ? [] : [{type:'text',left:'center',top:'middle',style:{text:map.horizon === 0 ? '货币 ETF 不评估高低位' : '位置或趋势证据不足，暂无法绘制',fill:MUTED,fontSize:11}}],
      series:[{type:'scatter',data:points,symbolSize:value => 10 + Math.max(0,Math.min(24,Number(value[2]||0)*.24)),label:{position:'top',color:INK2,fontSize:9,formatter:params=>params.data.name},emphasis:{focus:'self',label:{show:true}},markLine:{silent:true,symbol:'none',label:{show:false},lineStyle:{color:AXIS,type:'dashed'},data:[{xAxis:60},{yAxis:40},{yAxis:85}]}}],
    }),true);
    chart.off('click'); chart.on('click',params => { if (params.data?.sectorId) openEtfSectorDetail(params.data.sectorId); });
  }

  function renderEtf(payload) {
    const meta = payload.meta || {}, data = payload.data || {}, out = document.getElementById('rotation-etf-content');
    const overview = data.overview || {}, products = data.products || {};
    updateMeta('etfs',meta); etfOverview = overview; etfSectorCatalog = overview.sectors || [];
    etfProductCatalog = products.items || []; etfProductPagination = products.pagination || etfProductPagination;
    displayedEtfSnapshotId = meta.snapshot_id || '';
    displayedEtfTier = meta.tier || selectedEtfTier;
    const json = document.getElementById('rotation-etf-json'), csv = document.getElementById('rotation-etf-csv');
    const exportTier = displayedEtfTier === 'sandbox' ? '&tier=sandbox' : '';
    if (json) json.href = displayedEtfSnapshotId ? `/api/v1/rotation/etfs/export/${encodeURIComponent(displayedEtfSnapshotId)}?format=json${exportTier}` : '#';
    if (csv) csv.href = displayedEtfSnapshotId ? `/api/v1/rotation/etfs/export/${encodeURIComponent(displayedEtfSnapshotId)}?format=csv${exportTier}` : '#';
    if (!etfSectorCatalog.length && !etfProductCatalog.length) { out.innerHTML = emptyMarkup(meta,'尚未生成 ETF V3 研究快照。点击“更新研究”建立板块雷达。','etf'); return; }
    const assets = [['equity','境内权益'],['overseas_equity','海外权益'],['bond','债券'],['commodity','商品'],['money','货币']];
    const summaries = overview.summaries || [];
    const mergeQueueIds = (...values) => [...new Set(values.flat())];
    const queueGroups = [
      ['▲ 领涨',overview.queues?.leading||[]],
      ['△ 动量候选',overview.candidate_queues?.momentum_hot||[]],
      ['◆ 低位 / 候选',mergeQueueIds(overview.queues?.low_turn||[],overview.candidate_queues?.stage_low_rebound||[])],
      ['↗ 改善',overview.queues?.improving||[]],
      ['▼ 走弱',overview.queues?.weakening||[]],
      ['! 风险 / 候选',mergeQueueIds(overview.queues?.risk||[],overview.candidate_queues?.stage_high_activity||[])],
    ];
    const sectorLookup = new Map(etfSectorCatalog.map(item => [item.sector_id,item]));
    const categories = products.categories || [];
    const map = overview.map || {}, mapItems = (map.sector_ids || []).map(id => sectorLookup.get(id)).filter(Boolean);
    const stale = meta.staleness?.stale ? `<aside class="rotation-callout" data-tone="warning"><strong>刷新失败，继续展示旧研究</strong><span>${esc(meta.staleness.reason || '最新证据未通过')}</span></aside>` : '';
    const sandbox = meta.formal_eligible === false ? '<aside class="rotation-callout" data-tone="warning"><strong>本地降级预览</strong><span>母集仅包含本地已观测 ETF，适合探索分析，不会发布或覆盖正式快照。</span></aside>' : '';
    const refreshWarnings = overview.capabilities?.refresh_warnings || [];
    const warningMarkup = refreshWarnings.length ? `<aside class="rotation-callout" data-tone="warning"><strong>部分证据已降级</strong><ul>${refreshWarnings.map(value => `<li>${esc(value)}</li>`).join('')}</ul></aside>` : '';
    out.innerHTML = `${stale}${sandbox}${warningMarkup}${etfFreshnessMarkup(overview.freshness)}
      <div class="etf-asset-tabs" role="tablist" aria-label="ETF 资产类别">${assets.map(([value,label]) => `<button type="button" role="tab" data-etf-asset="${value}" aria-selected="${String(etfAsset===value)}" tabindex="${etfAsset===value?0:-1}">${label}</button>`).join('')}</div>
      <div class="sr-only" aria-live="polite">当前为${esc(assets.find(([value])=>value===etfAsset)?.[1] || '')}，${etfSectorCatalog.length} 个板块，位置口径为${esc(map.label || '待补')}</div>
      <section class="etf-summary-grid" aria-label="本期研究结论">${summaries.map(item => `<button type="button" class="etf-summary" data-kind="${esc(item.kind)}" data-evaluation="${esc(item.evaluation_status || 'unavailable')}" ${item.sector_id ? `data-etf-sector="${esc(item.sector_id)}"` : 'disabled'}><span>${esc(item.title)} · ${{confirmed:'已确认',candidate:'候选',unavailable:'暂不可评估'}[item.evaluation_status] || '待核查'}</span><strong>${esc(item.sector_name || (item.evaluation_status === 'unavailable' ? '证据待补' : '本期无'))}</strong><small>${esc(item.text)}</small></button>`).join('')}</section>
      <section class="etf-radar-grid"><div class="etf-radar-panel"><div class="rotation-section-head"><div><h3>板块地图</h3><p>横轴趋势强度 · 纵轴 ${esc(map.label || '研究位置')} · 面积为活跃度 · 红涨绿跌</p></div><output>${mapItems.length} 个重点 / ${etfSectorCatalog.length} 个全部</output></div><div class="rotation-chart etf-map" id="rotation-etf-map" role="img" aria-label="ETF 板块趋势与位置地图"></div><label class="etf-map-select">键盘选择重点板块<select data-etf-map-select><option value="">选择板块查看研究证据</option>${mapItems.map(item => `<option value="${esc(item.sector_id)}">${esc(item.sector_name)} · ${esc(item.state_label)} · ${number(item.trend_strength,0)}</option>`).join('')}</select></label></div>
      <aside class="etf-queues"><div class="rotation-section-head"><div><h3>状态队列</h3><p>“候选”只表示阶段条件成立，不冒充严格确认。</p></div></div>${queueGroups.map(([label,ids]) => `<section><h4>${label}<span>${ids.length}</span></h4><div>${ids.map(id => { const item=sectorLookup.get(id); return item ? `<button type="button" data-etf-sector="${esc(id)}"><strong>${esc(item.sector_name)}</strong><span>趋势 ${number(item.trend_strength,0)} · ${item.display_position==null?'位置待补':percent(item.display_position,0)}</span></button>` : ''; }).join('') || '<p>本期无满足条件的板块</p>'}</div></section>`).join('')}</aside></section>
      <section class="rotation-section etf-sector-library"><div class="rotation-section-head"><div><h3>板块研究表</h3><p>完整板块保留在表格；地图只展示确认、候选、风险及活跃度优先的重点板块。</p></div><output>${etfSectorCatalog.length} 个</output></div><div id="rotation-etf-sector-results"></div></section>
      <section class="rotation-section etf-product-library"><div class="rotation-section-head"><div><h3>ETF 产品库</h3><p>与板块表独立搜索、筛选和分页；每页最多 50 行。</p></div><output>${etfProductPagination.total || 0} 只</output></div><div class="rotation-filterbar etf-product-filters"><label>搜索产品<input data-rotation-etf-query type="search" value="${esc(etfQuery)}" placeholder="名称、代码、板块或指数"></label><label>类别<select data-rotation-etf-category><option value="">全部类别</option>${categories.map(value => `<option value="${esc(value)}" ${etfCategory===value?'selected':''}>${esc(value)}</option>`).join('')}</select></label><label>状态<select data-rotation-etf-state><option value="">全部状态</option>${Object.entries(ETF_STATE_LABELS).filter(([key])=>key!=='not_applicable').map(([value,label]) => `<option value="${value}" ${etfState===value?'selected':''}>${label}</option>`).join('')}</select></label><label>排序<select data-rotation-etf-sort><option value="trend" ${etfSort==='trend'?'selected':''}>趋势强度</option><option value="activity" ${etfSort==='activity'?'selected':''}>活跃度</option><option value="position" ${etfSort==='position'?'selected':''}>研究位置</option><option value="amount" ${etfSort==='amount'?'selected':''}>成交额</option><option value="return" ${etfSort==='return'?'selected':''}>20 日收益</option><option value="name" ${etfSort==='name'?'selected':''}>名称</option></select></label></div><div id="rotation-etf-product-results"></div></section>${issuesMarkup(meta)}`;
      drawEtfSectorTable(); drawEtfProductTable(); drawEtfMap();
      if (pendingEtfAssetFocus) {
        const asset = pendingEtfAssetFocus;
        if (asset === etfAsset) requestAnimationFrame(restorePendingEtfAssetFocus);
      }
  }

  async function loadEtfHistory() {
    const select = document.getElementById('rotation-etf-history');
    if (!select) return;
    const payload = await fetchView('etf-snapshots','/api/v1/rotation/etfs/snapshots?limit=60');
    select.innerHTML = '<option value="">最新正式快照</option>' + (payload.items || []).map(item =>
      `<option value="${esc(item.snapshot_id)}">${esc(item.as_of_date)} · ${esc(item.snapshot_id.slice(-8))}</option>`
    ).join('');
    select.value = selectedEtfTier === 'production' ? selectedEtfSnapshotId : '';
  }

  async function openEtfDetail(symbol) {
    const target = document.getElementById('rotation-etf-detail'); if (!target) return;
    if (target.hidden || !target.contains(document.activeElement)) etfDrawerReturnFocus = document.activeElement;
    target.hidden = false; target.setAttribute('aria-hidden','false'); target.setAttribute('aria-modal','true');
    target.innerHTML = '<div class="rotation-skeleton"><span></span><span></span></div>';
    try {
      const params = new URLSearchParams();
      if (displayedEtfSnapshotId) params.set('snapshot_id',displayedEtfSnapshotId);
      if (displayedEtfTier === 'sandbox') params.set('tier','sandbox');
      const payload = await api(`/api/v1/rotation/etfs/${encodeURIComponent(symbol)}?${params}`);
      const item=payload.data||{}, m=item.metrics||{}, funds=item.funds||{}, history=item.history||[], peers=item.peer_products||[];
      const eligibleCandidates = Object.values(item.candidates||{}).filter(value=>value.eligible);
      target.dataset.etfIntradaySymbol = item.symbol || symbol;
      target.dataset.etfIntradayLoaded = 'false';
      target.innerHTML = `<div class="rotation-detail-head"><div><span class="rotation-eyebrow">ETF PRODUCT RESEARCH</span><h3>${esc(item.name || symbol)}</h3><p>${esc(item.symbol || symbol)} · ${esc(item.sector_name || '板块待补')} · ${esc(item.normalized_index || '指数待补')}</p></div><button class="rotation-link" type="button" data-close-etf-panel aria-label="关闭 ${esc(item.name || symbol)} 研究抽屉">关闭</button></div>
        <div class="etf-drawer-tabs" role="tablist" aria-label="单只 ETF 研究详情"><button type="button" role="tab" data-etf-drawer-tab="conclusion" aria-selected="true" tabindex="0">结论</button><button type="button" role="tab" data-etf-drawer-tab="trend" aria-selected="false" tabindex="-1">趋势与分钟</button><button type="button" role="tab" data-etf-drawer-tab="funds" aria-selected="false" tabindex="-1">资金证据</button><button type="button" role="tab" data-etf-drawer-tab="products" aria-selected="false" tabindex="-1">同指数产品</button></div>
        <div class="etf-drawer-panel" data-etf-drawer-panel="conclusion"><div class="etf-conclusion"><span>所属板块主状态</span>${etfStateBadge(item)}${etfCandidateMarkup(item)}<strong>趋势 ${number(item.trend_strength,0)} · 活跃度 ${number(item.activity_score,0)}</strong><p>5 / 20 / 60 日收益：${returnPct(m.return_5d)} / ${returnPct(m.return_20d)} / ${returnPct(m.return_60d)}；量能比 ${number(m.amount_ratio_5v20,2)}×。</p><p class="etf-invalidation"><b>下一快照失效条件</b>${esc(item.invalidation||'—')}</p>${eligibleCandidates.map(value=>`<details><summary>${esc(value.label)} · 尚缺 ${value.unmet_conditions?.length||0} 项严格确认条件</summary><p>已满足：${esc((value.met_conditions||[]).join('、')||'—')}</p><p>未满足：${esc((value.unmet_conditions||[]).join('、')||'—')}</p></details>`).join('')}${(item.risk_badges||[]).map(badge=>`<span class="etf-risk-badge">${esc(badge.label)}</span>`).join('')}</div></div>
        <div class="etf-drawer-panel" data-etf-drawer-panel="trend" hidden><div class="rotation-detail-signals"><div><span>20 日位置</span><strong>${m.position_20d==null?'—':percent(m.position_20d,0)}</strong></div><div><span>60 日阶段位置</span><strong>${m.position_60d==null?'—':percent(m.position_60d,0)}</strong></div><div><span>250 日复权位置</span><strong>${m.position_250d==null?'—':percent(m.position_250d,0)}</strong><small>${m.position_250d==null?'可核查复权证据不足，不做长期判断':`距高点 ${returnPct(m.drawdown_250d)}`}</small></div></div><h4 class="etf-chart-label">复权研究价格</h4><div class="rotation-chart compact" id="rotation-etf-product-price"></div><h4 class="etf-chart-label">成交额</h4><div class="rotation-chart compact etf-amount-chart" id="rotation-etf-product-amount"></div><div class="etf-intraday-head"><h4>当日分钟走势</h4><span data-etf-intraday-status>打开此标签后按需读取，不参与日终状态</span></div><div class="rotation-chart compact" id="rotation-etf-product-intraday"></div></div>
        <div class="etf-drawer-panel" data-etf-drawer-panel="funds" hidden><div class="etf-fund-proof"><span>${esc(funds.period_label || '份额变化')}</span><strong class="${tone(funds.estimated_flow)}">${esc(fundEvidenceText(funds))}</strong><p>来源 ${esc(funds.source||'—')} · 有效日 ${esc(funds.effective_date||'—')}</p><p>${esc(funds.interpretation_note || '份额变化只代表一级市场净申购或赎回；二级市场有成交不必然改变份额。')}</p></div></div>
        <div class="etf-drawer-panel" data-etf-drawer-panel="products" hidden><p class="hint">只比较同一规范化指数；当前点击产品始终保留在首行。</p><div class="rotation-table-wrap"><table class="rotation-table"><thead><tr><th>产品</th><th class="numeric">20日额</th><th class="numeric">规模</th><th class="numeric">管理费</th><th>资金状态</th></tr></thead><tbody>${[item,...peers].map((member,index)=>`<tr ${index===0?'data-selected-product="true"':''}><td>${esc(member.name)}<div class="hint">${esc(member.symbol)}${index===0?' · 当前产品':member.is_representative?' · 代表产品':''}</div></td><td class="numeric">${amountMoney(member.metrics?.avg_amount_20d)}</td><td class="numeric">${amountMoney(member.metadata?.total_size)}</td><td class="numeric">${member.metadata?.management_fee==null?'—':percent(Number(member.metadata.management_fee),2)}</td><td>${esc(fundEvidenceText(member.funds))}</td></tr>`).join('')}</tbody></table></div></div>`;
      const price=mkChart('rotation-etf-product-price'); if (price) price.setOption(baseOpt({grid:{left:48,right:16,top:16,bottom:30},xAxis:timeAxis(),yAxis:{type:'value',scale:true,axisLabel:{color:MUTED},splitLine:{lineStyle:{color:GRID}}},series:[{type:'line',showSymbol:false,data:history.map(row=>[row.date,row.price]),lineStyle:{color:CHART_COLORS.primary,width:1.6}}]}));
      const amount=mkChart('rotation-etf-product-amount'); if (amount) amount.setOption(baseOpt({grid:{left:62,right:16,top:14,bottom:30},xAxis:timeAxis(),yAxis:{type:'value',axisLabel:{color:MUTED,formatter:value=>amountMoney(value)},splitLine:{lineStyle:{color:GRID}}},series:[{type:'bar',barMaxWidth:7,data:history.map(row=>[row.date,row.amount]),itemStyle:{color:CHART_COLORS.secondary}}]}));
      target.querySelector('[role="tab"]')?.focus({preventScroll:true});
    } catch (error) { target.innerHTML = errorMarkup(error); }
  }

  function closeEtfPanel() {
    const panel = document.getElementById('rotation-etf-detail'); if (!panel) return;
    panel.hidden = true; panel.setAttribute('aria-hidden','true'); panel.setAttribute('aria-modal','false');
    if (etfDrawerReturnFocus?.isConnected) etfDrawerReturnFocus.focus({preventScroll:true});
    etfDrawerReturnFocus = null;
  }

  async function loadEtfIntraday(drawer) {
    const symbol = drawer?.dataset.etfIntradaySymbol;
    if (!symbol || ['loading','true'].includes(drawer.dataset.etfIntradayLoaded)) return;
    drawer.dataset.etfIntradayLoaded = 'loading';
    const status = drawer.querySelector('[data-etf-intraday-status]');
    if (status) status.textContent = '正在读取单只 ETF 分钟缓存…';
    try {
      const params = new URLSearchParams();
      if (displayedEtfSnapshotId) params.set('snapshot_id',displayedEtfSnapshotId);
      if (displayedEtfTier === 'sandbox') params.set('tier','sandbox');
      const payload = await api(`/api/v1/rotation/etfs/${encodeURIComponent(symbol)}/intraday?${params}`);
      const data = payload.data || {}, series = data.series || [];
      drawer.dataset.etfIntradayLoaded = 'true';
      if (status) status.textContent = series.length
        ? `${data.cache_hit?'已使用缓存':'已按需读取'} · ${series.length} 条 · 不参与日终状态`
        : '分钟源暂未覆盖，日终研究不受影响';
      const chart=mkChart('rotation-etf-product-intraday');
      if (chart) chart.setOption(baseOpt({grid:{left:48,right:16,top:16,bottom:32},xAxis:{type:'time',axisLabel:{color:MUTED},axisLine:{lineStyle:{color:AXIS}}},yAxis:{type:'value',scale:true,axisLabel:{color:MUTED},splitLine:{lineStyle:{color:GRID}}},graphic:series.length?[]:[{type:'text',left:'center',top:'middle',style:{text:'分钟走势暂不可用',fill:MUTED,fontSize:11}}],series:[{type:'line',showSymbol:false,data:series.map(row=>[row.time,row.close]),lineStyle:{color:CHART_COLORS.primary,width:1.5}}]}));
    } catch (error) {
      drawer.dataset.etfIntradayLoaded = 'false';
      if (status) status.textContent = `分钟走势暂不可用：${error.message || '读取失败'}；日终研究不受影响`;
    }
  }

  async function openEtfSectorDetail(sectorId, memberPage = 1, indexKey = '') {
    const target = document.getElementById('rotation-etf-detail'); if (!target) return;
    if (target.hidden || !target.contains(document.activeElement)) etfDrawerReturnFocus = document.activeElement;
    target.hidden = false; target.setAttribute('aria-hidden','false'); target.setAttribute('aria-modal','true');
    target.dataset.etfSectorId = sectorId;
    target.dataset.etfSectorIndex = indexKey;
    target.innerHTML = '<div class="rotation-skeleton"><span></span><span></span></div>';
    try {
      const params = new URLSearchParams({page:String(memberPage),page_size:'25'});
      if (displayedEtfSnapshotId) params.set('snapshot_id',displayedEtfSnapshotId);
      if (displayedEtfTier === 'sandbox') params.set('tier','sandbox');
      if (indexKey) params.set('index_key',indexKey);
      const payload = await api(`/api/v1/rotation/etfs/sectors/${encodeURIComponent(sectorId)}?${params}`);
      const item=payload.data||{}, m=item.metrics||{}, funds=item.funds||{}, members=item.members||[], groups=item.index_groups||[], pagination=item.member_pagination||{};
      const eligibleCandidates = Object.values(item.candidates||{}).filter(value=>value.eligible);
      target.innerHTML = `<div class="rotation-detail-head"><div><span class="rotation-eyebrow">SECTOR RESEARCH</span><h3>${esc(item.sector_name)}</h3><p>${esc(item.category)} · ${item.index_count||0} 个指数 · ${item.member_count||0} 只产品</p></div><button class="rotation-link" type="button" data-close-etf-panel aria-label="关闭 ${esc(item.sector_name)} 研究抽屉">关闭</button></div>
        <div class="etf-drawer-tabs" role="tablist" aria-label="板块研究详情"><button type="button" role="tab" data-etf-drawer-tab="conclusion" aria-selected="true" tabindex="0">结论</button><button type="button" role="tab" data-etf-drawer-tab="trend" aria-selected="false" tabindex="-1">趋势位置</button><button type="button" role="tab" data-etf-drawer-tab="funds" aria-selected="false" tabindex="-1">资金证据</button><button type="button" role="tab" data-etf-drawer-tab="products" aria-selected="false" tabindex="-1">指数与产品</button></div>
        <div class="etf-drawer-panel" data-etf-drawer-panel="conclusion"><div class="etf-conclusion"><span>主状态</span>${etfStateBadge(item)}${etfCandidateMarkup(item)}<strong>趋势 ${number(item.trend_strength,0)} · 活跃度 ${number(item.activity_score,0)}</strong><p>5 / 20 / 60 日收益：${returnPct(m.return_5d)} / ${returnPct(m.return_20d)} / ${returnPct(m.return_60d)}；量能比 ${number(m.amount_ratio_5v20,2)}×。</p><p class="etf-invalidation"><b>下一快照失效条件</b>${esc(item.invalidation||'—')}</p>${eligibleCandidates.map(value=>`<details><summary>${esc(value.label)} · 候选</summary><p>已满足：${esc((value.met_conditions||[]).join('、')||'—')}</p><p>尚缺严格确认：${esc((value.unmet_conditions||[]).join('、')||'—')}</p></details>`).join('')}${(item.risk_badges||[]).map(badge=>`<span class="etf-risk-badge">${esc(badge.label)}</span>`).join('')}</div></div>
        <div class="etf-drawer-panel" data-etf-drawer-panel="trend" hidden><div class="rotation-detail-signals"><div><span>20 日位置</span><strong>${m.position_20d==null?'—':percent(m.position_20d,0)}</strong></div><div><span>60 日阶段位置</span><strong>${m.position_60d==null?'—':percent(m.position_60d,0)}</strong></div><div><span>250 日复权位置</span><strong>${m.position_250d==null?'—':percent(m.position_250d,0)}</strong><small>${m.position_250d==null?'可核查复权证据不足，不做长期判断':`距高点 ${returnPct(m.drawdown_250d)}`}</small></div></div><h4 class="etf-chart-label">板块复权研究价格</h4><div class="rotation-chart compact" id="rotation-etf-sector-price"></div><h4 class="etf-chart-label">板块成交额</h4><div class="rotation-chart compact etf-amount-chart" id="rotation-etf-sector-amount"></div></div>
        <div class="etf-drawer-panel" data-etf-drawer-panel="funds" hidden><div class="etf-fund-proof"><span>${esc(funds.period_label || '份额变化')}</span><strong class="${tone(funds.estimated_flow)}">${esc(fundEvidenceText(funds))}</strong><p>来源 ${esc(funds.source||'—')} · 有效日 ${esc(funds.effective_date||'—')} · 覆盖 ${finite(funds.coverage)==null?'—':percent(Number(funds.coverage)*100,0)}（${{high:'高',medium:'中',low:'低'}[funds.coverage_level]||'低'}）</p><p>${esc(funds.interpretation_note||funds.provenance_note||'份额变化只代表一级市场净申购或赎回。')}</p></div><div class="rotation-chart compact" id="rotation-etf-sector-funds"></div></div>
        <div class="etf-drawer-panel" data-etf-drawer-panel="products" hidden><label class="rotation-compact-field">规范化指数<select data-etf-sector-index><option value="">全部指数</option>${groups.map(group=>`<option value="${esc(group.index_key)}" ${item.selected_index_key===group.index_key?'selected':''}>${esc(group.normalized_index)} · ${group.member_count} 只</option>`).join('')}</select></label><div class="rotation-table-wrap"><table class="rotation-table"><thead><tr><th>产品</th><th class="numeric">20日额</th><th class="numeric">规模</th><th class="numeric">60 / 250日位置</th><th class="numeric">管理费</th><th>资金状态</th></tr></thead><tbody>${members.map(member=>`<tr><td><button type="button" data-etf-detail="${esc(member.symbol)}">${esc(member.name)}</button><div class="hint">${esc(member.symbol)}${member.is_representative?' · 代表':''}</div></td><td class="numeric">${amountMoney(member.metrics?.avg_amount_20d)}</td><td class="numeric">${amountMoney(member.metadata?.total_size)}</td><td class="numeric">${member.metrics?.position_60d==null?'—':percent(member.metrics.position_60d,0)} / ${member.metrics?.position_250d==null?'—':percent(member.metrics.position_250d,0)}</td><td class="numeric">${member.metadata?.management_fee==null?'—':percent(Number(member.metadata.management_fee),2)}</td><td>${esc(fundEvidenceText(member.funds))}</td></tr>`).join('')}</tbody></table></div><div class="rotation-pagination"><span>第 ${pagination.page||1}/${pagination.pages||1} 页 · ${pagination.total||0} 只</span><div class="rotation-pagination-actions"><button type="button" data-etf-sector-member-page="${Math.max(1,(pagination.page||1)-1)}" ${pagination.has_previous?'':'disabled'}>上一页</button><button type="button" data-etf-sector-member-page="${Math.min(pagination.pages||1,(pagination.page||1)+1)}" ${pagination.has_next?'':'disabled'}>下一页</button></div></div></div>`;
      const history=item.history||[];
      const price=mkChart('rotation-etf-sector-price'); if (price) price.setOption(baseOpt({grid:{left:48,right:16,top:16,bottom:30},xAxis:timeAxis(),yAxis:{type:'value',scale:true,axisLabel:{color:MUTED},splitLine:{lineStyle:{color:GRID}}},series:[{type:'line',showSymbol:false,data:history.map(row=>[row.date,row.price]),lineStyle:{color:CHART_COLORS.primary,width:1.6},areaStyle:{opacity:.04}}]}));
      const amount=mkChart('rotation-etf-sector-amount'); if (amount) amount.setOption(baseOpt({grid:{left:62,right:16,top:14,bottom:30},xAxis:timeAxis(),yAxis:{type:'value',axisLabel:{color:MUTED,formatter:value=>amountMoney(value)},splitLine:{lineStyle:{color:GRID}}},series:[{type:'bar',barMaxWidth:7,data:history.map(row=>[row.date,row.amount]),itemStyle:{color:CHART_COLORS.secondary}}]}));
      const fundHistory=funds.history||[], fundChart=mkChart('rotation-etf-sector-funds');
      if (fundChart) fundChart.setOption(baseOpt({grid:{left:66,right:16,top:14,bottom:30},xAxis:timeAxis(),yAxis:{type:'value',axisLabel:{color:MUTED,formatter:value=>money(value)},splitLine:{lineStyle:{color:GRID}}},series:[{type:'bar',barMaxWidth:8,data:fundHistory.map(row=>[row.date,row.estimated_flow]),itemStyle:{color:params=>finite(params.value[1])>=0?CHART_COLORS.up:CHART_COLORS.down}}]}));
      target.querySelector('[role="tab"]')?.focus({preventScroll:true});
    } catch (error) { target.innerHTML = errorMarkup(error); }
  }

  async function loadEtfCoverage() {
    const target = document.getElementById('rotation-etf-coverage');
    if (!target || !displayedEtfSnapshotId) return;
    target.hidden = false; target.innerHTML = '<div class="rotation-skeleton"><span></span></div>';
    try {
      const suffix = displayedEtfTier === 'sandbox' ? '?tier=sandbox' : '';
      const payload = await api(`/api/v1/rotation/etfs/snapshots/${encodeURIComponent(displayedEtfSnapshotId)}/coverage${suffix}`);
      const data = payload.data || {}, counts = data.share_semantic_counts || {};
      const currentMap=data.map_position?.[etfAsset]||{};
      target.innerHTML = `<div class="rotation-detail-head"><div><h3>ETF 证据覆盖</h3><p>${esc(payload.meta?.as_of || '')} · ${data.sector_count || 0} 个板块 · ${data.total_symbols || 0} 只产品</p></div><button class="rotation-link" type="button" data-close-etf-panel>关闭诊断</button></div><div class="rotation-detail-signals"><div><span>可核查复权</span><strong>${data.coverage?.verified_adjustment_products ?? 0}</strong><small>stockdb 优先，官方接口补缺</small></div><div><span>当前地图口径</span><strong>${esc(currentMap.label||'待补')}</strong><small>板块位置覆盖 ${percent(Number(currentMap.coverage||0)*100,0)}</small></div><div><span>份额已确认</span><strong>${(counts.confirmed_change || 0) + (counts.confirmed_zero || 0)}</strong><small>含 ${counts.confirmed_zero || 0} 只确认零变化</small></div><div><span>滞后 / 缺失</span><strong>${(counts.stale || 0)} / ${(counts.missing || 0)}</strong><small>分钟线按单只 ETF 打开时读取</small></div></div>${etfFreshnessMarkup(data.freshness)}`;
    } catch (error) { target.innerHTML = errorMarkup(error); }
  }

  async function maybeAutoRefreshEtf(meta) {
    const hint = meta?.refresh || {};
    if (!hint.recommended || !hint.input_id || selectedEtfSnapshotId) return;
    const button = document.querySelector('[data-etf-research-scan]');
    if (!button || button.disabled) return;
    try {
      if (sessionStorage.getItem(ETF_AUTO_INPUT_KEY) === hint.input_id) return;
      sessionStorage.setItem(ETF_AUTO_INPUT_KEY,hint.input_id);
    } catch (_) {}
    refreshResult('etf','检测到更新的本地 ETF 输入',`${hint.input_as_of || '最新日期'} · 正在单飞补算研究快照`,'info');
    await scanEtfResearch(button);
  }

  async function openEtfFundHistory() {
    const target=document.getElementById('rotation-etf-detail'); if (!target) return;
    etfDrawerReturnFocus=document.activeElement; target.hidden=false;
    target.setAttribute('aria-hidden','false'); target.setAttribute('aria-modal','true');
    target.innerHTML='<div class="rotation-skeleton"><span></span><span></span></div>';
    try {
      const payload=await api('/api/v1/rotation/etf-flows?include_items=false');
      const data=payload.data||{}, daily=data.daily||[];
      target.innerHTML=`<div class="rotation-detail-head"><div><span class="rotation-eyebrow">SECONDARY EVIDENCE</span><h3>宽基 ETF 资金历史</h3><p>保留原有三年宽基申赎历史，仅作附加资金证据，不改变板块主状态。</p></div><button class="rotation-link" type="button" data-close-etf-panel>关闭</button></div><div class="rotation-chart tall" id="rotation-etf-fund-history-chart"></div>`;
      const chart=mkChart('rotation-etf-fund-history-chart');
      if (chart) chart.setOption(baseOpt({legend:{top:0,textStyle:{color:INK2,fontSize:10}},grid:{left:70,right:18,top:38,bottom:54},xAxis:timeAxis(),yAxis:{type:'value',axisLabel:{color:MUTED,formatter:value=>money(value)},splitLine:{lineStyle:{color:GRID}}},dataZoom:chartZoom(daily.length,{initialPoints:260}),series:[{name:'当日净流',type:'bar',barMaxWidth:7,data:daily.map(row=>[row.date,row.flow]),itemStyle:{color:params=>finite(params.value[1])>=0?CHART_COLORS.up:CHART_COLORS.down}},{name:'累计净流',type:'line',showSymbol:false,data:daily.map(row=>[row.date,row.cumulative]),lineStyle:{color:CHART_COLORS.primary,width:1.5}}]}));
      target.querySelector('[data-close-etf-panel]')?.focus({preventScroll:true});
    } catch (error) { target.innerHTML=errorMarkup(error); }
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
      } else if (rotationActive && rotationPage === 'etfs') {
        const overviewParams = new URLSearchParams();
        const productParams = new URLSearchParams({page:String(etfProductPage),page_size:String(etfProductPageSize),sort:etfSort,order:etfSort === 'name' ? 'asc' : 'desc'});
        if (etfAsset) { overviewParams.set('asset', etfAsset); productParams.set('asset', etfAsset); }
        if (etfQuery.trim()) productParams.set('query',etfQuery.trim());
        if (etfCategory) productParams.set('category',etfCategory);
        if (etfState) productParams.set('state',etfState);
        if (selectedEtfSnapshotId) { overviewParams.set('snapshot_id',selectedEtfSnapshotId); productParams.set('snapshot_id',selectedEtfSnapshotId); }
        if (selectedEtfTier === 'sandbox') { overviewParams.set('tier','sandbox'); productParams.set('tier','sandbox'); }
        const [overview,products] = await Promise.all([
          fetchView(`etf-overview:${overviewParams}`,`/api/v1/rotation/etfs/overview?${overviewParams}`,force),
          fetchView(`etf-products:${productParams}`,`/api/v1/rotation/etfs?${productParams}`,force),
        ]);
        etfProductPage = Number(products.data?.pagination?.page || etfProductPage);
        payload = {meta:overview.meta,data:{overview:overview.data,products:products.data}};
        if (stillCurrent()) {
          renderEtf(payload);
          loadEtfHistory().catch(error => reportLocalError('ETF 研究','快照历史未能读取',error));
          maybeAutoRefreshEtf(overview.meta).catch(error => reportLocalError('ETF 研究','自动补算未能启动',error));
        }
      }
    } catch (error) {
      if (!stillCurrent()) return;
      const target = marketActive
        ? (marketPage === 'temperature' ? document.getElementById('market-temperature-content') : document.getElementById('market-style-content'))
        : document.getElementById(`rotation-${rotationPage === 'etfs' ? 'etf' : rotationPage}-content`);
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
    const route = `${TODAY_ROUTE_PREFIX}${page}`;
    if (updateHash && location.hash !== route) history.replaceState(null,'',route);
    if (page !== 'quotes') loadCurrent();
    requestAnimationFrame(() => Object.values(charts).forEach(chart => chart.resize()));
  }

  function setRotationPage(page, updateHash = true) {
    if (page === 'radar') page = 'overview';
    if (!['overview','industry','themes','etfs'].includes(page)) page = 'overview';
    activeRotationPage = page;
    document.querySelectorAll('[data-rotation-page]').forEach(button => {
      const selected = button.dataset.rotationPage === page;
      button.setAttribute('aria-selected',String(selected));
      button.tabIndex = selected ? 0 : -1;
    });
    document.querySelectorAll('[data-rotation-view]').forEach(view => { view.hidden = view.dataset.rotationView !== page; });
    const routePage = {overview:'rotation',industry:'industry',themes:'themes',etfs:'etfs'}[page] || 'rotation';
    const route = `${TODAY_ROUTE_PREFIX}${routePage}`;
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
        activeJob = await api(`/api/v1/jobs/${activeJob.id}`);
        button.textContent = `${activeJob.phase || '正在分析'} · ${activeJob.progress || 0}%`;
      }
      if (activeJob?.status === 'completed') {
        cache.clear(); themeCatalog = []; etfProductCatalog = []; etfSectorCatalog = [];
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
      const job = await post('/api/v1/market/analytics/refresh',{scope:selected,mode:'incremental',source:'auto',purpose:'current_analysis'});
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
    api(`/api/v1/jobs/${encodeURIComponent(saved.id)}`)
      .then(job => monitorRefresh(job,scope,button))
      .catch(() => clearActiveJob());
  }

  async function jumpToGroup(kind, code) {
    const page = kind === 'theme' ? 'themes' : 'industry';
    setRotationPage(page);
    await loadCurrent();
    openGroupDetail(kind,code);
  }

  async function scanEtfResearch(button, existingJob = null, tier = 'production') {
    const original = button.textContent;
    const selectedTier = existingJob?.tier || tier;
    button.disabled = true;
    button.textContent = '正在提交…';
    const jobBox = document.getElementById('rotation-etf-job');
    const cancel = document.querySelector('[data-etf-research-cancel]');
    const retry = document.querySelector('[data-etf-research-retry]');
    if (jobBox) jobBox.hidden = false;
    if (retry) retry.hidden = true;
    try {
      let job = existingJob || await api('/api/v1/rotation/etfs/scan',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({tier:selectedTier})});
      etfLastJob = job;
      if (cancel) cancel.hidden = !job.can_cancel;
      while (['queued','running','cancelling'].includes(job.status)) {
        button.textContent = `${Math.round(Number(job.progress || 0))}% ${job.phase || '读取本地库'}`;
        if (jobBox) jobBox.querySelector('[data-etf-job-status]').textContent = `${job.progress || 0}% · ${job.phase || '扫描中'} · ${job.detail || ''}`;
        await new Promise(resolve => setTimeout(resolve,800));
        job = await api(`/api/v1/jobs/${encodeURIComponent(job.id)}`);
        etfLastJob = job;
        if (cancel) cancel.hidden = !job.can_cancel;
      }
      if (!['completed','completed_with_warnings'].includes(job.status)) throw new Error(job.error || job.message || 'ETF 扫描失败');
      selectedEtfTier = selectedTier;
      selectedEtfSnapshotId = selectedTier === 'sandbox' ? (job.result?.preview_id || '') : '';
      Array.from(cache.keys()).filter(key => key.startsWith('etf-')).forEach(key => cache.delete(key));
      button.textContent = '扫描完成';
      if (jobBox) jobBox.querySelector('[data-etf-job-status]').textContent = job.message || (selectedTier === 'sandbox' ? 'ETF 本地降级预览已生成' : 'ETF 研究快照已发布');
      await loadCurrent(true);
      if (jobBox) jobBox.hidden = true;
    } catch (error) {
      button.textContent = '扫描失败';
      if (jobBox) jobBox.querySelector('[data-etf-job-status]').textContent = error.message || '扫描失败';
      if (retry) retry.hidden = !etfLastJob || !['failed','cancelled','interrupted'].includes(etfLastJob.status);
      reportLocalError('ETF 研究','全场 ETF 扫描未完成',error);
    } finally {
      if (cancel) cancel.hidden = true;
      setTimeout(() => { button.disabled = false; button.textContent = original; },1200);
    }
  }

  async function cancelEtfResearch(button) {
    if (!etfLastJob?.id) return;
    button.disabled = true;
    try {
      etfLastJob = await api(`/api/v1/jobs/${encodeURIComponent(etfLastJob.id)}/cancel`,{method:'POST',headers:{'Content-Type':'application/json'},body:'{}'});
      document.querySelector('#rotation-etf-job [data-etf-job-status]').textContent = '正在安全取消，已完成的缓存块会保留';
    } catch (error) { reportLocalError('ETF 研究','取消请求失败',error); }
    finally { button.disabled = false; }
  }

  async function retryEtfResearch(button) {
    if (!etfLastJob?.id) return;
    button.disabled = true;
    try {
      etfLastJob = await api(`/api/v1/jobs/${encodeURIComponent(etfLastJob.id)}/retry`,{method:'POST',headers:{'Content-Type':'application/json'},body:'{}'});
      button.hidden = true;
      await scanEtfResearch(document.querySelector('[data-etf-research-scan]'),etfLastJob,etfLastJob?.tier || 'production');
    } catch (error) { reportLocalError('ETF 研究','恢复任务失败',error); }
    finally { button.disabled = false; }
  }

  document.addEventListener('click', event => {
    const diagnostic = event.target.closest('[data-copy-provider-code]');
    if (diagnostic) {
      navigator.clipboard?.writeText(diagnostic.dataset.copyProviderCode || '');
      diagnostic.textContent = '已复制';
      setTimeout(() => { diagnostic.textContent = '复制'; },1000);
      return;
    }
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
        activeWindow = selected; themePage = 1;
        try { localStorage.setItem(WINDOW_KEY,String(activeWindow)); } catch (_) {}
        loadCurrent();
      }
      return;
    }
    const temperatureWindowButton = event.target.closest('[data-temperature-window]');
    if (temperatureWindowButton) {
      const selected = Number(temperatureWindowButton.dataset.temperatureWindow);
      if (WINDOWS.includes(selected) && selected !== temperatureWindow) {
        temperatureWindow = selected;
        try { localStorage.setItem(TEMPERATURE_WINDOW_KEY,String(temperatureWindow)); } catch (_) {}
        const cached = cache.get('temperature');
        if (cached && typeof cached.then !== 'function') renderTemperature(cached);
        else loadCurrent();
      }
      return;
    }
    const pageStep = event.target.closest('[data-rotation-page-step]');
    if (pageStep) {
      const [kind,step] = pageStep.dataset.rotationPageStep.split(':');
      if (kind === 'theme') { themePage = Math.max(1,themePage + Number(step)); loadCurrent(); }
      if (kind === 'etf-product') { etfProductPage = Math.max(1,etfProductPage + Number(step)); loadCurrent(); }
      if (kind === 'etf-sector') { etfSectorPage = Math.max(1,etfSectorPage + Number(step)); drawEtfSectorTable(); }
      return;
    }
    const pageTo = event.target.closest('[data-rotation-page-to]');
    if (pageTo) {
      const [kind,page] = pageTo.dataset.rotationPageTo.split(':');
      if (kind === 'theme') { themePage = Math.max(1,Number(page) || 1); loadCurrent(); }
      if (kind === 'etf-product') { etfProductPage = Math.max(1,Number(page) || 1); loadCurrent(); }
      if (kind === 'etf-sector') { etfSectorPage = Math.max(1,Number(page) || 1); drawEtfSectorTable(); }
      return;
    }
    const jump = event.target.closest('[data-rotation-jump]');
    if (jump) { jumpToGroup(jump.dataset.rotationJump,jump.dataset.code); return; }
    const refreshButton = event.target.closest('[data-rotation-refresh]');
    if (refreshButton) { refresh(refreshButton.dataset.rotationRefresh,refreshButton); return; }
    const etfScan = event.target.closest('[data-etf-research-scan]');
    if (etfScan) { scanEtfResearch(etfScan); return; }
    const etfPreview = event.target.closest('[data-etf-research-preview]');
    if (etfPreview) { scanEtfResearch(etfPreview,null,'sandbox'); return; }
    const etfCancel = event.target.closest('[data-etf-research-cancel]');
    if (etfCancel) { cancelEtfResearch(etfCancel); return; }
    const etfRetry = event.target.closest('[data-etf-research-retry]');
    if (etfRetry) { retryEtfResearch(etfRetry); return; }
    const etfSectorMemberPage = event.target.closest('[data-etf-sector-member-page]');
    if (etfSectorMemberPage) {
      const drawer=etfSectorMemberPage.closest('.rotation-etf-drawer');
      openEtfSectorDetail(drawer.dataset.etfSectorId,Number(etfSectorMemberPage.dataset.etfSectorMemberPage)||1,drawer.dataset.etfSectorIndex||''); return;
    }
    const etfDetail = event.target.closest('[data-etf-detail]');
    if (etfDetail) { openEtfDetail(etfDetail.dataset.etfDetail); return; }
    const etfSector = event.target.closest('[data-etf-sector]');
    if (etfSector) { openEtfSectorDetail(etfSector.dataset.etfSector); return; }
    const etfAssetButton = event.target.closest('[data-etf-asset]');
    if (etfAssetButton) {
      etfAsset=etfAssetButton.dataset.etfAsset; etfCategory=''; etfState='';
      etfAssetButton.closest('[role="tablist"]')?.querySelectorAll('[role="tab"]').forEach(tab=>{
        const selected=tab===etfAssetButton;
        tab.setAttribute('aria-selected',String(selected)); tab.tabIndex=selected?0:-1;
      });
      const category=document.querySelector('[data-rotation-etf-category]');
      const state=document.querySelector('[data-rotation-etf-state]');
      if (category) category.value='';
      if (state) state.value='';
      etfSort=etfAsset==='money'?'amount':'trend'; etfSectorPage=1; etfProductPage=1;
      loadCurrent().finally(restorePendingEtfAssetFocus); return;
    }
    const etfCoverage = event.target.closest('[data-etf-coverage]');
    if (etfCoverage) { loadEtfCoverage(); return; }
    const etfFundHistory = event.target.closest('[data-etf-fund-history]');
    if (etfFundHistory) { openEtfFundHistory(); return; }
    const closeEtf = event.target.closest('[data-close-etf-panel]');
    if (closeEtf) {
      const panel = closeEtf.closest('.rotation-detail');
      if (panel?.id === 'rotation-etf-detail') closeEtfPanel();
      else { panel.hidden = true; panel.setAttribute('aria-hidden','true'); }
      return;
    }
    const drawerTab = event.target.closest('[data-etf-drawer-tab]');
    if (drawerTab) {
      const target=drawerTab.dataset.etfDrawerTab, drawer=drawerTab.closest('.rotation-etf-drawer');
      drawer.querySelectorAll('[data-etf-drawer-tab]').forEach(tab=>{ const selected=tab===drawerTab; tab.setAttribute('aria-selected',String(selected)); tab.tabIndex=selected?0:-1; });
      drawer.querySelectorAll('[data-etf-drawer-panel]').forEach(panel=>{ panel.hidden=panel.dataset.etfDrawerPanel!==target; });
      if (target === 'trend') loadEtfIntraday(drawer);
      requestAnimationFrame(()=>Object.values(charts).forEach(chart=>chart.resize())); return;
    }
    const detail = event.target.closest('[data-rotation-detail]');
    if (detail) { openGroupDetail(detail.dataset.rotationDetail,detail.dataset.code); return; }
    const close = event.target.closest('[data-close-rotation-detail]');
    if (close) close.closest('.rotation-detail').hidden = true;
  });

  document.addEventListener('keydown', event => {
    if (event.key === 'Escape') {
      const drawer = document.getElementById('rotation-etf-detail');
      if (drawer && !drawer.hidden) {
        closeEtfPanel(); return;
      }
    }
    const openDrawer = document.getElementById('rotation-etf-detail');
    if (event.key === 'Tab' && openDrawer && !openDrawer.hidden) {
      const focusable=[...openDrawer.querySelectorAll('button:not([disabled]),select:not([disabled]),input:not([disabled]),a[href],summary,[tabindex]:not([tabindex="-1"])')].filter(value=>value.getClientRects().length);
      if (focusable.length) {
        const first=focusable[0], last=focusable[focusable.length-1];
        if (event.shiftKey && document.activeElement===first) { event.preventDefault(); last.focus(); return; }
        if (!event.shiftKey && document.activeElement===last) { event.preventDefault(); first.focus(); return; }
      }
    }
    const drawerTab = event.target.closest('[data-etf-drawer-tab]');
    if (drawerTab && ['ArrowLeft','ArrowRight','Home','End'].includes(event.key)) {
      const tabs=[...drawerTab.closest('[role="tablist"]').querySelectorAll('[data-etf-drawer-tab]')];
      const index=tabs.indexOf(drawerTab);
      const nextIndex=event.key==='Home'?0:event.key==='End'?tabs.length-1:(index+(event.key==='ArrowRight'?1:-1)+tabs.length)%tabs.length;
      event.preventDefault(); tabs[nextIndex].focus(); tabs[nextIndex].click(); return;
    }
    const mapOption = event.target.closest('[data-etf-map-option]');
    if (mapOption && ['ArrowLeft','ArrowRight','ArrowUp','ArrowDown','Home','End'].includes(event.key)) {
      const options=[...mapOption.closest('[role="listbox"]').querySelectorAll('[data-etf-map-option]')];
      const index=options.indexOf(mapOption), forward=['ArrowRight','ArrowDown'].includes(event.key);
      const nextIndex=event.key==='Home'?0:event.key==='End'?options.length-1:(index+(forward?1:-1)+options.length)%options.length;
      event.preventDefault(); mapOption.tabIndex=-1; options[nextIndex].tabIndex=0; options[nextIndex].focus(); return;
    }
    const etfAssetTab = event.target.closest('.etf-asset-tabs [role="tab"]');
    if (etfAssetTab && ['ArrowLeft','ArrowRight','Home','End'].includes(event.key)) {
      const tabs=[...etfAssetTab.closest('[role="tablist"]').querySelectorAll('[role="tab"]')];
      const index=tabs.indexOf(etfAssetTab);
      const nextIndex=event.key==='Home'?0:event.key==='End'?tabs.length-1:(index+(event.key==='ArrowRight'?1:-1)+tabs.length)%tabs.length;
      event.preventDefault(); pendingEtfAssetFocus=tabs[nextIndex].dataset.etfAsset;
      tabs[nextIndex].focus(); tabs[nextIndex].click(); return;
    }
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
      etfQuery = event.target.value; etfProductPage = 1;
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
      etfCategory = event.target.value; etfProductPage = 1; loadCurrent();
    } else if (event.target.matches('[data-rotation-etf-state]')) {
      etfState = event.target.value; etfProductPage = 1; loadCurrent();
    } else if (event.target.matches('[data-rotation-etf-sort]')) {
      etfSort = event.target.value; etfProductPage = 1; loadCurrent();
    } else if (event.target.matches('[data-rotation-etf-product-page-size]')) {
      etfProductPageSize = Math.min(50,Number(event.target.value) || 50); etfProductPage = 1; loadCurrent();
    } else if (event.target.matches('[data-rotation-etf-sector-page-size]')) {
      etfSectorPageSize = Number(event.target.value) || 25; etfSectorPage = 1; drawEtfSectorTable();
    } else if (event.target.matches('[data-etf-map-select]')) {
      if (event.target.value) openEtfSectorDetail(event.target.value);
    } else if (event.target.matches('[data-etf-sector-index]')) {
      const drawer=event.target.closest('.rotation-etf-drawer');
      drawer.dataset.etfSectorIndex=event.target.value;
      openEtfSectorDetail(drawer.dataset.etfSectorId,1,event.target.value);
    } else if (event.target.matches('#rotation-etf-history')) {
      selectedEtfTier = 'production'; selectedEtfSnapshotId = event.target.value; etfProductPage = 1; etfSectorPage = 1;
      Array.from(cache.keys()).filter(key => key.startsWith('etf-')).forEach(key => cache.delete(key));
      loadCurrent();
    }
  });

  async function mount(page) {
    if (['temperature', 'style'].includes(page)) {
      setMarketPage(page, false);
      recoverActiveJob();
      return;
    }
    const rotationPages = {rotation:'overview', industry:'industry', themes:'themes', etfs:'etfs'};
    setRotationPage(rotationPages[page] || 'overview', false);
    recoverActiveJob();
  }

  function unmount() {
    clearTimeout(searchTimer);
    searchTimer = 0;
    activeJob = null;
    window.QuantCharts?.activateTab('');
  }

  return {mount, unmount, refresh: () => loadCurrent(true)};
})();

export const {mount, unmount, refresh} = rotationFeature;
