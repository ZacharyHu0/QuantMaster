/* QuantMaster restrained chart system: shared theme, motion and lifecycle. */
var CHART_COLORS = Object.freeze({
  primary: '#4f8fd8',
  primarySoft: 'rgba(79,143,216,.16)',
  up: '#e66767',
  down: '#24a06b',
  warning: '#c99642',
  compare: '#8e84c8',
  neutral: '#aaa89f',
  danger: '#cf6d78',
  grid: '#2c2c2a',
  axis: '#41413e',
  surface: '#1a1a19',
  ink: '#f4f3ee',
  ink2: '#c3c2b7',
  muted: '#898781',
});
var PALETTE = [
  CHART_COLORS.primary, CHART_COLORS.warning, CHART_COLORS.down,
  CHART_COLORS.compare, CHART_COLORS.neutral, CHART_COLORS.danger,
];
var COMPARISON_COLORS = [
  CHART_COLORS.primary, CHART_COLORS.warning, CHART_COLORS.compare, CHART_COLORS.neutral,
];
var INK2 = CHART_COLORS.ink2;
var MUTED = CHART_COLORS.muted;
var GRID = CHART_COLORS.grid;
var AXIS = CHART_COLORS.axis;
var motionPreference = window.matchMedia('(prefers-reduced-motion: reduce)');
var REDUCED_MOTION = motionPreference.matches;
var charts = Object.create(null);

window.CHART_COLORS = CHART_COLORS;
window.PALETTE = PALETTE;
window.charts = charts;

if (window.echarts) {
  window.echarts.registerTheme('quantmaster', {
    color: PALETTE,
    backgroundColor: 'transparent',
    textStyle: {color: INK2, fontFamily: '-apple-system, BlinkMacSystemFont, "PingFang SC", "Microsoft YaHei", sans-serif'},
    line: {lineStyle: {width: 2}, symbol: 'circle', symbolSize: 5, smooth: false},
    bar: {itemStyle: {borderRadius: [2, 2, 0, 0]}},
    categoryAxis: {
      axisLine: {lineStyle: {color: AXIS}}, axisTick: {show: false},
      axisLabel: {color: MUTED}, splitLine: {show: false},
    },
    valueAxis: {
      axisLine: {show: false}, axisTick: {show: false}, axisLabel: {color: MUTED},
      splitLine: {lineStyle: {color: GRID, width: 1}},
    },
  });
}

function chartPointCount(option) {
  return (option && option.series || []).reduce(function (maximum, series) {
    return Math.max(maximum, Array.isArray(series && series.data) ? series.data.length : 0);
  }, 0);
}

function chartKind(option) {
  var kinds = (option && option.series || []).map(function (series) { return series && series.type; });
  if (kinds.includes('candlestick')) return 'kline';
  if (kinds.includes('scatter') || kinds.includes('effectScatter')) return 'scatter';
  if (kinds.includes('bar')) return 'bar';
  return 'line';
}

function motionProfile(kind, count) {
  if (REDUCED_MOTION) return {enabled: false, duration: 0, update: 0, stagger: 0};
  if (count > 1000) return {enabled: false, duration: 0, update: 0, stagger: 0, containerOnly: true};
  var dense = (kind === 'line' || kind === 'kline') ? count > 240 : count > 60;
  return {
    enabled: false,
    duration: 0,
    update: 0,
    stagger: dense ? 0 : 0,
  };
}

function roleForSeries(series, index) {
  var name = String(series && series.name || '').toLowerCase();
  if (/回撤|风险|失败|异常/.test(name)) return CHART_COLORS.danger;
  if (/跌|低位|弱势|negative|down/.test(name)) return CHART_COLORS.down;
  if (/涨|强势|positive|up/.test(name)) return CHART_COLORS.up;
  if (/阈值|警戒|ma10|中位|累计/.test(name)) return CHART_COLORS.warning;
  if (/ma20|对照|分位|quantile/.test(name)) return CHART_COLORS.compare;
  if (/基准|benchmark|中证|沪深/.test(name)) return CHART_COLORS.neutral;
  if (index >= 4) return CHART_COLORS.neutral;
  return COMPARISON_COLORS[index % COMPARISON_COLORS.length];
}

function mappedLegacyColor(color) {
  if (!color || typeof color !== 'string') return color;
  var value = color.toLowerCase();
  return ({
    '#3987e5': CHART_COLORS.primary,
    '#d95926': CHART_COLORS.up,
    '#199e70': CHART_COLORS.down,
    '#008300': CHART_COLORS.down,
    '#0ca30c': CHART_COLORS.down,
    '#c98500': CHART_COLORS.warning,
    '#9085e9': CHART_COLORS.compare,
    '#d55181': CHART_COLORS.danger,
    '#e66767': CHART_COLORS.up,
  })[value] || color;
}

function normalizeAxis(axis) {
  if (!axis) return axis;
  if (Array.isArray(axis)) return axis.map(normalizeAxis);
  var result = Object.assign({}, axis);
  result.axisLabel = Object.assign({color: MUTED, hideOverlap: true}, result.axisLabel || {});
  result.axisLine = Object.assign({}, result.axisLine || {}, {
    lineStyle: Object.assign({color: AXIS}, result.axisLine && result.axisLine.lineStyle || {}),
  });
  result.axisTick = Object.assign({show: false}, result.axisTick || {});
  if (result.type === 'value') {
    result.splitLine = Object.assign({show: true}, result.splitLine || {}, {
      lineStyle: Object.assign({color: GRID, width: 1}, result.splitLine && result.splitLine.lineStyle || {}),
    });
  }
  return result;
}

function prepareSeries(series, index, profile) {
  var result = Object.assign({}, series);
  result.id = result.id || ('qm-' + String(result.name || result.type || 'series') + '-' + index);
  var roleColor = roleForSeries(result, index);
  result.lineStyle = Object.assign({}, result.lineStyle || {});
  if (result.lineStyle.color) result.lineStyle.color = mappedLegacyColor(result.lineStyle.color);
  else if (result.type === 'line') result.lineStyle.color = roleColor;
  if (result.type === 'line') {
    result.lineStyle.width = result.lineStyle.width || (index === 0 ? 2.2 : 1.5);
    if (index >= 4 && result.lineStyle.type === undefined) result.lineStyle.type = 'dashed';
    result.showSymbol = result.showSymbol === undefined ? false : result.showSymbol;
    result.connectNulls = result.connectNulls === undefined ? false : result.connectNulls;
  }
  if (result.itemStyle) {
    result.itemStyle = Object.assign({}, result.itemStyle);
    if (result.itemStyle.color) result.itemStyle.color = mappedLegacyColor(result.itemStyle.color);
    if (result.itemStyle.color0) result.itemStyle.color0 = mappedLegacyColor(result.itemStyle.color0);
    if (result.itemStyle.borderColor) result.itemStyle.borderColor = mappedLegacyColor(result.itemStyle.borderColor);
    if (result.itemStyle.borderColor0) result.itemStyle.borderColor0 = mappedLegacyColor(result.itemStyle.borderColor0);
  } else if (result.type === 'bar' || result.type === 'scatter') {
    result.itemStyle = {color: roleColor};
  }
  if (profile.stagger && (result.type === 'bar' || result.type === 'scatter')) {
    result.animationDelay = function (dataIndex) { return Math.min(320, dataIndex * profile.stagger); };
    result.animationDelayUpdate = function (dataIndex) { return Math.min(160, dataIndex * Math.max(2, profile.stagger / 2)); };
  } else {
    result.animationDelay = 0;
    result.animationDelayUpdate = 0;
  }
  if (profile.enabled && (result.type === 'bar' || result.type === 'scatter')) result.universalTransition = true;
  return result;
}

function prepareDataZoom(dataZoom, hasYAxis) {
  if (!Array.isArray(dataZoom) || !dataZoom.length) return dataZoom;
  var result = dataZoom.map(function (zoom) {
    var next = Object.assign({}, zoom);
    // 滚轮由统一的轴向处理器接管，避免 ECharts 默认行为同时改变两个方向。
    if (next.type === 'inside') {
      next.zoomOnMouseWheel = false;
      next.moveOnMouseWheel = false;
      next.moveOnMouseMove = false;
    }
    return next;
  });
  var hasX = result.some(function (zoom) { return zoom.xAxisIndex !== undefined; });
  var hasY = result.some(function (zoom) { return zoom.yAxisIndex !== undefined; });
  if (hasX && hasYAxis && !hasY) {
    result.push({
      id: 'qm-zoom-y-wheel', type: 'inside', yAxisIndex: 0,
      start: 0, end: 100, filterMode: 'none',
      zoomOnMouseWheel: false, moveOnMouseWheel: false, moveOnMouseMove: false,
    });
  }
  return result;
}

function enhanceOption(option) {
  if (!option || typeof option !== 'object') return option;
  var kind = chartKind(option);
  var count = chartPointCount(option);
  var profile = motionProfile(kind, count);
  var result = Object.assign({}, option, {
    color: PALETTE,
    backgroundColor: 'transparent',
    animation: profile.enabled,
    animationDuration: profile.enabled ? profile.duration : 0,
    animationDurationUpdate: profile.enabled ? profile.update : 0,
    animationEasing: 'cubicOut',
    animationEasingUpdate: 'cubicOut',
    stateAnimation: {duration: REDUCED_MOTION ? 0 : 180, easing: 'cubicOut'},
  });
  result.xAxis = normalizeAxis(option.xAxis);
  result.yAxis = normalizeAxis(option.yAxis);
  result.dataZoom = prepareDataZoom(option.dataZoom, Boolean(result.yAxis));
  if (Array.isArray(option.series)) {
    result.series = option.series.map(function (series, index) { return prepareSeries(series, index, profile); });
  }
  if (option.legend) {
    result.legend = Object.assign({}, option.legend, {
      textStyle: Object.assign({color: INK2, fontSize: 10}, option.legend.textStyle || {}),
      itemWidth: option.legend.itemWidth || 12,
      itemHeight: option.legend.itemHeight || 3,
    });
  }
  result.tooltip = Object.assign({
    trigger: 'axis', confine: true, appendToBody: false,
    backgroundColor: CHART_COLORS.surface, borderColor: AXIS, borderWidth: 1,
    padding: [8, 10], textStyle: {color: CHART_COLORS.ink, fontSize: 11},
    extraCssText: 'box-shadow:0 12px 30px rgba(0,0,0,.28);border-radius:6px;',
  }, option.tooltip || {});
  result.__qmProfile = profile;
  return result;
}

function baseOpt(extra) {
  return Object.assign({
    color: PALETTE,
    backgroundColor: 'transparent',
    textStyle: {color: INK2},
    grid: {left: 60, right: 20, top: 36, bottom: 40, containLabel: false},
    tooltip: {
      trigger: 'axis', axisPointer: {type: 'cross', label: {backgroundColor: AXIS}},
      backgroundColor: CHART_COLORS.surface, borderColor: AXIS,
      textStyle: {color: CHART_COLORS.ink, fontSize: 12},
    },
  }, extra || {});
}

function timeAxis() {
  return {
    type: 'time', boundaryGap: false,
    axisLine: {lineStyle: {color: AXIS}}, axisLabel: {color: MUTED, hideOverlap: true},
    axisTick: {show: false}, splitLine: {show: false},
  };
}

function valAxis(formatter) {
  return {
    type: 'value', scale: true,
    axisLine: {show: false}, axisTick: {show: false},
    axisLabel: {color: MUTED, formatter: formatter},
    splitLine: {lineStyle: {color: GRID}},
  };
}

var chartObservers = new Map();

function clamp(value, minimum, maximum) {
  return Math.min(maximum, Math.max(minimum, value));
}

function wheelDataWindow(chart, event) {
  if (REDUCED_MOTION || !chart || chart.isDisposed()) return;
  var option = chart.getOption ? chart.getOption() : null;
  var zooms = option && option.dataZoom;
  if (!Array.isArray(zooms) || !zooms.length) return;
  var horizontalDelta = event.shiftKey ? event.deltaY : event.deltaX;
  var useHorizontal = event.shiftKey || Math.abs(horizontalDelta) > Math.abs(event.deltaY);
  var axis = useHorizontal ? 'x' : 'y';
  var delta = useHorizontal ? horizontalDelta : event.deltaY;
  if (!delta) return;
  var zoom = zooms.find(function (item) {
    return item.type === 'inside' && item[axis + 'AxisIndex'] !== undefined;
  });
  if (!zoom) return;

  var dom = chart.getDom();
  var rect = dom.getBoundingClientRect();
  if (!rect.width || !rect.height) return;
  var position = axis === 'x'
    ? clamp((event.clientX - rect.left) / rect.width, 0, 1)
    : clamp(1 - (event.clientY - rect.top) / rect.height, 0, 1);
  var start = Number(zoom.start);
  var end = Number(zoom.end);
  start = Number.isFinite(start) ? start : 0;
  end = Number.isFinite(end) ? end : 100;
  var span = Math.max(1, end - start);
  var nextStart;
  var nextEnd;
  if (event.ctrlKey) {
    var factor = Math.pow(1.16, clamp(delta / 100, -2, 2));
    var nextSpan = clamp(span * factor, 4, 100);
    var anchor = start + span * position;
    nextStart = anchor - nextSpan * position;
    nextStart = clamp(nextStart, 0, 100 - nextSpan);
    nextEnd = nextStart + nextSpan;
  } else {
    // 普通滚轮沿轴平移：右移查看更晚数据，向下查看更低数值。
    var direction = axis === 'x' ? 1 : -1;
    var distance = span * clamp(delta / 100, -2, 2) * 0.16 * direction;
    nextStart = clamp(start + distance, 0, 100 - span);
    nextEnd = nextStart + span;
    // 全量视图或抵达边界时不拦截页面，避免滚动陷阱。
    if (Math.abs(nextStart - start) < 0.001) return;
  }
  event.preventDefault();
  chart.dispatchAction({
    type: 'dataZoom', dataZoomId: zoom.id,
    start: nextStart, end: nextEnd,
  });
}

function installChartLifecycle(id, chart) {
  var nativeSetOption = chart.setOption.bind(chart);
  chart.__qmNativeSetOption = nativeSetOption;
  chart.setOption = function (option, options) {
    var enhanced = enhanceOption(option);
    chart.__qmLastOption = option;
    chart.__qmProfile = enhanced && enhanced.__qmProfile;
    if (enhanced && enhanced.__qmProfile) delete enhanced.__qmProfile;
    var setOptions = options === undefined ? {replaceMerge: ['series']} : options;
    var result = nativeSetOption(enhanced, setOptions);
    decorateChart(id);
    var dom = chart.getDom();
    if (chart.__qmProfile && chart.__qmProfile.containerOnly && dom) {
      dom.classList.remove('qm-chart-fade');
      requestAnimationFrame(function () { dom.classList.add('qm-chart-fade'); });
    }
    return result;
  };
  chart.getDom().addEventListener('wheel', function (event) {
    wheelDataWindow(chart, event);
  }, {capture: true, passive: false});
  if (window.ResizeObserver) {
    var observer = new ResizeObserver(function () {
      if (!chart.isDisposed() && chart.getDom().offsetParent !== null) chart.resize();
    });
    observer.observe(chart.getDom());
    chartObservers.set(id, observer);
  }
}

function mkChart(id, reset) {
  var el = document.getElementById(id);
  if (!el || !window.echarts) return null;
  var existing = charts[id];
  if (existing && (existing.isDisposed() || existing.getDom() !== el)) {
    if (!existing.isDisposed()) existing.dispose();
    if (chartObservers.has(id)) chartObservers.get(id).disconnect();
    chartObservers.delete(id);
    delete charts[id];
    existing = null;
  }
  if (existing && !reset) return existing;
  if (existing && reset === true) {
    existing.clear();
    return existing;
  }
  var chart = window.echarts.getInstanceByDom(el) || window.echarts.init(el, 'quantmaster', {renderer: 'canvas'});
  charts[id] = chart;
  if (!chart.__qmNativeSetOption) installChartLifecycle(id, chart);
  return chart;
}

function disposeChart(id) {
  var chart = charts[id];
  if (chart && !chart.isDisposed()) chart.dispose();
  if (chartObservers.has(id)) chartObservers.get(id).disconnect();
  chartObservers.delete(id);
  delete charts[id];
}

function decorateChart(id) {
  var el = document.getElementById(id);
  if (!el || el.classList.contains('spark') || id.indexOf('spark-') === 0 || id === 'news-factor-chart') return;
  el.setAttribute('role', 'img');
  var frame = el.closest('.panel, .rotation-section, .trading-chart-block, .paper-nav-panel, .news-dashboard section');
  if (!frame) frame = el.parentElement;
  if (!frame) return;
  frame.classList.add('qm-chart-frame');
  var heading = frame.querySelector('h2, h3, h4');
  if (heading && !el.getAttribute('aria-label')) el.setAttribute('aria-label', heading.textContent.trim());
}

function captureInteraction(chart) {
  var option = chart.getOption ? chart.getOption() : {};
  return {
    dataZoom: (option.dataZoom || []).map(function (zoom) { return {start: zoom.start, end: zoom.end, startValue: zoom.startValue, endValue: zoom.endValue}; }),
    selected: option.legend && option.legend[0] ? option.legend[0].selected : null,
  };
}

function restoreInteraction(chart, state) {
  if (state.selected) chart.dispatchAction({type: 'legendSelect', name: Object.keys(state.selected).find(function (name) { return state.selected[name]; })});
  state.dataZoom.forEach(function (zoom, index) {
    chart.dispatchAction(Object.assign({type: 'dataZoom', dataZoomIndex: index}, zoom));
  });
}

function replayChart(id) {
  var chart = charts[id];
  if (!chart || chart.isDisposed() || !chart.__qmLastOption) return false;
  var state = captureInteraction(chart);
  var option = chart.__qmLastOption;
  var emptySeries = (option.series || []).map(function (series, index) {
    return {id: series.id || ('qm-' + String(series.name || series.type || 'series') + '-' + index), type: series.type, data: []};
  });
  chart.__qmNativeSetOption({animation: false, series: emptySeries}, {replaceMerge: ['series'], silent: true});
  requestAnimationFrame(function () {
    if (chart.isDisposed()) return;
    chart.setOption(option, {notMerge: false, lazyUpdate: false});
    restoreInteraction(chart, state);
  });
  return true;
}

function stageTab(tab) {
  var root = document.getElementById('tab-' + tab);
  if (!root) return;
  root.querySelectorAll('tbody').forEach(function (body) {
    Array.from(body.rows).forEach(function (row) {
      Array.from(row.cells).forEach(function (cell) {
        var value = cell.textContent.trim().replace(/[,，]/g, '');
        if (/^[+−-]?(?:\d+(?:\.\d+)?|\.\d+)(?:%|亿|万|元|股|pp)?$/.test(value)) cell.classList.add('qm-numeric');
      });
    });
  });
}

function stageAddedRows(node) {
  if (!(node instanceof Element)) return;
  var rows = node.matches('tbody tr') ? [node] : Array.from(node.querySelectorAll('tbody tr'));
  rows.forEach(function (row) {
    Array.from(row.cells).forEach(function (cell) {
      var value = cell.textContent.trim().replace(/[,，]/g, '');
      if (/^[+−-]?(?:\d+(?:\.\d+)?|\.\d+)(?:%|亿|万|元|股|pp)?$/.test(value)) cell.classList.add('qm-numeric');
    });
  });
}

function activateChartTab(tab) {
  var root = document.getElementById('tab-' + tab);
  if (!root) return;
  stageTab(tab);
  requestAnimationFrame(function () {
    Object.keys(charts).forEach(function (id) {
      var chart = charts[id];
      if (!chart || chart.isDisposed() || !root.contains(chart.getDom())) return;
      chart.resize();
    });
  });
}

if (window.MutationObserver) {
  new MutationObserver(function (mutations) {
    mutations.forEach(function (mutation) {
      mutation.addedNodes.forEach(stageAddedRows);
    });
  }).observe(document.documentElement, {childList: true, subtree: true});
}

motionPreference.addEventListener && motionPreference.addEventListener('change', function (event) {
  REDUCED_MOTION = event.matches;
  window.REDUCED_MOTION = REDUCED_MOTION;
});

var chartResizeFrame = 0;
window.addEventListener('resize', function () {
  if (chartResizeFrame) cancelAnimationFrame(chartResizeFrame);
  chartResizeFrame = requestAnimationFrame(function () {
    chartResizeFrame = 0;
    if (typeof window.syncDecisionDetailWidth === 'function') window.syncDecisionDetailWidth();
    Object.keys(charts).forEach(function (id) {
      var chart = charts[id];
      if (!chart || chart.isDisposed()) return;
      chart.resize();
      if (chart.__quantmasterKlineData && typeof window.renderKlineSeries === 'function') {
        window.renderKlineSeries(chart, chart.__quantmasterKlineData);
      }
    });
  });
});

window.QuantCharts = Object.freeze({
  colors: CHART_COLORS,
  motionProfile: motionProfile,
  enhanceOption: enhanceOption,
  replay: replayChart,
  activateTab: activateChartTab,
  stageTab: stageTab,
  dispose: disposeChart,
});
