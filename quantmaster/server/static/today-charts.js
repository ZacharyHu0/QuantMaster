const COLORS = {
  primary: '#4f8fd8', up: '#e66767', down: '#24a06b', warning: '#c99642',
  neutral: '#aaa89f', danger: '#cf6d78', axis: '#41413e', surface: '#1a1a19',
  ink: '#f4f3ee', ink2: '#c3c2b7', muted: '#898781',
};
const instances = new Map();
const reducedMotion = matchMedia('(prefers-reduced-motion: reduce)');

function signed(value) {
  return `${value > 0 ? '+' : ''}${Number(value).toFixed(2)}%`;
}

function parsedDate(value) {
  const normalized = typeof value === 'string' && /^\d+$/.test(value) ? Number(value) : value;
  return new Date(normalized);
}

function shortDate(value) {
  const date = parsedDate(value);
  return Number.isNaN(date.getTime()) ? '—'
    : date.toLocaleDateString('zh-CN', {month: '2-digit', day: '2-digit'}).replaceAll('/', '.');
}

function month(value, includeYear = true) {
  const date = parsedDate(value);
  if (Number.isNaN(date.getTime())) return '—';
  const valueMonth = String(date.getMonth() + 1).padStart(2, '0');
  return includeYear ? `${date.getFullYear()}.${valueMonth}` : `${valueMonth}月`;
}

function dispose(root) {
  const instance = instances.get(root);
  if (!instance) return;
  instance.observer?.disconnect();
  if (instance.resize) window.removeEventListener('resize', instance.resize);
  if (instance.frame) cancelAnimationFrame(instance.frame);
  root.replaceChildren();
  instances.delete(root);
}

function surface(root, paint, hitTest) {
  dispose(root);
  const canvas = document.createElement('canvas');
  canvas.setAttribute('aria-hidden', 'true');
  const tooltip = hitTest ? document.createElement('span') : null;
  if (tooltip) {
    tooltip.className = 'native-chart-tooltip';
    tooltip.hidden = true;
    tooltip.setAttribute('role', 'status');
  }
  root.replaceChildren(canvas, ...(tooltip ? [tooltip] : []));
  let width = 0;
  let height = 0;
  let hover = -1;

  const draw = () => {
    const context = canvas.getContext('2d');
    context.clearRect(0, 0, width, height);
    paint(context, width, height, hover);
  };
  const resize = () => {
    const bounds = root.getBoundingClientRect();
    width = Math.max(1, bounds.width);
    height = Math.max(1, bounds.height);
    const ratio = Math.max(1, window.devicePixelRatio || 1);
    canvas.width = Math.round(width * ratio);
    canvas.height = Math.round(height * ratio);
    canvas.style.width = `${width}px`;
    canvas.style.height = `${height}px`;
    canvas.getContext('2d').setTransform(ratio, 0, 0, ratio, 0, 0);
    draw();
  };
  const instance = {canvas, draw, resize, observer: null, frame: 0};
  if (window.ResizeObserver) {
    instance.observer = new ResizeObserver(resize);
    instance.observer.observe(root);
  } else {
    window.addEventListener('resize', resize);
  }
  if (tooltip) {
    canvas.addEventListener('pointermove', event => {
      const bounds = canvas.getBoundingClientRect();
      const hit = hitTest(event.clientX - bounds.left, event.clientY - bounds.top, width, height);
      hover = hit?.index ?? -1;
      tooltip.hidden = !hit;
      if (hit) {
        tooltip.textContent = hit.text;
        tooltip.style.left = `${Math.max(10, Math.min(90, hit.left * 100))}%`;
      }
      draw();
    });
    canvas.addEventListener('pointerleave', () => {
      hover = -1;
      tooltip.hidden = true;
      draw();
    });
  }
  instances.set(root, instance);
  resize();
  return instance;
}

function linePath(context, points) {
  context.beginPath();
  points.forEach(([x, y], index) => index ? context.lineTo(x, y) : context.moveTo(x, y));
}

export function renderMarketSpark(root, item, changeSeries) {
  if (!root) return;
  const dates = changeSeries.map(point => point[0]);
  const values = changeSeries.map(point => Number(point[1]));
  const minimum = Math.min(...values, 0);
  const maximum = Math.max(...values, 0);
  const span = Math.max(.2, maximum - minimum);
  const padding = Math.max(.12, span * .16);
  const low = minimum - padding;
  const high = maximum + padding;
  const tone = values.at(-1) > 0 ? COLORS.up : values.at(-1) < 0 ? COLORS.down : COLORS.neutral;
  const daily = values.map((value, index) => {
    if (!index) return null;
    const previous = 1 + values[index - 1] / 100;
    return previous ? ((1 + value / 100) / previous - 1) * 100 : null;
  });
  const grid = {left: 22, right: 22, top: 8, bottom: 19};
  const geometry = (width, height) => values.map((value, index) => [
    grid.left + (width - grid.left - grid.right) * (values.length < 2 ? .5 : index / (values.length - 1)),
    grid.top + (height - grid.top - grid.bottom) * (high - value) / (high - low),
  ]);
  const hitTest = (x, _y, width) => {
    if (!values.length) return null;
    const plotWidth = Math.max(1, width - grid.left - grid.right);
    const index = Math.max(0, Math.min(values.length - 1,
      Math.round((x - grid.left) / plotWidth * (values.length - 1))));
    return {
      index, left: (grid.left + plotWidth * index / Math.max(1, values.length - 1)) / width,
      text: `${shortDate(dates[index])} · 区间涨跌 ${signed(values[index])} · 当日涨跌 ${daily[index] == null ? '—' : signed(daily[index])}`,
    };
  };
  const instance = surface(root, (context, width, height, hover) => {
    if (!values.length) return;
    const points = geometry(width, height);
    const zeroY = grid.top + (height - grid.top - grid.bottom) * high / (high - low);
    context.save();
    context.setLineDash([3, 3]);
    context.strokeStyle = 'rgba(195,194,183,.32)';
    context.beginPath(); context.moveTo(grid.left, zeroY); context.lineTo(width - grid.right, zeroY); context.stroke();
    context.restore();
    linePath(context, points);
    context.lineTo(points.at(-1)[0], zeroY); context.lineTo(points[0][0], zeroY); context.closePath();
    context.globalAlpha = .1; context.fillStyle = tone; context.fill(); context.globalAlpha = 1;
    linePath(context, points);
    context.strokeStyle = tone; context.lineWidth = 2; context.lineCap = 'round'; context.lineJoin = 'round'; context.stroke();
    const [lastX, lastY] = points.at(-1);
    context.fillStyle = tone; context.beginPath(); context.arc(lastX, lastY, 3.5, 0, Math.PI * 2); context.fill();
    context.fillStyle = COLORS.muted; context.font = '9px sans-serif'; context.textBaseline = 'bottom';
    const middle = Math.round((dates.length - 1) / 2);
    const labels = new Set();
    [[0, 'left'], [middle, 'center'], [dates.length - 1, 'right']].forEach(([index, align]) => {
      context.textAlign = align;
      const label = index === 0 || index === dates.length - 1
        ? month(dates[index], true) : month(dates[index], parsedDate(dates[index]).getFullYear() !== parsedDate(dates[0]).getFullYear());
      if (labels.has(label)) return;
      labels.add(label);
      context.fillText(label, points[index][0], height);
    });
    if (hover >= 0) {
      const [x, y] = points[hover];
      context.save(); context.setLineDash([2, 2]); context.strokeStyle = COLORS.axis;
      context.beginPath(); context.moveTo(x, grid.top); context.lineTo(x, height - grid.bottom); context.stroke(); context.restore();
      context.fillStyle = tone; context.beginPath(); context.arc(x, y, 3, 0, Math.PI * 2); context.fill();
    }
  }, hitTest);
  instance.canvas.dataset.nativeChart = 'market-spark';
  root.setAttribute('aria-label', `${item.name}区间走势图，最新区间涨跌 ${signed(values.at(-1) || 0)}`);
}

export function renderFearGreedHistory(root, data) {
  if (!root) return;
  const history = (data?.history || []).filter(point => point?.date && Number.isFinite(Number(point.score)));
  const grid = {left: 38, right: 14, top: 18, bottom: 28};
  const pointsFor = (width, height) => history.map((point, index) => [
    grid.left + (width - grid.left - grid.right) * (history.length < 2 ? .5 : index / (history.length - 1)),
    grid.top + (height - grid.top - grid.bottom) * (100 - Number(point.score)) / 100,
  ]);
  surface(root, (context, width, height, hover) => {
    const bottom = height - grid.bottom;
    context.font = '9px sans-serif'; context.fillStyle = COLORS.muted; context.textAlign = 'right';
    [0, 25, 50, 75, 100].forEach(value => {
      const y = grid.top + (bottom - grid.top) * (100 - value) / 100;
      context.fillText(String(value), grid.left - 6, y + 3);
    });
    const thresholdY = grid.top + (bottom - grid.top) * .9;
    context.save(); context.setLineDash([4, 3]); context.strokeStyle = COLORS.warning;
    context.beginPath(); context.moveTo(grid.left, thresholdY); context.lineTo(width - grid.right, thresholdY); context.stroke(); context.restore();
    context.textAlign = 'left'; context.fillStyle = COLORS.warning; context.fillText('≤10 · 罕见恐惧', grid.left + 4, thresholdY - 4);
    if (!history.length) { context.fillStyle = COLORS.muted; context.textAlign = 'center'; context.fillText('历史数据暂缺', width / 2, height / 2); return; }
    const points = pointsFor(width, height);
    linePath(context, points); context.lineTo(points.at(-1)[0], bottom); context.lineTo(points[0][0], bottom); context.closePath();
    context.globalAlpha = .08; context.fillStyle = COLORS.primary; context.fill(); context.globalAlpha = 1;
    linePath(context, points); context.strokeStyle = COLORS.primary; context.lineWidth = 2; context.stroke();
    context.fillStyle = COLORS.muted; context.textBaseline = 'bottom';
    context.textAlign = 'left'; context.fillText(shortDate(history[0].date), grid.left, height);
    context.textAlign = 'right'; context.fillText(shortDate(history.at(-1).date), width - grid.right, height);
    if (hover >= 0) { const [x, y] = points[hover]; context.fillStyle = COLORS.primary; context.beginPath(); context.arc(x, y, 3, 0, Math.PI * 2); context.fill(); }
  }, (x, _y, width) => {
    if (!history.length) return null;
    const index = Math.max(0, Math.min(history.length - 1, Math.round((x - grid.left) / Math.max(1, width - grid.left - grid.right) * (history.length - 1))));
    return {index, left: x / width, text: `${shortDate(history[index].date)} · 恐贪指数 ${Number(history[index].score).toFixed(1)}`};
  });
}

export function renderFearGreedGauge(root, data) {
  if (!root) return;
  const target = Number.isFinite(Number(data?.score)) ? Math.max(0, Math.min(100, Number(data.score))) : null;
  const animated = target != null && !reducedMotion.matches;
  let value = animated ? 0 : target ?? 0;
  const drawGauge = (context, width, height) => {
    const cx = width * .5, cy = height * .54, radius = Math.min(width, height) * .36;
    context.lineWidth = 7; context.lineCap = 'round';
    for (let score = 0; score < 100; score += 1) {
      const start = (210 - score * 2.4) * Math.PI / 180;
      const end = (210 - (score + 1.2) * 2.4) * Math.PI / 180;
      context.strokeStyle = score < 25 ? COLORS.danger : score < 45 ? COLORS.warning : score < 55 ? COLORS.neutral : score < 75 ? COLORS.primary : COLORS.down;
      context.beginPath(); context.arc(cx, cy, radius, -start, -end); context.stroke();
    }
    context.fillStyle = COLORS.muted; context.font = '600 13px sans-serif'; context.textAlign = 'center'; context.textBaseline = 'middle';
    [0, 25, 45, 55, 75, 100].forEach(score => {
      const angle = (210 - score * 2.4) * Math.PI / 180;
      context.fillText(String(score), cx + Math.cos(angle) * (radius + 17), cy - Math.sin(angle) * (radius + 17));
    });
    const angle = (210 - value * 2.4) * Math.PI / 180;
    const tone = (target ?? 0) < 25 ? COLORS.danger : (target ?? 0) < 45 ? COLORS.warning : (target ?? 0) < 55 ? COLORS.neutral : (target ?? 0) < 75 ? COLORS.primary : COLORS.down;
    context.strokeStyle = tone; context.lineWidth = 3; context.beginPath(); context.moveTo(cx, cy); context.lineTo(cx + Math.cos(angle) * radius * .56, cy - Math.sin(angle) * radius * .56); context.stroke();
    context.fillStyle = COLORS.surface; context.strokeStyle = tone; context.lineWidth = 2; context.beginPath(); context.arc(cx, cy, 5, 0, Math.PI * 2); context.fill(); context.stroke();
    context.fillStyle = target == null ? COLORS.muted : COLORS.ink; context.font = '720 30px sans-serif'; context.fillText(target == null ? '—' : value.toFixed(1), cx, cy + radius * .42);
    context.fillStyle = COLORS.ink2; context.font = '600 14px sans-serif'; context.fillText(data?.rating_label || '暂不可用', cx, cy + radius * .94);
  };
  const instance = surface(root, drawGauge);
  if (animated) {
    const started = performance.now();
    const animate = now => {
      const progress = Math.min(1, (now - started) / 640);
      value = target * (progress < .5 ? 4 * progress ** 3 : 1 - (-2 * progress + 2) ** 3 / 2);
      instance.draw();
      if (progress < 1) instance.frame = requestAnimationFrame(animate);
    };
    instance.frame = requestAnimationFrame(animate);
  }
}

export function disposeTodayCharts(scope = document) {
  for (const root of Array.from(instances.keys())) {
    if (root === scope || scope.contains(root)) dispose(root);
  }
}
