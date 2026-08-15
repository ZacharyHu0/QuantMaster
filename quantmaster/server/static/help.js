const helpFeature = (() => {
  'use strict';

  const root = document.getElementById('help-root');
  const helpTab = document.getElementById('tab-help');
  if (!root || !helpTab) return;

  const TRADING_DAYS = Number(helpTab.dataset.tradingDays);
  const RISK_FREE = Number(helpTab.dataset.riskFree);
  const defaultTrade = {
    commission_rate: 0.00025,
    commission_min: 5,
    stamp_tax_rate: 0.0005,
    transfer_fee_rate: 0.00001,
    slippage: 0.001,
    lot_size: 100,
  };
  let tradeSettings = {...defaultTrade};
  let loadPromise = null;
  let ready = false;
  let observer = null;
  let currentRoute = {topic:'start', anchor:''};

  const escapeHTML = value => String(value)
    .replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;').replaceAll("'", '&#39;');

  function route() {
    return currentRoute;
  }

  function setRoute(topic, anchor = '') {
    currentRoute = {topic:topic || 'start', anchor:anchor || ''};
  }

  function topicElement(topic) {
    const safeTopic = CSS.escape(topic || 'start');
    return root.querySelector(`[data-help-topic="${safeTopic}"]`)
      || root.querySelector('[data-help-topic="start"]');
  }

  function selectTopic(topic) {
    const chapter = topicElement(topic);
    const actual = chapter?.dataset.helpTopic || 'start';
    const activePart = chapter?.dataset.helpPart || '';
    root.querySelectorAll('[data-help-link]').forEach(link => {
      const active = link.dataset.helpLink === actual;
      link.classList.toggle('active', active);
      if (active) link.setAttribute('aria-current', 'location');
      else link.removeAttribute('aria-current');
    });
    root.querySelectorAll('[data-help-nav-part]').forEach(part => {
      part.classList.toggle('active', part.dataset.helpNavPart === activePart);
    });
  }

  function navigateToRoute({behavior = 'auto', focus = false} = {}) {
    if (!ready) return;
    const current = route();
    const chapter = topicElement(current.topic);
    const target = current.anchor
      ? document.getElementById(current.anchor) || chapter
      : current.topic === 'start' ? root.querySelector('.help-masthead') : chapter;
    if (!chapter || !target) return;
    const actualTopic = chapter.dataset.helpTopic;
    if (actualTopic !== current.topic) setRoute(actualTopic, '', {replace: true});
    selectTopic(actualTopic);
    requestAnimationFrame(() => {
      target.scrollIntoView({behavior, block: 'start'});
      if (focus) {
        const heading = target.querySelector('h1, h2, h3');
        if (heading) {
          heading.setAttribute('tabindex', '-1');
          heading.focus({preventScroll: true});
        }
      }
    });
  }

  function showLoadError(error) {
    root.className = 'help-load-error';
    root.setAttribute('aria-busy', 'false');
    root.innerHTML = `<h2>手册暂时没有载入</h2><p>${escapeHTML(error?.message || '无法读取本地帮助文件。')} 请确认 QuantMaster 服务仍在运行，然后重试。</p><button class="primary" id="help-retry" type="button">重新载入手册</button>`;
    document.getElementById('help-retry')?.addEventListener('click', () => {
      loadPromise = null;
      loadHelp();
    });
  }

  async function fetchTradeSettings() {
    const status = document.getElementById('help-settings-status');
    try {
      const response = await fetch('/api/v1/settings', {headers: {'Accept': 'application/json'}});
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      const payload = await response.json();
      const incoming = payload?.trade;
      if (!incoming || typeof incoming !== 'object') throw new Error('响应中没有交易参数');
      tradeSettings = {...defaultTrade};
      Object.keys(defaultTrade).forEach(key => {
        const value = Number(incoming[key]);
        if (Number.isFinite(value)) tradeSettings[key] = value;
      });
      renderTradeFields();
      if (status) {
        status.className = 'help-settings-status ready';
        status.textContent = '已载入项目交易参数。下方参数可临时修改，但不会写回设置。';
      }
    } catch (_) {
      tradeSettings = {...defaultTrade};
      renderTradeFields();
      if (status) {
        status.className = 'help-settings-status warning';
        status.textContent = '未能读取项目设置，当前使用页面默认值。你仍可在各工具的“本次计算参数”中手动修改。';
      }
    }
  }

  const tradeFieldDefinitions = [
    ['commission_rate', '佣金率（%）', 100, '0.0001'],
    ['commission_min', '每笔最低佣金（元）', 1, '0.01'],
    ['stamp_tax_rate', '卖出印花税率（%）', 100, '0.0001'],
    ['transfer_fee_rate', '过户费率（%）', 100, '0.0001'],
    ['slippage', '单边滑点（%）', 100, '0.001'],
    ['lot_size', '每手股数', 1, '1'],
  ];

  function renderTradeFields() {
    root.querySelectorAll('.trade-parameter-fields').forEach(container => {
      const previous = {};
      container.querySelectorAll('[data-trade-key]').forEach(input => { previous[input.dataset.tradeKey] = input.value; });
      container.innerHTML = tradeFieldDefinitions.map(([key, label, scale, step]) => {
        const value = previous[key] ?? String(tradeSettings[key] * scale);
        return `<label>${label}<input type="number" min="0" step="${step}" value="${escapeHTML(value)}" data-trade-key="${key}" data-scale="${scale}"></label>`;
      }).join('');
    });
    calculateAll();
  }

  function tradeFor(form) {
    const values = {...tradeSettings};
    form.querySelectorAll('[data-trade-key]').forEach(input => {
      const parsed = Number(input.value);
      const scale = Number(input.dataset.scale) || 1;
      if (Number.isFinite(parsed) && parsed >= 0) values[input.dataset.tradeKey] = parsed / scale;
    });
    values.lot_size = Math.max(1, Math.round(values.lot_size));
    return values;
  }

  const number = (form, name) => Number(new FormData(form).get(name));
  const percent = value => `${(value * 100).toFixed(2)}%`;
  const money = value => Number.isFinite(value)
    ? new Intl.NumberFormat('zh-CN', {style: 'currency', currency: 'CNY', minimumFractionDigits: 2}).format(value)
    : '—';
  const decimal = (value, digits = 3) => Number.isFinite(value) ? value.toFixed(digits) : '—';

  function setError(form, message = '') {
    const target = form.querySelector('[data-error]');
    if (target) target.textContent = message;
    form.classList.toggle('has-error', Boolean(message));
  }

  function output(form, name, value) {
    const target = form.querySelector(`[data-output="${name}"]`);
    if (target) target.textContent = value;
  }

  function calculateCompound(form) {
    const start = number(form, 'start');
    const end = number(form, 'end');
    const days = number(form, 'days');
    if (!(start > 0) || !(end > 0) || !(days >= 1)) {
      setError(form, '期初、期末必须大于 0，交易日必须至少为 1。');
      ['total', 'annual'].forEach(key => output(form, key, '—'));
      return;
    }
    setError(form);
    const total = end / start - 1;
    const annual = (end / start) ** (TRADING_DAYS / days) - 1;
    output(form, 'total', percent(total));
    output(form, 'annual', percent(annual));
    form.querySelector('[data-formula]').textContent = `按 ${TRADING_DAYS} 个交易日年化：(${end} / ${start})^(${TRADING_DAYS} / ${days}) − 1`;
  }

  function buySummary(quote, shares, trade) {
    const executionPrice = quote * (1 + trade.slippage);
    const amount = executionPrice * shares;
    const fee = Math.max(amount * trade.commission_rate, trade.commission_min)
      + amount * trade.transfer_fee_rate;
    return {executionPrice, amount, fee, total: amount + fee};
  }

  function sellSummary(quote, shares, trade) {
    const executionPrice = quote * (1 - trade.slippage);
    const amount = executionPrice * shares;
    const fee = Math.max(amount * trade.commission_rate, trade.commission_min)
      + amount * (trade.stamp_tax_rate + trade.transfer_fee_rate);
    return {executionPrice, amount, fee, net: amount - fee};
  }

  function breakevenQuote(target, shares, trade, seed) {
    let low = 0;
    let high = Math.max(seed, 0.01);
    for (let i = 0; i < 40 && sellSummary(high, shares, trade).net < target; i += 1) high *= 2;
    if (sellSummary(high, shares, trade).net < target) return NaN;
    for (let i = 0; i < 80; i += 1) {
      const mid = (low + high) / 2;
      if (sellSummary(mid, shares, trade).net >= target) high = mid;
      else low = mid;
    }
    return high;
  }

  function calculateCost(form) {
    const buyPrice = number(form, 'buy_price');
    const sellPrice = number(form, 'sell_price');
    const shares = number(form, 'shares');
    const trade = tradeFor(form);
    if (!(buyPrice > 0) || !(sellPrice > 0) || !Number.isInteger(shares) || shares < 1 || trade.slippage >= 1) {
      setError(form, '买卖报价必须大于 0，股数必须是正整数，单边滑点必须小于 100%。');
      ['buy_total', 'sell_net', 'pnl', 'breakeven'].forEach(key => output(form, key, '—'));
      return;
    }
    setError(form);
    const buy = buySummary(buyPrice, shares, trade);
    const sell = sellSummary(sellPrice, shares, trade);
    const breakeven = breakevenQuote(buy.total, shares, trade, buyPrice);
    output(form, 'buy_total', money(buy.total));
    output(form, 'sell_net', money(sell.net));
    output(form, 'pnl', money(sell.net - buy.total));
    output(form, 'breakeven', Number.isFinite(breakeven) ? `${breakeven.toFixed(4)} 元` : '无法求解');
  }

  function calculatePosition(form) {
    const equity = number(form, 'equity');
    const riskPct = number(form, 'risk_pct') / 100;
    const entry = number(form, 'entry');
    const stop = number(form, 'stop');
    const capPct = number(form, 'cap_pct') / 100;
    const trade = tradeFor(form);
    if (!(equity > 0) || !(riskPct > 0 && riskPct <= 1) || !(entry > 0)
        || !(stop > 0 && stop < entry) || !(capPct > 0 && capPct <= 1) || trade.slippage >= 1) {
      setError(form, '请填写正数；止损报价必须低于入场报价，风险与最大仓位必须在 0% 到 100% 之间。');
      ['shares', 'cost', 'loss', 'actual_risk'].forEach(key => output(form, key, '—'));
      return;
    }
    const riskBudget = equity * riskPct;
    const capBudget = equity * capPct;
    const lot = trade.lot_size;
    const lossAt = shares => buySummary(entry, shares, trade).total - sellSummary(stop, shares, trade).net;
    const allowed = shares => buySummary(entry, shares, trade).total <= capBudget + 1e-8
      && lossAt(shares) <= riskBudget + 1e-8;
    let highLots = Math.max(1, Math.ceil(capBudget / (entry * (1 + trade.slippage) * lot)) + 1);
    let lowLots = 0;
    while (lowLots < highLots) {
      const mid = Math.ceil((lowLots + highLots) / 2);
      if (allowed(mid * lot)) lowLots = mid;
      else highLots = mid - 1;
    }
    const shares = lowLots * lot;
    const buy = shares ? buySummary(entry, shares, trade) : {total: 0};
    const loss = shares ? lossAt(shares) : 0;
    setError(form, shares ? '' : `在当前风险、仓位与 ${lot} 股整手约束下，无法建立一手仓位。`);
    output(form, 'shares', `${shares.toLocaleString('zh-CN')} 股`);
    output(form, 'cost', money(buy.total));
    output(form, 'loss', money(loss));
    output(form, 'actual_risk', percent(loss / equity));
  }

  function calculateDrawdown(form) {
    const drawdown = number(form, 'drawdown') / 100;
    if (!(drawdown >= 0 && drawdown < 1)) {
      setError(form, '回撤必须在 0%（含）到 100%（不含）之间。');
      output(form, 'recovery', '—');
      return;
    }
    setError(form);
    output(form, 'recovery', percent(drawdown / (1 - drawdown)));
  }

  function calculateSharpe(form) {
    const total = number(form, 'total_return') / 100;
    const days = number(form, 'days');
    const dailyVol = number(form, 'daily_vol') / 100;
    const riskFree = number(form, 'risk_free') / 100;
    if (!(total > -1) || !(days >= 1) || !(dailyVol > 0) || !Number.isFinite(riskFree)) {
      setError(form, '累计收益必须大于 −100%，交易日必须至少为 1，日波动率必须大于 0。');
      ['annual_return', 'annual_vol', 'sharpe'].forEach(key => output(form, key, '—'));
      return;
    }
    setError(form);
    const annualReturn = (1 + total) ** (TRADING_DAYS / days) - 1;
    const annualVol = dailyVol * Math.sqrt(TRADING_DAYS);
    output(form, 'annual_return', percent(annualReturn));
    output(form, 'annual_vol', percent(annualVol));
    output(form, 'sharpe', decimal((annualReturn - riskFree) / annualVol));
    form.querySelector('[data-formula]').textContent = `(${percent(annualReturn)} − ${percent(riskFree)}) / ${percent(annualVol)}；按 ${TRADING_DAYS} 个交易日年化。`;
  }

  function averageRanks(values) {
    const sorted = values.map((value, index) => ({value, index})).sort((a, b) => a.value - b.value);
    const ranks = Array(values.length);
    for (let start = 0; start < sorted.length;) {
      let end = start + 1;
      while (end < sorted.length && sorted[end].value === sorted[start].value) end += 1;
      const rank = (start + 1 + end) / 2;
      for (let i = start; i < end; i += 1) ranks[sorted[i].index] = rank;
      start = end;
    }
    return ranks;
  }

  function pearson(left, right) {
    const leftMean = left.reduce((sum, value) => sum + value, 0) / left.length;
    const rightMean = right.reduce((sum, value) => sum + value, 0) / right.length;
    let covariance = 0;
    let leftSquares = 0;
    let rightSquares = 0;
    for (let i = 0; i < left.length; i += 1) {
      const a = left[i] - leftMean;
      const b = right[i] - rightMean;
      covariance += a * b;
      leftSquares += a * a;
      rightSquares += b * b;
    }
    const denominator = Math.sqrt(leftSquares * rightSquares);
    return denominator > 0 ? covariance / denominator : NaN;
  }

  function calculateRankIC(form) {
    const raw = String(new FormData(form).get('pairs') || '').trim();
    const lines = raw ? raw.split(/\r?\n/).filter(line => line.trim()) : [];
    const pairs = [];
    for (const line of lines) {
      const values = line.trim().split(/[\s,，;；]+/).filter(Boolean).map(Number);
      if (values.length !== 2 || values.some(value => !Number.isFinite(value))) {
        setError(form, `“${line.trim().slice(0, 32)}”无法解析。每行请输入两个数字，例如：1.2, 0.03`);
        ['count', 'rankic', 'meaning'].forEach(key => output(form, key, '—'));
        return;
      }
      pairs.push(values);
    }
    if (pairs.length < 3 || pairs.length > 50) {
      setError(form, '请输入 3–50 对有效数字。');
      ['count', 'rankic', 'meaning'].forEach(key => output(form, key, '—'));
      return;
    }
    const correlation = pearson(averageRanks(pairs.map(pair => pair[0])), averageRanks(pairs.map(pair => pair[1])));
    if (!Number.isFinite(correlation)) {
      setError(form, '至少有一列是常数，排名没有变化，无法计算 RankIC。');
      output(form, 'count', String(pairs.length));
      output(form, 'rankic', '—');
      output(form, 'meaning', '无法判断');
      return;
    }
    setError(form);
    output(form, 'count', String(pairs.length));
    output(form, 'rankic', decimal(correlation, 4));
    const strength = Math.abs(correlation) < 0.1 ? '接近无排序关系' : correlation > 0 ? '因子越高，未来收益排名总体越高' : '因子越高，未来收益排名总体越低';
    output(form, 'meaning', strength);
  }

  function parsePairLines(form, name, {min, max, label, validatePair}) {
    const raw = String(new FormData(form).get(name) || '').trim();
    const lines = raw ? raw.split(/\r?\n/).filter(line => line.trim()) : [];
    const pairs = [];
    for (const line of lines) {
      const values = line.trim().split(/[\s,，;；]+/).filter(Boolean).map(Number);
      if (values.length !== 2 || values.some(value => !Number.isFinite(value)) || !validatePair(values)) {
        return {error: `“${line.trim().slice(0, 32)}”无法解析。${label}`, pairs: []};
      }
      pairs.push(values);
    }
    if (pairs.length < min || pairs.length > max) {
      return {error: `请输入 ${min}–${max} 对有效数字。`, pairs: []};
    }
    return {error: '', pairs};
  }

  function calculateOLS(form) {
    const parsed = parsePairLines(form, 'pairs', {
      min: 3,
      max: 50,
      label: '每行请输入两个有限数字，例如：1.2, 3.4',
      validatePair: () => true,
    });
    if (parsed.error) {
      setError(form, parsed.error);
      ['count', 'alpha', 'beta', 'r2', 'residual_corr'].forEach(key => output(form, key, '—'));
      return;
    }
    const x = parsed.pairs.map(pair => pair[0]);
    const y = parsed.pairs.map(pair => pair[1]);
    const xMean = x.reduce((sum, value) => sum + value, 0) / x.length;
    const yMean = y.reduce((sum, value) => sum + value, 0) / y.length;
    const centeredX = x.map(value => value - xMean);
    const centeredY = y.map(value => value - yMean);
    const xSquares = centeredX.reduce((sum, value) => sum + value * value, 0);
    const ySquares = centeredY.reduce((sum, value) => sum + value * value, 0);
    if (!(xSquares > 1e-14) || !(ySquares > 1e-14)) {
      setError(form, 'x 与 y 都必须包含变化；常数列无法估计有效回归与 R²。');
      ['count', 'alpha', 'beta', 'r2', 'residual_corr'].forEach(key => output(form, key, '—'));
      return;
    }
    const beta = centeredX.reduce((sum, value, index) => sum + value * centeredY[index], 0) / xSquares;
    const alpha = yMean - beta * xMean;
    const residuals = y.map((value, index) => value - alpha - beta * x[index]);
    const residualSquares = residuals.reduce((sum, value) => sum + value * value, 0);
    const residualCorrelation = residualSquares < 1e-20 ? 0 : pearson(x, residuals);
    setError(form);
    output(form, 'count', String(x.length));
    output(form, 'alpha', decimal(Math.abs(alpha) < 5e-9 ? 0 : alpha, 4));
    output(form, 'beta', decimal(beta, 4));
    output(form, 'r2', decimal(1 - residualSquares / ySquares, 4));
    const stableResidualCorrelation = Number.isFinite(residualCorrelation) && Math.abs(residualCorrelation) >= 5e-9
      ? residualCorrelation : 0;
    output(form, 'residual_corr', decimal(stableResidualCorrelation, 4));
  }

  function calculateFDR(form) {
    const raw = String(new FormData(form).get('p_values') || '').trim();
    const tokens = raw ? raw.split(/[\s,，;；]+/).filter(Boolean) : [];
    const pValues = tokens.map(Number);
    const threshold = number(form, 'threshold');
    const rows = form.querySelector('[data-fdr-rows]');
    if (pValues.length < 2 || pValues.length > 100 || pValues.some(value => !Number.isFinite(value) || value < 0 || value > 1)
        || !(threshold > 0 && threshold <= 1)) {
      setError(form, '请输入 2–100 个 0 到 1 之间的 p 值；FDR 阈值必须大于 0 且不超过 1。');
      ['count', 'passed', 'cutoff'].forEach(key => output(form, key, '—'));
      if (rows) rows.innerHTML = '';
      return;
    }
    const sorted = pValues.map((value, index) => ({value, index})).sort((a, b) => a.value - b.value);
    const qValues = Array(pValues.length);
    let running = 1;
    for (let index = sorted.length - 1; index >= 0; index -= 1) {
      running = Math.min(running, sorted[index].value * sorted.length / (index + 1));
      qValues[sorted[index].index] = Math.min(1, running);
    }
    const passed = qValues.map(value => value <= threshold + 1e-12);
    const cutoffValues = pValues.filter((_, index) => passed[index]);
    setError(form);
    output(form, 'count', String(pValues.length));
    output(form, 'passed', String(passed.filter(Boolean).length));
    output(form, 'cutoff', cutoffValues.length ? decimal(Math.max(...cutoffValues), 4) : '无');
    if (rows) {
      rows.innerHTML = pValues.map((value, index) => `<tr><td>${index + 1}</td><td>${decimal(value, 4)}</td><td>${decimal(qValues[index], 4)}</td><td><span class="lab-verdict ${passed[index] ? 'pass' : 'hold'}">${passed[index] ? '通过' : '保留'}</span></td></tr>`).join('');
    }
  }

  function calculateCalibration(form) {
    const parsed = parsePairLines(form, 'pairs', {
      min: 4,
      max: 200,
      label: '概率必须在 0 到 1 之间，结果只能是 0 或 1，例如：0.65, 1',
      validatePair: ([probability, outcome]) => probability >= 0 && probability <= 1 && (outcome === 0 || outcome === 1),
    });
    const rows = form.querySelector('[data-calibration-rows]');
    if (parsed.error) {
      setError(form, parsed.error);
      ['count', 'base_rate', 'brier', 'ece'].forEach(key => output(form, key, '—'));
      if (rows) rows.innerHTML = '';
      return;
    }
    const bins = Array.from({length: 10}, () => ({count: 0, probability: 0, outcome: 0}));
    let outcomeSum = 0;
    let squaredError = 0;
    parsed.pairs.forEach(([probability, outcome]) => {
      const bin = bins[Math.min(9, Math.floor(probability * 10))];
      bin.count += 1;
      bin.probability += probability;
      bin.outcome += outcome;
      outcomeSum += outcome;
      squaredError += (probability - outcome) ** 2;
    });
    let ece = 0;
    bins.forEach(bin => {
      if (!bin.count) return;
      const averageProbability = bin.probability / bin.count;
      const eventRate = bin.outcome / bin.count;
      ece += bin.count / parsed.pairs.length * Math.abs(averageProbability - eventRate);
    });
    setError(form);
    output(form, 'count', String(parsed.pairs.length));
    output(form, 'base_rate', percent(outcomeSum / parsed.pairs.length));
    output(form, 'brier', decimal(squaredError / parsed.pairs.length, 4));
    output(form, 'ece', decimal(ece, 4));
    if (rows) {
      rows.innerHTML = bins.map((bin, index) => {
        const averageProbability = bin.count ? bin.probability / bin.count : NaN;
        const eventRate = bin.count ? bin.outcome / bin.count : NaN;
        const difference = bin.count ? Math.abs(averageProbability - eventRate) : NaN;
        const upper = index === 9 ? '1.0]' : `${((index + 1) / 10).toFixed(1)})`;
        return `<tr><td>[${(index / 10).toFixed(1)}, ${upper}</td><td>${bin.count}</td><td>${bin.count ? percent(averageProbability) : '—'}</td><td>${bin.count ? percent(eventRate) : '—'}</td><td>${bin.count ? percent(difference) : '—'}</td></tr>`;
      }).join('');
    }
  }

  function calculatePortfolioRisk(form) {
    const weightA = number(form, 'weight_a') / 100;
    const weightB = number(form, 'weight_b') / 100;
    const volA = number(form, 'vol_a') / 100;
    const volB = number(form, 'vol_b') / 100;
    const correlation = number(form, 'correlation');
    if (!(weightA >= 0 && weightA <= 1) || !(weightB >= 0 && weightB <= 1)
        || Math.abs(weightA + weightB - 1) > 1e-6 || !(volA > 0) || !(volB > 0)
        || !(correlation >= -1 && correlation <= 1)) {
      setError(form, '两项权重必须在 0% 到 100% 之间且合计 100%；波动率必须大于 0，相关系数必须在 −1 到 1 之间。');
      ['covariance', 'portfolio_vol', 'diversification', 'risk_a', 'risk_b'].forEach(key => output(form, key, '—'));
      return;
    }
    const covariance = volA * volB * correlation;
    const componentA = weightA * (weightA * volA * volA + weightB * covariance);
    const componentB = weightB * (weightA * covariance + weightB * volB * volB);
    const variance = Math.max(0, componentA + componentB);
    const portfolioVol = Math.sqrt(variance);
    setError(form);
    output(form, 'covariance', decimal(covariance, 6));
    output(form, 'portfolio_vol', percent(portfolioVol));
    output(form, 'diversification', portfolioVol > 1e-12 ? decimal((weightA * volA + weightB * volB) / portfolioVol, 4) : '无法定义');
    output(form, 'risk_a', variance > 1e-14 ? percent(componentA / variance) : '无法定义');
    output(form, 'risk_b', variance > 1e-14 ? percent(componentB / variance) : '无法定义');
  }

  function calculate(form) {
    const container = form.closest('[data-calculator], [data-lab]');
    const calculator = container?.dataset.calculator || container?.dataset.lab;
    ({
      compound: calculateCompound,
      cost: calculateCost,
      position: calculatePosition,
      drawdown: calculateDrawdown,
      sharpe: calculateSharpe,
      rankic: calculateRankIC,
      ols: calculateOLS,
      fdr: calculateFDR,
      calibration: calculateCalibration,
      risk: calculatePortfolioRisk,
    })[calculator]?.(form);
  }

  function calculateAll() {
    root.querySelectorAll('.calculator form').forEach(calculate);
  }

  function setupCalculators() {
    const riskFree = root.querySelector('#calc-sharpe [name="risk_free"]');
    if (riskFree && !riskFree.value) riskFree.value = String(RISK_FREE * 100);
    root.querySelectorAll('.calculator form').forEach(form => {
      const error = form.querySelector('[data-error]');
      if (error) {
        if (!error.id) error.id = `${form.id}-error`;
        form.querySelectorAll('input, textarea').forEach(input => input.setAttribute('aria-describedby', error.id));
      }
      form.addEventListener('input', () => calculate(form));
      form.addEventListener('submit', event => event.preventDefault());
    });
    renderTradeFields();
    fetchTradeSettings();
  }

  function selectCode(code) {
    const selection = window.getSelection();
    if (!selection) return false;
    const range = document.createRange();
    range.selectNodeContents(code);
    selection.removeAllRanges();
    selection.addRange(range);
    return selection.toString().length > 0;
  }

  async function copyCode(button) {
    const code = button.closest('.help-code')?.querySelector('code');
    if (!code) return;
    const original = '复制代码';
    let message = '';
    try {
      if (!navigator.clipboard?.writeText) throw new Error('Clipboard API unavailable');
      await navigator.clipboard.writeText(code.textContent);
      message = '已复制';
    } catch (_) {
      message = selectCode(code) ? '已选中，请按 Ctrl+C' : '复制失败，请手动选择';
    }
    button.textContent = message;
    button.dataset.copyState = message === '已复制' ? 'success' : 'fallback';
    window.setTimeout(() => {
      button.textContent = original;
      delete button.dataset.copyState;
    }, 1800);
  }

  function setupCodeCopy() {
    root.querySelectorAll('[data-copy-code]').forEach(button => {
      button.addEventListener('click', () => copyCode(button));
    });
  }

  function excerpt(text, terms) {
    const clean = text.replace(/\s+/g, ' ').trim();
    const lower = clean.toLocaleLowerCase('zh-CN');
    const first = terms.map(term => lower.indexOf(term)).filter(index => index >= 0).sort((a, b) => a - b)[0] ?? 0;
    const start = Math.max(0, first - 38);
    const end = Math.min(clean.length, start + 118);
    return `${start ? '…' : ''}${clean.slice(start, end)}${end < clean.length ? '…' : ''}`;
  }

  function highlighted(text, terms) {
    if (!terms.length) return escapeHTML(text);
    const escapedTerms = terms.map(term => term.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'));
    const pattern = new RegExp(`(${escapedTerms.join('|')})`, 'gi');
    return escapeHTML(text).replace(pattern, '<mark class="help-search-mark">$1</mark>');
  }

  function setupSearch() {
    const input = document.getElementById('help-search-input');
    const clear = document.getElementById('help-search-clear');
    const results = document.getElementById('help-search-results');
    const units = Array.from(root.querySelectorAll('[data-search-unit]')).filter(element => element.id).map(element => {
      const chapter = element.closest('[data-help-topic]');
      return {
        element,
        id: element.id,
        topic: chapter?.dataset.helpTopic || 'start',
        chapter: chapter?.dataset.helpTitle || '',
        title: element.dataset.searchTitle || element.querySelector('h3')?.textContent || '',
        text: element.textContent.replace(/\s+/g, ' ').trim(),
      };
    });

    function search() {
      const query = input.value.trim();
      clear.hidden = !query;
      if (!query) {
        results.hidden = true;
        results.innerHTML = '';
        return;
      }
      const terms = query.toLocaleLowerCase('zh-CN').split(/\s+/).filter(Boolean);
      const matches = units.filter(unit => {
        const haystack = `${unit.chapter} ${unit.title} ${unit.text}`.toLocaleLowerCase('zh-CN');
        return terms.every(term => haystack.includes(term));
      }).slice(0, 12);
      results.hidden = false;
      if (!matches.length) {
        results.innerHTML = `<div class="help-search-empty"><strong>没有找到“${escapeHTML(query)}”</strong><p>试试更短的概念词，例如“成本”“复权”或“回撤”。</p></div>`;
        return;
      }
      results.innerHTML = `<p class="help-search-summary">找到 ${matches.length} 个相关条目</p><div class="help-search-list">${matches.map(unit => {
        const snippet = excerpt(unit.text, terms);
        return `<a class="help-search-result" href="#help/${encodeURIComponent(unit.topic)}/${encodeURIComponent(unit.id)}"><strong>${highlighted(unit.title, terms)}</strong><p>${highlighted(snippet, terms)}</p></a>`;
      }).join('')}</div>`;
    }

    let searchTimer = null;
    input.addEventListener('input', () => {
      window.clearTimeout(searchTimer);
      searchTimer = window.setTimeout(search, 150);
    });
    input.addEventListener('keydown', event => {
      if (event.key === 'Escape') {
        input.value = '';
        search();
      }
    });
    clear.addEventListener('click', () => {
      input.value = '';
      search();
      input.focus();
    });
  }

  function setupNavigation() {
    root.addEventListener('click', event => {
      const projectLink = event.target.closest('[data-help-tab]');
      if (projectLink) {
        document.dispatchEvent(new CustomEvent('quantmaster:navigate', {
          detail:{tab:projectLink.dataset.helpTab},
        }));
        return;
      }
      const helpLink = event.target.closest('a[href^="#help/"]');
      if (helpLink) {
        event.preventDefault();
        const [topic, anchor] = helpLink.hash.slice(6).split('/').map(part => {
          try { return decodeURIComponent(part); } catch (_) { return part; }
        });
        setRoute(topic, anchor);
        navigateToRoute({behavior: 'smooth', focus: true});
      }
      if (helpLink) root.querySelector('.help-mobile-toc')?.removeAttribute('open');
    });

    observeTopics();
  }

  function observeTopics() {
    if (observer || !('IntersectionObserver' in window)) return;
    observer = new IntersectionObserver(entries => {
      const visible = entries.filter(entry => entry.isIntersecting)
        .sort((a, b) => a.boundingClientRect.top - b.boundingClientRect.top)[0];
      if (visible?.target?.dataset.helpTopic) selectTopic(visible.target.dataset.helpTopic);
    }, {rootMargin: '-18% 0px -68% 0px', threshold: 0});
    root.querySelectorAll('[data-help-topic]').forEach(chapter => observer.observe(chapter));
  }

  function setup() {
    root.className = '';
    root.setAttribute('aria-busy', 'false');
    setupSearch();
    setupNavigation();
    setupCalculators();
    setupCodeCopy();
  }

  function loadHelp() {
    if (ready) {
      observeTopics();
      navigateToRoute();
      return Promise.resolve();
    }
    if (loadPromise) return loadPromise;
    root.setAttribute('aria-busy', 'true');
    loadPromise = fetch('/static/help-content.html', {headers: {'Accept': 'text/html'}, cache: 'no-cache'})
      .then(response => {
        if (!response.ok) throw new Error(`本地帮助文件返回 HTTP ${response.status}`);
        return response.text();
      })
      .then(html => {
        root.innerHTML = html;
        ready = true;
        setup();
        navigateToRoute();
      })
      .catch(error => {
        loadPromise = null;
        showLoadError(error);
    });
    return loadPromise;
  }

  function unmount() {
    observer?.disconnect();
    observer = null;
  }

  return {mount:loadHelp, unmount, refresh:loadHelp};
})();

export const {mount, unmount, refresh} = helpFeature;
