(() => {
  'use strict';

  const btState = {activeId: '', runs: [], selected: new Set(), prompted: new Set(), timer: 0};
  const paperState = {activeId: '', accounts: [], includeArchived: false, activeReport: null};

  const escapeHtml = value => typeof window.esc === 'function'
    ? window.esc(String(value ?? ''))
    : String(value ?? '').replace(/[&<>'"]/g, char => ({
      '&': '&amp;', '<': '&lt;', '>': '&gt;', "'": '&#39;', '"': '&quot;',
    })[char]);
  const number = value => value == null || !Number.isFinite(Number(value))
    ? '—' : Number(value).toLocaleString('zh-CN', {maximumFractionDigits: 2});
  const percent = value => value == null || !Number.isFinite(Number(value))
    ? '—' : `${(Number(value) * 100).toFixed(2)}%`;
  const signedClass = value => Number(value) > 0 ? 'up' : Number(value) < 0 ? 'down' : '';
  const statusLabel = {
    queued: '排队中', running: '运行中', interrupted: '恢复中', completed: '已完成',
    failed: '失败', cancelled: '已取消', proposed: '待确认', confirmed: '待开盘',
    blocked: '部分受阻', superseded: '已替代', filled: '已成交', skipped: '无需调整',
    created: '已创建', accepted: '已接受', open: '可撮合', partially_filled: '部分成交',
    waiting_market_open: '等待开市', waiting_price: '等待价格', waiting_market_data: '等待行情',
    expired: '已过期', rejected: '已拒绝', retry_wait: '等待重试', idle: '空闲',
    leased: '已领取', stalled: '已卡死', orphaned: '无主任务', manual_recovery: '待人工处理',
    active: '运行中', paused: '已暂停', archived: '已删除', auto: '自动', manual: '手动',
  };
  const orderReason = {
    missing_open: '缺少开盘价', limit_up: '涨停无法买入', limit_down: '跌停无法卖出',
    suspended: '停牌无法成交', missing_actual_limit: '缺少真实涨跌停价',
    insufficient_cash: '现金不足', t_plus_one: 'T+1 可卖数量不足', newer_cycle: '被新提案替代',
    strategy_changed: '策略修改后已替代',
  };
  const strategyLabel = strategy => {
    if (!strategy) return '历史策略未知';
    if (strategy?.kind === 'decision') {
      const profile = {
        risk_adjusted:'扣费风险收益', short_term:'短期命中收益', stable:'稳定可解释',
      }[strategy.profile] || strategy.profile;
      return `Hybrid v2 · ${profile} · ${strategy.holding_days} 日`;
    }
    if (strategy?.kind === 'swing') return '旧 Swing · 只读';
    if (strategy?.kind === 'lab_version') return `Lab OOF · ${strategy.horizon} 日 · ${String(strategy.version_id).slice(0, 8)}`;
    return strategy?.factor || '因子策略';
  };

  async function mutate(path, method = 'POST', body) {
    return window.QuantMasterAPI(path, {
      method, cache: 'no-store',
      headers: {'Content-Type': 'application/json'},
      body: body === undefined ? undefined : JSON.stringify(body),
    });
  }

  function setButtonBusy(button, busy, busyLabel = '处理中…') {
    if (!button) return;
    if (busy) {
      button.dataset.label = button.textContent;
      button.textContent = busyLabel;
      button.disabled = true;
      button.setAttribute('aria-busy', 'true');
    } else {
      button.textContent = button.dataset.label || button.textContent;
      button.disabled = false;
      button.removeAttribute('aria-busy');
    }
  }

  function renderError(container, error, context = '') {
    container.innerHTML = `<div class="trading-error" role="alert">${context ? `${escapeHtml(context)}：` : ''}${escapeHtml(error?.message || error)}</div>`;
  }

  function renderWarnings(warnings) {
    if (!warnings?.length) return '';
    return `<div class="trading-warning-list">${warnings.map(item => {
      const message = typeof item === 'string' ? item : item.message;
      return `<div class="trading-warning">${escapeHtml(message)}</div>`;
    }).join('')}</div>`;
  }

  /* ---------------- 回测 ---------------- */
  const btForm = document.getElementById('bt-form');
  const btOut = document.getElementById('bt-out');
  const btJob = document.getElementById('bt-job-state');
  const btHistory = document.getElementById('bt-history');
  const btCompare = document.getElementById('bt-compare');
  const factorCompletion = document.querySelector('[data-factor-completion]');
  const factorInput = document.getElementById('bt-factor-input');
  const factorMenu = document.getElementById('bt-factor-options');
  const factorTrigger = factorCompletion?.querySelector('.factor-completion-trigger');
  const nativeFactorPopover = typeof factorMenu?.showPopover === 'function';
  const factorStatusLabel = {
    draft:'草稿', validating:'验证中', candidate:'候选', approved:'已批准',
    production:'生产', degraded:'降级', archived:'已归档',
  };
  let factorCatalog = Array.isArray(window.QuantMasterFactorCatalog)
    ? window.QuantMasterFactorCatalog : [];
  let factorMatches = [];
  let factorActiveIndex = -1;
  let factorCatalogLoading = false;
  let factorCatalogError = '';
  let factorPositionFrame = 0;

  if (factorMenu && !nativeFactorPopover) factorMenu.hidden = true;

  const factorKey = value => String(value || '').normalize('NFKC')
    .trim().replace(/\s+/g, ' ').toLocaleLowerCase('zh-CN');

  function currentFactorToken() {
    const value = factorInput?.value || '';
    const caret = factorInput?.selectionStart ?? value.length;
    const start = value.lastIndexOf(',', Math.max(0, caret - 1)) + 1;
    const nextComma = value.indexOf(',', caret);
    const end = nextComma < 0 ? value.length : nextComma;
    return {value, start, end, query:value.slice(start, end).trim()};
  }

  function factorMenuIsOpen() {
    if (!factorMenu) return false;
    return nativeFactorPopover
      ? factorMenu.matches(':popover-open')
      : !factorMenu.hidden;
  }

  function positionFactorCompletion() {
    if (!factorMenuIsOpen() || !factorInput) return;
    const rect = factorInput.getBoundingClientRect();
    const viewportPadding = 12, gap = 6;
    const width = Math.min(
      Math.max(rect.width, 420), window.innerWidth - viewportPadding * 2,
    );
    const left = Math.min(
      Math.max(viewportPadding, rect.left), window.innerWidth - width - viewportPadding,
    );
    const below = window.innerHeight - rect.bottom - gap - viewportPadding;
    const above = rect.top - gap - viewportPadding;
    const opensAbove = below < 220 && above > below;
    const available = Math.max(120, Math.min(310, opensAbove ? above : below));
    const options = factorMenu.querySelector('.factor-completion-options');
    if (options) options.style.maxHeight = `${available}px`;
    factorMenu.style.width = `${width}px`;
    factorMenu.style.left = `${left}px`;
    factorMenu.classList.toggle('opens-above', opensAbove);
    factorMenu.style.top = opensAbove
      ? `${Math.max(viewportPadding, rect.top - gap - factorMenu.offsetHeight)}px`
      : `${rect.bottom + gap}px`;
  }

  function queueFactorCompletionPosition() {
    if (!factorMenuIsOpen() || factorPositionFrame) return;
    factorPositionFrame = window.requestAnimationFrame(() => {
      factorPositionFrame = 0;
      positionFactorCompletion();
    });
  }

  function openFactorCompletion() {
    if (!factorMenu || !factorInput || factorInput.disabled) return;
    if (!factorMenuIsOpen()) {
      if (nativeFactorPopover) factorMenu.showPopover();
      else factorMenu.hidden = false;
    }
    factorInput.setAttribute('aria-expanded', 'true');
    factorTrigger?.setAttribute('aria-expanded', 'true');
    positionFactorCompletion();
    queueFactorCompletionPosition();
  }

  function closeFactorCompletion() {
    if (!factorMenu || !factorInput) return;
    if (nativeFactorPopover && factorMenuIsOpen()) factorMenu.hidePopover();
    else if (!nativeFactorPopover) factorMenu.hidden = true;
    factorInput.setAttribute('aria-expanded', 'false');
    factorTrigger?.setAttribute('aria-expanded', 'false');
    factorInput.removeAttribute('aria-activedescendant');
    factorActiveIndex = -1;
  }

  function setFactorActive(index, scroll = true) {
    if (!factorMatches.length) return;
    factorActiveIndex = (index + factorMatches.length) % factorMatches.length;
    factorMenu.querySelectorAll('[role="option"]').forEach((option, optionIndex) => {
      const active = optionIndex === factorActiveIndex;
      option.classList.toggle('active', active);
      option.setAttribute('aria-selected', String(active));
      if (active) {
        factorInput.setAttribute('aria-activedescendant', option.id);
        if (scroll) option.scrollIntoView({block:'nearest'});
      }
    });
  }

  function filteredFactorCatalog(query) {
    const token = currentFactorToken();
    const selected = new Set([
      ...token.value.slice(0, token.start).split(','),
      ...token.value.slice(token.end).split(','),
    ].map(factorKey).filter(Boolean));
    const normalizedQuery = factorKey(query);
    const unique = new Map();
    factorCatalog.forEach(item => {
      const name = String(item?.name || '').trim();
      if (!name) return;
      const nameKey = factorKey(name), slugKey = factorKey(item.slug || '');
      if (selected.has(nameKey) || (slugKey && selected.has(slugKey))) return;
      const haystack = factorKey(`${name} ${item.slug || ''} ${item.description || ''} ${item.category || ''}`);
      if (normalizedQuery && !haystack.includes(normalizedQuery)) return;
      const score = !normalizedQuery ? 4
        : nameKey === normalizedQuery || slugKey === normalizedQuery ? 0
        : nameKey.startsWith(normalizedQuery) ? 1
        : slugKey.startsWith(normalizedQuery) ? 2 : 3;
      const previous = unique.get(nameKey);
      if (!previous || score < previous.score) unique.set(nameKey, {item, score});
    });
    return [...unique.values()]
      .sort((left, right) => left.score - right.score
        || String(left.item.name).localeCompare(
          String(right.item.name), 'zh-CN', {numeric:true, sensitivity:'base'},
        ))
      .slice(0, 16).map(entry => entry.item);
  }

  function renderFactorCompletion() {
    if (!factorMenu || !factorInput || factorInput.disabled) {
      closeFactorCompletion();
      return;
    }
    const {query} = currentFactorToken();
    factorMatches = filteredFactorCatalog(query);
    if (factorCatalogLoading && !factorCatalog.length) {
      factorMenu.innerHTML = '<div class="factor-completion-empty">正在载入因子目录…</div>';
      factorActiveIndex = -1;
    } else if (!factorMatches.length) {
      const message = factorCatalogError
        ? '候选目录暂时不可用，仍可手动输入名称或表达式'
        : `没有匹配“${escapeHtml(query)}”的因子；仍可手动输入表达式`;
      factorMenu.innerHTML = `<div class="factor-completion-empty">${message}</div>`;
      factorActiveIndex = -1;
    } else {
      factorMenu.innerHTML = `<div class="factor-completion-options">${factorMatches.map((item, index) => {
        const lab = item.source === 'quant_lab';
        const meta = lab
          ? `Quant Lab${item.status ? ` · ${factorStatusLabel[item.status] || item.status}` : ''}`
          : '内置因子';
        const secondary = item.description || item.category || item.slug || '可执行因子';
        return `<div id="bt-factor-option-${index}" class="factor-completion-option" role="option" aria-selected="false" data-factor-option="${index}">
          <span class="factor-completion-kind" aria-hidden="true">${lab ? 'Q' : 'ƒ'}</span>
          <span class="factor-completion-copy"><b>${escapeHtml(item.name)}</b><small>${escapeHtml(secondary)}</small></span>
          <span class="factor-completion-meta">${escapeHtml(meta)}</span>
        </div>`;
      }).join('')}</div>`;
      setFactorActive(0, false);
    }
    openFactorCompletion();
  }

  function insertFactorMatch(index, deferClose = false) {
    const item = factorMatches[index];
    if (!item || !factorInput) return;
    const token = currentFactorToken();
    const prefix = token.value.slice(0, token.start);
    const suffix = token.value.slice(token.end);
    const spacer = token.start > 0 && !/\s$/.test(prefix) ? ' ' : '';
    const replacement = `${spacer}${item.name}`;
    factorInput.value = `${prefix}${replacement}${suffix}`;
    const caret = prefix.length + replacement.length;
    factorInput.setSelectionRange(caret, caret);
    factorInput.dispatchEvent(new Event('change', {bubbles:true}));
    factorInput.focus();
    if (deferClose) window.setTimeout(closeFactorCompletion, 0);
    else closeFactorCompletion();
  }

  function useFactorCatalog(items) {
    factorCatalog = Array.isArray(items) ? items : [];
    factorCatalogError = '';
    if (document.activeElement === factorInput) renderFactorCompletion();
  }

  async function loadFactorCatalog() {
    if (factorCatalogLoading) return;
    factorCatalogLoading = true;
    if (document.activeElement === factorInput) renderFactorCompletion();
    try {
      const payload = await api('/api/v1/research/factors', {cache:'no-store'});
      useFactorCatalog(payload.factors || []);
      window.QuantMasterFactorCatalog = factorCatalog;
    } catch (error) {
      factorCatalogError = error?.message || '因子目录加载失败';
      if (document.activeElement === factorInput) renderFactorCompletion();
    } finally {
      factorCatalogLoading = false;
      if (document.activeElement === factorInput) renderFactorCompletion();
    }
  }

  if (factorInput && factorMenu) {
    factorInput.addEventListener('focus', () => {
      renderFactorCompletion();
      if (!factorCatalog.length) loadFactorCatalog();
    });
    factorInput.addEventListener('click', renderFactorCompletion);
    factorInput.addEventListener('input', renderFactorCompletion);
    factorInput.addEventListener('keydown', event => {
      if (event.key === 'ArrowDown' || event.key === 'ArrowUp') {
        event.preventDefault();
        if (!factorMenuIsOpen()) renderFactorCompletion();
        setFactorActive(factorActiveIndex + (event.key === 'ArrowDown' ? 1 : -1));
      } else if ((event.key === 'Enter' || event.key === 'Tab')
          && factorMenuIsOpen() && factorActiveIndex >= 0) {
        event.preventDefault();
        insertFactorMatch(factorActiveIndex);
      } else if (event.key === 'Escape' && factorMenuIsOpen()) {
        event.preventDefault();
        closeFactorCompletion();
      }
    });
    factorTrigger?.addEventListener('pointerdown', event => event.preventDefault());
    factorTrigger?.addEventListener('click', () => {
      if (factorMenuIsOpen()) {
        closeFactorCompletion();
        return;
      }
      factorInput.focus({preventScroll:true});
      renderFactorCompletion();
    });
    factorInput.addEventListener('blur', () => window.setTimeout(() => {
      if (!factorCompletion?.contains(document.activeElement)) closeFactorCompletion();
    }, 0));
    factorMenu.addEventListener('pointerdown', event => event.preventDefault());
    factorMenu.addEventListener('mousemove', event => {
      const option = event.target.closest('[data-factor-option]');
      if (option) setFactorActive(Number(option.dataset.factorOption), false);
    });
    factorMenu.addEventListener('click', event => {
      const option = event.target.closest('[data-factor-option]');
      if (option) {
        event.preventDefault();
        insertFactorMatch(Number(option.dataset.factorOption), true);
      }
    });
    factorMenu.addEventListener('toggle', () => {
      const open = factorMenuIsOpen();
      factorInput.setAttribute('aria-expanded', String(open));
      factorTrigger?.setAttribute('aria-expanded', String(open));
      if (open) queueFactorCompletionPosition();
    });
    document.addEventListener('pointerdown', event => {
      if (!factorCompletion?.contains(event.target)) closeFactorCompletion();
    });
    document.addEventListener('quantmaster:factor-catalog', event => {
      useFactorCatalog(event.detail);
    });
    window.addEventListener('resize', queueFactorCompletionPosition);
    document.addEventListener('scroll', queueFactorCompletionPosition, true);
    loadFactorCatalog();
  }

  function syncBacktestFields() {
    if (!btForm) return;
    const kind = btForm.elements.strategy.value;
    btForm.querySelectorAll('[data-bt-field]').forEach(field => {
      const visible = field.dataset.btField.split(/\s+/).includes(kind);
      field.hidden = !visible;
      field.querySelectorAll('input,select').forEach(input => {
        input.disabled = !visible;
        if (input.name === 'factor') input.required = visible;
        if (input.name === 'version_id') input.required = visible;
      });
    });
    if (kind !== 'factor') closeFactorCompletion();
  }

  function backtestPayload(form) {
    const fd = new FormData(form);
    const kind = String(fd.get('strategy'));
    const strategy = kind === 'decision' ? {
      kind:'decision', profile:String(fd.get('profile') || 'risk_adjusted'),
      top_n:Number(fd.get('top_n')), holding_days:Number(fd.get('holding_days')),
      cap_weight:0.25, policy_snapshot:{},
    } : kind === 'lab_version' ? {
      kind:'lab_version', version_id:String(fd.get('version_id') || '').trim(),
      horizon:Number(fd.get('holding_days')), top_n:Number(fd.get('top_n')),
      rebalance_days:Number(fd.get('holding_days')), cap_weight:0.10,
    } : {
      kind: 'factor', factor: String(fd.get('factor') || '').trim(),
      top_n: Number(fd.get('top_n')), rebalance: String(fd.get('rebalance')),
      weighting: String(fd.get('weighting')), cap_weight: 0.35,
    };
    const optionalNumber = name => {
      const value = String(fd.get(name) || '').trim();
      return value ? Number(value) : null;
    };
    return {
      name: String(fd.get('name') || '').trim(), strategy,
      universe: String(fd.get('universe')), start: String(fd.get('start')),
      end: String(fd.get('end') || '') || null,
      benchmark: String(fd.get('benchmark') || '') || null,
      initial_capital: Number(fd.get('initial_capital')),
      stop_loss: optionalNumber('stop_loss'), take_profit: optionalNumber('take_profit'),
      research_tier: String(fd.get('research_tier') || 'auto'),
    };
  }

  function renderBacktestProgress(run) {
    if (!run || !['queued', 'running', 'interrupted'].includes(run.status)) {
      btJob.innerHTML = '';
      return;
    }
    const progress = Number(run.progress || 0);
    btJob.innerHTML = `<div class="trading-progress" role="status">
      <strong>${escapeHtml(run.phase || statusLabel[run.status])}</strong>
      <span>${escapeHtml(run.detail || `任务 ${run.id.slice(0, 8)}`)} · ${progress}%</span>
      <div class="trading-progress-track" aria-label="回测进度 ${progress}%"><i style="width:${Math.max(2, progress)}%"></i></div>
      <button class="trading-secondary" type="button" data-bt-cancel="${run.id}">取消任务</button>
    </div>`;
  }

  function renderMetrics(metrics) {
    const cells = [
      ['累计收益', percent(metrics.total_return), signedClass(metrics.total_return)],
      ['年化收益', percent(metrics.annual_return), signedClass(metrics.annual_return)],
      ['最大回撤', percent(-Math.abs(Number(metrics.max_drawdown || 0))), 'risk'],
      ['夏普', number(metrics.sharpe), ''],
      ['信息比率', number(metrics.information_ratio), ''],
      ['Sortino', number(metrics.sortino), ''],
      ['Profit Factor', number(metrics.profit_factor), ''],
      ['交易成本', number(metrics.total_trade_cost), ''],
    ];
    return `<div class="trading-metrics" aria-label="核心绩效">${cells.map(([label, value, tone]) =>
      `<div class="trading-metric"><span>${label}</span><strong class="${tone}">${value}</strong></div>`
    ).join('')}</div>`;
  }

  function renderManifest(manifest) {
    const range = manifest.date_range || {};
    const dataset = manifest.dataset || {};
    const quality = manifest.data_quality || {};
    return `<details class="trading-manifest"><summary>复现清单与数据质量</summary><dl>
      <dt>配置哈希</dt><dd><code>${escapeHtml(manifest.config_hash || '—')}</code></dd>
      <dt>应用版本</dt><dd>${escapeHtml(manifest.app_version || '—')}</dd>
      <dt>策略快照</dt><dd>${escapeHtml(JSON.stringify(manifest.strategy_snapshot || {}))}</dd>
      <dt>候选质量</dt><dd>${manifest.universe_quality === 'production' ? 'PIT 历史成分' : '固定候选 / 沙盒'} · ${number(manifest.symbol_count)} 只</dd>
      <dt>研究等级</dt><dd>${manifest.research_tier === 'production' ? 'PRODUCTION · RAW EXECUTION' : 'SANDBOX · APPROXIMATION'}</dd>
      <dt>实际区间</dt><dd>${escapeHtml((range.actual || []).join(' 至 ') || '—')}</dd>
      <dt>可用标的</dt><dd>${number(quality.usable_symbol_count)} / ${number(quality.requested_symbol_count)} 只</dd>
      <dt>可成交信号</dt><dd>${number(quality.executable_signals)} / ${number(quality.selected_signals)}</dd>
      <dt>基准状态</dt><dd>${escapeHtml({complete:'已加载', unavailable:'不可用', not_requested:'未选择'}[quality.benchmark_status] || quality.benchmark_status || '—')}</dd>
      <dt>成员哈希</dt><dd><code>${escapeHtml(dataset.manifest?.membership_hash || '—')}</code></dd>
      <dt>成交数据哈希</dt><dd><code>${escapeHtml(manifest.research_data?.manifest_hash || '—')}</code></dd>
      <dt>成交语义</dt><dd>${escapeHtml(manifest.execution || '—')}</dd>
    </dl></details>`;
  }

  function renderRiskDiagnostics(artifact) {
    const risk = artifact.risk_diagnostics || {};
    const interval = risk.annual_return_confidence_95 || [];
    const stress = artifact.stress_tests || [];
    const lifecycle = artifact.trade_lifecycle || {};
    const attribution = artifact.attribution || {};
    if (!interval.length && !stress.length && !Object.keys(attribution).length) return '';
    return `<details class="trading-manifest"><summary>风险、成本压力与信号归因</summary><dl>
      <dt>年化收益 95% 区间</dt><dd>${interval.length ? `${percent(interval[0])} 至 ${percent(interval[1])}` : '—'}</dd>
      <dt>完整开平仓周期</dt><dd>${number(lifecycle.round_trips)} 次 · 平均 ${number(lifecycle.average_holding_days)} 个交易日</dd>
      <dt>容量参与率 P95</dt><dd>${percent(artifact.metrics?.capacity_p95_participation)}</dd>
      <dt>成本压力</dt><dd>${stress.map(item => `${number(item.cost_multiplier)}× → ${percent(item.stressed_total_return)}`).join(' · ') || '—'}</dd>
      <dt>信号归因</dt><dd>${Object.keys(attribution).map(escapeHtml).join(' · ') || '传统策略仅输出目标权重'}</dd>
    </dl></details>`;
  }

  function renderTrades(trades) {
    const recent = (trades || []).slice(-100).reverse();
    if (!recent.length) return '<div class="trading-empty"><strong>没有产生成交</strong><span>检查信号、行情覆盖和整手资金约束。</span></div>';
    return `<div class="trading-table-wrap"><table><thead><tr><th>日期</th><th>标的</th><th>方向</th><th>价格</th><th>数量</th><th>金额</th><th>费用</th><th>备注</th></tr></thead><tbody>
      ${recent.map(trade => `<tr><td>${escapeHtml(trade.date)}</td><td>${escapeHtml(trade.symbol)}</td><td class="${trade.side === 'buy' ? 'up' : 'down'}">${trade.side === 'buy' ? '买入' : '卖出'}</td><td>${number(trade.price)}</td><td>${number(trade.shares)}</td><td>${number(trade.amount)}</td><td>${number(trade.cost)}</td><td>${escapeHtml(trade.note || '—')}</td></tr>`).join('')}
    </tbody></table></div>`;
  }

  function renderBlockedOrders(orders) {
    const recent = (orders || []).slice(-100).reverse();
    if (!recent.length) return '';
    return `<div class="trading-history-head"><h3>受阻订单</h3><span>展示最近 100 条，共 ${number(orders.length)} 条；同一目标会跨交易日重试</span></div>
      <div class="trading-table-wrap"><table><thead><tr><th>日期</th><th>标的</th><th>方向</th><th>原因</th><th>备注</th></tr></thead><tbody>
        ${recent.map(order => `<tr><td>${escapeHtml(order.date)}</td><td>${escapeHtml(order.symbol)}</td><td>${order.side === 'buy' ? '买入' : order.side === 'sell' ? '卖出' : '调仓'}</td><td>${escapeHtml(orderReason[order.reason] || order.reason)}</td><td>${escapeHtml(order.note || '—')}</td></tr>`).join('')}
      </tbody></table></div>`;
  }

  function drawBacktestCharts(artifact, compareRuns = null) {
    requestAnimationFrame(() => {
      if (typeof window.echarts === 'undefined' || typeof mkChart !== 'function') return;
      const navSeries = compareRuns
        ? compareRuns.map(run => ({name: run.name, type: 'line', data: run.nav, showSymbol: false, lineStyle: {width: 2}}))
        : [
          {name: artifact.manifest?.strategy_name || '策略', type: 'line', data: artifact.nav || [], showSymbol: false, lineStyle: {width: 2}},
          ...((artifact.benchmark_nav || []).length ? [{name: '基准', type: 'line', data: artifact.benchmark_nav, showSymbol: false, lineStyle: {width: 1}}] : []),
        ];
      mkChart('bt-workbench-nav').setOption(baseOpt({
        legend: {textStyle: {color: INK2}, top: 0}, xAxis: timeAxis(), yAxis: valAxis(), series: navSeries,
      }), true);
      if (!compareRuns && document.getElementById('bt-workbench-dd')) {
        mkChart('bt-workbench-dd').setOption(baseOpt({
          xAxis: timeAxis(), yAxis: valAxis(value => `${(value * 100).toFixed(0)}%`),
          series: [{name: '回撤', type: 'line', data: artifact.drawdown || [], showSymbol: false, lineStyle: {width: 2, color: CHART_COLORS.danger}, areaStyle:{opacity:.16,color:CHART_COLORS.danger}}],
        }), true);
      }
    });
  }

  async function promptBacktestProblem(run) {
    const problem = run.result?.problem;
    if (!problem || run.legacy_read_only || btState.prompted.has(run.id)) return;
    btState.prompted.add(run.id);
    const accepted = await window.QuantMasterProblemDialog.open(
      problem, run.result?.data_quality || null,
    );
    if (!accepted || !problem.can_continue) return;
    try {
      btJob.innerHTML = '<div class="trading-success">已确认数据偏差，正在创建仅使用可用数据的新任务。</div>';
      const config = JSON.parse(JSON.stringify(run.config || {}));
      config.allow_partial = true;
      const retry = await mutate('/api/v1/backtests', 'POST', config);
      await loadBacktests();
      await openBacktest(retry.id, {prompt:true});
    } catch (error) {
      renderError(btJob, error, '重新创建回测失败');
    }
  }

  function renderBacktestResult(run, {prompt = false} = {}) {
    const artifact = run.artifact;
    if (!artifact) {
      if (run.status === 'failed') {
        const problem = run.result?.problem;
        renderError(btOut, problem?.message || run.error || '回测失败');
        if (problem) {
          window.QuantMasterRunInfo.sync('backtest-result', [problem]);
          if (prompt) queueMicrotask(() => promptBacktestProblem(run));
        } else window.QuantMasterRunInfo.sync('backtest-result', []);
      }
      else if (run.status === 'cancelled') {
        window.QuantMasterRunInfo.sync('backtest-result', []);
        btOut.innerHTML = '<div class="trading-empty"><strong>任务已取消</strong><span>没有写入不完整的结果产物。</span></div>';
      } else {
        window.QuantMasterRunInfo.sync('backtest-result', []);
        btOut.innerHTML = '<div class="trading-empty"><strong>任务正在执行</strong><span>可以离开本页，任务状态会保存在本地。</span></div>';
      }
      return;
    }
    window.QuantMasterRunInfo.sync('backtest-result', (artifact.manifest?.warnings || []).map((item, index) => ({
      id:`backtest-result:${item.code || index}`, severity:item.severity || item.level || 'warning',
      source:'策略回测', title:item.title || '回测结果包含注意事项',
      message:typeof item === 'string' ? item : item.message,
      action:item.action || '查看回测复现清单中的数据质量说明。',
    })));
    btOut.innerHTML = `${renderWarnings(artifact.manifest?.warnings)}${renderMetrics(artifact.metrics || {})}
      <div class="trading-result-actions">
        <a class="trading-secondary" href="/api/v1/backtests/${run.id}/export?format=json">导出完整 JSON</a>
        <a class="trading-secondary" href="/api/v1/backtests/${run.id}/export?format=trades_csv">导出成交 CSV</a>
        ${run.legacy_read_only ? '<span class="trading-secondary">旧 Swing 回测仅供历史查看</span>' : run.config?.strategy?.kind === 'lab_version' ? '<span class="trading-secondary">OOF 回测不可直接提升模拟账户</span>' : '<button class="trading-secondary" type="button" data-bt-promote-toggle>创建模拟账户</button>'}
        <form class="trading-promote" data-bt-promote hidden><input name="name" maxlength="40" required value="${escapeHtml(run.name.slice(0, 34))} 验证" aria-label="模拟账户名称"><button class="trading-primary" type="submit">确认创建</button></form>
      </div>
      <div class="trading-chart-grid"><div class="trading-chart-block"><h4>策略净值与基准</h4><div class="trading-chart" id="bt-workbench-nav"></div></div><div class="trading-chart-block"><h4>回撤路径</h4><div class="trading-chart small" id="bt-workbench-dd"></div></div></div>
      ${renderManifest(artifact.manifest || {})}
      ${renderRiskDiagnostics(artifact)}
      <div class="trading-history-head"><h3>成交明细</h3><span>展示最近 100 笔，共 ${number((artifact.trades || []).length)} 笔</span></div>
      ${renderTrades(artifact.trades)}${renderBlockedOrders(artifact.blocked_orders)}`;
    drawBacktestCharts(artifact);
  }

  async function openBacktest(runId, {poll = true, prompt = false} = {}) {
    btState.activeId = runId;
    clearTimeout(btState.timer);
    try {
      const run = await api(`/api/v1/backtests/${runId}`, {cache: 'no-store'});
      renderBacktestProgress(run);
      renderBacktestResult(run, {prompt});
      renderBacktestHistory();
      if (poll && ['queued', 'running', 'interrupted'].includes(run.status)) {
        btState.timer = window.setTimeout(() => openBacktest(runId, {prompt}), 900);
      } else {
        await loadBacktests(false);
      }
    } catch (error) {
      renderError(btOut, error, '无法读取回测');
    }
  }

  function renderBacktestHistory() {
    const count = document.getElementById('bt-history-count');
    count.textContent = `${btState.runs.length} 次实验 · ${btState.selected.size} 个已选`;
    btCompare.disabled = btState.selected.size < 2 || btState.selected.size > 4;
    if (!btState.runs.length) {
      btHistory.innerHTML = '<div class="trading-empty"><strong>还没有实验记录</strong><span>左侧创建的回测会保存在这里。</span></div>';
      return;
    }
    btHistory.innerHTML = btState.runs.map(run => {
      const metrics = run.result?.metrics || {};
      const selectable = run.status === 'completed';
      return `<div class="trading-history-row" data-run-id="${run.id}">
        <input type="checkbox" data-bt-select aria-label="选择 ${escapeHtml(run.name)} 进行比较" ${btState.selected.has(run.id) ? 'checked' : ''} ${selectable ? '' : 'disabled'}>
        <span class="trading-history-name" title="${escapeHtml(run.name)}">${escapeHtml(run.name)}</span>
        <span>${escapeHtml(run.config?.universe || '—')} · ${escapeHtml(strategyLabel(run.config?.strategy))}</span>
        <span class="${signedClass(metrics.annual_return)}">${percent(metrics.annual_return)}</span>
        <span class="trading-status ${run.status}">${statusLabel[run.status] || run.status}</span>
        <button type="button" data-bt-open>${run.id === btState.activeId ? '当前' : '打开'}</button>
      </div>`;
    }).join('');
  }

  async function loadBacktests(render = true) {
    try {
      const payload = await api('/api/v1/backtests?limit=80', {cache: 'no-store'});
      btState.runs = payload.items || [];
      if (render) renderBacktestHistory();
    } catch (error) {
      renderError(btHistory, error, '无法读取回测历史');
    }
  }

  async function compareBacktests() {
    setButtonBusy(btCompare, true, '正在比较…');
    try {
      const result = await mutate('/api/v1/backtests/compare', 'POST', {run_ids: [...btState.selected]});
      const metricNames = [
        ['annual_return', '年化收益', percent], ['max_drawdown', '最大回撤', percent],
        ['sharpe', '夏普', number], ['information_ratio', '信息比率', number],
        ['total_trade_cost', '交易成本', number],
      ];
      btJob.innerHTML = '';
      btOut.innerHTML = `<div class="trading-success">正在比较 ${result.runs.length} 个可复现实验；净值均以 1 为起点。</div>
        <div class="trading-chart-block"><h4>净值叠加</h4><div class="trading-chart" id="bt-workbench-nav"></div></div>
        <div class="trading-table-wrap"><table><thead><tr><th>指标</th>${result.runs.map(run => `<th>${escapeHtml(run.name)}</th>`).join('')}</tr></thead><tbody>
          ${metricNames.map(([key, label, formatter]) => `<tr><td>${label}</td>${result.runs.map(run => `<td>${formatter(run.metrics?.[key])}</td>`).join('')}</tr>`).join('')}
        </tbody></table></div>${renderWarnings(result.runs.flatMap(run => run.warnings || []))}`;
      drawBacktestCharts(null, result.runs);
    } catch (error) {
      renderError(btOut, error, '比较失败');
    } finally {
      setButtonBusy(btCompare, false);
      btCompare.disabled = btState.selected.size < 2 || btState.selected.size > 4;
    }
  }

  if (btForm) {
    btForm.elements.strategy.addEventListener('change', syncBacktestFields);
    btForm.onsubmit = async event => {
      event.preventDefault();
      if (!btForm.reportValidity()) return;
      const submit = btForm.querySelector('[type="submit"]');
      setButtonBusy(submit, true, '正在创建…');
      btJob.innerHTML = '';
      try {
        const run = await mutate('/api/v1/backtests', 'POST', backtestPayload(btForm));
        await loadBacktests();
        await openBacktest(run.id, {prompt:true});
      } catch (error) {
        renderError(btOut, error, '回测未能创建');
      } finally {
        setButtonBusy(submit, false);
      }
    };
    syncBacktestFields();
  }

  btHistory?.addEventListener('change', event => {
    const checkbox = event.target.closest('[data-bt-select]');
    if (!checkbox) return;
    const id = checkbox.closest('[data-run-id]').dataset.runId;
    if (checkbox.checked && btState.selected.size >= 4) {
      checkbox.checked = false;
      btJob.innerHTML = '<div class="trading-warning">一次最多比较 4 个实验。</div>';
      return;
    }
    checkbox.checked ? btState.selected.add(id) : btState.selected.delete(id);
    renderBacktestHistory();
  });
  btHistory?.addEventListener('click', event => {
    const button = event.target.closest('[data-bt-open]');
    if (button) openBacktest(button.closest('[data-run-id]').dataset.runId, {prompt:true});
  });
  btJob?.addEventListener('click', async event => {
    const button = event.target.closest('[data-bt-cancel]');
    if (!button) return;
    setButtonBusy(button, true, '正在停止…');
    try {
      await mutate(`/api/v1/backtests/${button.dataset.btCancel}/cancel`, 'POST');
      await openBacktest(button.dataset.btCancel);
    } catch (error) { renderError(btJob, error, '取消失败'); }
  });
  btOut?.addEventListener('click', event => {
    const toggle = event.target.closest('[data-bt-promote-toggle]');
    if (!toggle) return;
    const form = btOut.querySelector('[data-bt-promote]');
    form.hidden = !form.hidden;
    if (!form.hidden) form.elements.name.focus();
  });
  btOut?.addEventListener('submit', async event => {
    const form = event.target.closest('[data-bt-promote]');
    if (!form) return;
    event.preventDefault();
    const button = form.querySelector('button');
    setButtonBusy(button, true, '正在创建…');
    try {
      const account = await mutate(`/api/v1/backtests/${btState.activeId}/paper-account`, 'POST', {
        name: String(new FormData(form).get('name')).trim(), mode: 'manual',
      });
      btJob.innerHTML = `<div class="trading-success">已创建模拟账户“${escapeHtml(account.name)}”，默认需要人工确认提案。</div>`;
      form.hidden = true;
      await loadPaperAccounts(false);
    } catch (error) { renderError(btJob, error, '模拟账户创建失败'); }
    finally { setButtonBusy(button, false); }
  });
  btCompare?.addEventListener('click', compareBacktests);
  document.getElementById('bt-refresh')?.addEventListener('click', () => loadBacktests());

  /* ---------------- 模拟盘 ---------------- */
  const paperForm = document.getElementById('paper-form');
  const paperList = document.getElementById('paper-account-list');
  const paperOut = document.getElementById('paper-out');
  const paperStatus = document.getElementById('paper-status');
  const paperActions = document.getElementById('paper-account-actions');
  const paperEditForm = document.getElementById('paper-edit-form');

  function syncPaperFields() {
    if (!paperForm) return;
    const kind = paperForm.elements.strategy.value;
    paperForm.querySelectorAll('[data-paper-field]').forEach(field => {
      const visible = field.dataset.paperField.split(/\s+/).includes(kind);
      field.hidden = !visible;
      field.querySelectorAll('input,select').forEach(input => { input.disabled = !visible; });
    });
  }

  function strategyFromForm(form, original = null) {
    const fd = new FormData(form);
    const kind = String(fd.get('strategy'));
    const profile = String(fd.get('profile') || 'risk_adjusted');
    const holdingDays = Number(fd.get('holding_days'));
    const preservedPolicy = original?.kind === 'decision' && original.profile === profile &&
      Number(original.holding_days) === holdingDays ? original.policy_snapshot || {} : {};
    return kind === 'decision'
      ? {kind:'decision', profile, top_n:Number(fd.get('top_n')), holding_days:holdingDays, cap_weight:Number(original?.cap_weight || 0.25), policy_snapshot:preservedPolicy}
      : {kind:'factor', factor:String(fd.get('factor')).trim(), top_n:Number(fd.get('top_n')), rebalance:String(fd.get('rebalance')), weighting:String(original?.weighting || 'equal'), cap_weight:Number(original?.cap_weight || 0.35)};
  }

  function paperPayload(form) {
    const fd = new FormData(form);
    return {
      name: String(fd.get('name')).trim(), strategy: strategyFromForm(form),
      universe: String(fd.get('universe')), initial_capital: Number(fd.get('initial_capital')),
      mode: fd.get('auto') ? 'auto' : 'manual', source_backtest_id: '',
    };
  }

  function renderAccountList() {
    if (!paperState.accounts.length) {
      paperList.innerHTML = paperState.includeArchived
        ? '<div class="trading-empty"><strong>没有已删除账户</strong><span>删除采用可恢复归档，不会清除历史账本。</span></div>'
        : '<div class="trading-empty"><strong>还没有模拟账户</strong><span>新建账户，或从已完成回测创建。</span></div>';
      return;
    }
    paperList.innerHTML = paperState.accounts.map(account => `<button type="button" class="paper-account-button ${account.id === paperState.activeId ? 'active' : ''}" data-paper-account="${account.id}" data-archived="${account.status === 'archived'}">
      <strong>${escapeHtml(account.name)}</strong><span>${escapeHtml(account.universe || '候选未知')} · ${strategyLabel(account.strategy)} · ${statusLabel[account.mode]}</span>
      <i class="trading-status ${account.status}">${statusLabel[account.status] || account.status}</i>
    </button>`).join('');
  }

  function strategyFacts(account) {
    const strategy = account.strategy || {};
    const facts = [
      ['策略', strategyLabel(strategy)],
      ['候选', account.universe
        ? `${account.universe} · ${(account.universe_snapshot?.symbols || []).length} 只`
        : '历史候选未知'],
      ['执行方式', account.mode === 'auto' ? '每日自动交易' : '手动运行'],
      ['初始资金', account.initial_capital == null
        ? '历史初始资金未知' : number(account.initial_capital)],
    ];
    if (strategy.kind === 'factor') {
      facts.push(['因子表达式', strategy.factor], ['调仓频率', {D:'每日', W:'每周', M:'每月'}[strategy.rebalance] || strategy.rebalance]);
    } else {
      facts.push(['持有期', `${strategy.holding_days || strategy.horizon || '—'} 个交易日`]);
    }
    if (strategy.kind) {
      facts.push(['持仓数', number(strategy.top_n)], ['单票上限', percent(strategy.cap_weight)]);
    }
    const policy = strategy.policy_snapshot || {};
    if (strategy.kind === 'decision') {
      facts.push(['模型版本', policy.model_version || '规则基线'], ['策略画像', strategy.profile || '—']);
    }
    return facts;
  }

  function renderStrategyPanel(account) {
    const snapshot = account.universe_snapshot || {};
    const members = snapshot.symbols || [];
    const management = account.management || {};
    const managementState = !account.strategy
      ? ['历史事实可查看', '策略等元数据无可靠证据；账户已暂停，账本成交与现金流仍可查看。', 'paused']
      : account.status === 'archived'
      ? ['只读归档', '策略和历史账本仍可查看；恢复账户或复制策略后继续。', 'archived']
      : management.pending_strategy_change
      ? ['切换待执行', `新策略将在 ${management.strategy_effective_after || '后续交易日'} 信号日之后执行。`, 'pending']
      : ['可直接编辑', '修改会建立新策略分段，旧周期和成交历史保持原样。', 'editable'];
    const source = !account.strategy ? '历史来源未知' : account.source_backtest_id
      ? `回测 ${String(account.source_backtest_id).slice(0, 8)}`
      : snapshot.cloned_from ? `复制自 ${String(snapshot.cloned_from).slice(0, 8)}` : '直接创建';
    return `<section class="paper-strategy-panel" aria-labelledby="paper-strategy-title">
      <div class="paper-strategy-head"><h3 id="paper-strategy-title">独立策略快照</h3>${account.strategy_hash ? `<code title="完整策略哈希">${escapeHtml(account.strategy_hash)}</code>` : '<span>历史策略未知</span>'}</div>
      <div class="paper-strategy-management" data-state="${managementState[2]}"><strong>${managementState[0]}</strong><span>${managementState[1]}</span></div>
      <div class="paper-strategy-grid">
        ${strategyFacts(account).map(([label, value]) => `<div><span>${escapeHtml(label)}</span><strong>${escapeHtml(value)}</strong></div>`).join('')}
        <div><span>策略来源</span><strong>${escapeHtml(source)}</strong></div>
        <div><span>候选快照</span><strong>${account.strategy ? `${escapeHtml(snapshot.as_of || '创建时固化')} · ${escapeHtml(snapshot.quality || '—')}` : '历史候选快照未知'}</strong></div>
      </div>
      <details class="paper-strategy-members"><summary>查看 ${members.length} 只快照成员</summary><p>${members.length ? members.map(escapeHtml).join(' · ') : '候选快照为空'}</p></details>
    </section>`;
  }

  function renderPaperSummary(report) {
    return `<div class="paper-summary" aria-label="账户摘要">
      <div><span>总资产</span><strong>${number(report.total_assets)}</strong></div>
      <div><span>现金</span><strong>${number(report.cash)}</strong></div>
      <div><span>累计收益</span><strong class="${signedClass(report.total_return)}">${percent(report.total_return)}</strong></div>
      <div><span>累计费用</span><strong>${number(report.fees)}</strong></div>
    </div>`;
  }

  function orderStatus(order) {
    const rawReason = order.waiting_reason || order.reason;
    const reason = rawReason ? ` · ${orderReason[rawReason] || rawReason}` : '';
    return `${statusLabel[order.status] || order.status}${reason}`;
  }

  function timeValue(value) {
    if (!value) return '—';
    const parsed = new Date(value);
    return Number.isNaN(parsed.getTime()) ? String(value) : parsed.toLocaleString('zh-CN', {hour12: false});
  }

  function renderAutomation(automation) {
    if (!automation) return `<section class="paper-task-panel" aria-labelledby="paper-task-title">
      <div><span class="paper-kicker">后台撮合任务</span><h3 id="paper-task-title">尚无运行记录</h3></div>
      <p>订单业务状态仍以下方记录为准；未启动后台任务不代表订单卡死。</p></section>`;
    const status = automation.task_status || automation.status || 'idle';
    const diagnostic = automation.diagnostic_code || automation.failure_code || '';
    const isProblem = ['stalled', 'orphaned', 'manual_recovery'].includes(status) || automation.health === 'needs_manual_recovery';
    return `<section class="paper-task-panel" data-health="${isProblem ? 'problem' : 'normal'}" aria-labelledby="paper-task-title">
      <div class="paper-task-heading"><div><span class="paper-kicker">后台撮合任务</span><h3 id="paper-task-title">${escapeHtml(statusLabel[status] || status)}</h3></div><span class="trading-status ${escapeHtml(status)}">${escapeHtml(statusLabel[status] || status)}</span></div>
      <dl class="paper-task-facts">
        <div><dt>当前 owner</dt><dd>${escapeHtml(automation.owner || automation.lease_owner || '—')}</dd></div>
        <div><dt>租约到期</dt><dd>${escapeHtml(timeValue(automation.lease_expires_at || automation.lease_expires))}</dd></div>
        <div><dt>最近心跳</dt><dd>${escapeHtml(timeValue(automation.heartbeat_at))}</dd></div>
        <div><dt>下次尝试</dt><dd>${escapeHtml(timeValue(automation.next_attempt_at || automation.next_retry_at))}</dd></div>
      </dl>
      <p>${escapeHtml(automation.last_progress || automation.last_error || '任务没有报告异常。')}${diagnostic ? ` <code>${escapeHtml(diagnostic)}</code>` : ''}</p>
      ${automation.recovered_lease ? '<small>本次已安全接管过期租约，并从上次进度继续。</small>' : ''}
    </section>`;
  }

  function renderFill(fill) {
    return `<li><time>${escapeHtml(timeValue(fill.filled_at || fill.time))}</time><strong>${number(fill.qty || fill.quantity)} @ ${number(fill.price)}</strong><span>费用 ${number(fill.fee)} · ${escapeHtml(fill.market_ref || fill.market_data_ref || '无行情引用')} · ${escapeHtml(fill.rule_version || '无规则版本')}</span></li>`;
  }

  function renderOrder(order) {
    const fills = order.fills || [];
    const requested = order.requested_qty ?? order.shares;
    const filled = order.filled_qty ?? (order.status === 'filled' ? order.shares : 0);
    const remaining = order.remaining_qty ?? Math.max(0, Number(requested || 0) - Number(filled || 0));
    const integrity = order.integrity_code || order.diagnostic_code || '';
    const progress = order.market_data_progress || order.last_progress || '';
    return `<article class="paper-order" data-status="${escapeHtml(order.status)}" data-integrity="${integrity ? 'conflict' : 'ok'}">
      <div class="paper-order-main"><div><strong>${escapeHtml(order.symbol || '未知标的')}</strong><span>${percent(order.target_weight)} · ${order.side === 'buy' ? '买入' : order.side === 'sell' ? '卖出' : '调仓'}</span></div><span class="trading-status ${escapeHtml(order.status)}">${escapeHtml(orderStatus(order))}</span></div>
      <dl class="paper-order-facts"><div><dt>申报 / 已成交 / 剩余</dt><dd>${number(requested)} / ${number(filled)} / ${number(remaining)}</dd></div><div><dt>均价 / 费用</dt><dd>${number(order.avg_fill_price ?? order.price)} / ${number(order.fee)}</dd></div><div><dt>下次检查</dt><dd>${escapeHtml(timeValue(order.next_check_at || order.next_attempt_at))}</dd></div><div><dt>最近进展</dt><dd>${escapeHtml(timeValue(order.last_progress_at || order.updated_at))}</dd></div></dl>
      ${(progress || order.required_market_range || order.latest_market_data_at) ? `<p class="paper-order-progress">${escapeHtml(progress || '行情等待中')}${order.required_market_range ? ` · 需要 ${escapeHtml(order.required_market_range)}` : ''}${order.latest_market_data_at ? ` · 最近可用 ${escapeHtml(order.latest_market_data_at)}` : ''}</p>` : ''}
      ${integrity ? `<p class="paper-order-conflict" role="alert">核心数量冲突：<code>${escapeHtml(integrity)}</code>。未补造成交，需要人工核对。</p>` : ''}
      ${fills.length ? `<details class="paper-fills"><summary>${fills.length} 笔 fill 明细</summary><ol>${fills.map(renderFill).join('')}</ol></details>` : ''}
    </article>`;
  }

  function renderCycles(cycles) {
    if (!cycles?.length) return '<div class="trading-empty"><strong>暂无调仓周期</strong><span>自动账户将在每日收盘数据就绪后生成；手动账户可使用上方按钮。</span></div>';
    return cycles.slice(0, 12).map(cycle => `<section class="paper-cycle" data-cycle-id="${cycle.id}">
      <div class="paper-cycle-head"><div><strong>信号日 ${escapeHtml(cycle.signal_date)}</strong><span class="trading-status ${cycle.status}">${statusLabel[cycle.status] || cycle.status}</span><span>${cycle.execution_date ? `最近处理 ${escapeHtml(cycle.execution_date)}` : '尚未到执行日'}</span></div>
        <div class="paper-cycle-actions">${cycle.status === 'proposed' ? '<button class="trading-primary" type="button" data-cycle-confirm>确认并等待开盘</button>' : ''}</div></div>
      ${renderWarnings(cycle.warnings)}
      <div class="paper-orders" aria-label="订单业务状态">${(cycle.orders || []).map(renderOrder).join('') || '<div class="trading-empty"><strong>当前持仓已符合目标</strong><span>本周期不需要下单。</span></div>'}</div>
    </section>`).join('');
  }

  function drawPaperNav(payload) {
    const panel = document.getElementById('paper-nav-panel');
    if (!payload.dates?.length) {
      panel.hidden = true;
      return;
    }
    panel.hidden = false;
    requestAnimationFrame(() => {
      if (typeof mkChart !== 'function') return;
      const dates = payload.dates.map(value => String(value || '').slice(0, 10));
      const spansYears = dates.length > 1 && dates[0].slice(0, 4) !== dates.at(-1).slice(0, 4);
      mkChart('paper-nav-chart').setOption(baseOpt({
        grid: {left: 46, right: 20, top: 24, bottom: 32},
        tooltip: {trigger: 'axis', confine: true, formatter: params => {
          const point = params[0];
          return `${dates[point?.dataIndex] || ''}<br>模拟盘 TWR&nbsp;&nbsp;<b>${Number(point?.value).toFixed(4)}</b>`;
        }},
        xAxis: {
          type: 'category', data: dates, boundaryGap: false,
          axisTick: {show: false}, axisLine: {lineStyle: {color: AXIS}},
          axisLabel: {color: MUTED, hideOverlap: true, showMinLabel: true, showMaxLabel: true,
            formatter: value => spansYears ? value.replaceAll('-', '.') : value.slice(5).replace('-', '.')},
        },
        yAxis: valAxis(),
        series: [{name: '模拟盘 TWR', type: 'line', showSymbol: dates.length < 8,
          symbolSize: 5, lineStyle: {width: 2}, data: payload.twr}],
      }), true);
    });
  }

  async function openPaperAccount(accountId) {
    paperState.activeId = accountId;
    paperState.activeReport = null;
    renderAccountList();
    let account = paperState.accounts.find(item => item.id === accountId);
    if (!account) return;
    if (paperEditForm) paperEditForm.hidden = true;
    document.getElementById('paper-edit').setAttribute('aria-expanded', 'false');
    document.getElementById('paper-copy').setAttribute('aria-expanded', 'false');
    document.getElementById('paper-account-title').textContent = account.name;
    const executionMode = account.mode === 'auto' ? '每日自动交易' : '手动运行';
    document.getElementById('paper-account-meta').textContent = `${account.universe || '候选未知'} · ${strategyLabel(account.strategy)} · ${executionMode}${account.strategy_hash ? ` · 快照 ${account.strategy_hash.slice(0, 10)}` : ''}`;
    paperActions.hidden = false;
    document.getElementById('paper-propose').disabled = account.status !== 'active';
    document.getElementById('paper-process').disabled = account.status !== 'active';
    const pause = document.getElementById('paper-pause');
    pause.textContent = account.status === 'paused' ? '恢复' : '暂停';
    pause.disabled = account.status === 'archived';
    pause.hidden = account.status === 'archived';
    document.getElementById('paper-delete').hidden = account.status === 'archived';
    document.getElementById('paper-restore').hidden = account.status !== 'archived';
    const edit = document.getElementById('paper-edit');
    edit.disabled = account.status === 'archived';
    paperOut.innerHTML = '<div class="trading-skeleton"></div>';
    try {
      const payload = await api(`/api/v1/paper/accounts/${accountId}/report`, {cache: 'no-store'});
      paperState.activeReport = payload;
      account = payload.account || account;
      const index = paperState.accounts.findIndex(item => item.id === accountId);
      if (index >= 0) paperState.accounts[index] = account;
      renderAccountList();
      document.getElementById('paper-account-title').textContent = account.name;
      document.getElementById('paper-account-meta').textContent = `${account.universe || '候选未知'} · ${strategyLabel(account.strategy)} · ${account.mode === 'auto' ? '每日自动交易' : '手动运行'}${account.strategy_hash ? ` · 快照 ${account.strategy_hash.slice(0, 10)}` : ''}`;
      edit.textContent = '编辑账户与策略';
      edit.title = '修改策略、候选或调仓频率后，按 15:00 分界排入后续真实交易日开盘';
      edit.disabled = account.status === 'archived' || !account.management?.strategy_editable;
      paperOut.innerHTML = `${renderWarnings(payload.warnings)}${renderAutomation(payload.automation)}${renderStrategyPanel(account)}${renderPaperSummary(payload.report)}
        <div class="trading-history-head"><h3>订单周期</h3><span>${account.mode === 'auto' ? '每日自动检查；信号后的下一交易日开盘撮合' : '确认只会排队，下一可用交易日开盘才撮合'}</span></div>${renderCycles(payload.cycles)}`;
      drawPaperNav(payload);
    } catch (error) {
      renderError(paperOut, error, '账户报告读取失败');
    }
  }

  async function loadPaperAccounts(openFirst = true) {
    try {
      const payload = await api(
        `/api/v1/paper/accounts?include_archived=${paperState.includeArchived}`,
        {cache: 'no-store'},
      );
      paperState.accounts = payload.items || [];
      renderAccountList();
      if (!paperState.accounts.length) {
        paperState.activeId = '';
        paperState.activeReport = null;
        paperActions.hidden = true;
        paperEditForm.hidden = true;
        document.getElementById('paper-nav-panel').hidden = true;
        document.getElementById('paper-account-title').textContent = '选择一个账户';
        document.getElementById('paper-account-meta').textContent = '策略快照和订单互相隔离';
        paperOut.innerHTML = '<div class="trading-empty"><strong>还没有选中账户</strong><span>选择已有账户，或创建一份新的策略快照。</span></div>';
        return;
      }
      if (paperState.activeId && paperState.accounts.some(item => item.id === paperState.activeId)) {
        if (openFirst) await openPaperAccount(paperState.activeId);
      } else if (openFirst && paperState.accounts.length) {
        await openPaperAccount(paperState.accounts[0].id);
      }
    } catch (error) {
      renderError(paperList, error, '模拟账户读取失败');
    }
  }

  window.loadPaper = () => loadPaperAccounts(true);

  document.getElementById('paper-new-toggle')?.addEventListener('click', event => {
    const button = event.currentTarget;
    paperForm.hidden = !paperForm.hidden;
    button.setAttribute('aria-expanded', String(!paperForm.hidden));
    button.textContent = paperForm.hidden ? '新建账户' : '收起表单';
    if (!paperForm.hidden) paperForm.elements.name.focus();
  });
  if (paperForm) {
    paperForm.elements.strategy.addEventListener('change', syncPaperFields);
    paperForm.onsubmit = async event => {
      event.preventDefault();
      if (!paperForm.reportValidity()) return;
      const button = paperForm.querySelector('[type="submit"]');
      setButtonBusy(button, true, '正在创建…');
      try {
        const account = await mutate('/api/v1/paper/accounts', 'POST', paperPayload(paperForm));
        paperForm.reset();
        syncPaperFields();
        paperForm.hidden = true;
        const toggle = document.getElementById('paper-new-toggle');
        toggle.textContent = '新建账户';
        toggle.setAttribute('aria-expanded', 'false');
        await loadPaperAccounts(false);
        await openPaperAccount(account.id);
        paperStatus.innerHTML = `<div class="trading-success">账户已创建。产生调仓或成交历史前，可直接编辑账户与策略。</div>`;
      } catch (error) {
        renderError(paperStatus, error, '账户创建失败');
      } finally { setButtonBusy(button, false); }
    };
    syncPaperFields();
  }

  let paperEditorStrategyLocked = false;

  function syncPaperEditFields() {
    if (!paperEditForm) return;
    const kind = paperEditForm.elements.strategy.value;
    paperEditForm.querySelectorAll('[data-paper-edit-field]').forEach(field => {
      const visible = field.dataset.paperEditField.split(/\s+/).includes(kind);
      field.hidden = !visible;
      field.querySelectorAll('input,select').forEach(input => {
        input.disabled = !visible || paperEditorStrategyLocked;
      });
    });
    ['strategy', 'universe', 'top_n'].forEach(name => {
      paperEditForm.elements[name].disabled = paperEditorStrategyLocked;
    });
    paperEditForm.querySelector('[data-paper-capital] input').disabled =
      paperEditForm.elements.action.value === 'edit';
    paperEditForm.querySelectorAll('label').forEach(label => {
      const control = label.querySelector('input,select');
      label.toggleAttribute('data-strategy-locked', Boolean(
        control?.disabled && !['name', 'mode', 'initial_capital'].includes(control.name),
      ));
    });
  }

  function setEditorValue(name, value) {
    const control = paperEditForm?.elements[name];
    if (!control) return;
    if (control.tagName === 'SELECT' && ![...control.options].some(option => option.value === String(value))) {
      control.add(new Option(String(value), String(value)));
    }
    control.value = value == null ? '' : String(value);
  }

  function openPaperEditor(action) {
    const account = paperState.activeReport?.account ||
      paperState.accounts.find(item => item.id === paperState.activeId);
    if (!account || !paperEditForm) return;
    if (!account.strategy) return;
    const copy = action === 'copy';
    const strategy = account.strategy || {};
    paperEditForm.elements.action.value = action;
    setEditorValue('name', copy ? `${account.name} · 调整` : account.name);
    setEditorValue('mode', copy ? 'manual' : account.mode);
    setEditorValue('strategy', strategy.kind || 'factor');
    setEditorValue('universe', account.universe);
    setEditorValue('factor', strategy.factor || 'mom_20d');
    setEditorValue('rebalance', strategy.rebalance || 'W');
    setEditorValue('holding_days', strategy.holding_days || strategy.horizon || 3);
    setEditorValue('profile', strategy.profile || 'risk_adjusted');
    setEditorValue('top_n', strategy.top_n || 5);
    setEditorValue('initial_capital', account.initial_capital);
    paperEditorStrategyLocked = false;
    document.getElementById('paper-editor-title').textContent = copy
      ? '复制并调整策略' : '编辑账户';
    document.getElementById('paper-editor-note').textContent = copy
      ? '新账户会固化独立策略与候选快照，原账户和历史账本保持不变。'
      : '可修改策略、候选和调仓频率；保存后建立新分段，并自动排入后续真实交易日开盘。';
    document.getElementById('paper-edit-submit').textContent = copy ? '创建调整账户' : '保存账户';
    syncPaperEditFields();
    paperEditForm.hidden = false;
    document.getElementById('paper-edit').setAttribute('aria-expanded', String(!copy));
    document.getElementById('paper-copy').setAttribute('aria-expanded', String(copy));
    paperEditForm.elements.name.focus();
  }

  paperEditForm?.elements.strategy.addEventListener('change', syncPaperEditFields);
  paperEditForm?.addEventListener('submit', async event => {
    event.preventDefault();
    if (!paperEditForm.reportValidity()) return;
    const account = paperState.activeReport?.account ||
      paperState.accounts.find(item => item.id === paperState.activeId);
    if (!account) return;
    const action = paperEditForm.elements.action.value;
    const button = document.getElementById('paper-edit-submit');
    setButtonBusy(button, true, action === 'copy' ? '正在创建…' : '正在保存…');
    try {
      const body = {
        name: String(paperEditForm.elements.name.value).trim(),
        mode: String(paperEditForm.elements.mode.value),
      };
      if (action === 'copy' || !paperEditorStrategyLocked) {
        body.strategy = strategyFromForm(paperEditForm, account.strategy);
        body.universe = String(paperEditForm.elements.universe.value);
      }
      let saved;
      if (action === 'copy') {
        saved = await mutate('/api/v1/paper/accounts', 'POST', {
          ...body,
          initial_capital: Number(paperEditForm.elements.initial_capital.value),
          source_backtest_id: account.source_backtest_id || '',
        });
      } else {
        saved = await mutate(`/api/v1/paper/accounts/${account.id}`, 'PATCH', body);
      }
      paperEditForm.hidden = true;
      document.getElementById('paper-edit').setAttribute('aria-expanded', 'false');
      document.getElementById('paper-copy').setAttribute('aria-expanded', 'false');
      await loadPaperAccounts(false);
      await openPaperAccount(saved.id);
      const transition = saved.transition;
      const savedMessage = transition?.status === 'waiting_data'
        ? transition.message
        : transition?.signal_date
        ? `账户设置已保存；新策略按 ${transition.signal_date} 作为信号日，随后首个真实交易日开盘执行。`
        : '账户设置已保存。';
      paperStatus.innerHTML = `<div class="${transition?.status === 'waiting_data' ? 'trading-warning' : 'trading-success'}">${action === 'copy' ? '已创建独立调整账户，原账户保持不变。' : escapeHtml(savedMessage)}</div>`;
    } catch (error) {
      renderError(paperStatus, error, action === 'copy' ? '调整账户创建失败' : '账户保存失败');
    } finally { setButtonBusy(button, false); }
  });

  document.getElementById('paper-edit')?.addEventListener('click', () => openPaperEditor('edit'));
  document.getElementById('paper-copy')?.addEventListener('click', () => openPaperEditor('copy'));
  document.getElementById('paper-edit-cancel')?.addEventListener('click', () => {
    paperEditForm.hidden = true;
    document.getElementById('paper-edit').setAttribute('aria-expanded', 'false');
    document.getElementById('paper-copy').setAttribute('aria-expanded', 'false');
  });

  document.getElementById('paper-show-archived')?.addEventListener('change', async event => {
    paperState.includeArchived = event.currentTarget.checked;
    await loadPaperAccounts(true);
  });

  document.getElementById('paper-delete')?.addEventListener('click', async event => {
    const account = paperState.activeReport?.account ||
      paperState.accounts.find(item => item.id === paperState.activeId);
    if (!account || !window.confirm(`删除模拟账户“${account.name}”？\n\n账户会停止自动运行并从默认列表移除，历史账本仍可恢复。`)) return;
    const button = event.currentTarget;
    setButtonBusy(button, true, '正在删除…');
    try {
      await mutate(`/api/v1/paper/accounts/${account.id}`, 'DELETE');
      paperState.activeId = '';
      paperState.activeReport = null;
      await loadPaperAccounts(true);
      paperStatus.innerHTML = '<div class="trading-success">账户已删除；勾选“已删除”可以查看或恢复。</div>';
    } catch (error) { renderError(paperStatus, error, '账户删除失败'); }
    finally { setButtonBusy(button, false); }
  });

  document.getElementById('paper-restore')?.addEventListener('click', async event => {
    const account = paperState.activeReport?.account ||
      paperState.accounts.find(item => item.id === paperState.activeId);
    if (!account) return;
    const button = event.currentTarget;
    setButtonBusy(button, true, '正在恢复…');
    try {
      await mutate(`/api/v1/paper/accounts/${account.id}`, 'PATCH', {status:'paused'});
      await loadPaperAccounts(false);
      await openPaperAccount(account.id);
      paperStatus.innerHTML = '<div class="trading-success">账户已恢复为暂停状态；确认策略后可手动恢复运行。</div>';
    } catch (error) { renderError(paperStatus, error, '账户恢复失败'); }
    finally { setButtonBusy(button, false); }
  });

  paperList?.addEventListener('click', event => {
    const button = event.target.closest('[data-paper-account]');
    if (button) openPaperAccount(button.dataset.paperAccount);
  });

  document.getElementById('paper-propose')?.addEventListener('click', async event => {
    const button = event.currentTarget;
    setButtonBusy(button, true, '正在生成…');
    paperStatus.innerHTML = '<div class="trading-progress"><strong>正在计算最新收盘信号</strong><span>此步骤不会写入成交账本</span><div class="trading-progress-track"><i style="width:55%"></i></div></div>';
    try {
      const result = await mutate(`/api/v1/paper/accounts/${paperState.activeId}/proposals`, 'POST');
      if (result.status === 'not_due') {
        paperStatus.innerHTML = `<div class="trading-warning">${escapeHtml(result.message)}</div>`;
      } else {
        paperStatus.innerHTML = `<div class="trading-success">提案已生成${result.status === 'confirmed' ? '并按账户设置自动确认' : ''}；成交账本尚未写入。</div>`;
      }
      await openPaperAccount(paperState.activeId);
    } catch (error) { renderError(paperStatus, error, '提案生成失败'); }
    finally { setButtonBusy(button, false); }
  });

  document.getElementById('paper-process')?.addEventListener('click', async event => {
    const button = event.currentTarget;
    setButtonBusy(button, true, '正在检查开盘…');
    try {
      const result = await mutate(`/api/v1/paper/accounts/${paperState.activeId}/process`, 'POST');
      const messages = {
        idle: '没有待撮合订单。', waiting_open: '下一交易日开盘价尚未到达，账本没有变化。',
        completed: `本轮已成交 ${result.filled?.length || 0} 笔。`,
        blocked: `已成交 ${result.filled?.length || 0} 笔，${result.blocked?.length || 0} 笔因交易规则继续等待。`,
      };
      paperStatus.innerHTML = `<div class="${result.status === 'blocked' ? 'trading-warning' : 'trading-success'}">${escapeHtml(messages[result.status] || result.message || '处理完成')}</div>`;
      await openPaperAccount(paperState.activeId);
    } catch (error) { renderError(paperStatus, error, '订单处理失败'); }
    finally { setButtonBusy(button, false); }
  });

  document.getElementById('paper-pause')?.addEventListener('click', async event => {
    const account = paperState.accounts.find(item => item.id === paperState.activeId);
    if (!account) return;
    const button = event.currentTarget;
    setButtonBusy(button, true, '正在保存…');
    try {
      const next = account.status === 'paused' ? 'active' : 'paused';
      await mutate(`/api/v1/paper/accounts/${account.id}`, 'PATCH', {status: next});
      await loadPaperAccounts(false);
      await openPaperAccount(account.id);
      paperStatus.innerHTML = `<div class="trading-success">账户已${next === 'paused' ? '暂停，不再生成新提案' : '恢复'}。</div>`;
    } catch (error) { renderError(paperStatus, error, '账户状态修改失败'); }
    finally { setButtonBusy(button, false); }
  });

  paperOut?.addEventListener('click', async event => {
    const button = event.target.closest('[data-cycle-confirm]');
    if (!button) return;
    const cycleId = button.closest('[data-cycle-id]').dataset.cycleId;
    setButtonBusy(button, true, '正在确认…');
    try {
      await mutate(`/api/v1/paper/cycles/${cycleId}/confirm`, 'POST');
      paperStatus.innerHTML = '<div class="trading-success">提案已确认并进入待开盘；此刻仍未写入成交。</div>';
      await openPaperAccount(paperState.activeId);
    } catch (error) { renderError(paperStatus, error, '确认失败'); }
    finally { setButtonBusy(button, false); }
  });

  loadBacktests();
  loadPaperAccounts(false);
})();
