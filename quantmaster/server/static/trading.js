(() => {
  'use strict';

  const btState = {activeId: '', runs: [], selected: new Set(), prompted: new Set(), timer: 0};
  const paperState = {activeId: '', accounts: []};
  let csrfToken = '';

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
    active: '运行中', paused: '已暂停', archived: '已归档', auto: '自动', manual: '手动',
  };
  const orderReason = {
    missing_open: '缺少开盘价', limit_up: '涨停无法买入', limit_down: '跌停无法卖出',
    suspended: '停牌无法成交', missing_actual_limit: '缺少真实涨跌停价',
    insufficient_cash: '现金不足', t_plus_one: 'T+1 可卖数量不足', newer_cycle: '被新提案替代',
  };
  const strategyLabel = strategy => {
    if (strategy?.kind === 'decision') {
      const profile = {
        risk_adjusted:'扣费风险收益', short_term:'短期命中收益', stable:'稳定可解释',
      }[strategy.profile] || strategy.profile;
      return `Hybrid v2 · ${profile} · ${strategy.holding_days} 日`;
    }
    if (strategy?.kind === 'swing') return `${strategy.holding_days} 日短线`;
    if (strategy?.kind === 'lab_version') return `Lab OOF · ${strategy.horizon} 日 · ${String(strategy.version_id).slice(0, 8)}`;
    return strategy?.factor || '因子策略';
  };

  async function ensureCsrf() {
    if (csrfToken) return csrfToken;
    const settings = await api('/api/settings', {cache: 'no-store'});
    csrfToken = settings.csrf_token || '';
    if (!csrfToken) throw new Error('未取得本机操作令牌，请刷新页面后重试。');
    return csrfToken;
  }

  async function mutate(path, method = 'POST', body) {
    const token = await ensureCsrf();
    try {
      return await window.QuantMasterAPI(path, {
        method, cache: 'no-store',
        headers: {'Content-Type': 'application/json', 'X-CSRF-Token': token},
        body: body === undefined ? undefined : JSON.stringify(body),
      });
    } catch (error) {
      if (error?.status === 403) csrfToken = '';
      throw error;
    }
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
    } : kind === 'swing' ? {
      kind: 'swing', top_n: Number(fd.get('top_n')),
      holding_days: Number(fd.get('holding_days')), cap_weight: 0.25,
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
      ['累计收益', percent(metrics.total_return), metrics.total_return],
      ['年化收益', percent(metrics.annual_return), metrics.annual_return],
      ['最大回撤', percent(-Math.abs(Number(metrics.max_drawdown || 0))), -Math.abs(Number(metrics.max_drawdown || 0))],
      ['夏普', number(metrics.sharpe), metrics.sharpe],
      ['信息比率', number(metrics.information_ratio), metrics.information_ratio],
      ['Sortino', number(metrics.sortino), metrics.sortino],
      ['Profit Factor', number(metrics.profit_factor), metrics.profit_factor],
      ['交易成本', number(metrics.total_trade_cost), null],
    ];
    return `<div class="trading-metrics" aria-label="核心绩效">${cells.map(([label, value, signed]) =>
      `<div class="trading-metric"><span>${label}</span><strong class="${signedClass(signed)}">${value}</strong></div>`
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
          series: [{name: '回撤', type: 'line', data: artifact.drawdown || [], showSymbol: false, lineStyle: {width: 2, color: '#d55181'}}],
        }), true);
      }
    });
  }

  async function promptBacktestProblem(run) {
    const problem = run.result?.problem;
    if (!problem || btState.prompted.has(run.id)) return;
    btState.prompted.add(run.id);
    const accepted = await window.QuantMasterProblemDialog.open(
      problem, run.result?.data_quality || null,
    );
    if (!accepted || !problem.can_continue) return;
    try {
      btJob.innerHTML = '<div class="trading-success">已确认数据偏差，正在创建仅使用可用数据的新任务。</div>';
      const config = JSON.parse(JSON.stringify(run.config || {}));
      config.allow_partial = true;
      const retry = await mutate('/api/backtests', 'POST', config);
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
        <a class="trading-secondary" href="/api/backtests/${run.id}/export?format=json">导出完整 JSON</a>
        <a class="trading-secondary" href="/api/backtests/${run.id}/export?format=trades_csv">导出成交 CSV</a>
        ${run.config?.strategy?.kind === 'lab_version' ? '<span class="trading-secondary">OOF 回测不可直接提升模拟账户</span>' : '<button class="trading-secondary" type="button" data-bt-promote-toggle>创建模拟账户</button>'}
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
      const run = await api(`/api/backtests/${runId}`, {cache: 'no-store'});
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
        <span>${escapeHtml(run.config?.universe || '—')} · ${escapeHtml(run.config?.strategy?.kind || '—')}</span>
        <span class="${signedClass(metrics.annual_return)}">${percent(metrics.annual_return)}</span>
        <span class="trading-status ${run.status}">${statusLabel[run.status] || run.status}</span>
        <button type="button" data-bt-open>${run.id === btState.activeId ? '当前' : '打开'}</button>
      </div>`;
    }).join('');
  }

  async function loadBacktests(render = true) {
    try {
      const payload = await api('/api/backtests?limit=80', {cache: 'no-store'});
      btState.runs = payload.items || [];
      if (render) renderBacktestHistory();
    } catch (error) {
      renderError(btHistory, error, '无法读取回测历史');
    }
  }

  async function compareBacktests() {
    setButtonBusy(btCompare, true, '正在比较…');
    try {
      const result = await mutate('/api/backtests/compare', 'POST', {run_ids: [...btState.selected]});
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
        const run = await mutate('/api/backtests', 'POST', backtestPayload(btForm));
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
      await mutate(`/api/backtests/${button.dataset.btCancel}/cancel`, 'POST');
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
      const account = await mutate(`/api/backtests/${btState.activeId}/paper-account`, 'POST', {
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

  function syncPaperFields() {
    if (!paperForm) return;
    const kind = paperForm.elements.strategy.value;
    paperForm.querySelectorAll('[data-paper-field]').forEach(field => {
      const visible = field.dataset.paperField.split(/\s+/).includes(kind);
      field.hidden = !visible;
      field.querySelectorAll('input,select').forEach(input => { input.disabled = !visible; });
    });
  }

  function paperPayload(form) {
    const fd = new FormData(form);
    const kind = String(fd.get('strategy'));
    const strategy = kind === 'decision'
      ? {kind:'decision', profile:String(fd.get('profile') || 'risk_adjusted'), top_n:Number(fd.get('top_n')), holding_days:Number(fd.get('holding_days')), cap_weight:0.25, policy_snapshot:{}}
      : kind === 'swing'
      ? {kind: 'swing', top_n: Number(fd.get('top_n')), holding_days: Number(fd.get('holding_days')), cap_weight: 0.25}
      : {kind: 'factor', factor: String(fd.get('factor')).trim(), top_n: Number(fd.get('top_n')), rebalance: String(fd.get('rebalance')), weighting: 'equal', cap_weight: 0.35};
    return {
      name: String(fd.get('name')).trim(), strategy,
      universe: String(fd.get('universe')), initial_capital: Number(fd.get('initial_capital')),
      mode: fd.get('auto') ? 'auto' : 'manual', source_backtest_id: '',
    };
  }

  function renderAccountList() {
    if (!paperState.accounts.length) {
      paperList.innerHTML = '<div class="trading-empty"><strong>还没有模拟账户</strong><span>新建账户，或从已完成回测创建。</span></div>';
      return;
    }
    paperList.innerHTML = paperState.accounts.map(account => `<button type="button" class="paper-account-button ${account.id === paperState.activeId ? 'active' : ''}" data-paper-account="${account.id}">
      <strong>${escapeHtml(account.name)}</strong><span>${escapeHtml(account.universe)} · ${strategyLabel(account.strategy)} · ${statusLabel[account.mode]}</span>
      <i class="trading-status ${account.status}">${statusLabel[account.status] || account.status}</i>
    </button>`).join('');
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
    const reason = order.reason ? ` · ${orderReason[order.reason] || order.reason}` : '';
    return `${statusLabel[order.status] || order.status}${reason}`;
  }

  function renderCycles(cycles) {
    if (!cycles?.length) return '<div class="trading-empty"><strong>暂无调仓周期</strong><span>收盘后生成提案，确认前不会写入成交。</span></div>';
    return cycles.slice(0, 12).map(cycle => `<section class="paper-cycle" data-cycle-id="${cycle.id}">
      <div class="paper-cycle-head"><div><strong>信号日 ${escapeHtml(cycle.signal_date)}</strong><span class="trading-status ${cycle.status}">${statusLabel[cycle.status] || cycle.status}</span><span>${cycle.execution_date ? `最近处理 ${escapeHtml(cycle.execution_date)}` : '尚未到执行日'}</span></div>
        <div class="paper-cycle-actions">${cycle.status === 'proposed' ? '<button class="trading-primary" type="button" data-cycle-confirm>确认并等待开盘</button>' : ''}</div></div>
      ${renderWarnings(cycle.warnings)}
      <div class="paper-orders"><table><thead><tr><th>标的</th><th>目标权重</th><th>方向</th><th>数量</th><th>成交价</th><th>费用</th><th>状态</th></tr></thead><tbody>
        ${(cycle.orders || []).map(order => `<tr><td>${escapeHtml(order.symbol)}</td><td>${percent(order.target_weight)}</td><td>${order.side === 'buy' ? '买入' : order.side === 'sell' ? '卖出' : '调仓'}</td><td>${number(order.shares)}</td><td>${number(order.price)}</td><td>${number(order.fee)}</td><td><span class="trading-status ${order.status}">${escapeHtml(orderStatus(order))}</span></td></tr>`).join('') || '<tr><td colspan="7">当前持仓已经符合目标</td></tr>'}
      </tbody></table></div>
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
      mkChart('paper-nav-chart').setOption(baseOpt({
        xAxis: timeAxis(), yAxis: valAxis(),
        series: [{name: '模拟盘 TWR', type: 'line', showSymbol: false, lineStyle: {width: 2}, data: payload.dates.map((date, index) => [date, payload.twr[index]])}],
      }), true);
    });
  }

  async function openPaperAccount(accountId) {
    paperState.activeId = accountId;
    renderAccountList();
    const account = paperState.accounts.find(item => item.id === accountId);
    if (!account) return;
    document.getElementById('paper-account-title').textContent = account.name;
    document.getElementById('paper-account-meta').textContent = `${account.universe} · ${strategyLabel(account.strategy)} · 快照 ${account.strategy_hash.slice(0, 10)}`;
    paperActions.hidden = false;
    document.getElementById('paper-propose').disabled = account.status !== 'active';
    document.getElementById('paper-process').disabled = account.status !== 'active';
    const pause = document.getElementById('paper-pause');
    pause.textContent = account.status === 'paused' ? '恢复' : '暂停';
    pause.disabled = account.status === 'archived';
    paperOut.innerHTML = '<div class="trading-skeleton"></div>';
    try {
      const payload = await api(`/api/paper/accounts/${accountId}/report`, {cache: 'no-store'});
      paperOut.innerHTML = `${renderWarnings(payload.warnings)}${renderPaperSummary(payload.report)}
        <div class="trading-history-head"><h3>订单周期</h3><span>确认只会排队，下一可用交易日开盘才撮合</span></div>${renderCycles(payload.cycles)}`;
      drawPaperNav(payload);
    } catch (error) {
      renderError(paperOut, error, '账户报告读取失败');
    }
  }

  async function loadPaperAccounts(openFirst = true) {
    try {
      const payload = await api('/api/paper/accounts', {cache: 'no-store'});
      paperState.accounts = payload.items || [];
      renderAccountList();
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
        const account = await mutate('/api/paper/accounts', 'POST', paperPayload(paperForm));
        paperForm.reset();
        syncPaperFields();
        paperForm.hidden = true;
        const toggle = document.getElementById('paper-new-toggle');
        toggle.textContent = '新建账户';
        toggle.setAttribute('aria-expanded', 'false');
        await loadPaperAccounts(false);
        await openPaperAccount(account.id);
        paperStatus.innerHTML = `<div class="trading-success">账户已创建。策略和候选已固化为快照；修改策略请复制新账户。</div>`;
      } catch (error) {
        renderError(paperStatus, error, '账户创建失败');
      } finally { setButtonBusy(button, false); }
    };
    syncPaperFields();
  }

  paperList?.addEventListener('click', event => {
    const button = event.target.closest('[data-paper-account]');
    if (button) openPaperAccount(button.dataset.paperAccount);
  });

  document.getElementById('paper-propose')?.addEventListener('click', async event => {
    const button = event.currentTarget;
    setButtonBusy(button, true, '正在生成…');
    paperStatus.innerHTML = '<div class="trading-progress"><strong>正在计算最新收盘信号</strong><span>此步骤不会写入成交账本</span><div class="trading-progress-track"><i style="width:55%"></i></div></div>';
    try {
      const result = await mutate(`/api/paper/accounts/${paperState.activeId}/proposals`, 'POST');
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
      const result = await mutate(`/api/paper/accounts/${paperState.activeId}/process`, 'POST');
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
      await mutate(`/api/paper/accounts/${account.id}`, 'PATCH', {status: next});
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
      await mutate(`/api/paper/cycles/${cycleId}/confirm`, 'POST');
      paperStatus.innerHTML = '<div class="trading-success">提案已确认并进入待开盘；此刻仍未写入成交。</div>';
      await openPaperAccount(paperState.activeId);
    } catch (error) { renderError(paperStatus, error, '确认失败'); }
    finally { setButtonBusy(button, false); }
  });

  loadBacktests();
  loadPaperAccounts(false);
})();
