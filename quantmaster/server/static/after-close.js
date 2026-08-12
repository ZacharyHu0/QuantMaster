(() => {
  const state = {
    loaded:false, loading:null, snapshot:null, labels:[], level:'L1', sector:'',
    jobId:'', poll:null, stockdbPoll:null, stockdbActive:false,
  };
  const root = document.getElementById('tab-after-close');
  if (!root) return;

  const number = (value, digits = 1) => value == null ? '—' : Number(value).toFixed(digits);
  const percent = value => value == null ? '—' : `${(Number(value) * 100).toFixed(1)}%`;
  const money = value => {
    if (value == null) return '—';
    const n = Number(value);
    if (Math.abs(n) >= 1e8) return `${(n / 1e8).toFixed(1)}亿`;
    if (Math.abs(n) >= 1e4) return `${(n / 1e4).toFixed(0)}万`;
    return n.toFixed(0);
  };
  const signClass = value => Number(value) > 0 ? 'up' : Number(value) < 0 ? 'down' : '';

  function renderCoverage() {
    const coverage = state.snapshot?.coverage || {};
    const fields = coverage.field_coverage || {};
    const optional = [
      ['amount','成交额'], ['float_mv','流通市值'], ['total_mv','总市值'],
      ['pe_ttm','市盈率'], ['pb','市净率'], ['is_st','风险警示标记'],
    ];
    const items = [
      ['证券覆盖', `${coverage.observed_symbols ?? 0} / ${coverage.expected_symbols ?? 0}`],
      ['开盘、最高、最低、收盘和成交量完整率', percent(coverage.required_ohlcv_ratio)],
      ['申万一级', `${coverage.board_counts?.L1 ?? 0} 个`],
      ['申万二/三级', `${(coverage.board_counts?.L2 ?? 0) + (coverage.board_counts?.L3 ?? 0)} 个`],
      ['概念目录', `${coverage.board_counts?.CONCEPT ?? 0} 个`],
      ...optional.map(([key, label]) => [label, percent(fields[key]?.latest_ratio)]),
    ];
    document.getElementById('after-close-coverage').innerHTML = items.map(item =>
      `<span>${esc(item[0])}<strong>${esc(item[1])}</strong></span>`).join('');
  }

  function filteredCandidates() {
    const candidates = state.snapshot?.candidates || [];
    return state.sector
      ? candidates.filter(item => (item.sectors || []).some(sector => sector.code === state.sector))
      : candidates;
  }

  function renderSectors() {
    const rows = (state.snapshot?.sectors || []).filter(item => item.level === state.level);
    document.getElementById('after-close-sector-summary').textContent = `${rows.length} 个板块 · 点击筛选候选`;
    const body = document.getElementById('after-close-sectors');
    body.innerHTML = rows.length ? rows.map(item => `
      <tr data-sector-code="${esc(item.code)}" class="${item.code === state.sector ? 'selected' : ''}">
        <td class="after-close-rank">${item.rank}</td>
        <td><span class="after-close-name"><strong>${esc(item.name)}</strong><small>${esc(item.code)} · ${item.eligible_members}/${item.total_members}</small></span></td>
        <td class="after-close-score">${number(item.score)}<small>${item.sensitivity?.equal?.['20']?.rank_delta ?? 0} 位敏感性差</small></td>
        <td class="${signClass(item.return_20d)}">${percent(item.return_20d)}</td>
        <td class="${signClass(item.relative_20d)}">${percent(item.relative_20d)}</td>
        <td>${percent(item.breadth_20d)}</td><td>${percent(item.coverage)}</td>
      </tr>`).join('') : '<tr><td colspan="7" class="after-close-empty">该层级暂无可评分板块</td></tr>';
  }

  function renderCandidates() {
    const rows = filteredCandidates();
    const selected = state.sector
      ? (state.snapshot?.sectors || []).find(item => item.code === state.sector)?.name : '';
    document.getElementById('after-close-candidate-summary').textContent = selected
      ? `${selected} · ${rows.length} 只入选` : `${rows.length} 只 · 全部优先板块`;
    document.getElementById('after-close-clear-sector').hidden = !state.sector;
    document.getElementById('after-close-candidates').innerHTML = rows.length ? rows.map(item => `
      <tr data-candidate-symbol="${esc(item.symbol)}">
        <td class="after-close-rank">${item.rank}</td>
        <td><span class="after-close-name"><strong>${esc(item.name || item.symbol)}</strong><small>${esc(item.symbol)}</small></span></td>
        <td class="after-close-score">${number(item.score)}<small>${item.shadow?.score == null ? '影子无排名' : `影子 ${number(item.shadow.score)} · ${item.shadow.rank_delta > 0 ? '+' : ''}${item.shadow.rank_delta || 0}`}</small></td>
        <td class="${signClass(item.metrics?.return_20d)}">${percent(item.metrics?.return_20d)}</td>
        <td class="${signClass(item.metrics?.amount_change)}">${percent(item.metrics?.amount_change)}</td>
        <td class="${signClass(item.metrics?.drawdown_20d)}">${percent(item.metrics?.drawdown_20d)}</td>
        <td class="after-close-evidence">${(item.reasons || []).map(esc).join('；')}</td>
      </tr>`).join('') : '<tr><td colspan="7" class="after-close-empty">该板块没有进入正式研究候选的成分</td></tr>';
  }

  function renderLabels() {
    const target = document.querySelector('[data-after-close-labels]');
    target.innerHTML = state.labels.length ? state.labels.map(item =>
      `<span><strong>${item.horizon}D</strong> 命中 ${percent(item.hit_rate)} · 均值 ${percent(item.mean_return)} · 全市场超额 ${percent(item.excess_mean_return)} · 中证800超额 ${percent(item.excess_vs_csi800)} · 回撤 ${percent(item.mean_max_drawdown)}</span>`
    ).join('') : '尚无足够未来交易日，未生成标签';
  }

  async function loadHealth() {
    const target = document.querySelector('[data-after-close-health]');
    const status = document.querySelector('[data-after-close-health-status]');
    if (!target || !status) return;
    try {
      const health = await api('/api/v1/after-close/diagnostics?limit=500');
      status.textContent = health.manual_review_eligible ? 'V2 可人工评审' : 'V2 观察中';
      const v2 = (health.summaries || []).find(item => item.score_version === 'QM_AFTER_CLOSE_V2_SHADOW' && item.horizon === 5);
      const drift = health.drift || {};
      const worst = Object.entries(drift.features || {}).sort((a,b) => Number(b[1].psi || 0) - Number(a[1].psi || 0))[0];
      const check = health.promotion_checks?.five_day_snapshots || {};
      target.innerHTML = `
        <article><small>当前正式评分</small><strong>${esc(health.active_score_version || 'QM_AFTER_CLOSE_V1')}</strong><p>提升或回滚只影响之后创建的快照。</p></article>
        <article><small>V2 五日有效标签</small><strong>${check.value || 0} / ${check.required || 60}</strong><p>${v2?.conclusion || '样本不足'}</p></article>
        <article><small>分布漂移</small><strong>${esc(drift.status || 'insufficient')}</strong><p>${worst ? `${esc(worst[0])} PSI ${number(worst[1].psi, 3)}` : '尚无足够历史分布'}</p></article>
        <article><small>人工评审资格</small><strong>${health.manual_review_eligible ? '通过' : '未通过'}</strong><p>不会自动替换 V1，也不会触发交易动作。</p></article>`;
    } catch (error) {
      status.textContent = '健康度读取失败';
      target.innerHTML = `<article><p>${esc(error.message)}</p></article>`;
    }
  }

  function staleCopy(snapshot) {
    const reason = String(snapshot.staleness?.reason || '').trim();
    const outdated = reason.match(
      /本地库最新交易日\s*(\d{4}-\d{2}-\d{2})，预期至少为\s*(\d{4}-\d{2}-\d{2})/,
    );
    if (outdated) {
      const [, actual, expected] = outdated;
      return {
        title:`扫描库尚未更新至 ${expected}`,
        detail:`盘后扫描专用的 free-stockdb 目前只到 ${actual}，因此没有发布新结果。页面继续显示 ${snapshot.as_of_date} 的正式快照。`,
        help:`其他页面的 Tushare 或行情缓存即使已到 ${expected}，也不会用于本扫描。请更新 free-stockdb，等待状态显示“数据已验证至 ${expected}”，再重新运行扫描。`,
      };
    }
    return {
      title:'最新扫描未通过完整性检查',
      detail:`${reason || '本次扫描未能生成新的正式快照。'} 页面继续显示 ${snapshot.as_of_date} 的正式快照。`,
      help:'请检查本地行情库状态，解决上述问题后重新运行扫描。',
    };
  }

  function render() {
    const snapshot = state.snapshot;
    if (!snapshot) {
      document.getElementById('after-close-sectors').innerHTML = '<tr><td colspan="7" class="after-close-empty">尚无正式快照。运行扫描后，只有通过完整性门的数据才会在这里出现。</td></tr>';
      document.getElementById('after-close-candidates').innerHTML = '<tr><td colspan="7" class="after-close-empty">等待板块与研究候选</td></tr>';
      return;
    }
    const stale = snapshot.staleness?.stale;
    document.getElementById('after-close-asof').innerHTML = `
      <dt>正式快照</dt><dd>${esc(snapshot.as_of_date)}</dd>
      <dt>数据状态</dt><dd>${stale ? '沿用旧快照' : '完整性通过'}</dd>
      <dt>评分方法</dt><dd title="${esc(snapshot.score_version)}">当前研究评分规则</dd>
      <dt>数据身份</dt><dd title="${esc(snapshot.input_hash)}">已锁定，可复核</dd>`;
    const staleBox = document.getElementById('after-close-stale');
    if (stale) {
      const copy = staleCopy(snapshot);
      staleBox.querySelector('[data-after-close-stale-title]').textContent = copy.title;
      staleBox.querySelector('[data-after-close-stale-detail]').textContent = copy.detail;
      staleBox.querySelector('[data-after-close-stale-help]').textContent = copy.help;
    }
    staleBox.hidden = !stale;
    const json = document.getElementById('after-close-json');
    const csv = document.getElementById('after-close-csv');
    json.href = `/api/v1/after-close/export/${encodeURIComponent(snapshot.snapshot_id)}?format=json`;
    csv.href = `/api/v1/after-close/export/${encodeURIComponent(snapshot.snapshot_id)}?format=csv`;
    renderCoverage(); renderSectors(); renderCandidates(); renderLabels(); void loadHealth();
  }

  async function loadHistory() {
    const data = await api('/api/v1/after-close/snapshots?limit=60');
    const select = document.getElementById('after-close-history');
    select.innerHTML = '<option value="">最新正式快照</option>' + (data.items || []).map(item =>
      `<option value="${esc(item.snapshot_id)}">${esc(item.as_of_date)} · ${esc(item.snapshot_id.slice(-8))}</option>`).join('');
  }

  async function load(snapshotId = '') {
    if (state.loading) return state.loading;
    state.loading = (async () => {
      const path = snapshotId
        ? `/api/v1/after-close/snapshots/${encodeURIComponent(snapshotId)}`
        : '/api/v1/after-close/snapshots/latest';
      const data = await api(path);
      state.snapshot = data.snapshot || null; state.labels = data.labels || [];
      state.sector = ''; state.loaded = true; render();
      await loadHistory();
      if (snapshotId) document.getElementById('after-close-history').value = snapshotId;
    })().catch(error => {
      document.getElementById('after-close-sectors').innerHTML = `<tr><td colspan="7" class="after-close-empty">${esc(error.message)}</td></tr>`;
    }).finally(() => { state.loading = null; });
    return state.loading;
  }

  function progress(job) {
    const box = document.getElementById('after-close-progress');
    box.hidden = false;
    box.querySelector('[data-after-close-phase]').textContent = `${job.phase || '扫描中'}${job.detail ? ` · ${job.detail}` : ''}`;
    box.querySelector('[data-after-close-percent]').textContent = `${job.progress || 0}%`;
    box.querySelector('.load-fill').style.transform = `scaleX(${Math.max(0, Math.min(100, job.progress || 0)) / 100})`;
    box.querySelector('[data-after-close-cancel]').disabled = !job.can_cancel;
  }

  function stockdbIsActive(status) {
    return ['queued','updating','restarting'].includes(status?.state)
      || ['queued','stopping','syncing','restarting','validating'].includes(status?.phase);
  }

  function renderStockdbUpdate(status) {
    const box = document.getElementById('after-close-source-status');
    const updateButton = document.getElementById('after-close-update-data');
    const scanButton = document.querySelector('#after-close-scan-form button.primary');
    const rerunButton = document.getElementById('after-close-rerun');
    const active = stockdbIsActive(status);
    const failed = ['error','degraded'].includes(status?.state)
      || ['failed','manual_required'].includes(status?.update_result);
    const succeeded = status?.update_result === 'success';
    const warnings = Array.isArray(status?.validation?.warnings)
      ? status.validation.warnings.filter(Boolean) : [];
    const succeededWithWarnings = succeeded && warnings.length > 0;
    const target = status?.target_session ? ` · 目标 ${status.target_session}` : '';
    const actual = status?.actual_session ? ` / 实际 ${status.actual_session}` : '';
    const validated = status?.validated_session || status?.actual_session || '';
    let message = status?.message || '正在读取扫描数据状态';
    if (succeededWithWarnings) {
      message = status?.message
        || `扫描数据已更新至 ${validated || '目标日'}；存在 ${warnings.length} 项覆盖警告，可继续扫描或再次更新。`;
    } else if (succeeded) {
      message = validated
        ? `扫描数据已更新至 ${validated}，现在可以运行扫描生成最新快照。`
        : '扫描数据更新完成，现在可以运行扫描生成最新快照。';
    } else if (failed) {
      message = `扫描数据更新未完成：${message}`;
    } else if (active) {
      message = `${message}${target}${actual}`;
    }
    state.stockdbActive = active;
    box.dataset.tone = failed ? 'error' : succeededWithWarnings
      ? 'warning' : succeeded ? 'success' : 'progress';
    box.querySelector('[data-after-close-source-message]').textContent = message;
    box.hidden = false;
    updateButton.textContent = active ? '正在更新扫描数据…' : '更新扫描数据';
    updateButton.disabled = active;
    scanButton.disabled = active || Boolean(state.jobId);
    rerunButton.disabled = active || Boolean(state.jobId);
    return active;
  }

  async function pollStockdbUpdate() {
    clearTimeout(state.stockdbPoll);
    state.stockdbPoll = null;
    try {
      const status = await api('/api/v1/settings/free-stockdb');
      if (renderStockdbUpdate(status)) {
        state.stockdbPoll = setTimeout(() => void pollStockdbUpdate(), 800);
      }
    } catch (error) {
      renderStockdbUpdate({state:'degraded', update_result:'failed', message:error.message});
    }
  }

  async function updateScanData() {
    const button = document.getElementById('after-close-update-data');
    button.disabled = true;
    button.textContent = '正在提交更新…';
    try {
      const status = await post('/api/v1/settings/free-stockdb/update', {});
      renderStockdbUpdate(status);
      state.stockdbPoll = setTimeout(() => void pollStockdbUpdate(), 250);
    } catch (error) {
      renderStockdbUpdate({state:'degraded', update_result:'failed', message:error.message});
    }
  }

  async function pollJob() {
    if (!state.jobId) return;
    const job = await api(`/api/v1/jobs/${encodeURIComponent(state.jobId)}`);
    progress(job);
    if (['completed','completed_with_warnings'].includes(job.status)) {
      state.jobId = ''; clearTimeout(state.poll); state.poll = null;
      await load(); return;
    }
    if (['failed','cancelled'].includes(job.status)) {
      state.jobId = ''; clearTimeout(state.poll); state.poll = null;
      document.getElementById('after-close-progress').hidden = true;
      await load(); return;
    }
    state.poll = setTimeout(() => void pollJob(), 900);
  }

  async function submit(force) {
    const form = document.getElementById('after-close-scan-form');
    busy(form, true, '正在提交…');
    try {
      const job = await post('/api/v1/after-close/scan', {
        as_of:document.getElementById('after-close-as-of').value || '', force:Boolean(force),
      });
      state.jobId = job.id; progress(job); void pollJob();
    } finally {
      busy(form, false);
      form.querySelector('button.primary').disabled = state.stockdbActive;
    }
  }

  function syncReplayAction() {
    document.getElementById('after-close-rerun').hidden =
      !document.getElementById('after-close-as-of').value;
  }

  document.getElementById('after-close-scan-form').addEventListener('submit', event => {
    event.preventDefault(); void submit(false);
  });
  document.getElementById('after-close-update-data').addEventListener('click', () => {
    void updateScanData();
  });
  document.getElementById('after-close-rerun').addEventListener('click', () => void submit(true));
  document.getElementById('after-close-as-of').addEventListener('input', syncReplayAction);
  document.querySelectorAll('[data-after-close-open-stockdb]').forEach(button => {
    button.addEventListener('click', async () => {
      await window.QuantMasterManagement?.open('local-data');
      const target = document.getElementById('free-stockdb-sidecar-status');
      target?.scrollIntoView({
        behavior:window.matchMedia('(prefers-reduced-motion: reduce)').matches ? 'auto' : 'smooth',
        block:'center',
      });
    });
  });
  document.querySelector('[data-after-close-cancel]').addEventListener('click', async () => {
    if (state.jobId) await post(`/api/v1/jobs/${encodeURIComponent(state.jobId)}/cancel`, {});
  });
  document.getElementById('after-close-history').addEventListener('change', event => void load(event.target.value));
  document.getElementById('after-close-levels').addEventListener('click', event => {
    const button = event.target.closest('[data-level]'); if (!button) return;
    state.level = button.dataset.level; state.sector = '';
    document.querySelectorAll('#after-close-levels [data-level]').forEach(item => item.classList.toggle('active', item === button));
    renderSectors(); renderCandidates();
  });
  document.getElementById('after-close-sectors').addEventListener('click', event => {
    const row = event.target.closest('[data-sector-code]'); if (!row) return;
    state.sector = row.dataset.sectorCode; renderSectors(); renderCandidates();
  });
  document.getElementById('after-close-clear-sector').addEventListener('click', () => {
    state.sector = ''; renderSectors(); renderCandidates();
  });
  document.getElementById('after-close-copy').addEventListener('click', async () => {
    if (!state.snapshot) return;
    const symbols = filteredCandidates().map(item => item.symbol);
    const name = `盘后-${state.snapshot.as_of_date}${state.sector ? `-${state.sector}` : ''}`;
    const result = await post('/api/v1/after-close/candidates/copy', {name, symbols});
    document.getElementById('after-close-candidate-summary').textContent = `已复制 ${result.count} 只到候选池 ${result.name}`;
  });

  window.loadAfterClose = () => load();
})();
