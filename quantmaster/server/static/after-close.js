(() => {
  const state = {loaded:false, loading:null, snapshot:null, labels:[], level:'L1', sector:'', jobId:'', poll:null};
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
    const optional = ['amount','float_mv','total_mv','pe_ttm','pb','is_st'];
    const items = [
      ['证券覆盖', `${coverage.observed_symbols ?? 0} / ${coverage.expected_symbols ?? 0}`],
      ['完整 OHLCV', percent(coverage.required_ohlcv_ratio)],
      ['申万一级', `${coverage.board_counts?.L1 ?? 0} 个`],
      ['申万二/三级', `${(coverage.board_counts?.L2 ?? 0) + (coverage.board_counts?.L3 ?? 0)} 个`],
      ['概念目录', `${coverage.board_counts?.CONCEPT ?? 0} 个`],
      ...optional.map(key => [key, percent(fields[key]?.latest_ratio)]),
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
        <td class="after-close-score">${number(item.score)}</td>
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
        <td class="after-close-score">${number(item.score)}</td>
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
      <dt>评分版本</dt><dd>${esc(snapshot.score_version)}</dd>
      <dt>输入哈希</dt><dd title="${esc(snapshot.input_hash)}">${esc(snapshot.input_hash.slice(0, 12))}</dd>`;
    const staleBox = document.getElementById('after-close-stale');
    staleBox.hidden = !stale;
    staleBox.textContent = stale ? `最近一次扫描未发布：${snapshot.staleness.reason}` : '';
    const json = document.getElementById('after-close-json');
    const csv = document.getElementById('after-close-csv');
    json.href = `/api/v1/after-close/export/${encodeURIComponent(snapshot.snapshot_id)}?format=json`;
    csv.href = `/api/v1/after-close/export/${encodeURIComponent(snapshot.snapshot_id)}?format=csv`;
    renderCoverage(); renderSectors(); renderCandidates(); renderLabels();
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

  async function pollJob() {
    if (!state.jobId) return;
    const job = await api(`/api/v1/jobs/after_close/${encodeURIComponent(state.jobId)}`);
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
    } finally { busy(form, false); }
  }

  document.getElementById('after-close-scan-form').addEventListener('submit', event => {
    event.preventDefault(); void submit(false);
  });
  document.getElementById('after-close-rerun').addEventListener('click', () => void submit(true));
  document.querySelector('[data-after-close-cancel]').addEventListener('click', async () => {
    if (state.jobId) await post(`/api/v1/jobs/after_close/${encodeURIComponent(state.jobId)}/cancel`, {});
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
