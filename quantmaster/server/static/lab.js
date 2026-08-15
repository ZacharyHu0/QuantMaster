(function () {
  'use strict';

  const state = {
    initialized: false,
    dashboard: null,
    workbench: null,
    selectedHorizon: 3,
    overview: null,
    factors: [],
    jobs: [],
    experiments: [],
    studies: [],
    studyDetail: null,
    studyDetailLoadingId: '',
    miningRuns: [],
    selectedMiningRun: '',
    selectedStudyId: '',
    selectedVersion: '',
    status: '',
    search: '',
    factorCategory: '',
    factorKind: '',
    factorValidation: '',
    factorHorizon: '',
    factorTag: '',
    correlationHorizon: 3,
    correlationSelection: new Set(),
    selectedModel: 'ridge',
    suggestion: null,
    suggestionTask: null,
    timer: null,
    formsDirty: false,
    selectedJobId: '',
    jobDetail: null,
    jobEvents: [],
    jobLastSeq: 0,
    jobDetailLoading: false,
    jobDrawerOpener: null,
    controllers: new Map(),
    polling: false,
    preflightResolver: null,
    preflightContext: null,
    coverageRepairScope: 'critical',
  };

  const modelMeta = {
    ridge: ['RIDGE', '线性基线 · 快速、透明，适合验证特征是否真正有效'],
    mlp: ['MLP', '非线性截面映射 · 以当日特征预测未来收益'],
    tcn: ['TCN', '扩张卷积 · 捕捉多尺度时序结构'],
    gru: ['GRU', '门控循环网络 · 建模持久性和状态转换'],
    transformer: ['TRANSFORMER', '自注意力序列模型 · 学习跨时间依赖'],
    dae: ['DAE', '去噪自编码器 · 稳健潜变量与监督得分头'],
  };

  const statusLabel = {
    draft: '草稿', validating: '验证中', candidate: '待审', approved: '已批准',
    production: '生产', degraded: '降级', archived: '归档', queued: '排队',
    running: '运行', interrupted: '恢复中', completed: '完成', failed: '失败',
    completed_with_warnings: '部分完成', cancelled: '取消', paused: '暂停',
    historical_candidate: '历史候选', shadow_challenger: 'Shadow Challenger',
    paper: 'Paper', champion: 'Champion', retired: '退役',
  };

  const kindLabel = {
    prepare_data: '冻结数据快照', validate: '统一因子验证', discover_genetic: '遗传因子发现',
    discover_llm: 'AI 因子发现', discover_python: 'Python AutoMiner',
    train: '模型训练', optimize: '共享多周期优化',
    bias_audit: '防偏差审计',
    research_cycle: '每周组合研究', shadow_score: '每日影子评分',
  };

  const activeJobStatuses = new Set(['queued', 'running', 'paused', 'interrupted']);
  const terminalJobStatuses = new Set([
    'completed', 'completed_with_warnings', 'failed', 'cancelled',
  ]);

  const paramLabel = {
    universe: '候选池', start: '研究起点', end: '研究终点', horizon: '预测周期',
    count: '目标候选数', rounds: '反馈轮数', top_n: '保留候选数', population: '种群规模',
    generations: '进化代数', version_id: '因子版本', model: '模型',
    sequence_length: '序列长度', epochs: '训练轮数', device: '计算设备',
    study_id: 'Study 编号', budget_hours: '预算小时', max_trials: '最大 Trials',
    research_tier: '研究等级', models: '候选模型',
    candidate_limit: '代码候选上限', finalists: 'Pareto 入围数',
  };

  const eventLabel = {
    queued: '进入队列', started: '开始执行', progress: '阶段更新', completed: '执行完成',
    completed_with_warnings: '部分完成', failed: '执行失败', cancelled: '已取消',
    cancel_requested: '请求安全停止', interrupted: '执行中断', retry_of: '重新运行',
    retried_as: '已创建重跑任务', llm_attempt_started: '模型请求开始',
    llm_response_received: '模型已响应', llm_attempt_failed: '模型请求未完成',
    llm_retry_scheduled: '准备重试', llm_candidate_checked: '候选本地校验',
    llm_round_completed: '本轮完成',
  };

  function h(value) {
    return String(value == null ? '' : value).replace(/[&<>'"]/g, char => ({
      '&': '&amp;', '<': '&lt;', '>': '&gt;', "'": '&#39;', '"': '&quot;',
    })[char]);
  }

  function number(value, digits = 2) {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed.toFixed(digits) : '—';
  }

  function percent(value, digits = 1) {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? `${(parsed * 100).toFixed(digits)}%` : '—';
  }

  function formatDate(value, includeSeconds = false) {
    if (!value) return '—';
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) return String(value).replace('T', ' ').slice(0, 19);
    return new Intl.DateTimeFormat('zh-CN', {
      month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit',
      second: includeSeconds ? '2-digit' : undefined, hour12: false,
    }).format(date);
  }

  function formatDuration(start, end) {
    const startAt = new Date(start || '').getTime();
    const endAt = new Date(end || '').getTime();
    if (!Number.isFinite(startAt) || !Number.isFinite(endAt) || endAt < startAt) return '—';
    const seconds = Math.round((endAt - startAt) / 1000);
    if (seconds < 60) return `${seconds} 秒`;
    const minutes = Math.floor(seconds / 60);
    const rest = seconds % 60;
    if (minutes < 60) return `${minutes} 分 ${rest} 秒`;
    const hours = Math.floor(minutes / 60);
    return `${hours} 小时 ${minutes % 60} 分`;
  }

  function jobPhase(job) {
    if (!job) return '—';
    if (job.status === 'failed') return job.detail || job.error || '执行失败';
    if (job.status === 'completed_with_warnings') return job.detail || '已保留可用结果';
    return job.detail || job.phase || statusLabel[job.status] || job.status;
  }

  function redactObject(value) {
    if (Array.isArray(value)) return value.map(redactObject);
    if (!value || typeof value !== 'object') return value;
    return Object.fromEntries(Object.entries(value).map(([key, item]) => [
      key,
      /(?:api.?key|token|secret|password|authorization|credential)/i.test(key)
        ? '[已隐藏]'
        : redactObject(item),
    ]));
  }

  function displayValue(key, value) {
    if (key === 'horizon' && Number.isFinite(Number(value))) return `${value} 日`;
    if (typeof value === 'boolean') return value ? '是' : '否';
    if (value == null || value === '') return '—';
    if (typeof value === 'object') return JSON.stringify(redactObject(value));
    return String(value);
  }

  async function request(path, options = {}) {
    const {requestKey = '', ...requestOptions} = options;
    let controller = null;
    if (requestKey) {
      state.controllers.get(requestKey)?.abort();
      controller = new AbortController();
      state.controllers.set(requestKey, controller);
      requestOptions.signal = controller.signal;
    }
    try {
      return await window.QuantMasterAPI(path, {
        headers: {'Content-Type': 'application/json', ...(requestOptions.headers || {})},
        ...requestOptions,
      });
    } finally {
      if (requestKey && state.controllers.get(requestKey) === controller) {
        state.controllers.delete(requestKey);
      }
    }
  }

  function announce(message) {
    const target = document.getElementById('lab-announcer');
    if (!target) return;
    target.textContent = '';
    window.setTimeout(() => { target.textContent = message; }, 20);
  }

  function issueAction(error) {
    return error?.problem?.action || error?.error?.action || '';
  }

  function showError(title, error) {
    if (typeof window.reportLocalError === 'function') {
      window.reportLocalError('Quant Lab', title, error);
    } else {
      console.error(title, error);
      const action = issueAction(error);
      window.alert(`${title}：${error.message || error}${action ? `\n${action}` : ''}`);
    }
  }

  async function copyText(value) {
    if (navigator.clipboard?.writeText) {
      await navigator.clipboard.writeText(value);
      return;
    }
    const field = document.createElement('textarea');
    field.value = value;
    field.setAttribute('readonly', '');
    field.style.cssText = 'position:fixed;left:-9999px;opacity:0';
    document.body.appendChild(field);
    field.select();
    const copied = document.execCommand('copy');
    field.remove();
    if (!copied) throw new Error('浏览器未允许访问剪贴板，请手动复制命令。');
  }

  function dialogOffset(dialog, axis) {
    return Number.parseFloat(dialog.style.getPropertyValue(`--dialog-${axis}`)) || 0;
  }

  function viewportBox() {
    const viewport = window.visualViewport;
    return {
      left: viewport?.offsetLeft || 0,
      top: viewport?.offsetTop || 0,
      width: viewport?.width || window.innerWidth,
      height: viewport?.height || window.innerHeight,
    };
  }

  function clampDialogOffset(dialog, x, y) {
    const safe = 12;
    const viewport = viewportBox();
    const currentX = dialogOffset(dialog, 'x');
    const currentY = dialogOffset(dialog, 'y');
    const rect = dialog.getBoundingClientRect();
    const baseLeft = rect.left - currentX;
    const baseTop = rect.top - currentY;
    const minX = viewport.left + safe - baseLeft;
    const maxX = viewport.left + viewport.width - safe - baseLeft - rect.width;
    const minY = viewport.top + safe - baseTop;
    const maxY = viewport.top + viewport.height - safe - baseTop - rect.height;
    return {
      x: minX <= maxX ? Math.min(maxX, Math.max(minX, x)) : 0,
      y: minY <= maxY ? Math.min(maxY, Math.max(minY, y)) : 0,
    };
  }

  function moveDialog(dialog, x, y) {
    const next = clampDialogOffset(dialog, x, y);
    dialog.style.setProperty('--dialog-x', `${next.x}px`);
    dialog.style.setProperty('--dialog-y', `${next.y}px`);
  }

  function resetDialogPosition(dialog) {
    dialog.style.setProperty('--dialog-x', '0px');
    dialog.style.setProperty('--dialog-y', '0px');
    dialog.classList.remove('is-dragging');
  }

  function setupDraggableDialog(dialog) {
    const handle = dialog.querySelector('.lab-dialog-head');
    if (!handle || handle.dataset.dragReady) return;
    handle.dataset.dragReady = 'true';
    let drag = null;

    handle.addEventListener('pointerdown', event => {
      if (event.pointerType === 'mouse' && event.button !== 0) return;
      if (event.target.closest('button,a,input,textarea,select,label')) return;
      drag = {
        pointerId: event.pointerId,
        startX: event.clientX,
        startY: event.clientY,
        offsetX: dialogOffset(dialog, 'x'),
        offsetY: dialogOffset(dialog, 'y'),
      };
      handle.setPointerCapture(event.pointerId);
      dialog.classList.add('is-dragging');
      event.preventDefault();
    });

    handle.addEventListener('pointermove', event => {
      if (!drag || drag.pointerId !== event.pointerId) return;
      moveDialog(
        dialog,
        drag.offsetX + event.clientX - drag.startX,
        drag.offsetY + event.clientY - drag.startY
      );
    });

    const finishDrag = event => {
      if (!drag || drag.pointerId !== event.pointerId) return;
      if (handle.hasPointerCapture(event.pointerId)) handle.releasePointerCapture(event.pointerId);
      drag = null;
      dialog.classList.remove('is-dragging');
    };
    handle.addEventListener('pointerup', finishDrag);
    handle.addEventListener('pointercancel', finishDrag);

    const keepInViewport = () => {
      if (dialog.open) moveDialog(dialog, dialogOffset(dialog, 'x'), dialogOffset(dialog, 'y'));
    };
    window.addEventListener('resize', keepInViewport);
    window.visualViewport?.addEventListener('resize', keepInViewport);
  }

  function openFactorDialog(expression) {
    const dialog = document.getElementById('lab-factor-dialog');
    resetDialogPosition(dialog);
    if (expression !== undefined) dialog.querySelector('[name=expression]').value = expression || '';
    if (!dialog.open) dialog.showModal();
  }

  function isLabActive() {
    return document.getElementById('tab-lab')?.classList.contains('active');
  }

  function formatBytes(value) {
    const bytes = Number(value || 0);
    if (!Number.isFinite(bytes) || bytes <= 0) return '—';
    const units = ['B', 'KiB', 'MiB', 'GiB', 'TiB'];
    const power = Math.min(units.length - 1, Math.floor(Math.log(bytes) / Math.log(1024)));
    return `${number(bytes / (1024 ** power), power > 1 ? 1 : 0)} ${units[power]}`;
  }

  function resolvePreflight(confirmed) {
    const resolver = state.preflightResolver;
    state.preflightResolver = null;
    state.preflightContext = null;
    if (document.getElementById('lab-preflight-dialog')?.open) {
      document.getElementById('lab-preflight-dialog').close();
    }
    if (resolver) resolver(Boolean(confirmed));
  }

  function coverageRepairMarkup(plan) {
    if (!plan?.repair_symbol_count && !plan?.membership_missing) return '';
    const counts = plan.counts || {};
    const gaps = plan.gaps || [];
    const providers = plan.providers || [];
    const includeWarmup = state.coverageRepairScope === 'all';
    const repairCount = Number(
      includeWarmup ? plan.repair_symbol_count : plan.critical_repair_symbol_count
    );
    return `<section class="lab-coverage-repair" aria-labelledby="lab-coverage-title">
      <div class="lab-coverage-head"><div><span>LOCAL POOL SURFACE</span><h4 id="lab-coverage-title">数据扇区健康度</h4></div><b>${Number(plan.repair_symbol_count || 0).toLocaleString()} / ${Number(plan.symbol_count || 0).toLocaleString()} 只待补</b></div>
      <div class="lab-coverage-numbers"><div><span>关键缺口</span><b>${Number((counts.critical || 0) + (counts.missing || 0)).toLocaleString()}</b></div><div><span>仅预热缺口</span><b>${Number(counts.warmup || 0).toLocaleString()}</b></div><div><span>估算缺失交易日</span><b>${Number(plan.missing_session_count || 0).toLocaleString()}</b></div><div><span>PIT 成分</span><b>${plan.membership_missing ? '缺失' : '已就绪'}</b></div></div>
      <div class="lab-coverage-map-wrap"><canvas class="lab-coverage-map" id="lab-coverage-map" role="img" aria-label="${h(`共 ${plan.symbol_count || 0} 个标的，${plan.repair_symbol_count || 0} 个存在数据缺口`)}"></canvas><div class="lab-coverage-tooltip" id="lab-coverage-tooltip" hidden></div></div>
      <div class="lab-coverage-legend"><span><i data-health="complete"></i>完整 ${Number(counts.complete || 0)}</span><span><i data-health="warmup"></i>预热缺口 ${Number(counts.warmup || 0)}</span><span><i data-health="critical"></i>关键缺口 ${Number(counts.critical || 0)}</span><span><i data-health="missing"></i>缺失/损坏 ${Number(counts.missing || 0)}</span></div>
      <details class="lab-gap-list"><summary>查看缺口标的与区间 <span>${gaps.length.toLocaleString()} 条</span></summary><div class="lab-gap-list-head"><span>本地只显示前 160 条；完整清单可复制。</span><button type="button" data-copy-coverage>复制完整缺口</button></div><div>${gaps.slice(0, 160).map(item => `<p><b>${h(item.symbol)}</b><span>${h((item.segments || []).map(segment => `${segment.start}→${segment.end}${segment.kind === 'warmup' ? ' 预热' : ''}`).join(' · '))}</span><small>${Number(item.missing_sessions || 0).toLocaleString()} 日</small></p>`).join('')}</div></details>
      <div class="lab-repair-scope" role="group" aria-label="数据补齐范围"><button type="button" data-coverage-scope="critical" aria-pressed="${!includeWarmup}"><b>关键缺口</b><span>${Number(plan.critical_repair_symbol_count || 0).toLocaleString()} 只 · 推荐</span></button><button type="button" data-coverage-scope="all" aria-pressed="${includeWarmup}"><b>完整补齐</b><span>${Number(plan.repair_symbol_count || 0).toLocaleString()} 只 · 含预热段</span></button></div>
      <div class="lab-provider-actions">${providers.map(item => {
        const blocked = !item.available || (plan.membership_missing && !item.can_fill_membership);
        const requests = includeWarmup ? item.estimated_requests : item.estimated_critical_requests;
        return `<button type="button" data-coverage-repair="${h(item.id)}" ${blocked || !repairCount ? 'disabled' : ''}><span>${h(item.label)}</span><b>补齐 ${repairCount.toLocaleString()} 只</b><small>最多约 ${Number(requests || 0).toLocaleString()} 次请求 · ${h(item.reason || '')}</small></button>`;
      }).join('')}</div>
      <p class="lab-coverage-policy">只有点击上方数据源才会联网；原研究任务不会隐式补数。补齐结果写入本地数据池并重新生成快照。</p>
    </section>`;
  }

  function renderCoverageMap(plan) {
    const canvas = document.getElementById('lab-coverage-map');
    const tooltip = document.getElementById('lab-coverage-tooltip');
    const cells = plan?.cells || [];
    if (!canvas || !tooltip || !cells.length) return;
    const width = Math.max(280, canvas.parentElement.clientWidth);
    const pitch = width < 520 ? 6 : 7;
    const columns = Math.max(20, Math.floor(width / pitch));
    const rows = Math.ceil(cells.length / columns);
    const height = Math.max(34, rows * pitch);
    const ratio = Math.min(2, window.devicePixelRatio || 1);
    canvas.style.width = `${width}px`;
    canvas.style.height = `${height}px`;
    canvas.width = Math.round(width * ratio);
    canvas.height = Math.round(height * ratio);
    const context = canvas.getContext('2d');
    context.scale(ratio, ratio);
    const colors = {complete:'#315f52', warmup:'#8b7435', critical:'#9e483f', missing:'#d15b4d'};
    cells.forEach((item, index) => {
      const x = (index % columns) * pitch;
      const y = Math.floor(index / columns) * pitch;
      context.fillStyle = colors[item.health] || colors.complete;
      context.fillRect(x, y, pitch - 1, pitch - 1);
    });
    const locate = event => {
      const rect = canvas.getBoundingClientRect();
      const column = Math.floor((event.clientX - rect.left) / pitch);
      const row = Math.floor((event.clientY - rect.top) / pitch);
      return {item:cells[row * columns + column], x:event.clientX - rect.left, y:event.clientY - rect.top};
    };
    canvas.addEventListener('pointermove', event => {
      const found = locate(event);
      if (!found.item) { tooltip.hidden = true; return; }
      const labels = {complete:'完整', warmup:'预热缺口', critical:'关键缺口', missing:'缺失/损坏'};
      tooltip.innerHTML = `<b>${h(found.item.symbol)}</b><span>${h(labels[found.item.health] || found.item.health)} · 缺 ${Number(found.item.missing_sessions || 0)} 日</span><small>需要 ${h((found.item.required || []).join(' → '))}<br>本地 ${h((found.item.available || []).filter(Boolean).join(' → ') || '无')}</small>`;
      tooltip.style.transform = `translate(${Math.min(width - 210, found.x + 10)}px,${Math.max(4, found.y - 70)}px)`;
      tooltip.hidden = false;
    });
    canvas.addEventListener('pointerleave', () => { tooltip.hidden = true; });
  }

  function renderPreflight(report, label) {
    const target = document.getElementById('lab-preflight-body');
    const confirm = document.getElementById('lab-preflight-confirm');
    const blockers = report.blockers || [];
    const warnings = report.warnings || [];
    const estimate = report.estimate || {};
    const dataset = report.dataset || {};
    const compute = report.compute || {};
    const providerChoiceRequired = Boolean(
      report.operation === 'prepare_data'
      && (report.coverage?.repair_symbol_count || report.coverage?.membership_missing)
    );
    document.getElementById('lab-preflight-title').textContent = label || '确认运行条件';
    target.innerHTML = `<div class="lab-preflight-summary"><div><span>${report.runnable ? '可以运行' : '暂时不能运行'}</span><b>${h(dataset.universe || '本地研究')} · ${Number(dataset.symbol_count || estimate.symbols || 0).toLocaleString()} 标的</b><small>快照截至 ${h(dataset.as_of || '未知')} · ${h(report.data_policy || 'prefer_local')} · ${h(report.resource_class || 'cpu').toUpperCase()}</small></div><div class="lab-preflight-device">${h(compute.effective_device || 'cpu')}</div></div>
      <div class="lab-preflight-issues">${[
        ...blockers.map(item => ({...item, blocker:true})),
        ...warnings.map(item => ({...item, blocker:false})),
      ].map(item => `<div class="lab-preflight-issue ${item.blocker ? 'blocker' : ''}"><b>${h(item.message || item.code)}</b><p>${h(item.action || '确认后继续')}</p><small>${h(item.code || 'NOTICE')}</small></div>`).join('') || '<div class="lab-preflight-issue"><b>未发现阻塞项</b><p>任务会使用当前冻结快照和已显示的计算设备。</p><small>READY</small></div>'}</div>
      ${coverageRepairMarkup(report.coverage)}
      <div class="lab-preflight-estimate"><span>${Number(estimate.sessions || 0).toLocaleString()} 交易日</span><span>${Number(estimate.samples || 0).toLocaleString()} 样本</span><span>特征 ${h(formatBytes(estimate.feature_bytes))}</span><span>磁盘 ${h(formatBytes(estimate.disk_bytes))}</span>${report.operation === 'prepare_data' ? `<span>预计峰值 ${h(formatBytes(estimate.required_peak_bytes))}</span><span>当前可用 ${h(formatBytes(estimate.disk_free_bytes))}</span><span>安全预留 ${h(formatBytes(estimate.reserve_bytes))}</span>` : ''}</div>`;
    confirm.disabled = !report.runnable || providerChoiceRequired;
    confirm.textContent = providerChoiceRequired
      ? '请选择补齐数据源'
      : report.runnable ? (warnings.length ? '使用当前快照运行' : '确认并运行') : '请先补齐数据';
    if (report.coverage) renderCoverageMap(report.coverage);
  }

  async function confirmPreflight(operation, params, label) {
    try {
      const report = await request('/api/v1/lab/preflight', {
        method:'POST', body:JSON.stringify({operation, params}), requestKey:'preflight',
      });
      state.coverageRepairScope = 'critical';
      state.preflightContext = {operation, params:{...params}, report};
      renderPreflight(report, label || kindLabel[operation] || '任务预检');
      const dialog = document.getElementById('lab-preflight-dialog');
      if (!dialog.open) dialog.showModal();
      (report.runnable
        ? document.getElementById('lab-preflight-confirm')
        : dialog.querySelector('[data-coverage-repair]:not(:disabled),[data-preflight-close]'))?.focus({preventScroll:true});
      return await new Promise(resolve => {
        if (state.preflightResolver) state.preflightResolver(false);
        state.preflightResolver = resolve;
      });
    } catch (error) {
      showError('任务预检失败', error);
      return false;
    }
  }

  async function startCoverageRepair(provider, button) {
    const context = state.preflightContext;
    const coverage = context?.report?.coverage;
    if (!coverage) return;
    button.disabled = true;
    const previous = button.innerHTML;
    button.innerHTML = `<span>${h(button.querySelector('span')?.textContent || provider)}</span><b>正在创建补齐任务…</b><small>原研究任务不会同时运行</small>`;
    try {
      const job = await request('/api/v1/lab/jobs', {
        method:'POST', body:JSON.stringify({kind:'prepare_data', params:{
          universe:coverage.universe,
          start:coverage.start,
          end:coverage.end,
          provider,
          include_warmup:state.coverageRepairScope === 'all',
          data_policy:'refresh_missing',
        }}),
      });
      state.jobs.unshift(job);
      resolvePreflight(false);
      renderTaskTray();
      renderJobTable();
      setView('automation');
      announce(`${provider === 'tushare' ? 'Tushare' : '本机 StockDB'} 数据补齐任务已加入队列`);
      schedulePolling(true);
    } catch (error) {
      button.disabled = false;
      button.innerHTML = previous;
      showError('数据补齐任务未能创建', error);
    }
  }

  async function copyCoverageGaps(button) {
    const coverage = state.preflightContext?.report?.coverage;
    if (!coverage) return;
    const rows = (coverage.gaps || []).flatMap(item => (item.segments || []).map(segment =>
      `${item.symbol}\t${segment.kind}\t${segment.start}\t${segment.end}\t${item.missing_sessions || 0}`
    ));
    await copyText(['symbol\tkind\tstart\tend\tmissing_sessions', ...rows].join('\n'));
    const previous = button.textContent;
    button.textContent = `已复制 ${rows.length} 段`;
    window.setTimeout(() => { button.textContent = previous; }, 1600);
  }

  function setView(view) {
    document.querySelectorAll('[data-lab-view]').forEach(button => {
      button.classList.toggle('active', button.dataset.labView === view);
      if (button.dataset.labView === view) button.setAttribute('aria-current', 'page');
      else button.removeAttribute('aria-current');
    });
    document.querySelectorAll('[data-lab-panel]').forEach(panel => {
      panel.classList.toggle('active', panel.dataset.labPanel === view);
    });
    if (view === 'automation') refreshJobs();
    if (view === 'optimization') refreshStudies();
    if (view === 'discover') refreshMiningRuns();
    if (view === 'library' && !state.factors.length) refreshFactors();
  }

  function renderReadiness() {
    const dashboard = state.dashboard || {};
    const admission = dashboard.preflight || {};
    const capabilities = dashboard.readiness || {};
    const models = capabilities.models || {};
    const dataset = admission.dataset || {};
    const worker = dashboard.worker || {};
    const firstIssue = (admission.blockers || admission.warnings || [])[0] || {};
    const cuda = models.gpu || {};
    const dependencyReady = Boolean(models.sklearn && models.torch && capabilities.optuna);
    const cards = [
      {
        label:'数据快照', state:admission.runnable ? (admission.state === 'ready' ? 'ready' : 'degraded') : 'blocked',
        title:`${Number(dataset.symbol_count || 0).toLocaleString()} 标的 · ${dataset.state || '未知'}`,
        detail:`as_of ${dataset.as_of || '未知'}${firstIssue.action ? ` · ${firstIssue.action}` : ''}`,
      },
      {
        label:'计算设备', state:cuda.available ? 'ready' : models.torch ? 'degraded' : 'blocked',
        title:cuda.available ? (cuda.name || 'CUDA 可用') : (models.sklearn ? 'CPU / Ridge 可用' : '模型后端缺失'),
        detail:`请求 ${models.requested_device || 'auto'} · 实际 ${models.device || 'cpu'}`,
      },
      {
        label:'研究依赖', state:dependencyReady ? 'ready' : models.sklearn ? 'degraded' : 'blocked',
        title:dependencyReady ? 'PyTorch · Optuna · Ridge' : '部分能力需要安装',
        detail:`LLM ${capabilities.llm?.configured ? '已配置' : '按需配置'} · Tushare ${capabilities.tushare?.configured ? '已配置' : '按需配置'}`,
      },
      {
        label:'任务 Worker', state:worker.status === 'running' ? 'ready' : worker.status === 'draining' ? 'degraded' : 'blocked',
        title:worker.status === 'running' ? `就绪 · ${worker.active_job_ids?.length || 0} 个活动任务` : (worker.status || '未启动'),
        detail:`最大并发 ${worker.max_workers || '—'} · ${worker.accepting ? '正在接收任务' : '未接收新任务'}`,
      },
    ];
    const target = document.getElementById('lab-readiness-grid');
    target.innerHTML = cards.map(card => `<div class="lab-readiness-card ${card.state}"><span>${h(card.label)}</span><strong title="${h(card.title)}">${h(card.title)}</strong><small>${h(card.detail)}</small></div>`).join('');
    target.setAttribute('aria-busy', 'false');
    const stateTarget = document.getElementById('lab-readiness-state');
    stateTarget.className = admission.state || 'blocked';
    stateTarget.textContent = admission.runnable ? (admission.state === 'ready' ? '全部就绪' : '可运行 · 有提示') : '存在阻塞项';
  }

  function renderSnapshot() {
    const target = document.getElementById('lab-snapshot-card');
    const dashboard = state.dashboard || {};
    const snapshot = dashboard.snapshot?.snapshot_hash
      ? dashboard.snapshot : (dashboard.preflight?.dataset || {});
    const admission = dashboard.preflight || {};
    const issue = (admission.blockers || admission.warnings || [])[0] || {};
    const snapshotState = snapshot.state || admission.state || 'unknown';
    target.innerHTML = `<div class="lab-snapshot-head"><div><span>当前冻结快照</span><b>${h(snapshot.universe || state.overview?.research?.universe || '—')}</b><small>${h(snapshot.start || state.overview?.research?.start || '—')} → ${h(snapshot.end || '今天')}</small></div><span class="lab-snapshot-state ${h(snapshotState)}">${h(String(snapshotState).toUpperCase())}</span></div>
      <dl class="lab-snapshot-grid"><div><dt>实际 as_of</dt><dd>${h(snapshot.as_of || '未知')}</dd></div><div><dt>历史标的</dt><dd>${Number(snapshot.symbol_count || 0).toLocaleString()}</dd></div><div><dt>本地大小</dt><dd>${h(formatBytes(snapshot.bytes))}</dd></div><div><dt>生产资格</dt><dd>${snapshot.production_eligible ? '可进入门禁' : '仅限研究'}</dd></div></dl>
      <p class="lab-snapshot-action">${h(issue.action || '文件身份未变化时，验证、训练与优化会复用同一份快照。')}</p>`;
  }

  function renderCapabilities() {
    const capabilities = state.overview?.capabilities;
    if (!capabilities) return;
    const available = new Set(capabilities.models?.available_models || []);
    const items = [
      ['safe-dsl', '安全 DSL', true],
      ['pit', 'CSI800 点时成分', capabilities.tushare?.production_membership],
      ['ridge', 'RIDGE', available.has('ridge')],
      ['torch', '深度学习', capabilities.models?.torch],
      ['llm', `AI · ${capabilities.llm?.provider || '未配置'}`, capabilities.llm?.configured],
      ['python-miner', 'Python AutoMiner', capabilities.python_mining_enabled],
    ];
    document.getElementById('lab-capabilities').innerHTML = items.map(item =>
      `<span class="lab-capability ${item[2] ? 'ready' : 'warning'}" data-capability="${item[0]}"><i></i>${h(item[1])} · ${item[2] ? '就绪' : '按需配置'}</span>`
    ).join('');
    syncPythonMiningGate();
  }

  function syncPythonMiningGate() {
    const form = document.getElementById('lab-discovery-form');
    const gate = document.getElementById('lab-python-gate');
    if (!form || !gate) return;
    const selected = form.elements.method?.value === 'python';
    const enabled = Boolean(state.overview?.capabilities?.python_mining_enabled);
    gate.hidden = !selected || enabled;
    const submit = form.querySelector('[type=submit]');
    if (submit) submit.textContent = selected && !enabled ? '需要先启用 AutoMiner' : '加入发现队列';
  }

  function renderOverview() {
    if (!state.overview) return;
    const statuses = state.overview.factor_statuses || {};
    const total = Object.values(statuses).reduce((sum, value) => sum + Number(value || 0), 0);
    const metrics = [
      ['因子版本', total, `${state.overview.capabilities.catalog_size} 个内置基线`],
      ['待审候选', statuses.candidate || 0, '人工门控'],
      ['运行任务', state.overview.active_jobs || 0, '可恢复队列'],
      ['生产 Champion', state.overview.deployments || 0, '仅研究部署'],
    ];
    document.getElementById('lab-metrics').innerHTML = metrics.map(item =>
      `<div class="lab-metric"><span>${item[0]}</span><strong>${item[1]}</strong><small>${item[2]}</small></div>`
    ).join('');
    const stages = [
      ['草稿', (statuses.draft || 0) + (statuses.validating || 0)],
      ['候选', statuses.candidate || 0], ['已批准', statuses.approved || 0],
      ['生产', statuses.production || 0], ['归档', statuses.archived || 0],
    ];
    const peak = Math.max(1, ...stages.map(item => item[1]));
    document.getElementById('lab-pipeline').innerHTML = stages.map(item =>
      `<div class="lab-pipeline-stage"><b>${item[1]}</b><span>${item[0]}</span><i style="--ratio:${Math.max(.04, item[1] / peak)}"></i></div>`
    ).join('');
    renderJobList('lab-overview-jobs', state.jobs.slice(0, 5));
    renderModels();
    const research = state.overview.research || {};
    document.getElementById('lab-asof').textContent = `${research.universe || '—'} · ${research.start || '—'} → NOW`;
    document.getElementById('lab-budget-card').innerHTML = `
      <div class="lab-block-head"><div><span>COMPUTE BUDGET</span><h4>每日重型研究预算</h4></div><span class="lab-beta">LOCAL</span></div>
      <div class="lab-budget-number"><strong>${number(research.daily_budget_hours, 1)}</strong><span>小时 / 日</span></div>
      <div class="lab-budget-track"><i></i></div>
      <div class="lab-budget-meta"><span>${h((research.weekly_days || []).join(' · '))} 执行</span><span>${h((research.window || []).join(' → '))}</span></div>`;
  }

  function renderReturnChart(curve) {
    const target = document.getElementById('lab-return-chart');
    if (!target) return;
    const horizons = [1, 3, 5, 7, 10, 20, 30];
    const challenger = new Map((curve?.challenger || []).filter(item => !item.missing).map(item => [Number(item.horizon), item]));
    const champion = new Map((curve?.champion || []).filter(item => !item.missing).map(item => [Number(item.horizon), item]));
    const baseline = new Map((curve?.baseline || []).map(item => [Number(item.horizon), item]));
    const values = [
      ...challenger.values(), ...champion.values(), ...baseline.values(),
    ].flatMap(item => [
      Number(item.annual_net_excess_return || 0),
      ...((item.ci_95 || []).map(Number)),
    ]).filter(Number.isFinite);
    if (!challenger.size && !champion.size) {
      target.innerHTML = '<div class="lab-return-empty">尚无冻结后的组合收益证据。运行每周研究后，这里会显示七周期曲线。</div>';
      return;
    }
    const width = 760, height = 270, left = 58, right = 22, top = 18, bottom = 38;
    const minimum = Math.min(-0.05, ...values), maximum = Math.max(0.05, ...values);
    const padding = Math.max(0.02, (maximum - minimum) * 0.12);
    const low = minimum - padding, high = maximum + padding;
    const x = horizon => left + horizons.indexOf(horizon) * (width - left - right) / (horizons.length - 1);
    const y = value => top + (high - value) * (height - top - bottom) / (high - low);
    const ticks = Array.from({length:5}, (_, index) => low + index * (high - low) / 4);
    const seriesPath = (series, key) => horizons.filter(item => series.has(item)).map((horizon, index) => {
      const value = Number(series.get(horizon)[key] || 0);
      return `${index ? 'L' : 'M'}${x(horizon).toFixed(1)},${y(value).toFixed(1)}`;
    }).join(' ');
    const grid = ticks.map(value => `<line class="grid" x1="${left}" x2="${width-right}" y1="${y(value)}" y2="${y(value)}"/><text class="axis-label" x="${left-8}" y="${y(value)+3}" text-anchor="end">${(value*100).toFixed(0)}%</text>`).join('');
    const labels = horizons.map(value => `<text class="axis-label" x="${x(value)}" y="${height-13}" text-anchor="middle">${value}D</text>`).join('');
    const confidence = horizons.filter(value => challenger.has(value)).map(value => {
      const item = challenger.get(value), ci = item.ci_95 || [0, 0];
      return `<line class="confidence" x1="${x(value)}" x2="${x(value)}" y1="${y(Number(ci[1] || 0))}" y2="${y(Number(ci[0] || 0))}"/>`;
    }).join('');
    const points = horizons.filter(value => challenger.has(value)).map(value => {
      const item = challenger.get(value), annual = Number(item.annual_net_excess_return || 0);
      const title = `${value}日 · 年化净超额 ${percent(annual)} · 净IR ${number(item.net_information_ratio)} · 最大回撤 ${percent(item.max_drawdown)} · 换手 ${percent(item.turnover)} · 成本 ${percent(item.cost_annual)} · 样本 ${item.samples || 0}`;
      return `<circle class="point" cx="${x(value)}" cy="${y(annual)}" r="4.5"><title>${h(title)}</title></circle>`;
    }).join('');
    target.innerHTML = `<svg viewBox="0 0 ${width} ${height}" aria-hidden="true">${grid}${labels}<line class="grid" x1="${left}" x2="${width-right}" y1="${y(0)}" y2="${y(0)}"/>${confidence}<path class="baseline-line" d="${seriesPath(baseline, 'annual_net_excess_return')}"/><path class="champion-line" d="${seriesPath(champion, 'annual_net_excess_return')}"/><path class="challenger-line" d="${seriesPath(challenger, 'annual_net_excess_return')}"/>${points}</svg>`;
  }

  function renderStrategyWorkbench() {
    const workbench = state.workbench || {horizons:[1,3,5,7,10,20,30], matrix:[]};
    const matrix = workbench.matrix || [];
    const selected = matrix.find(item => Number(item.horizon) === Number(state.selectedHorizon)) || matrix[0] || {};
    state.selectedHorizon = Number(selected.horizon || state.selectedHorizon);
    document.getElementById('lab-strategy-horizons').innerHTML = (workbench.horizons || []).map(value =>
      `<button type="button" role="tab" data-strategy-horizon="${value}" aria-selected="${Number(value) === state.selectedHorizon}">${value} 日</button>`
    ).join('');
    const metrics = selected.metrics || {};
    const failures = selected.gates?.failures || [];
    const promotion = selected.status === 'shadow_challenger'
      ? `<button class="lab-button lab-button-quiet" type="button" data-promote-strategy="${h(selected.strategy_id)}" data-promotion-target="paper">申请进入 Paper</button>`
      : selected.status === 'paper'
        ? `<button class="lab-button lab-button-quiet" type="button" data-promote-strategy="${h(selected.strategy_id)}" data-promotion-target="champion">申请成为 Champion</button>` : '';
    document.getElementById('lab-horizon-evidence').innerHTML = selected.strategy_id ? `
      <div class="lab-evidence-status"><div><small>${state.selectedHorizon} 日独立证据</small><b>${h(statusLabel[selected.status] || selected.status)}</b></div><span class="${h(selected.status)}">${selected.gates?.passed ? 'SEALED PASS' : 'NOT PROMOTABLE'}</span></div>
      <div class="lab-evidence-stats"><div><span>年化净超额</span><b>${percent(metrics.net_annual_excess_return)}</b></div><div><span>净 IR</span><b>${number(metrics.net_information_ratio)}</b></div><div><span>最大回撤</span><b>${percent(metrics.max_drawdown)}</b></div><div><span>年化成本</span><b>${percent(metrics.cost_annual)}</b></div><div><span>正收益折</span><b>${Number(metrics.positive_folds || 0)} / 4</b></div><div><span>影子成熟日</span><b>${Number(selected.shadow?.matured_signal_days || 0)}</b></div></div>
      <p class="lab-evidence-gate ${selected.gates?.passed ? 'pass' : ''}">${selected.gates?.passed ? '密封门槛通过；自动进入影子，Paper 与 Champion 仍需人工确认。' : h(failures[0] || '该周期尚无可晋级组合，不能借用其他周期证据。')}</p>${promotion}` : '<div class="lab-empty">该周期尚无 3–8 个去相关合格因子，不能构建可信组合。</div>';
    const funnel = workbench.funnel || {};
    const funnelStages = [
      ['历史候选', funnel.historical_candidate || 0], ['Shadow', funnel.shadow_challenger || 0],
      ['Paper', funnel.paper || 0], ['Champion', funnel.champion || 0],
      ['降级/退役', (funnel.degraded || 0) + (funnel.retired || 0)],
    ];
    document.getElementById('lab-strategy-funnel').innerHTML = funnelStages.map(item => `<div><b>${item[1]}</b><span>${item[0]}</span></div>`).join('');
    const portfolio = workbench.portfolio || {};
    const actionSource = document.getElementById('lab-action-source');
    if (actionSource) {
      actionSource.textContent = portfolio.source === 'local_real_ledger'
        ? `${portfolio.reliable ? '本地真实账本' : '本地账本需复核'} · ${Number(portfolio.holding_count || 0)} 只 · ${portfolio.valuation_date || '未知日期'}`
        : '等待每日评分读取本地账本';
      actionSource.title = (portfolio.warnings || []).join('；');
    }
    const actions = (workbench.latest_actions || []).filter(item => Number(item.horizon) === state.selectedHorizon);
    const actionLabels = {buy:'买入', add:'加仓', hold:'持有', reduce:'减仓', exit:'退出', review:'人工复核'};
    document.getElementById('lab-action-list').innerHTML = actions.length ? actions.map(item => `<div class="lab-action-row"><b>${h(item.symbol)}</b><strong class="${h(item.action)}">${h(actionLabels[item.action] || item.action)}</strong><span>当前 ${percent(item.current_weight)}</span><span>目标 ${percent(item.target_weight)}</span><span>差异 ${percent(item.difference)}</span></div>`).join('') : '<div class="lab-empty">该周期尚无最新影子动作；数据不足时不会强制退出。</div>';
    document.getElementById('lab-horizon-matrix').innerHTML = matrix.map(item => `<button type="button" class="lab-horizon-cell ${Number(item.horizon) === state.selectedHorizon ? 'active' : ''}" data-strategy-horizon="${item.horizon}"><b>${item.horizon}D</b><span>${h(statusLabel[item.status] || item.status)}</span><small>${item.strategy_id ? `${percent(item.metrics?.net_annual_excess_return)} · IR ${number(item.metrics?.net_information_ratio)}` : h(item.outcome?.reason || '等待组合')}</small></button>`).join('');
    renderReturnChart(workbench.return_curve || {});
  }

  function syncResearchForms() {
    if (state.formsDirty) return;
    const research = state.overview?.research || {};
    const horizons = (research.horizons || [3]).map(Number);
    const preferred = horizons.includes(3) ? 3 : horizons[0];
    for (const id of ['lab-discovery-form', 'lab-optimize-form']) {
      const form = document.getElementById(id);
      if (!form) continue;
      if (research.universe) form.elements.universe.value = research.universe;
      if (research.start) form.elements.start.value = research.start;
      const select = form.elements.horizon;
      const selected = horizons.includes(Number(select.value)) ? Number(select.value) : preferred;
      select.innerHTML = horizons.map(value =>
        `<option value="${value}" ${value === selected ? 'selected' : ''}>${value} 日</option>`
      ).join('');
    }
  }

  function renderMlSetup() {
    const target = document.getElementById('lab-ml-setup');
    const models = state.overview?.capabilities?.models || {};
    if (!target) return;
    target.classList.toggle('ready', Boolean(models.torch));
    if (models.torch) {
      const names = (models.available_models || []).map(key => modelMeta[key]?.[0] || key).join(' · ');
      target.innerHTML = `<div class="lab-ml-setup-summary"><i class="lab-ml-setup-status"></i><div><b>深度学习后端已就绪</b><span>${h(names || 'PyTorch 模型可用')}；可直接创建训练任务。</span></div></div>
        <span class="lab-ml-setup-device">DEVICE · ${h(models.device || 'CPU')}</span>`;
      return;
    }
    const title = models.sklearn ? '当前可运行 Ridge；深度模型还未安装' : '模型后端未完整安装';
    const detail = models.sklearn
      ? '按顺序执行以下命令即可启用 MLP、TCN、GRU、Transformer 与 DAE。'
      : '先安装机器学习依赖，再诊断环境并启动网页服务或独立研究 Worker。';
    const commands = [
      ['01', 'lab-ml-install-command', 'python -m pip install -e ".[data,ml]"', '复制安装命令'],
      ['02', 'lab-ml-doctor-command', 'qm lab doctor', '复制诊断命令'],
      ['03', 'lab-ml-serve-command', 'qm serve', '复制网页服务命令'],
      ['04', 'lab-ml-worker-command', 'qm lab worker', '复制独立 Worker 命令'],
    ];
    target.innerHTML = `<div class="lab-ml-setup-summary"><i class="lab-ml-setup-status"></i><div><b>${h(title)}</b><span>${h(detail)} 安装后请重启正在运行的网页服务。GPU 用户请使用 <a class="lab-ml-setup-link" href="https://docs.pytorch.org/get-started/locally/" target="_blank" rel="noopener noreferrer">PyTorch 官方安装选择器</a>。</span></div></div>
      <div class="lab-ml-setup-steps">${commands.map(command => `<div class="lab-ml-command"><span>${command[0]}</span><code id="${command[1]}">${h(command[2])}</code><button type="button" data-copy-target="${command[1]}" aria-label="${command[3]}">复制</button></div>`).join('')}</div>`;
  }

  function renderModels() {
    const availableModels = state.overview?.capabilities?.models?.available_models || [];
    const available = new Set(availableModels);
    if (!available.has(state.selectedModel) && availableModels.length) state.selectedModel = availableModels[0];
    renderMlSetup();
    const ribbon = document.getElementById('lab-model-ribbon');
    if (ribbon) ribbon.innerHTML = Object.entries(modelMeta).map(([key, meta]) =>
      `<div class="lab-model-chip ${available.has(key) ? 'available' : ''}"><b>${meta[0]}</b><span>${available.has(key) ? 'READY' : 'ML OPTIONAL'}</span></div>`
    ).join('');
    const grid = document.getElementById('lab-model-grid');
    if (grid) grid.innerHTML = Object.entries(modelMeta).map(([key, meta]) =>
      `<button type="button" class="lab-model-card ${state.selectedModel === key && available.has(key) ? 'active' : ''} ${available.has(key) ? 'available' : ''}" data-model="${key}" aria-disabled="${available.has(key) ? 'false' : 'true'}">
        <b>${meta[0]}</b><span>${available.has(key) ? 'READY' : 'OPTIONAL'}</span><p>${meta[1]}</p></button>`
    ).join('');
  }

  function renderJobList(targetId, jobs) {
    const target = document.getElementById(targetId);
    if (!target) return;
    if (!jobs.length) {
      target.innerHTML = '<div class="lab-empty">暂无研究任务</div>';
      return;
    }
    target.innerHTML = jobs.map(job => {
      const device = job.telemetry?.effective_device || job.preflight?.compute?.effective_device || 'cpu';
      const resource = job.resource_class || job.preflight?.resource_class || 'cpu';
      const checkpoint = job.checkpoint || job.result?.checkpoint || {};
      const partitionText = checkpoint.persisted != null
        ? ` · 已持久化 ${checkpoint.persisted}${checkpoint.total != null ? `/${checkpoint.total}` : ''}` : '';
      return `<button type="button" class="lab-job-row ${h(job.status)}" data-job-detail="${h(job.id)}">
      <span><b>${h(kindLabel[job.kind] || job.kind)}</b><small>${h(resource.toUpperCase())} · ${h(device)} · ${h(jobPhase(job))}${h(partitionText)} · ${h(formatDate(job.created_at))}</small></span>
      <strong>${job.progress || 0}%</strong></button>`;
    }).join('');
  }

  function renderJobTable() {
    const target = document.getElementById('lab-job-table');
    if (!target) return;
    if (!state.jobs.length) {
      target.innerHTML = '<div class="lab-empty">暂无任务。可从 AI 发现或模型实验创建。</div>';
      return;
    }
    target.innerHTML = `<div class="table-scroll"><table class="lab-job-table"><thead><tr><th>任务</th><th>状态</th><th>资源 / 设备</th><th>阶段</th><th>进度</th><th>创建</th><th>操作</th></tr></thead><tbody>${state.jobs.map(job => {
      const device = job.telemetry?.effective_device || job.preflight?.compute?.effective_device || 'cpu';
      const resource = job.resource_class || job.preflight?.resource_class || 'cpu';
      return `<tr>
      <td><button class="lab-job-link" type="button" data-job-detail="${h(job.id)}">${h(kindLabel[job.kind] || job.kind)}</button></td><td><span class="lab-status ${h(job.status)}">${h(statusLabel[job.status] || job.status)}</span></td>
      <td>${h(resource.toUpperCase())} · ${h(device)}</td><td title="${h(jobPhase(job))}">${h(jobPhase(job))}</td><td>${job.progress || 0}%</td><td>${h(formatDate(job.created_at))}</td>
      <td class="lab-job-actions"><button type="button" data-job-detail="${h(job.id)}">查看</button>${activeJobStatuses.has(job.status) ? `<button class="danger" type="button" data-cancel-job="${h(job.id)}">取消</button>` : ''}</td></tr>`;
    }).join('')}</tbody></table></div>`;
  }

  function renderTaskTray() {
    const active = state.jobs.find(job => activeJobStatuses.has(job.status));
    const tray = document.getElementById('lab-task-tray');
    if (!tray) return;
    tray.hidden = !active;
    if (!active) return;
    document.getElementById('lab-task-title').textContent = kindLabel[active.kind] || active.kind;
    const checkpoint = active.checkpoint || active.result?.checkpoint || {};
    const partitionText = checkpoint.persisted != null
      ? ` · 已持久化 ${checkpoint.persisted}${checkpoint.total != null ? `/${checkpoint.total}` : ''}` : '';
    const retryText = checkpoint.safe_retry_point ? ` · 可从 ${checkpoint.safe_retry_point} 重试` : '';
    document.getElementById('lab-task-phase').textContent = `${jobPhase(active)}${partitionText}${retryText}`;
    document.getElementById('lab-task-percent').textContent = `${active.progress || 0}%`;
    document.getElementById('lab-task-fill').style.setProperty('--progress', (active.progress || 0) / 100);
    document.getElementById('lab-task-open').dataset.jobDetail = active.id;
  }

  function compactJobEvents(events) {
    const compacted = [];
    const positions = new Map();
    for (const event of events) {
      const phase = String(event.phase || '').trim();
      const stage = event.type === 'progress'
        ? phase
          .replace(/\s*·\s*\d+\s*\/\s*\d+(?:\s*·.*)?$/, '')
          .replace(/\s+\d+\s*\/\s*\d+(?:\s*·.*)?$/, '')
          .trim()
        : phase;
      const phaseUpdate = event.type === 'progress' && stage && phase !== stage
        ? phase.slice(stage.length).replace(/^\s*·?\s*/, '')
        : '';
      const compactKey = event.type === 'llm_candidate_checked'
        ? `${event.type}:${event.round || 0}`
        : event.type === 'progress' && stage
          ? `${event.type}:${stage}`
          : '';
      const position = compactKey ? positions.get(compactKey) : undefined;
      const previous = position == null ? null : compacted[position];
      const updateCount = Math.max(1, Number(event._count) || 1);
      const detailParts = [phaseUpdate, event.detail]
        .map(value => String(value || '').trim()).filter(Boolean);
      const latestDetail = [...new Set(detailParts)].join(' · ');
      if (previous) {
        const count = previous._count + updateCount;
        const firstCreatedAt = previous._firstCreatedAt;
        previous._lastCreatedAt = event.created_at;
        Object.assign(previous, event, {
          _compactKey: compactKey, _count: count, _stage: stage,
          _latestDetail: latestDetail, _firstCreatedAt: firstCreatedAt,
          _lastCreatedAt: event._lastCreatedAt || event.created_at,
        });
      } else {
        compacted.push({
          ...event, _compactKey: compactKey, _count: updateCount, _stage: stage,
          _latestDetail: latestDetail,
          _firstCreatedAt: event._firstCreatedAt || event.created_at,
          _lastCreatedAt: event._lastCreatedAt || event.created_at,
        });
        if (compactKey) positions.set(compactKey, compacted.length - 1);
      }
    }
    return compacted;
  }

  function renderJobParams(params) {
    const clean = redactObject(params || {});
    const config = clean.config && typeof clean.config === 'object' ? clean.config : {};
    const entries = [
      ...Object.entries(clean).filter(([key]) => key !== 'config' && !key.startsWith('_')),
      ...Object.entries(config),
    ];
    if (!entries.length) return '<div class="lab-job-empty-line">本任务没有额外参数</div>';
    return `<dl class="lab-job-param-grid">${entries.map(([key, value]) => `
      <div><dt>${h(paramLabel[key] || key)}</dt><dd>${h(displayValue(key, value))}</dd></div>`).join('')}</dl>`;
  }

  function jobErrorCopy(job) {
    const structured = job.error_info && typeof job.error_info === 'object' ? job.error_info : {};
    if (structured.code) return {
      what: structured.message || job.error || '任务未能完成',
      why: `错误代码 ${structured.code}${structured.retryable ? ' · 修复后可重试' : ''}`,
      how: structured.action || '查看最后事件与本机日志后重试。',
    };
    const raw = String(job.error || job.detail || '任务未能完成');
    const timeout = /timed?\s*out|timeout|read operation|超时/i.test(raw);
    if (timeout) return {
      what: '模型服务在本轮最长等待时间内没有返回完整响应。',
      why: '网络、模型排队或生成时间过长都可能触发；系统已按 180 / 240 / 360 / 480 秒窗口自动尝试。',
      how: '确认模型服务可用后可按原参数重跑。若已有前序轮次结果，任务会以“部分完成”保留它们。',
    };
    return {
      what: raw,
      why: '执行器已保存停止前的阶段、参数与事件，可从下方时间线定位最后一个成功步骤。',
      how: '先检查错误与最后事件；修复数据或模型配置后，按原参数重新运行。',
    };
  }

  function renderJobResult(job) {
    const result = job.result && typeof job.result === 'object' ? job.result : {};
    const warnings = Array.isArray(result.warnings) ? result.warnings : [];
    const candidates = Array.isArray(result.candidates) ? result.candidates : [];
    const resultItems = [];
    if (result.snapshot) resultItems.push(['数据快照', String(result.snapshot).slice(0, 16)]);
    if (result.method) resultItems.push(['研究方法', String(result.method).toUpperCase()]);
    if (candidates.length) resultItems.push(['候选产出', `${candidates.length} 个`]);
    if (result.rounds_requested) resultItems.push(['完成轮次', `${result.rounds_completed || 0} / ${result.rounds_requested}`]);
    if (Number.isFinite(Number(result.attempts))) resultItems.push(['模型调用', `${result.attempts} 次`]);
    if (Number.isFinite(Number(result.llm_calls))) resultItems.push(['模型调用', `${result.llm_calls} / 3 次`]);
    if (Number.isFinite(Number(result.candidate_count))) resultItems.push(['代码候选', `${result.candidate_count} 个`]);
    if (Number.isFinite(Number(result.finalist_count))) resultItems.push(['Pareto 入围', `${result.finalist_count} 个`]);
    if (result.experiment_id) resultItems.push(['实验编号', String(result.experiment_id).slice(0, 16)]);
    if (result.version_id) resultItems.push(['产出版本', String(result.version_id).slice(0, 16)]);
    if (result.study_id) resultItems.push(['优化 Study', String(result.study_id).slice(0, 16)]);
    if (Array.isArray(result.trials)) resultItems.push(['已完成 Trials', `${result.trials.length} 个`]);
    if (result.sealed_metrics?.net_information_ratio != null) resultItems.push(['密封净 IR', number(result.sealed_metrics.net_information_ratio, 3)]);
    if (result.metrics?.correlation != null) resultItems.push(['相关性', number(result.metrics.correlation, 4)]);
    if (result.metrics?.mse != null) resultItems.push(['验证 MSE', number(result.metrics.mse, 6)]);
    const checkpoint = result.checkpoint && typeof result.checkpoint === 'object'
      ? result.checkpoint : {};
    const storage = result.storage && typeof result.storage === 'object'
      ? result.storage : (job.preflight?.storage || {});
    const partitions = result.partitions && typeof result.partitions === 'object'
      ? result.partitions : {};
    const persistedRaw = result.persisted_partitions ?? partitions.persisted;
    const partitionTotalRaw = result.total_partitions ?? partitions.total;
    const persisted = persistedRaw == null ? NaN : Number(persistedRaw);
    const partitionTotal = partitionTotalRaw == null ? NaN : Number(partitionTotalRaw);
    if (result.stage || checkpoint.stage) resultItems.push(['持久化阶段', result.stage || checkpoint.stage]);
    if (Number.isFinite(persisted)) {
      resultItems.push(['已持久化分区', Number.isFinite(partitionTotal) ? `${persisted} / ${partitionTotal}` : `${persisted}`]);
    }
    if (result.safe_retry_point || checkpoint.safe_retry_point) {
      resultItems.push(['安全重试点', result.safe_retry_point || checkpoint.safe_retry_point]);
    }
    if (storage.purpose) resultItems.push(['存储用途', storage.purpose]);
    if (storage.instance) resultItems.push(['运行实例', storage.instance]);
    if (storage.access) resultItems.push(['读写模式', storage.access]);
    if (storage.display_path) resultItems.push(['存储位置', storage.display_path]);
    const estimatedBytes = Number(storage.estimated_bytes ?? result.estimated_bytes);
    const freeBytes = Number(storage.free_bytes ?? result.free_bytes);
    if (Number.isFinite(estimatedBytes)) resultItems.push(['预计峰值空间', formatBytes(estimatedBytes)]);
    if (Number.isFinite(freeBytes)) resultItems.push(['卷剩余空间', formatBytes(freeBytes)]);
    const telemetry = job.telemetry || result.telemetry || {};
    if (telemetry.effective_device) resultItems.push(['实际设备', telemetry.effective_device]);
    if (telemetry.gpu_name) resultItems.push(['GPU', telemetry.gpu_name]);
    if (telemetry.amp) resultItems.push(['混合精度', String(telemetry.amp).toUpperCase()]);
    if (telemetry.peak_gpu_memory_mb > 0) resultItems.push(['峰值显存', `${number(telemetry.peak_gpu_memory_mb, 1)} MiB`]);
    if (telemetry.samples_per_second > 0) resultItems.push(['训练吞吐', `${number(telemetry.samples_per_second, 1)} samples/s`]);
    if (result.snapshot?.snapshot_hash) resultItems.push(['数据快照', String(result.snapshot.snapshot_hash).slice(0, 16)]);
    if (!Object.keys(result).length && job.status !== 'failed') {
      return '<div class="lab-job-empty-line">任务结束后将在这里显示候选、指标或数据快照。</div>';
    }
    return `${warnings.length ? `<div class="lab-job-warning"><b>结果已保留，但有未完成项</b>${warnings.map(item => `<p>${h(typeof item === 'object' ? item.message || JSON.stringify(item) : item)}</p>`).join('')}</div>` : ''}
      ${resultItems.length ? `<dl class="lab-job-result-grid">${resultItems.map(item => `<div><dt>${h(item[0])}</dt><dd>${h(item[1])}</dd></div>`).join('')}</dl>` : ''}
      ${candidates.length ? `<div class="lab-job-candidates"><span>候选版本</span>${candidates.map((item, index) => `<button type="button" data-factor-version="${h(item.id || item.version_id || '')}">${h(item.name || `候选 ${index + 1}`)}</button>`).join('')}</div>` : ''}
      ${result.version_id ? `<div class="lab-job-candidates"><span>模型版本</span><button type="button" data-factor-version="${h(result.version_id)}">查看因子证据</button></div>` : ''}
      <details class="lab-job-raw"><summary>查看原始结果 JSON</summary><pre>${h(JSON.stringify(redactObject(result), null, 2))}</pre></details>`;
  }

  function renderJobTimeline(events) {
    if (!events.length) return '<div class="lab-job-empty-line">尚无执行事件</div>';
    return `<ol class="lab-job-timeline">${events.map(event => {
      const type = String(event.type || 'progress');
      const description = type === 'progress'
        ? event._stage || event.phase || eventLabel[type]
        : event.detail || event.phase || event.message || eventLabel[type] || type;
      const attempt = event.attempt ? ` · 尝试 ${event.attempt}/${event.max_attempts || 4}` : '';
      const timeout = event.timeout_seconds ? ` · 最长 ${event.timeout_seconds} 秒` : '';
      const detail = type === 'progress' ? event._latestDetail : '';
      return `<li class="${h(type)}"><i></i><div><span>${h(eventLabel[type] || type)}${h(attempt)}${h(timeout)}</span><b>${h(description)}</b><small>${detail ? `${h(detail)} · ` : ''}${h(formatDate(event._lastCreatedAt, true))}${event.progress != null ? ` · ${h(event.progress)}%` : ''}</small></div></li>`;
    }).join('')}</ol>`;
  }

  function latestCheckpoint(job) {
    if (job?.checkpoint && typeof job.checkpoint === 'object') return job.checkpoint;
    if (job?.result?.checkpoint && typeof job.result.checkpoint === 'object') return job.result.checkpoint;
    const event = [...state.jobEvents].reverse().find(item => item.type === 'partition_checkpoint');
    if (!event) return {};
    return {
      stage: event.stage || event.metadata?.stage || event.phase,
      persisted: event.persisted ?? event.metadata?.persisted,
      total: event.total ?? event.metadata?.total,
      safe_retry_point: event.safe_retry_point || event.metadata?.safe_retry_point,
    };
  }

  function renderJobDetail() {
    const body = document.getElementById('lab-job-drawer-body');
    const job = state.jobDetail;
    if (!body || !job) return;
    const progress = Math.max(0, Math.min(100, Number(job.progress) || 0));
    const heartbeatAge = Date.now() - new Date(job.heartbeat_at || 0).getTime();
    const heartbeatFresh = job.status === 'running' && heartbeatAge >= 0 && heartbeatAge < 15000;
    const duration = formatDuration(job.started_at || job.created_at, job.finished_at || new Date().toISOString());
    const title = kindLabel[job.kind] || job.kind;
    const telemetry = job.telemetry || {};
    const effectiveDevice = telemetry.effective_device || job.preflight?.compute?.effective_device || 'cpu';
    const resourceClass = job.resource_class || job.preflight?.resource_class || 'cpu';
    document.getElementById('lab-job-drawer-title').textContent = title;
    document.getElementById('lab-job-drawer-kicker').textContent = `RESEARCH JOB · ${String(job.id).slice(0, 8).toUpperCase()}`;
    const errorCopy = job.status === 'failed' ? jobErrorCopy(job) : null;
    const checkpoint = latestCheckpoint(job);
    const checkpointItems = [];
    if (checkpoint.stage) checkpointItems.push(['当前持久化阶段', checkpoint.stage]);
    if (checkpoint.persisted != null) checkpointItems.push([
      '已持久化分区', checkpoint.total != null
        ? `${checkpoint.persisted} / ${checkpoint.total}` : String(checkpoint.persisted),
    ]);
    if (checkpoint.safe_retry_point) checkpointItems.push(['安全重试点', checkpoint.safe_retry_point]);
    body.innerHTML = `<div class="lab-job-summary">
        <div class="lab-job-summary-row"><span class="lab-status ${h(job.status)}">${h(statusLabel[job.status] || job.status)}</span><strong>${progress}%</strong></div>
        <div class="lab-job-detail-progress"><i style="--progress:${progress / 100}"></i></div>
        <h4>${h(job.phase || statusLabel[job.status] || job.status)}</h4>
        <p>${h(job.detail || (job.status === 'running' ? '执行器正在处理当前阶段。' : job.error || '任务记录已保存。'))}</p>
        <div class="lab-job-runtime"><span class="${heartbeatFresh ? 'live' : ''}"><i></i>${heartbeatFresh ? '执行器心跳正常' : job.worker ? `执行器 ${h(job.worker)}` : '无活动执行器'}</span><span>${h(resourceClass.toUpperCase())} · ${h(effectiveDevice)} · 耗时 ${h(duration)}</span></div>
      </div>
      ${errorCopy ? `<section class="lab-job-error"><span>FAILED</span><h4>${h(errorCopy.what)}</h4><dl><div><dt>可能原因</dt><dd>${h(errorCopy.why)}</dd></div><div><dt>下一步</dt><dd>${h(errorCopy.how)}</dd></div></dl></section>` : ''}
      <div class="lab-job-drawer-actions">
        ${activeJobStatuses.has(job.status) ? `<button class="danger" type="button" data-cancel-job="${h(job.id)}">安全停止</button>` : ''}
        ${terminalJobStatuses.has(job.status) ? `<button class="primary" type="button" data-retry-job="${h(job.id)}">按原参数重跑</button>` : ''}
      </div>
      ${checkpointItems.length ? `<section class="lab-job-detail-section"><div class="lab-job-section-head"><span>CHECKPOINT</span><h4>持久化进度</h4></div><dl class="lab-job-result-grid">${checkpointItems.map(item => `<div><dt>${h(item[0])}</dt><dd>${h(item[1])}</dd></div>`).join('')}</dl></section>` : ''}
      <section class="lab-job-detail-section"><div class="lab-job-section-head"><span>INPUT</span><h4>研究参数</h4></div>${renderJobParams(job.params)}</section>
      <section class="lab-job-detail-section"><div class="lab-job-section-head"><span>OUTPUT</span><h4>任务产出</h4></div>${renderJobResult(job)}</section>
      <section class="lab-job-detail-section"><div class="lab-job-section-head"><span>STAGES · ${state.jobEvents.length}</span><h4>执行时间线</h4></div>${renderJobTimeline(state.jobEvents)}</section>
      <section class="lab-job-detail-section lab-job-identifiers"><div class="lab-job-section-head"><span>AUDIT</span><h4>审计标识</h4></div><dl><div><dt>任务 ID</dt><dd>${h(job.id)}</dd></div><div><dt>创建时间</dt><dd>${h(formatDate(job.created_at, true))}</dd></div><div><dt>最近心跳</dt><dd>${h(formatDate(job.heartbeat_at, true))}</dd></div></dl></section>`;
  }

  async function refreshJobDetail({reset = false} = {}) {
    const jobId = state.selectedJobId;
    if (!jobId || state.jobDetailLoading) return;
    if (reset) {
      state.jobEvents = [];
      state.jobLastSeq = 0;
    }
    state.jobDetailLoading = true;
    try {
      const job = await request(`/api/v1/jobs/${encodeURIComponent(jobId)}`);
      if (state.selectedJobId !== jobId) return;
      const response = await request(`/api/v1/jobs/${encodeURIComponent(jobId)}/events?after=${state.jobLastSeq}&limit=2000`);
      if (state.selectedJobId !== jobId) return;
      const events = response.items || [];
      if (events.length) {
        state.jobLastSeq = Math.max(state.jobLastSeq, ...events.map(item => Number(item.seq) || 0));
        state.jobEvents = compactJobEvents([...state.jobEvents, ...events]);
      }
      state.jobDetail = job;
      renderJobDetail();
    } catch (error) {
      if (state.selectedJobId === jobId) {
        document.getElementById('lab-job-drawer-body').innerHTML = `<div class="lab-job-load-error"><b>任务详情读取失败</b><p>${h(error.message || error)}</p><button type="button" data-job-detail="${h(jobId)}">重试</button></div>`;
      }
    } finally {
      state.jobDetailLoading = false;
    }
  }

  function openJobDetail(jobId, opener = null) {
    if (!jobId) return;
    const changed = state.selectedJobId !== jobId;
    state.selectedJobId = jobId;
    state.jobDrawerOpener = opener || document.activeElement;
    const drawer = document.getElementById('lab-job-drawer');
    drawer.classList.add('is-open');
    drawer.setAttribute('aria-hidden', 'false');
    document.getElementById('lab-job-backdrop').hidden = false;
    if (changed) {
      state.jobDetail = state.jobs.find(job => job.id === jobId) || null;
      state.jobEvents = [];
      state.jobLastSeq = 0;
      document.getElementById('lab-job-drawer-body').innerHTML = '<div class="lab-job-loading"><i></i><span>正在读取任务、事件与产出…</span></div>';
    }
    drawer.querySelector('[data-close-job-drawer]')?.focus({preventScroll:true});
    refreshJobDetail({reset: changed});
  }

  function closeJobDetail() {
    const drawer = document.getElementById('lab-job-drawer');
    drawer.classList.remove('is-open');
    drawer.setAttribute('aria-hidden', 'true');
    document.getElementById('lab-job-backdrop').hidden = true;
    state.selectedJobId = '';
    const opener = state.jobDrawerOpener;
    state.jobDrawerOpener = null;
    if (opener?.isConnected) opener.focus({preventScroll:true});
  }

  function filteredFactors() {
    const search = state.search.trim().toLowerCase();
    return state.factors.filter(item => {
      if (state.status && item.status !== state.status) return false;
      const spec = item.spec || {};
      if (state.factorCategory && item.category !== state.factorCategory) return false;
      if (state.factorKind && item.kind !== state.factorKind) return false;
      if (state.factorValidation === 'passed' && item.validation_passed !== true) return false;
      if (state.factorValidation === 'failed' && item.validation_passed !== false) return false;
      if (state.factorValidation === 'unvalidated' && item.validation_passed != null) return false;
      if (state.factorHorizon && !(spec.horizons || []).map(Number).includes(Number(state.factorHorizon))) return false;
      if (state.factorTag && !(spec.tags || []).includes(state.factorTag)) return false;
      const searchable = [item.name, item.slug, spec.expression, spec.description, spec.rationale, ...(spec.tags || [])].join(' ');
      return !search || searchable.toLowerCase().includes(search);
    });
  }

  function factorCanCorrelate(item) {
    return item.kind === 'expression' && (item.spec?.horizons || []).map(Number).includes(state.correlationHorizon);
  }

  function syncCorrelationControls() {
    const count = state.correlationSelection.size;
    const label = document.getElementById('lab-correlation-count');
    const button = document.getElementById('lab-run-correlation');
    if (label) label.textContent = `已选 ${count} / 30`;
    if (button) button.disabled = count < 2 || count > 30;
  }

  function populateFactorFilters() {
    const options = (id, values) => {
      const select = document.getElementById(id);
      if (!select) return;
      const current = select.value;
      select.innerHTML = `<option value="">全部</option>${values.map(value => `<option value="${h(value)}">${h(value)}</option>`).join('')}`;
      select.value = values.includes(current) ? current : '';
    };
    options('lab-factor-category', [...new Set(state.factors.map(item => item.category).filter(Boolean))].sort());
    options('lab-factor-kind', [...new Set(state.factors.map(item => item.kind).filter(Boolean))].sort());
    options('lab-factor-tag', [...new Set(state.factors.flatMap(item => item.spec?.tags || []))].sort());
  }

  function renderFactorList() {
    const target = document.getElementById('lab-factor-list');
    if (!target) return;
    const factors = filteredFactors();
    if (!factors.length) {
      target.innerHTML = '<div class="lab-empty">没有符合筛选条件的因子</div>';
      return;
    }
    target.innerHTML = factors.map(item => {
      const checked = state.correlationSelection.has(item.version_id);
      const comparable = factorCanCorrelate(item);
      const validation = item.validation_passed === true ? '验证通过' : item.validation_passed === false ? '验证未通过' : '未验证';
      return `<div class="lab-factor-item ${state.selectedVersion === item.version_id ? 'active' : ''}">
        <label class="lab-factor-select" title="${comparable ? '加入相关性分析' : `仅支持所选周期的表达式因子可比较`}"><input type="checkbox" data-correlation-version="${h(item.version_id)}" ${checked ? 'checked' : ''} ${comparable ? '' : 'disabled'}><span></span></label>
        <button type="button" data-factor-version="${h(item.version_id)}">
          <div class="lab-factor-item-head"><b>${h(item.name)}</b><span>${h(statusLabel[item.status] || item.status)}</span></div>
          <p>${h(item.spec?.description || item.spec?.rationale || '尚未填写因子含义')}</p>
          <code>${h(item.spec?.expression || item.slug)}</code><small>V${item.version} · ${h(item.category)} · ${h(item.kind)} · ${h(validation)}</small>
        </button></div>`;
    }).join('');
    syncCorrelationControls();
  }

  async function selectFactor(versionId) {
    state.selectedVersion = versionId;
    state.suggestion = null;
    renderFactorList();
    const evidence = document.getElementById('lab-factor-evidence');
    evidence.innerHTML = '<div class="lab-empty">读取版本证据…</div>';
    try {
      const [detail, history] = await Promise.all([
        request(`/api/v1/lab/factors/${encodeURIComponent(versionId)}`),
        request(`/api/v1/lab/factors/${encodeURIComponent(versionId)}/history`),
      ]);
      renderEvidence(detail, history.items || []);
      renderCopilot(detail);
    } catch (error) {
      evidence.innerHTML = `<div class="lab-empty">${h(error.message)}</div>`;
    }
  }

  function renderEvidence(detail, history = []) {
    const report = detail.validation;
    const horizons = report?.horizons || {};
    const gates = report?.gates;
    const gateText = !report ? '尚未运行统一验证' : gates?.passed ? '全部晋级门槛通过' :
      [...(gates?.hard_failures || []), ...(gates?.soft_failures || [])].join('；') || '待人工复核';
    const horizonCards = Object.values(horizons).map(item => `<div class="lab-horizon"><span>${item.horizon}D · OOS RANK IC</span>
      <b>${number(item.oos_rank_ic, 4)}</b><small>ICIR ${number(item.oos_icir, 3)} · Q ${number(item.q_value, 3)}</small></div>`).join('');
    const robustness = report?.robustness;
    const robustnessTests = robustness ? [
      ['MONTE CARLO', robustness.monte_carlo,
        `${number((robustness.monte_carlo?.probability_positive_ic || 0) * 100, 0)}% 正向 IC`],
      ['参数敏感性', robustness.parameter_sensitivity,
        robustness.parameter_sensitivity?.applicable === false ? '无显式窗口参数' : `${number((robustness.parameter_sensitivity?.same_sign_ratio || 0) * 100, 0)}% 邻域同号`],
      ['WFA', robustness.walk_forward,
        `${number((robustness.walk_forward?.sign_consistency || 0) * 100, 0)}% 折叠同号`],
      ['穿透测试', robustness.penetration,
        `${number(robustness.penetration?.concentration?.effective_names, 1)} 有效个股`],
    ] : [];
    const robustnessCards = robustnessTests.map(([label, item, detailText]) =>
      `<div class="lab-robustness-item ${item?.passed ? 'pass' : 'fail'}"><i></i><span>${h(label)}</span><b>${item?.passed ? 'PASS' : 'FAIL'}</b><small>${h(detailText)}</small></div>`
    ).join('');
    const canApprove = detail.status === 'candidate';
    const canDeploy = ['approved', 'degraded', 'production'].includes(detail.status);
    const researchHorizons = (state.overview?.research?.horizons || [3]).map(Number);
    const validatedHorizons = Object.keys(horizons).map(Number).filter(Number.isFinite);
    const deployHorizons = validatedHorizons.length ? validatedHorizons : researchHorizons;
    const preferredHorizon = deployHorizons.includes(3) ? 3 : deployHorizons[0];
    const tags = detail.spec?.tags || [];
    const versionHistory = history.map(item => `<li class="${item.id === detail.id ? 'active' : ''}"><button type="button" data-factor-version="${h(item.id)}">V${item.version}</button><span>${h(statusLabel[item.status] || item.status)} · ${item.validation_passed === true ? '验证通过' : item.validation_passed === false ? '验证未通过' : '未验证'} · ${h(formatDate(item.updated_at))}</span></li>`).join('');
    document.getElementById('lab-factor-evidence').innerHTML = `
      <div class="lab-evidence-title"><div><h4>${h(detail.name)}</h4><span>${h(statusLabel[detail.status] || detail.status)}</span></div><code>${h(detail.spec?.expression || detail.slug)}</code></div>
      <div class="lab-factor-meaning"><span>因子含义</span><p>${h(detail.spec?.description || '尚未填写说明')}</p><span>研究逻辑</span><p>${h(detail.spec?.rationale || '尚未填写研究逻辑')}</p><small>${tags.length ? tags.map(tag => `#${h(tag)}`).join(' · ') : '无标签'} · 支持 ${(detail.spec?.horizons || []).map(value => `${value}D`).join(' / ') || '未声明周期'}</small></div>
      <div class="lab-evidence-meta"><div><span>CANDIDATE SCORE</span><b>${number(report?.candidate_score, 1)}</b></div><div><span>COVERAGE</span><b>${report ? number(report.coverage * 100, 1) + '%' : '—'}</b></div><div><span>MAX CORR</span><b>${number(report?.max_existing_correlation, 2)}</b></div></div>
      <div class="lab-gate ${gates?.passed ? 'pass' : ''}"><i></i><span>${h(gateText)}</span></div>
      <div class="lab-horizon-grid">${horizonCards || '<div class="lab-empty">验证后显示 1 / 3 / 5 / 7 日证据</div>'}</div>
      ${robustness ? `<div class="lab-robustness"><div class="lab-robustness-head"><span>ROBUSTNESS GATE</span><b>${robustness.tests_passed}/${robustness.tests_applicable}</b></div><div class="lab-robustness-grid">${robustnessCards}</div></div>` : ''}
      <div class="lab-version-history"><span>VERSION HISTORY</span><ol>${versionHistory || '<li><span>暂无版本记录</span></li>'}</ol></div>
      <div class="lab-evidence-actions">
        <button class="primary" type="button" data-validate-version="${h(detail.id)}">运行统一验证</button>
        ${report?.gates?.bias_audit_required ? `<button type="button" data-audit-version="${h(detail.id)}">运行防偏差审计</button>` : ''}
        ${canApprove ? `<button type="button" data-approve-version="${h(detail.id)}">人工批准</button>` : ''}
        ${canDeploy ? `<div class="lab-deploy-config" aria-label="Champion 生效范围">
          <label>周期<select data-deploy-horizon>${deployHorizons.map(value => `<option value="${value}" ${value === preferredHorizon ? 'selected' : ''}>${value} 日</option>`).join('')}</select></label>
          <label>画像<select data-deploy-profile><option value="all">全部画像</option><option value="risk_adjusted">扣费风险收益</option><option value="short_term">短期命中收益</option><option value="stable">稳定可解释</option></select></label>
          <label>范围<select data-deploy-scope><option value="exact">仅当前候选</option><option value="a_share">全部 A 股候选</option></select></label>
          <button type="button" data-deploy-version="${h(detail.id)}">设为 Champion</button>
        </div>` : ''}
      </div>`;
  }

  function correlationCell(value) {
    if (!Number.isFinite(Number(value))) return '<td class="empty">—</td>';
    const rho = Number(value);
    const alpha = Math.max(.08, Math.min(.88, Math.abs(rho)));
    const color = rho >= 0 ? `rgba(219,75,75,${alpha})` : `rgba(64,132,255,${alpha})`;
    return `<td style="background:${color}" title="ρ = ${rho.toFixed(4)}">${rho.toFixed(2)}</td>`;
  }

  function renderCorrelation(result) {
    const names = new Map(result.items.map(item => [item.version_id, item.name]));
    const pairs = result.high_correlations || [];
    const pairRows = pairs.map(item => `<tr><td>${h(names.get(item.left_version_id))}</td><td>${h(names.get(item.right_version_id))}</td><td>${number(item.rho, 4)}</td><td>超过 |ρ| ≥ ${number(result.threshold, 2)}</td></tr>`).join('');
    document.getElementById('lab-correlation-body').innerHTML = `
      <div class="lab-correlation-summary"><div><span>比较口径</span><b>${h(result.universe)} · ${result.horizon} 日</b><small>${h(result.start)} → ${h(result.end)}</small></div><div><span>高相关阈值</span><b>|ρ| ≥ ${number(result.threshold, 2)}</b><small>可在设置 → Quant Lab 修改</small></div><div><span>冻结快照</span><b>${h(String(result.snapshot_hash).slice(0, 12))}</b><small>${result.items.length} 个因子</small></div></div>
      <p class="lab-correlation-explanation">${h(result.explanation)}</p>
      <div class="lab-heatmap-scroll"><table class="lab-heatmap"><thead><tr><th></th>${result.items.map(item => `<th title="${h(item.name)}">${h(item.name)}</th>`).join('')}</tr></thead><tbody>${result.items.map((item, row) => `<tr><th>${h(item.name)}</th>${result.matrix[row].map(correlationCell).join('')}</tr>`).join('')}</tbody></table></div>
      <div class="lab-correlation-pairs"><h4>高相关因子对</h4>${pairs.length ? `<div class="table-scroll"><table><thead><tr><th>因子 A</th><th>因子 B</th><th>ρ</th><th>判断</th></tr></thead><tbody>${pairRows}</tbody></table></div>` : `<div class="lab-empty">当前阈值下没有高相关因子对；这不等于因子已通过完整鲁棒性验证。</div>`}</div>`;
  }

  async function runCorrelation() {
    const dialog = document.getElementById('lab-correlation-dialog');
    const body = document.getElementById('lab-correlation-body');
    if (!dialog.open) dialog.showModal();
    body.innerHTML = '<div class="lab-job-loading"><i></i><span>正在从同一冻结快照计算日截面秩相关…</span></div>';
    const research = state.overview?.research || {};
    try {
      const result = await request('/api/v1/lab/factors/correlation-matrix', {method:'POST', body:JSON.stringify({
        version_ids:[...state.correlationSelection], universe:research.universe || 'csi800',
        start:research.start || '2015-01-01', end:new Date().toISOString().slice(0, 10),
        horizon:state.correlationHorizon,
      })});
      renderCorrelation(result);
    } catch (error) {
      body.innerHTML = `<div class="lab-empty">${h(error.message)}</div>`;
    }
  }

  function renderCopilot(detail) {
    const target = document.getElementById('lab-copilot');
    const suggestion = state.suggestion;
    const task = state.suggestionTask;
    const taskHtml = task ? `<div class="lab-suggestion"><span>CLOUD TASK · ${h(task.status)}</span><p>${h(task.phase || task.detail || '正在排队')}</p><small>${Number(task.progress || 0)}% · ${h(String(task.id || '').slice(-8))}</small>
      <div class="lab-job-drawer-actions">${['queued','running','cancelling'].includes(task.status) ? `<button class="danger" type="button" data-cancel-job="${h(task.id)}">取消</button>` : ''}</div></div>` : '';
    target.innerHTML = `<div class="lab-copilot-mark">AI</div><h4>研究 Copilot</h4>
      <p>仅把表达式结构与本地验证指标交给助手。建议不会覆盖原版本，也不会自动批准。</p>
      <button class="lab-button lab-button-quiet lab-copilot-action" type="button" data-suggest-version="${h(detail.id)}">生成本地修正建议</button>
      <button class="lab-button lab-button-quiet lab-copilot-action" type="button" data-suggest-cloud-version="${h(detail.id)}">生成云端修正建议</button>
      ${taskHtml}${suggestion ? `<div class="lab-suggestion"><span>SUGGESTED PATCH</span><code>${h(suggestion.expression)}</code><p>${h(suggestion.rationale)}</p>
        ${(suggestion.risks || []).length ? `<ul>${suggestion.risks.map(risk => `<li>${h(risk)}</li>`).join('')}</ul>` : ''}
        <button class="lab-button lab-button-primary lab-copilot-action" type="button" data-apply-suggestion="${h(suggestion.id)}">应用为新版本</button></div>` : ''}`;
  }

  async function watchCloudSuggestion(task, versionId) {
    state.suggestionTask = task;
    while (state.suggestionTask?.id === task.id && ['queued','running','cancelling'].includes(task.status)) {
      await new Promise(resolve => window.setTimeout(resolve, 600));
      try {
        task = await request(task.links?.self || `/api/v1/jobs/${encodeURIComponent(task.id)}`);
      } catch (error) {
        task = {...task, status:'failed', detail:error.message};
      }
      if (state.selectedVersion === versionId) {
        const detail = await request(`/api/v1/lab/factors/${encodeURIComponent(versionId)}`);
        renderCopilot(detail);
      }
    }
    if (state.suggestionTask?.id !== task.id) return;
    state.suggestionTask = null;
    if (task.status === 'completed' && task.result?.result) {
      state.suggestion = task.result.result;
    } else if (task.status !== 'completed') {
      announce(`云端修正建议${statusLabel[task.status] || task.status}；请按当前设置重新提交。`);
    }
    if (state.selectedVersion === versionId) {
      const detail = await request(`/api/v1/lab/factors/${encodeURIComponent(versionId)}`);
      renderCopilot(detail);
    }
  }

  function renderExperiments() {
    const target = document.getElementById('lab-experiment-list');
    if (!target) return;
    if (!state.experiments.length) {
      target.innerHTML = '<div class="lab-empty">尚无模型实验。选择上方模型发起第一次基线训练。</div>';
      return;
    }
    target.innerHTML = `<div class="table-scroll"><table class="lab-job-table"><thead><tr><th>实验</th><th>模型</th><th>状态</th><th>实际设备</th><th>相关性</th><th>吞吐</th><th>峰值显存</th><th>产出版本</th><th>更新时间</th></tr></thead><tbody>${state.experiments.map(item => {
      const telemetry = item.result_json?.telemetry || {};
      return `<tr>
      <td>${h(item.name)}</td><td>${h((item.method || '').toUpperCase())}</td><td><span class="lab-status ${h(item.status)}">${h(statusLabel[item.status] || item.status)}</span></td>
      <td>${h(telemetry.effective_device || item.result_json?.device || 'cpu')}</td><td>${number(item.result_json?.metrics?.correlation, 4)}</td><td>${telemetry.samples_per_second ? `${number(telemetry.samples_per_second, 1)}/s` : '—'}</td><td>${telemetry.peak_gpu_memory_mb ? `${number(telemetry.peak_gpu_memory_mb, 1)} MiB` : '—'}</td><td>${item.result_json?.version_id ? `<button type="button" data-factor-version="${h(item.result_json.version_id)}">${h(statusLabel[item.result_json.version_status] || item.result_json.version_status || '影子候选')}</button>` : '—'}</td><td>${h((item.updated_at || '').slice(0, 16).replace('T', ' '))}</td></tr>`;
    }).join('')}</tbody></table></div>`;
  }

  function renderStudyList() {
    const target = document.getElementById('lab-study-list');
    if (!target) return;
    if (!state.studies.length) {
      target.innerHTML = '<div class="lab-empty">尚无 Study。默认协议使用过去三年训练、未来一年测试，并将最后一年保持密封。</div>';
      return;
    }
    target.innerHTML = `<div class="table-scroll"><table class="lab-study-table"><thead><tr><th>Study</th><th>状态</th><th>候选</th><th>Trials</th><th>密封集</th><th>更新</th><th></th></tr></thead><tbody>${state.studies.map(item => {
      const result = item.result || {};
      const candidate = result.version_id ? 'Shadow Candidate' : result.candidate === false ? '未晋级' : '—';
      const trialCount = Number(result.trial_count ?? (result.trials || []).length);
      const sealed = Boolean(result.sealed || result.sealed_metrics);
      return `<tr><td><button type="button" data-study-id="${h(item.id)}">${h(item.config?.universe || '—')} · ${h(String(item.id).slice(0, 8))}</button></td><td><span class="lab-status ${h(item.status)}">${h(statusLabel[item.status] || item.status)}</span></td><td>${h(candidate)}</td><td>${trialCount}</td><td>${sealed ? '已锁定评估' : '未完成'}</td><td>${h(formatDate(item.updated_at))}</td><td>${['paused','failed','interrupted'].includes(item.status) ? `<button type="button" data-resume-study="${h(item.id)}">恢复</button>` : ''}</td></tr>`;
    }).join('')}</tbody></table></div>`;
  }

  function renderStudyDetail() {
    const target = document.getElementById('lab-study-detail');
    if (!target) return;
    const summary = state.studies.find(item => item.id === state.selectedStudyId) || state.studies[0];
    if (!summary) {
      target.innerHTML = '<div class="lab-empty">创建或选择一个 Study，查看折线时间轴、Pareto 推荐和密封证据。</div>';
      return;
    }
    state.selectedStudyId = summary.id;
    const study = state.studyDetail?.id === summary.id ? state.studyDetail : null;
    if (!study) {
      target.innerHTML = '<div class="lab-job-loading"><i></i><span>正在读取所选 Study 的审计证据…</span></div>';
      void loadStudyDetail(summary.id);
      return;
    }
    const result = study.result || {};
    const protocol = result.protocol || study.config?.protocol || {};
    const sealed = result.sealed_holdout || {};
    const foldCount = Number(protocol.development_folds || 3);
    const folds = Array.from({length: foldCount}, (_, index) => `<div><span>DEV ${String(index + 1).padStart(2, '0')}</span><b>Purged fold</b><small>${protocol.test_window || 244} 交易日 · ${index + 1}/${foldCount}</small></div>`).join('');
    const sealedMetrics = result.sealed_metrics || {};
    const recommended = result.recommended || null;
    const feasible = (result.trials || []).filter(item => item.feasible);
    const pareto = feasible.filter(item => item.pareto !== false);
    const trials = pareto.sort((a,b) => Number(b.metrics?.net_information_ratio || -999) - Number(a.metrics?.net_information_ratio || -999)).slice(0, 6);
    target.innerHTML = `<div class="lab-study-hero"><div><span>STUDY · ${h(String(study.id).slice(0, 12).toUpperCase())}</span><h4>${h(study.config?.universe || '—')} 共享多周期研究</h4><p>${h(study.config?.start || '—')} → ${h(study.config?.end || '—')} · 最长 ${number(study.config?.budget_hours, 1)} 小时</p></div><span class="lab-status ${h(study.status)}">${h(statusLabel[study.status] || study.status)}</span></div>
      <div class="lab-fold-timeline">${folds}<div><span>SEALED</span><b>${sealed.test_start ? `${h(sealed.test_start)} → ${h(sealed.test_end)}` : '等待锁参'}</b><small>只评估一次 · 不回流选参</small></div></div>
      <div class="lab-study-evidence"><div><span>可行 / Pareto</span><b>${feasible.length} / ${pareto.length}</b></div><div><span>净信息比率</span><b>${number(sealedMetrics.net_information_ratio, 2)}</b></div><div><span>RankIC</span><b>${number(sealedMetrics.rank_ic, 4)}</b></div><div><span>最大回撤</span><b>${sealedMetrics.max_drawdown == null ? '—' : number(sealedMetrics.max_drawdown * 100, 1) + '%'}</b></div></div>
      <div class="pareto-heading lab-block-head"><div><span>FEASIBLE FRONT</span><h4>锁参依据</h4></div><span class="lab-beta">${recommended ? h(String(recommended.params?.model || '').toUpperCase()) : 'WAITING'}</span></div>
      <div class="lab-pareto-list">${trials.length ? trials.map(item => `<div class="${recommended?.number === item.number ? 'recommended' : ''}"><span>#${item.number}</span><b>${h(item.params?.model || '—')}</b><span>IR ${number(item.metrics?.net_information_ratio, 2)}</span><span>IC ${number(item.metrics?.rank_ic, 3)}</span><span>DD ${number((item.metrics?.max_drawdown || 0) * 100, 1)}%</span></div>`).join('') : '<div><span>—</span><b>尚无满足 FDR、稳定性与成本门槛的 Trial</b><span></span><span></span><span></span></div>'}</div>
      <div class="lab-study-actions">${result.version_id ? `<button type="button" data-factor-version="${h(result.version_id)}">查看 Shadow 候选</button>` : ''}${['paused','failed','interrupted'].includes(study.status) ? `<button type="button" data-resume-study="${h(study.id)}">从检查点恢复</button>` : ''}${study.job_id ? `<button type="button" data-job-detail="${h(study.job_id)}">打开任务时间线</button>` : ''}</div>`;
  }

  async function loadStudyDetail(studyId) {
    if (!studyId || state.studyDetailLoadingId === studyId) return;
    state.studyDetailLoadingId = studyId;
    try {
      const detail = await request(`/api/v1/lab/studies/${encodeURIComponent(studyId)}`, {
        requestKey:'study-detail',
      });
      if (state.selectedStudyId !== studyId) return;
      state.studyDetail = detail;
      renderStudyDetail();
    } catch (error) {
      if (state.selectedStudyId === studyId) {
        const target = document.getElementById('lab-study-detail');
        if (target) target.innerHTML = `<div class="lab-job-load-error"><b>Study 详情读取失败</b><p>${h(error.message || error)}</p><button type="button" data-study-id="${h(studyId)}">重试</button></div>`;
      }
    } finally {
      if (state.studyDetailLoadingId === studyId) state.studyDetailLoadingId = '';
    }
  }

  async function refreshStudies() {
    try {
      const response = await request('/api/v1/lab/studies?limit=100');
      state.studies = response.items || [];
      state.studyDetail = null;
      renderStudyList();
      renderStudyDetail();
    } catch (error) {
      if (isLabActive()) showError('Study 账本刷新失败', error);
    }
  }

  function renderMiningRuns() {
    const target = document.getElementById('lab-mining-runs');
    if (!target) return;
    if (!state.miningRuns.length) {
      target.innerHTML = '<div class="lab-empty">尚无 AutoMiner 批次。启用开关后可发起首次受限代码挖掘。</div>';
      return;
    }
    target.innerHTML = state.miningRuns.slice(0, 8).map(item => {
      const result = item.result || {};
      const selected = item.id === state.selectedMiningRun;
      return `<div class="lab-mining-run ${selected ? 'selected' : ''}"><button type="button" data-mining-run="${h(item.id)}" aria-pressed="${selected}">${h(String(item.id).slice(0, 12).toUpperCase())}</button><span class="lab-status ${h(item.status)}">${h(statusLabel[item.status] || item.status)}</span><span>${h(result.research_quality || '等待数据门禁')}</span><span>${Number(result.candidate_count || 0)} 候选 / ${Number(result.finalist_count || 0)} 入围</span><span>${h(formatDate(item.updated_at))}</span></div>`;
    }).join('');
  }

  function renderMiningCandidates(run) {
    const target = document.getElementById('lab-mining-candidates');
    if (!target) return;
    const candidates = run?.candidates || [];
    const result = run?.result || {};
    const config = run?.config || {};
    const batch = String(run?.id || '').slice(0, 12).toUpperCase();
    const summary = `<div class="lab-mining-selection" tabindex="-1">
      <div><span>SELECTED BATCH</span><b>${h(batch || '—')}</b><small>${h(statusLabel[run?.status] || run?.status || '未知状态')} · 更新于 ${h(formatDate(run?.updated_at))}</small></div>
      <dl><div><dt>研究等级</dt><dd>${h(result.research_quality || config.research_tier || '等待数据门禁')}</dd></div><div><dt>候选 / 入围</dt><dd>${Number(result.candidate_count || candidates.length)} / ${Number(result.finalist_count || 0)}</dd></div><div><dt>LLM 调用</dt><dd>${Number(result.llm_calls || 0)} / 3</dd></div></dl>
      ${run?.job_id ? `<button type="button" data-job-detail="${h(run.job_id)}">查看关联任务</button>` : ''}
    </div>`;
    if (!candidates.length) {
      target.innerHTML = `${summary}<div class="lab-empty">该批次正在 ${h(result.stage || '准备数据与候选')}；候选写入后会显示 TRAIN / VALID / 密封 TEST 对比。</div>`;
      return;
    }
    target.innerHTML = `${summary}<div class="table-scroll"><table class="lab-mining-table"><thead><tr><th>候选</th><th>状态</th><th>Pareto</th><th>TRAIN IC</th><th>VALID IC / q</th><th>SEALED TEST IC</th><th>版本</th></tr></thead><tbody>${candidates.map(item => {
      const proposal = item.proposal || {}, metrics = item.metrics || {};
      return `<tr><td>${h(proposal.name || item.candidate_key)}</td><td>${h(item.status)}</td><td>${item.pareto_rank == null ? '—' : '#' + Number(item.pareto_rank)}</td><td>${number(metrics.train_metrics?.rank_ic, 4)}</td><td>${number(metrics.valid_metrics?.rank_ic, 4)} / ${number(metrics.valid_metrics?.q_value, 3)}</td><td>${number(metrics.test_metrics?.rank_ic, 4)}</td><td>${item.version_id ? `<button type="button" data-factor-version="${h(item.version_id)}">查看候选</button>` : '—'}</td></tr>`;
    }).join('')}</tbody></table></div>`;
  }

  async function loadMiningRun(runId, {reveal = false} = {}) {
    if (!runId) return;
    state.selectedMiningRun = runId;
    renderMiningRuns();
    const target = document.getElementById('lab-mining-candidates');
    if (target) target.innerHTML = '<div class="lab-job-loading"><i></i><span>正在读取所选批次…</span></div>';
    try {
      const run = await request(`/api/v1/lab/mining/runs/${encodeURIComponent(runId)}`);
      if (state.selectedMiningRun !== runId) return;
      renderMiningCandidates(run);
      if (reveal) {
        target?.scrollIntoView({behavior:'smooth', block:'nearest'});
        target?.querySelector('.lab-mining-selection')?.focus({preventScroll:true});
      }
    } catch (error) {
      if (isLabActive()) showError('AutoMiner 候选对比加载失败', error);
    }
  }

  async function refreshMiningRuns() {
    try {
      const response = await request('/api/v1/lab/mining/runs?limit=20');
      state.miningRuns = response.items || [];
      renderMiningRuns();
      const selected = state.miningRuns.some(item => item.id === state.selectedMiningRun)
        ? state.selectedMiningRun : state.miningRuns[0]?.id;
      if (selected) await loadMiningRun(selected);
    } catch (error) {
      if (isLabActive()) showError('AutoMiner 账本刷新失败', error);
    }
  }

  async function previewPythonSplit() {
    const form = document.getElementById('lab-discovery-form');
    const target = document.getElementById('lab-split-preview');
    if (!form || !target || form.elements.method.value !== 'python') return;
    target.hidden = false;
    target.innerHTML = '<span>正在计算 TRAIN / VALID / TEST 边界…</span>';
    try {
      const value = await request('/api/v1/lab/mining/preview', {method:'POST', body:JSON.stringify({
        start:form.elements.start.value, end:new Date().toISOString().slice(0,10),
        horizon:+form.elements.horizon.value,
      })});
      target.innerHTML = ['train','valid','test'].map(name => {
        const item = value.split?.[name] || {};
        return `<span><b>${name.toUpperCase()} · ${Number(item.days || 0)}D</b><small>${h(item.start || '—')} → ${h(item.end || '—')}${name === 'test' ? '<br>入围顺序冻结后只读一次' : ''}</small></span>`;
      }).join('');
    } catch (error) {
      target.innerHTML = `<span>无法生成切分预览：${h(error.message || error)}</span>`;
    }
  }

  async function refreshOverview() {
    const [dashboard, workbench] = await Promise.all([
      request('/api/v1/lab/dashboard', {requestKey:'dashboard'}),
      request(`/api/v1/lab/workbench?horizon=${state.selectedHorizon}`, {requestKey:'workbench'}),
    ]);
    state.dashboard = dashboard;
    state.workbench = workbench;
    state.overview = {
      ...(dashboard.summary || {}),
      capabilities:dashboard.readiness || {},
      research:dashboard.research || {},
    };
    state.jobs = dashboard.jobs || [];
    state.experiments = dashboard.experiments || [];
    state.studies = dashboard.studies || state.studies;
    state.studyDetail = null;
    renderReadiness();
    renderCapabilities();
    renderSnapshot();
    renderOverview();
    renderStrategyWorkbench();
    renderExperiments();
    renderJobTable();
    renderStudyList();
    renderStudyDetail();
    renderTaskTray();
    syncResearchForms();
  }

  async function refreshFactors() {
    const response = await request('/api/v1/lab/factors?limit=500');
    state.factors = response.items || [];
    const known = new Set(state.factors.map(item => item.version_id));
    state.correlationSelection = new Set([...state.correlationSelection].filter(id => known.has(id)));
    populateFactorFilters();
    renderFactorList();
    document.dispatchEvent(new CustomEvent('quantmaster:factors-changed'));
    if (!state.selectedVersion && state.factors.length) selectFactor(state.factors[0].version_id);
  }

  async function refreshJobs() {
    try {
      const previous = new Map(state.jobs.map(job => [job.id, job.status]));
      const [response, experiments] = await Promise.all([
        request('/api/v1/lab/jobs?limit=100&summary=true', {requestKey:'jobs'}),
        request('/api/v1/lab/experiments?limit=50&summary=true', {requestKey:'experiments'}),
      ]);
      state.jobs = response.items || [];
      renderJobList('lab-overview-jobs', state.jobs.slice(0, 5));
      renderJobTable();
      renderTaskTray();
      state.experiments = experiments.items || [];
      renderExperiments();
      const finished = state.jobs.find(job =>
        terminalJobStatuses.has(job.status) && activeJobStatuses.has(previous.get(job.id))
      );
      if (finished) announce(`${kindLabel[finished.kind] || finished.kind}${statusLabel[finished.status] || finished.status}`);
      if (finished && ['research_cycle', 'shadow_score'].includes(finished.kind)) {
        await refreshOverview();
      }
      if (state.jobs.some(job => job.kind === 'optimize' && activeJobStatuses.has(job.status))) await refreshStudies();
      if (state.selectedJobId) await refreshJobDetail();
    } catch (error) {
      if (error?.cause?.name !== 'AbortError' && isLabActive()) showError('任务状态刷新失败', error);
    }
  }

  async function enqueue(kind, params) {
    const confirmed = await confirmPreflight(kind, params, kindLabel[kind] || '任务预检');
    if (!confirmed) return null;
    const job = await request('/api/v1/lab/jobs', {
      method: 'POST', body: JSON.stringify({kind, params}),
    });
    state.jobs.unshift(job);
    renderTaskTray();
    renderJobTable();
    announce(`${kindLabel[kind] || kind}已加入队列`);
    schedulePolling(true);
    return job;
  }

  function pollingNeeded() {
    return !document.hidden && isLabActive()
      && state.jobs.some(job => activeJobStatuses.has(job.status));
  }

  function pollingDelay() {
    if (state.jobs.some(job => job.status === 'running')) return 2200;
    if (state.jobs.some(job => job.status === 'queued' || job.status === 'interrupted')) return 3800;
    return 7000;
  }

  function schedulePolling(immediate = false) {
    if (state.timer) window.clearTimeout(state.timer);
    state.timer = null;
    if (!pollingNeeded()) return;
    state.timer = window.setTimeout(async () => {
      if (state.polling || !pollingNeeded()) return;
      state.polling = true;
      try {
        await refreshJobs();
      } finally {
        state.polling = false;
        schedulePolling();
      }
    }, immediate ? 0 : pollingDelay());
  }

  function bindEvents() {
    setupDraggableDialog(document.getElementById('lab-factor-dialog'));
    const preflightDialog = document.getElementById('lab-preflight-dialog');
    preflightDialog.addEventListener('cancel', event => {
      event.preventDefault();
      resolvePreflight(false);
    });
    for (const id of ['lab-discovery-form']) {
      document.getElementById(id)?.addEventListener('input', () => { state.formsDirty = true; });
    }
    document.getElementById('tab-lab').addEventListener('click', async event => {
      const scopeButton = event.target.closest('[data-coverage-scope]');
      if (scopeButton) {
        state.coverageRepairScope = scopeButton.dataset.coverageScope;
        const context = state.preflightContext;
        if (context) renderPreflight(
          context.report, document.getElementById('lab-preflight-title')?.textContent,
        );
        document.querySelector(`[data-coverage-scope="${state.coverageRepairScope}"]`)?.focus({preventScroll:true});
        return;
      }
      const repairButton = event.target.closest('[data-coverage-repair]');
      if (repairButton) {
        await startCoverageRepair(repairButton.dataset.coverageRepair, repairButton);
        return;
      }
      const copyCoverage = event.target.closest('[data-copy-coverage]');
      if (copyCoverage) {
        try { await copyCoverageGaps(copyCoverage); }
        catch (error) { showError('复制缺口清单失败', error); }
        return;
      }
      if (event.target.closest('[data-preflight-close]')) {
        resolvePreflight(false);
        return;
      }
      if (event.target.closest('#lab-preflight-confirm')) {
        if (!event.target.closest('#lab-preflight-confirm').disabled) resolvePreflight(true);
        return;
      }
      if (event.target.closest('[data-close-job-drawer]')) {
        closeJobDetail();
        return;
      }
      const jobDetail = event.target.closest('[data-job-detail]');
      if (jobDetail) {
        openJobDetail(jobDetail.dataset.jobDetail, jobDetail);
        return;
      }
      const viewButton = event.target.closest('[data-lab-view],[data-lab-go]');
      if (viewButton) setView(viewButton.dataset.labView || viewButton.dataset.labGo);
      const horizonButton = event.target.closest('[data-strategy-horizon]');
      if (horizonButton) {
        state.selectedHorizon = Number(horizonButton.dataset.strategyHorizon);
        renderStrategyWorkbench();
      }
      if (event.target.closest('[data-lab-action="research-cycle"]')) {
        const research = state.overview?.research || {};
        try {
          const job = await enqueue('research_cycle', {
            universe:research.universe || 'csi800', start:research.start || '2015-01-01',
            end:new Date().toISOString().slice(0, 10),
          });
          if (job) setView('automation');
        } catch (error) { showError('每周研究任务未能创建', error); }
      }
      if (event.target.closest('[data-lab-action="shadow-score"]')) {
        const research = state.overview?.research || {};
        try {
          await enqueue('shadow_score', {
            universe:research.universe || 'csi800', start:research.start || '2015-01-01',
            end:new Date().toISOString().slice(0, 10),
          });
        } catch (error) { showError('每日影子评分未能创建', error); }
      }
      const promotion = event.target.closest('[data-promote-strategy]');
      if (promotion) {
        const target = promotion.dataset.promotionTarget;
        const reason = window.prompt(`请输入进入 ${target === 'paper' ? 'Paper' : 'Champion'} 的人工确认理由：`, '已核对密封证据与实际跟踪结果');
        if (!reason) return;
        try {
          await request(`/api/v1/lab/strategies/${promotion.dataset.promoteStrategy}/promotions`, {
            method:'POST', body:JSON.stringify({target, actor:'web', reason}),
          });
          await refreshOverview();
          announce('策略生命周期已更新');
        } catch (error) { showError('策略晋级未通过', error); }
      }
      if (event.target.closest('[data-lab-action="create-factor"]')) {
        openFactorDialog();
      }
      if (event.target.closest('[data-lab-action="prepare-data"]')) {
        const research = state.overview?.research || {};
        try {
          const job = await enqueue('prepare_data', {
            universe:research.universe || 'csi800', start:research.start || '2015-01-01',
            end:new Date().toISOString().slice(0, 10), data_policy:'refresh_missing',
          });
          if (job) setView('automation');
        } catch (error) { showError('数据准备任务未能创建', error); }
      }
      if (event.target.closest('[data-lab-close-dialog]')) {
        document.getElementById('lab-factor-dialog').close();
      }
      if (event.target.closest('[data-correlation-close]')) {
        document.getElementById('lab-correlation-dialog').close();
      }
      const copyButton = event.target.closest('[data-copy-target]');
      if (copyButton) {
        const command = document.getElementById(copyButton.dataset.copyTarget)?.textContent || '';
        const previousLabel = copyButton.textContent;
        try {
          await copyText(command);
          copyButton.textContent = '已复制';
          copyButton.disabled = true;
          window.setTimeout(() => {
            copyButton.textContent = previousLabel;
            copyButton.disabled = false;
          }, 1600);
        } catch (error) {
          showError('命令复制失败', error);
        }
      }
      const factor = event.target.closest('[data-factor-version]');
      if (factor) {
        if (factor.closest('#lab-job-drawer')) closeJobDetail();
        if (!factor.closest('#lab-factor-list')) setView('library');
        selectFactor(factor.dataset.factorVersion);
      }
      const studyButton = event.target.closest('[data-study-id]');
      if (studyButton) {
        const studyId = studyButton.dataset.studyId;
        if (state.selectedStudyId !== studyId) state.studyDetail = null;
        state.selectedStudyId = studyId;
        renderStudyDetail();
      }
      const resumeStudy = event.target.closest('[data-resume-study]');
      if (resumeStudy) try {
        resumeStudy.disabled = true;
        if (!await confirmPreflight('optimize', {study_id:resumeStudy.dataset.resumeStudy}, '恢复滚动优化')) return;
        await request(`/api/v1/lab/studies/${encodeURIComponent(resumeStudy.dataset.resumeStudy)}/resume`, {method:'POST'});
        await Promise.all([refreshStudies(), refreshJobs()]);
      } catch (error) { showError('Study 恢复失败', error); } finally { resumeStudy.disabled = false; }
      const model = event.target.closest('[data-model]');
      if (model) {
        if (model.getAttribute('aria-disabled') === 'true') {
          const setup = document.getElementById('lab-ml-setup');
          setup?.scrollIntoView({behavior:'smooth', block:'center'});
          setup?.focus({preventScroll:true});
          return;
        }
        state.selectedModel = model.dataset.model;
        renderModels();
      }
      const filter = event.target.closest('[data-status]');
      if (filter) {
        state.status = filter.dataset.status;
        document.querySelectorAll('#lab-status-filters button').forEach(button => button.classList.toggle('active', button === filter));
        renderFactorList();
      }
      const validate = event.target.closest('[data-validate-version]');
      if (validate) {
        try {
          const research = state.overview?.research || {};
          await enqueue('validate', {
            version_id: validate.dataset.validateVersion,
            universe: research.universe || 'csi800', start: research.start || '2015-01-01',
            end: new Date().toISOString().slice(0, 10),
          });
        } catch (error) { showError('验证任务未能创建', error); }
      }
      const approve = event.target.closest('[data-approve-version]');
      if (approve) {
        const reason = window.prompt('如需覆盖软门槛，请填写可审计的研究理由；全部通过可留空。', '') ?? null;
        if (reason !== null) try {
          if (!await confirmPreflight('approve', {version_id:approve.dataset.approveVersion}, '批准候选版本')) return;
          await request(`/api/v1/lab/factors/${approve.dataset.approveVersion}/approve`, {method:'POST', body:JSON.stringify({actor:'web', reason})});
          await Promise.all([refreshOverview(), refreshFactors()]);
        } catch (error) { showError('候选未能批准', error); }
      }
      const audit = event.target.closest('[data-audit-version]');
      if (audit) try {
        const research = state.overview?.research || {};
        const job = await enqueue('bias_audit', {
          version_id:audit.dataset.auditVersion, universe:research.universe || 'csi800',
          start:research.start || '2015-01-01', end:new Date().toISOString().slice(0,10),
        });
        if (!job) return;
        await refreshJobs();
        setView('automation');
      } catch (error) { showError('偏差审计任务未能创建', error); }
      const deploy = event.target.closest('[data-deploy-version]');
      if (deploy && window.confirm('设为研究 Champion？这不会连接真实券商。')) try {
        const research = state.overview?.research || {};
        const config = deploy.closest('.lab-deploy-config');
        const horizon = Number(config?.querySelector('[data-deploy-horizon]')?.value || 3);
        const profile = config?.querySelector('[data-deploy-profile]')?.value || 'all';
        const scope = config?.querySelector('[data-deploy-scope]')?.value || 'exact';
        if (!await confirmPreflight('deploy', {
          version_id:deploy.dataset.deployVersion, universe:research.universe || 'csi800',
          horizon, profile, scope,
        }, '部署研究 Champion')) return;
        await request(`/api/v1/lab/factors/${deploy.dataset.deployVersion}/deploy`, {method:'POST', body:JSON.stringify({universe:research.universe || 'csi800', horizon, profile, scope, actor:'web'})});
        await Promise.all([refreshOverview(), refreshFactors(), refreshMiningRuns()]);
      } catch (error) { showError('Champion 切换失败', error); }
      const suggest = event.target.closest('[data-suggest-version], [data-suggest-cloud-version]');
      if (suggest) try {
        suggest.disabled = true;
        const research = state.overview?.research || {};
        if (!await confirmPreflight('discover_llm', {
          universe:research.universe || 'csi800', start:research.start || '2015-01-01',
          end:new Date().toISOString().slice(0,10),
        }, '生成 AI 修正建议')) return;
        const versionId = suggest.dataset.suggestVersion || suggest.dataset.suggestCloudVersion;
        const cloud = Boolean(suggest.dataset.suggestCloudVersion);
        const automaticCloudSend = Boolean(research.allow_cloud_sample);
        const outboundConfirmed = cloud && (automaticCloudSend || window.confirm('云端建议会把因子表达式与本地验证摘要发送给当前模型服务，是否继续？'));
        if (cloud && !outboundConfirmed) return;
        const result = await request(`/api/v1/lab/factors/${versionId}/suggestions`, {
          method:'POST', body:JSON.stringify({use_cloud:cloud, outbound_confirmed:outboundConfirmed}),
        });
        const detail = await request(`/api/v1/lab/factors/${versionId}`);
        if (cloud) {
          state.suggestion = null;
          state.suggestionTask = result;
          renderCopilot(detail);
          void watchCloudSuggestion(result, versionId);
          return;
        }
        state.suggestion = result;
        renderCopilot(detail);
      } catch (error) { showError('修正建议生成失败', error); } finally { suggest.disabled = false; }
      const apply = event.target.closest('[data-apply-suggestion]');
      if (apply) try {
        const version = await request(`/api/v1/lab/suggestions/${apply.dataset.applySuggestion}/apply`, {method:'POST', body:JSON.stringify({actor:'web', reason:''})});
        await refreshFactors();
        selectFactor(version.id);
      } catch (error) { showError('建议未能应用', error); }
      const cancel = event.target.closest('[data-cancel-job]');
      if (cancel) try {
        await request(`/api/v1/jobs/${cancel.dataset.cancelJob}/cancel`, {method:'POST'});
        await refreshJobs();
      } catch (error) { showError('任务取消失败', error); }
      const retry = event.target.closest('[data-retry-job]');
      if (retry) {
        const source = state.jobDetail;
        const costly = ['discover_llm', 'discover_python', 'train'].includes(source?.kind);
        const confirmed = !costly || window.confirm(
          `${kindLabel[source.kind] || source.kind} 会重新消耗模型或训练资源。确定按完全相同的参数重跑吗？`
        );
        if (!confirmed) return;
        try {
          retry.disabled = true;
          if (!await confirmPreflight(source.kind, source.params || {}, '按原参数重跑')) return;
          const created = await request(`/api/v1/jobs/${retry.dataset.retryJob}/retry`, {method:'POST'});
          await refreshJobs();
          openJobDetail(created.id, retry);
        } catch (error) {
          showError('任务重跑未能创建', error);
        } finally {
          retry.disabled = false;
        }
      }
    });

    document.getElementById('lab-factor-search').addEventListener('input', event => {
      state.search = event.target.value;
      renderFactorList();
    });

    const factorFilterState = {
      'lab-factor-category': 'factorCategory',
      'lab-factor-kind': 'factorKind',
      'lab-factor-validation': 'factorValidation',
      'lab-factor-horizon': 'factorHorizon',
      'lab-factor-tag': 'factorTag',
    };
    Object.entries(factorFilterState).forEach(([id, key]) => {
      document.getElementById(id)?.addEventListener('change', event => {
        state[key] = event.target.value;
        renderFactorList();
      });
    });
    document.getElementById('lab-correlation-horizon')?.addEventListener('change', event => {
      state.correlationHorizon = Number(event.target.value);
      const retained = [...state.correlationSelection].filter(versionId => {
        const item = state.factors.find(factor => factor.version_id === versionId);
        return item && factorCanCorrelate(item);
      });
      if (retained.length !== state.correlationSelection.size) announce('已移除不支持该预测周期的因子');
      state.correlationSelection = new Set(retained);
      renderFactorList();
    });
    document.getElementById('lab-factor-list')?.addEventListener('change', event => {
      const input = event.target.closest('[data-correlation-version]');
      if (!input) return;
      if (input.checked && state.correlationSelection.size >= 30) {
        input.checked = false;
        announce('相关性分析最多选择 30 个因子');
        return;
      }
      if (input.checked) state.correlationSelection.add(input.dataset.correlationVersion);
      else state.correlationSelection.delete(input.dataset.correlationVersion);
      syncCorrelationControls();
    });
    document.getElementById('lab-run-correlation')?.addEventListener('click', runCorrelation);

    document.querySelectorAll('#lab-discovery-form [name=method]').forEach(input => input.addEventListener('change', event => {
      const llm = event.target.value === 'llm';
      const python = event.target.value === 'python';
      document.querySelectorAll('[data-genetic-field]').forEach(item => item.hidden = llm || python);
      document.querySelectorAll('[data-dsl-field]').forEach(item => item.hidden = python);
      document.querySelectorAll('[data-llm-field]').forEach(item => item.hidden = !(llm || python));
      document.querySelectorAll('[data-python-field]').forEach(item => item.hidden = !python);
      const rounds = document.querySelector('#lab-discovery-form [name=rounds]');
      if (rounds) {
        rounds.max = python ? '3' : '5';
        rounds.value = python ? '3' : Math.min(2, Number(rounds.value) || 2);
      }
      document.getElementById('lab-split-preview').hidden = !python;
      syncPythonMiningGate();
      if (python) previewPythonSplit();
    }));
    for (const name of ['start','horizon']) {
      document.querySelector(`#lab-discovery-form [name=${name}]`)?.addEventListener('change', previewPythonSplit);
    }

    document.getElementById('lab-discovery-form').addEventListener('submit', async event => {
      event.preventDefault();
      const form = new FormData(event.target);
      const method = form.get('method');
      const base = {universe:form.get('universe'), start:form.get('start'), end:new Date().toISOString().slice(0,10), horizon:+form.get('horizon')};
      if (method === 'python' && !state.overview?.capabilities?.python_mining_enabled) {
        const gate = document.getElementById('lab-python-gate');
        gate.hidden = false;
        gate.focus();
        return;
      }
      try {
        let operation;
        let params;
        if (method === 'llm') {
          operation = 'discover_llm';
          params = {...base, count:+form.get('top'), rounds:+form.get('rounds')};
        } else if (method === 'python') {
          operation = 'discover_python';
          params = {...base, rounds:+form.get('rounds'), candidate_limit:+form.get('candidates'), finalists:+form.get('finalists')};
        } else {
          operation = 'discover_genetic';
          params = {...base, top_n:+form.get('top'), population:+form.get('population'), generations:+form.get('generations')};
        }
        const job = await enqueue(operation, params);
        if (!job) return;
        state.formsDirty = false;
        setView('automation');
      } catch (error) { showError('发现任务未能创建', error); }
    });

    document.getElementById('lab-optimize-form').addEventListener('submit', async event => {
      event.preventDefault();
      const form = new FormData(event.target);
      const models = form.getAll('models');
      if (!models.length) {
        showError('Study 配置不完整', new Error('至少选择一个模型'));
        return;
      }
      const universe = String(form.get('universe'));
      try {
        const params = {
          universe, start:form.get('start'), end:new Date().toISOString().slice(0,10), models,
          budget_hours:+form.get('budget_hours'), max_trials:+form.get('max_trials'),
          top_n:+form.get('top_n'), sequence_length:+form.get('sequence_length'),
          research_tier:universe === 'csi800' ? 'production' : 'sandbox',
        };
        if (!await confirmPreflight('optimize', params, '创建滚动优化 Study')) return;
        const study = await request('/api/v1/lab/studies', {method:'POST', body:JSON.stringify(params)});
        state.formsDirty = false;
        state.selectedStudyId = study.id;
        await Promise.all([refreshStudies(), refreshJobs()]);
      } catch (error) { showError('优化 Study 未能创建', error); }
    });

    document.getElementById('lab-factor-create-form').addEventListener('submit', async event => {
      event.preventDefault();
      const form = new FormData(event.target);
      try {
        const version = await request('/api/v1/lab/factors', {method:'POST', body:JSON.stringify({
          name:form.get('name'), expression:form.get('expression'), category:form.get('category'), rationale:form.get('rationale'),
        })});
        document.getElementById('lab-factor-dialog').close();
        event.target.reset();
        await refreshFactors();
        setView('library');
        selectFactor(version.id);
      } catch (error) { showError('因子草稿创建失败', error); }
    });

    document.getElementById('lab-refresh-jobs').addEventListener('click', refreshJobs);
    document.getElementById('lab-refresh-studies').addEventListener('click', refreshStudies);
    document.getElementById('lab-open-python-settings')?.addEventListener('click', () => {
      document.querySelector('[data-tab="settings"]')?.click();
      window.setTimeout(() => {
        document.querySelector('[data-settings-section="lab"]')?.click();
        document.querySelector('[name="lab.ai_python_mining_enabled"]')?.focus();
      }, 0);
    });
    document.getElementById('lab-mining-runs')?.addEventListener('click', event => {
      const button = event.target.closest('[data-mining-run]');
      if (button) loadMiningRun(button.dataset.miningRun, {reveal:true});
    });
    document.addEventListener('keydown', event => {
      if (event.key === 'Escape' && state.selectedJobId) {
        closeJobDetail();
        return;
      }
      if (event.key !== 'Tab' || !state.selectedJobId) return;
      const drawer = document.getElementById('lab-job-drawer');
      const focusable = [...drawer.querySelectorAll(
        'button:not([disabled]),a[href],input:not([disabled]),select:not([disabled]),textarea:not([disabled]),[tabindex]:not([tabindex="-1"])'
      )].filter(item => item.getClientRects().length);
      if (!focusable.length) return;
      const first = focusable[0];
      const last = focusable[focusable.length - 1];
      if (event.shiftKey && document.activeElement === first) {
        event.preventDefault();
        last.focus();
      } else if (!event.shiftKey && document.activeElement === last) {
        event.preventDefault();
        first.focus();
      }
    });
    document.addEventListener('visibilitychange', () => {
      if (document.hidden) {
        if (state.timer) window.clearTimeout(state.timer);
        state.timer = null;
        state.controllers.forEach(controller => controller.abort());
        return;
      }
      if (!isLabActive()) return;
      const refresh = state.dashboard ? refreshJobs() : refreshOverview();
      refresh.finally(() => schedulePolling());
    });
  }

  async function loadQuantLab() {
    if (!state.initialized) {
      state.initialized = true;
      bindEvents();
      try {
        await refreshOverview();
      } catch (error) {
        showError('研究工作台加载失败', error);
      }
      schedulePolling();
    } else {
      refreshJobs().finally(() => schedulePolling());
    }
  }

  function openExpression(expression) {
    loadQuantLab();
    setView('library');
    openFactorDialog(expression);
  }

  window.loadQuantLab = loadQuantLab;
  window.quantLabOpenExpression = openExpression;
  document.addEventListener('quantmaster:settings-persisted', event => {
    if (!(event.detail?.changed_fields || []).some(field => field.startsWith('lab.'))) return;
    refreshOverview().catch(error => {
      if (isLabActive()) showError('研究设置同步失败', error);
    });
  });
})();
