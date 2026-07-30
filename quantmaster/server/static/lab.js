(function () {
  'use strict';

  const state = {
    initialized: false,
    overview: null,
    factors: [],
    jobs: [],
    experiments: [],
    studies: [],
    miningRuns: [],
    selectedMiningRun: '',
    selectedStudyId: '',
    selectedVersion: '',
    status: '',
    search: '',
    selectedModel: 'ridge',
    suggestion: null,
    timer: null,
    formsDirty: false,
    selectedJobId: '',
    jobDetail: null,
    jobEvents: [],
    jobLastSeq: 0,
    jobDetailLoading: false,
    jobDrawerOpener: null,
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
  };

  const kindLabel = {
    prepare_data: '冻结数据快照', validate: '统一因子验证', discover_genetic: '遗传因子发现',
    discover_llm: 'AI 因子发现', discover_python: 'Python AutoMiner',
    train: '模型训练', optimize: '共享多周期优化',
    bias_audit: '防偏差审计',
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
    return window.QuantMasterAPI(path, {
      headers: {'Content-Type': 'application/json', ...(options.headers || {})},
      ...options,
    });
  }

  function showError(title, error) {
    if (typeof window.reportLocalError === 'function') {
      window.reportLocalError('Quant Lab', title, error);
    } else {
      console.error(title, error);
      window.alert(`${title}：${error.message || error}`);
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

  function setView(view) {
    document.querySelectorAll('[data-lab-view]').forEach(button => {
      button.classList.toggle('active', button.dataset.labView === view);
    });
    document.querySelectorAll('[data-lab-panel]').forEach(panel => {
      panel.classList.toggle('active', panel.dataset.labPanel === view);
    });
    if (view === 'automation') refreshJobs();
    if (view === 'optimization') refreshStudies();
    if (view === 'discover') refreshMiningRuns();
  }

  function renderCapabilities() {
    const capabilities = state.overview?.capabilities;
    if (!capabilities) return;
    const available = new Set(capabilities.models?.available_models || []);
    const items = [
      ['safe-dsl', 'SAFE DSL', true],
      ['pit', 'PIT CSI800', capabilities.tushare?.production_membership],
      ['ridge', 'RIDGE', available.has('ridge')],
      ['torch', 'DEEP LEARNING', capabilities.models?.torch],
      ['llm', `AI · ${capabilities.llm?.provider || 'OFFLINE'}`, capabilities.llm?.configured],
      ['python-miner', 'PYTHON AUTOMINER', capabilities.python_mining_enabled],
      ['worker', 'RECOVERABLE WORKER', true],
    ];
    document.getElementById('lab-capabilities').innerHTML = items.map(item =>
      `<span class="lab-capability ${item[2] ? 'ready' : 'warning'}" data-capability="${item[0]}"><i></i>${h(item[1])} · ${item[2] ? 'READY' : 'SETUP'}</span>`
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

  function syncResearchForms() {
    if (state.formsDirty) return;
    const research = state.overview?.research || {};
    const horizons = (research.horizons || [3]).map(Number);
    const preferred = horizons.includes(3) ? 3 : horizons[0];
    for (const id of ['lab-discovery-form', 'lab-train-form', 'lab-optimize-form']) {
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
    const form = document.getElementById('lab-train-form');
    if (form) {
      form.elements.model.value = state.selectedModel;
      document.getElementById('lab-selected-model').textContent = modelMeta[state.selectedModel]?.[0] || 'MODEL';
      const submit = form.querySelector('[type=submit]');
      const canTrain = available.has(state.selectedModel);
      submit.disabled = !canTrain;
      submit.textContent = canTrain ? '开始训练' : '先安装模型后端';
    }
  }

  function renderJobList(targetId, jobs) {
    const target = document.getElementById(targetId);
    if (!target) return;
    if (!jobs.length) {
      target.innerHTML = '<div class="lab-empty">暂无研究任务</div>';
      return;
    }
    target.innerHTML = jobs.map(job => `<button type="button" class="lab-job-row ${h(job.status)}" data-job-detail="${h(job.id)}">
      <span><b>${h(kindLabel[job.kind] || job.kind)}</b><small>${h(jobPhase(job))} · ${h(formatDate(job.created_at))}</small></span>
      <strong>${job.progress || 0}%</strong></button>`).join('');
  }

  function renderJobTable() {
    const target = document.getElementById('lab-job-table');
    if (!target) return;
    if (!state.jobs.length) {
      target.innerHTML = '<div class="lab-empty">暂无任务。可从 AI 发现或模型实验创建。</div>';
      return;
    }
    target.innerHTML = `<div class="table-scroll"><table class="lab-job-table"><thead><tr><th>任务</th><th>状态</th><th>阶段</th><th>进度</th><th>创建</th><th>操作</th></tr></thead><tbody>${state.jobs.map(job => `<tr>
      <td><button class="lab-job-link" type="button" data-job-detail="${h(job.id)}">${h(kindLabel[job.kind] || job.kind)}</button></td><td><span class="lab-status ${h(job.status)}">${h(statusLabel[job.status] || job.status)}</span></td>
      <td title="${h(jobPhase(job))}">${h(jobPhase(job))}</td><td>${job.progress || 0}%</td><td>${h(formatDate(job.created_at))}</td>
      <td class="lab-job-actions"><button type="button" data-job-detail="${h(job.id)}">查看</button>${activeJobStatuses.has(job.status) ? `<button class="danger" type="button" data-cancel-job="${h(job.id)}">取消</button>` : ''}</td></tr>`).join('')}</tbody></table></div>`;
  }

  function renderTaskTray() {
    const active = state.jobs.find(job => activeJobStatuses.has(job.status));
    const tray = document.getElementById('lab-task-tray');
    if (!tray) return;
    tray.hidden = !active;
    if (!active) return;
    document.getElementById('lab-task-title').textContent = kindLabel[active.kind] || active.kind;
    document.getElementById('lab-task-phase').textContent = jobPhase(active);
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

  function renderJobDetail() {
    const body = document.getElementById('lab-job-drawer-body');
    const job = state.jobDetail;
    if (!body || !job) return;
    const progress = Math.max(0, Math.min(100, Number(job.progress) || 0));
    const heartbeatAge = Date.now() - new Date(job.heartbeat_at || 0).getTime();
    const heartbeatFresh = job.status === 'running' && heartbeatAge >= 0 && heartbeatAge < 15000;
    const duration = formatDuration(job.started_at || job.created_at, job.finished_at || new Date().toISOString());
    const title = kindLabel[job.kind] || job.kind;
    document.getElementById('lab-job-drawer-title').textContent = title;
    document.getElementById('lab-job-drawer-kicker').textContent = `RESEARCH JOB · ${String(job.id).slice(0, 8).toUpperCase()}`;
    const errorCopy = job.status === 'failed' ? jobErrorCopy(job) : null;
    body.innerHTML = `<div class="lab-job-summary">
        <div class="lab-job-summary-row"><span class="lab-status ${h(job.status)}">${h(statusLabel[job.status] || job.status)}</span><strong>${progress}%</strong></div>
        <div class="lab-job-detail-progress"><i style="--progress:${progress / 100}"></i></div>
        <h4>${h(job.phase || statusLabel[job.status] || job.status)}</h4>
        <p>${h(job.detail || (job.status === 'running' ? '执行器正在处理当前阶段。' : job.error || '任务记录已保存。'))}</p>
        <div class="lab-job-runtime"><span class="${heartbeatFresh ? 'live' : ''}"><i></i>${heartbeatFresh ? '执行器心跳正常' : job.worker ? `执行器 ${h(job.worker)}` : '无活动执行器'}</span><span>耗时 ${h(duration)}</span></div>
      </div>
      ${errorCopy ? `<section class="lab-job-error"><span>FAILED</span><h4>${h(errorCopy.what)}</h4><dl><div><dt>可能原因</dt><dd>${h(errorCopy.why)}</dd></div><div><dt>下一步</dt><dd>${h(errorCopy.how)}</dd></div></dl></section>` : ''}
      <div class="lab-job-drawer-actions">
        ${activeJobStatuses.has(job.status) ? `<button class="danger" type="button" data-cancel-job="${h(job.id)}">安全停止</button>` : ''}
        ${terminalJobStatuses.has(job.status) ? `<button class="primary" type="button" data-retry-job="${h(job.id)}">按原参数重跑</button>` : ''}
      </div>
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
      const job = await request(`/api/lab/jobs/${encodeURIComponent(jobId)}`);
      if (state.selectedJobId !== jobId) return;
      const response = await request(`/api/lab/jobs/${encodeURIComponent(jobId)}/events?after=${state.jobLastSeq}&limit=2000`);
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
      return !search || `${item.name} ${item.slug} ${spec.expression || ''}`.toLowerCase().includes(search);
    });
  }

  function renderFactorList() {
    const target = document.getElementById('lab-factor-list');
    if (!target) return;
    const factors = filteredFactors();
    if (!factors.length) {
      target.innerHTML = '<div class="lab-empty">没有符合筛选条件的因子</div>';
      return;
    }
    target.innerHTML = factors.map(item => `<button type="button" class="lab-factor-item ${state.selectedVersion === item.version_id ? 'active' : ''}" data-factor-version="${h(item.version_id)}">
      <div class="lab-factor-item-head"><b>${h(item.name)}</b><span>${h(statusLabel[item.status] || item.status)}</span></div>
      <code>${h(item.spec?.expression || item.slug)}</code><small>V${item.version} · ${h(item.category)} · ${h(item.kind)}</small></button>`).join('');
  }

  async function selectFactor(versionId) {
    state.selectedVersion = versionId;
    state.suggestion = null;
    renderFactorList();
    const evidence = document.getElementById('lab-factor-evidence');
    evidence.innerHTML = '<div class="lab-empty">读取版本证据…</div>';
    try {
      const detail = await request(`/api/lab/factors/${encodeURIComponent(versionId)}`);
      renderEvidence(detail);
      renderCopilot(detail);
    } catch (error) {
      evidence.innerHTML = `<div class="lab-empty">${h(error.message)}</div>`;
    }
  }

  function renderEvidence(detail) {
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
    document.getElementById('lab-factor-evidence').innerHTML = `
      <div class="lab-evidence-title"><div><h4>${h(detail.name)}</h4><span>${h(statusLabel[detail.status] || detail.status)}</span></div><code>${h(detail.spec?.expression || detail.slug)}</code></div>
      <div class="lab-evidence-meta"><div><span>CANDIDATE SCORE</span><b>${number(report?.candidate_score, 1)}</b></div><div><span>COVERAGE</span><b>${report ? number(report.coverage * 100, 1) + '%' : '—'}</b></div><div><span>MAX CORR</span><b>${number(report?.max_existing_correlation, 2)}</b></div></div>
      <div class="lab-gate ${gates?.passed ? 'pass' : ''}"><i></i><span>${h(gateText)}</span></div>
      <div class="lab-horizon-grid">${horizonCards || '<div class="lab-empty">验证后显示 1 / 3 / 5 / 7 日证据</div>'}</div>
      ${robustness ? `<div class="lab-robustness"><div class="lab-robustness-head"><span>ROBUSTNESS GATE</span><b>${robustness.tests_passed}/${robustness.tests_applicable}</b></div><div class="lab-robustness-grid">${robustnessCards}</div></div>` : ''}
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

  function renderCopilot(detail) {
    const target = document.getElementById('lab-copilot');
    const suggestion = state.suggestion;
    target.innerHTML = `<div class="lab-copilot-mark">AI</div><h4>研究 Copilot</h4>
      <p>仅把表达式结构与本地验证指标交给助手。建议不会覆盖原版本，也不会自动批准。</p>
      <button class="lab-button lab-button-quiet lab-copilot-action" type="button" data-suggest-version="${h(detail.id)}">生成本地修正建议</button>
      ${suggestion ? `<div class="lab-suggestion"><span>SUGGESTED PATCH</span><code>${h(suggestion.expression)}</code><p>${h(suggestion.rationale)}</p>
        ${(suggestion.risks || []).length ? `<ul>${suggestion.risks.map(risk => `<li>${h(risk)}</li>`).join('')}</ul>` : ''}
        <button class="lab-button lab-button-primary lab-copilot-action" type="button" data-apply-suggestion="${h(suggestion.id)}">应用为新版本</button></div>` : ''}`;
  }

  function renderExperiments() {
    const target = document.getElementById('lab-experiment-list');
    if (!target) return;
    if (!state.experiments.length) {
      target.innerHTML = '<div class="lab-empty">尚无模型实验。选择上方模型发起第一次基线训练。</div>';
      return;
    }
    target.innerHTML = `<div class="table-scroll"><table class="lab-job-table"><thead><tr><th>实验</th><th>模型</th><th>状态</th><th>相关性</th><th>验证 MSE</th><th>产出版本</th><th>更新时间</th></tr></thead><tbody>${state.experiments.map(item => `<tr>
      <td>${h(item.name)}</td><td>${h((item.method || '').toUpperCase())}</td><td><span class="lab-status ${h(item.status)}">${h(statusLabel[item.status] || item.status)}</span></td>
      <td>${number(item.result_json?.metrics?.correlation, 4)}</td><td>${number(item.result_json?.metrics?.mse, 6)}</td><td>${item.result_json?.version_id ? `<button type="button" data-factor-version="${h(item.result_json.version_id)}">${h(statusLabel[item.result_json.version_status] || item.result_json.version_status || '影子候选')}</button>` : '—'}</td><td>${h((item.updated_at || '').slice(0, 16).replace('T', ' '))}</td></tr>`).join('')}</tbody></table></div>`;
  }

  function renderStudyList() {
    const target = document.getElementById('lab-study-list');
    if (!target) return;
    if (!state.studies.length) {
      target.innerHTML = '<div class="lab-empty">尚无 Study。默认协议会在开发期选参，并将末尾 252 日保持密封。</div>';
      return;
    }
    target.innerHTML = `<div class="table-scroll"><table class="lab-study-table"><thead><tr><th>Study</th><th>状态</th><th>候选</th><th>Trials</th><th>密封集</th><th>更新</th><th></th></tr></thead><tbody>${state.studies.map(item => {
      const result = item.result || {};
      const candidate = result.version_id ? 'Shadow Candidate' : result.candidate === false ? '未晋级' : '—';
      return `<tr><td><button type="button" data-study-id="${h(item.id)}">${h(item.config?.universe || '—')} · ${h(String(item.id).slice(0, 8))}</button></td><td><span class="lab-status ${h(item.status)}">${h(statusLabel[item.status] || item.status)}</span></td><td>${h(candidate)}</td><td>${(result.trials || []).length}</td><td>${result.sealed_metrics ? '已锁定评估' : '未完成'}</td><td>${h(formatDate(item.updated_at))}</td><td>${['paused','failed','interrupted'].includes(item.status) ? `<button type="button" data-resume-study="${h(item.id)}">恢复</button>` : ''}</td></tr>`;
    }).join('')}</tbody></table></div>`;
  }

  function renderStudyDetail() {
    const target = document.getElementById('lab-study-detail');
    if (!target) return;
    const study = state.studies.find(item => item.id === state.selectedStudyId) || state.studies[0];
    if (!study) {
      target.innerHTML = '<div class="lab-empty">创建或选择一个 Study，查看折线时间轴、Pareto 推荐和密封证据。</div>';
      return;
    }
    state.selectedStudyId = study.id;
    const result = study.result || {};
    const protocol = result.protocol || study.config?.protocol || {};
    const sealed = result.sealed_holdout || {};
    const folds = ['DEV 01','DEV 02','DEV 03','DEV 04'].map((name, index) => `<div><span>${name}</span><b>Purged fold</b><small>${protocol.fold_test_days || 63} 交易日 · ${index + 1}/4</small></div>`).join('');
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

  async function refreshStudies() {
    try {
      const response = await request('/api/lab/studies?limit=100');
      state.studies = response.items || [];
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
      const run = await request(`/api/lab/mining/runs/${encodeURIComponent(runId)}`);
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
      const response = await request('/api/lab/mining/runs?limit=20');
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
      const value = await request('/api/lab/mining/preview', {method:'POST', body:JSON.stringify({
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
    state.overview = await request('/api/lab/overview');
    state.jobs = state.overview.recent_jobs || [];
    state.experiments = state.overview.recent_experiments || [];
    state.studies = state.overview.recent_studies || state.studies;
    renderCapabilities();
    renderOverview();
    renderExperiments();
    renderJobTable();
    renderStudyList();
    renderStudyDetail();
    renderTaskTray();
    syncResearchForms();
  }

  async function refreshFactors() {
    const response = await request('/api/lab/factors?limit=500');
    state.factors = response.items || [];
    renderFactorList();
    document.dispatchEvent(new CustomEvent('quantmaster:factors-changed'));
    if (!state.selectedVersion && state.factors.length) selectFactor(state.factors[0].version_id);
  }

  async function refreshJobs() {
    try {
      const response = await request('/api/lab/jobs?limit=100');
      state.jobs = response.items || [];
      renderJobList('lab-overview-jobs', state.jobs.slice(0, 5));
      renderJobTable();
      renderTaskTray();
      const experiments = await request('/api/lab/experiments?limit=50');
      state.experiments = experiments.items || [];
      renderExperiments();
      if (state.jobs.some(job => job.kind === 'optimize')) await refreshStudies();
      if (state.selectedJobId) await refreshJobDetail();
    } catch (error) {
      if (isLabActive()) showError('任务状态刷新失败', error);
    }
  }

  async function enqueue(kind, params) {
    const job = await request('/api/lab/jobs', {
      method: 'POST', body: JSON.stringify({kind, params}),
    });
    state.jobs.unshift(job);
    renderTaskTray();
    renderJobTable();
    return job;
  }

  function bindEvents() {
    setupDraggableDialog(document.getElementById('lab-factor-dialog'));
    for (const id of ['lab-discovery-form', 'lab-train-form']) {
      document.getElementById(id)?.addEventListener('input', () => { state.formsDirty = true; });
    }
    document.getElementById('tab-lab').addEventListener('click', async event => {
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
      if (event.target.closest('[data-lab-action="create-factor"]')) {
        openFactorDialog();
      }
      if (event.target.closest('[data-lab-close-dialog]')) {
        document.getElementById('lab-factor-dialog').close();
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
        state.selectedStudyId = studyButton.dataset.studyId;
        renderStudyDetail();
      }
      const resumeStudy = event.target.closest('[data-resume-study]');
      if (resumeStudy) try {
        resumeStudy.disabled = true;
        await request(`/api/lab/studies/${encodeURIComponent(resumeStudy.dataset.resumeStudy)}/resume`, {method:'POST'});
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
        document.querySelector('#lab-train-form [name=model]').value = state.selectedModel;
        document.getElementById('lab-selected-model').textContent = modelMeta[state.selectedModel][0];
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
          await request(`/api/lab/factors/${approve.dataset.approveVersion}/approve`, {method:'POST', body:JSON.stringify({actor:'web', reason})});
          await Promise.all([refreshOverview(), refreshFactors()]);
        } catch (error) { showError('候选未能批准', error); }
      }
      const audit = event.target.closest('[data-audit-version]');
      if (audit) try {
        const research = state.overview?.research || {};
        await request('/api/lab/audits', {method:'POST', body:JSON.stringify({
          version_id:audit.dataset.auditVersion, universe:research.universe || 'csi800',
          start:research.start || '2015-01-01', end:new Date().toISOString().slice(0,10),
        })});
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
        await request(`/api/lab/factors/${deploy.dataset.deployVersion}/deploy`, {method:'POST', body:JSON.stringify({universe:research.universe || 'csi800', horizon, profile, scope, actor:'web'})});
        await Promise.all([refreshOverview(), refreshFactors(), refreshMiningRuns()]);
      } catch (error) { showError('Champion 切换失败', error); }
      const suggest = event.target.closest('[data-suggest-version]');
      if (suggest) try {
        suggest.disabled = true;
        state.suggestion = await request(`/api/lab/factors/${suggest.dataset.suggestVersion}/suggestions`, {method:'POST', body:JSON.stringify({use_cloud:false})});
        const detail = await request(`/api/lab/factors/${suggest.dataset.suggestVersion}`);
        renderCopilot(detail);
      } catch (error) { showError('修正建议生成失败', error); } finally { suggest.disabled = false; }
      const apply = event.target.closest('[data-apply-suggestion]');
      if (apply) try {
        const version = await request(`/api/lab/suggestions/${apply.dataset.applySuggestion}/apply`, {method:'POST', body:JSON.stringify({actor:'web', reason:''})});
        await refreshFactors();
        selectFactor(version.id);
      } catch (error) { showError('建议未能应用', error); }
      const cancel = event.target.closest('[data-cancel-job]');
      if (cancel) try {
        await request(`/api/lab/jobs/${cancel.dataset.cancelJob}/cancel`, {method:'POST'});
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
          const created = await request(`/api/lab/jobs/${retry.dataset.retryJob}/retry`, {method:'POST'});
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
        if (method === 'llm') await enqueue('discover_llm', {...base, count:+form.get('top'), rounds:+form.get('rounds')});
        else if (method === 'python') await enqueue('discover_python', {...base, rounds:+form.get('rounds'), candidate_limit:+form.get('candidates'), finalists:+form.get('finalists')});
        else await enqueue('discover_genetic', {...base, top_n:+form.get('top'), population:+form.get('population'), generations:+form.get('generations')});
        state.formsDirty = false;
        setView('automation');
      } catch (error) { showError('发现任务未能创建', error); }
    });

    document.getElementById('lab-train-form').addEventListener('submit', async event => {
      event.preventDefault();
      const form = new FormData(event.target);
      const available = new Set(state.overview?.capabilities?.models?.available_models || []);
      if (!available.has(form.get('model'))) {
        document.getElementById('lab-ml-setup')?.focus();
        return;
      }
      try {
        await enqueue('train', {
          model:form.get('model'), universe:form.get('universe'), start:form.get('start'),
          end:new Date().toISOString().slice(0,10), horizon:+form.get('horizon'),
          sequence_length:+form.get('sequence_length'), config:{epochs:+form.get('epochs')},
        });
        state.formsDirty = false;
        setView('automation');
      } catch (error) { showError('训练任务未能创建', error); }
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
        const study = await request('/api/lab/studies', {method:'POST', body:JSON.stringify({
          universe, start:form.get('start'), end:new Date().toISOString().slice(0,10), models,
          budget_hours:+form.get('budget_hours'), max_trials:+form.get('max_trials'),
          top_n:+form.get('top_n'), sequence_length:+form.get('sequence_length'),
          research_tier:universe === 'csi800' ? 'production' : 'sandbox',
        })});
        state.formsDirty = false;
        state.selectedStudyId = study.id;
        await Promise.all([refreshStudies(), refreshJobs()]);
      } catch (error) { showError('优化 Study 未能创建', error); }
    });

    document.getElementById('lab-factor-create-form').addEventListener('submit', async event => {
      event.preventDefault();
      const form = new FormData(event.target);
      try {
        const version = await request('/api/lab/factors', {method:'POST', body:JSON.stringify({
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
      if (event.key === 'Escape' && state.selectedJobId) closeJobDetail();
    });
  }

  async function loadQuantLab() {
    if (!state.initialized) {
      state.initialized = true;
      bindEvents();
      try {
        await Promise.all([refreshOverview(), refreshFactors(), refreshMiningRuns()]);
      } catch (error) {
        showError('研究工作台加载失败', error);
      }
      state.timer = window.setInterval(() => {
        if (isLabActive() && (state.selectedJobId || state.jobs.some(job => activeJobStatuses.has(job.status)))) refreshJobs();
      }, 3000);
    } else {
      refreshJobs();
    }
  }

  function openExpression(expression) {
    loadQuantLab();
    setView('library');
    openFactorDialog(expression);
  }

  window.loadQuantLab = loadQuantLab;
  window.quantLabOpenExpression = openExpression;
  document.addEventListener('quantmaster:settings-applied', event => {
    if (!(event.detail?.changed_fields || []).some(field => field.startsWith('lab.'))) return;
    refreshOverview().catch(error => {
      if (isLabActive()) showError('研究设置同步失败', error);
    });
  });
})();
