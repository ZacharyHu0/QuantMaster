(function () {
  'use strict';

  const state = {
    initialized: false,
    overview: null,
    factors: [],
    jobs: [],
    experiments: [],
    selectedVersion: '',
    status: '',
    search: '',
    selectedModel: 'ridge',
    suggestion: null,
    timer: null,
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
    cancelled: '取消', paused: '暂停',
  };

  const kindLabel = {
    prepare_data: '冻结数据快照', validate: '统一因子验证', discover_genetic: '遗传因子发现',
    discover_llm: 'AI 因子发现', train: '模型训练',
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

  async function request(path, options = {}) {
    const response = await fetch(path, {
      headers: {'Content-Type': 'application/json', ...(options.headers || {})},
      ...options,
    });
    if (!response.ok) {
      let detail = `请求失败 (${response.status})`;
      try { detail = (await response.json()).detail || detail; } catch (_) { /* no-op */ }
      throw new Error(Array.isArray(detail) ? detail.map(item => item.msg).join('；') : detail);
    }
    return response.json();
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
      ['worker', 'RECOVERABLE WORKER', true],
    ];
    document.getElementById('lab-capabilities').innerHTML = items.map(item =>
      `<span class="lab-capability ${item[2] ? 'ready' : 'warning'}" data-capability="${item[0]}"><i></i>${h(item[1])} · ${item[2] ? 'READY' : 'SETUP'}</span>`
    ).join('');
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
    target.innerHTML = jobs.map(job => `<div class="lab-job-row ${h(job.status)}">
      <div><b>${h(kindLabel[job.kind] || job.kind)}</b><small>${h(job.phase || statusLabel[job.status] || job.status)} · ${h((job.created_at || '').slice(0, 16).replace('T', ' '))}</small></div>
      <span>${job.progress || 0}%</span></div>`).join('');
  }

  function renderJobTable() {
    const target = document.getElementById('lab-job-table');
    if (!target) return;
    if (!state.jobs.length) {
      target.innerHTML = '<div class="lab-empty">暂无任务。可从 AI 发现或模型实验创建。</div>';
      return;
    }
    target.innerHTML = `<div class="table-scroll"><table class="lab-job-table"><thead><tr><th>任务</th><th>状态</th><th>阶段</th><th>进度</th><th>创建</th><th></th></tr></thead><tbody>${state.jobs.map(job => `<tr>
      <td>${h(kindLabel[job.kind] || job.kind)}</td><td><span class="lab-status ${h(job.status)}">${h(statusLabel[job.status] || job.status)}</span></td>
      <td>${h(job.phase || '—')}</td><td>${job.progress || 0}%</td><td>${h((job.created_at || '').slice(0, 16).replace('T', ' '))}</td>
      <td>${['queued','running','paused','interrupted'].includes(job.status) ? `<button data-cancel-job="${h(job.id)}">取消</button>` : ''}</td></tr>`).join('')}</tbody></table></div>`;
  }

  function renderTaskTray() {
    const active = state.jobs.find(job => ['running', 'queued', 'interrupted', 'paused'].includes(job.status));
    const tray = document.getElementById('lab-task-tray');
    if (!tray) return;
    tray.hidden = !active;
    if (!active) return;
    document.getElementById('lab-task-title').textContent = kindLabel[active.kind] || active.kind;
    document.getElementById('lab-task-phase').textContent = active.phase || statusLabel[active.status] || active.status;
    document.getElementById('lab-task-percent').textContent = `${active.progress || 0}%`;
    document.getElementById('lab-task-fill').style.setProperty('--progress', (active.progress || 0) / 100);
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
    const canApprove = detail.status === 'candidate';
    const canDeploy = ['approved', 'degraded'].includes(detail.status);
    document.getElementById('lab-factor-evidence').innerHTML = `
      <div class="lab-evidence-title"><div><h4>${h(detail.name)}</h4><span>${h(statusLabel[detail.status] || detail.status)}</span></div><code>${h(detail.spec?.expression || detail.slug)}</code></div>
      <div class="lab-evidence-meta"><div><span>CANDIDATE SCORE</span><b>${number(report?.candidate_score, 1)}</b></div><div><span>COVERAGE</span><b>${report ? number(report.coverage * 100, 1) + '%' : '—'}</b></div><div><span>MAX CORR</span><b>${number(report?.max_existing_correlation, 2)}</b></div></div>
      <div class="lab-gate ${gates?.passed ? 'pass' : ''}"><i></i><span>${h(gateText)}</span></div>
      <div class="lab-horizon-grid">${horizonCards || '<div class="lab-empty">验证后显示 1 / 3 / 5 / 7 日证据</div>'}</div>
      <div class="lab-evidence-actions">
        <button class="primary" type="button" data-validate-version="${h(detail.id)}">运行统一验证</button>
        ${canApprove ? `<button type="button" data-approve-version="${h(detail.id)}">人工批准</button>` : ''}
        ${canDeploy ? `<button type="button" data-deploy-version="${h(detail.id)}">设为 Champion</button>` : ''}
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
    target.innerHTML = `<div class="table-scroll"><table class="lab-job-table"><thead><tr><th>实验</th><th>模型</th><th>状态</th><th>相关性</th><th>验证 MSE</th><th>更新时间</th></tr></thead><tbody>${state.experiments.map(item => `<tr>
      <td>${h(item.name)}</td><td>${h((item.method || '').toUpperCase())}</td><td><span class="lab-status ${h(item.status)}">${h(statusLabel[item.status] || item.status)}</span></td>
      <td>${number(item.result_json?.metrics?.correlation, 4)}</td><td>${number(item.result_json?.metrics?.mse, 6)}</td><td>${h((item.updated_at || '').slice(0, 16).replace('T', ' '))}</td></tr>`).join('')}</tbody></table></div>`;
  }

  async function refreshOverview() {
    state.overview = await request('/api/lab/overview');
    state.jobs = state.overview.recent_jobs || [];
    state.experiments = state.overview.recent_experiments || [];
    renderCapabilities();
    renderOverview();
    renderExperiments();
    renderJobTable();
    renderTaskTray();
  }

  async function refreshFactors() {
    const response = await request('/api/lab/factors?limit=500');
    state.factors = response.items || [];
    renderFactorList();
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
    document.getElementById('tab-lab').addEventListener('click', async event => {
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
      if (factor) selectFactor(factor.dataset.factorVersion);
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
      const deploy = event.target.closest('[data-deploy-version]');
      if (deploy && window.confirm('设为研究 Champion？这不会连接真实券商。')) try {
        const research = state.overview?.research || {};
        await request(`/api/lab/factors/${deploy.dataset.deployVersion}/deploy`, {method:'POST', body:JSON.stringify({universe:research.universe || 'csi800', horizon:3, actor:'web'})});
        await Promise.all([refreshOverview(), refreshFactors()]);
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
    });

    document.getElementById('lab-factor-search').addEventListener('input', event => {
      state.search = event.target.value;
      renderFactorList();
    });

    document.querySelectorAll('#lab-discovery-form [name=method]').forEach(input => input.addEventListener('change', event => {
      const llm = event.target.value === 'llm';
      document.querySelectorAll('[data-genetic-field]').forEach(item => item.hidden = llm);
      document.querySelectorAll('[data-llm-field]').forEach(item => item.hidden = !llm);
    }));

    document.getElementById('lab-discovery-form').addEventListener('submit', async event => {
      event.preventDefault();
      const form = new FormData(event.target);
      const method = form.get('method');
      const base = {universe:form.get('universe'), start:form.get('start'), end:new Date().toISOString().slice(0,10), horizon:+form.get('horizon')};
      try {
        if (method === 'llm') await enqueue('discover_llm', {...base, count:+form.get('top'), rounds:+form.get('rounds')});
        else await enqueue('discover_genetic', {...base, top_n:+form.get('top'), population:+form.get('population'), generations:+form.get('generations')});
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
        setView('automation');
      } catch (error) { showError('训练任务未能创建', error); }
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
    document.getElementById('lab-task-open').addEventListener('click', () => setView('automation'));
  }

  async function loadQuantLab() {
    if (!state.initialized) {
      state.initialized = true;
      bindEvents();
      try {
        await Promise.all([refreshOverview(), refreshFactors()]);
      } catch (error) {
        showError('研究工作台加载失败', error);
      }
      state.timer = window.setInterval(() => {
        if (isLabActive() && state.jobs.some(job => ['queued','running','interrupted','paused'].includes(job.status))) refreshJobs();
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
})();
