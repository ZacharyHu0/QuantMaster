(() => {
  const form = document.getElementById('stock-analysis-form');
  if (!form) return;

  const input = document.getElementById('stock-analysis-query');
  const stage = document.getElementById('stock-analysis-stage');
  const live = document.getElementById('stock-analysis-live');
  const liveDimensions = document.getElementById('stock-analysis-live-dimensions');
  const currentPhase = document.getElementById('stock-analysis-current-phase');
  const elapsedNode = document.getElementById('stock-analysis-elapsed');
  const etaNode = document.getElementById('stock-analysis-eta');
  const cancelButton = document.getElementById('stock-analysis-cancel');
  const reportRoot = document.getElementById('stock-analysis-report');
  const suggestions = document.getElementById('stock-analysis-suggestions');
  const STORAGE_KEY = 'qm.stock-analysis.active.v2';
  const terminalStatuses = new Set(['completed', 'completed_with_errors', 'failed', 'cancelled']);
  const dimensionDefs = [
    ['fundamental', '01', '基本面'], ['technical', '02', '技术面'],
    ['news', '03', '消息面'], ['capital', '04', '资金面'],
    ['sentiment', '05', '心理面'], ['macro', '06', '宏观面'],
  ];
  const stateLabels = {
    waiting:'等待', collecting:'取数', inference:'推理', complete:'完成', degraded:'降级',
  };

  let activeRun = null;
  let activeToken = 0;
  let activeSuggestion = -1;
  let suggestionItems = [];
  let suggestionTimer = 0;
  let suggestionRequest = 0;
  let loaded = false;
  let tickTimer = 0;
  let lastProgress = 0;
  let lastEventSeq = 0;
  const dimensionState = new Map(dimensionDefs.map(([key]) => [key, {
    state:'waiting', detail:'等待前序阶段', result:null,
  }]));

  function scoreClass(value) {
    const score = Number(value || 0);
    return score >= 58 ? 'up' : score <= 42 ? 'down' : '';
  }

  function statusLabel(value) {
    return {complete:'数据较完整', partial:'部分数据', unavailable:'数据缺失'}[value] || '待核查';
  }

  function fmt(value, digits = 2, suffix = '') {
    return value == null || !Number.isFinite(Number(value))
      ? '—' : `${Number(value).toFixed(digits)}${suffix}`;
  }

  function safeUrl(value) {
    const url = String(value || '');
    return /^https?:\/\//i.test(url) ? url : '';
  }

  function displayText(value, fallback = '') {
    if (value == null) return fallback;
    if (typeof value === 'object') {
      for (const key of ['text', 'summary', 'message', 'content']) {
        if (value[key] != null) return displayText(value[key], fallback);
      }
      return fallback;
    }
    const text = String(value).trim();
    if (!text) return fallback;
    if (text.startsWith('{') || text.startsWith('[')) {
      try { return displayText(JSON.parse(text), fallback); }
      catch (_) {
        const legacy = text.match(/^\{\s*['"]text['"]\s*:\s*(['"])([\s\S]*?)\1\s*,\s*['"]evidence_ids['"]\s*:/);
        return legacy ? legacy[2].trim() : fallback;
      }
    }
    return text;
  }

  function displayList(value) {
    return Array.isArray(value) ? value.map(item => displayText(item)).filter(Boolean) : [];
  }

  function warningText(value) {
    const text = displayText(value, '上游服务返回了不可读的结构化错误，已按降级处理。');
    return text.replace(/\{[\s\S]*$/, '上游请求失败，已按降级处理。');
  }

  function duration(seconds) {
    const value = Math.max(0, Math.round(Number(seconds) || 0));
    const minutes = Math.floor(value / 60);
    return `${String(minutes).padStart(2, '0')}:${String(value % 60).padStart(2, '0')}`;
  }

  function saveRun(value) {
    try { localStorage.setItem(STORAGE_KEY, JSON.stringify(value)); } catch (_) { /* private mode */ }
  }

  function storedRun() {
    try { return JSON.parse(localStorage.getItem(STORAGE_KEY) || 'null'); }
    catch (_) { return null; }
  }

  function setMode(mode) {
    const radio = form.querySelector(`input[name="mode"][value="${mode === 'quick' ? 'quick' : 'deep'}"]`);
    if (radio) radio.checked = true;
    const submit = form.querySelector('button.primary');
    if (submit && !submit.disabled) submit.textContent = mode === 'quick' ? '开始快速研究' : '开始深度研究';
  }

  function resetDimensions() {
    dimensionState.forEach(value => Object.assign(value, {
      state:'waiting', detail:'等待前序阶段', result:null,
    }));
    renderLiveDimensions();
  }

  function renderLiveDimensions() {
    liveDimensions.innerHTML = dimensionDefs.map(([key, number, title]) => {
      const value = dimensionState.get(key);
      const result = value.result;
      return `<article class="sa-live-dimension" data-state="${esc(value.state)}" data-live-dimension="${key}">
        <div class="sa-live-dimension-head"><strong>${number} · ${title}</strong><span>${stateLabels[value.state] || '等待'}</span></div>
        <p>${esc(value.detail || '等待前序阶段')}</p>
        ${result ? `<div class="sa-live-score ${scoreClass(result.score)}">${fmt(result.score, 1)} / 100 · ${esc(result.stance || '')}</div>` : ''}
      </article>`;
    }).join('');
  }

  function updateDimension(key, stateValue, detail = '', result = null) {
    const value = dimensionState.get(key);
    if (!value) return;
    value.state = stateValue;
    value.detail = detail || value.detail;
    if (result) value.result = result;
    renderLiveDimensions();
    if (result) renderProgressiveReport();
  }

  function evidenceLinks(item) {
    const seen = new Set();
    return (item.evidence || []).filter(value => {
      const url = safeUrl(value?.source?.url);
      if (!url || seen.has(url)) return false;
      seen.add(url);
      return true;
    }).slice(0, 8).map(value => {
      const source = value.source || {};
      return `<a href="${esc(source.url)}" target="_blank" rel="noopener noreferrer"
        title="${esc(value.title || source.name || '证据来源')}"><em>L${esc(source.level || '—')}</em>${esc(source.name || value.title || '查看来源')}</a>`;
    }).join('');
  }

  function metricMarkup(metric) {
    const label = esc(displayText(metric.label, '指标'));
    const display = esc(displayText(metric.display, '—'));
    const note = displayText(metric.note);
    const url = safeUrl(metric.url);
    return `<div class="sa-metric"><span>${label}</span><strong title="${display}">${url
      ? `<a href="${esc(url)}" target="_blank" rel="noopener noreferrer">${display}</a>`
      : display}</strong>${note ? `<small title="${esc(note)}">${esc(note)}</small>` : ''}</div>`;
  }

  function dimensionMarkup(item) {
    const findings = [
      ...displayList(item.signals).map(value => ({value, risk:false})),
      ...displayList(item.risks).map(value => ({value, risk:true})),
    ].slice(0, 8);
    const counterpoints = displayList(item.counterpoints);
    const openQuestions = displayList(item.open_questions);
    const citations = evidenceLinks(item);
    return `<article class="sa-dimension" data-dimension="${esc(item.key)}">
      <div class="sa-dimension-index"><span>${esc(item.number)}</span><strong>${esc(item.title)}</strong>
        <small>${esc(item.as_of ? `截至 ${displayText(item.as_of)}` : '数据时间待核查')}</small></div>
      <div class="sa-dimension-summary"><div class="sa-dimension-score ${scoreClass(item.score)}">
          <strong>${fmt(item.score, 1)}</strong><span>${esc(displayText(item.stance, '待核查'))} · ${statusLabel(item.status)} · ${item.review_passes >= 2 ? '双重审查' : item.generation === 'llm_assisted' ? '模型复核' : '规则生成'}</span></div>
        <p>${esc(displayText(item.summary, '该维结论格式异常，请重新运行分析。'))}</p>
        ${item.degraded_reason ? `<div class="sa-degraded-reason">降级：${esc(warningText(item.degraded_reason))}</div>` : ''}
        ${findings.length ? `<ul class="sa-evidence-list">${findings.map(row =>
          `<li class="${row.risk ? 'risk' : ''}">${esc(row.value)}</li>`).join('')}</ul>` : ''}
        ${counterpoints.length ? `<div class="sa-counter-review"><strong>反方审查</strong><ul>${counterpoints.map(value => `<li>${esc(value)}</li>`).join('')}</ul></div>` : ''}
        ${openQuestions.length ? `<div class="sa-open-questions"><strong>仍待核查</strong><ul>${openQuestions.map(value => `<li>${esc(value)}</li>`).join('')}</ul></div>` : ''}
        ${citations ? `<div class="sa-citations" aria-label="证据来源">${citations}</div>` : ''}</div>
      <div class="sa-metrics">${(item.metrics || []).slice(0, 12).map(metricMarkup).join('')
        || '<div class="sa-metric"><span>数据状态</span><strong>暂无可用指标</strong></div>'}</div>
    </article>`;
  }

  function renderProgressiveReport() {
    const results = dimensionDefs.map(([key]) => dimensionState.get(key).result).filter(Boolean);
    if (!results.length) return;
    reportRoot.innerHTML = `<section class="sa-report sa-progressive-report">
      <div class="sa-section-heading"><h3>已完成研判</h3><span>${results.length} / 6 · 完成一维即交付</span></div>
      <section class="sa-dimensions" aria-label="渐进六维分析">${results.map(dimensionMarkup).join('')}</section>
    </section>`;
  }

  function reportText(report) {
    const instrument = report.instrument || {};
    const overall = report.overall || {};
    const lines = [
      `${instrument.name || instrument.symbol}（${instrument.symbol || ''}）六维分析`,
      `综合分 ${overall.score} / 100 · ${overall.stance} · 数据覆盖 ${overall.coverage}%`,
      displayText(overall.thesis), displayText(overall.summary), '',
    ];
    (report.dimensions || []).forEach(item => {
      lines.push(`${item.number} ${item.title}｜${item.score}/100｜${item.stance}`);
      lines.push(displayText(item.summary));
      displayList(item.signals).slice(0, 4).forEach(value => lines.push(`- ${value}`));
      displayList(item.risks).slice(0, 3).forEach(value => lines.push(`- 风险：${value}`));
      (item.evidence || []).filter(value => safeUrl(value?.source?.url)).slice(0, 5)
        .forEach(value => lines.push(`- 来源：${value.source.name} ${value.source.url}`));
      lines.push('');
    });
    lines.push(report.disclaimer || '仅作研究，不构成投资建议。');
    return lines.filter((value, index) => value || lines[index - 1]).join('\n');
  }

  function researchDepthMarkup(report) {
    const research = report.research || {};
    const depth = research.depth || {};
    if (!depth.label) return '';
    const gaps = displayList(depth.gaps);
    const counts = depth.evidence_counts || {};
    return `<section class="sa-depth-audit" data-depth-status="${esc(depth.status || 'degraded')}">
      <div class="sa-section-heading"><h3>${esc(displayText(depth.label, '研究完整度待核查'))}</h3>
        <span>完整度 ${fmt(depth.score, 1)} / 100</span></div>
      <div class="sa-depth-grid">
        <div><span>逐维证据</span><strong>${dimensionDefs.map(([key, , title]) => `${title} ${Number(counts[key] || 0)}`).join(' · ')}</strong></div>
        <div><span>审查进度</span><strong>首轮 ${Number(depth.dimension_review_passes || 0)}/6 · 反方 ${Number(depth.counter_review_passes || 0)}/6 · 终审 ${depth.final_reviewed ? '完成' : '未完成'}</strong></div>
      </div>
      ${gaps.length ? `<div class="sa-depth-gaps"><strong>为什么没有达到目标强度</strong><ul>${gaps.map(value => `<li>${esc(value)}</li>`).join('')}</ul></div>` : ''}
    </section>`;
  }

  function deepReviewMarkup(report) {
    const review = report.deep_review || {};
    if (!review.summary && review.status !== 'complete') return '';
    const groups = [
      ['证据冲突', review.contradictions], ['仍然未知', review.unknowns],
      ['潜在催化剂', review.catalysts], ['结论失效条件', review.invalidation_conditions],
    ];
    return `<section class="sa-deep-review"><div class="sa-section-heading"><h3>深度证伪终审</h3><span>${review.status === 'complete' ? '独立二次复核' : '未完整执行'}</span></div>
      ${displayText(review.summary) ? `<p>${esc(displayText(review.summary))}</p>` : ''}
      <div class="sa-deep-review-grid">${groups.map(([title, values]) => {
        const items = displayList(values);
        return items.length ? `<article><h4>${title}</h4><ul>${items.map(value => `<li>${esc(value)}</li>`).join('')}</ul></article>` : '';
      }).join('')}</div></section>`;
  }

  function renderReport(report) {
    const instrument = report.instrument || {};
    const quote = report.quote || {};
    const overall = report.overall || {};
    const research = report.research || {};
    const risks = [
      ...displayList(overall.risks),
      ...(Array.isArray(report.warnings) ? report.warnings.map(warningText).filter(Boolean) : []),
    ].slice(0, 14);
    stage.hidden = true;
    reportRoot.innerHTML = `<article class="sa-report">
      <header class="sa-report-head">
        <div class="sa-report-identity"><span class="sa-report-symbol">${esc(instrument.symbol || '')} · ${esc(instrument.market_label || instrument.market || '')}</span>
          <h2>${esc(instrument.name || instrument.en_name || instrument.symbol || '标的')}</h2>
          <p class="sa-thesis">${esc(displayText(overall.thesis, '结论待核查'))}</p><p class="sa-summary">${esc(displayText(overall.summary))}</p></div>
        <div class="sa-report-score"><div class="sa-score-heading"><span>COMPOSITE SCORE</span><strong>${esc(overall.stance || '待核查')}</strong></div>
          <div class="sa-score-number ${scoreClass(overall.score)}"><strong>${fmt(overall.score, 1)}</strong><span>/ 100</span></div>
          <div class="sa-score-track" style="--sa-score:${Math.max(0, Math.min(100, Number(overall.score) || 0))}%" aria-label="综合分 ${fmt(overall.score, 1)}"></div>
          <div class="sa-report-meta"><div><span>最近收盘</span><strong>${fmt(quote.current)} · ${fmt(quote.change_pct, 2, '%')}</strong></div>
            <div><span>数据截至</span><strong>${esc(report.data_as_of || '—')}</strong></div>
            <div><span>数据覆盖</span><strong>${fmt(overall.coverage, 0, '%')}</strong></div>
            <div><span>结论置信</span><strong>${fmt(overall.confidence, 0, '%')}</strong></div></div>
          <div class="sa-report-tools"><button class="sa-copy" type="button" data-sa-copy>复制报告摘要</button></div></div>
      </header>
      ${researchDepthMarkup(report)}
      <section class="sa-dimensions" aria-label="六维分析">${(report.dimensions || []).map(dimensionMarkup).join('')}</section>
      ${deepReviewMarkup(report)}
      <section class="sa-scenarios"><div class="sa-section-heading"><h3>情景验证</h3><span>条件触发，不是确定性预测</span></div>
        <div class="sa-scenario-list">${(report.scenarios || []).map(item => `<article class="sa-scenario">
          <span>${esc(item.priority || '')}</span><h4>${esc(item.title || '')}</h4>
          <p><strong>触发</strong>　${esc(item.condition || '')}</p><p><strong>应对</strong>　${esc(item.response || '')}</p></article>`).join('')}</div></section>
      ${risks.length ? `<section class="sa-risk-ledger"><h3>总风险清单</h3><ul>${risks.map(value => `<li>${esc(value)}</li>`).join('')}</ul></section>` : ''}
      <div class="sa-research-meta"><span>${research.mode === 'quick' ? '快速联网研究' : '深度双重审查'}</span>
        <span>耗时 ${duration(research.elapsed_seconds)}</span><span>${research.evidence_count || 0} 条证据</span>
        <span>${(research.sources || []).length} 个去重来源</span><span>报告 schema ${esc(report.schema_version || '—')}</span></div>
      <footer class="sa-disclaimer">${esc(report.disclaimer || '')}</footer>
    </article>`;
    reportRoot.querySelector('[data-sa-copy]')?.addEventListener('click', async event => {
      const button = event.currentTarget;
      try {
        await navigator.clipboard.writeText(reportText(report));
        button.textContent = '已复制';
        setTimeout(() => { if (button.isConnected) button.textContent = '复制报告摘要'; }, 1400);
      } catch (error) { reportLocalError('个股分析', '报告摘要未能复制', error); }
    });
    if (!REDUCED_MOTION) reportRoot.scrollIntoView({behavior:'smooth', block:'start'});
  }

  function renderFailure(error, status = 'failed') {
    const message = error?.problem?.message || error?.message || (status === 'cancelled' ? '任务已取消' : '分析任务未完成');
    stage.hidden = false;
    stage.innerHTML = `<div class="sa-failure"><span aria-hidden="true">×</span><div><h3>${status === 'cancelled' ? '分析已取消' : '报告没有生成'}</h3>
      <p>${esc(message)}${status === 'cancelled' ? '' : '。可以保留同一标的并重新提交。'}</p></div></div>`;
  }

  function showLive(run) {
    stage.hidden = true;
    live.hidden = false;
    cancelButton.disabled = terminalStatuses.has(run.status);
    currentPhase.textContent = run.phase || '正在创建统一任务';
    clearInterval(tickTimer);
    tickTimer = setInterval(updateClock, 1000);
    updateClock();
  }

  function updateClock() {
    if (!activeRun) return;
    const elapsed = Math.max(0, (Date.now() - activeRun.startedAt) / 1000);
    elapsedNode.textContent = duration(elapsed);
    if (terminalStatuses.has(activeRun.status)) {
      etaNode.textContent = '已结束';
      return;
    }
    if (activeRun.eta != null) etaNode.textContent = duration(activeRun.eta);
    else if (lastProgress > 4) etaNode.textContent = duration(Math.max(0, elapsed * (100 - lastProgress) / lastProgress));
    else etaNode.textContent = activeRun.mode === 'quick' ? '约 03:00' : '约 10:00';
  }

  function eventParts(value) {
    const nested = value?.event && typeof value.event === 'object' ? value.event : {};
    const type = value?.type || nested.type || value?.event_type || '';
    const payload = value?.payload || nested.payload || value?.data || nested.data || nested;
    return {type, payload:payload && typeof payload === 'object' ? payload : {}, seq:Number(value?.seq || 0)};
  }

  function applyEvent(value) {
    const {type, payload, seq} = eventParts(value);
    lastEventSeq = Math.max(lastEventSeq, seq);
    if (payload.progress != null) lastProgress = Math.max(lastProgress, Number(payload.progress) || 0);
    if (type === 'evidence_collection_started') {
      currentPhase.textContent = '联网取证与结构化数据采集';
      dimensionDefs.forEach(([key]) => updateDimension(key, 'collecting', '正在并发核对来源'));
    } else if (type === 'evidence_search_started') {
      currentPhase.textContent = `联网搜索 · 第 ${payload.round || '—'} 轮${payload.queries ? ` · ${payload.query || 1}/${payload.queries}` : ''}`;
    } else if (type === 'dimension_started') {
      currentPhase.textContent = `${dimensionDefs.find(row => row[0] === payload.dimension)?.[2] || '维度'}研判`;
      updateDimension(payload.dimension, payload.stage === 'rules' ? 'collecting' : 'inference',
        payload.stage === 'rules' ? '正在执行确定性评分' : '证据已就绪，正在独立推理');
    } else if (type === 'dimension_audit_started') {
      currentPhase.textContent = `${dimensionDefs.find(row => row[0] === payload.dimension)?.[2] || '维度'}反方审查`;
      updateDimension(payload.dimension, 'inference', '第一轮完成，正在寻找反例、遗漏与时点错配');
    } else if (type === 'dimension_completed' || type === 'dimension_degraded') {
      const result = payload.result || payload.dimension_result;
      updateDimension(payload.dimension || result?.key, type === 'dimension_degraded' ? 'degraded' : 'complete',
        type === 'dimension_degraded' ? (result?.degraded_reason || '已使用规则结果') : '研判完成，可立即核查', result);
      currentPhase.textContent = `已完成 ${payload.completed || dimensionDefs.filter(([key]) => dimensionState.get(key).result).length} / 6 维`;
    } else if (type === 'final_review_started') {
      currentPhase.textContent = '六维交叉复核';
    } else if (type === 'deep_final_review_started') {
      currentPhase.textContent = '深度证伪终审';
    } else if (type === 'analysis_completed') {
      lastProgress = 100;
      if (payload.report) renderReport(payload.report);
    }
  }

  async function refreshAnalysis(run) {
    const data = await window.QuantMasterAPI(`/api/v1/market/stock-analyses/${encodeURIComponent(run.analysisId)}`, {cache:'no-store'});
    const report = data.report || (data.schema_version ? data : null);
    for (const item of (data.dimensions || report?.dimensions || [])) {
      updateDimension(item.key, item.degraded_reason ? 'degraded' : 'complete',
        item.degraded_reason || '研判完成，可立即核查', item);
    }
    if (report && (data.status == null || terminalStatuses.has(data.status) || report.overall?.thesis)) renderReport(report);
    return data;
  }

  async function pollRun(run, token) {
    showLive(run);
    while (activeRun === run && token === activeToken) {
      try {
        const events = await window.QuantMasterAPI(
          `/api/v1/jobs/${encodeURIComponent(run.jobId)}/events?after=${lastEventSeq}`, {cache:'no-store'},
        );
        const items = events.items || events.events || [];
        items.forEach(applyEvent);
        if (items.some(value => ['dimension_completed', 'dimension_degraded', 'analysis_completed']
          .includes(eventParts(value).type))) await refreshAnalysis(run);

        const payload = await window.QuantMasterAPI(`/api/v1/jobs/${encodeURIComponent(run.jobId)}`, {cache:'no-store'});
        const job = payload.job || payload;
        run.status = job.status || run.status;
        run.phase = job.phase || job.current_phase || currentPhase.textContent;
        run.eta = job.estimated_remaining_seconds ?? job.eta_seconds ?? null;
        if (job.progress != null) lastProgress = Math.max(lastProgress, Number(job.progress) || 0);
        currentPhase.textContent = run.phase;
        saveRun(run);
        cancelButton.disabled = terminalStatuses.has(run.status) || run.status === 'cancelling';
        if (terminalStatuses.has(run.status)) {
          clearInterval(tickTimer);
          if (run.status === 'completed' || run.status === 'completed_with_errors') {
            const finalAnalysis = await refreshAnalysis(run);
            if (!finalAnalysis.report) {
              renderFailure({message:finalAnalysis.error || '最终报告产物暂时不可用'});
            }
          }
          else renderFailure({message:job.error || job.message}, run.status);
          return;
        }
      } catch (error) {
        if (token !== activeToken) return;
        currentPhase.textContent = '连接暂时中断，后台任务仍在运行';
        reportLocalError('个股分析', '任务状态暂时无法读取', error);
      }
      await new Promise(resolve => setTimeout(resolve, 900));
    }
  }

  async function beginRun(query, mode) {
    activeToken += 1;
    lastEventSeq = 0;
    lastProgress = 0;
    resetDimensions();
    reportRoot.innerHTML = '';
    const idempotency = crypto.randomUUID ? crypto.randomUUID() : `${Date.now()}-${Math.random()}`;
    const data = await window.QuantMasterAPI('/api/v1/market/stock-analyses', {
      method:'POST', headers:{'Content-Type':'application/json', 'Idempotency-Key':idempotency},
      body:JSON.stringify({query, mode}),
    });
    const run = {
      analysisId:data.analysis_id, jobId:data.job_id, query, mode,
      status:data.status || 'queued', phase:'任务已提交，等待取数', startedAt:Date.now(), eta:null,
    };
    if (!run.analysisId || !run.jobId) throw new Error('后台没有返回可跟踪的分析任务，请稍后重试。');
    activeRun = run;
    saveRun(run);
    pollRun(run, activeToken);
  }

  cancelButton.addEventListener('click', async () => {
    if (!activeRun || terminalStatuses.has(activeRun.status)) return;
    cancelButton.disabled = true;
    currentPhase.textContent = '正在请求安全取消';
    try {
      await window.QuantMasterAPI(`/api/v1/jobs/${encodeURIComponent(activeRun.jobId)}/cancel`, {method:'POST'});
      activeRun.status = 'cancelling';
      saveRun(activeRun);
    } catch (error) {
      cancelButton.disabled = false;
      reportLocalError('个股分析', '取消请求未能送达', error);
    }
  });

  form.querySelectorAll('input[name="mode"]').forEach(radio => radio.addEventListener('change', () => setMode(radio.value)));

  function hideSuggestions() {
    suggestionRequest += 1;
    clearTimeout(suggestionTimer);
    suggestions.hidden = true;
    input.setAttribute('aria-expanded', 'false');
    input.removeAttribute('aria-activedescendant');
    activeSuggestion = -1;
  }

  function syncSuggestionSelection() {
    suggestions.querySelectorAll('[data-sa-suggestion]').forEach((button, index) => {
      const selected = index === activeSuggestion;
      button.setAttribute('aria-selected', String(selected));
      if (selected) input.setAttribute('aria-activedescendant', button.id);
    });
  }

  function chooseSuggestion(index) {
    const item = suggestionItems[index];
    if (!item) return;
    input.value = item.symbol;
    hideSuggestions();
    input.focus();
  }

  async function searchSuggestions() {
    const query = input.value.trim();
    if (query.length < 2) { hideSuggestions(); return; }
    const requestId = ++suggestionRequest;
    try {
      const data = await window.QuantMasterAPI(
        `/api/v1/market/instruments/search?q=${encodeURIComponent(query)}&limit=8&online=false`,
        {cache:'no-store'},
      );
      if (requestId !== suggestionRequest || input.value.trim() !== query) return;
      suggestionItems = data.items || [];
      activeSuggestion = suggestionItems.length ? 0 : -1;
      suggestions.innerHTML = suggestionItems.length ? suggestionItems.map((item, index) =>
        `<button class="sa-suggestion" id="sa-suggestion-${index}" type="button" role="option"
          aria-selected="${index === activeSuggestion}" data-sa-suggestion="${index}">
          <span><strong>${esc(item.name || item.en_name || item.symbol)}</strong><small>${esc(item.symbol)}</small></span>
          <em>${esc(item.market_label || item.market || '')}</em></button>`
      ).join('') : '<div class="sa-suggestion-empty">本地证券主数据中没有匹配项；可直接输入完整代码后尝试。</div>';
      suggestions.hidden = false;
      input.setAttribute('aria-expanded', 'true');
      syncSuggestionSelection();
    } catch (error) {
      hideSuggestions();
      reportLocalError('个股分析', '标的建议未能读取', error);
    }
  }

  input.addEventListener('input', () => {
    hideSuggestions();
    suggestionTimer = setTimeout(searchSuggestions, 180);
  });
  input.addEventListener('keydown', event => {
    if (suggestions.hidden || !suggestionItems.length) return;
    if (event.key === 'ArrowDown') {
      event.preventDefault(); activeSuggestion = (activeSuggestion + 1) % suggestionItems.length; syncSuggestionSelection();
    } else if (event.key === 'ArrowUp') {
      event.preventDefault(); activeSuggestion = (activeSuggestion - 1 + suggestionItems.length) % suggestionItems.length; syncSuggestionSelection();
    } else if (event.key === 'Enter' && activeSuggestion >= 0) {
      event.preventDefault(); chooseSuggestion(activeSuggestion);
    } else if (event.key === 'Escape') hideSuggestions();
  });
  suggestions.addEventListener('click', event => {
    const button = event.target.closest('[data-sa-suggestion]');
    if (button) chooseSuggestion(Number(button.dataset.saSuggestion));
  });
  document.addEventListener('pointerdown', event => {
    if (!event.target.closest('.sa-search-shell')) hideSuggestions();
  });
  document.querySelectorAll('[data-sa-example]').forEach(button => button.addEventListener('click', () => {
    input.value = button.dataset.saExample;
    hideSuggestions();
    form.requestSubmit();
  }));

  form.addEventListener('submit', async event => {
    event.preventDefault();
    const query = input.value.trim();
    if (!query) return;
    const mode = form.elements.mode.value === 'quick' ? 'quick' : 'deep';
    hideSuggestions();
    busy(form, true, mode === 'quick' ? '快速研究中…' : '深度研究中…');
    try { await beginRun(query, mode); }
    catch (error) {
      renderFailure(error);
      reportLocalError('个股分析', '六维任务未能提交', error);
    } finally { busy(form, false); setMode(mode); }
  });

  window.loadStockAnalysis = async () => {
    if (loaded) return;
    loaded = true;
    resetDimensions();
    const saved = storedRun();
    if (saved?.analysisId && saved?.jobId) {
      activeRun = saved;
      input.value = saved.query || '';
      setMode(saved.mode);
      activeToken += 1;
      try { await refreshAnalysis(saved); } catch (_) { /* poll supplies the diagnostic */ }
      pollRun(saved, activeToken);
    } else queueMicrotask(() => input.focus({preventScroll:true}));
  };
})();
