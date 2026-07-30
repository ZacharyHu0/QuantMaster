(() => {
  const form = document.getElementById('stock-analysis-form');
  if (!form) return;
  const input = document.getElementById('stock-analysis-query');
  const stage = document.getElementById('stock-analysis-stage');
  const reportRoot = document.getElementById('stock-analysis-report');
  const suggestions = document.getElementById('stock-analysis-suggestions');
  const phases = [
    [5, '确认标的'], [22, '读取行情'], [38, '计算技术面'], [54, '核查基本面'],
    [68, '消息与资金'], [80, '心理与宏观'], [92, '综合判断'], [100, '分析完成'],
  ];
  let controller = null;
  let activeSuggestion = -1;
  let suggestionItems = [];
  let suggestionTimer = 0;
  let suggestionRequest = 0;
  let loaded = false;

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

  function setProgress(event) {
    const progress = Math.max(0, Math.min(100, Number(event.progress) || 0));
    if (!stage.querySelector('.sa-progress')) {
      stage.innerHTML = `<div class="sa-progress" role="status">
        <div class="sa-progress-value"><strong data-sa-percent>0%</strong><span>ANALYSIS PROGRESS</span></div>
        <div class="sa-progress-body"><h3 data-sa-phase>准备分析</h3><p data-sa-detail>正在创建任务…</p>
          <div class="sa-progress-track" role="progressbar" aria-label="个股分析进度" aria-valuemin="0" aria-valuemax="100" aria-valuenow="0">
            <div class="sa-progress-fill"></div></div>
          <ol class="sa-progress-steps">${phases.map(([, label]) => `<li>${esc(label)}</li>`).join('')}</ol>
        </div></div>`;
    }
    const root = stage.querySelector('.sa-progress');
    root.style.setProperty('--sa-progress', progress / 100);
    root.querySelector('[data-sa-percent]').textContent = `${Math.round(progress)}%`;
    root.querySelector('[data-sa-phase]').textContent = event.phase || '正在分析';
    root.querySelector('[data-sa-detail]').textContent = event.detail || '';
    root.querySelector('[role=progressbar]').setAttribute('aria-valuenow', String(Math.round(progress)));
    root.querySelectorAll('.sa-progress-steps li').forEach((item, index) => {
      const [threshold] = phases[index];
      const previous = index ? phases[index - 1][0] : 0;
      item.classList.toggle('complete', progress >= threshold);
      item.classList.toggle('active', progress >= previous && progress < threshold);
    });
  }

  function metricMarkup(metric) {
    const label = esc(metric.label || '指标');
    const display = esc(metric.display || '—');
    const url = safeUrl(metric.url);
    return `<div class="sa-metric"><span>${label}</span><strong title="${display}">${url
      ? `<a href="${esc(url)}" target="_blank" rel="noopener noreferrer">${display}</a>`
      : display}</strong>${metric.note ? `<small title="${esc(metric.note)}">${esc(metric.note)}</small>` : ''}</div>`;
  }

  function dimensionMarkup(item) {
    const evidence = [
      ...(item.signals || []).map(value => ({value, risk:false})),
      ...(item.risks || []).map(value => ({value, risk:true})),
    ].slice(0, 6);
    return `<article class="sa-dimension" data-dimension="${esc(item.key)}">
      <div class="sa-dimension-index"><span>${esc(item.number)}</span><strong>${esc(item.title)}</strong>
        <small>${esc(item.as_of ? `截至 ${item.as_of}` : '数据时间待核查')}</small></div>
      <div class="sa-dimension-summary"><div class="sa-dimension-score ${scoreClass(item.score)}">
          <strong>${fmt(item.score, 1)}</strong><span>${esc(item.stance)} · ${statusLabel(item.status)}</span></div>
        <p>${esc(item.summary || '暂无可用结论。')}</p>
        ${evidence.length ? `<ul class="sa-evidence-list">${evidence.map(row =>
          `<li class="${row.risk ? 'risk' : ''}">${esc(row.value)}</li>`).join('')}</ul>` : ''}</div>
      <div class="sa-metrics">${(item.metrics || []).slice(0, 10).map(metricMarkup).join('')
        || '<div class="sa-metric"><span>数据状态</span><strong>暂无可用指标</strong></div>'}</div>
    </article>`;
  }

  function reportText(report) {
    const instrument = report.instrument || {};
    const overall = report.overall || {};
    const lines = [
      `${instrument.name || instrument.symbol}（${instrument.symbol || ''}）六维分析`,
      `综合分 ${overall.score} / 100 · ${overall.stance} · 数据覆盖 ${overall.coverage}%`,
      overall.thesis || '', overall.summary || '', '',
    ];
    (report.dimensions || []).forEach(item => {
      lines.push(`${item.number} ${item.title}｜${item.score}/100｜${item.stance}`);
      lines.push(item.summary || '');
      (item.signals || []).slice(0, 3).forEach(value => lines.push(`- ${value}`));
      (item.risks || []).slice(0, 2).forEach(value => lines.push(`- 风险：${value}`));
      lines.push('');
    });
    lines.push(report.disclaimer || '仅作研究，不构成投资建议。');
    return lines.filter((value, index) => value || lines[index - 1]).join('\n');
  }

  function renderReport(report) {
    const instrument = report.instrument || {};
    const quote = report.quote || {};
    const overall = report.overall || {};
    const risks = [...(overall.risks || []), ...(report.warnings || [])].slice(0, 10);
    stage.hidden = true;
    reportRoot.innerHTML = `<article class="sa-report">
      <header class="sa-report-head">
        <div class="sa-report-identity"><span class="sa-report-symbol">${esc(instrument.symbol || '')} · ${esc(instrument.market_label || instrument.market || '')}</span>
          <h2>${esc(instrument.name || instrument.en_name || instrument.symbol || '标的')}</h2>
          <p class="sa-thesis">${esc(overall.thesis || '')}</p><p class="sa-summary">${esc(overall.summary || '')}</p></div>
        <div class="sa-report-score"><div class="sa-score-heading"><span>COMPOSITE SCORE</span><strong>${esc(overall.stance || '待核查')}</strong></div>
          <div class="sa-score-number ${scoreClass(overall.score)}"><strong>${fmt(overall.score, 1)}</strong><span>/ 100</span></div>
          <div class="sa-score-track" style="--sa-score:${Math.max(0, Math.min(100, Number(overall.score) || 0))}%" aria-label="综合分 ${fmt(overall.score, 1)}"></div>
          <div class="sa-report-meta"><div><span>最近收盘</span><strong>${fmt(quote.current)} · ${fmt(quote.change_pct, 2, '%')}</strong></div>
            <div><span>数据截至</span><strong>${esc(report.data_as_of || '—')}</strong></div>
            <div><span>数据覆盖</span><strong>${fmt(overall.coverage, 0, '%')}</strong></div>
            <div><span>结论置信</span><strong>${fmt(overall.confidence, 0, '%')}</strong></div></div>
          <div class="sa-report-tools"><button class="sa-copy" type="button" data-sa-copy>复制报告摘要</button></div></div>
      </header>
      <section class="sa-dimensions" aria-label="六维分析">${(report.dimensions || []).map(dimensionMarkup).join('')}</section>
      <section class="sa-scenarios"><div class="sa-section-heading"><h3>情景验证</h3><span>条件触发，不是确定性预测</span></div>
        <div class="sa-scenario-list">${(report.scenarios || []).map(item => `<article class="sa-scenario">
          <span>${esc(item.priority || '')}</span><h4>${esc(item.title || '')}</h4>
          <p><strong>触发</strong>　${esc(item.condition || '')}</p><p><strong>应对</strong>　${esc(item.response || '')}</p></article>`).join('')}</div></section>
      ${risks.length ? `<section class="sa-risk-ledger"><h3>总风险清单</h3><ul>${risks.map(value => `<li>${esc(value)}</li>`).join('')}</ul></section>` : ''}
      <footer class="sa-disclaimer">${esc(report.disclaimer || '')}　框架：stock-analysis-framework v1.0（安全适配版）</footer>
    </article>`;
    reportRoot.querySelector('[data-sa-copy]')?.addEventListener('click', async event => {
      const button = event.currentTarget;
      try {
        await navigator.clipboard.writeText(reportText(report));
        button.textContent = '已复制';
        setTimeout(() => { if (button.isConnected) button.textContent = '复制报告摘要'; }, 1400);
      } catch (error) {
        reportLocalError('个股分析', '报告摘要未能复制', error);
      }
    });
    if (!REDUCED_MOTION) reportRoot.scrollIntoView({behavior:'smooth', block:'start'});
  }

  function renderFailure(error) {
    const message = error?.problem?.message || error?.message || '分析任务未完成';
    stage.hidden = false;
    stage.innerHTML = `<div class="sa-failure"><span aria-hidden="true">×</span><div><h3>报告没有生成</h3>
      <p>${esc(message)}。请检查代码/名称和本地数据源后重试。</p></div></div>`;
  }

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
      if (selected) {
        input.setAttribute('aria-activedescendant', button.id);
        button.scrollIntoView({block:'nearest'});
      }
    });
  }

  function chooseSuggestion(index) {
    const item = suggestionItems[index];
    if (!item) return;
    input.value = item.symbol;
    input.dataset.instrumentName = item.name || item.en_name || '';
    hideSuggestions();
    input.focus();
  }

  async function searchSuggestions() {
    const query = input.value.trim();
    if (query.length < 2) {
      hideSuggestions();
      return;
    }
    const requestId = ++suggestionRequest;
    try {
      const data = await window.QuantMasterAPI(
        `/api/instruments/search?q=${encodeURIComponent(query)}&limit=8&online=false`,
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
    delete input.dataset.instrumentName;
    hideSuggestions();
    suggestionTimer = setTimeout(searchSuggestions, 180);
  });
  input.addEventListener('keydown', event => {
    if (suggestions.hidden || !suggestionItems.length) return;
    if (event.key === 'ArrowDown') {
      event.preventDefault();
      activeSuggestion = (activeSuggestion + 1) % suggestionItems.length;
      syncSuggestionSelection();
    } else if (event.key === 'ArrowUp') {
      event.preventDefault();
      activeSuggestion = (activeSuggestion - 1 + suggestionItems.length) % suggestionItems.length;
      syncSuggestionSelection();
    } else if (event.key === 'Enter' && activeSuggestion >= 0) {
      event.preventDefault();
      chooseSuggestion(activeSuggestion);
    } else if (event.key === 'Escape') hideSuggestions();
  });
  suggestions.addEventListener('click', event => {
    const button = event.target.closest('[data-sa-suggestion]');
    if (button) chooseSuggestion(Number(button.dataset.saSuggestion));
  });
  document.addEventListener('pointerdown', event => {
    if (!event.target.closest('.sa-search-shell')) hideSuggestions();
  });
  document.querySelectorAll('[data-sa-example]').forEach(button => {
    button.addEventListener('click', () => {
      input.value = button.dataset.saExample;
      hideSuggestions();
      form.requestSubmit();
    });
  });

  form.addEventListener('submit', async event => {
    event.preventDefault();
    const query = input.value.trim();
    if (!query) return;
    hideSuggestions();
    controller?.abort();
    controller = new AbortController();
    stage.hidden = false;
    reportRoot.innerHTML = '';
    setProgress({progress:1, phase:'准备分析', detail:`正在创建 ${query} 的六维分析任务`});
    busy(form, true, '分析生成中…');
    let report = null;
    try {
      await window.QuantMasterNDJSON('/api/stock-analysis/stream', {
        method:'POST', headers:{'Content-Type':'application/json'},
        body:JSON.stringify({query}), signal:controller.signal,
      }, streamEvent => {
        if (streamEvent.type === 'progress') setProgress(streamEvent);
        if (streamEvent.type === 'result') report = streamEvent.data;
      });
      if (!report) throw new Error('后台完成但没有返回分析报告');
      renderReport(report);
    } catch (error) {
      if (error?.name !== 'AbortError') {
        renderFailure(error);
        reportLocalError('个股分析', '六维报告未能生成', error);
      }
    } finally {
      busy(form, false);
    }
  });

  window.loadStockAnalysis = () => {
    if (loaded) return;
    loaded = true;
    queueMicrotask(() => input.focus({preventScroll:true}));
  };
})();
