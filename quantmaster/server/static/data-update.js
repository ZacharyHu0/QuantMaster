/* One durable update action, independent of the current workspace. */
(() => {
  const api = window.QuantMasterAPI;
  const button = document.getElementById('data-update-start');
  const summary = document.getElementById('data-update-summary');
  const details = document.getElementById('data-update-details');
  const auto = document.getElementById('data-update-auto');
  const steps = document.getElementById('data-update-steps');
  const active = job => job && ['queued','running','cancelling','interrupted'].includes(job.status);
  const labels = {stockdb:'本地行情库', consumers:'常用行情缓存', analysis:'市场与板块分析'};
  const states = {queued:'排队中', interrupted:'等待依赖或恢复', cancelling:'正在取消', completed:'完成', running:'执行中', failed:'未完成', partial:'部分完成', cancelled:'已取消', blocked:'暂未执行'};
  let submitting = null;
  let current = null;
  let revision = null;
  let polling = false;
  let timer;

  function render(value) {
    current = value.job;
    button.disabled = !!submitting || active(current);
    button.textContent = active(current) ? '后台更新中…' : '更新数据与分析';
    summary.textContent = current ? (current.detail || (active(current) ? '已排队，后台继续执行' : states[current.status] || current.status)) : '尚未手动更新；可查看自动维护状态';
    const a = value.automatic || {};
    summary.textContent = `行情库至 ${a.data_as_of || '尚未验收'} · ${summary.textContent}`;
    summary.title = summary.textContent;
    document.getElementById('data-update-current').textContent = summary.textContent;
    auto.textContent = `行情库自动更新：${a.enabled ? '已开启' : '已关闭'}；缓存自动维护：${a.cache_enabled ? '已开启' : '已关闭'}。后台：${a.worker_available ? '在线' : '离线，自动维护暂不可执行'}。行情已验收到 ${a.data_as_of || '尚无证据'}（目标 ${a.target_session || '待确认'}）。下一次 ${a.next_retry_at || a.next_update_at || '尚未安排'}。${a.detail || ''}`;
    steps.replaceChildren();
    const rows = Object.entries(current?.steps || {}).map(([key, v]) => ({kind:labels[key] || key, ...v}));
    rows.push(...(current?.results || []));
    const childIds = new Set((current?.children || []).map(j => j.id));
    rows.push(...(value.tasks || []).filter(j => j.type === 'data.refresh' && !childIds.has(j.id)).map(j => ({
      kind:`自动维护 · ${j.universe_name || j.scope || '行情缓存'}`,
      status:j.failed || j.warning_count ? 'partial' : j.status,
      detail:j.detail || `缺口 ${j.failed || 0}，证据警告 ${j.warning_count || 0}`,
    })));
    for (const row of rows) {
      const li = document.createElement('li');
      li.textContent = `${row.kind} · ${states[row.status] || row.status} · ${row.detail || ''}${row.data_as_of ? ' · ' + row.data_as_of : ''}`;
      steps.append(li);
    }
    // Ignore intermediate source changes: publish completed cache/analysis work only.
    const next = JSON.stringify((value.tasks || []).filter(j => !active(j)).map(j => [j.id,j.status,j.finished_at]));
    if (revision !== null && revision !== next) document.dispatchEvent(new CustomEvent('quantmaster:data-updated'));
    revision = next;
  }

  async function poll() {
    if (polling) return;
    polling = true;
    clearTimeout(timer);
    try { render(await api('/api/v1/data/update')); }
    catch (_) { summary.textContent = '更新状态连接暂时中断；后台任务不会因此取消，正在重新连接'; }
    finally {
      polling = false;
      timer = setTimeout(poll, document.hidden ? 30000 : active(current) ? 2000 : 15000);
    }
  }

  function start() {
    details.open = true;
    if (submitting || active(current)) return submitting || Promise.resolve(current);
    button.disabled = true;
    summary.textContent = '正在提交更新…';
    submitting = api('/api/v1/data/update', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({})})
      .then(job => { current = job; summary.textContent = job.detail || '更新已接纳，后台继续执行'; return job; })
      .catch(error => { summary.textContent = `更新未能接纳：${error.message}；请查看自动维护状态后重试`; })
      .finally(() => { submitting = null; button.disabled = active(current); void poll(); });
    return submitting;
  }
  button.addEventListener('click', start);
  document.addEventListener('keydown', event => { if (event.key === 'Escape') details.open = false; });
  document.addEventListener('visibilitychange', () => { if (!document.hidden) void poll(); });
  window.QuantMasterDataUpdate = {start};
  void poll();
})();
