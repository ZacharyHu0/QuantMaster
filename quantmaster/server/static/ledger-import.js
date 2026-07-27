(() => {
  'use strict';

  const form = document.getElementById('csv-preview-form');
  const fileInput = document.getElementById('broker-csv');
  const mappingRoot = document.getElementById('csv-mapping');
  const previewRoot = document.getElementById('csv-preview');
  const actions = document.getElementById('csv-submit-actions');
  const status = document.getElementById('csv-import-status');
  let preview = null;
  let failedRows = [];

  function html(value) {
    return String(value ?? '').replace(/[&<>"']/g, char => ({
      '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;',
    }[char]));
  }

  function setStep(step) {
    document.querySelectorAll('[data-import-step]').forEach(item => {
      const value = Number(item.dataset.importStep);
      item.classList.toggle('active', value === step);
      item.classList.toggle('done', value < step);
    });
  }

  function selectedMapping() {
    const result = {fees: []};
    mappingRoot.querySelectorAll('select[data-map]').forEach(select => {
      if (select.value) result[select.dataset.map] = select.value;
    });
    mappingRoot.querySelectorAll('input[data-fee]:checked').forEach(input => result.fees.push(input.value));
    return result;
  }

  function optionList(columns, selected, optional = false) {
    const empty = optional ? '<option value="">不映射</option>' : '<option value="">请选择</option>';
    return empty + columns.map(column => `<option value="${html(column)}" ${column === selected ? 'selected' : ''}>${html(column)}</option>`).join('');
  }

  function renderMapping(data) {
    const labels = {date: '成交日期', symbol: '证券代码', side: '买卖方向', price: '成交价格', shares: '成交数量', note: '备注（可选）'};
    mappingRoot.hidden = false;
    mappingRoot.innerHTML = `<div class="group-heading"><div><h3>列映射</h3><p>系统已按常见中英文字段名自动匹配；费用可以选择多列并求和。</p></div>
      <button class="ghost" id="csv-remap" type="button">重新验证映射</button></div>
      <div class="mapping-grid">${Object.entries(labels).map(([key, label]) => `<label>${label}<select data-map="${key}">${optionList(data.columns, data.suggested_mapping[key], key === 'note')}</select></label>`).join('')}</div>
      <div class="mapping-grid" style="margin-top:12px"><label>费用列（可多选）<span>${data.columns.map(column => `<label style="display:inline-flex;align-items:center;gap:4px;margin:5px 10px 0 0"><input data-fee type="checkbox" value="${html(column)}" ${data.suggested_mapping.fees.includes(column) ? 'checked' : ''} style="min-width:auto">${html(column)}</label>`).join('')}</span></label></div>`;
    document.getElementById('csv-remap').addEventListener('click', () => runPreview(selectedMapping(), false));
  }

  function renderPreview(data) {
    preview = data;
    failedRows = data.rows.filter(row => row.errors?.length);
    previewRoot.hidden = false;
    actions.hidden = false;
    setStep(2);
    const rows = data.rows.map(row => {
      const record = row.record || {};
      const rowStatus = row.errors?.length ? row.errors.join('；') : row.duplicate ? '疑似重复' : '有效';
      return `<tr><td>${row.row_number}</td><td>${html(record.date || '—')}</td><td>${html(record.symbol || '—')}</td>
        <td>${html(record.side === 'buy' ? '买入' : record.side === 'sell' ? '卖出' : '—')}</td>
        <td>${html(record.price ?? '—')}</td><td>${html(record.shares ?? '—')}</td><td>${html(record.fee ?? '—')}</td>
        <td class="${row.errors?.length ? 'up' : ''}">${html(rowStatus)}</td></tr>`;
    }).join('');
    previewRoot.innerHTML = `<div class="preview-summary"><span>编码 <strong>${html(data.encoding)}</strong></span>
      <span>总计 <strong>${data.total_rows}</strong></span><span>有效 <strong>${data.valid_count}</strong></span>
      <span>坏行 <strong>${data.error_count}</strong></span><span>重复 <strong>${data.duplicate_count}</strong></span></div>
      ${data.batch_duplicate ? '<div class="check-result warning">检测到相同文件哈希：这份文件此前已经导入。</div>' : ''}
      <div class="table-scroll"><table><thead><tr><th>行</th><th>日期</th><th>代码</th><th>方向</th><th>价格</th><th>数量</th><th>费用</th><th>状态</th></tr></thead>
      <tbody>${rows || '<tr><td colspan="8">没有数据行</td></tr>'}</tbody></table></div>`;
    status.textContent = `服务端已重新解析 ${data.total_rows} 行；提交时还会再次解析，不信任浏览器预览。`;
    document.getElementById('csv-download-errors').hidden = !failedRows.length;
  }

  async function runPreview(mapping = null, resetMapping = true) {
    if (!fileInput.files[0]) return;
    status.textContent = '正在识别编码并逐行校验…';
    setStep(1);
    try {
      await window.QuantMasterManagement.ensureSettings();
      const body = new FormData();
      body.append('file', fileInput.files[0]);
      if (mapping) body.append('mapping', JSON.stringify(mapping));
      const data = await window.QuantMasterManagement.request('/api/ledger/import/preview', {method: 'POST', body});
      if (resetMapping) renderMapping(data);
      renderPreview(data);
    } catch (error) {
      status.textContent = `预览失败：${error.message}`;
      status.className = 'err';
    }
  }

  form.addEventListener('submit', event => {
    event.preventDefault();
    status.className = 'hint';
    runPreview(null, true);
  });

  fileInput.addEventListener('change', () => {
    const file = fileInput.files[0];
    form.querySelector('.file-drop strong').textContent = file ? file.name : '选择券商导出的 CSV';
    previewRoot.hidden = true;
    mappingRoot.hidden = true;
    actions.hidden = true;
    setStep(1);
  });

  document.getElementById('csv-submit').addEventListener('click', async event => {
    if (!fileInput.files[0] || !preview) return;
    const button = event.target;
    button.disabled = true;
    status.className = 'hint';
    status.textContent = '正在重新解析并写入单个数据库事务…';
    try {
      const body = new FormData();
      body.append('file', fileInput.files[0]);
      body.append('mapping', JSON.stringify(selectedMapping()));
      body.append('strict', document.querySelector('[name="csv-mode"]:checked').value === 'strict');
      body.append('include_duplicates', document.getElementById('csv-duplicates').checked);
      const data = await window.QuantMasterManagement.request('/api/ledger/import/submit', {method: 'POST', body});
      failedRows = data.failed_rows || [];
      status.textContent = `已导入 ${data.imported} 笔；跳过坏行 ${data.skipped_invalid}，跳过重复 ${data.skipped_duplicates}。`;
      setStep(3);
      document.getElementById('csv-download-errors').hidden = !failedRows.length;
      if (typeof window.loadLedger === 'function') await window.loadLedger();
      if (typeof window.loadAssetLists === 'function') await window.loadAssetLists(false);
    } catch (error) {
      failedRows = error.detail?.failed_rows || failedRows;
      status.className = 'err';
      status.textContent = `未导入：${error.message}`;
      document.getElementById('csv-download-errors').hidden = !failedRows.length;
    } finally {
      button.disabled = false;
    }
  });

  document.getElementById('csv-download-errors').addEventListener('click', () => {
    if (!failedRows.length) return;
    const quote = value => `"${String(value ?? '').replace(/"/g, '""')}"`;
    const csv = ['row_number,errors,raw', ...failedRows.map(row => [
      row.row_number, quote((row.errors || []).join('；')), quote(JSON.stringify(row.raw || {})),
    ].join(','))].join('\r\n');
    const blob = new Blob(['\ufeff', csv], {type: 'text/csv;charset=utf-8'});
    const link = document.createElement('a');
    link.href = URL.createObjectURL(blob);
    link.download = 'quantmaster-import-errors.csv';
    link.click();
    setTimeout(() => URL.revokeObjectURL(link.href), 1000);
  });
})();
