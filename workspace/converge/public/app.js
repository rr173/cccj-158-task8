'use strict';

const $ = (id) => document.getElementById(id);
const state = {
  token: localStorage.getItem('converge-token') || '',
  role: localStorage.getItem('converge-role') || '',
  selectedObject: null,
  selectedCase: null,
};

const ROLE_FROM_TOKEN = {
  'operator-token': 'operator',
  'reviewer1-token': 'reviewer',
  'reviewer2-token': 'reviewer',
  'inspector-token': 'inspector',
  'admin-token': 'admin',
};

function short(h, n = 10) { return typeof h === 'string' ? h.slice(0, n) : h; }
function esc(s) { return String(s).replace(/[&<>"]/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c])); }

function toast(message, kind = '') {
  const box = document.createElement('div');
  if (kind) box.className = kind === 'ok' ? 'okbox' : 'errbox';
  box.textContent = message;
  $('toast').appendChild(box);
  setTimeout(() => box.remove(), 6000);
}

async function api(method, path, body) {
  const res = await fetch(path, {
    method,
    headers: {
      'Authorization': `Bearer ${state.token}`,
      ...(body !== undefined ? { 'Content-Type': 'application/json' } : {}),
    },
    body: body !== undefined ? JSON.stringify(body) : undefined,
  });
  const text = await res.text();
  let data = null;
  try { data = text ? JSON.parse(text) : null; } catch { data = { raw: text }; }
  if (!res.ok) {
    const err = new Error((data && data.error && (data.error.code + ': ' + data.error.message)) || `HTTP ${res.status}`);
    err.status = res.status;
    err.payload = data;
    throw err;
  }
  return data;
}

function digestInput(s) {
  // Small helper: hash arbitrary text client-side isn't needed; the server only
  // accepts 64-hex digests, so we derive via SubtleCrypto SHA-256.
  return crypto.subtle.digest('SHA-256', new TextEncoder().encode(s)).then((b) =>
    [...new Uint8Array(b)].map((x) => x.toString(16).padStart(2, '0')).join(''));
}

// ------------------------------------------------------------------ session --

function applyRole() {
  $('rolepill').textContent = state.role ? `角色：${state.role}` : '未认证';
  const isOp = state.role === 'operator';
  const isRev = state.role === 'reviewer';
  const isInsp = state.role === 'inspector';
  const isAdmin = state.role === 'admin';
  $('submitPanel').hidden = !isOp;
  $('casesPanel').hidden = !isRev;
  $('receiptsPanel').hidden = !isOp && !isRev && !isInsp && !isAdmin;
  $('objectsHint').hidden = !isInsp;
  $('objectsTable').style.display = isInsp ? 'none' : '';
  // Compression actions are operator-only; reviewers see read-only status;
  // inspectors get only the desensitised proof list.
  $('archiveOps').style.display = isOp ? '' : 'none';
  $('archivePanel').hidden = !isOp && !isRev && !isAdmin;
  $('proofsPanel').hidden = !isInsp;
  // Reliable subscriptions: admin manages, operator views status, inspector
  // reads only desensitised notification proofs.
  $('subsPanel').hidden = !isAdmin && !isOp && !isInsp;
  $('subsAdminOps').style.display = isAdmin ? '' : 'none';
  $('subsTitle').textContent = isInsp ? '脱敏通知证明' : '下游事件订阅';
  if (isInsp) $('detailPanel').hidden = true;
  refresh();
}

$('tokenSelect').addEventListener('change', (e) => {
  const t = e.target.value;
  if (!t) return;
  state.token = t;
  state.role = ROLE_FROM_TOKEN[t] || 'unknown';
  localStorage.setItem('converge-token', state.token);
  localStorage.setItem('converge-role', state.role);
  applyRole();
});
$('tokenInput').addEventListener('change', async (e) => {
  state.token = e.target.value.trim();
  state.role = '';
  try {
    // Ask the server for the exact role; admin and operator share read routes,
    // so inferring from permission probes mis-classifies an admin as operator.
    const me = await api('GET', '/api/whoami');
    state.role = me.role;
  } catch {
    state.role = '';
  }
  localStorage.setItem('converge-token', state.token);
  localStorage.setItem('converge-role', state.role);
  applyRole();
});
$('refreshBtn').addEventListener('click', refresh);

// ----------------------------------------------------------------- envelope --

$('genDigest').addEventListener('click', async () => {
  const seed = Math.random().toString(36) + Date.now();
  $('f_change').value = await digestInput(seed);
});
$('genReq').addEventListener('click', () => {
  $('f_req').value = 'req-' + Math.random().toString(36).slice(2, 12) + Date.now().toString(36);
});

function readEnvelope() {
  const preds = $('f_preds').value.split('\n').map((s) => s.trim()).filter(Boolean);
  return {
    object_key: $('f_object').value.trim(),
    site: $('f_site').value.trim(),
    site_seq: Number($('f_seq').value),
    coverage_region: $('f_region').value.trim(),
    change_digest: $('f_change').value.trim(),
    predecessor_digests: preds,
    request_key: $('f_req').value.trim(),
  };
}

$('submitBtn').addEventListener('click', async () => {
  const pre = $('submitResult');
  pre.hidden = false;
  try {
    const r = await api('POST', '/api/envelopes', readEnvelope());
    pre.textContent = JSON.stringify(r, null, 2);
    toast(r.status === 'suspended' ? '信封已挂起：等待前驱' : '上报完成：' + r.status, 'ok');
    loadObjects();
  } catch (e) {
    pre.textContent = JSON.stringify(e.payload || e.message, null, 2);
    toast(e.message, 'err');
  }
});
$('previewDigest').addEventListener('click', async () => {
  try {
    const r = await api('POST', '/api/envelopes/digest', readEnvelope());
    $('submitResult').hidden = false;
    $('submitResult').textContent = JSON.stringify(r, null, 2);
  } catch (e) { toast(e.message, 'err'); }
});

// ------------------------------------------------------------------ objects --

async function loadObjects() {
  if (state.role === 'inspector') return;
  const tbody = $('objectsTable').querySelector('tbody');
  try {
    const { objects } = await api('GET', '/api/objects');
    if (!objects.length) {
      tbody.innerHTML = '<tr><td colspan="4" class="muted">尚无对象</td></tr>';
      return;
    }
    tbody.innerHTML = objects.map((o) => `
      <tr class="clickable" data-key="${esc(o.object_key)}">
        <td class="mono">${esc(o.object_key)}</td>
        <td><span class="tag ${esc(o.status)}">${esc(o.status)}</span>${o.open_case ? `<div class="muted mono" style="margin-top:3px">${esc(short(o.open_case, 14))}</div>` : ''}</td>
        <td class="mono">${o.current_receipt ? esc(short(o.current_receipt, 14)) : '<span class="muted">—</span>'}</td>
        <td>${o.suspended.length ? o.suspended.map((s) => `<div class="warn mono">${esc(short(s.digest, 8))} ← 缺 ${s.missing.length}</div>`).join('') : '<span class="muted">—</span>'}</td>
      </tr>`).join('');
    tbody.querySelectorAll('tr.clickable').forEach((tr) =>
      tr.addEventListener('click', () => showObject(tr.dataset.key)));
    if (state.selectedObject) showObject(state.selectedObject);
  } catch (e) {
    tbody.innerHTML = `<tr><td colspan="4" class="err">${esc(e.message)}</td></tr>`;
  }
}

async function showObject(key) {
  state.selectedObject = key;
  const panel = $('detailPanel');
  panel.hidden = false;
  $('detailTitle').textContent = '对象 ' + key;
  const cpInput = $('cpObject');
  if (cpInput) cpInput.value = key;
  try {
    const d = await api('GET', '/api/objects/' + encodeURIComponent(key));
    drawGraph(d);
    $('suspInfo').innerHTML = d.suspended.length
      ? d.suspended.map((s) => `<div class="warn">挂起节点 ${esc(short(s.digest))}：缺失前驱 ${s.missing.map((m) => esc(short(m))).join(', ')}</div>`).join('')
      : '无挂起信封';
    $('detailBody').innerHTML = renderDetail(d);
  } catch (e) {
    $('detailBody').innerHTML = `<p class="err">${esc(e.message)}</p>`;
  }
}

function renderDetail(d) {
  const parts = [];
  if (d.current) {
    parts.push(`<div class="candidate"><b>当前决定</b> <span class="tag settled">${esc(d.current.decision_kind)}</span>
      <div class="mono muted">receipt #${d.current.seq} ${esc(short(d.current.hash))}</div>
      <pre>${esc(JSON.stringify(d.current.contributions, null, 2))}</pre></div>`);
  }
  if (d.open_case) {
    parts.push(`<div class="candidate"><b>开放冲突</b> <span class="tag conflict">${esc(d.open_case.case_id.slice(0, 14))}</span>
      <div>争议区段：${d.open_case.disputed_regions.map(esc).join(', ')}</div>
      <div class="muted">已录复核：${d.open_case.reviews.length}/2</div></div>`);
  }
  if (d.checkpoint) {
    parts.push(`<div class="candidate"><b>历史检查点</b> <span class="tag archived">归档 @#${d.checkpoint.boundary_seq}</span>
      <div class="mono muted">段 ${esc(short(d.checkpoint.segment_id, 20))} · ${d.checkpoint.verified ? '<span class="ok">已验真</span>' : '<span class="err">未通过</span>'}</div></div>`);
  }
  parts.push('<table><thead><tr><th>节点</th><th>类型</th><th>说明</th></tr></thead><tbody>' +
    d.nodes.map((n) => {
      const coldTag = n.archived ? ' <span class="tag archived">归档</span>' : '';
      const desc = n.kind === 'change'
        ? `site=<b>${esc(n.site)}</b> #${n.site_seq} 区段 ${esc(n.region)}<br><span class="muted">变更 ${esc(short(n.change_digest))}</span>${n.suspended ? '<br><span class="warn">挂起</span>' : ''}`
        : `${esc(n.decision_kind)}<br><span class="muted">${n.contributions.map((c) => esc(c.region)).join(', ')}</span>${n.reviewers ? '<br>复核：' + n.reviewers.map(esc).join(' / ') : ''}`;
      return `<tr><td class="mono">${esc(short(n.digest, 12))}${coldTag}</td><td>${n.kind}</td><td>${desc}</td></tr>`;
    }).join('') + '</tbody></table>');
  return parts.join('');
}

// Simple layered DAG layout: change/merge nodes in topological columns.
function drawGraph(d) {
  const svg = $('graphSvg');
  const W = svg.clientWidth || 640, H = 420;
  const byDigest = new Map(d.nodes.map((n) => [n.digest, n]));
  const depth = new Map();
  const computeDepth = (dig, visiting = new Set()) => {
    if (depth.has(dig)) return depth.get(dig);
    if (visiting.has(dig)) return 0;
    visiting.add(dig);
    const n = byDigest.get(dig);
    const parents = n ? (n.kind === 'change' ? n.predecessors : n.parents) : [];
    const liveParents = parents.filter((p) => byDigest.has(p));
    const dep = liveParents.length ? 1 + Math.max(...liveParents.map((p) => computeDepth(p, visiting))) : 0;
    depth.set(dig, dep);
    return dep;
  };
  d.nodes.forEach((n) => computeDepth(n.digest));

  const cols = new Map();
  for (const n of d.nodes) {
    const dep = depth.get(n.digest) || 0;
    if (!cols.has(dep)) cols.set(dep, []);
    cols.get(dep).push(n);
  }
  const maxCol = Math.max(1, cols.size);
  const colW = Math.max(150, (W - 80) / maxCol);
  const pos = new Map();
  [...cols.keys()].sort((a, b) => a - b).forEach((dep) => {
    const list = cols.get(dep);
    const rowH = Math.min(80, (H - 40) / Math.max(1, list.length));
    list.forEach((n, i) => pos.set(n.digest, { x: 30 + dep * colW, y: 20 + i * rowH, n }));
  });

  const NS = 'http://www.w3.org/2000/svg';
  svg.innerHTML = '';
  svg.setAttribute('viewBox', `0 0 ${W} ${H}`);

  // Edges first.
  for (const e of d.edges) {
    if (!e.live && !e.archived) continue;
    const a = pos.get(e.from), b = pos.get(e.to);
    if (!a || !b) continue;
    const path = document.createElementNS(NS, 'path');
    path.setAttribute('class', e.archived && !e.live ? 'edge archived-edge' : 'edge');
    path.setAttribute('d', `M ${a.x + 118} ${a.y + 22} C ${a.x + colW * 0.6} ${a.y + 22}, ${b.x + colW * 0.4} ${b.y + 22}, ${b.x} ${b.y + 22}`);
    svg.appendChild(path);
  }
  // Missing predecessor markers.
  for (const s of d.suspended) {
    const b = pos.get(s.digest);
    if (!b) continue;
    for (const m of s.missing) {
      const line = document.createElementNS(NS, 'path');
      line.setAttribute('class', 'edge missing');
      line.setAttribute('d', `M ${b.x - 40} ${b.y + 22} L ${b.x} ${b.y + 22}`);
      svg.appendChild(line);
      const t = document.createElementNS(NS, 'text');
      t.setAttribute('class', 'node-sub');
      t.setAttribute('x', b.x - 40); t.setAttribute('y', b.y + 12);
      t.textContent = '缺 ' + short(m, 6);
      svg.appendChild(t);
    }
  }
  // Nodes.
  for (const [dig, p] of pos) {
    const n = p.n;
    const g = document.createElementNS(NS, 'g');
    const r = document.createElementNS(NS, 'rect');
    r.setAttribute('class', `node-rect ${n.kind}${n.suspended ? ' susp' : ''}${n.archived ? ' archived' : ''}`);
    r.setAttribute('x', p.x); r.setAttribute('y', p.y); r.setAttribute('width', 124); r.setAttribute('height', 44); r.setAttribute('rx', 7);
    g.appendChild(r);
    const t1 = document.createElementNS(NS, 'text');
    t1.setAttribute('class', 'node-text');
    t1.setAttribute('x', p.x + 7); t1.setAttribute('y', p.y + 17);
    t1.textContent = (n.kind === 'change' ? '◦ ' : '◆ ') + short(dig, 9);
    g.appendChild(t1);
    const t2 = document.createElementNS(NS, 'text');
    t2.setAttribute('class', 'node-sub');
    t2.setAttribute('x', p.x + 7); t2.setAttribute('y', p.y + 34);
    t2.textContent = n.kind === 'change' ? `${n.site}#${n.site_seq} ${n.region}` : n.decision_kind;
    g.appendChild(t2);
    svg.appendChild(g);
  }
}

// -------------------------------------------------------------------- cases --

async function loadCases() {
  if (state.role !== 'reviewer') return;
  try {
    const { cases } = await api('GET', '/api/cases');
    if (!cases.length) { $('casesList').innerHTML = '<p class="muted">当前无待判定冲突。</p>'; return; }
    $('casesList').innerHTML = cases.map((c) => `
      <div class="candidate" data-case="${esc(c.case_id)}">
        <b>${esc(c.object_key)}</b> <span class="tag conflict">冲突</span>
        <div class="mono muted">case ${esc(short(c.case_id, 16))}</div>
        <div>争议区段：${c.disputed_regions.map(esc).join(', ')}</div>
        <div class="muted">已录复核 ${2 - c.awaiting}/2</div>
        <div class="choices"></div>
      </div>`).join('');
    document.querySelectorAll('#casesList .candidate').forEach(async (el) => {
      const id = el.dataset.case;
      const full = await api('GET', '/api/cases/' + encodeURIComponent(id));
      const box = el.querySelector('.choices');
      box.innerHTML = full.disputed_regions.map((region) => `
        <label>区段 <b>${esc(region)}</b>
          <select data-region="${esc(region)}">
            <option value="">— 选择保留的变更摘要 —</option>
            ${[...new Set(full.candidates.filter((c) => c.region === region).map((c) => c.change_digest))]
              .map((dig) => `<option value="${esc(dig)}">${esc(short(dig, 20))}</option>`).join('')}
          </select>
        </label>`).join('') +
        (full.reviews.length ? `<div class="muted">一审（${esc(full.reviews[0].reviewer_name)}）：${esc(JSON.stringify(full.reviews[0].choices))}</div>` : '') +
        '<button class="ghost" style="margin-top:8px">提交复核</button>';
      box.querySelector('button').addEventListener('click', async () => {
        const choices = {};
        box.querySelectorAll('select').forEach((s) => { if (s.value) choices[s.dataset.region] = s.value; });
        try {
          const r = await api('POST', '/api/reviews/' + encodeURIComponent(id), { choices });
          toast('复核结果：' + r.status, r.status === 'adjudicated' ? 'ok' : '');
          loadCases(); loadObjects();
        } catch (e) {
          toast(e.message + (e.payload?.error?.details ? ' ' + JSON.stringify(e.payload.error.details) : ''), 'err');
        }
      });
    });
  } catch (e) {
    $('casesList').innerHTML = `<p class="err">${esc(e.message)}</p>`;
  }
}

// ----------------------------------------------------------------- receipts --

async function loadReceipts() {
  const key = $('receiptFilter').value.trim();
  try {
    const { receipts } = await api('GET', '/api/receipts' + (key ? '?object_key=' + encodeURIComponent(key) : ''));
    $('receiptsList').innerHTML = receipts.length ? `
      <table><thead><tr><th>#</th><th>对象</th><th>类型</th><th>回执链哈希</th></tr></thead><tbody>
      ${receipts.map((r) => `<tr class="clickable" data-h="${esc(r.hash)}">
        <td>${r.seq}</td><td class="mono">${esc(r.object_key)}</td><td>${esc(r.decision_kind)}</td>
        <td class="mono">${esc(short(r.hash, 18))}${r.prev_receipt ? '<div class="muted">↑ ' + esc(short(r.prev_receipt, 14)) + '</div>' : '<div class="muted">链首</div>'}</td>
      </tr>`).join('')}
      </tbody></table>
      ${key ? `<div style="margin-top:8px"><a href="/api/objects/${encodeURIComponent(key)}/receipts/download" id="dlLink">下载该对象脱敏收据链（JSON）</a></div>` : ''}
    ` : '<p class="muted">无收据。</p>';
    document.querySelectorAll('#receiptsList tr.clickable').forEach((tr) =>
      tr.addEventListener('click', async () => {
        const r = await api('GET', '/api/receipts/' + encodeURIComponent(tr.dataset.h));
        const pre = $('receiptVerify');
        pre.hidden = false;
        pre.textContent = JSON.stringify(r, null, 2);
      }));
    const dl = $('dlLink');
    if (dl) dl.addEventListener('click', (e) => {
      // Ensure the download is authenticated.
      e.preventDefault();
      fetch(dl.href, { headers: { Authorization: `Bearer ${state.token}` } })
        .then((x) => x.blob()).then((blob) => {
          const a = document.createElement('a');
          a.href = URL.createObjectURL(blob);
          a.download = `receipts-${key}.json`;
          a.click();
        });
    });
  } catch (e) {
    $('receiptsList').innerHTML = `<p class="err">${esc(e.message)}</p>`;
  }
}
$('loadReceipts').addEventListener('click', loadReceipts);
$('verifyModeBtn').addEventListener('click', () => {
  const pre = $('receiptVerify');
  pre.hidden = false;
  pre.contentEditable = 'true';
  pre.textContent = pre.textContent || '// 粘贴一条收据 JSON，然后点“校验”';
  const btn = document.createElement('button');
  btn.textContent = '校验此收据';
  btn.className = 'ghost';
  btn.style.marginTop = '6px';
  btn.onclick = async () => {
    try {
      const receipt = JSON.parse(pre.textContent.replace(/^\/\/.*$/gm, ''));
      const r = await api('POST', '/api/receipts/verify', { receipt });
      toast('校验通过：' + JSON.stringify(r), 'ok');
    } catch (e) { toast('校验失败：' + e.message, 'err'); }
  };
  if (pre.dataset.verifyBtn === '1') return;
  pre.dataset.verifyBtn = '1';
  pre.after(btn);
});

// ------------------------------------------------------------------- archive --

function fmtBytes(n) {
  if (n < 1024) return `${n} B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KiB`;
  return `${(n / 1024 / 1024).toFixed(2)} MiB`;
}

async function loadArchiveStatus() {
  if (state.role !== 'operator' && state.role !== 'reviewer') return;
  try {
    const s = await api('GET', '/api/archive/status');
    const segs = (s.cold.segments || []).map((x) =>
      `<div class="mono ${x.verified ? 'ok' : 'err'}">${x.verified ? '✓' : '✗'} 段 ${esc(short(x.segment_id, 14))}` +
      ` · ${x.object_key ? esc(x.object_key) : '?'} @#${x.boundary_receipt_seq ?? '?'} · ${fmtBytes(x.bytes)}` +
      `${x.error ? ` <span class="err">(${esc(x.error)})</span>` : ''}</div>`).join('');
    const cps = (s.checkpoints || []).map((c) =>
      `<div class="mono">检查点 <b>${esc(c.object_key)}</b> @#${c.boundary_seq} · 段 ${esc(short(c.segment_id, 12))}` +
      ` · 归档节点 ${c.archived_nodes} · ${c.verified ? '<span class="ok">已验真</span>' : '<span class="err">未通过</span>'}</div>`).join('');
    const prog = (s.restore.objects || []).map((p) =>
      `<div class="mono muted">恢复 ${esc(p.object_key)}：已物化 ${p.materialized_nodes}/${p.archived_nodes}` +
      `${p.pending_nodes ? ` · 待恢复 ${p.pending_nodes}` : ''}</div>`).join('');
    const blk = (s.blockers || []).length
      ? `<div class="err" style="margin-top:6px">阻塞原因：${s.blockers.map((b) => esc(b.kind + ' ' + (b.object_key || b.segment_id || '') + ' ' + JSON.stringify(b.reasons || b.reason || b.phase || ''))).join('；')}</div>`
      : '<div class="ok" style="margin-top:6px">无阻塞</div>';
    $('archiveStatus').innerHTML =
      `<div class="row"><div>热存储 WAL：<b>${fmtBytes(s.hot.wal_bytes)}</b><br><span class="muted">热节点 ${s.hot.nodes} · 热收据 ${s.hot.receipts}</span></div>` +
      `<div>冷归档：<b>${fmtBytes(s.cold.archive_bytes)}</b><br><span class="muted">段 ${s.cold.segment_count} · 保留策略 ≥${s.policy.retain_decisions}</span></div></div>` +
      `<div style="margin-top:8px">${cps || '<span class="muted">尚无检查点</span>'}</div>` +
      `<div style="margin-top:6px">${segs || ''}</div>` +
      `<div style="margin-top:6px">${prog}</div>` +
      (s.restore.recent && s.restore.recent.length ? `<div class="muted mono" style="margin-top:6px">最近恢复：${s.restore.recent.slice(0, 3).map((r) => `${esc(r.reason)} ${r.object_keys.join(',')}`).join('；')}</div>` : '') +
      blk;
  } catch (e) {
    $('archiveStatus').innerHTML = `<span class="err">${esc(e.message)}</span>`;
  }
}

$('cpBtn').addEventListener('click', async () => {
  const object_key = $('cpObject').value.trim() || state.selectedObject;
  if (!object_key) { toast('请先选择或填写对象键', 'err'); return; }
  try {
    const r = await api('POST', '/api/archive/checkpoints', { object_key, task_key: 'cp-' + Math.random().toString(36).slice(2, 10) });
    toast(r.idempotent_replay ? '检查点已存在（幂等返回）' : `检查点已创建 @#${r.boundary_seq}，回收 ${fmtBytes(r.reclaimed_bytes)}`, 'ok');
    loadArchiveStatus(); loadObjects();
  } catch (e) { toast(e.message + (e.payload?.error?.details ? ' ' + JSON.stringify(e.payload.error.details) : ''), 'err'); }
});

$('restoreBtn').addEventListener('click', async () => {
  const object_key = $('cpObject').value.trim() || state.selectedObject;
  if (!object_key) { toast('请先选择或填写对象键', 'err'); return; }
  try {
    const r = await api('POST', '/api/archive/restore', { object_key });
    toast(`恢复：${r.status}（物化 ${r.restored_nodes} 个节点）`, 'ok');
    loadArchiveStatus(); loadObjects();
    if (state.selectedObject === object_key) showObject(object_key);
  } catch (e) { toast(e.message + (e.payload?.error?.details ? ' ' + JSON.stringify(e.payload.error.details) : ''), 'err'); }
});

async function loadProofs() {
  if (state.role !== 'inspector') return;
  try {
    const { proofs } = await api('GET', '/api/archive/proofs');
    $('proofsList').innerHTML = proofs.length ? proofs.map((p) =>
      `<div class="candidate">
        <b>${esc(p.object_key || '未知对象')}</b> @#${p.boundary_receipt_seq ?? '?'}
        <span class="tag ${p.verified ? 'settled' : 'conflict'}">${p.verified ? '已验真' : '未通过'}</span>
        <div class="mono muted">段 ${esc(short(p.segment_id, 18))}</div>
        <div class="muted">${p.frame_count} 帧 · ${p.envelope_count} 信封索引 · 创建于 ${p.created_at ? esc(p.created_at) : '—'}</div>
        ${p.error ? `<div class="err">${esc(p.error)}</div>` : ''}
        <div style="margin-top:6px"><a href="/api/archive/segments/${encodeURIComponent(p.segment_id)}/pack" class="proofDl" data-id="${esc(p.segment_id)}">下载脱敏证明包（JSON）</a></div>
      </div>`).join('') : '<p class="muted">尚无归档段。</p>';
    document.querySelectorAll('.proofDl').forEach((a) => a.addEventListener('click', (e) => {
      e.preventDefault();
      fetch(a.href, { headers: { Authorization: `Bearer ${state.token}` } })
        .then(async (x) => {
          if (!x.ok) throw new Error('HTTP ' + x.status + ' ' + (await x.text()));
          return x.blob();
        }).then((blob) => {
          const u = URL.createObjectURL(blob);
          const link = document.createElement('a');
          link.href = u; link.download = `archive-proof-${a.dataset.id.slice(0, 12)}.json`;
          link.click();
        }).catch((err) => toast('证明包下载失败：' + err.message, 'err'));
    }));
  } catch (e) {
    $('proofsList').innerHTML = `<span class="err">${esc(e.message)}</span>`;
  }
}

// ---------------------------------------------------------- subscriptions ----

function kindTag(k) {
  const map = { 'decision.created': '对象新决定', 'case.closed': '冲突案闭合', 'cold_segment.generated': '冷段生成' };
  return map[k] || k;
}

async function loadSubscriptions() {
  if (state.role !== 'admin' && state.role !== 'operator') return;
  try {
    const { subscriptions } = await api('GET', '/api/subscriptions');
    if (!subscriptions.length) {
      $('subsList').innerHTML = '<p class="muted">尚无下游订阅。</p>';
      return;
    }
    $('subsList').innerHTML = subscriptions.map((s) => {
      const statusTag = s.status === 'active' ? '<span class="tag settled">active</span>'
        : s.status === 'paused' ? '<span class="tag suspended">paused</span>'
        : `<span class="tag pending">${esc(s.status)}</span>`;
      const isAdmin = state.role === 'admin';
      const deadRows = (s.dead_letter || []).map((d) => `
        <div class="err mono" style="margin-top:4px">
          死信 #${d.event_seq} ${esc(short(d.event_id, 14))} · 尝试 ${d.attempts} · ${esc(d.last_error || '')}
          ${isAdmin ? `<button class="ghost sub-redeliver" data-id="${esc(s.id)}" data-seq="${d.event_seq}" style="margin-left:8px; padding:2px 8px;">再次发送</button>` : ''}
        </div>`).join('');
      return `<div class="candidate" data-sub="${esc(s.id)}">
        <div><b class="mono">${esc(short(s.id, 18))}</b> ${statusTag}
          <span class="muted">密钥 v${s.current_key_version}</span></div>
        <div class="mono muted" style="word-break:break-all">${esc(s.callback_url)}</div>
        <div class="muted">关注：${s.event_kinds.map(kindTag).join('、')}</div>
        <div style="margin-top:4px">
          待发送 <b>${s.pending_count}</b> · 在途 ${s.in_flight_count} · 死信 <b class="${s.dead_count ? 'err' : ''}">${s.dead_count}</b>
          · 最近确认游标 <b class="mono">#${s.confirmed_event_seq}</b>
        </div>
        ${s.last_acked ? `<div class="muted mono">最近确认：${esc(short(s.last_acked.event_id, 14))} (${esc(s.last_acked.kind)}) @ ${esc(s.last_acked.confirmed_at)}</div>` : ''}
        ${deadRows}
        ${isAdmin ? `<div style="margin-top:8px; display:flex; gap:6px;">
          <button class="ghost sub-pause" data-id="${esc(s.id)}" type="button">暂停</button>
          <button class="ghost sub-resume" data-id="${esc(s.id)}" type="button">恢复</button>
          <button class="ghost sub-rotate" data-id="${esc(s.id)}" type="button">更换签名密钥</button>
        </div>` : ''}
      </div>`;
    }).join('');

    if (state.role === 'admin') {
      document.querySelectorAll('.sub-pause').forEach((b) => b.onclick = async () => {
        try { await api('POST', `/api/subscriptions/${b.dataset.id}/pause`, {}); toast('已暂停', 'ok'); loadSubscriptions(); }
        catch (e) { toast(e.message, 'err'); }
      });
      document.querySelectorAll('.sub-resume').forEach((b) => b.onclick = async () => {
        try { await api('POST', `/api/subscriptions/${b.dataset.id}/resume`, {}); toast('已恢复', 'ok'); loadSubscriptions(); }
        catch (e) { toast(e.message, 'err'); }
      });
      document.querySelectorAll('.sub-rotate').forEach((b) => b.onclick = async () => {
        const key = (crypto.randomUUID?.() || ('k' + Math.random().toString(36).slice(2) + Date.now().toString(36))) +
          '-' + Math.random().toString(36).slice(2, 12);
        if (!confirm('为该订阅生成并更换为新的签名密钥版本？\n（在途批次继续使用旧版本）')) return;
        try { await api('POST', `/api/subscriptions/${b.dataset.id}/rotate-key`, { signing_key: key }); toast('签名密钥已更换为新版本', 'ok'); loadSubscriptions(); }
        catch (e) { toast(e.message, 'err'); }
      });
      document.querySelectorAll('.sub-redeliver').forEach((b) => b.onclick = async () => {
        try {
          await api('POST', `/api/subscriptions/${b.dataset.id}/redeliver`, { event_seq: Number(b.dataset.seq) });
          toast('已重新入队（原事件标识与顺序不变）', 'ok'); loadSubscriptions();
        } catch (e) { toast(e.message, 'err'); }
      });
    }
  } catch (e) {
    $('subsList').innerHTML = `<span class="err">${esc(e.message)}</span>`;
  }
}

async function loadSubProofs() {
  if (state.role !== 'inspector') return;
  try {
    const { subscriptions } = await api('GET', '/api/subscriptions/proofs');
    if (!subscriptions.length) { $('subsList').innerHTML = '<p class="muted">尚无订阅通知证明。</p>'; return; }
    $('subsList').innerHTML = subscriptions.map((s) => `
      <div class="candidate">
        <b class="mono">${esc(short(s.id, 18))}</b>
        <span class="tag ${s.status === 'active' ? 'settled' : 'suspended'}">${esc(s.status)}</span>
        <div class="muted">关注：${s.event_kinds.map(kindTag).join('、')}</div>
        <div style="margin-top:4px">待发送 ${s.pending_count} · 在途 ${s.in_flight_count} · 死信 ${s.dead_count}
          · 最近确认游标 <b class="mono">#${s.confirmed_event_seq}</b></div>
        ${s.last_acked ? `<div class="muted mono">最近确认事件 ${esc(short(s.last_acked.event_id, 14))} (${esc(s.last_acked.kind)}) @ ${esc(s.last_acked.confirmed_at)}</div>` : ''}
        <div style="margin-top:6px"><a href="/api/subscriptions/${encodeURIComponent(s.id)}/proofs" class="sub-proof" data-id="${esc(s.id)}">查看脱敏通知证明（JSON）</a></div>
      </div>`).join('');
    document.querySelectorAll('.sub-proof').forEach((a) => a.addEventListener('click', (e) => {
      e.preventDefault();
      fetch(a.href, { headers: { Authorization: `Bearer ${state.token}` } })
        .then(async (x) => { if (!x.ok) throw new Error('HTTP ' + x.status + ' ' + await x.text()); return x.text(); })
        .then((txt) => {
          const pre = document.createElement('pre');
          pre.textContent = txt;
          const w = window.open('', '_blank');
          if (w) { w.document.body.style.background = '#0f1420'; w.document.body.style.color = '#e6ebf5'; w.document.body.appendChild(pre); }
        }).catch((err) => toast('证明读取失败：' + err.message, 'err'));
    }));
  } catch (e) {
    $('subsList').innerHTML = `<span class="err">${esc(e.message)}</span>`;
  }
}

$('genKey')?.addEventListener('click', () => {
  $('subKey').value = (crypto.randomUUID?.() || ('k' + Math.random().toString(36).slice(2))) +
    '-' + Math.random().toString(36).slice(2, 14);
});
$('subCreateBtn')?.addEventListener('click', async () => {
  const url = $('subUrl').value.trim();
  const key = $('subKey').value.trim();
  const event_kinds = [...document.querySelectorAll('#subKinds input:checked')].map((c) => c.value);
  if (!url) return toast('请填写回调地址', 'err');
  if (!key || key.length < 16) return toast('签名密钥至少 16 字符', 'err');
  if (!event_kinds.length) return toast('请至少选择一种关注事件', 'err');
  try {
    const r = await api('POST', '/api/subscriptions', { callback_url: url, event_kinds, signing_key: key });
    toast('订阅已登记：' + r.id, 'ok');
    $('subUrl').value = ''; $('subKey').value = '';
    loadSubscriptions();
  } catch (e) { toast(e.message + (e.payload?.error?.details ? ' ' + JSON.stringify(e.payload.error.details) : ''), 'err'); }
});
$('subPumpBtn')?.addEventListener('click', async () => {
  try {
    const r = await api('POST', '/api/subscriptions/pump', {});
    const rs = r.results || [];
    const acked = rs.reduce((a, x) => a + (x.acked || 0), 0);
    toast(`投递周期完成：确认 ${acked} 条`, 'ok');
    loadSubscriptions();
  } catch (e) { toast(e.message, 'err'); }
});

// ------------------------------------------------------------------- boot ---

function refresh() {
  if (!state.token) return;
  loadObjects();
  loadCases();
  loadReceipts();
  loadArchiveStatus();
  loadProofs();
  loadSubscriptions();
  loadSubProofs();
}
if (state.token) {
  $('tokenSelect').value = state.token.startsWith('reviewer') || ROLE_FROM_TOKEN[state.token] ? state.token : '';
  applyRole();
}
