/* diffdiag console. All business state is re-read from the server on load,
 * on the 5s poll and on every manual refresh; the page keeps no success
 * state of its own. Server strings are rendered via textContent only, and
 * error panels show code + message — never request payloads. */
'use strict';

const $ = (id) => document.getElementById(id);
const state = {
  token: sessionStorage.getItem('diffdiag.token') || '',
  role: null,
  selectedTask: null,
};

function showError(err) {
  const box = $('error-box');
  box.textContent = err && err.code ? `${err.code}: ${err.message}` : String(err && err.message ? err.message : err);
  box.classList.remove('hidden');
}
function clearError() {
  $('error-box').classList.add('hidden');
  $('error-box').textContent = '';
}

async function api(method, path, body) {
  const res = await fetch(path, {
    method,
    headers: {
      ...(state.token ? { Authorization: 'Bearer ' + state.token } : {}),
      ...(body !== undefined ? { 'Content-Type': 'application/json' } : {}),
    },
    body: body !== undefined ? JSON.stringify(body) : undefined,
    cache: 'no-store',
  });
  const text = await res.text();
  let json = null;
  try { json = text ? JSON.parse(text) : null; } catch { json = null; }
  if (res.status === 401) { logout(); throw Object.assign(new Error('未认证'), { code: 'UNAUTHENTICATED' }); }
  if (!res.ok) {
    const e = new Error((json && json.error && json.error.message) || res.statusText);
    e.code = (json && json.error && json.error.code) || res.status;
    throw e;
  }
  return json;
}

function el(tag, text) {
  const e = document.createElement(tag);
  if (text !== undefined) e.textContent = text;
  return e;
}
function short(h) { return h ? h.slice(0, 16) + '…' : '—'; }
function rowCells(table, cells) {
  const tr = el('tr');
  for (const c of cells) {
    const td = el('td');
    if (typeof c === 'string') td.textContent = c;
    else td.appendChild(c);
    tr.appendChild(td);
  }
  table.appendChild(tr);
}

function logout() {
  sessionStorage.removeItem('diffdiag.token');
  state.token = '';
  state.role = null;
  render();
}

async function refresh() {
  if (!state.token) { render(); return; }
  try {
    const me = await api('GET', '/api/whoami');
    state.role = me.role;
    render();
    if (me.role === 'operator') await loadOperator();
    if (me.role === 'analyst') await loadAnalyst();
    if (me.role === 'observer') await loadObserver();
    clearError();
  } catch (e) {
    if (e.code !== 'UNAUTHENTICATED') showError(e);
  }
}

function render() {
  $('login').classList.toggle('hidden', !!state.role);
  for (const r of ['operator', 'analyst', 'observer']) {
    $(r).classList.toggle('hidden', state.role !== r);
  }
  $('who').textContent = state.role ? `角色：${state.role}` : '';
  $('refresh').classList.toggle('hidden', !state.role);
  $('logout').classList.toggle('hidden', !state.role);
}

/* ---------------------------------------------------------- operator -- */
async function loadOperator() {
  const [versions, tasks, objects] = await Promise.all([
    api('GET', '/api/algo-versions'),
    api('GET', '/api/comparisons'),
    api('GET', '/api/objects'),
  ]);
  const vt = $('algo-table').querySelector('tbody');
  vt.textContent = '';
  for (const v of versions) {
    const btn = el('button', v.status === 'active' ? '当前版本' : '切换到此版本');
    btn.disabled = v.status === 'active';
    btn.onclick = async () => {
      try { await api('POST', '/api/algo-versions/activate', { version: v.version }); await refresh(); }
      catch (e) { showError(e); }
    };
    rowCells(vt, [v.version, v.status, v.description || '', btn]);
  }
  renderTaskTable($('op-task-table').querySelector('tbody'), tasks, true);
  const ot = $('op-object-table').querySelector('tbody');
  ot.textContent = '';
  for (const o of objects) {
    rowCells(ot, [o.group, o.object_key, String(o.receipts), String(o.head_seq), String(o.watermark)]);
  }
}

function renderTaskTable(tbody, tasks, withCancel) {
  tbody.textContent = '';
  for (const t of tasks) {
    const cells = [
      t.task_key, t.state, t.stage || '—', t.algo_version,
      `${t.left_watermark}/${t.right_watermark}`, short(t.proof_digest),
    ];
    if (withCancel) {
      const btn = el('button', '取消');
      btn.disabled = t.state !== 'queued';
      btn.onclick = async () => {
        try { await api('POST', `/api/comparisons/${encodeURIComponent(t.task_key)}/cancel`); await refresh(); }
        catch (e) { showError(e); }
      };
      cells.push(btn);
    } else {
      const btn = el('button', '查看');
      btn.disabled = t.state !== 'done';
      btn.onclick = () => loadDiagnostic(t.task_key);
      cells.push(btn);
    }
    rowCells(tbody, cells);
  }
}

/* ----------------------------------------------------------- analyst -- */
async function loadAnalyst() {
  const [objects, tasks] = await Promise.all([
    api('GET', '/api/objects'),
    api('GET', '/api/comparisons'),
  ]);
  const groups = [...new Set(objects.map((o) => o.group))].sort();
  for (const id of ['left-group', 'right-group']) {
    const sel = $(id);
    const cur = sel.value;
    sel.textContent = '';
    for (const g of groups) {
      const opt = el('option', g);
      opt.value = g;
      sel.appendChild(opt);
    }
    if (groups.includes(cur)) sel.value = cur;
  }
  renderTaskTable($('an-task-table').querySelector('tbody'), tasks, false);
  if (state.selectedTask) {
    const t = tasks.find((x) => x.task_key === state.selectedTask);
    if (t && t.state === 'done') await loadDiagnostic(t.task_key);
  }
}

async function loadDiagnostic(taskKey) {
  state.selectedTask = taskKey;
  try {
    const d = await api('GET', `/api/comparisons/${encodeURIComponent(taskKey)}/diagnostic`);
    $('diag-task').textContent = `（${d.task_key}）`;
    $('diag-empty').classList.add('hidden');
    $('diag-body').classList.remove('hidden');
    $('diag-meta').textContent =
      `算法 ${d.algo_version} · 水位 ${d.left_watermark}/${d.right_watermark} · 证明 ${short(d.proof_digest)} · 证据链头 ${short(d.explain_head)}`;
    renderSkeleton($('skel-left'), d.skeleton_left, d.affected, 'left');
    renderSkeleton($('skel-right'), d.skeleton_right, d.affected, 'right');
    $('diag-divergence').textContent = d.divergence
      ? JSON.stringify(d.divergence, null, 2)
      : '两组一致（无分歧）';
    const af = $('diag-affected');
    af.textContent = '';
    for (const a of d.affected) {
      af.appendChild(el('li', `[${a.side}] ${short(a.node_ref)}${a.seq !== undefined ? ' seq=' + a.seq : ''}${a.depth !== undefined ? ' depth=' + a.depth : ''}`));
    }
    if (!d.affected.length) af.appendChild(el('li', '（无）'));
    const ev = $('diag-evidence');
    ev.textContent = '';
    for (const n of d.explanations) {
      ev.appendChild(el('li', `#${n.ordinal} ${n.kind} · ${short(n.digest)} ← ${short(n.prev)}`));
    }
  } catch (e) {
    showError(e);
  }
}

function renderSkeleton(ul, skel, affected, side) {
  ul.textContent = '';
  if (!skel) return;
  const mark = new Set(affected.filter((a) => a.side === side).map((a) => a.node_ref));
  for (const digest of skel.topo) {
    const li = el('li', short(digest));
    if (mark.has(digest)) li.className = 'affected';
    ul.appendChild(li);
  }
}

/* ---------------------------------------------------------- observer -- */
async function loadObserver() {
  const proofs = await api('GET', '/api/proofs');
  const pt = $('proof-table').querySelector('tbody');
  pt.textContent = '';
  for (const p of proofs) {
    rowCells(pt, [p.task_key, p.state, p.algo_version, `${p.left_watermark}/${p.right_watermark}`, short(p.proof_digest)]);
  }
}

/* ------------------------------------------------------------- wiring -- */
$('login-btn').onclick = () => {
  state.token = $('token').value.trim();
  sessionStorage.setItem('diffdiag.token', state.token);
  refresh();
};
$('logout').onclick = logout;
$('refresh').onclick = refresh;
$('worker-run').onclick = async () => {
  try { await api('POST', '/api/worker/run'); await refresh(); } catch (e) { showError(e); }
};
$('algo-register').onclick = async () => {
  try {
    await api('POST', '/api/algo-versions', { version: $('algo-version').value.trim(), description: $('algo-desc').value });
    await refresh();
  } catch (e) { showError(e); }
};
$('package-submit').onclick = async () => {
  try {
    const body = JSON.parse($('package-json').value);
    const r = await api('POST', '/api/packages', body);
    $('package-result').textContent = `accepted=${r.accepted} duplicates=${r.duplicates} watermark=${r.watermark}`;
    await refresh();
  } catch (e) { showError(e); }
};
$('compare-create').onclick = async () => {
  try {
    await api('POST', '/api/comparisons', {
      task_key: $('task-key').value.trim(),
      left_group: $('left-group').value,
      right_group: $('right-group').value,
    });
    await refresh();
  } catch (e) { showError(e); }
};
$('verify-btn').onclick = async () => {
  try {
    const r = await api('POST', '/api/proofs/verify', { task_key: $('verify-key').value.trim() });
    $('verify-result').textContent = JSON.stringify(r, null, 2);
  } catch (e) { showError(e); }
};

document.addEventListener('visibilitychange', () => { if (!document.hidden) refresh(); });
setInterval(refresh, 5000);
render();
if (state.token) refresh();
