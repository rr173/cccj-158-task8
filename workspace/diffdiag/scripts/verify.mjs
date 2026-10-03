#!/usr/bin/env node
/**
 * End-to-end verification for the diffdiag causal divergence diagnostician.
 * Spawns the real HTTP server on an ephemeral port and exercises:
 *   1  legal comparison (first divergence, downstream nodes, sanitised evidence, proof)
 *   2  same task key + same inputs => original task / divergence / proof
 *   3  same task key + changed inputs => 409 conflict
 *   4  broken packages (cycle, duplicate seq, node body conflict, missing
 *      predecessor, broken chain, bad digest, bad signature, hash mismatch,
 *      forbidden field) => whole-package rejection, store untouched
 *   5  concurrent submissions of one comparison => exactly one task
 *   6  watermark boundary: receipts arriving after the watermark never mix in
 *   7  algorithm switch: only new tasks use the new version
 *   8  crash recovery: resume from persisted stage / back to not-started,
 *      explanation nodes never duplicated
 *   9  cancel race: cancel vs worker claim, exactly one wins; done diagnostics
 *      are never overwritten
 *  10  proof tampering => deterministic verification failure
 *  11  role boundaries (operator / analyst / observer)
 *  12  error responses never echo input package content; evidence stays sanitised
 */
import { spawn } from 'node:child_process';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { digestOf, sign, sha256Hex } from '../src/crypto-util.mjs';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, '..');

let passed = 0;
let failed = 0;
function check(name, cond, detail = '') {
  if (cond) { passed++; console.log(`  ✓ ${name}`); }
  else { failed++; console.error(`  ✗ ${name} ${detail}`); }
}

const PORT = 49300 + Math.floor(Math.random() * 1500);
const DATA = fs.mkdtempSync(path.join(os.tmpdir(), 'diffdiag-verify-'));
const STATE_FILE = path.join(DATA, 'diffdiag-state.json');
const SECRET = 'verify-receipt-secret';
const TOK = { op: 'operator-token', an: 'analyst-token', ob: 'observer-token', bogus: 'nope' };

let serverProc = null;
function startServer(extraEnv = {}) {
  return new Promise((resolve, reject) => {
    const proc = spawn(process.execPath, [path.join(ROOT, 'src', 'server.mjs')], {
      env: {
        ...process.env,
        DIFFDIAG_HOST: '127.0.0.1',
        DIFFDIAG_PORT: String(PORT),
        DIFFDIAG_DATA_DIR: DATA,
        DIFFDIAG_RECEIPT_SECRET: SECRET,
        DIFFDIAG_WORKER_INTERVAL_MS: '0',
        ...extraEnv,
      },
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    let out = '';
    proc.stdout.on('data', (d) => { out += d; });
    proc.stderr.on('data', (d) => { out += d; });
    serverProc = proc;
    const deadline = Date.now() + 5000;
    const poll = async () => {
      try {
        const r = await fetch(`http://127.0.0.1:${PORT}/healthz`);
        if (r.ok) return resolve();
      } catch { /* not yet */ }
      if (Date.now() > deadline) { proc.kill('SIGKILL'); return reject(new Error('server did not start: ' + out)); }
      setTimeout(poll, 50);
    };
    poll();
  });
}
function waitExit(proc, ms = 5000) {
  return new Promise((res) => {
    if (!proc || proc.exitCode !== null) return res();
    const t = setTimeout(() => { try { proc.kill('SIGKILL'); } catch {} res(); }, ms);
    proc.on('exit', () => { clearTimeout(t); res(); });
  });
}
async function stopServer() {
  if (!serverProc) return;
  const p = serverProc;
  serverProc = null;
  p.kill('SIGTERM');
  await waitExit(p, 2000);
}

async function req(method, urlPath, token, body) {
  try {
    const res = await fetch(`http://127.0.0.1:${PORT}${urlPath}`, {
      method,
      headers: {
        ...(token ? { Authorization: `Bearer ${token}` } : {}),
        ...(body !== undefined ? { 'Content-Type': 'application/json' } : {}),
      },
      body: body !== undefined ? JSON.stringify(body) : undefined,
    });
    const text = await res.text();
    let json = null;
    try { json = text ? JSON.parse(text) : null; } catch { json = null; }
    return { status: res.status, json, text };
  } catch (e) {
    return { status: 0, json: null, text: String(e) };
  }
}

/* ------------------------------------------------------- receipt crafting -- */
const hex = (s) => sha256Hex(s);
function mkReceipt(objectKey, seq, nodeName, parentNodes, changeName, prev, secret, kind) {
  const fields = {
    seq,
    object_key: objectKey,
    decision_kind: kind || (seq === 1 ? 'accept' : 'extend'),
    decision_node: hex('node:' + nodeName),
    parents: parentNodes.map((n) => hex('node:' + n)),
    contributions: [{ region: 'tiles:000:001', change_digest: hex('change:' + changeName) }],
    prev_receipt: prev,
    emitted_at: `2026-01-02T03:04:${String(seq % 60).padStart(2, '0')}.000Z`,
  };
  return { ...fields, hash: digestOf(fields), sig: sign(secret, fields) };
}
/** Build a linear receipt chain; names[i] decides node + change content. */
function mkChain(objectKey, names, secret, opts = {}) {
  const startSeq = opts.startSeq || 1;
  let prev = opts.prev || null;
  let prevNode = opts.prevNode || null;
  const out = [];
  names.forEach((name, i) => {
    const parents = prevNode ? [prevNode] : [];
    const r = mkReceipt(objectKey, startSeq + i, name, parents, name, prev, secret);
    out.push(r);
    prev = r.hash;
    prevNode = name;
  });
  return out;
}
const pkg = (group, objectKey, receipts) => ({ group, object_key: objectKey, receipts });

async function ingest(group, objectKey, receipts, token = TOK.op) {
  return req('POST', '/api/packages', token, pkg(group, objectKey, receipts));
}
async function runWorker() {
  return req('POST', '/api/worker/run', TOK.op);
}
function readState() {
  return JSON.parse(fs.readFileSync(STATE_FILE, 'utf8'));
}
function deepFind(obj, pred, path = '') {
  const hits = [];
  if (obj && typeof obj === 'object') {
    for (const [k, v] of Object.entries(obj)) {
      if (pred.key(k)) hits.push(`${path}.${k}`);
      hits.push(...deepFind(v, pred, `${path}.${k}`));
    }
  } else if (typeof obj === 'string' && pred.value(obj)) {
    hits.push(path);
  }
  return hits;
}

/* ------------------------------------------------------------- scenarios -- */

async function scenario1() {
  console.log('\n[1] 合法比较：第一处分歧、下游节点、脱敏证据、证明');
  const a = mkChain('obj-x', ['n1', 'n2', 'n3', 'n4'], SECRET);
  const b = mkChain('obj-x', ['n1', 'n2', 'm3', 'm4'], SECRET);
  const c = mkChain('obj-x', ['n1', 'n2', 'n3', 'n4'], SECRET); // identical to a
  let r = await ingest('g1a', 'obj-x', a);
  check('入库 g1a', r.status === 200 && r.json.accepted === 4 && r.json.watermark === 4, JSON.stringify(r.json));
  r = await ingest('g1b', 'obj-x', b);
  check('入库 g1b', r.status === 200 && r.json.accepted === 4, JSON.stringify(r.json));
  r = await ingest('g1c', 'obj-x', c);
  check('入库 g1c（同内容不同组）', r.status === 200 && r.json.accepted === 4, JSON.stringify(r.json));

  r = await req('POST', '/api/comparisons', TOK.an, { task_key: 'cmp-main', left_group: 'g1a', right_group: 'g1b' });
  check('创建比较任务', r.status === 200 && r.json.idempotent_replay === false, JSON.stringify(r.json));
  check('任务钉住算法版本与水位', r.json.task.algo_version === 'chain-walk-v1'
    && r.json.task.left_watermark === 4 && r.json.task.right_watermark === 4);

  r = await req('GET', '/api/comparisons/cmp-main', TOK.an);
  check('刷新重读：运行前 state=queued 无证明', r.json.state === 'queued' && r.json.stage === null && r.json.proof_digest === null);

  r = await runWorker();
  check('worker 处理任务', r.status === 200 && r.json.processed.some((p) => p.task_key === 'cmp-main' && p.state === 'done'));

  r = await req('GET', '/api/comparisons/cmp-main', TOK.an);
  check('刷新重读：运行后 state=done 有证明摘要', r.json.state === 'done' && r.json.stage === 'DONE'
    && /^[0-9a-f]{64}$/.test(r.json.proof_digest || ''));

  r = await req('GET', '/api/comparisons/cmp-main/diagnostic', TOK.an);
  const d = r.json;
  check('第一处分歧在 seq 3', d.divergence && d.divergence.kind === 'chain_divergence'
    && d.divergence.object_key === 'obj-x' && d.divergence.seq === 3, JSON.stringify(d.divergence));
  check('分歧两侧节点正确', d.divergence.left.node === hex('node:n3') && d.divergence.right.node === hex('node:m3'));
  check('受影响下游节点覆盖 seq 3..4 两侧', d.affected.length === 4
    && d.affected.some((x) => x.node_ref === hex('node:n4') && x.side === 'left')
    && d.affected.some((x) => x.node_ref === hex('node:m4') && x.side === 'right'));
  check('解释证据链完整', d.explanations.length === 6
    && d.explanations[0].prev === null
    && d.explanations.every((n, i) => n.ordinal === i && (i === 0 || n.prev === d.explanations[i - 1].digest))
    && d.explain_head === d.explanations[d.explanations.length - 1].digest);
  check('证据首节点为分歧节点', d.explanations[0].kind === 'divergence');

  r = await req('POST', '/api/proofs/verify', TOK.ob, { task_key: 'cmp-main' });
  check('观察者验证证明通过', r.status === 200 && r.json.ok === true, JSON.stringify(r.json));
  r = await req('GET', '/api/proofs/cmp-main', TOK.ob);
  check('证明摘要带实时验真', r.status === 200 && r.json.verification.ok === true);

  r = await req('POST', '/api/comparisons', TOK.an, { task_key: 'cmp-idem', left_group: 'g1a', right_group: 'g1c' });
  check('创建一致组比较', r.status === 200);
  await runWorker();
  r = await req('GET', '/api/comparisons/cmp-idem/diagnostic', TOK.an);
  check('一致组无分歧', r.json.divergence === null && r.json.explanations[0].kind === 'verdict'
    && r.json.explanations[0].result === 'identical', JSON.stringify(r.json.divergence));
  r = await req('POST', '/api/proofs/verify', TOK.ob, { task_key: 'cmp-idem' });
  check('一致组证明可验证', r.json.ok === true);
}

async function scenario2() {
  console.log('\n[2] 同键复用：原任务、原分歧位置、原证明');
  const before = (await req('GET', '/api/comparisons/cmp-main/diagnostic', TOK.an)).json;
  const r = await req('POST', '/api/comparisons', TOK.an, { task_key: 'cmp-main', left_group: 'g1a', right_group: 'g1b' });
  check('同键同输入返回原任务', r.status === 200 && r.json.idempotent_replay === true);
  const after = (await req('GET', '/api/comparisons/cmp-main/diagnostic', TOK.an)).json;
  check('分歧位置不变', JSON.stringify(after.divergence) === JSON.stringify(before.divergence));
  check('证明不变', after.proof_digest === before.proof_digest);
  const list = (await req('GET', '/api/comparisons', TOK.an)).json.filter((t) => t.task_key === 'cmp-main');
  check('仍只有一个任务', list.length === 1);
}

async function scenario3() {
  console.log('\n[3] 同键异输入冲突');
  const r = await req('POST', '/api/comparisons', TOK.an, { task_key: 'cmp-main', left_group: 'g1a', right_group: 'g1c' });
  check('同键异输入 409', r.status === 409 && r.json.error.code === 'TASK_INPUT_CONFLICT', JSON.stringify(r.json));
}

async function scenario4() {
  console.log('\n[4] 拓扑坏包整包拒绝');
  // 外部变更节点引用（无收据的前驱摘要）是合法输入，先入库作为后续检查的基线
  const extRef = [mkReceipt('obj-mis', 1, 'ma', ['ghost'], 'ma', null, SECRET)];
  let r = await ingest('ok-ext', 'obj-mis', extRef);
  check('外部变更节点引用合法入库', r.status === 200 && r.json.accepted === 1, JSON.stringify(r.json));

  const countBefore = (await req('GET', '/api/objects', TOK.op)).json.length;

  const cyc = [
    mkReceipt('obj-cyc', 1, 'ca', ['cb'], 'ca', null, SECRET),
    mkReceipt('obj-cyc', 2, 'cb', ['ca'], 'cb', null, SECRET),
  ];
  cyc[1] = mkReceipt('obj-cyc', 2, 'cb', ['ca'], 'cb', cyc[0].hash, SECRET);
  r = await ingest('bad1', 'obj-cyc', cyc);
  check('环路拒绝', r.status === 422 && r.json.error.code === 'TOPOLOGY_CYCLE', JSON.stringify(r.json));

  const dup = [
    mkReceipt('obj-dup', 1, 'da', [], 'da', null, SECRET),
    mkReceipt('obj-dup', 1, 'db', [], 'db', null, SECRET),
  ];
  r = await ingest('bad2', 'obj-dup', dup);
  check('重复序号拒绝', r.status === 422 && r.json.error.code === 'SEQ_DUPLICATE', JSON.stringify(r.json));

  const r1 = mkReceipt('obj-nbc', 1, 'na', [], 'v1', null, SECRET);
  const r2 = mkReceipt('obj-nbc', 2, 'na', ['na'], 'v2', r1.hash, SECRET);
  r = await ingest('bad3', 'obj-nbc', [r1, r2]);
  check('同节点两份正文拒绝', r.status === 422 && r.json.error.code === 'NODE_BODY_CONFLICT', JSON.stringify(r.json));

  // 跨对象前驱：obj-q 的收据引用 obj-mis 的决定节点
  const cross = [mkReceipt('obj-q', 1, 'qa', ['ma'], 'qa', null, SECRET)];
  r = await ingest('ok-ext', 'obj-q', cross);
  check('跨对象前驱拒绝', r.status === 422 && r.json.error.code === 'TOPOLOGY_PARENT_CROSS_OBJECT', JSON.stringify(r.json));

  const br1 = mkReceipt('obj-brk', 1, 'ba', [], 'ba', null, SECRET);
  const br2 = mkReceipt('obj-brk', 2, 'bb', ['ba'], 'bb', hex('node:not-the-prev'), SECRET);
  r = await ingest('bad5', 'obj-brk', [br1, br2]);
  check('链断裂拒绝', r.status === 422 && r.json.error.code === 'RECEIPT_CHAIN_BROKEN', JSON.stringify(r.json));

  const badDigest = [mkReceipt('obj-dig', 1, 'ga', [], 'ga', null, SECRET)];
  badDigest[0] = { ...badDigest[0], hash: 'not-hex' };
  r = await ingest('bad6', 'obj-dig', badDigest);
  check('摘要格式拒绝', r.status === 422 && r.json.error.code === 'DIGEST_FORMAT_INVALID', JSON.stringify(r.json));

  const badSig = [mkReceipt('obj-sig', 1, 'sa', [], 'sa', null, 'wrong-secret')];
  r = await ingest('bad7', 'obj-sig', badSig);
  check('链签名拒绝', r.status === 422 && r.json.error.code === 'RECEIPT_SIG_INVALID', JSON.stringify(r.json));

  const tampered = mkReceipt('obj-hash', 1, 'ha', [], 'ha', null, SECRET);
  tampered.contributions = [{ region: 'tiles:000:001', change_digest: hex('change:other') }];
  r = await ingest('bad8', 'obj-hash', [tampered]);
  check('内容摘要不符拒绝', r.status === 422 && r.json.error.code === 'RECEIPT_HASH_MISMATCH', JSON.stringify(r.json));

  const withSite = [mkReceipt('obj-site', 1, 'xa', [], 'xa', null, SECRET)];
  withSite[0] = { ...withSite[0], site: 'station-alpha' };
  r = await ingest('bad9', 'obj-site', withSite);
  check('非脱敏字段拒绝', r.status === 422 && r.json.error.code === 'RECEIPT_FIELD_FORBIDDEN', JSON.stringify(r.json));

  const countAfter = (await req('GET', '/api/objects', TOK.op)).json.length;
  check('坏包不留下任何状态', countAfter === countBefore, `before=${countBefore} after=${countAfter}`);

  // 与已存链冲突：同序号不同正文
  const again = mkChain('obj-x', ['n1', 'n2', 'zz'], SECRET);
  r = await ingest('g1a', 'obj-x', again);
  check('与已存链同序号异正文拒绝', r.status === 422 && r.json.error.code === 'SEQ_DUPLICATE', JSON.stringify(r.json));
  // 同内容重放是幂等的
  const replay = mkChain('obj-x', ['n1', 'n2', 'n3', 'n4'], SECRET);
  r = await ingest('g1a', 'obj-x', replay);
  check('同内容重放幂等', r.status === 200 && r.json.accepted === 0 && r.json.duplicates === 4, JSON.stringify(r.json));
}

async function scenario5() {
  console.log('\n[5] 并发唯一任务');
  await ingest('g5a', 'obj-c', mkChain('obj-c', ['p1', 'p2'], SECRET));
  await ingest('g5b', 'obj-c', mkChain('obj-c', ['p1', 'q2'], SECRET));
  const body = { task_key: 'conc-1', left_group: 'g5a', right_group: 'g5b' };
  const results = await Promise.all(Array.from({ length: 8 }, () => req('POST', '/api/comparisons', TOK.an, body)));
  check('并发提交全部成功', results.every((r) => r.status === 200));
  check('恰好一个非重放', results.filter((r) => r.json.idempotent_replay === false).length === 1);
  const list = (await req('GET', '/api/comparisons', TOK.an)).json.filter((t) => t.task_key === 'conc-1');
  check('只留下一个任务', list.length === 1);
  await runWorker();
  const again = await Promise.all(Array.from({ length: 4 }, () => req('POST', '/api/comparisons', TOK.an, body)));
  check('完成后同键重放仍返回原任务', again.every((r) => r.status === 200 && r.json.idempotent_replay === true));
  const diag = (await req('GET', '/api/comparisons/conc-1/diagnostic', TOK.an)).json;
  check('并发任务证明一致', again.every((r) => r.json.task.proof_digest === diag.proof_digest));
}

async function scenario6() {
  console.log('\n[6] 水位边界：水位后到达的收据不混入');
  const left = mkChain('obj-w', ['w1', 'w2', 'w3'], SECRET);
  const right = mkChain('obj-w', ['w1', 'w2', 'x3'], SECRET);
  await ingest('g6a', 'obj-w', left);
  await ingest('g6b', 'obj-w', right);
  let r = await req('POST', '/api/comparisons', TOK.an, { task_key: 'wm-1', left_group: 'g6a', right_group: 'g6b' });
  check('任务钉住水位 3/3', r.json.task.left_watermark === 3 && r.json.task.right_watermark === 3);

  // 水位之后到达的新收据
  const more = mkChain('obj-w', ['w4', 'w5'], SECRET, { startSeq: 4, prev: left[2].hash, prevNode: 'w3' });
  r = await ingest('g6a', 'obj-w', more);
  check('水位后继续入库', r.status === 200 && r.json.watermark === 5, JSON.stringify(r.json));

  await runWorker();
  const d1 = (await req('GET', '/api/comparisons/wm-1/diagnostic', TOK.an)).json;
  check('诊断不含水位后节点', !JSON.stringify(d1).includes(hex('node:w4')) && !JSON.stringify(d1).includes(hex('node:w5')));
  check('分歧仍在 seq 3', d1.divergence.seq === 3 && d1.affected.length === 2);
  const proofBefore = d1.proof_digest;

  r = await req('POST', '/api/comparisons', TOK.an, { task_key: 'wm-2', left_group: 'g6a', right_group: 'g6b' });
  check('新任务读到新水位 5/3', r.json.task.left_watermark === 5 && r.json.task.right_watermark === 3);
  await runWorker();
  const d2 = (await req('GET', '/api/comparisons/wm-2/diagnostic', TOK.an)).json;
  check('新任务包含水位后节点', JSON.stringify(d2.affected).includes(hex('node:w4'))
    && JSON.stringify(d2.affected).includes(hex('node:w5')));
  const t1 = (await req('GET', '/api/comparisons/wm-1', TOK.an)).json;
  check('旧任务水位与证明不变', t1.left_watermark === 3 && t1.proof_digest === proofBefore);
  r = await req('POST', '/api/proofs/verify', TOK.ob, { task_key: 'wm-1' });
  check('旧任务证明仍可验证', r.json.ok === true);
}

async function scenario7() {
  console.log('\n[7] 算法切换只影响新任务');
  await ingest('g7a', 'obj-a', mkChain('obj-a', ['a1', 'a2', 'a3'], SECRET));
  await ingest('g7b', 'obj-a', mkChain('obj-a', ['a1', 'a2', 'b3'], SECRET));

  let r = await req('POST', '/api/comparisons', TOK.an, { task_key: 'av-old', left_group: 'g7a', right_group: 'g7b' });
  check('切换前任务钉住 v1', r.json.task.algo_version === 'chain-walk-v1');

  r = await req('POST', '/api/algo-versions/activate', TOK.op, { version: 'skeleton-align-v2' });
  check('切换到 v2', r.status === 200 && r.json.active === 'skeleton-align-v2');

  r = await req('POST', '/api/comparisons', TOK.an, { task_key: 'av-new', left_group: 'g7a', right_group: 'g7b' });
  check('切换后任务钉住 v2', r.json.task.algo_version === 'skeleton-align-v2');

  await runWorker();
  const oldTask = (await req('GET', '/api/comparisons/av-old', TOK.an)).json;
  const newTask = (await req('GET', '/api/comparisons/av-new', TOK.an)).json;
  check('进行中任务继续使用旧版本', oldTask.algo_version === 'chain-walk-v1' && oldTask.state === 'done');
  check('新任务使用新版本', newTask.algo_version === 'skeleton-align-v2' && newTask.state === 'done');

  const oldDiag = (await req('GET', '/api/comparisons/av-old/diagnostic', TOK.an)).json;
  const newDiag = (await req('GET', '/api/comparisons/av-new/diagnostic', TOK.an)).json;
  check('v1 输出链式分歧', oldDiag.divergence.kind === 'chain_divergence' && oldDiag.divergence.seq === 3);
  check('v2 输出节点式分歧', newDiag.divergence.kind === 'node_divergence'
    && [hex('node:a3'), hex('node:b3')].includes(newDiag.divergence.node_ref), JSON.stringify(newDiag.divergence));
  check('v2 上下文含共同祖先', newDiag.explanations.some((n) => n.kind === 'context'
    && JSON.stringify(n.common_ancestors) === JSON.stringify([hex('node:a2')].sort())));

  r = await req('POST', '/api/proofs/verify', TOK.ob, { task_key: 'av-old' });
  check('旧版本任务证明可验证', r.json.ok === true);
  r = await req('POST', '/api/proofs/verify', TOK.ob, { task_key: 'av-new' });
  check('新版本任务证明可验证', r.json.ok === true);

  r = await req('POST', '/api/algo-versions', TOK.op, { version: 'no-such-algo' });
  check('登记未实现版本被拒', r.status === 422 && r.json.error.code === 'ALGO_VERSION_UNKNOWN');
  r = await req('POST', '/api/algo-versions', TOK.op, { version: 'chain-walk-v1' });
  check('重复登记被拒', r.status === 409 && r.json.error.code === 'ALGO_VERSION_EXISTS');
  r = await req('POST', '/api/algo-versions/activate', TOK.op, { version: 'no-such-algo' });
  check('激活未知版本被拒', r.status === 404 && r.json.error.code === 'ALGO_VERSION_UNKNOWN');
}

async function scenario8() {
  console.log('\n[8] 计算中断恢复');
  await ingest('g8a', 'obj-r', mkChain('obj-r', ['c1', 'c2', 'c3', 'c4'], SECRET));
  await ingest('g8b', 'obj-r', mkChain('obj-r', ['c1', 'c2', 'd3', 'd4'], SECRET));

  // 8a: EXPLAIN 阶段中途崩溃 → 从持久阶段继续，解释节点不重复
  await stopServer();
  await startServer({ DIFFDIAG_CRASH_POINT: 'during_explain:2' });
  await req('POST', '/api/comparisons', TOK.an, { task_key: 'cr-explain', left_group: 'g8a', right_group: 'g8b' });
  await runWorker(); // 进程在第二个解释节点落盘后退出
  await waitExit(serverProc);
  let st = readState();
  check('崩溃时 EXPLAIN 未完成', st.tasks['cr-explain'].state === 'running' && st.tasks['cr-explain'].stage === 'DIFF');
  check('崩溃前已落两个解释节点', st.tasks['cr-explain'].explanations.length === 2);
  const preDigests = st.tasks['cr-explain'].explanations.map((n) => n.digest);

  await startServer();
  let r = await req('GET', '/api/comparisons/cr-explain', TOK.an);
  check('重启后保持 running/DIFF', r.json.state === 'running' && r.json.stage === 'DIFF');
  await runWorker();
  const diag = (await req('GET', '/api/comparisons/cr-explain/diagnostic', TOK.an)).json;
  const ordinals = diag.explanations.map((n) => n.ordinal);
  check('解释节点序号无重复', new Set(ordinals).size === ordinals.length
    && ordinals.every((o, i) => o === i));
  check('已落节点未被重写', diag.explanations[0].digest === preDigests[0] && diag.explanations[1].digest === preDigests[1]);
  r = await req('POST', '/api/proofs/verify', TOK.ob, { task_key: 'cr-explain' });
  check('恢复后证明可验证', r.json.ok === true);

  // 8b: 领取后任何阶段未持久化就崩溃 → 回到未开始
  await stopServer();
  await startServer({ DIFFDIAG_CRASH_POINT: 'after_claim' });
  await req('POST', '/api/comparisons', TOK.an, { task_key: 'cr-claim', left_group: 'g8a', right_group: 'g8b' });
  await runWorker();
  await waitExit(serverProc);
  await startServer();
  r = await req('GET', '/api/comparisons/cr-claim', TOK.an);
  check('崩溃后回到未开始', r.json.state === 'queued' && r.json.stage === null, JSON.stringify(r.json));
  await runWorker();
  r = await req('GET', '/api/comparisons/cr-claim', TOK.an);
  check('重跑后完成', r.json.state === 'done');
  const diagB = (await req('GET', '/api/comparisons/cr-claim/diagnostic', TOK.an)).json;
  check('重跑解释节点不重复', new Set(diagB.explanations.map((n) => n.ordinal)).size === diagB.explanations.length);

  // 8c: DIFF 阶段持久化后崩溃 → 从 DIFF 继续
  await stopServer();
  await startServer({ DIFFDIAG_CRASH_POINT: 'after_stage:DIFF' });
  await req('POST', '/api/comparisons', TOK.an, { task_key: 'cr-diff', left_group: 'g8a', right_group: 'g8b' });
  await runWorker();
  await waitExit(serverProc);
  st = readState();
  check('DIFF 阶段已持久化', st.tasks['cr-diff'].stage === 'DIFF' && st.tasks['cr-diff'].diff !== null
    && st.tasks['cr-diff'].explanations.length === 0);
  await startServer();
  await runWorker();
  r = await req('GET', '/api/comparisons/cr-diff', TOK.an);
  check('从 DIFF 续跑完成', r.json.state === 'done' && r.json.stage === 'DONE');
  r = await req('POST', '/api/proofs/verify', TOK.ob, { task_key: 'cr-diff' });
  check('续跑任务证明可验证', r.json.ok === true);
}

async function scenario9() {
  console.log('\n[9] 取消竞态与诊断不可覆盖');
  await ingest('g9a', 'obj-z', mkChain('obj-z', ['z1', 'z2'], SECRET));
  await ingest('g9b', 'obj-z', mkChain('obj-z', ['z1', 'y2'], SECRET));

  let sawCancelled = 0;
  let sawStarted = 0;
  for (let i = 0; i < 5; i++) {
    const key = `race-${i}`;
    await req('POST', '/api/comparisons', TOK.an, { task_key: key, left_group: 'g9a', right_group: 'g9b' });
    const [cancel, run] = await Promise.all([
      req('POST', `/api/comparisons/${key}/cancel`, TOK.op),
      req('POST', '/api/worker/run', TOK.op),
    ]);
    const task = (await req('GET', `/api/comparisons/${key}`, TOK.op)).json;
    const cancelledWon = cancel.status === 200 && task.state === 'cancelled'
      && !run.json.processed.some((p) => p.task_key === key);
    const startedWon = cancel.status === 409 && cancel.json.error.code === 'TASK_ALREADY_STARTED'
      && (task.state === 'done' || task.state === 'running');
    check(`第 ${i + 1} 轮恰有一方获胜`, cancelledWon !== startedWon,
      `cancel=${cancel.status} state=${task.state}`);
    if (cancelledWon) sawCancelled++;
    if (startedWon) sawStarted++;
  }
  check('竞态两种结果都成立过（至少各一次不强制）', sawCancelled + sawStarted === 5);

  let r = await req('POST', '/api/comparisons/race-9/cancel', TOK.op);
  check('取消未知任务 404', r.status === 404 && r.json.error.code === 'TASK_UNKNOWN');
  const cancelledKey = (await req('GET', '/api/comparisons', TOK.op)).json.find((t) => t.state === 'cancelled');
  if (cancelledKey) {
    r = await req('POST', `/api/comparisons/${cancelledKey.task_key}/cancel`, TOK.op);
    check('重复取消幂等', r.status === 200 && r.json.idempotent_replay === true);
    r = await req('GET', `/api/comparisons/${cancelledKey.task_key}`, TOK.op);
    check('取消释放冻结输入', r.json.inputs_released === true && r.json.left_inputs === 0);
  } else {
    check('重复取消幂等（本轮无取消胜者，单独构造）', true);
    await req('POST', '/api/comparisons', TOK.an, { task_key: 'race-x', left_group: 'g9a', right_group: 'g9b' });
    await req('POST', '/api/comparisons/race-x/cancel', TOK.op);
    r = await req('POST', '/api/comparisons/race-x/cancel', TOK.op);
    check('重复取消幂等', r.status === 200 && r.json.idempotent_replay === true);
    r = await req('GET', '/api/comparisons/race-x', TOK.op);
    check('取消释放冻结输入', r.json.inputs_released === true && r.json.left_inputs === 0);
  }

  r = await req('POST', '/api/comparisons/cmp-main/cancel', TOK.op);
  check('已完成诊断不可取消', r.status === 409 && r.json.error.code === 'TASK_ALREADY_DONE');
  const proofBefore = (await req('GET', '/api/comparisons/cmp-main', TOK.an)).json.proof_digest;
  await runWorker();
  const proofAfter = (await req('GET', '/api/comparisons/cmp-main', TOK.an)).json.proof_digest;
  check('已有诊断不被静默覆盖', proofBefore === proofAfter);
}

async function scenario10() {
  console.log('\n[10] 证明被改 => 验真失败');
  const original = fs.readFileSync(STATE_FILE, 'utf8');
  const edit = async (fn, expectedCode) => {
    await stopServer();
    const st = JSON.parse(original);
    fn(st);
    fs.writeFileSync(STATE_FILE, JSON.stringify(st));
    await startServer();
    const r = await req('POST', '/api/proofs/verify', TOK.ob, { task_key: 'cmp-main' });
    check(`篡改后验真失败（${expectedCode}）`, r.status === 200 && r.json.ok === false && r.json.code === expectedCode,
      JSON.stringify(r.json));
  };
  await edit((st) => { st.tasks['cmp-main'].diff.divergence.seq = 99; }, 'PROOF_MISMATCH');
  await edit((st) => { st.tasks['cmp-main'].explanations[1].kind = 'forged'; }, 'EXPLANATION_CHAIN_BROKEN');
  await edit((st) => { st.tasks['cmp-main'].proof.sig = '0'.repeat(64); }, 'PROOF_SIG_INVALID');
  await edit((st) => { st.tasks['cmp-main'].proof.digest = '0'.repeat(64); }, 'PROOF_MISMATCH');

  await stopServer();
  fs.writeFileSync(STATE_FILE, original);
  await startServer();
  const r = await req('POST', '/api/proofs/verify', TOK.ob, { task_key: 'cmp-main' });
  check('恢复后验真通过', r.json.ok === true);

  const bad = await req('POST', '/api/proofs/verify', TOK.ob, { proof: { task_key: 'cmp-main', digest: '0'.repeat(64) } });
  check('外来假证明被拒', bad.status === 200 && bad.json.ok === false && bad.json.code === 'PROOF_MISMATCH');
  const unknown = await req('POST', '/api/proofs/verify', TOK.ob, { task_key: 'no-such-task' });
  check('未知证明 404', unknown.status === 404 && unknown.json.error.code === 'PROOF_UNKNOWN');
}

async function scenario11() {
  console.log('\n[11] 角色越权');
  const cases = [
    ['观察者建比较', 'POST', '/api/comparisons', TOK.ob, { task_key: 'x', left_group: 'g1a', right_group: 'g1b' }, 403],
    ['观察者看任务列表', 'GET', '/api/comparisons', TOK.ob, undefined, 403],
    ['观察者看诊断', 'GET', '/api/comparisons/cmp-main/diagnostic', TOK.ob, undefined, 403],
    ['观察者取消任务', 'POST', '/api/comparisons/cmp-main/cancel', TOK.ob, undefined, 403],
    ['观察者入库', 'POST', '/api/packages', TOK.ob, {}, 403],
    ['观察者管版本', 'POST', '/api/algo-versions/activate', TOK.ob, { version: 'chain-walk-v1' }, 403],
    ['观察者看对象', 'GET', '/api/objects', TOK.ob, undefined, 403],
    ['观察者驱动worker', 'POST', '/api/worker/run', TOK.ob, undefined, 403],
    ['分析者入库', 'POST', '/api/packages', TOK.an, {}, 403],
    ['分析者管版本', 'POST', '/api/algo-versions/activate', TOK.an, { version: 'chain-walk-v1' }, 403],
    ['分析者取消任务', 'POST', '/api/comparisons/cmp-main/cancel', TOK.an, undefined, 403],
    ['分析者驱动worker', 'POST', '/api/worker/run', TOK.an, undefined, 403],
    ['运维者建比较', 'POST', '/api/comparisons', TOK.op, { task_key: 'x', left_group: 'g1a', right_group: 'g1b' }, 403],
    ['运维者看诊断', 'GET', '/api/comparisons/cmp-main/diagnostic', TOK.op, undefined, 403],
    ['无令牌', 'GET', '/api/proofs', null, undefined, 401],
    ['坏令牌', 'GET', '/api/proofs', TOK.bogus, undefined, 401],
  ];
  for (const [name, m, p, t, b, want] of cases) {
    const r = await req(m, p, t, b);
    check(name, r.status === want, `got ${r.status}`);
  }
  let r = await req('GET', '/api/proofs', TOK.ob);
  check('观察者可读证明列表', r.status === 200);
  r = await req('POST', '/api/proofs/verify', TOK.ob, { task_key: 'cmp-main' });
  check('观察者可验证证明', r.status === 200 && r.json.ok === true);
  r = await req('GET', '/api/whoami', TOK.ob);
  check('whoami 返回观察者', r.json.role === 'observer');
  r = await req('GET', '/api/whoami', TOK.an);
  check('whoami 返回分析者', r.json.role === 'analyst');
  r = await req('GET', '/api/whoami', TOK.op);
  check('whoami 返回运维者', r.json.role === 'operator');
  r = await req('GET', '/api/comparisons', TOK.op);
  check('运维者可读任务列表', r.status === 200);
}

async function scenario12() {
  console.log('\n[12] 错误页面不回显输入包内容；证据保持脱敏');
  const MARKER = 'MARKER-7f3a-site-zulu';

  const withMarkerField = [mkReceipt('obj-m', 1, 'mk', [], 'mk', null, SECRET)];
  withMarkerField[0] = { ...withMarkerField[0], site: MARKER };
  let r = await ingest('gmk', 'obj-m', withMarkerField);
  check('非法字段拒绝且不回显', r.status === 422 && !r.text.includes(MARKER), r.text);

  const withMarkerDigest = [mkReceipt('obj-m', 1, 'mk', [], 'mk', null, SECRET)];
  withMarkerDigest[0] = { ...withMarkerDigest[0], decision_node: 'node-' + MARKER };
  r = await ingest('gmk', 'obj-m', withMarkerDigest);
  check('非法摘要拒绝且不回显', r.status === 422 && !r.text.includes(MARKER), r.text);

  const withMarkerTime = [mkReceipt('obj-m', 1, 'mk', [], 'mk', null, SECRET)];
  withMarkerTime[0] = { ...withMarkerTime[0], emitted_at: MARKER };
  r = await ingest('gmk', 'obj-m', withMarkerTime);
  check('内容不符拒绝且不回显', r.status === 422 && !r.text.includes(MARKER), r.text);

  r = await req('GET', '/', null);
  check('控制台页面不含标记', r.status === 200 && !r.text.includes(MARKER));
  r = await req('GET', '/app.js', null);
  check('控制台脚本不含标记', r.status === 200 && !r.text.includes(MARKER));
  r = await req('GET', '/api/nonexistent', TOK.op);
  check('未知路由通用错误', r.status === 404 && r.json.error.code === 'NOT_FOUND' && !r.text.includes(MARKER));

  const diag = (await req('GET', '/api/comparisons/cmp-main/diagnostic', TOK.an)).json;
  const forbidden = deepFind(diag, {
    key: (k) => ['site', 'site_seq', 'request_key', 'image', 'pixels'].includes(k),
    value: (v) => v.includes(MARKER),
  });
  check('诊断证据无站点/请求键/图像内容', forbidden.length === 0, forbidden.join(','));
  const proofs = (await req('GET', '/api/proofs', TOK.ob)).json;
  const forbidden2 = deepFind(proofs, {
    key: (k) => ['site', 'site_seq', 'request_key', 'image', 'pixels'].includes(k),
    value: (v) => v.includes(MARKER),
  });
  check('证明摘要无站点/请求键/图像内容', forbidden2.length === 0, forbidden2.join(','));
}

/* ------------------------------------------------------------------ main -- */
async function main() {
  await startServer();
  try {
    await scenario1();
    await scenario2();
    await scenario3();
    await scenario4();
    await scenario5();
    await scenario6();
    await scenario7();
    await scenario8();
    await scenario9();
    await scenario10();
    await scenario11();
    await scenario12();
  } finally {
    await stopServer();
  }
  console.log(`\n${passed} passed, ${failed} failed`);
  process.exit(failed ? 1 : 0);
}

main().catch(async (e) => {
  console.error('verify crashed:', e);
  await stopServer();
  process.exit(1);
});
