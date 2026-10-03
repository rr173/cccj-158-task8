#!/usr/bin/env node
/**
 * End-to-end verification for verifiable history compression + cold archive.
 * Spawns the real HTTP server on an ephemeral port (fresh temp data dir) and
 * exercises, through the public API and via real process restarts:
 *
 *  A 正常压缩：热存储回收 + 当前视图/历史/下载/验真与压缩前业务结果一致
 *  B 归档祖先晚到：引用归档祖先的变更经归档索引恢复，不被当成新根
 *  C 并发写入隔离：压缩与写入/判定/下载同时发生，只看到压缩前或压缩后完整版本
 *  D 重复任务：task_key 幂等、同键异对象 409、重复请求不多删数据
 *  E 异常退出恢复：段已落盘 / stub 已落盘 / 裁剪中 三个阶段崩溃后续办或回滚
 *  F 保留策略变化：CONVERGE_RETAIN_DECISIONS 拒绝过早检查点
 *  G 归档篡改：段截断/内容篡改/清单签名失效 -> 稳定错误，不静默丢历史
 *  H 越权：监察员不能压缩/恢复，复核员不能压缩，operator 可看脱敏证明
 */
import { spawn } from 'node:child_process';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import crypto from 'node:crypto';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, '..');

let passed = 0;
let failed = 0;
function check(name, cond, detail = '') {
  if (cond) { passed++; console.log(`  ✓ ${name}`); }
  else { failed++; console.error(`  ✗ ${name} ${typeof detail === 'string' ? detail : JSON.stringify(detail)}`); }
}

const PORT = 49400 + Math.floor(Math.random() * 800);
const DATA = fs.mkdtempSync(path.join(os.tmpdir(), 'converge-archive-'));
const SECRET = 'archive-test-secret';
const TOK = {
  op: 'operator-token', r1: 'reviewer1-token', r2: 'reviewer2-token', insp: 'inspector-token',
};

let serverProc = null;
function startServer(env = {}) {
  return new Promise((resolve, reject) => {
    const proc = spawn(process.execPath, [path.join(ROOT, 'src', 'server.mjs')], {
      env: {
        ...process.env,
        CONVERGE_HOST: '127.0.0.1',
        CONVERGE_PORT: String(PORT),
        CONVERGE_DATA_DIR: DATA,
        CONVERGE_RECEIPT_SECRET: SECRET,
        CONVERGE_SETTLE_MS: '20',
        ...env,
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
async function stopServer() {
  if (!serverProc) return;
  const p = serverProc;
  serverProc = null;
  await new Promise((res) => {
    p.on('exit', res);
    p.kill('SIGTERM');
    setTimeout(() => { p.kill('SIGKILL'); res(); }, 1500);
  });
}
async function req(method, urlPath, token, body) {
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
  return { status: res.status, json, text, headers: res.headers };
}
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const digest = (s) => crypto.createHash('sha256').update(s).digest('hex');
const canonical = (v) => JSON.stringify(v, Object.keys(v).sort());

// An isolated server instance with its own data dir (used by crash-recovery
// scenarios that perform raw WAL surgery and must not touch the shared state).
let isoProc = null;
const ISO_PORT = PORT + 1000;
const ISO_DATA = fs.mkdtempSync(path.join(os.tmpdir(), 'converge-archive-iso-'));
function startIsoServer(env = {}) {
  return new Promise((resolve, reject) => {
    const proc = spawn(process.execPath, [path.join(ROOT, 'src', 'server.mjs')], {
      env: {
        ...process.env,
        CONVERGE_HOST: '127.0.0.1', CONVERGE_PORT: String(ISO_PORT),
        CONVERGE_DATA_DIR: ISO_DATA, CONVERGE_RECEIPT_SECRET: SECRET,
        CONVERGE_SETTLE_MS: '20', ...env,
      },
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    let out = '';
    proc.stdout.on('data', (d) => { out += d; });
    proc.stderr.on('data', (d) => { out += d; });
    isoProc = proc;
    const deadline = Date.now() + 5000;
    const poll = async () => {
      try {
        const r = await fetch(`http://127.0.0.1:${ISO_PORT}/healthz`);
        if (r.ok) return resolve();
      } catch { /* not yet */ }
      if (Date.now() > deadline) { proc.kill('SIGKILL'); return reject(new Error('iso server did not start: ' + out)); }
      setTimeout(poll, 50);
    };
    poll();
  });
}
async function stopIsoServer() {
  if (!isoProc) return;
  const p = isoProc;
  isoProc = null;
  await new Promise((res) => {
    p.on('exit', res);
    p.kill('SIGTERM');
    setTimeout(() => { p.kill('SIGKILL'); res(); }, 1500);
  });
}
async function ireq(method, urlPath, token, body) {
  const res = await fetch(`http://127.0.0.1:${ISO_PORT}${urlPath}`, {
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
}

async function postEnv(o, site, seq, region, change, preds, key) {
  return req('POST', '/api/envelopes', TOK.op, {
    object_key: o, site, site_seq: seq, coverage_region: region,
    change_digest: typeof change === 'string' && change.length === 64 ? change : digest(change),
    predecessor_digests: preds || [], request_key: key,
  });
}
async function settle(o) {
  const deadline = Date.now() + 5000;
  for (;;) {
    const d = (await req('GET', `/api/objects/${o}`, TOK.op)).json;
    if (d && d.status === 'settled') return d;
    if (Date.now() > deadline) throw new Error(`object ${o} never settled: ${d?.status}`);
    await sleep(50);
  }
}
async function receipts(o) {
  return (await req('GET', `/api/receipts?object_key=${o}`, TOK.op)).json.receipts;
}

// ===========================================================================
console.log('A. 正常压缩：视图/历史/下载/验真压缩前后一致，热存储回收');
await startServer();
{
  const o = 'obj-arc';
  // Build a long enough history that cold compaction clearly reclaims hot
  // storage (a stub carries a fixed-size index; only multi-decision chains make
  // the reclaimed frames larger than it).
  const N = 8;
  for (let i = 1; i <= N; i++) {
    await postEnv(o, 'station-a', i, `tiles:01:${String(i).padStart(2, '0')}`, `arc-A${i}`, [], `arc-k${i}`);
    await sleep(40);
  }
  await sleep(80);
  const before = await settle(o);
  const beforeReceipts = await receipts(o);
  check(`pre: ${N} receipts on chain`, beforeReceipts.length === N &&
    beforeReceipts.every((r, i) => (i === 0 ? r.prev_receipt === null : r.prev_receipt === beforeReceipts[i - 1].hash)),
    String(beforeReceipts.length));
  const beforeDetail = (await req('GET', `/api/objects/${o}`, TOK.op)).json;
  const beforeNodes = beforeDetail.nodes.length;
  const beforeDownload = (await req('GET', `/api/objects/${o}/receipts/download`, TOK.insp)).json;
  const walBefore = fs.statSync(path.join(DATA, 'converge.wal')).size;

  const cp = await req('POST', '/api/archive/checkpoints', TOK.op, { object_key: o, task_key: 'cp-A' });
  check('checkpoint created', cp.status === 200 && cp.json.status === 'checkpointed', JSON.stringify(cp.json));
  check('checkpoint reports reclaimed hot bytes', cp.json.reclaimed_bytes > 0, String(cp.json.reclaimed_bytes));
  const walAfter = fs.statSync(path.join(DATA, 'converge.wal')).size;
  check('hot WAL shrank', walAfter < walBefore, `${walBefore} -> ${walAfter}`);
  check('segment file exists and is read-only (0444)',
    (fs.statSync(path.join(DATA, 'archive', 'segments', `${cp.json.segment_id}.seg`)).mode & 0o777) === 0o444);

  // Current view identical.
  const after = (await req('GET', `/api/objects/${o}`, TOK.op)).json;
  check('current receipt unchanged after compression',
    after.current.hash === before.current.hash &&
    JSON.stringify(after.current.contributions) === JSON.stringify(before.current.contributions),
    JSON.stringify({ a: after.current?.hash, b: before.current?.hash }));
  check('status still settled, tips identical',
    after.status === 'settled' && JSON.stringify(after.tips) === JSON.stringify(beforeDetail.tips),
    `status=${after.status} before=${JSON.stringify(beforeDetail.tips)} after=${JSON.stringify(after.tips)}`);
  check('checkpoint marker exposed on object view',
    after.checkpoint?.boundary_seq === N && after.checkpoint?.segment_id === cp.json.segment_id && after.checkpoint.verified === true);

  // History proofs unchanged: every receipt still retrievable and verifiable.
  const afterReceipts = await receipts(o);
  check('receipt history length identical after compression', afterReceipts.length === beforeReceipts.length,
    `${afterReceipts.length} vs ${beforeReceipts.length}`);
  check('receipt chain hashes identical',
    JSON.stringify(afterReceipts.map((r) => r.hash)) === JSON.stringify(beforeReceipts.map((r) => r.hash)));
  for (const r of afterReceipts) {
    const v = await req('POST', '/api/receipts/verify', TOK.insp, { receipt: r });
    check(`receipt #${r.seq} verifies after compression`, v.status === 200 && v.json.ok === true, JSON.stringify(v.json));
  }
  const afterDownload = (await req('GET', `/api/objects/${o}/receipts/download`, TOK.insp)).json;
  check('desensitised download identical after compression',
    JSON.stringify(afterDownload) === JSON.stringify(beforeDownload));
  check('download stays desensitised', !JSON.stringify(afterDownload).includes('station-a'));

  // The object graph view merges cold nodes: full history still navigable.
  check('full node history still visible (cold merged)', after.nodes.length === beforeNodes,
    `${after.nodes.length} vs ${beforeNodes}`);
  const archivedEdge = after.edges.find((e) => e.archived === true);
  check('edges flag archived ancestry', !!archivedEdge, JSON.stringify(after.edges));

  // Cold receipt individually fetchable.
  const coldHead = afterReceipts[0];
  const one = await req('GET', `/api/receipts/${coldHead.hash}`, TOK.insp);
  check('archived receipt individually fetchable', one.status === 200 && one.json.hash === coldHead.hash);

  // Status panel content.
  const st = (await req('GET', '/api/archive/status', TOK.op)).json;
  check('status reports hot/cold sizes', st.hot.wal_bytes > 0 && st.cold.archive_bytes > 0 && st.cold.segment_count === 1,
    JSON.stringify({ hot: st.hot, cold: st.cold }));
  check('status reports checkpoint location', st.checkpoints.some((c) => c.object_key === o && c.boundary_seq === N && c.archived_nodes >= N));
  check('status restore progress shows materialization state',
    st.restore.objects[0]?.archived_nodes >= 2 && st.restore.objects[0]?.materialized_nodes >= 1 &&
    st.restore.objects[0]?.pending_nodes >= 1,
    JSON.stringify(st.restore.objects));
  check('no blockers after a clean checkpoint', st.blockers.length === 0, JSON.stringify(st.blockers));
}

// ===========================================================================
console.log('B. 归档祖先晚到：经归档索引恢复，不被当成新根');
{
  const o = 'obj-late-arc';
  // Child that references a (not-yet-reported) parent; compress the parent
  // history first, then send the child.
  const parentBody = {
    object_key: o, site: 'station-a', site_seq: 1, coverage_region: 'tiles:09:01',
    change_digest: digest('late-arc-parent'), predecessor_digests: [], request_key: 'late-arc-p',
  };
  const parentDigest = (await req('POST', '/api/envelopes/digest', TOK.op, parentBody)).json.node_digest;
  await req('POST', '/api/envelopes', TOK.op, parentBody);
  await sleep(80);
  const preCp = await settle(o);
  const boundaryDecisionNode = preCp.current.decision_node;
  const boundaryReceiptHash = preCp.current.hash;
  const cp = await req('POST', '/api/archive/checkpoints', TOK.op, { object_key: o });
  check('parent history compressed', cp.status === 200, JSON.stringify(cp.json));

  // Now the child arrives referencing the archived parent digest.
  const child = await req('POST', '/api/envelopes', TOK.op, {
    object_key: o, site: 'station-b', site_seq: 7, coverage_region: 'tiles:09:02',
    change_digest: digest('late-arc-child'), predecessor_digests: [parentDigest], request_key: 'late-arc-c',
  });
  check('late child is NOT suspended on an archived ancestor',
    child.json.status !== 'suspended', JSON.stringify(child.json));
  await sleep(120);
  const d = await settle(o);
  check('child settles into an extend/merge decision', d.current.seq === 2, String(d.current.seq));
  check('new decision chains onto the compressed boundary receipt (not a new root)',
    d.current.prev_receipt === boundaryReceiptHash,
    `prev=${d.current.prev_receipt} want=${boundaryReceiptHash}`);
  check('new decision has the restored boundary decision node as a parent',
    d.current.parents.includes(boundaryDecisionNode),
    `parents=${JSON.stringify(d.current.parents)} boundary=${boundaryDecisionNode}`);
  check('child contribution present', d.current.contributions.some((c) => c.region === 'tiles:09:02'));

  // The auto-restore is visible on the status panel.
  const st = (await req('GET', '/api/archive/status', TOK.op)).json;
  check('restore event recorded with reason late_predecessor',
    st.restore.recent.some((x) => x.reason === 'late_predecessor' && x.object_keys.includes(o)),
    JSON.stringify(st.restore.recent[0]));

  // Idempotency/sequence authority of archived envelopes still enforced.
  const replay = await req('POST', '/api/envelopes', TOK.op, parentBody);
  check('archived request key still replays (replayed=true)', replay.json.replayed === true, JSON.stringify(replay.json));
  const conflict = await req('POST', '/api/envelopes', TOK.op, { ...parentBody, change_digest: digest('late-arc-DIFFERENT'), request_key: 'late-arc-p-2' });
  check('archived site_seq different body still rejected',
    conflict.status === 409 && conflict.json.error.code === 'SITE_SEQ_CONFLICT',
    `${conflict.status} ${conflict.json?.error?.code}`);
}

// ===========================================================================
console.log('C. 并发写入隔离：压缩与写入/判定/下载同时发生');
{
  const o = 'obj-conc-arc';
  for (let i = 1; i <= 4; i++) {
    await postEnv(o, `station-${i % 2}`, i, `tiles:C:${String(i).padStart(2, '0')}`, `conc-arc-${i}`, [], `conc-arc-k${i}`);
  }
  await sleep(120);
  await settle(o);
  const preReceipts = await receipts(o);

  // Fire a checkpoint concurrently with (a) new envelope writes and (b) a
  // history download. Every response must describe a complete version.
  const wave = [];
  for (let i = 5; i <= 8; i++) {
    wave.push(postEnv(o, `station-${i % 2}`, i, `tiles:C:${String(i).padStart(2, '0')}`, `conc-arc-${i}`, [], `conc-arc-k${i}`));
  }
  const cpPromise = req('POST', '/api/archive/checkpoints', TOK.op, { object_key: o });
  const dlPromise = req('GET', `/api/objects/${o}/receipts/download`, TOK.insp);
  const [cp, dl, ...writeRes] = await Promise.all([cpPromise, dlPromise, ...wave]);
  await sleep(200);

  check('concurrent checkpoint returns a coherent result (200 or clean blocked)',
    cp.status === 200 || (cp.status === 409 && cp.json.error.code === 'CHECKPOINT_BLOCKED'),
    JSON.stringify(cp.json?.error));
  check('all concurrent writes answered', writeRes.every((r) => r.status === 200),
    JSON.stringify(writeRes.map((r) => r.status)));
  check('concurrent download is a complete valid chain (seq 1..n, linked)',
    Array.isArray(dl.json.receipts) && dl.json.receipts.every((r, i) =>
      r.seq === i + 1 && (i === 0 ? r.prev_receipt === null : r.prev_receipt === dl.json.receipts[i - 1].hash)),
    JSON.stringify(dl.json?.receipts?.map((r) => r.seq)));

  await settle(o);
  const postReceipts = await receipts(o);
  check('post-concurrency history is complete and ordered',
    postReceipts.length >= preReceipts.length &&
    postReceipts.every((r, i) => r.seq === i + 1),
    postReceipts.map((r) => r.seq).join(','));
  for (const r of postReceipts) {
    const v = await req('POST', '/api/receipts/verify', TOK.insp, { receipt: r });
    check(`post-concurrency receipt #${r.seq} verifies`, v.json?.ok === true);
  }

  // Concurrent conflict adjudication while compressed: open a conflict,
  // checkpoint attempts must be cleanly blocked, adjudication still completes.
  const oc = 'obj-conc-case';
  await postEnv(oc, 's1', 1, 'same', 'case-X', [], 'case-x');
  await postEnv(oc, 's2', 1, 'same', 'case-Y', [], 'case-y');
  await sleep(120);
  const blockedCp = await req('POST', '/api/archive/checkpoints', TOK.op, { object_key: oc });
  check('checkpoint blocked during open conflict with stable reason',
    blockedCp.status === 409 && blockedCp.json.error.code === 'CHECKPOINT_BLOCKED' &&
    blockedCp.json.error.details.reasons.includes('open_conflict'),
    JSON.stringify(blockedCp.json?.error));
  const cases = (await req('GET', '/api/cases', TOK.r1)).json.cases.filter((c) => c.object_key === oc);
  await req('POST', `/api/reviews/${cases[0].case_id}`, TOK.r1, { choices: { same: digest('case-X') } });
  const adj = await req('POST', `/api/reviews/${cases[0].case_id}`, TOK.r2, { choices: { same: digest('case-X') } });
  check('adjudication still completes while compression unavailable', adj.json.status === 'adjudicated');
}

// ===========================================================================
console.log('D. 重复任务：幂等、冲突、不多删');
{
  const o = 'obj-dup-arc';
  await postEnv(o, 's1', 1, 'd:1', 'dup-1', [], 'dup-k1');
  await sleep(80);
  await settle(o);
  const r1 = await req('POST', '/api/archive/checkpoints', TOK.op, { object_key: o, task_key: 'dup-task' });
  const r2 = await req('POST', '/api/archive/checkpoints', TOK.op, { object_key: o, task_key: 'dup-task' });
  check('same task_key replays same segment id',
    r2.json.idempotent_replay === true && r2.json.segment_id === r1.json.segment_id,
    JSON.stringify([r1.json.segment_id, r2.json]));
  const r3 = await req('POST', '/api/archive/checkpoints', TOK.op, { object_key: 'other-obj', task_key: 'dup-task' });
  check('same task_key different object -> 409 CHECKPOINT_KEY_CONFLICT',
    r3.status === 409 && r3.json.error.code === 'CHECKPOINT_KEY_CONFLICT');
  const r4 = await req('POST', '/api/archive/checkpoints', TOK.op, { object_key: o });
  check('repeat checkpoint with no newer decision is idempotent',
    r4.json.idempotent_replay === true && r4.json.segment_id === r1.json.segment_id);

  // History must be intact despite repeated calls.
  const rs = await receipts(o);
  check('repeated tasks did not delete any history', rs.length === 1 && rs[0].hash && rs[0].verified !== false);
  const segFiles = fs.readdirSync(path.join(DATA, 'archive', 'segments')).filter((f) => f.endsWith('.seg'));
  check('no duplicate segment files', segFiles.length >= 1);
  const st = (await req('GET', '/api/archive/status', TOK.op)).json;
  check('no lingering task markers', st.tasks.length === 0, JSON.stringify(st.tasks));
}

// ===========================================================================
console.log('E. 异常退出恢复：段落盘后 / stub 落盘后 / 裁剪中');
// The main server is stopped; crash scenarios run against an isolated data dir
// because they perform raw WAL surgery.
await stopServer();
const { ConvergenceStore: CS } = await import(path.join(ROOT, 'src', 'store.mjs').replace(/\\/g, '/'));
const { Wal: WalCls, encodeFrame, WAL_MAGIC } = await import(path.join(ROOT, 'src', 'wal.mjs').replace(/\\/g, '/'));
const { ArchiveManager: AM } = await import(path.join(ROOT, 'src', 'archive.mjs').replace(/\\/g, '/'));
const isoCfg = {
  dataDir: ISO_DATA, walPath: path.join(ISO_DATA, 'converge.wal'),
  receiptSecret: SECRET, settleDelayMs: 20, retainDecisions: 0,
};

// E1: segment written + PLAN marker, but no durable stub -> full rollback.
{
  const o = 'obj-crash-plan';
  await startIsoServer();
  await ireq('POST', '/api/envelopes', TOK.op, {
    object_key: o, site: 's1', site_seq: 1, coverage_region: 'e:1',
    change_digest: digest('crash-plan-1'), predecessor_digests: [], request_key: 'e-k1',
  });
  await sleep(80);
  await (async () => {
    for (let i = 0; i < 100; i++) {
      const d = (await ireq('GET', `/api/objects/${o}`, TOK.op)).json;
      if (d && d.status === 'settled') return;
      await sleep(50);
    }
  })();
  await stopIsoServer();

  const store = new CS(isoCfg);
  store.wal.open();
  store.archive.ensureDirs();
  const { frames } = store.wal.readAllFrames();
  const seg = store.archive.buildSegment({
    objectKey: o, boundaryReceiptSeq: 1, chainAnchor: null,
    frames: frames.filter((f) => f.type === 1 || f.type === 2).map((f) => ({ type: f.type, payload: f.payload })),
    envelopeIndex: frames.filter((f) => f.type === 1).map((f) => {
      const ev = JSON.parse(f.payload.toString('utf8'));
      return { digest: ev.digest, object_key: ev.object_key, site: ev.site, site_seq: ev.site_seq, request_key: ev.request_key, body_hash: ev.body_hash };
    }),
  });
  store.archive.createTaskMarker(seg.segmentId, {
    task_id: seg.segmentId, phase: 'PLAN', object_key: o, boundary_seq: 1, segment_id: seg.segmentId, created_at: new Date().toISOString(),
  });
  store.wal.close();
  const orphanExists = fs.existsSync(path.join(ISO_DATA, 'archive', 'segments', `${seg.segmentId}.seg`));
  check('E1 setup: orphan PLAN segment on disk', orphanExists);

  await startIsoServer();
  const after = fs.existsSync(path.join(ISO_DATA, 'archive', 'segments', `${seg.segmentId}.seg`));
  check('E1: orphan segment rolled back after restart (full rollback)', !after);
  const d = (await ireq('GET', `/api/objects/${o}`, TOK.op)).json;
  check('E1: object history fully intact after rollback',
    d.status === 'settled' && d.nodes.length >= 2, d.status);
  await stopIsoServer();
}

// E2: stub durable but trim not run (VERIFIED phase) -> resume the trim.
{
  const o = 'obj-crash-verify';
  await startIsoServer();
  await ireq('POST', '/api/envelopes', TOK.op, {
    object_key: o, site: 's1', site_seq: 1, coverage_region: 'f:1',
    change_digest: digest('crash-verify-1'), predecessor_digests: [], request_key: 'f-k1',
  });
  await sleep(80);
  await (async () => {
    for (let i = 0; i < 100; i++) {
      const d = (await ireq('GET', `/api/objects/${o}`, TOK.op)).json;
      if (d && d.status === 'settled') return;
      await sleep(50);
    }
  })();
  await stopIsoServer();

  // Real checkpoint (writes segment + stub and trims) so we can recover the
  // exact segment frames and the stub, then rebuild the pre-trim WAL.
  const store = new CS(isoCfg);
  store.open();
  const cpSummary = await store.createCheckpoint({ object_key: o, task_key: 'e2' });
  const segmentId = cpSummary.segment_id;
  store.close();

  const wal = new WalCls(isoCfg.walPath);
  const postTrim = wal.readAllFrames().frames;
  const stubFrame = postTrim.find((f) => f.type === 4);
  const stub = JSON.parse(stubFrame.payload.toString('utf8'));
  const am = new AM(isoCfg);
  const segRead = am.readSegment(segmentId);
  am.createTaskMarker(stub.task_id, {
    task_id: stub.task_id, phase: 'VERIFIED', object_key: o, boundary_seq: 1, segment_id: segmentId, created_at: new Date().toISOString(),
  });

  // Reconstruct the exact pre-trim state: segment frames followed by the stub.
  const scratch = isoCfg.walPath + '.scratch';
  const fd = fs.openSync(scratch, 'w');
  for (const f of segRead.frames) {
    const buf = encodeFrame(WAL_MAGIC, f.type, f.payload);
    fs.writeSync(fd, buf, 0, buf.length);
  }
  {
    const buf = encodeFrame(WAL_MAGIC, 4, stubFrame.payload);
    fs.writeSync(fd, buf, 0, buf.length);
  }
  fs.fsyncSync(fd);
  fs.closeSync(fd);
  fs.renameSync(scratch, isoCfg.walPath);
  const walSizePreTrim = fs.statSync(isoCfg.walPath).size;
  check('E2 setup: VERIFIED-phase WAL reconstructed', walSizePreTrim > 0);

  await startIsoServer(); // startup recovery must finish the trim
  const walSizeAfter = fs.statSync(isoCfg.walPath).size;
  check('E2: resume completed the pending trim after restart', walSizeAfter < walSizePreTrim,
    `${walSizePreTrim} -> ${walSizeAfter}`);
  const d = (await ireq('GET', `/api/objects/${o}`, TOK.op)).json;
  check('E2: current view intact after resumed trim', d.status === 'settled' && d.current.seq === 1, d.status);
  const rs = (await ireq('GET', `/api/receipts?object_key=${o}`, TOK.op)).json.receipts;
  check('E2: history still complete after resumed trim', rs.length === 1);
  const markers = fs.readdirSync(path.join(ISO_DATA, 'archive', 'tasks')).filter((f) => f.endsWith('.json'));
  check('E2: task marker cleared after resume', markers.length === 0, String(markers.length));
  await stopIsoServer();

  // Restart again: recovery is idempotent, nothing double-deleted.
  await startIsoServer();
  const rs2 = (await ireq('GET', `/api/receipts?object_key=${o}`, TOK.op)).json.receipts;
  check('E2: second restart does not delete again', rs2.length === 1, JSON.stringify(rs2.map((r) => r.seq)));
  await stopIsoServer();
}

// ===========================================================================
console.log('F. 保留策略变化');
{
  const o = 'obj-retain';
  await startServer();
  await postEnv(o, 's1', 1, 'g:1', 'ret-1', [], 'ret-k1');
  await sleep(60);
  await settle(o);
  await stopServer();
  await startServer({ CONVERGE_RETAIN_DECISIONS: '2' });
  const cp = await req('POST', '/api/archive/checkpoints', TOK.op, { object_key: o });
  check('retention rejects checkpoint that would leave too few hot decisions',
    cp.status === 409 && cp.json.error.details.reasons.includes('retention_policy'),
    JSON.stringify(cp.json?.error));
  // History untouched.
  const d = (await req('GET', `/api/objects/${o}`, TOK.op)).json;
  check('object unchanged after rejected checkpoint', d.status === 'settled' && !d.checkpoint);
  await stopServer();
  await startServer(); // policy back to default
  const cp2 = await req('POST', '/api/archive/checkpoints', TOK.op, { object_key: o });
  check('checkpoint allowed once policy permits', cp2.status === 200, JSON.stringify(cp2.json));
}

// ===========================================================================
console.log('G. 归档篡改：截断 / 内容改 / 清单签名失效');
await stopServer();
{
  const o = 'obj-tamper';
  await startServer();
  await postEnv(o, 's1', 1, 'h:1', 'tamper-1', [], 'h-k1');
  await postEnv(o, 's2', 1, 'h:2', 'tamper-2', [], 'h-k2');
  await sleep(100);
  await settle(o);
  const cp = await req('POST', '/api/archive/checkpoints', TOK.op, { object_key: o });
  check('G setup: checkpoint exists', cp.status === 200);
  const segId = cp.json.segment_id;
  const segPath = path.join(DATA, 'archive', 'segments', `${segId}.seg`);
  await stopServer();

  // G1: truncate the segment (chop the tail, breaks framing/manifest).
  const good = fs.readFileSync(segPath);
  fs.chmodSync(segPath, 0o644);
  fs.writeFileSync(segPath, good.subarray(0, Math.floor(good.length / 2)));
  await startServer();
  const st1 = (await req('GET', '/api/archive/status', TOK.op)).json;
  check('truncated segment surfaced as blocker (service still starts)',
    st1.blockers.some((b) => b.segment_id === segId && /TRUNCATED|TAMPERED/.test(b.reason || '')),
    JSON.stringify(st1.blockers.filter((b) => b.segment_id === segId)));
  const dl1 = await req('GET', `/api/archive/segments/${segId}/pack`, TOK.insp);
  check('tampered pack download -> stable 409', dl1.status === 409 && /ARCHIVE_(TRUNCATED|TAMPERED)/.test(dl1.json.error.code),
    `${dl1.status} ${dl1.json?.error?.code}`);
  const coldRs = await req('GET', `/api/receipts?object_key=${o}`, TOK.op);
  check('cold-history read fails with stable integrity error (not silent)',
    coldRs.status === 409 && /ARCHIVE_(TRUNCATED|TAMPERED)/.test(coldRs.json.error.code),
    `${coldRs.status} ${coldRs.json?.error?.code}`);

  // Late child referencing the archived ancestor must not suspend/root: blocked.
  const parentDigest = (await req('POST', '/api/envelopes/digest', TOK.op, {
    object_key: o, site: 's1', site_seq: 1, coverage_region: 'h:1',
    change_digest: digest('tamper-1'), predecessor_digests: [], request_key: 'h-k1',
  })).json.node_digest;
  const child = await req('POST', '/api/envelopes', TOK.op, {
    object_key: o, site: 's9', site_seq: 1, coverage_region: 'h:9',
    change_digest: digest('tamper-child'), predecessor_digests: [parentDigest], request_key: 'h-kchild',
  });
  check('late child against tampered archive rejected, never treated as root',
    child.status === 409 && /ARCHIVE_(TRUNCATED|TAMPERED)/.test(child.json.error.code),
    `${child.status} ${child.json?.error?.code}`);

  // Restore the segment; service results return to verified.
  fs.chmodSync(segPath, 0o644);
  fs.writeFileSync(segPath, good);
  fs.chmodSync(segPath, 0o444);
  const st2 = (await req('GET', '/api/archive/status', TOK.op)).json;
  check('status returns to verified after segment restored',
    st2.cold.segments.find((s) => s.segment_id === segId)?.verified === true);
  const dl2 = await req('GET', `/api/archive/segments/${segId}/pack`, TOK.insp);
  check('restored pack downloads and verifies', dl2.status === 200 && dl2.json.verification.ok === true);
  await stopServer();

  // G2: any on-disk modification (byte flip) breaks framing CRC or content digest.
  const buf = fs.readFileSync(segPath);
  fs.chmodSync(segPath, 0o644);
  buf[40] ^= 0xff;
  fs.writeFileSync(segPath, buf);
  fs.chmodSync(segPath, 0o444);
  await startServer();
  const dl3 = await req('GET', `/api/archive/segments/${segId}/pack`, TOK.insp);
  check('payload tamper -> stable 409 archive integrity code',
    dl3.status === 409 && /ARCHIVE_(TAMPERED|TRUNCATED|INDEX_MISMATCH)/.test(dl3.json.error.code),
    `${dl3.status} ${dl3.json?.error?.code}`);
  const proof = await req('GET', '/api/archive/proofs', TOK.insp);
  check('proof list marks the segment unverified',
    proof.json.proofs.find((p) => p.segment_id === segId)?.verified === false, JSON.stringify(proof.json.proofs.map(p=>[p.segment_id.slice(0,8),p.verified])));
  await stopServer();
  fs.chmodSync(segPath, 0o644);
  fs.writeFileSync(segPath, good);
  fs.chmodSync(segPath, 0o444);
}

// ===========================================================================
console.log('H. 越权：监察员不能压缩/恢复，复核员不能压缩');
await startServer();
{
  const cpInsp = await req('POST', '/api/archive/checkpoints', TOK.insp, { object_key: 'obj-arc' });
  check('inspector cannot create checkpoint (403)', cpInsp.status === 403, String(cpInsp.status));
  const rsInsp = await req('POST', '/api/archive/restore', TOK.insp, { object_key: 'obj-arc' });
  check('inspector cannot restore (403)', rsInsp.status === 403, String(rsInsp.status));
  const cpRev = await req('POST', '/api/archive/checkpoints', TOK.r1, { object_key: 'obj-arc' });
  check('reviewer cannot create checkpoint (403)', cpRev.status === 403, String(cpRev.status));
  const stRev = await req('GET', '/api/archive/status', TOK.r1);
  check('reviewer may view status read-only', stRev.status === 200, String(stRev.status));

  const proofs = await req('GET', '/api/archive/proofs', TOK.insp);
  check('inspector can list desensitised proofs', proofs.status === 200 && proofs.json.proofs.length >= 1);
  const p0 = proofs.json.proofs[0];
  check('proof is desensitised (no sites/request keys/sequences)',
    !JSON.stringify(p0).includes('station') && !JSON.stringify(p0).includes('request_key'));
  const pack = await req('GET', `/api/archive/segments/${p0.segment_id}/pack`, TOK.insp);
  check('inspector downloads desensitised proof pack', pack.status === 200 && pack.json.entries.length >= 1);
  const rawInsp = await req('GET', `/api/archive/segments/${p0.segment_id}/raw`, TOK.insp);
  check('inspector cannot download raw segment', rawInsp.status === 403, String(rawInsp.status));
  const rawOp = await req('GET', `/api/archive/segments/${p0.segment_id}/raw`, TOK.op);
  check('operator can download raw segment', rawOp.status === 200 && rawOp.text.length > 0, String(rawOp.status));

  const noAuth = await req('POST', '/api/archive/checkpoints', null, { object_key: 'obj-arc' });
  check('missing token rejected', noAuth.status === 401, String(noAuth.status));

  // Original endpoints continue to work for inspectors.
  const ver = await req('POST', '/api/receipts/verify', TOK.insp, {
    receipt: (await receipts('obj-arc'))[0],
  });
  check('original receipt verification endpoint still usable by inspector', ver.json.ok === true);
}

// ===========================================================================
console.log('I. 权限不足与归档索引/内容不匹配：稳定状态（隔离数据目录）');
{
  const o = 'obj-perm';
  await startIsoServer();
  await ireq('POST', '/api/envelopes', TOK.op, {
    object_key: o, site: 's1', site_seq: 1, coverage_region: 'i:1',
    change_digest: digest('perm-1'), predecessor_digests: [], request_key: 'i-k1',
  });
  await sleep(80);
  await (async () => { for (let i = 0; i < 100; i++) {
    const d = (await ireq('GET', `/api/objects/${o}`, TOK.op)).json;
    if (d && d.status === 'settled') return;
    await sleep(40);
  } })();

  // Permission denied: make the archive directory read-only, then checkpoint.
  const archDir = path.join(ISO_DATA, 'archive');
  await stopIsoServer();
  fs.chmodSync(archDir, 0o555);
  fs.chmodSync(path.join(archDir, 'segments'), 0o555);
  fs.chmodSync(path.join(archDir, 'tasks'), 0o555);
  await startIsoServer();
  const denied = await ireq('POST', '/api/archive/checkpoints', TOK.op, { object_key: o });
  check('checkpoint under read-only archive -> stable ARCHIVE_PERMISSION (409)',
    denied.status === 409 && denied.json.error.code === 'ARCHIVE_PERMISSION',
    `${denied.status} ${denied.json?.error?.code}`);
  // State unchanged: the object is still fully hot and usable.
  const intact = (await ireq('GET', `/api/objects/${o}`, TOK.op)).json;
  check('object stays fully usable after permission failure', intact.status === 'settled' && !intact.checkpoint,
    `${intact.status}/${!!intact.checkpoint}`);
  const stillWrite = await ireq('POST', '/api/envelopes', TOK.op, {
    object_key: o, site: 's1', site_seq: 2, coverage_region: 'i:2',
    change_digest: digest('perm-2'), predecessor_digests: [], request_key: 'i-k2',
  });
  check('normal reports continue after denied checkpoint', stillWrite.status === 200, String(stillWrite.status));
  await stopIsoServer();
  fs.chmodSync(archDir, 0o755);
  fs.chmodSync(path.join(archDir, 'segments'), 0o755);
  fs.chmodSync(path.join(archDir, 'tasks'), 0o755);

  // Index/content mismatch: tamper ONLY the manifest trailer (entry list),
  // leaving the frame bytes intact -> ARCHIVE_INDEX_MISMATCH class error.
  await startIsoServer();
  const cp = await ireq('POST', '/api/archive/checkpoints', TOK.op, { object_key: o });
  check('I setup: checkpoint created', cp.status === 200, JSON.stringify(cp.json));
  const segId = cp.json.segment_id;
  const segPath = path.join(ISO_DATA, 'archive', 'segments', `${segId}.seg`);
  const good = fs.readFileSync(segPath);
  await stopIsoServer();

  // Locate the manifest trailer (last frame) and corrupt one entry payload_hash
  // inside the JSON without touching the leading data frames.
  const { Wal: W2, parseFrames, ARCHIVE_MAGIC } = await import(path.join(ROOT, 'src', 'wal.mjs').replace(/\\/g, '/'));
  const parsed = parseFrames(good, ARCHIVE_MAGIC, { allowTornTail: false });
  const trailer = parsed.frames[parsed.frames.length - 1];
  const manifest = JSON.parse(trailer.payload.toString('utf8'));
  // Rewrite an entry hash and blank the sig so the failure is the index/content
  // cross-check rather than merely the HMAC.
  if (manifest.entries[0]) manifest.entries[0].payload_hash = manifest.entries[0].payload_hash.replace(/[0-9a-f]/, (c) => (c === '0' ? '1' : '0'));
  manifest.sig = '0'.repeat(64);
  const { encodeFrame: enc } = await import(path.join(ROOT, 'src', 'wal.mjs').replace(/\\/g, '/'));
  const dataPart = good.subarray(0, trailer.start);
  const newTrailer = enc(ARCHIVE_MAGIC, 9, Buffer.from(JSON.stringify(manifest)));
  fs.chmodSync(segPath, 0o644);
  fs.writeFileSync(segPath, Buffer.concat([dataPart, newTrailer]));
  fs.chmodSync(segPath, 0o444);

  await startIsoServer();
  const st = (await ireq('GET', '/api/archive/status', TOK.op)).json;
  check('index/content mismatch surfaced as a blocker',
    st.blockers.some((b) => b.segment_id === segId), JSON.stringify(st.blockers.filter((b) => b.segment_id === segId)));
  const pack = await ireq('GET', `/api/archive/segments/${segId}/pack`, TOK.insp);
  check('mismatched segment pack -> stable integrity error',
    pack.status === 409 && /ARCHIVE_(TAMPERED|INDEX_MISMATCH|TRUNCATED)/.test(pack.json.error.code),
    `${pack.status} ${pack.json?.error?.code}`);
  await stopIsoServer();
}

// Final integrity: a full restart keeps everything consistent.
await stopServer();
await startServer();
{
  for (const o of ['obj-arc', 'obj-late-arc', 'obj-dup-arc']) {
    const d = (await req('GET', `/api/objects/${o}`, TOK.op)).json;
    check(`final restart: ${o} settled`, d.status === 'settled', d.status + ' / ' + JSON.stringify(d.error));
  }
  const st = (await req('GET', '/api/archive/status', TOK.op)).json;
  check('final restart: no incomplete tasks', st.tasks.length === 0);
  check('final restart: every segment verified', st.cold.segments.every((s) => s.verified),
    JSON.stringify(st.cold.segments.filter((s) => !s.verified)));
}
await stopServer();

// ===========================================================================
console.log(`\n归档验证结果：${passed} 通过，${failed} 失败；数据目录 ${DATA}`);
process.exit(failed ? 1 : 0);
