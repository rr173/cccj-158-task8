#!/usr/bin/env node
/**
 * End-to-end verification for the convergence service.
 * Spawns the real HTTP server on an ephemeral port and exercises:
 *   1 normal sibling merge (deterministic order, union coverage)
 *   2 late-arriving ancestor: suspend -> resume -> settle
 *   3 request-key idempotency + same-key/different-body rejection
 *   4 same-region conflict, dual independent reviewers, mismatch then agreement
 *   5 concurrent reports coalesce into exactly one decision
 *   6 torn-WAL recovery and crash between second review and decision
 *   7 role-based access control
 *   8 receipt-chain tamper => deterministic error; mid-WAL corruption => refuse start
 *   9 receipts carry no site / request key / sequence (desensitisation)
 */
import { spawn } from 'node:child_process';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, '..');

let passed = 0;
let failed = 0;
function check(name, cond, detail = '') {
  if (cond) { passed++; console.log(`  ✓ ${name}`); }
  else { failed++; console.error(`  ✗ ${name} ${detail}`); }
}
function fail(name, detail) { check(name, false, detail || ''); }

const PORT = 49100 + Math.floor(Math.random() * 1500);
const DATA = fs.mkdtempSync(path.join(os.tmpdir(), 'converge-verify-'));
const SECRET = 'test-secret';
const TOK = {
  op: 'operator-token', r1: 'reviewer1-token', r2: 'reviewer2-token', insp: 'inspector-token', bogus: 'nope',
};

let serverProc = null;
function startServer() {
  return new Promise((resolve, reject) => {
    const proc = spawn(process.execPath, [path.join(ROOT, 'src', 'server.mjs')], {
      env: {
        ...process.env,
        CONVERGE_HOST: '127.0.0.1',
        CONVERGE_PORT: String(PORT),
        CONVERGE_DATA_DIR: DATA,
        CONVERGE_RECEIPT_SECRET: SECRET,
        CONVERGE_SETTLE_MS: '20',
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
  await new Promise((res) => { p.on('exit', res); p.kill('SIGTERM'); setTimeout(() => { p.kill('SIGKILL'); res(); }, 1500); });
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
  return { status: res.status, json };
}
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
import nodeCrypto from 'node:crypto';
const digest = (s) => nodeCrypto.createHash('sha256').update(s).digest('hex');

// --------------------------------------------------------------------------
console.log('1. 正常合成（互斥区段兄弟分支，确定顺序）');
await startServer();
{
  const o = 'obj-normal';
  // Simultaneous arrival: neither envelope is "older" than the quiet window.
  const [r1, r2] = await Promise.all([
    req('POST', '/api/envelopes', TOK.op, {
      object_key: o, site: 'station-alpha', site_seq: 1,
      coverage_region: 'tiles:01:01', change_digest: digest('change-A1'),
      predecessor_digests: [], request_key: 'req-n1',
    }),
    req('POST', '/api/envelopes', TOK.op, {
      object_key: o, site: 'station-bravo', site_seq: 1,
      coverage_region: 'tiles:01:02', change_digest: digest('change-B1'),
      predecessor_digests: [], request_key: 'req-n2',
    }),
  ]);
  check('both submissions accepted', r1.status === 200 && r2.status === 200,
    `${r1.status} ${JSON.stringify(r1.json?.error)} | ${r2.status} ${JSON.stringify(r2.json?.error)}`);
  await sleep(200);
  const list = (await req('GET', '/api/objects', TOK.op)).json;
  const row = list.objects.find((x) => x.object_key === o);
  check('object settled', row && row.status === 'settled', JSON.stringify(row));
  const receipts = (await req('GET', `/api/receipts?object_key=${o}`, TOK.op)).json.receipts;
  check('exactly one decision emitted', receipts.length === 1, `got ${receipts.length}`);
  const rec = receipts[0];
  check('decision is a merge', rec?.decision_kind === 'merge', String(rec?.decision_kind));
  check('contributions ordered deterministically by region',
    JSON.stringify(rec?.contributions.map((c) => c.region)) === JSON.stringify(['tiles:01:01', 'tiles:01:02']),
    JSON.stringify(rec?.contributions));
  check('chain head has no prev', rec?.prev_receipt === null);
  check('receipt desensitised (no site/request_key/site_seq)',
    rec && !('site' in rec) && !('request_key' in rec) && !('site_seq' in rec));

  console.log('1b. 后续兄弟产生链上第二环（extend/merge 链）');
  const r3 = await req('POST', '/api/envelopes', TOK.op, {
    object_key: o, site: 'station-alpha', site_seq: 2,
    coverage_region: 'tiles:01:03', change_digest: digest('change-A2'),
    predecessor_digests: [], request_key: 'req-n3',
  });
  check('third submit 200', r3.status === 200, JSON.stringify(r3.json?.error));
  await sleep(200);
  const receipts2 = (await req('GET', `/api/receipts?object_key=${o}`, TOK.op)).json.receipts;
  check('second ring on the chain', receipts2.length === 2 &&
    receipts2[1]?.prev_receipt === receipts2[0]?.hash && receipts2[1]?.seq === 2,
    JSON.stringify(receipts2.map((r) => [r.seq, r.prev_receipt === null ? null : r.prev_receipt?.slice(0, 6)])));
  check('union coverage retained in current decision',
    receipts2[1]?.contributions.length === 3, JSON.stringify(receipts2[1]?.contributions));
  const v = receipts2[1] ? await req('POST', '/api/receipts/verify', TOK.op, { receipt: receipts2[1] }) : { status: 0, json: { ok: false } };
  check('fresh receipt verifies', v.status === 200 && v.json.ok === true, JSON.stringify(v.json));
}

console.log('2. 前驱晚到：挂起 -> 祖先补齐 -> 自动续办');
{
  const o = 'obj-late';
  // The child is reported first and must suspend.
  const childBody = {
    object_key: o, site: 'station-bravo', site_seq: 7,
    coverage_region: 'tiles:02:05', change_digest: digest('change-child'),
    predecessor_digests: ['PRE_DIGEST_PLACEHOLDER'], request_key: 'req-child',
  };
  // Real parent digest computed as the server would (canonical envelope hash).
  const parentBody = {
    object_key: o, site: 'station-alpha', site_seq: 3,
    coverage_region: 'tiles:02:04', change_digest: digest('change-parent'),
    predecessor_digests: [], request_key: 'req-parent',
  };
  const preview = (await req('POST', '/api/envelopes/digest', TOK.op, parentBody)).json;
  childBody.predecessor_digests = [preview.node_digest];

  const c = await req('POST', '/api/envelopes', TOK.op, childBody);
  check('child suspended on missing ancestor', c.json?.status === 'suspended' &&
    c.json.missing_predecessors?.includes(preview.node_digest), JSON.stringify(c.json));
  await sleep(100);
  const suspRow = (await req('GET', `/api/objects/${o}`, TOK.op)).json;
  check('object view reports suspension reason', suspRow?.status === 'suspended' && suspRow.suspended.length === 1,
    suspRow?.status);

  // Late parent arrives.
  const p = await req('POST', '/api/envelopes', TOK.op, parentBody);
  check('parent accepted', p.status === 200, JSON.stringify(p.json?.error));
  await sleep(250);
  const after = (await req('GET', `/api/objects/${o}`, TOK.op)).json;
  check('auto-resumed and settled after ancestor arrived', after?.status === 'settled',
    `status=${after?.status} suspended=${after?.suspended?.length}`);
  const rec = after?.current;
  check('settled decision includes child region', rec?.contributions.some((x) => x.region === 'tiles:02:05'),
    JSON.stringify(rec?.contributions));
}

console.log('3. 请求键幂等与同键异正文冲突');
{
  const o = 'obj-idem';
  const body = {
    object_key: o, site: 'station-alpha', site_seq: 1,
    coverage_region: 'r-x', change_digest: digest('idem-1'),
    predecessor_digests: [], request_key: 'req-idem-key',
  };
  const a = await req('POST', '/api/envelopes', TOK.op, body);
  await sleep(200);
  const b = await req('POST', '/api/envelopes', TOK.op, body);
  check('same key + same body replays original result', b.json?.replayed === true &&
    b.json.current_receipt === a.json?.current_receipt,
    `a=${a.json?.status}/${a.json?.current_receipt?.slice(0, 8)} b=${b.json?.status}/${b.json?.current_receipt?.slice(0, 8)}`);

  const conflicting = { ...body, change_digest: digest('idem-DIFFERENT') };
  const c = await req('POST', '/api/envelopes', TOK.op, conflicting);
  check('same key + different body rejected 409', c.status === 409 && c.json?.error.code === 'REQUEST_KEY_BODY_CONFLICT',
    JSON.stringify(c.json));

  // site monotonic sequence: same (site,seq) different payload
  const d = await req('POST', '/api/envelopes', TOK.op, { ...body, request_key: 'req-other-key', change_digest: digest('idem-DIFFERENT') });
  check('same site_seq different content rejected 409', d.status === 409 && d.json?.error.code === 'SITE_SEQ_CONFLICT',
    JSON.stringify(d.json?.error));

  // Concurrent duplicate request key: only one durable result.
  const body2 = { ...body, site_seq: 2, coverage_region: 'r-y', change_digest: digest('idem-2'), request_key: 'req-race-key' };
  const rs = await Promise.all([
    req('POST', '/api/envelopes', TOK.op, body2),
    req('POST', '/api/envelopes', TOK.op, body2),
    req('POST', '/api/envelopes', TOK.op, body2),
  ]);
  await sleep(200);
  const codes = rs.map((x) => x.status);
  check('concurrent same-key requests all 200', codes.every((s) => s === 200), JSON.stringify(codes));
  const obj = (await req('GET', `/api/objects/${o}`, TOK.op)).json;
  check('concurrent duplicate created no phantom decisions',
    (obj?.nodes || []).filter((n) => n.kind === 'change').length === 2,
    `change nodes=${(obj?.nodes || []).filter((n) => n.kind === 'change').length}`);
}

console.log('4. 同区段冲突 + 两名不同复核员（先分歧后一致）');
{
  const o = 'obj-conflict';
  const dA = digest('conf-A'), dB = digest('conf-B');
  const [a, b] = await Promise.all([
    req('POST', '/api/envelopes', TOK.op, {
      object_key: o, site: 'station-alpha', site_seq: 1,
      coverage_region: 'same-region', change_digest: dA,
      predecessor_digests: [], request_key: 'req-ca',
    }),
    req('POST', '/api/envelopes', TOK.op, {
      object_key: o, site: 'station-bravo', site_seq: 1,
      coverage_region: 'same-region', change_digest: dB,
      predecessor_digests: [], request_key: 'req-cb',
    }),
  ]);
  check('conflicting reports accepted as inputs', a.status === 200 && b.status === 200);
  await sleep(250);
  const cases = (await req('GET', '/api/cases', TOK.r1)).json.cases;
  const cs = cases.find((c) => c.object_key === o);
  check('conflict case opened', !!cs, JSON.stringify(cases));
  check('two candidate digests offered', new Set(cs?.candidates.map((c) => c.change_digest)).size === 2);

  const sameReviewerTwice = await req('POST', `/api/reviews/${cs.case_id}`, TOK.r1, { choices: { 'same-region': dA } });
  check('first review recorded', sameReviewerTwice.json?.status === 'awaiting_second_review', JSON.stringify(sameReviewerTwice.json));
  const again = await req('POST', `/api/reviews/${cs.case_id}`, TOK.r1, { choices: { 'same-region': dA } });
  check('same reviewer cannot adjudicate twice (403)', again.status === 403 && again.json?.error.code === 'REVIEWER_MUST_DIFFER',
    JSON.stringify(again.json));

  // Reviewer 2 disagrees.
  const disagree = await req('POST', `/api/reviews/${cs.case_id}`, TOK.r2, { choices: { 'same-region': dB } });
  check('dissenting second review rejected atomically (409 REVIEW_MISMATCH)',
    disagree.status === 409 && disagree.json?.error.code === 'REVIEW_MISMATCH', JSON.stringify(disagree.json?.error));
  const mid = (await req('GET', `/api/cases/${cs.case_id}`, TOK.r2)).json;
  check('case still open with exactly one durable opinion', mid?.reviews.length === 1 && mid.awaiting === 1,
    JSON.stringify(mid?.reviews.length));

  // Meanwhile a new branch lands on a disjoint region while the case is open.
  const concurrent = await req('POST', '/api/envelopes', TOK.op, {
    object_key: o, site: 'station-charlie', site_seq: 1,
    coverage_region: 'other-region', change_digest: digest('conf-C'),
    predecessor_digests: [], request_key: 'req-cc',
  });
  check('new branch during open conflict is queued (not decided)', concurrent.status === 200);
  await sleep(150);
  const receiptsBefore = (await req('GET', `/api/receipts?object_key=${o}`, TOK.op)).json.receipts;
  check('no decision leaked while conflict open', receiptsBefore.length === 0, `got ${receiptsBefore.length}`);

  // Reviewer 2 agrees with reviewer 1 => adjudication.
  const agree = await req('POST', `/api/reviews/${cs.case_id}`, TOK.r2, { choices: { 'same-region': dA } });
  check('matching pair adjudicates', agree.json?.status === 'adjudicated', JSON.stringify(agree.json));
  await sleep(250);
  const detail = (await req('GET', `/api/objects/${o}`, TOK.op)).json;
  check('object settled after adjudication', detail.status === 'settled', detail.status);
  const rec = detail.current;
  check('decision records both reviewer names', Array.isArray(rec?.reviewers) && rec.reviewers.length === 2,
    JSON.stringify(rec?.reviewers));
  check('adjudicated choice and disjoint branch both present',
    rec?.contributions.find((c) => c.region === 'same-region')?.change_digest === dA &&
    rec.contributions.some((c) => c.region === 'other-region'),
    JSON.stringify(rec?.contributions));
  check('still exactly one current decision', (await req('GET', `/api/receipts?object_key=${o}`, TOK.op)).json.receipts.length === 1);
}

console.log('5. 并发上报收敛为唯一决定');
{
  const o = 'obj-concurrent';
  const N = 8;
  const tasks = [];
  for (let i = 0; i < N; i++) {
    tasks.push(req('POST', '/api/envelopes', TOK.op, {
      object_key: o, site: `station-${i % 3}`, site_seq: Math.floor(i / 3) + 1,
      coverage_region: `region-${i}`, change_digest: digest(`c-${i}`),
      predecessor_digests: [], request_key: `req-conc-${i}`,
    }));
  }
  // plus an exact duplicate pair
  const dupBody = {
    object_key: o, site: 'station-dup', site_seq: 1,
    coverage_region: 'region-dup', change_digest: digest('c-dup'),
    predecessor_digests: [], request_key: 'req-conc-dup',
  };
  tasks.push(req('POST', '/api/envelopes', TOK.op, dupBody));
  tasks.push(req('POST', '/api/envelopes', TOK.op, dupBody));
  const results = await Promise.all(tasks);
  check('all concurrent submissions answered 200', results.every((r) => r.status === 200),
    JSON.stringify(results.map((r) => r.status)));
  await sleep(300);
  const receipts = (await req('GET', `/api/receipts?object_key=${o}`, TOK.op)).json.receipts;
  check('exactly one decision for the concurrent wave', receipts.length === 1, `got ${receipts.length}`);
  check('all N+1 distinct regions represented', new Set(receipts[0]?.contributions.map((c) => c.region)).size === N + 1,
    `got ${receipts[0]?.contributions.length}`);
}

console.log('6. 越权拒绝');
{
  const envBody = {
    object_key: 'obj-rbac', site: 'x', site_seq: 1, coverage_region: 'r',
    change_digest: digest('rbac'), predecessor_digests: [], request_key: 'req-rbac',
  };
  const inspPost = await req('POST', '/api/envelopes', TOK.insp, envBody);
  check('inspector cannot submit envelopes', inspPost.status === 403, String(inspPost.status));
  const inspObjs = await req('GET', '/api/objects', TOK.insp);
  check('inspector cannot see graph (station/request fields)', inspObjs.status === 403, String(inspObjs.status));
  const opReview = await req('POST', '/api/receipts/verify', TOK.insp, {});
  check('inspector may verify receipts', opReview.status !== 403, String(opReview.status));
  const opReviewPost = await req('POST', '/api/reviews/whatever', TOK.op, { choices: {} });
  check('operator cannot adjudicate', opReviewPost.status === 403, String(opReviewPost.status));
  const noAuth = await req('GET', '/api/receipts');
  check('missing token rejected', noAuth.status === 401, String(noAuth.status));
  const badToken = await req('GET', '/api/receipts', TOK.bogus);
  check('unknown token rejected', badToken.status === 403, String(badToken.status));
  const download = await req('GET', '/api/objects/obj-normal/receipts/download', TOK.insp);
  check('inspector receipt download works and is desensitised',
    download.status === 200 && !JSON.stringify(download.json).includes('station-alpha'),
    String(download.status));
}

console.log('7. 收据篡改 -> 确定错误码');
{
  const receipts = (await req('GET', '/api/receipts?object_key=obj-normal', TOK.op)).json.receipts;
  const target = receipts[receipts.length - 1];
  if (!target) { check('target receipt exists for tamper tests', false, 'no receipts'); }
  else {
  const tampered = JSON.parse(JSON.stringify(target));
  tampered.contributions[0].change_digest = digest('ATTACKER-PIXELS');
  const t1 = await req('POST', '/api/receipts/verify', TOK.op, { receipt: tampered });
  check('content tamper => RECEIPT_HASH_MISMATCH', t1.status === 422 && t1.json?.error.code === 'RECEIPT_HASH_MISMATCH',
    JSON.stringify(t1.json?.error));

  const tampered2 = JSON.parse(JSON.stringify(target));
  tampered2.sig = tampered2.sig.replace(/[0-9a-f]/, (c) => (c === 'a' ? 'b' : 'a'));
  const t2 = await req('POST', '/api/receipts/verify', TOK.op, { receipt: tampered2 });
  check('signature tamper => RECEIPT_SIG_INVALID', t2.status === 422 && t2.json?.error.code === 'RECEIPT_SIG_INVALID',
    JSON.stringify(t2.json?.error));

  const tampered3 = JSON.parse(JSON.stringify(target));
  tampered3.hash = digest('forged');
  const t3 = await req('POST', '/api/receipts/verify', TOK.op, { receipt: tampered3 });
  check('hash forgery => HASH_MISMATCH deterministically', t3.json?.error?.code === 'RECEIPT_HASH_MISMATCH',
    JSON.stringify(t3.json?.error));

  const okAgain = await req('POST', '/api/receipts/verify', TOK.op, { receipt: target });
  check('untampered receipt still verifies', okAgain.status === 200 && okAgain.json.ok === true);
  }
}

// ---- restart: clean shutdown then reopen ----------------------------------
console.log('8. 崩溃续办（torn tail 与 二审/决定之间崩溃）');
await stopServer();
await sleep(200);

// 8a. Append a torn final frame to the WAL.
const walPath = path.join(DATA, 'converge.wal');
const beforeSize = fs.statSync(walPath).size;
fs.appendFileSync(walPath, Buffer.from([0x43, 0x56, 0x01, 0x00, 0x00, 0x10, 0x00, 0xaa, 0xbb])); // claims 4096 bytes, delivers 2
await startServer();
const health = await req('GET', '/healthz');
check('server starts over a torn tail', health.status === 200);
const survived = (await req('GET', '/api/objects', TOK.op)).json.objects.map((o) => o.object_key).sort();
check('all pre-crash objects recovered',
  JSON.stringify(survived) === JSON.stringify(['obj-concurrent', 'obj-conflict', 'obj-idem', 'obj-late', 'obj-normal']),
  JSON.stringify(survived));
const normRecs = (await req('GET', '/api/receipts?object_key=obj-normal', TOK.op)).json.receipts;
check('no duplicate decisions after restart', normRecs.length === 2 && normRecs.map((r) => r.seq).join(',') === '1,2',
  JSON.stringify(normRecs.map((r) => r.seq)));
await stopServer();

// 8b. Build a conflict, record one review, then simulate a crash after the
// second review frame was fsynced but before the decision frame landed.
{
  const { Wal } = await import(path.join(ROOT, 'src', 'wal.mjs').replace(/\\/g, '/'));
  fs.appendFileSync ? null : null;
  const o = 'obj-crash-adj';
  await startServer();
  const dA = digest('adj-A'), dB = digest('adj-B');
  await Promise.all([
    req('POST', '/api/envelopes', TOK.op, {
      object_key: o, site: 's1', site_seq: 1, coverage_region: 'reg',
      change_digest: dA, predecessor_digests: [], request_key: 'req-adj-a',
    }),
    req('POST', '/api/envelopes', TOK.op, {
      object_key: o, site: 's2', site_seq: 1, coverage_region: 'reg',
      change_digest: dB, predecessor_digests: [], request_key: 'req-adj-b',
    }),
  ]);
  await sleep(250);
  const cs = (await req('GET', '/api/cases', TOK.r1)).json.cases.find((c) => c.object_key === o);
  check('crash setup: conflict open', !!cs, 'no case');
  const first = await req('POST', `/api/reviews/${cs.case_id}`, TOK.r1, { choices: { reg: dA } });
  check('crash setup: first review durable', first.json?.status === 'awaiting_second_review', JSON.stringify(first.json));
  await stopServer();

  // Inject the second review frame directly (as if the process died right after).
  const wal = new Wal(walPath);
  wal.open();
  wal.append(3, JSON.stringify({
    case_id: cs.case_id, object_key: o,
    reviewer_id: 'anon-reviewer-02', reviewer_name: 'reviewer-02',
    choices: { reg: dA },
  }));
  wal.close();
  const afterSecondReviewSize = fs.statSync(walPath).size;
  check('injected second review without decision', afterSecondReviewSize > beforeSize);

  await startServer();
  await sleep(200);
  const obj = (await req('GET', `/api/objects/${o}`, TOK.op)).json;
  check('recovery completed the pending adjudication', obj.status === 'settled' &&
    obj.current?.decision_kind === 'adjudicate', `${obj.status}/${obj.current?.decision_kind}`);
  const recs1 = (await req('GET', `/api/receipts?object_key=${o}`, TOK.op)).json.receipts;
  check('exactly one decision after crash recovery', recs1.length === 1 && recs1[0].seq === 1,
    `got ${recs1.length}`);
  const cases = (await req('GET', '/api/cases', TOK.r1)).json.cases;
  check('case closed after recovery', !cases.some((c) => c.object_key === o));

  // Restart again: idempotence of recovery.
  await stopServer();
  await startServer();
  const recs2 = (await req('GET', `/api/receipts?object_key=${o}`, TOK.op)).json.receipts;
  check('second restart does not duplicate the decision', recs2.length === 1, `got ${recs2.length}`);
  const verify = await req('POST', '/api/receipts/verify', TOK.op, { receipt: recs2[0] });
  check('recovered receipt chain verifies', verify.status === 200 && verify.json.ok === true, JSON.stringify(verify.json));
}

// 8c. Mid-file WAL corruption must make the service refuse to start.
console.log('9. WAL 中段篡改 -> 拒绝启动（不跳过已提交事件）');
await stopServer();
{
  const buf = fs.readFileSync(walPath);
  // Corrupt a payload byte well inside the file (not in the final frame).
  const victim = Math.floor(buf.length / 2);
  buf[victim] ^= 0xff;
  fs.writeFileSync(walPath, buf);

  const proc = spawn(process.execPath, [path.join(ROOT, 'src', 'server.mjs')], {
    env: {
      ...process.env, CONVERGE_HOST: '127.0.0.1', CONVERGE_PORT: String(PORT),
      CONVERGE_DATA_DIR: DATA, CONVERGE_RECEIPT_SECRET: SECRET, CONVERGE_SETTLE_MS: '20',
    },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  let stderr = '';
  proc.stderr.on('data', (d) => { stderr += d; });
  const code = await new Promise((resolve) => proc.on('exit', (c) => resolve(c)));
  check('corrupt WAL aborts startup', code !== 0, `exit=${code}`);
  check('abort reason surfaced (crc/magic)', /crc mismatch|bad magic/i.test(stderr), stderr.slice(0, 300));

  // Restore good bytes for the summary snapshot at the end isn't needed; data dir is disposable.
}

// --------------------------------------------------------------------------
console.log(`\n结果：${passed} 通过，${failed} 失败；数据目录 ${DATA}`);
await stopServer().catch(() => {});
process.exit(failed ? 1 : 0);
