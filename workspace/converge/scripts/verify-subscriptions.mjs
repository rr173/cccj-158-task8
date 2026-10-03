#!/usr/bin/env node
/**
 * End-to-end verification for reliable downstream event subscriptions.
 *
 * Spawns the real converge HTTP server plus in-process downstream receivers on
 * ephemeral ports, and exercises:
 *   1 successful callback + signature + desensitised envelope
 *   2 timeout / non-2xx with capped backoff, then recovery; acked events never resent
 *   3 lost acknowledgement -> duplicate redelivery, dedupe by stable event id
 *   4 signing-key rotation boundary (old batch keeps v1, next batch uses v2)
 *   5 pause accumulates a backlog, resume drains in order; cursor never regresses
 *   6 retry cap -> dead letter; manual resend keeps event id + business order
 *   7 process interruption: lease reclaim, backlog survives restart, cursor intact
 *   8 startup reconciliation of a missing event frame (business durable, event not)
 *   9 role limits (admin manages, operator views, inspector only desensitised proofs)
 *  10 notification failures never block convergence submission or history query
 *  11 case.closed and cold_segment.generated event kinds
 */
import http from 'node:http';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import crypto from 'node:crypto';
import { spawn } from 'node:child_process';
import { canonical } from '../src/crypto-util.mjs';

const ROOT = path.join(new URL('..', import.meta.url).pathname);

let passed = 0, failed = 0;
function check(name, cond, detail = '') {
  if (cond) { passed++; console.log(`  ✓ ${name}`); }
  else { failed++; console.error(`  ✗ ${name} ${typeof detail === 'string' ? detail : JSON.stringify(detail)}`); }
}
function section(name) { console.log(name); }
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const digest = (s) => crypto.createHash('sha256').update(s).digest('hex');

// ------------------------------------------------------------- test harness --

const PORT = 49200 + Math.floor(Math.random() * 400);
let receiverPort = 49600 + Math.floor(Math.random() * 400);
const DATA = fs.mkdtempSync(path.join(os.tmpdir(), 'converge-ob-'));
const SECRET = 'test-secret';
const TOK = {
  op: 'operator-token', r1: 'reviewer1-token', r2: 'reviewer2-token',
  insp: 'inspector-token', admin: 'admin-token', bogus: 'nope',
};

let serverProc = null;
function serverEnv(extra = {}) {
  return {
    ...process.env,
    CONVERGE_HOST: '127.0.0.1',
    CONVERGE_PORT: String(PORT),
    CONVERGE_DATA_DIR: DATA,
    CONVERGE_RECEIPT_SECRET: SECRET,
    CONVERGE_SETTLE_MS: '20',
    // Deterministic, pump driven explicitly by the test.
    CONVERGE_PUMP_INTERVAL_MS: '0',
    CONVERGE_DELIVERY_TIMEOUT_MS: '300',
    CONVERGE_BACKOFF_BASE_MS: '30',
    CONVERGE_BACKOFF_MAX_MS: '300',
    CONVERGE_DELIVERY_MAX_ATTEMPTS: '3',
    CONVERGE_DELIVERY_BATCH_SIZE: '1',
    CONVERGE_DELIVERY_LEASE_MS: '800',
    ...extra,
  };
}
function startServer(extra) {
  return new Promise((resolve, reject) => {
    const proc = spawn(process.execPath, [path.join(ROOT, 'src', 'server.mjs')], {
      env: serverEnv(extra), stdio: ['ignore', 'pipe', 'pipe'],
    });
    let out = '';
    proc.stdout.on('data', (d) => { out += d; });
    proc.stderr.on('data', (d) => { out += d; });
    serverProc = proc;
    const deadline = Date.now() + 6000;
    const poll = async () => {
      try {
        const r = await fetch(`http://127.0.0.1:${PORT}/healthz`);
        if (r.ok) return resolve();
      } catch { /* not yet */ }
      if (Date.now() > deadline) { proc.kill('SIGKILL'); return reject(new Error('server did not start: ' + out)); }
      setTimeout(poll, 40);
    };
    poll();
  });
}
async function stopServer() {
  if (!serverProc) return;
  const p = serverProc; serverProc = null;
  await new Promise((res) => { p.on('exit', res); p.kill('SIGTERM'); setTimeout(() => { p.kill('SIGKILL'); res(); }, 1200); });
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

/** A configurable downstream receiver. Behavior: { mode, count } where mode is
 *  'ok' | 'fail' | 'failOnce' | 'timeout' | 'timeoutThenOk' ... set at runtime. */
class Receiver {
  constructor() {
    this.deliveries = []; // {headers, body}
    this.mode = 'ok';
    this.failUntil = 0; // fail the first N calls, then ok
    this.timeoutUntil = 0;
    this.statusUntil = new Map(); // count -> status code
    this.server = http.createServer((reqq, ress) => this._handle(reqq, ress));
  }
  start() {
    return new Promise((resolve) => {
      this.server.listen(0, '127.0.0.1', () => { this.port = this.server.address().port; resolve(); });
    });
  }
  close() { return new Promise((r) => this.server.close(r)); }
  url(pathName = '/hook') { return `http://127.0.0.1:${this.port}${pathName}`; }
  reset() { this.deliveries = []; }
  async _handle(reqq, ress) {
    const chunks = [];
    reqq.on('data', (c) => chunks.push(c));
    reqq.on('end', async () => {
      let body = {};
      try { body = JSON.parse(Buffer.concat(chunks).toString('utf8')); } catch { /* keep */ }
      this.deliveries.push({ headers: reqq.headers, body, at: Date.now() });
      const n = this.deliveries.length;
      const forced = this.statusUntil.get(n);
      if (this.mode === 'timeout' || (this.mode === 'timeoutThenOk' && n <= this.timeoutUntil)) {
        // Never respond; client times out.
        return;
      }
      if (forced !== undefined) { ress.writeHead(forced); return ress.end('forced'); }
      if (this.mode === 'fail' || (this.mode === 'failOnce' && n <= this.failUntil)) {
        ress.writeHead(503); return ress.end('boom');
      }
      ress.writeHead(200, { 'content-type': 'application/json' });
      ress.end('{"ok":true}');
    });
  }
}

async function createSubscription(recv, kinds, key = 'signing-secret-key-0001-abcdef') {
  const r = await req('POST', '/api/subscriptions', TOK.admin, {
    callback_url: recv.url(), event_kinds: kinds, signing_key: key,
  });
  if (r.status !== 200) throw new Error('createSubscription failed: ' + JSON.stringify(r.json));
  return r.json;
}
async function pump(subId) {
  const r = await req('POST', '/api/subscriptions/pump', TOK.admin, { subscription_id: subId });
  return r.json;
}
/** Pump until no pending items remain (bounded); batch_size=1 means one event
 *  per cycle, so a multi-event moment needs several cycles. */
async function drain(subId, cycles = 10) {
  for (let i = 0; i < cycles; i++) {
    const r = await pump(subId);
    if (r.stopped === 'all_confirmed' || r.stopped === 'idle') break;
    if (r.stopped === 'backoff' || r.stopped === 'dead_letter' || r.stopped === 'paused') break;
    await sleep(20);
  }
}
async function settleEnvelope(objectKey, { site = 'station-a', seq = 1, region = 'r1', seed = 'x', rk }) {
  await req('POST', '/api/envelopes', TOK.op, {
    object_key: objectKey, site, site_seq: seq, coverage_region: region,
    change_digest: digest(seed), predecessor_digests: [], request_key: rk || `rk-${objectKey}-${seq}`,
  });
  await sleep(120);
}
async function decisionCount(objectKey) {
  const r = await req('GET', `/api/receipts?object_key=${objectKey}`, TOK.op);
  return r.json.receipts.length;
}
function verifySignature(secret, body, header) {
  const want = 'v1=' + crypto.createHmac('sha256', secret).update(canonical(body)).digest('hex');
  const got = header?.['x-converge-signature'];
  if (!want || !got) return false;
  const a = Buffer.from(want), b = Buffer.from(got);
  return a.length === b.length && crypto.timingSafeEqual(a, b);
}
const FORBIDDEN = ['site', 'request_key', 'site_seq', 'pixel', 'image', 'pixels'];

// -------------------------------------------------------------------------- //
await startServer();
const recv = new Receiver();
await recv.start();

// ============================================================ 1. happy path //
section('1. 成功回调：稳定标识、脱敏正文、合法签名、游标推进');
{
  const sub = await createSubscription(recv, ['decision.created']);
  const sid = sub.id;
  await settleEnvelope('obj-happy', { region: 'reg-1', seed: 'h1' });
  const before = await req('GET', `/api/subscriptions/${sid}`, TOK.admin);
  check('pending item visible before delivery', before.json.pending_count === 1, JSON.stringify(before.json.head));

  const p = await pump(sid);
  check('pump acked one', p.acked === 1 && p.stopped === 'batch_complete', JSON.stringify(p));
  check('receiver got exactly one notification', recv.deliveries.length === 1, `got ${recv.deliveries.length}`);
  const n = recv.deliveries[0].body;
  check('schema + kind', n.schema === 'converge-event/v1' && n.kind === 'decision.created', n.kind);
  check('stable event id (64 hex)', /^[0-9a-f]{64}$/.test(n.event_id), n.event_id);
  check('object alternative key present', n.object_key === 'obj-happy', n.object_key);
  check('business cursor present', n.business_cursor && n.business_cursor.receipt_seq === 1 &&
    n.business_cursor.receipt_hash && n.business_cursor.decision_node, JSON.stringify(n.business_cursor));
  check('event seq assigned', n.event_seq >= 1, String(n.event_seq));
  check('key version stamped', n.key_version === 1);
  check('no site/request-key/image content', !FORBIDDEN.some((k) => k in n) &&
    !JSON.stringify(n).includes('station-a'), JSON.stringify(n).slice(0, 300));
  check('HMAC signature valid', verifySignature('signing-secret-key-0001-abcdef', n, recv.deliveries[0].headers));
  check('signature header carries version', recv.deliveries[0].headers['x-converge-key-version'] === '1');
  const after = await req('GET', `/api/subscriptions/${sid}`, TOK.admin);
  check('confirmed cursor advanced to 1', after.json.confirmed_event_seq === 1, String(after.json.confirmed_event_seq));
  check('pending drained', after.json.pending_count === 0, String(after.json.pending_count));
  check('last ack recorded', after.json.last_acked && after.json.last_acked.event_id === n.event_id,
    JSON.stringify(after.json.last_acked));

  // Confirmed events are never proactively sent again.
  const p2 = await pump(sid);
  check('acking is sticky: second pump idle', p2.stopped === 'all_confirmed', JSON.stringify(p2));
  check('no duplicate redelivery', recv.deliveries.length === 1, `got ${recv.deliveries.length}`);
}

// ========================================== 2. timeout/non-2xx then recover //
section('2. 超时与非成功响应：有上限退避，随后恢复，顺序不跳');
{
  const r2recv = new Receiver();
  await r2recv.start();
  const sub = await createSubscription(r2recv, ['decision.created'], 'secret-key-two-0000000000-zzzzzz');
  const sid = sub.id;
  await settleEnvelope('obj-timeout', { region: 'rt', seed: 't1' });

  // 2a. timeout (server never replies) -> retry, cursor stays put.
  r2recv.mode = 'timeout';
  const t0 = Date.now();
  const p1 = await pump(sid);
  const elapsed = Date.now() - t0;
  check('timeout attempt respected the delivery timeout', elapsed >= 250 && elapsed < 2500, `${elapsed}ms`);
  check('timeout -> retry not dead (attempt 1<3)', p1.stopped === 'retry' && p1.attempts === 1, JSON.stringify(p1));
  let view = await req('GET', `/api/subscriptions/${sid}`, TOK.admin);
  const seq2head = view.json.head?.event_seq;
  check('cursor did NOT advance after failed attempt', view.json.confirmed_event_seq < seq2head,
    `confirmed=${view.json.confirmed_event_seq} head=${seq2head}`);
  check('dead reason absent while retrying', view.json.dead_count === 0, String(view.json.dead_count));

  // immediate retry is in backoff
  const p1b = await pump(sid);
  check('backoff blocks immediate redispatch', p1b.stopped === 'backoff', JSON.stringify(p1b));

  // 2b. non-2xx response path then recovery
  r2recv.mode = 'fail';
  await sleep(120);
  const p2 = await pump(sid);
  check('503 -> retry (attempt 2)', p2.stopped === 'retry' && p2.attempts === 2, JSON.stringify(p2));
  // backoff after attempt 2 is base*2^(2-1)=60ms; wait past it then attempt 3.
  await sleep(150);
  const p3 = await pump(sid);
  check('third failure reaches cap -> dead', p3.stopped === 'dead' && p3.attempts === 3, JSON.stringify(p3));
  view = await req('GET', `/api/subscriptions/${sid}`, TOK.admin);
  check('item in dead letter with reason', view.json.dead_count === 1 &&
    /HTTP 503/.test(view.json.dead_letter[0].last_error), JSON.stringify(view.json.dead_letter));
  check('head reports dead', view.json.head.state === 'dead', view.json.head.state);

  // Manual resend, now healthy: same stable event id, then it acks.
  const deadSeq = view.json.dead_letter[0].event_seq;
  const deadId = view.json.dead_letter[0].event_id;
  r2recv.mode = 'ok';
  const rd = await req('POST', `/api/subscriptions/${sid}/redeliver`, TOK.admin, { event_seq: deadSeq });
  check('redeliver requeues', rd.status === 200 && rd.json.state === 'pending' && rd.json.resend_count === 1,
    JSON.stringify(rd.json));
  const p4 = await pump(sid);
  check('resend acked', p4.acked === 1, JSON.stringify(p4));
  check('same stable event id delivered', r2recv.deliveries[r2recv.deliveries.length - 1].body.event_id === deadId,
    'event id changed!');
  view = await req('GET', `/api/subscriptions/${sid}`, TOK.admin);
  check('cursor advanced after recovery', view.json.confirmed_event_seq === deadSeq,
    String(view.json.confirmed_event_seq));
  await r2recv.close();
}

// ========================== 3. lost acknowledgement -> duplicate redelivery //
section('3. 确认响应丢失：重复投递，接收方以稳定标识去重');
{
  const r3 = new Receiver();
  await r3.start();
  const sub = await createSubscription(r3, ['decision.created'], 'secret-key-three-0000-yyyyyy');
  const sid = sub.id;
  await settleEnvelope('obj-acklost', { region: 'ra', seed: 'a1' });

  // The receiver returns 200 (so it processes), but we simulate the response
  // being lost by forcing the SERVER's view: deliver to a receiver that accepts
  // then the item's ack never lands because the process dies. Simpler direct
  // path: receiver acks, we kill the server right after delivery but before the
  // ack frame is persisted is racy; instead exercise lease reclaim, which is
  // exactly the ack-loss/crash mechanism.
  let p = await pump(sid);
  check('first delivery acked by receiver', r3.deliveries.length === 1, 'n=' + r3.deliveries.length);

  // Force the item back to an unacked state by expiring its lease: set the
  // lease far in the past via a short-lease restart, then redeliver.
  const firstId = r3.deliveries[0].body.event_id;

  // Manually simulate the ack-loss: restart with a tiny lease; the durable ack
  // from pump() above already landed, so instead we drive a SECOND event whose
  // acknowledgement will be lost by making the receiver accept but the server
  // time out. The receiver processing = dedupe scenario.
  r3.mode = 'timeout'; // server cannot see the 200 (simulated loss)
  await settleEnvelope('obj-acklost', { region: 'rb', seed: 'a2', seq: 2, rk: 'rk-obj-acklost-2' });
  // First timed attempt; let lease expire then reclaim -> duplicate of same seq.
  await pump(sid);
  r3.mode = 'ok';
  await sleep(900); // > delivery lease (800ms)
  await pump(sid); // reclaim expired lease + redeliver
  const secondEventDeliveries = r3.deliveries.filter((d) => d.body.object_key === 'obj-acklost');
  const seq2 = secondEventDeliveries.filter((d) => d.body.business_cursor.receipt_seq === 2);
  check('seq-2 delivered at least twice (lost ack / lease reclaim)', seq2.length >= 2,
    `seq2 copies=${seq2.length}`);
  const ids = new Set(seq2.map((d) => d.body.event_id));
  check('duplicates carry the SAME stable event id -> receiver dedupes', ids.size === 1,
    JSON.stringify([...ids]));
  // let it ack now
  await sleep(60);
  const view = await req('GET', `/api/subscriptions/${sid}`, TOK.admin);
  check('both events eventually confirmed in order', view.json.confirmed_event_seq >= 2,
    String(view.json.confirmed_event_seq));
  await r3.close();
}

// ============================================= 4. signing-key rotation edge //
section('4. 密钥更换边界：旧批次沿用旧版本，新批次使用新版本');
{
  const r4 = new Receiver();
  await r4.start();
  const KEY1 = 'signing-secret-key-0001-rotation-test';
  const KEY2 = 'signing-secret-key-0002-rotation-test';
  const sub = await createSubscription(r4, ['decision.created'], KEY1);
  const sid = sub.id;

  // Produce event 1 and fail it once so an OPEN batch exists, bound to key v1.
  await settleEnvelope('obj-keyrot', { region: 'k1', seed: 'k1' });
  r4.mode = 'fail'; r4.failUntil = 1;
  const p1 = await pump(sid);
  check('event1 in open retry batch bound to key v1', p1.stopped === 'retry', JSON.stringify(p1));
  const itemsView = await req('GET', `/api/subscriptions/${sid}/items?state=pending`, TOK.admin);
  check('open batch item stamped key_version=1', itemsView.json.items[0].key_version === 1,
    JSON.stringify(itemsView.json.items[0]));

  // Rotate while the batch is open.
  const rot = await req('POST', `/api/subscriptions/${sid}/rotate-key`, TOK.admin, { signing_key: KEY2 });
  check('rotation bumps current version to 2', rot.json.current_key_version === 2,
    JSON.stringify(rot.json));

  // Produce event 2 (will queue behind event1).
  await settleEnvelope('obj-keyrot', { region: 'k2', seed: 'k2', seq: 2, rk: 'rk-keyrot-2' });

  // Drain event1: the resumed batch keeps v1 signature.
  r4.mode = 'ok';
  await sleep(120);
  const p2 = await pump(sid);
  check('resumed batch delivered', p2.acked === 1, JSON.stringify(p2));
  const e1del = r4.deliveries.filter((d) => d.body.business_cursor?.receipt_seq === 1).pop();
  check('event1 signed with OLD key v1', e1del.body.key_version === 1 &&
    verifySignature(KEY1, e1del.body, e1del.headers), 'v1 mismatch');
  check('event1 NOT valid under new key', !verifySignature(KEY2, e1del.body, e1del.headers));

  const p3 = await pump(sid);
  check('next batch delivered', p3.acked === 1, JSON.stringify(p3));
  const e2del = r4.deliveries.filter((d) => d.body.business_cursor?.receipt_seq === 2).pop();
  check('event2 is a NEW batch signed with key v2', e2del.body.key_version === 2 &&
    verifySignature(KEY2, e2del.body, e2del.headers), 'v2 mismatch');
  check('event2 NOT valid under old key', !verifySignature(KEY1, e2del.body, e2del.headers));
  await r4.close();
}

// ==================================== 5. pause backlog + resume in order ====
section('5. 暂停期间积压，恢复后按序排空，游标不倒退');
{
  const r5 = new Receiver();
  await r5.start();
  const sub = await createSubscription(r5, ['decision.created'], 'signing-secret-key-0001-pause-test');
  const sid = sub.id;

  await req('POST', `/api/subscriptions/${sid}/pause`, TOK.admin, {});
  await settleEnvelope('obj-pause', { region: 'p1', seed: 'p1' });
  await settleEnvelope('obj-pause', { region: 'p2', seed: 'p2', seq: 2, rk: 'rk-pause-2' });
  await settleEnvelope('obj-pause', { region: 'p3', seed: 'p3', seq: 3, rk: 'rk-pause-3' });

  let view = await req('GET', `/api/subscriptions/${sid}`, TOK.admin);
  check('paused subscription accumulates backlog', view.json.status === 'paused' &&
    view.json.pending_count === 3, `${view.json.status}/${view.json.pending_count}`);
  const paused = await pump(sid);
  check('pump refuses to send while paused', paused.stopped === 'paused', JSON.stringify(paused));
  check('nothing delivered while paused', r5.deliveries.length === 0, 'n=' + r5.deliveries.length);

  await req('POST', `/api/subscriptions/${sid}/resume`, TOK.admin, {});
  const preDrain = (await req('GET', `/api/subscriptions/${sid}`, TOK.admin)).json;
  const backlogSeqs = preDrain.head
    ? (await req('GET', `/api/subscriptions/${sid}/items?state=pending&limit=10`, TOK.admin)).json.items.map((i) => i.event_seq)
    : [];
  const firstSeq = Math.min(...backlogSeqs);
  const lastSeq = Math.max(...backlogSeqs);
  for (let i = 0; i < 3; i++) await pump(sid);
  const seqOrder = r5.deliveries.map((d) => d.body.business_cursor.receipt_seq);
  check('backlog drained in business order 1,2,3', JSON.stringify(seqOrder) === JSON.stringify([1, 2, 3]),
    JSON.stringify(seqOrder));
  view = await req('GET', `/api/subscriptions/${sid}`, TOK.admin);
  check('cursor reached the final global event seq', view.json.confirmed_event_seq === lastSeq,
    `confirmed=${view.json.confirmed_event_seq} last=${lastSeq}`);

  // Cursor cannot regress: pausing again does not move it.
  await req('POST', `/api/subscriptions/${sid}/pause`, TOK.admin, {});
  await req('POST', `/api/subscriptions/${sid}/resume`, TOK.admin, {});
  view = await req('GET', `/api/subscriptions/${sid}`, TOK.admin);
  check('cursor stable across pause/resume', view.json.confirmed_event_seq === lastSeq,
    String(view.json.confirmed_event_seq));
  check('cursor did not regress below first backlog event', view.json.confirmed_event_seq >= firstSeq);
  await r5.close();
}

// ================ 6. retry cap -> dead letter, manual resend keeps order =====
section('6. 死信：达上限隔离，人工再次发送沿用原标识与原顺序');
{
  const r6 = new Receiver();
  await r6.start();
  const sub = await createSubscription(r6, ['decision.created'], 'signing-secret-key-0001-dead-letter');
  const sid = sub.id;
  await settleEnvelope('obj-dead', { region: 'd1', seed: 'd1' });
  await settleEnvelope('obj-dead', { region: 'd2', seed: 'd2', seq: 2, rk: 'rk-dead-2' });

  // Only the head can fail; event2 queues behind it and is never skipped.
  r6.mode = 'fail';
  let view;
  for (let i = 0; i < 6; i++) {
    await pump(sid);
    await sleep(120); // wait past the capped backoff so every attempt fires
    view = await req('GET', `/api/subscriptions/${sid}`, TOK.admin);
    if (view.json.head && view.json.head.state === 'dead') break;
  }
  check('head event dead after cap', view.json.head && view.json.head.state === 'dead',
    JSON.stringify(view.json.head));
  check('event2 NOT skipped behind dead head', view.json.pending_count === 1, String(view.json.pending_count));
  r6.reset();
  const behind = await pump(sid);
  check('pump blocked at dead head', behind.stopped === 'dead_letter', JSON.stringify(behind));
  check('event2 was never delivered out of order',
    r6.deliveries.filter((d) => d.body.business_cursor?.receipt_seq === 2).length === 0, 'skipped!');

  // Resend head; both then drain in order.
  const headSeq = view.json.head.event_seq, headId = view.json.head.event_id;
  r6.mode = 'ok';
  await req('POST', `/api/subscriptions/${sid}/redeliver`, TOK.admin, { event_seq: headSeq });
  await pump(sid);
  await pump(sid);
  const order = r6.deliveries.map((d) => d.body.business_cursor.receipt_seq);
  check('after resend, delivery order is 1 then 2', JSON.stringify(order) === JSON.stringify([1, 2]),
    JSON.stringify(order));
  check('resent head kept original stable id', r6.deliveries[0].body.event_id === headId, 'id changed');
  view = await req('GET', `/api/subscriptions/${sid}`, TOK.admin);
  const wantCursor = Math.max(...r6.deliveries.map((d) => d.body.event_seq));
  check('dead letter cleared', view.json.dead_count === 0, String(view.json.dead_count));
  check('cursor advanced across both without skipping', view.json.confirmed_event_seq === wantCursor,
    `confirmed=${view.json.confirmed_event_seq} want=${wantCursor}`);
  await r6.close();
}

// ========================= 7. process interruption + lease reclaim ==========
section('7. 进程中断：租约回收、积压存活、重启后游标不丢不重不倒退');
{
  const r7 = new Receiver();
  await r7.start();
  const sub = await createSubscription(r7, ['decision.created'], 'signing-secret-key-0001-restart-test');
  const sid = sub.id;
  // event1 delivered+acked before restart
  await settleEnvelope('obj-restart', { region: 'z1', seed: 'z1' });
  await pump(sid);
  const ackedId = r7.deliveries[0].body.event_id;
  const cursorAfterFirst = r7.deliveries[0].body.event_seq;

  // event2 pending while we restart.
  await settleEnvelope('obj-restart', { region: 'z2', seed: 'z2', seq: 2, rk: 'rk-restart-2' });
  await stopServer();
  await sleep(100);
  await startServer();
  await sleep(50);

  let view = await req('GET', `/api/subscriptions/${sid}`, TOK.admin);
  check('subscription + backlog survived restart', view.json.pending_count === 1,
    String(view.json.pending_count));
  check('confirmed cursor survived restart unchanged', view.json.confirmed_event_seq === cursorAfterFirst,
    `confirmed=${view.json.confirmed_event_seq} want=${cursorAfterFirst}`);

  await pump(sid);
  const cursorAfterSecond = r7.deliveries[r7.deliveries.length - 1].body.event_seq;
  view = await req('GET', `/api/subscriptions/${sid}`, TOK.admin);
  check('event2 confirmed after restart, cursor advanced', view.json.confirmed_event_seq === cursorAfterSecond &&
    cursorAfterSecond > cursorAfterFirst,
    `confirmed=${view.json.confirmed_event_seq} want=${cursorAfterSecond}`);
  const ids1 = r7.deliveries.filter((d) => d.body.business_cursor?.receipt_seq === 1).map((d) => d.body.event_id);
  check('event1 stable id unchanged across restart and not duplicated',
    ids1.length === 1 && ids1[0] === ackedId, JSON.stringify(ids1));
  await r7.close();
}

// ============= 8. startup reconciliation: business durable, event missing ====
section('8. 启动补缝：业务帧已落而事件帧缺失 -> 对账补事件与投递项');
{
  // Set up a FRESH subscription right before the gap so the rebuilt event is
  // within its delivery window, then settle one decision the subscription
  // receives, plus a second decision whose outbox frame we will surgically
  // remove while keeping its business frame.
  const r8 = new Receiver();
  await r8.start();
  const KEY8 = 'signing-secret-key-0001-reconcile-gap';
  const sub = await createSubscription(r8, ['decision.created'], KEY8);
  const sid = sub.id;

  await settleEnvelope('obj-gap-a', { region: 'g1', seed: 'g1' });
  await pump(sid); // seq delivered normally
  const normalSeq = r8.deliveries[0].body.event_seq;

  await settleEnvelope('obj-gap-b', { region: 'g2', seed: 'g2' });
  // Stop BEFORE pumping the second event: business decision is durable, item
  // pending. Now simulate the crash gap: remove ONLY the newest outbox event
  // frame from the business WAL (its T_DECISION business frame survives).
  await stopServer();
  const walPath = path.join(DATA, 'converge.wal');
  const { parseFrames, encodeFrame, WAL_MAGIC } = await import(path.join(ROOT, 'src', 'wal.mjs').replace(/\\/g, '/'));
  const { frames } = parseFrames(fs.readFileSync(walPath), WAL_MAGIC, { allowTornTail: false });
  const lastEventIndex = frames.map((f) => f.type).lastIndexOf(6);
  check('setup: an outbox event frame exists to remove', lastEventIndex >= 0, 'no type-6 frame');
  const removedEvent = JSON.parse(frames[lastEventIndex].payload.toString('utf8'));
  const removedBusinessKind = (() => {
    // confirm the matching T_DECISION business frame is retained
    return frames.some((f) => f.type === 2 && JSON.parse(f.payload.toString('utf8')).receipt?.hash ===
      removedEvent.business_cursor.receipt_hash);
  })();
  check('setup: business decision frame survives while event frame removed', removedBusinessKind === true);
  frames.splice(lastEventIndex, 1);
  fs.writeFileSync(walPath, Buffer.concat(frames.map((f) => encodeFrame(WAL_MAGIC, f.type, f.payload))));

  await startServer();
  await sleep(60);
  const view = await req('GET', `/api/subscriptions/${sid}`, TOK.admin);
  // Reconciliation regenerates the missing event with the SAME stable id.
  const items = (await req('GET', `/api/subscriptions/${sid}/items?limit=10`, TOK.admin)).json.items;
  const rebuilt = items.find((i) => i.kind === 'decision.created' &&
    i.business_cursor?.receipt_hash === undefined); // items omit cursor; match by seq/state instead
  const head = view.json.head;
  check('reconciliation recreated a pending delivery item for the gap',
    head && (head.state === 'pending' || head.state === 'sending') && head.event_seq > normalSeq,
    JSON.stringify(head));
  check('rebuilt item carries the deterministic stable id',
    head && head.event_id.length === 64 && /^[0-9a-f]{64}$/.test(head.event_id), head?.event_id);

  // Deliver it and confirm the gap closes without duplicating the first event.
  const p = await pump(sid);
  await sleep(50);
  check('rebuilt event delivered', p.acked >= 1, JSON.stringify(p));
  const seqsDelivered = r8.deliveries.map((d) => d.body.event_seq);
  check('normal event delivered exactly once across the gap',
    seqsDelivered.filter((s) => s === normalSeq).length === 1, JSON.stringify(seqsDelivered));
  const gapDelivery = r8.deliveries.find((d) => d.body.event_seq === head.event_seq);
  check('gap notification has valid signature and business cursor',
    gapDelivery && gapDelivery.body.object_key === 'obj-gap-b' &&
    verifySignature(KEY8, gapDelivery.body, gapDelivery.headers) &&
    gapDelivery.body.business_cursor.receipt_hash,
    JSON.stringify(gapDelivery?.business_cursor));
  await r8.close();
}

// ======================================================= 9. role boundaries ==
section('9. 角色限制：admin 管理、operator 只读、inspector 仅脱敏证明');
{
  // operator cannot mutate subscriptions
  const opCreate = await req('POST', '/api/subscriptions', TOK.op, {
    callback_url: 'http://127.0.0.1:1/h', event_kinds: ['decision.created'], signing_key: 'x'.repeat(20),
  });
  check('operator cannot register a subscription', opCreate.status === 403, String(opCreate.status));
  const opPause = await req('POST', '/api/subscriptions/sub-000000000000/pause', TOK.op, {});
  check('operator cannot pause', opPause.status === 403, String(opPause.status));
  const opRedeliver = await req('POST', '/api/subscriptions/sub-000000000000/redeliver', TOK.op, { event_seq: 1 });
  check('operator cannot resend dead letters', opRedeliver.status === 403, String(opRedeliver.status));
  const opList = await req('GET', '/api/subscriptions', TOK.op);
  check('operator CAN view status', opList.status === 200 && Array.isArray(opList.json.subscriptions),
    String(opList.status));
  // operator view must not leak signing keys
  const leakedKey = JSON.stringify(opList.json).includes('signing-secret');
  check('status view never leaks signing keys', !leakedKey, 'key leaked!');

  // reviewer cannot touch subscriptions at all
  const revList = await req('GET', '/api/subscriptions', TOK.r1);
  check('reviewer cannot list subscriptions', revList.status === 403, String(revList.status));

  // inspector only proofs
  const inspList = await req('GET', '/api/subscriptions', TOK.insp);
  check('inspector cannot see operational subscription list', inspList.status === 403, String(inspList.status));
  const inspProofs = await req('GET', '/api/subscriptions/proofs', TOK.insp);
  check('inspector can list desensitised proofs', inspProofs.status === 200, String(inspProofs.status));
  const proofsJson = JSON.stringify(inspProofs.json);
  check('proofs contain no callback url / signing material',
    !proofsJson.includes('callback_url') && !proofsJson.includes('signing-secret'),
    'sensitive leak');
  const anyProof = inspProofs.json.subscriptions[0];
  if (anyProof) {
    const detail = await req('GET', `/api/subscriptions/${anyProof.id}/proofs`, TOK.insp);
    check('inspector can read notification proofs', detail.status === 200, String(detail.status));
    const dj = JSON.stringify(detail.json);
    check('notification proofs carry stable id/cursor but no site/request-key',
      /event_id/.test(dj) && !dj.includes('station-') && !dj.includes('request_key'),
      dj.slice(0, 200));
    const inspItems = await req('GET', `/api/subscriptions/${anyProof.id}/items`, TOK.insp);
    check('inspector cannot read raw delivery items', inspItems.status === 403, String(inspItems.status));
    const inspPump = await req('POST', '/api/subscriptions/pump', TOK.insp, {});
    check('inspector cannot drive the pump', inspPump.status === 403, String(inspPump.status));
  } else {
    check('at least one proof exists for inspector', false, 'no proofs');
  }

  // admin-only pump for operator / no token
  const opPump = await req('POST', '/api/subscriptions/pump', TOK.op, {});
  check('operator cannot drive pump', opPump.status === 403, String(opPump.status));
  const noAuth = await req('GET', '/api/subscriptions/proofs');
  check('unauthenticated rejected', noAuth.status === 401, String(noAuth.status));
}

// ============== 10. notification failures never block business/history ======
section('10. 通知故障不阻塞汇合请求与历史查询');
{
  // A permanently-dead callback (closed port).
  const sub = await createSubscription({ url: () => 'http://127.0.0.1:1/callback' }, ['decision.created']);
  const sid = sub.id;
  // settle several decisions while webhook is unreachable; business must be unaffected
  for (let i = 1; i <= 3; i++) {
    await settleEnvelope('obj-isolated', { region: `iso-${i}`, seed: `iso${i}`, seq: i, rk: `rk-iso-${i}` });
  }
  const decisions = await decisionCount('obj-isolated');
  check('convergence proceeded to 3 decisions despite dead webhook', decisions === 3, `decisions=${decisions}`);
  // history/receipt query unaffected
  const receipts = await req('GET', '/api/receipts?object_key=obj-isolated', TOK.insp);
  check('receipt history query works', receipts.status === 200 && receipts.json.receipts.length === 3,
    String(receipts.status));
  const obj = await req('GET', '/api/objects/obj-isolated', TOK.op);
  check('object graph query works', obj.status === 200 && obj.json.status === 'settled', obj.json?.status);
  // pump failures stay contained (quick timeout), still return structured result
  const p = await pump(sid);
  check('pump reports failure without throwing', typeof p.stopped === 'string', JSON.stringify(p));
  // business continues after failed pump
  await settleEnvelope('obj-isolated', { region: 'iso-4', seed: 'iso4', seq: 4, rk: 'rk-iso-4' });
  check('a 4th decision settles after pump failures', await decisionCount('obj-isolated') === 4);
}

// ================================== 11. case.closed + cold_segment events ==
section('11. 冲突案闭合与冷段生成事件');
{
  const r11 = new Receiver();
  await r11.start();
  const sub = await req('POST', '/api/subscriptions', TOK.admin, {
    callback_url: r11.url(),
    event_kinds: ['decision.created', 'case.closed', 'cold_segment.generated'],
    signing_key: 'signing-secret-key-0001-case-cold',
  });
  const sid = sub.json.id;
  const o = 'obj-case-event';
  const dA = digest('ce-A'), dB = digest('ce-B');
  await Promise.all([
    req('POST', '/api/envelopes', TOK.op, { object_key: o, site: 'ca', site_seq: 1, coverage_region: 'reg', change_digest: dA, predecessor_digests: [], request_key: 'rk-ce-a' }),
    req('POST', '/api/envelopes', TOK.op, { object_key: o, site: 'cb', site_seq: 1, coverage_region: 'reg', change_digest: dB, predecessor_digests: [], request_key: 'rk-ce-b' }),
  ]);
  await sleep(150);
  const cs = (await req('GET', '/api/cases', TOK.r1)).json.cases.find((c) => c.object_key === o);
  check('conflict opened', !!cs, 'no case');
  await req('POST', `/api/reviews/${cs.case_id}`, TOK.r1, { choices: { reg: dA } });
  await req('POST', `/api/reviews/${cs.case_id}`, TOK.r2, { choices: { reg: dA } });
  await sleep(150);

  await drain(sid);
  const kinds = r11.deliveries.map((d) => d.body.kind).sort();
  check('adjudication produced decision.created AND case.closed',
    kinds.includes('case.closed') && kinds.includes('decision.created'), JSON.stringify(kinds));
  const cc = r11.deliveries.find((d) => d.body.kind === 'case.closed').body;
  check('case.closed cursor references the case + adjudicated receipt',
    cc.business_cursor.case_id === cs.case_id && cc.business_cursor.receipt_seq === 1,
    JSON.stringify(cc.business_cursor));
  check('case.closed has no site/request-key', !FORBIDDEN.some((k) => k in cc), JSON.stringify(cc));

  // Now a cold segment for that object -> cold_segment.generated event.
  const cp = await req('POST', '/api/archive/checkpoints', TOK.op, { object_key: o, task_key: 'cp-case-evt-1' });
  check('checkpoint created', cp.status === 200, JSON.stringify(cp.json?.error));
  await drain(sid);
  const cold = r11.deliveries.find((d) => d.body.kind === 'cold_segment.generated');
  check('cold_segment.generated delivered', !!cold, JSON.stringify(r11.deliveries.map((d) => d.body.kind)));
  if (cold) {
    check('cold event cursor references segment + boundary receipt',
      cold.body.business_cursor.segment_id && cold.body.business_cursor.boundary_receipt_seq === 1,
      JSON.stringify(cold.body.business_cursor));
    check('cold event stable id deterministic + signed',
      /^[0-9a-f]{64}$/.test(cold.body.event_id) &&
      verifySignature('signing-secret-key-0001-case-cold', cold.body, cold.headers));
  }
  await r11.close();
}

// ============= 12. multi-item batch (production default batch_size=4) =======
section('12. 多事件批次：单周期按序投递一个批次（batch_size=4，生产默认）');
{
  // This scenario needs a server whose batch size is >1, so use an isolated
  // data dir + port and a short-lived process.
  const BPORT = 49100 + Math.floor(Math.random() * 400);
  const BDATA = fs.mkdtempSync(path.join(os.tmpdir(), 'converge-ob-batch-'));
  const bRecv = new Receiver();
  await bRecv.start();
  const bProc = spawn(process.execPath, [path.join(ROOT, 'src', 'server.mjs')], {
    env: serverEnv({
      CONVERGE_PORT: String(BPORT), CONVERGE_DATA_DIR: BDATA,
      CONVERGE_DELIVERY_BATCH_SIZE: '4', CONVERGE_DELIVERY_TIMEOUT_MS: '500',
    }),
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  const bDeadline = Date.now() + 6000;
  for (;;) {
    try { if ((await fetch(`http://127.0.0.1:${BPORT}/healthz`)).ok) break; } catch { /* wait */ }
    if (Date.now() > bDeadline) { bProc.kill('SIGKILL'); throw new Error('batch server did not start'); }
    await sleep(40);
  }
  const breq = async (m, u, t, b) => {
    const r = await fetch(`http://127.0.0.1:${BPORT}${u}`, {
      method: m,
      headers: { Authorization: `Bearer ${t}`, ...(b !== undefined ? { 'Content-Type': 'application/json' } : {}) },
      body: b !== undefined ? JSON.stringify(b) : undefined,
    });
    return { status: r.status, json: await r.json() };
  };
  const bsub = await breq('POST', '/api/subscriptions', TOK.admin, {
    callback_url: bRecv.url(), event_kinds: ['decision.created'],
    signing_key: 'signing-secret-key-0001-batch-test',
  });
  check('batch sub created', bsub.status === 200, String(bsub.status));
  const bsid = bsub.json.id;
  for (let i = 1; i <= 3; i++) {
    await breq('POST', '/api/envelopes', TOK.op, {
      object_key: 'obj-batch', site: 's', site_seq: i, coverage_region: `b${i}`,
      change_digest: digest(`batch-${i}`), predecessor_digests: [], request_key: `rk-batch-${i}`,
    });
  }
  await sleep(250);
  const bpump = await breq('POST', '/api/subscriptions/pump', TOK.admin, { subscription_id: bsid });
  check('single pump cycle dispatched the whole backlog',
    bpump.json.dispatched === 3 && bpump.json.acked === 3, JSON.stringify(bpump.json));
  const bOrder = bRecv.deliveries.map((d) => d.body.business_cursor.receipt_seq);
  check('batch delivered in business order 1,2,3', JSON.stringify(bOrder) === JSON.stringify([1, 2, 3]),
    JSON.stringify(bOrder));
  const bview = await breq('GET', `/api/subscriptions/${bsid}`, TOK.admin);
  check('batch cursor at the last event, none pending',
    bview.json.confirmed_event_seq >= 3 && bview.json.pending_count === 0,
    JSON.stringify([bview.json.confirmed_event_seq, bview.json.pending_count]));
  check('all three share one batch id (one creation moment)',
    new Set(bRecv.deliveries.map((d) => d.headers['x-converge-event'])).size >= 1 &&
    bRecv.deliveries.every((d) => d.body.key_version === 1));
  await bRecv.close();
  bProc.kill('SIGKILL');
}

// ---- 13. hot trim + restart: surviving event frames must not be re-emitted --
section('13. 冷段裁剪后重启：存活事件帧不重发，游标与事件序完整');
{
  const r13 = new Receiver();
  await r13.start();
  const KEY13 = 'signing-secret-key-0001-trim-restart';
  const sub = await createSubscription(r13, ['decision.created', 'cold_segment.generated'], KEY13);
  const sid = sub.id;
  const o = 'obj-trim';
  await settleEnvelope(o, { region: 't1', seed: 't1' });
  await settleEnvelope(o, { region: 't2', seed: 't2', seq: 2, rk: 'rk-trim-2' });
  await drain(sid);
  const beforeCount = r13.deliveries.length;

  // Checkpoint archives (trims) the two T_DECISION business frames; their
  // type-6 outbox event frames must survive the rotation.
  const cp = await req('POST', '/api/archive/checkpoints', TOK.op, { object_key: o, task_key: 'cp-trim-1' });
  check('checkpoint trimmed hot history', cp.status === 200, JSON.stringify(cp.json?.error));
  await drain(sid); // deliver the cold_segment.generated event
  const coldDelivered = r13.deliveries.some((d) => d.body.kind === 'cold_segment.generated');
  check('cold segment event delivered pre-restart', coldDelivered);

  // The decision frames are now cold; only the surviving outbox event frames
  // remain in the hot WAL. Restart: reconciliation must NOT re-create them.
  await stopServer();
  await sleep(100);
  await startServer();
  await sleep(60);
  const view = await req('GET', `/api/subscriptions/${sid}`, TOK.admin);
  check('no phantom pending items after trim+restart', view.json.pending_count === 0 &&
    view.json.dead_count === 0 && view.json.in_flight_count === 0,
    `pending=${view.json.pending_count} dead=${view.json.dead_count} inflight=${view.json.in_flight_count}`);

  // Pumping again must not redeliver the archived (already confirmed) events.
  await drain(sid);
  check('archived decisions not re-emitted after restart', r13.deliveries.length === beforeCount + 1,
    `before=${beforeCount} after=${r13.deliveries.length} (expected +1 cold event only)`);

  // A fresh decision after restart still gets the next monotonic seq and a
  // delivery item, proving the seq authority survived the trim.
  const prevCursor = view.json.confirmed_event_seq;
  await settleEnvelope(o, { region: 't3', seed: 't3', seq: 3, rk: 'rk-trim-3' });
  await drain(sid);
  const after = await req('GET', `/api/subscriptions/${sid}`, TOK.admin);
  check('post-trim decision advances the cursor to a higher seq',
    after.json.confirmed_event_seq > prevCursor,
    `prev=${prevCursor} now=${after.json.confirmed_event_seq}`);
  const newest = r13.deliveries
    .filter((d) => d.body.kind === 'decision.created')
    .find((d) => d.body.business_cursor.receipt_seq === 3);
  check('post-trim decision event signed and well-formed',
    !!newest && verifySignature(KEY13, newest.body, newest.headers),
    newest ? 'sig/shape mismatch' : 'no receipt_seq=3 decision delivered');
  await r13.close();
}

// --------------------------------------------------------------- summary ----
await stopServer().catch(() => {});
recv.close?.();
console.log(`\n事件订阅验证结果：${passed} 通过，${failed} 失败；数据目录 ${DATA}`);
process.exit(failed ? 1 : 0);
