#!/usr/bin/env node
/**
 * Integration verification: receipts produced by a REAL converge server are
 * ingested by diffdiag without transcoding (same canonical hashing and HMAC
 * secret), compiled into causal skeletons, diffed and proven.
 *
 *   1 converge settles decisions for one object; receipts are downloaded via
 *     the inspector-safe endpoint at two different watermarks (before/after
 *     one more decision)
 *   2 both exports ingest cleanly into two diffdiag groups
 *   3 the comparison finds the first divergence exactly where the second
 *     export continues; identical exports yield an identical verdict
 *   4 tampering with one converge receipt in transit => whole-package reject
 */
import { spawn } from 'node:child_process';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, '..');
const CONVERGE = path.resolve(ROOT, '..', 'converge');

let passed = 0;
let failed = 0;
function check(name, cond, detail = '') {
  if (cond) { passed++; console.log(`  ✓ ${name}`); }
  else { failed++; console.error(`  ✗ ${name} ${detail}`); }
}

const CPORT = 49600 + Math.floor(Math.random() * 200);
const DPORT = CPORT + 100;
const CDIR = fs.mkdtempSync(path.join(os.tmpdir(), 'converge-live-'));
const DDIR = fs.mkdtempSync(path.join(os.tmpdir(), 'diffdiag-live-'));
const SECRET = 'live-integration-secret';
const procs = [];

function start(file, env) {
  const p = spawn(process.execPath, [file], {
    env: { ...process.env, ...env },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  p.stderr.on('data', (d) => process.stderr.write(d));
  procs.push(p);
  return p;
}
async function waitHealth(url) {
  for (let i = 0; i < 120; i++) {
    try { const r = await fetch(url); if (r.ok) return; } catch { /* retry */ }
    await new Promise((r) => setTimeout(r, 60));
  }
  throw new Error('server did not start: ' + url);
}
const J = { 'Content-Type': 'application/json' };
const OP = { Authorization: 'Bearer operator-token', ...J };
const AN = { Authorization: 'Bearer analyst-token', ...J };
const OB = { Authorization: 'Bearer observer-token', ...J };
const INSP = { Authorization: 'Bearer inspector-token' };
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const hex64 = (s) => s.replace(/[^0-9a-f]/g, 'a').padEnd(64, '0').slice(0, 64);

async function main() {
  start(path.join(CONVERGE, 'src', 'server.mjs'), {
    CONVERGE_PORT: String(CPORT),
    CONVERGE_DATA_DIR: CDIR,
    CONVERGE_RECEIPT_SECRET: SECRET,
    CONVERGE_SETTLE_MS: '30',
  });
  start(path.join(ROOT, 'src', 'server.mjs'), {
    DIFFDIAG_PORT: String(DPORT),
    DIFFDIAG_DATA_DIR: DDIR,
    DIFFDIAG_RECEIPT_SECRET: SECRET,
    DIFFDIAG_WORKER_INTERVAL_MS: '0',
  });
  await waitHealth(`http://127.0.0.1:${CPORT}/healthz`);
  await waitHealth(`http://127.0.0.1:${DPORT}/healthz`);

  // converge: three sequential decisions on obj-live
  async function envelope(i, change, preds) {
    const r = await fetch(`http://127.0.0.1:${CPORT}/api/envelopes`, {
      method: 'POST',
      headers: OP,
      body: JSON.stringify({
        object_key: 'obj-live',
        site: 'station-live',
        site_seq: i,
        coverage_region: 'tiles:001:001',
        change_digest: hex64(change),
        predecessor_digests: preds,
        request_key: `rk-live-${i}`,
      }),
    });
    if (!r.ok) throw new Error('envelope failed: ' + (await r.text()));
    await sleep(150);
    const o = await (await fetch(`http://127.0.0.1:${CPORT}/api/objects/obj-live`, { headers: OP })).json();
    return o.current.decision_node;
  }
  const d1 = await envelope(1, 'change-1', []);
  const d2 = await envelope(2, 'change-2', [d1]);

  async function download() {
    const r = await fetch(`http://127.0.0.1:${CPORT}/api/objects/obj-live/receipts/download`, { headers: INSP });
    if (!r.ok) throw new Error('download failed: ' + r.status);
    return r.json();
  }
  const exportV1 = await download(); // watermark: 2 decisions
  const d3 = await envelope(3, 'change-3', [d2]);
  const exportV2 = await download(); // watermark: 3 decisions
  check('converge 产出两批收据', exportV1.receipts.length === 2 && exportV2.receipts.length === 3);

  // diffdiag: ingest both exports as two groups
  async function ingest(group, pack) {
    const r = await fetch(`http://127.0.0.1:${DPORT}/api/packages`, {
      method: 'POST',
      headers: OP,
      body: JSON.stringify({ group, object_key: pack.object_key, receipts: pack.receipts }),
    });
    return { status: r.status, json: await r.json() };
  }
  let r = await ingest('export-v1', exportV1);
  check('第一批入库', r.status === 200 && r.json.accepted === 2, JSON.stringify(r.json));
  r = await ingest('export-v2', exportV2);
  check('第二批入库', r.status === 200 && r.json.accepted === 3, JSON.stringify(r.json));

  // comparison: v1 export vs v2 export => divergence where v2 continues
  r = await fetch(`http://127.0.0.1:${DPORT}/api/comparisons`, {
    method: 'POST', headers: AN,
    body: JSON.stringify({ task_key: 'live-watermark', left_group: 'export-v1', right_group: 'export-v2' }),
  });
  check('创建比较', r.status === 200);
  await fetch(`http://127.0.0.1:${DPORT}/api/worker/run`, { method: 'POST', headers: OP });
  r = await fetch(`http://127.0.0.1:${DPORT}/api/comparisons/live-watermark/diagnostic`, { headers: AN });
  const diag = await r.json();
  check('第一处分歧在第三批到达处', diag.divergence && diag.divergence.seq === 3
    && diag.divergence.left === null && diag.divergence.right !== null, JSON.stringify(diag.divergence));
  check('下游节点为新到达决定', diag.affected.length === 1 && diag.affected[0].side === 'right');
  r = await fetch(`http://127.0.0.1:${DPORT}/api/proofs/verify`, {
    method: 'POST', headers: OB, body: JSON.stringify({ task_key: 'live-watermark' }),
  });
  check('证明可验证', (await r.json()).ok === true);

  // identical exports => identical verdict
  r = await fetch(`http://127.0.0.1:${DPORT}/api/comparisons`, {
    method: 'POST', headers: AN,
    body: JSON.stringify({ task_key: 'live-same', left_group: 'export-v2', right_group: 'export-v2' }),
  });
  await fetch(`http://127.0.0.1:${DPORT}/api/worker/run`, { method: 'POST', headers: OP });
  r = await fetch(`http://127.0.0.1:${DPORT}/api/comparisons/live-same/diagnostic`, { headers: AN });
  check('同组对比一致', (await r.json()).divergence === null);

  // tamper one converge receipt in transit => whole package rejected
  const tampered = JSON.parse(JSON.stringify(exportV2));
  tampered.receipts[1].decision_kind = 'merge';
  r = await ingest('export-tampered', tampered);
  check('传输中篡改收据整包拒绝', r.status === 422 && r.json.error.code === 'RECEIPT_HASH_MISMATCH', JSON.stringify(r.json));
  const tamperedSig = JSON.parse(JSON.stringify(exportV2));
  tamperedSig.receipts[0].sig = '0'.repeat(64);
  r = await ingest('export-tampered2', tamperedSig);
  check('签名被换整包拒绝', r.status === 422 && r.json.error.code === 'RECEIPT_SIG_INVALID', JSON.stringify(r.json));

  console.log(`\n${passed} passed, ${failed} failed`);
  for (const p of procs) p.kill('SIGTERM');
  process.exit(failed ? 1 : 0);
}

main().catch((e) => {
  console.error(e);
  for (const p of procs) p.kill('SIGKILL');
  process.exit(1);
});
