import fs from 'node:fs';
import { Wal } from './wal.mjs';
import { Mutex } from './mutex.mjs';
import { canonical, digestOf, sha256Hex, sign } from './crypto-util.mjs';
import { ArchiveManager, ArchiveError, isFsPermissionError } from './archive.mjs';
import {
  OutboxManager, T_OUTBOX_EVENT, eventIds,
} from './outbox.mjs';

export class ApiError extends Error {
  constructor(status, code, message, details = undefined) {
    super(message);
    this.status = status;
    this.code = code;
    this.details = details;
  }
}

const T_ENVELOPE = 1;
const T_DECISION = 2;
const T_REVIEW = 3;
const T_CHECKPOINT = 4;
const T_RESTORED = 5;

const TOKEN_RE = /^[A-Za-z0-9:._-]{1,128}$/;
const HEX64_RE = /^[0-9a-f]{64}$/;

function nowIso() {
  return new Date().toISOString();
}

/**
 * The convergence store. All public mutating methods take the process-wide mutex;
 * reads are synchronous snapshots taken on the same event-loop turn after a lock
 * acquisition (server reads lock too, so they never observe a half-written state).
 *
 * Hot/cold storage:
 *  - HOT state is rebuilt from the (trimmed) WAL on startup.
 *  - A per-object checkpoint copies the ancestor closure into a read-only,
 *    content-addressed archive segment and appends a T_CHECKPOINT stub to the
 *    WAL; the original frames are then rotated out of the WAL (space reclaim).
 *  - The stub keeps the boundary decision node + receipt and the envelope
 *    idempotency/sequence index, so the current view, idempotency authority and
 *    receipt-chain linkage are byte-identical after compression.
 *  - Cold history is transparently re-readable; a late change that references an
 *    archived ancestor restores that closure through the archive index instead
 *    of treating the digest as a new root.
 */
export class ConvergenceStore {
  constructor(config) {
    this.config = config;
    this.wal = new Wal(config.walPath);
    this.lock = new Mutex();
    this.archive = new ArchiveManager(config);
    this.outbox = new OutboxManager(config, this.wal);

    this.nodes = new Map(); // digest -> node
    this.objects = new Map(); // objectKey -> object state
    this.siteSeqs = new Map(); // `${objectKey}|${site}|${seq}` -> node digest (HOT)
    this.requestIndex = new Map(); // requestKey -> { body_hash, node_digest } (HOT)
    this.receipts = new Map(); // receipt hash -> receipt (HOT)
    this.reviews = new Map(); // caseId -> review state
    this.receiptSeq = new Map(); // objectKey -> last hot seq

    // Cold index rebuilt from T_CHECKPOINT stubs.
    this.checkpoints = new Map(); // objectKey -> latest stub
    this.archivedNodes = new Map(); // node digest -> {object_key, kind, segment_id}
    this.archivedReceipts = new Map(); // receipt hash -> {seq, segment_id, payload_hash}
    this.archivedRequestKeys = new Map(); // requestKey -> {body_hash, node_digest}
    this.archivedSiteSeqs = new Map(); // seqKey -> node digest
    this.archiveTaskKeys = new Map(); // task_key -> checkpoint summary
    this.segmentHealth = new Map(); // segmentId -> {ok,error?}
    this.restoreLog = []; // recent restore events (bounded)

    this._waiters = new Map(); // objectKey -> [resolve]
    this._timers = new Map(); // objectKey -> timeout
    this._settleGen = new Map(); // objectKey -> generation, stale callbacks no-op
  }

  // ---------------------------------------------------------------- opening --

  open() {
    this.wal.open();
    const { events, truncatedBytes } = this.wal.readAll();
    if (truncatedBytes > 0) {
      // Torn tail from a crash mid-write: drop it before appending new frames.
      const total = fs.statSync(this.config.walPath).size;
      this.wal.truncate(total - truncatedBytes);
    }
    // The outbox log is opened before replay so fan-out items survive a crash
    // between a business+event batch commit and the fan-out append.
    this.outbox.open();
    for (const ev of events) this._replayEvent(ev.type, ev.payload);

    this._loadSegments();
    this._recoverCheckpointTasks();
    this._pruneHotToCold();
    this._rebuildDerived();
    this._recoverSettlements();

    // Transactional-outbox reconciliation: deterministically close any
    // "business durable, event missing" gap and fan events out to every
    // subscription. Descriptors are emitted in WAL append order so new event
    // seqs preserve business order.
    this.outbox.reconcile(this._outboxEventDescriptors());
    this.outbox.startPump();
  }

  close() {
    for (const t of this._timers.values()) clearTimeout(t);
    this._timers.clear();
    this.outbox.close();
    this.wal.close();
  }

  // ---------------------------------------------------------------- replay --

  _replayEvent(type, payloadBuf) {
    if (type === T_CHECKPOINT) {
      this._indexCheckpointStub(JSON.parse(payloadBuf.toString('utf8')));
      return;
    }
    if (type === T_RESTORED) {
      const wrap = JSON.parse(payloadBuf.toString('utf8'));
      for (const e of wrap.events || []) this._replay(e.t, Buffer.from(e.p, 'utf8'));
      return;
    }
    if (type === T_OUTBOX_EVENT) {
      // Durable notification log: rebuild global event seq authority.
      this.outbox.indexReplayedEvent(JSON.parse(payloadBuf.toString('utf8')));
      return;
    }
    this._replay(type, payloadBuf);
  }

  _replay(type, payloadBuf) {
    const ev = JSON.parse(payloadBuf.toString('utf8'));
    if (type === T_ENVELOPE) {
      this._indexEnvelope(ev);
    } else if (type === T_DECISION) {
      this.nodes.set(ev.node.digest, ev.node);
      this.receipts.set(ev.receipt.hash, ev.receipt);
      this.receiptSeq.set(ev.receipt.object_key, ev.receipt.seq);
      const obj = this._obj(ev.node.object_key);
      obj.currentNode = ev.node.digest;
      obj.currentReceipt = ev.receipt.hash;
      if (ev.node.decision_kind === 'adjudicate') {
        const cs = this.reviews.get(ev.node.case_id);
        if (cs) cs.resolved = true;
      }
    } else if (type === T_REVIEW) {
      let cs = this.reviews.get(ev.case_id);
      if (!cs) {
        cs = {
          caseId: ev.case_id,
          objectKey: ev.object_key,
          disputed: ev.disputed,
          candidates: ev.candidates,
          parents: ev.parents,
          baseContributions: ev.base_contributions,
          reviewers: [],
          choices: null,
          resolved: false,
        };
        this.reviews.set(ev.case_id, cs);
      }
      cs.reviewers.push({ reviewer_id: ev.reviewer_id, reviewer_name: ev.reviewer_name, choices: ev.choices });
      if (cs.reviewers.length === 1) cs.choices = ev.choices;
    }
  }

  _indexEnvelope(ev) {
    const node = {
      digest: ev.digest,
      kind: 'change',
      object_key: ev.object_key,
      site: ev.site,
      site_seq: ev.site_seq,
      region: ev.coverage_region,
      change_digest: ev.change_digest,
      predecessors: ev.predecessor_digests,
      request_key: ev.request_key,
    };
    this.nodes.set(node.digest, node);
    this.siteSeqs.set(`${node.object_key}|${node.site}|${node.site_seq}`, node.digest);
    this.requestIndex.set(node.request_key, { body_hash: ev.body_hash, node_digest: node.digest });
  }

  // ------------------------------------------------- cold index from stubs --

  _indexCheckpointStub(stub) {
    this.checkpoints.set(stub.object_key, stub);
    for (const n of stub.archived_nodes || []) {
      this.archivedNodes.set(n.digest, { segment_id: n.segment_id, object_key: n.object_key || null });
    }
    for (const r of stub.archived_receipts || []) {
      this.archivedReceipts.set(r.hash, { seq: r.seq, segment_id: r.segment_id, payload_hash: r.payload_hash });
    }
    for (const e of stub.envelope_index || []) {
      this.archivedRequestKeys.set(e.request_key, { body_hash: e.body_hash, node_digest: e.digest });
      this.archivedSiteSeqs.set(`${e.object_key}|${e.site}|${e.site_seq}`, e.digest);
    }
    // The boundary node/receipt stay HOT (they anchor the live view).
    this.nodes.set(stub.boundary_node.digest, stub.boundary_node);
    this.receipts.set(stub.boundary_receipt.hash, stub.boundary_receipt);
    this.receiptSeq.set(stub.object_key, Math.max(this.receiptSeq.get(stub.object_key) || 0, stub.boundary_seq));
  }

  /** Discover segments on disk and record their verification status. */
  _loadSegments() {
    this.archive.ensureDirs();
    for (const f of fs.readdirSync(this.archive.segmentDir)) {
      if (!f.endsWith('.seg')) continue;
      const id = f.slice(0, -4);
      try {
        const { manifest } = this.archive.readSegment(id);
        this.archive.remember(manifest);
        this.segmentHealth.set(id, { ok: true });
      } catch (e) {
        if (e instanceof ArchiveError) this.segmentHealth.set(id, { ok: false, error: e.code });
        else throw e;
      }
    }
  }

  /**
   * Resume-or-rollback a checkpoint interrupted by a process exit.
   *
   *   PLAN     segment written, hot stub not yet durable  -> roll the segment back
   *   VERIFIED stub durable, hot WAL not yet trimmed       -> finish the trim
   *   TRIMMED  marker removal interrupted                 -> finish, drop marker
   *
   * Segments are content-addressed and the trim only removes frames listed in a
   * durable stub, so every phase is idempotent and no frame is ever deleted
   * twice.
   */
  _recoverCheckpointTasks() {
    for (const marker of this.archive.listTaskMarkers()) {
      const { task_id, phase, segment_id, object_key } = marker;
      const stubRefsSegment = [...this.checkpoints.values()].some((s) => s.segment_id === segment_id);
      let health = null;
      try {
        this.archive.readSegment(segment_id);
        health = 'ok';
      } catch (e) {
        if (e instanceof ArchiveError) health = e.code;
      }
      if (phase === 'PLAN' && !stubRefsSegment) {
        // Nothing durable ever referenced this segment: full rollback.
        if (health === 'ok') this._deleteSegment(segment_id);
        this.archive.removeTaskMarker(task_id);
        continue;
      }
      if (health !== 'ok') {
        // Segment is gone/tampered and a stub may reference it: leave state
        // intact, surface via status; never delete hot data on uncertainty.
        this.segmentHealth.set(segment_id, { ok: false, error: health || 'ARCHIVE_MISSING' });
        this.archive.removeTaskMarker(task_id);
        continue;
      }
      // VERIFIED or TRIMMED (or PLAN with a durable stub): complete the trim.
      this._trimHotWAL();
      this.archive.removeTaskMarker(task_id);
    }
    // Clean any leftover segment temp files from crashes mid-rename.
    for (const f of fs.readdirSync(this.archive.segmentDir)) {
      if (f.endsWith('.seg.tmp')) {
        try { fs.unlinkSync(this.archive.segmentPath(f.slice(0, -4))); } catch { /* best effort */ }
      }
    }
  }

  _deleteSegment(segmentId) {
    const p = this.archive.segmentPath(segmentId);
    try {
      fs.chmodSync(p, 0o644);
      fs.unlinkSync(p);
    } catch (e) {
      if (isFsPermissionError(e)) {
        throw new ApiError(409, 'ARCHIVE_PERMISSION', 'cannot remove incomplete archive segment', { segment_id: segmentId });
      }
      if (e.code !== 'ENOENT') throw e;
    }
  }

  /**
   * Remove hot in-memory frames/receipts/bindings now covered by checkpoint
   * stubs. Restored ancestors that a LIVE post-boundary node references stay
   * materialised (see _hotRequiredParents): pruning them would re-suspend that
   * later change after a restart.
   */
  _pruneHotToCold() {
    const boundaryNodes = new Set();
    const boundaryReceipts = new Set();
    for (const stub of this.checkpoints.values()) {
      boundaryNodes.add(stub.boundary_node.digest);
      boundaryReceipts.add(stub.boundary_receipt.hash);
    }
    const requiredParents = this._hotRequiredParents();
    for (const digest of this.archivedNodes.keys()) {
      if (boundaryNodes.has(digest)) continue;
      if (requiredParents.has(digest)) continue; // restored ancestor still in use
      this.nodes.delete(digest);
    }
    for (const hash of this.archivedReceipts.keys()) {
      if (!boundaryReceipts.has(hash)) this.receipts.delete(hash);
    }
    // Bindings of archived envelopes live in the stubs' envelope index.
    for (const e of this._cumulativeEnvelopeIndex()) {
      this.requestIndex.delete(e.request_key);
      this.siteSeqs.delete(`${e.object_key}|${e.site}|${e.site_seq}`);
    }
  }

  _cumulativeEnvelopeIndex() {
    const all = new Map();
    for (const stub of this.checkpoints.values()) {
      for (const e of stub.envelope_index || []) all.set(e.digest, e);
    }
    return [...all.values()];
  }

  /**
   * Parent digests required by LIVE post-boundary nodes. Ancestors that exist
   * only to be referenced by a checkpoint boundary node are cold; but an
   * ancestor restored for a newer change/decision must stay hot or that newer
   * node would suspend again after a restart.
   */
  _hotRequiredParents() {
    const boundaries = new Set([...this.checkpoints.values()].map((s) => s.boundary_node.digest));
    const required = new Set();
    for (const n of this.nodes.values()) {
      if (boundaries.has(n.digest)) continue; // boundary's own ancestry is cold
      if (this.archivedNodes.has(n.digest)) continue; // cold node needs no hot parents
      const parents = n.kind === 'change' ? n.predecessors : n.parents;
      for (const p of parents) required.add(p);
    }
    return required;
  }

  /**
   * Per-object set of frame payload hashes that have a verified cold copy and
   * belong to an object whose checkpoint chain reaches that segment. A frame is
   * never removable just because some OTHER object's segment happens to contain
   * an identical byte string; the object must have its own durable stub chain.
   */
  _coveredFrameHashesByObject() {
    const byObject = new Map(); // objectKey -> Set(payloadHash)
    for (const key of this.checkpoints.keys()) {
      const set = new Set();
      for (const part of this._safeSegmentsForObject(key)) {
        for (const f of part.frames) set.add(sha256Hex(f.payload));
      }
      byObject.set(key, set);
    }
    return byObject;
  }

  /**
   * Rotate the WAL, dropping frames that have a verified cold copy in an
   * archive segment. Coverage is derived from the segment CONTENT (not from a
   * list stored in the stub), scoped per object, so this is safe to re-run
   * after a crash and one object's compression can never delete another's data.
   */
  _trimHotWAL() {
    const coveredByObject = this._coveredFrameHashesByObject();
    const requiredParents = this._hotRequiredParents();
    const { frames } = this.wal.readAllFrames();

    // Only the newest T_CHECKPOINT frame per object survives the rotation.
    const lastStubByObject = new Map();
    for (const f of frames) {
      if (f.type !== T_CHECKPOINT) continue;
      try { lastStubByObject.set(JSON.parse(f.payload.toString('utf8')).object_key, f); } catch { /* malformed: leave alone */ }
    }

    const keep = [];
    for (const f of frames) {
      if (f.type === T_CHECKPOINT) {
        let key = null;
        try { key = JSON.parse(f.payload.toString('utf8')).object_key; } catch { /* keep malformed */ }
        if (key !== null && lastStubByObject.get(key) !== f) continue; // superseded copy
        keep.push({ type: f.type, payload: f.payload });
        continue;
      }
      if (f.type === T_RESTORED) {
        // A restore wrapper is retained while the live graph depends on any
        // node it materialised. Dropping it would make a post-boundary change
        // suspend again after restart (its archived ancestor would vanish).
        let wrap;
        try { wrap = JSON.parse(f.payload.toString('utf8')); } catch { keep.push({ type: f.type, payload: f.payload }); continue; }
        const restoresRequired = (wrap.events || []).some((e) => {
          if (e.t === T_ENVELOPE) return requiredParents.has(JSON.parse(e.p).digest);
          if (e.t === T_DECISION) return requiredParents.has(JSON.parse(e.p).node.digest);
          return false;
        });
        if (!restoresRequired) {
          // Droppable only if every object it touches has its own cold coverage.
          const allCovered = (wrap.object_keys || []).every((k) =>
            (wrap.events || []).every((e) => {
              const set = coveredByObject.get(k);
              if (!set) return false;
              return set.has(sha256Hex(Buffer.from(e.p, 'utf8')));
            }));
          if (allCovered) continue;
        }
        keep.push({ type: f.type, payload: f.payload });
        continue;
      }
      if (f.type === T_OUTBOX_EVENT) {
        // The global notification log + per-subscription seq authority: it is
        // never part of graph/cold ancestry and must survive every hot trim.
        keep.push({ type: f.type, payload: f.payload });
        continue;
      }
      // A plain envelope/decision frame is removable only by its own object.
      let objKey = null;
      try {
        const ev = JSON.parse(f.payload.toString('utf8'));
        objKey = f.type === T_ENVELOPE ? ev.object_key : ev.node?.object_key || null;
      } catch { /* keep unparseable */ objKey = null; }
      if (objKey && coveredByObject.get(objKey)?.has(sha256Hex(f.payload))) continue;
      keep.push({ type: f.type, payload: f.payload });
    }
    this.wal.replaceFrames(keep);
  }

  _rebuildDerived() {
    this.objects.clear();
    for (const node of this.nodes.values()) this._obj(node.object_key);
    // Restore each object's current-decision pointers from its highest-seq
    // receipt (the object map was just recreated empty during WAL replay).
    for (const obj of this.objects.values()) {
      let latest = null;
      for (const r of this.receipts.values()) {
        if (r.object_key === obj.key && (!latest || r.seq > latest.seq)) latest = r;
      }
      // Cold receipts can only be older than the hot boundary, never the head.
      if (latest) {
        obj.currentReceipt = latest.hash;
        obj.currentNode = latest.decision_node;
      }
    }
  }

  _obj(key) {
    let o = this.objects.get(key);
    if (!o) {
      o = { key, currentNode: null, currentReceipt: null, hasOpenConflict: false, openCaseId: null };
      this.objects.set(key, o);
    }
    return o;
  }

  // ------------------------------------------------------- graph derivation --

  _childrenIndex() {
    const children = new Map();
    for (const node of this.nodes.values()) {
      const parents = node.kind === 'change' ? node.predecessors : node.parents;
      for (const p of parents) {
        if (!this.nodes.has(p)) continue; // suspended/cold predecessor
        if (!children.has(p)) children.set(p, []);
        children.get(p).push(node.digest);
      }
    }
    return children;
  }

  _missingPreds(node) {
    return node.kind === 'change' ? node.predecessors.filter((p) => !this.nodes.has(p)) : [];
  }

  /** Compute live tips and suspended envelopes for one object. */
  _objectTips(key, children) {
    const mine = [...this.nodes.values()].filter((n) => n.object_key === key);
    const suspended = [];
    const live = [];
    for (const n of mine) {
      const missing = this._missingPreds(n);
      if (missing.length) suspended.push({ digest: n.digest, missing });
      else live.push(n);
    }
    const tips = live.filter((n) => !(children.get(n.digest) || []).some((c) => this.nodes.get(c)?.object_key === key)).map((n) => n.digest);
    return { tips: this._sortNodeDigests(tips), suspended };
  }

  _sortNodeDigests(digests) {
    return [...digests].sort((a, b) => {
      const x = this.nodes.get(a);
      const y = this.nodes.get(b);
      const kx = x.kind === 'change' ? `0|${x.site}|${String(x.site_seq).padStart(12, '0')}|${x.digest}` : `1|${x.digest}`;
      const ky = y.kind === 'change' ? `0|${y.site}|${String(y.site_seq).padStart(12, '0')}|${y.digest}` : `1|${y.digest}`;
      return kx < ky ? -1 : kx > ky ? 1 : 0;
    });
  }

  _coverage(node) {
    if (node.kind === 'change') return [{ region: node.region, change_digest: node.change_digest, tip: node.digest }];
    return node.contributions.map((c) => ({ ...c, tip: node.digest }));
  }

  // ------------------------------------------------------------- validation --

  _validateEnvelope(body) {
    if (!body || typeof body !== 'object' || Array.isArray(body)) {
      throw new ApiError(400, 'BAD_REQUEST', 'envelope must be a JSON object');
    }
    const allowed = ['object_key', 'predecessor_digests', 'coverage_region', 'change_digest', 'site', 'site_seq', 'request_key'];
    for (const k of Object.keys(body)) {
      if (!allowed.includes(k)) throw new ApiError(400, 'BAD_REQUEST', `unknown field ${k}`);
    }
    const str = (name) => {
      const v = body[name];
      if (typeof v !== 'string' || !TOKEN_RE.test(v)) throw new ApiError(400, 'BAD_REQUEST', `${name} must be a 1-128 char token`);
      return v;
    };
    const hex = (name) => {
      const v = body[name];
      if (typeof v !== 'string' || !HEX64_RE.test(v)) throw new ApiError(400, 'BAD_REQUEST', `${name} must be 64 hex chars`);
      return v;
    };
    const object_key = str('object_key');
    const coverage_region = str('coverage_region');
    const change_digest = hex('change_digest');
    const site = str('site');
    const request_key = str('request_key');
    const site_seq = body.site_seq;
    if (!Number.isInteger(site_seq) || site_seq < 0) throw new ApiError(400, 'BAD_REQUEST', 'site_seq must be a non-negative integer');
    let preds = body.predecessor_digests;
    if (!Array.isArray(preds)) throw new ApiError(400, 'BAD_REQUEST', 'predecessor_digests must be an array');
    preds = preds.map((p) => {
      if (typeof p !== 'string' || !HEX64_RE.test(p)) throw new ApiError(400, 'BAD_REQUEST', 'predecessor digests must be 64 hex chars');
      return p;
    });
    if (new Set(preds).size !== preds.length) throw new ApiError(400, 'BAD_REQUEST', 'duplicate predecessor digests');
    preds = [...preds].sort();
    return { object_key, predecessor_digests: preds, coverage_region, change_digest, site, site_seq, request_key };
  }

  envelopeDigestInput(env) {
    return {
      object_key: env.object_key,
      predecessor_digests: [...env.predecessor_digests].sort(),
      coverage_region: env.coverage_region,
      change_digest: env.change_digest,
      site: env.site,
      site_seq: env.site_seq,
    };
  }

  // ------------------------------------------------------------ submission --

  async submitEnvelope(rawBody) {
    const prepared = await this.lock.runExclusive(() => this._acceptEnvelopeLocked(rawBody));
    if (prepared.immediate) return prepared.result;
    return this._awaitSettle(prepared.objectKey, prepared.nodeDigest, prepared.requestKey);
  }

  async previewEnvelopeDigest(rawBody) {
    const env = this._validateEnvelope(rawBody);
    return { node_digest: digestOf(this.envelopeDigestInput(env)) };
  }

  _acceptEnvelopeLocked(rawBody) {
    const env = this._validateEnvelope(rawBody);
    const bodyHash = digestOf(canonical(env));

    // Idempotency authority is the request key; bindings archived by a
    // checkpoint stay authoritative in the cold envelope index.
    const seen = this.requestIndex.get(env.request_key) || this.archivedRequestKeys.get(env.request_key);
    if (seen) {
      if (seen.body_hash !== bodyHash) {
        throw new ApiError(409, 'REQUEST_KEY_BODY_CONFLICT', 'same request_key was already used with a different body', {
          request_key: env.request_key,
        });
      }
      return { immediate: true, result: { ...this._resultFor(env.object_key, seen.node_digest), replayed: true, request_key: env.request_key } };
    }

    const nodeDigest = digestOf(this.envelopeDigestInput(env));

    // Site monotonic sequence: a (object, site, seq) triple binds exactly one payload.
    const seqKey = `${env.object_key}|${env.site}|${env.site_seq}`;
    const priorAtSeq = this.siteSeqs.get(seqKey) || this.archivedSiteSeqs.get(seqKey);
    if (priorAtSeq && priorAtSeq !== nodeDigest) {
      throw new ApiError(409, 'SITE_SEQ_CONFLICT', 'site already reported a different envelope for this sequence', {
        site: env.site,
        site_seq: env.site_seq,
      });
    }

    // A referenced ancestor may have been compressed into cold storage. Restore
    // it through the archive index BEFORE accepting the envelope, so the new
    // node is never mistaken for a root. Tampered cold storage rejects the write
    // deterministically instead of silently suspending.
    const coldMissing = env.predecessor_digests.filter((p) => !this.nodes.has(p) && this.archivedNodes.has(p));
    if (coldMissing.length) {
      try {
        this._restoreAncestry(coldMissing, 'late_predecessor');
      } catch (e) {
        if (e instanceof ArchiveError) {
          throw new ApiError(409, e.code, `archived dependency cannot be recovered: ${e.message}`, e.details);
        }
        throw e;
      }
    }

    // The envelope event is the durable request-key binding; re-reporting the
    // same content under a new key appends the binding, not a second node.
    const ev = { ...env, digest: nodeDigest, body_hash: bodyHash };
    this.wal.append(T_ENVELOPE, JSON.stringify(ev));
    this._indexEnvelope(ev);
    this._obj(env.object_key); // register even if the envelope will suspend

    const missing = env.predecessor_digests.filter((p) => !this.nodes.has(p));
    if (missing.length) {
      return {
        immediate: true,
        result: {
          status: 'suspended',
          request_key: env.request_key,
          node_digest: nodeDigest,
          object_key: env.object_key,
          missing_predecessors: missing,
          replayed: false,
        },
      };
    }

    this._cascadeResolution(env.object_key);
    const obj = this._obj(env.object_key);
    if (obj.hasOpenConflict) {
      return { immediate: true, result: { ...this._resultFor(env.object_key, nodeDigest), replayed: false, request_key: env.request_key } };
    }
    return { immediate: false, objectKey: env.object_key, nodeDigest, requestKey: env.request_key };
  }

  /** After a live node arrives, activate any envelopes whose ancestry is now complete. */
  _cascadeResolution(seedObjectKey) {
    const affected = new Set([seedObjectKey]);
    let changed = true;
    while (changed) {
      changed = false;
      for (const node of this.nodes.values()) {
        if (node.kind !== 'change' || node._resolved) continue;
        if (this._missingPreds(node).length === 0) {
          node._resolved = true;
          affected.add(node.object_key);
          changed = true;
        }
      }
    }
    for (const key of affected) this._scheduleSettle(key);
  }

  // -------------------------------------------------------------- settling --

  /**
   * (Re)arm the debounced settle for an object. Each arriving envelope bumps a
   * generation counter and resets the quiet-period window. A burst of
   * concurrent/out-of-order reports therefore collapses into exactly one
   * convergence pass and one decision; stale timer callbacks self-cancel.
   */
  _scheduleSettle(objectKey) {
    const obj = this._obj(objectKey);
    if (obj.hasOpenConflict) return;
    const gen = (this._settleGen.get(objectKey) || 0) + 1;
    this._settleGen.set(objectKey, gen);
    const existing = this._timers.get(objectKey);
    if (existing) clearTimeout(existing);
    const timer = setTimeout(() => {
      this._timers.delete(objectKey);
      if (this._settleGen.get(objectKey) !== gen) return; // superseded
      this.lock.runExclusive(() => {
        if (this._settleGen.get(objectKey) !== gen) return;
        this._settleObject(objectKey);
      }).catch(() => {});
    }, this.config.settleDelayMs);
    if (existing) this._timers.set(objectKey, timer);
    else this._timers.set(objectKey, timer);
  }

  /**
   * Wait OUTSIDE the lock for the scheduled settle of this object to run.
   * Re-reading state on the wake turn (no lock held; the settle itself ran under
   * the lock) is safe because JS reads of plain values are atomic on this turn.
   */
  _awaitSettle(objectKey, nodeDigest, requestKey) {
    return new Promise((resolve) => {
      const finish = () => resolve({ ...this._resultFor(objectKey, nodeDigest), replayed: false, request_key: requestKey });
      const list = this._waiters.get(objectKey) || [];
      list.push(finish);
      this._waiters.set(objectKey, list);
      // Safety bound; the settle timer is the normal wake path.
      setTimeout(() => {
        const cur = this._waiters.get(objectKey);
        if (!cur || !cur.includes(finish)) return;
        this._waiters.set(objectKey, cur.filter((w) => w !== finish));
        finish();
      }, Math.max(5000, this.config.settleDelayMs * 20));
    });
  }

  _wake(objectKey) {
    const list = this._waiters.get(objectKey);
    if (!list) return;
    this._waiters.delete(objectKey);
    for (const w of list) w();
  }

  _regionMap(coverages) {
    const byRegion = new Map();
    for (const { cov } of coverages) {
      for (const c of cov) {
        if (!byRegion.has(c.region)) byRegion.set(c.region, c.change_digest);
      }
    }
    return byRegion;
  }

  /**
   * Deterministic redundancy reduction.
   * Pass 1 drops exact duplicate siblings (canonical order keeps the first copy);
   * pass 2 drops raw changes strictly contained in one surviving merge node (the
   * restart case), so an emitted merge never re-conflicts with what it contains.
   */
  _essentialTips(coverages) {
    const redundant = new Set();
    for (let i = 0; i < coverages.length; i++) {
      const cur = coverages[i];
      if (cur.cov.length === 0) { redundant.add(cur.tip); continue; }
      const equalToAnother = coverages.some((o, j) =>
        j < i && !redundant.has(o.tip) && cur.cov.length === o.cov.length &&
        cur.cov.every((c) => o.cov.some((x) => x.region === c.region && x.change_digest === c.change_digest)));
      if (equalToAnother) redundant.add(cur.tip);
    }
    const alive = coverages.filter((c) => !redundant.has(c.tip));
    for (const cur of alive) {
      const containedByOne = alive.some((o) =>
        o.tip !== cur.tip &&
        o.cov.length >= cur.cov.length &&
        cur.cov.every((c) => o.cov.some((x) => x.region === c.region && x.change_digest === c.change_digest)));
      if (containedByOne) redundant.add(cur.tip);
    }
    return coverages.map((c) => c.tip).filter((t) => !redundant.has(t));
  }

  _disputedRegions(coverages) {
    const byRegion = new Map();
    for (const { tip, cov } of coverages) {
      for (const c of cov) {
        if (!byRegion.has(c.region)) byRegion.set(c.region, []);
        byRegion.get(c.region).push({ tip, change_digest: c.change_digest });
      }
    }
    const disputed = [];
    const candidates = [];
    for (const region of [...byRegion.keys()].sort()) {
      const entries = byRegion.get(region);
      const distinct = new Set(entries.map((e) => e.change_digest));
      if (distinct.size > 1) {
        disputed.push(region);
        candidates.push(...entries.map((e) => ({ region, tip: e.tip, change_digest: e.change_digest })));
      }
    }
    candidates.sort((a, b) => {
      const k = `${a.region}|${a.change_digest}|${a.tip}`;
      const l = `${b.region}|${b.change_digest}|${b.tip}`;
      return k < l ? -1 : 1;
    });
    return { disputed, candidates };
  }

  /** One deterministic convergence pass for an object. */
  _settleObject(objectKey) {
    const obj = this._obj(objectKey);
    if (obj.hasOpenConflict) {
      this._wake(objectKey);
      return;
    }
    const children = this._childrenIndex();
    const { tips, suspended } = this._objectTips(objectKey, children);
    if (suspended.length || tips.length === 0) {
      this._wake(objectKey);
      return;
    }

    let coverages = tips.map((d) => ({ tip: d, cov: this._coverage(this.nodes.get(d)) }));

    // Drop duplicate siblings and raw changes already contained in an existing
    // decision BEFORE testing for collisions. Otherwise a freshly emitted merge
    // would immediately re-conflict with the raw changes it synthesised (this
    // matters on the post-decision settle turn and during recovery).
    const essentialTips = this._essentialTips(coverages);
    if (essentialTips.length !== coverages.length) {
      const keep = new Set(essentialTips);
      coverages = coverages.filter((c) => keep.has(c.tip));
    }
    const { disputed, candidates } = this._disputedRegions(coverages);

    if (disputed.length) {
      this._openConflict(obj, essentialTips, disputed, candidates, coverages);
      this._wake(objectKey);
      return;
    }

    const regionMap = this._regionMap(coverages);
    const contributions = [...regionMap.entries()]
      .sort((a, b) => (a[0] < b[0] ? -1 : 1))
      .map(([region, change_digest]) => ({ region, change_digest }));

    // Content-based idempotence: if the persisted current decision already
    // describes exactly this region->digest map, there is nothing new to
    // decide. Comparing content (not node identity / parent set) is what makes
    // recovery safe: the same convergent content reached via different parent
    // bookkeeping must not yield a second receipt.
    if (obj.currentNode) {
      const current = this.nodes.get(obj.currentNode);
      const currentCov = new Map((current?.contributions || []).map((c) => [c.region, c.change_digest]));
      const sameContent = currentCov.size === contributions.length &&
        contributions.every((c) => currentCov.get(c.region) === c.change_digest);
      if (sameContent) {
        this._wake(objectKey);
        return;
      }
    }

    const multipleSiblings = essentialTips.length > 1;
    const decisionKind = obj.currentReceipt ? (multipleSiblings ? 'merge' : 'extend') : multipleSiblings ? 'merge' : 'accept';
    this._emitDecision(objectKey, essentialTips, contributions, decisionKind, null);
    this._wake(objectKey);
  }

  _openConflict(obj, tips, disputed, candidates, coverages) {
    const parents = this._sortNodeDigests(tips);
    const baseContributions = [];
    const seenRegion = new Set();
    for (const { cov } of coverages) {
      for (const c of cov) {
        if (!seenRegion.has(c.region)) {
          seenRegion.add(c.region);
          baseContributions.push({ region: c.region, change_digest: c.change_digest });
        }
      }
    }
    baseContributions.sort((a, b) => (a.region < b.region ? -1 : 1));
    const caseId = this._caseIdFor(obj.key, disputed, candidates);

    const existing = this.reviews.get(caseId);
    if (!existing || existing.resolved) {
      this.reviews.set(caseId, {
        caseId,
        objectKey: obj.key,
        disputed,
        candidates: candidates.map((c) => ({ ...c })).sort((a, b) => {
          const k = `${a.region}|${a.change_digest}|${a.tip}`;
          const l = `${b.region}|${b.change_digest}|${b.tip}`;
          return k < l ? -1 : 1;
        }),
        parents,
        baseContributions,
        reviewers: [],
        choices: null,
        resolved: false,
      });
    }
    obj.hasOpenConflict = true;
    obj.openCaseId = caseId;
  }

  // ------------------------------------------------------------- decisions --

  _emitDecision(objectKey, parents, contributions, decisionKind, adjudication) {
    const obj = this._obj(objectKey);
    const nodeInput = {
      object_key: objectKey,
      parents,
      contributions,
      decision_kind: decisionKind,
      ...(adjudication ? { case_id: adjudication.caseId, reviewers: adjudication.reviewerNames } : {}),
    };
    const node = {
      digest: digestOf(nodeInput),
      kind: 'merge',
      ...nodeInput,
    };
    if (this.nodes.has(node.digest)) {
      // Deterministic replay-safe dedup: same convergence already emitted.
      obj.currentNode = node.digest;
      obj.currentReceipt = this._receiptByNode(node.digest)?.hash ?? obj.currentReceipt;
      return node;
    }

    const seq = (this.receiptSeq.get(objectKey) || 0) + 1;
    const occurredAt = nowIso();
    const receiptFields = {
      seq,
      object_key: objectKey,
      decision_kind: decisionKind,
      decision_node: node.digest,
      parents,
      contributions,
      ...(adjudication
        ? { case_id: adjudication.caseId, reviewers: adjudication.reviewerNames }
        : {}),
      prev_receipt: obj.currentReceipt,
      emitted_at: occurredAt,
    };
    const hash = digestOf(receiptFields);
    const receipt = { ...receiptFields, hash, sig: sign(this.config.receiptSecret, receiptFields) };

    // Transactional outbox: the business decision frame and its notification
    // event(s) are ONE durable batch. An adjudication that closes a conflict
    // produces a decision.created event AND a case.closed event in the same
    // batch (same persisted result, no notification gap either way).
    const eventDescriptors = [this._decisionEventDescriptor(receipt, occurredAt)];
    if (adjudication) {
      eventDescriptors.push(this._caseClosedEventDescriptor(
        adjudication.caseId, receipt, occurredAt));
    }
    this.outbox.appendWithBusiness(
      [[T_DECISION, JSON.stringify({ node, receipt })]],
      eventDescriptors,
    );

    this.nodes.set(node.digest, node);
    this.receipts.set(hash, receipt);
    this.receiptSeq.set(objectKey, seq);
    obj.currentNode = node.digest;
    obj.currentReceipt = hash;
    if (adjudication) {
      const cs = this.reviews.get(adjudication.caseId);
      if (cs) cs.resolved = true;
      obj.hasOpenConflict = false;
      obj.openCaseId = null;
    }
    return node;
  }

  _decisionEventDescriptor(receipt, occurredAt) {
    return {
      kind: 'decision.created',
      event_id: eventIds.decisionCreated(receipt.hash),
      object_key: receipt.object_key,
      business_cursor: {
        object_key: receipt.object_key,
        receipt_seq: receipt.seq,
        receipt_hash: receipt.hash,
        decision_node: receipt.decision_node,
        prev_receipt: receipt.prev_receipt,
      },
      occurred_at: occurredAt,
      data: {
        decision_kind: receipt.decision_kind,
        contributions: receipt.contributions,
        ...(receipt.case_id ? { case_id: receipt.case_id } : {}),
      },
    };
  }

  _caseClosedEventDescriptor(caseId, receipt, occurredAt) {
    return {
      kind: 'case.closed',
      event_id: eventIds.caseClosed(caseId, receipt.hash),
      object_key: receipt.object_key,
      business_cursor: {
        object_key: receipt.object_key,
        receipt_seq: receipt.seq,
        receipt_hash: receipt.hash,
        decision_node: receipt.decision_node,
        case_id: caseId,
      },
      occurred_at: occurredAt,
      data: {
        case_id: caseId,
        decision_kind: receipt.decision_kind,
        decision_node: receipt.decision_node,
      },
    };
  }

  _receiptByNode(nodeDigest) {
    for (const r of this.receipts.values()) if (r.decision_node === nodeDigest) return r;
    try {
      for (const r of this._coldReceiptsForObjects(null)) if (r.decision_node === nodeDigest) return r;
    } catch (e) {
      if (!(e instanceof ArchiveError)) throw e;
      // Cold lookup is best-effort here; verification paths read cold explicitly.
    }
    return null;
  }

  // --------------------------------------------------------------- reviews --

  async submitReview(caseId, reviewerId, reviewerName, rawChoices) {
    return this.lock.runExclusive(() => this._submitReviewLocked(caseId, reviewerId, reviewerName, rawChoices));
  }

  _validateChoices(cs, rawChoices) {
    if (!rawChoices || typeof rawChoices !== 'object' || Array.isArray(rawChoices)) {
      throw new ApiError(400, 'BAD_REQUEST', 'choices must be an object region->change_digest');
    }
    const keys = Object.keys(rawChoices).sort();
    const want = [...cs.disputed].sort();
    if (canonical(keys) !== canonical(want)) {
      throw new ApiError(400, 'BAD_REQUEST', 'choices must cover exactly the disputed regions', { expected: want });
    }
    for (const region of want) {
      const choice = rawChoices[region];
      const ok = cs.candidates.some((c) => c.region === region && c.change_digest === choice);
      if (!ok) throw new ApiError(400, 'BAD_REQUEST', `choice for ${region} is not a candidate change_digest`);
    }
    const out = {};
    for (const region of want) out[region] = rawChoices[region];
    return out;
  }

  _submitReviewLocked(caseId, reviewerId, reviewerName, rawChoices) {
    const cs = this.reviews.get(caseId);
    if (!cs || cs.resolved) throw new ApiError(404, 'CASE_NOT_FOUND', 'no open conflict with this case id');
    if (cs.objectKey && !this.objects.has(cs.objectKey)) throw new ApiError(404, 'CASE_NOT_FOUND', 'object missing');

    if (cs.reviewers.some((r) => r.reviewer_id === reviewerId)) {
      throw new ApiError(403, 'REVIEWER_MUST_DIFFER', 'the same reviewer may not record both decisions');
    }
    if (cs.reviewers.length >= 2) throw new ApiError(409, 'CASE_ALREADY_DECIDED', 'case already has two reviews');

    const choices = this._validateChoices(cs, rawChoices);

    const isFirst = cs.reviewers.length === 0;

    if (isFirst) {
      const ev = {
        case_id: caseId,
        object_key: cs.objectKey,
        reviewer_id: reviewerId,
        reviewer_name: reviewerName,
        choices,
        disputed: cs.disputed,
        candidates: cs.candidates,
        parents: cs.parents,
        base_contributions: cs.baseContributions,
      };
      this.wal.append(T_REVIEW, JSON.stringify(ev));
      cs.reviewers.push({ reviewer_id: reviewerId, reviewer_name: reviewerName, choices });
      cs.choices = choices;
      return { status: 'awaiting_second_review', case_id: caseId, object_key: cs.objectKey };
    }

    // Second opinion: compare BEFORE appending. A dissenting second opinion is
    // rejected atomically (nothing persisted) so a later matching pair can
    // adjudicate and the WAL never carries an ambiguous triple.
    const first = cs.reviewers[0];
    if (canonical(first.choices) !== canonical(choices)) {
      throw new ApiError(409, 'REVIEW_MISMATCH', 'second reviewer disagrees with the first; case remains open', {
        first_reviewer: first.reviewer_name,
        first_choices: first.choices,
      });
    }
    const ev = {
      case_id: caseId,
      object_key: cs.objectKey,
      reviewer_id: reviewerId,
      reviewer_name: reviewerName,
      choices,
    };
    this.wal.append(T_REVIEW, JSON.stringify(ev));
    cs.reviewers.push({ reviewer_id: reviewerId, reviewer_name: reviewerName, choices });
    const node = this._finalizeAdjudication(cs, choices, [
      { reviewer_id: first.reviewer_id, reviewer_name: first.reviewer_name },
      { reviewer_id: reviewerId, reviewer_name: reviewerName },
    ]);
    if (!node) {
      // Reviews matched, but a branch arriving concurrently opened a new case.
      const obj = this._obj(cs.objectKey);
      return { status: 'new_conflict_opened', case_id: obj.openCaseId, object_key: cs.objectKey };
    }
    return { status: 'adjudicated', case_id: caseId, object_key: cs.objectKey, current_receipt: this._obj(cs.objectKey).currentReceipt };
  }

  /**
   * Apply a matched pair of reviews against the CURRENT live tips, not the
   * snapshot from when the case opened. Branches that arrived while the case
   * was open are folded in; a genuinely new disagreement (a third digest for a
   * disputed region, or a fresh collision elsewhere) opens a new case instead
   * of emitting a decision.
   */
  _finalizeAdjudication(cs, choices, reviewers) {
    const children = this._childrenIndex();
    const { tips, suspended } = this._objectTips(cs.objectKey, children);
    const coverages = tips.map((d) => ({ tip: d, cov: this._coverage(this.nodes.get(d)) }));

    // Drop every unchosen candidate contribution of a disputed region. A tip
    // whose coverage becomes empty (the rejected side of a same-region split)
    // drops out entirely so it cannot reappear as a live tip afterwards.
    const kept = [];
    for (const { tip, cov } of coverages) {
      const remaining = cov.filter((c) => !cs.disputed.includes(c.region) || c.change_digest === choices[c.region]);
      if (remaining.length) kept.push({ tip, cov: remaining });
    }
    const { disputed, candidates } = this._disputedRegions(kept);

    if (disputed.length || suspended.length) {
      // Adjudication is stale: the live graph still contains a disagreement or
      // awaits an ancestor. Close the old case and open one for the live state.
      cs.resolved = true;
      const obj = this._obj(cs.objectKey);
      if (!suspended.length) {
        this._openConflict(obj, tips, disputed, candidates, kept);
      }
      return null;
    }

    // Same redundancy reduction as the normal settle pass, plus union.
    const regionMap = this._regionMap(kept);
    const contributions = [...regionMap.entries()]
      .sort((a, b) => (a[0] < b[0] ? -1 : 1))
      .map(([region, change_digest]) => ({ region, change_digest }));

    // Graph parents are EVERY current live tip, including the rejected side of a
    // split: the decision supersedes it (so it must stop being a live tip) and
    // the auditable version graph records that it was adjudicated away. Only the
    // winning digests survive in `contributions`.
    const graphParents = this._sortNodeDigests(tips);
    return this._emitDecision(
      cs.objectKey,
      graphParents,
      contributions,
      'adjudicate',
      { caseId: cs.caseId, reviewerNames: reviewers.map((r) => r.reviewer_name) },
    );
  }

  _caseIdFor(objectKey, disputed, candidates) {
    return digestOf({
      object_key: objectKey,
      disputed,
      candidates: candidates.map((c) => `${c.region}:${c.change_digest}@${c.tip}`),
    });
  }

  // -------------------------------------------------------------- recovery --

  /**
   * Re-run convergence after WAL replay. Deterministic: an emitted decision
   * dedupes on node digest; a crash between the second review append and the
   * decision append is completed here exactly once. No settle timer survives a
   * restart, so this pass replaces it.
   */
  _recoverSettlements() {
    for (const node of this.nodes.values()) {
      if (node.kind === 'change' && this._missingPreds(node).length === 0) node._resolved = true;
    }

    let progressed = true;
    let guard = 0;
    while (progressed && guard++ < 100000) {
      progressed = false;
      for (const key of [...this.objects.keys()].sort()) {
        const obj = this._obj(key);
        const children = this._childrenIndex();
        const { tips, suspended } = this._objectTips(key, children);
        if (suspended.length || tips.length === 0) continue;

        let coverages = tips.map((d) => ({ tip: d, cov: this._coverage(this.nodes.get(d)) }));
        // Collapse duplicates/contained tips before collision analysis, matching
        // the live settle path, so an emitted merge cannot re-conflict with the
        // raw changes it contains.
        const essential = this._essentialTips(coverages);
        if (essential.length !== coverages.length) {
          const keep = new Set(essential);
          coverages = coverages.filter((c) => keep.has(c.tip));
        }
        const { disputed, candidates } = this._disputedRegions(coverages);

        if (!disputed.length) {
          if (obj.hasOpenConflict) {
            obj.hasOpenConflict = false;
            obj.openCaseId = null;
          }
          const before = obj.currentNode;
          this._settleObject(key);
          if (obj.currentNode !== before) progressed = true;
          continue;
        }

        const caseId = this._caseIdFor(key, disputed, candidates);
        const stored = this.reviews.get(caseId);
        if (stored && stored.reviewers.length >= 2 && !stored.resolved) {
          // Crash window: second review durable, decision not yet.
          const [first, second] = stored.reviewers;
          if (canonical(first.choices) === canonical(second.choices)) {
            const node = this._finalizeAdjudication(stored, first.choices, [
              { reviewer_id: first.reviewer_id, reviewer_name: first.reviewer_name },
              { reviewer_id: second.reviewer_id, reviewer_name: second.reviewer_name },
            ]);
            if (node) {
              // All adjudicated tips (rejected side included) are now parents of
              // the decision, so they stop being live tips. _emitDecision clears
              // the conflict flags; a genuinely new concurrent branch would have
              // made _finalizeAdjudication return null and reopen a case.
              progressed = true;
            }
            continue;
          }
        }
        // Re-mark an open conflict (zero or one recorded review).
        if (stored) {
          obj.hasOpenConflict = true;
          obj.openCaseId = caseId;
        } else {
          this._openConflict(obj, tips, disputed, candidates, coverages);
        }
      }
    }
  }

  // ------------------------------------------------- outbox reconciliation --

  /**
   * Enumerate every business moment that must have a notification, in business
   * WAL append order:
   *   - each decision frame -> decision.created (+ case.closed for adjudicates)
   *   - each checkpoint stub -> cold_segment.generated
   * Outbox event frames are the durable mirror; reconcile() appends any mirror
   * entry that is missing (closing a crash gap) and idempotently fans out.
   *
   * Scanning the raw WAL in file order (rather than in-memory maps) means a
   * recovered order matches the original append order even after a hot trim.
   */
  _outboxEventDescriptors() {
    const descriptors = [];
    const { frames } = this.wal.readAllFrames();
    for (const f of frames) {
      let parsed;
      try { parsed = JSON.parse(f.payload.toString('utf8')); } catch { continue; }
      if (f.type === T_DECISION) {
        const { receipt } = parsed;
        if (!receipt) continue;
        descriptors.push(this._decisionEventDescriptor(receipt, receipt.emitted_at));
        if (receipt.decision_kind === 'adjudicate' && receipt.case_id) {
          descriptors.push(this._caseClosedEventDescriptor(receipt.case_id, receipt, receipt.emitted_at));
        }
      } else if (f.type === T_CHECKPOINT) {
        const stub = parsed;
        descriptors.push({
          kind: 'cold_segment.generated',
          event_id: eventIds.coldSegment(stub.segment_id),
          object_key: stub.object_key,
          business_cursor: {
            object_key: stub.object_key,
            boundary_receipt_seq: stub.boundary_seq,
            boundary_receipt_hash: stub.boundary_receipt?.hash || null,
            segment_id: stub.segment_id,
          },
          occurred_at: stub.created_at,
          data: {
            segment_id: stub.segment_id,
            chain_anchor: stub.chain_anchor,
            boundary_receipt_seq: stub.boundary_seq,
            content_digest: null,
            archived_frames: (stub.archived_nodes || []).length,
          },
        });
      }
    }
    // Decisions restored from cold storage (T_RESTORED wrappers) are separate
    // frames that predate this process: their decision.created event was emitted
    // when they were first produced and survives in the (trimmed) event log, so
    // they are intentionally not enumerated here.
    return descriptors;
  }

  // =============================================================== CHECKPOINTS

  async createCheckpoint(rawBody) {
    return this.lock.runExclusive(() => this._createCheckpointLocked(rawBody || {}));
  }

  _validateCheckpointBody(body) {
    if (!body || typeof body !== 'object' || Array.isArray(body)) {
      throw new ApiError(400, 'BAD_REQUEST', 'checkpoint request must be a JSON object');
    }
    const object_key = body.object_key;
    if (typeof object_key !== 'string' || !TOKEN_RE.test(object_key)) {
      throw new ApiError(400, 'BAD_REQUEST', 'object_key must be a 1-128 char token');
    }
    let boundarySeq = body.boundary_seq;
    if (boundarySeq !== undefined && boundarySeq !== null) {
      if (!Number.isInteger(boundarySeq) || boundarySeq < 1) {
        throw new ApiError(400, 'BAD_REQUEST', 'boundary_seq must be a positive integer');
      }
    } else {
      boundarySeq = null;
    }
    let taskKey = body.task_key;
    if (taskKey !== undefined && taskKey !== null) {
      if (typeof taskKey !== 'string' || !TOKEN_RE.test(taskKey)) {
        throw new ApiError(400, 'BAD_REQUEST', 'task_key must be a 1-128 char token');
      }
    } else {
      taskKey = null;
    }
    return { object_key, boundarySeq, taskKey };
  }

  /** Why an object cannot be checkpointed right now (empty array = archivable). */
  _checkpointBlockers(key, targetSeq) {
    const reasons = [];
    const obj = this.objects.get(key);
    if (!obj || !this.receiptSeq.get(key)) reasons.push('not_settled');
    if (obj?.hasOpenConflict) reasons.push('open_conflict');
    const children = this._childrenIndex();
    const { suspended } = obj ? this._objectTips(key, children) : { suspended: [] };
    if (suspended.length) reasons.push('suspended_envelopes');
    const latest = this.receiptSeq.get(key) || 0;
    const target = targetSeq || latest;
    if (target > latest) reasons.push('boundary_not_reached');
    if (latest - target < (this.config.retainDecisions || 0)) reasons.push('retention_policy');
    return reasons;
  }

  _createCheckpointLocked(body) {
    const { object_key: key, boundarySeq, taskKey } = this._validateCheckpointBody(body);

    if (taskKey && this.archiveTaskKeys.has(taskKey)) {
      const prior = this.archiveTaskKeys.get(taskKey);
      if (prior.object_key !== key) {
        throw new ApiError(409, 'CHECKPOINT_KEY_CONFLICT', 'task_key already used for a different object', {
          task_key: taskKey, object_key: prior.object_key,
        });
      }
      return { ...prior, idempotent_replay: true };
    }

    const blockers = this._checkpointBlockers(key, boundarySeq);
    if (blockers.length) {
      throw new ApiError(409, 'CHECKPOINT_BLOCKED', 'object cannot be checkpointed in its current state', {
        object_key: key, reasons: blockers,
      });
    }

    const latestSeq = this.receiptSeq.get(key);
    const target = boundarySeq || latestSeq;
    const prev = this.checkpoints.get(key) || null;
    if (prev && prev.boundary_seq >= target) {
      // Nothing newer to compress: a repeat request returns the same checkpoint.
      return { ...this._checkpointSummary(prev, 0, 0), idempotent_replay: true };
    }

    const walBytesBefore = fs.statSync(this.config.walPath).size;

    // Closure = ancestor closure of the boundary decision among HOT nodes.
    // With an explicit boundary older than the head, later decisions and their
    // raw changes stay hot; with the default (head) boundary every hot node of
    // the object is an ancestor. No suspended envelopes / open conflicts by the
    // guard above.
    const boundaryReceipt = [...this.receipts.values()].find((r) => r.object_key === key && r.seq === target);
    if (!boundaryReceipt) throw new ApiError(409, 'CHECKPOINT_BLOCKED', 'boundary receipt not in hot storage', { object_key: key });
    const closure = new Set();
    {
      const q = [boundaryReceipt.decision_node];
      while (q.length) {
        const d = q.shift();
        if (closure.has(d)) continue;
        const n = this.nodes.get(d);
        if (!n) continue; // already in an earlier segment (chain anchor covers it)
        closure.add(d);
        const parents = n.kind === 'change' ? n.predecessors : n.parents;
        for (const p of parents) if (!closure.has(p)) q.push(p);
      }
    }

    // Select the NEW frames for this segment (reviews stay in the hot WAL; they
    // are adjudication metadata, not graph/log ancestry).
    const allFrames = this.wal.readAllFrames().frames;
    const selected = [];
    for (const f of allFrames) {
      if (f.type !== T_ENVELOPE && f.type !== T_DECISION) continue;
      let ev;
      try { ev = JSON.parse(f.payload.toString('utf8')); } catch { continue; }
      const belongs = f.type === T_ENVELOPE
        ? ev.object_key === key && closure.has(ev.digest)
        : ev.node.object_key === key && closure.has(ev.node.digest);
      if (belongs) selected.push({ type: f.type, payload: f.payload, ev });
    }

    // Cumulative envelope index: request-key / site-seq authority survives trim.
    const envelopeIndex = new Map();
    for (const e of prev?.envelope_index || []) envelopeIndex.set(e.digest, { ...e });
    for (const s of selected.filter((s) => s.type === T_ENVELOPE)) {
      const ev = s.ev;
      envelopeIndex.set(ev.digest, {
        digest: ev.digest, object_key: ev.object_key, site: ev.site, site_seq: ev.site_seq,
        request_key: ev.request_key, body_hash: ev.body_hash,
      });
    }

    let segmentResult;
    try {
      segmentResult = this.archive.buildSegment({
        objectKey: key,
        boundaryReceiptSeq: target,
        chainAnchor: prev?.segment_id || null,
        frames: selected.map((s) => ({ type: s.type, payload: s.payload })),
        envelopeIndex: [...envelopeIndex.values()],
      });
    } catch (e) {
      if (e instanceof ArchiveError) throw new ApiError(409, e.code, e.message, e.details);
      throw e;
    }
    const { segmentId, manifest } = segmentResult;
    this.archive.remember(manifest);
    this.segmentHealth.set(segmentId, { ok: true });

    const taskId = digestOf({ kind: 'checkpoint-task', object_key: key, boundary_seq: target, segment_id: segmentId });
    this.archive.createTaskMarker(taskId, {
      task_id: taskId, phase: 'PLAN', object_key: key, boundary_seq: target, segment_id: segmentId,
      created_at: nowIso(),
    });

    // Cumulative archived-node index; the boundary node itself stays hot.
    const archivedNodes = new Map();
    for (const n of prev?.archived_nodes || []) archivedNodes.set(n.digest, { ...n });
    for (const s of selected) {
      if (s.type === T_ENVELOPE) {
        archivedNodes.set(s.ev.digest, { digest: s.ev.digest, object_key: key, kind: 'change', segment_id: segmentId });
      } else {
        archivedNodes.set(s.ev.node.digest, { digest: s.ev.node.digest, object_key: key, kind: 'merge', segment_id: segmentId });
      }
    }
    archivedNodes.delete(boundaryReceipt.decision_node);

    // Cumulative archived-receipt index; the new boundary receipt stays hot.
    const archivedReceipts = new Map();
    for (const r of prev?.archived_receipts || []) archivedReceipts.set(r.hash, { ...r });
    if (prev) {
      archivedReceipts.set(prev.boundary_receipt.hash, {
        hash: prev.boundary_receipt.hash, seq: prev.boundary_seq,
        segment_id: prev.segment_id, payload_hash: prev.boundary_payload_hash,
      });
    }
    for (const s of selected.filter((s) => s.type === T_DECISION)) {
      if (s.ev.receipt.seq < target) {
        archivedReceipts.set(s.ev.receipt.hash, {
          hash: s.ev.receipt.hash, seq: s.ev.receipt.seq,
          segment_id: segmentId, payload_hash: sha256Hex(s.payload),
        });
      }
    }

    const boundaryFrame = selected.find((s) => s.type === T_DECISION && s.ev.receipt.seq === target);
    const stub = {
      task_id: taskId,
      segment_id: segmentId,
      object_key: key,
      boundary_seq: target,
      boundary_node: this.nodes.get(boundaryReceipt.decision_node),
      boundary_receipt: boundaryReceipt,
      boundary_payload_hash: boundaryFrame ? sha256Hex(boundaryFrame.payload) : null,
      chain_anchor: prev?.segment_id || null,
      created_at: nowIso(),
      archived_nodes: [...archivedNodes.values()],
      archived_receipts: [...archivedReceipts.values()],
      envelope_index: [...envelopeIndex.values()],
    };

    // The stub append is the durable commit point: after it lands, the trim must
    // complete even if the process exits — startup recovery finishes the job.
    // The cold_segment.generated notification is part of the SAME durable
    // batch, so an archived segment is never silently unannounced.
    const coldOccurredAt = stub.created_at;
    const coldDescriptor = {
      kind: 'cold_segment.generated',
      event_id: eventIds.coldSegment(segmentId),
      object_key: key,
      business_cursor: {
        object_key: key,
        boundary_receipt_seq: target,
        boundary_receipt_hash: boundaryReceipt.hash,
        segment_id: segmentId,
      },
      occurred_at: coldOccurredAt,
      data: {
        segment_id: segmentId,
        chain_anchor: stub.chain_anchor,
        boundary_receipt_seq: target,
        content_digest: this.archive.get(segmentId)?.content_digest || null,
        archived_frames: selected.length,
      },
    };
    this.outbox.appendWithBusiness(
      [[T_CHECKPOINT, JSON.stringify(stub)]],
      [coldDescriptor],
    );
    this.archive.updateTaskMarker(taskId, { phase: 'VERIFIED' });
    this._indexCheckpointStub(stub);

    this._trimHotWAL();
    this.archive.updateTaskMarker(taskId, { phase: 'TRIMMED' });
    this.archive.removeTaskMarker(taskId);
    this._pruneHotToCold();

    const walBytesAfter = fs.statSync(this.config.walPath).size;
    const summary = this._checkpointSummary(stub, selected.length, Math.max(0, walBytesBefore - walBytesAfter));
    if (taskKey) this.archiveTaskKeys.set(taskKey, summary);
    return summary;
  }

  _checkpointSummary(stub, archivedFrames, reclaimedBytes) {
    return {
      status: 'checkpointed',
      task_id: stub.task_id,
      object_key: stub.object_key,
      boundary_seq: stub.boundary_seq,
      segment_id: stub.segment_id,
      chain_anchor: stub.chain_anchor,
      content_digest: this.archive.get(stub.segment_id)?.content_digest || null,
      archived_nodes: (stub.archived_nodes || []).length,
      archived_frames: archivedFrames,
      reclaimed_bytes: reclaimedBytes,
      created_at: stub.created_at,
    };
  }

  // =============================================================== RESTORE --

  /**
   * Materialise archived ancestry into the hot WAL through the archive index.
   * Every restored event is verified against its segment manifest before being
   * indexed; restored material is appended as one T_RESTORED frame, so a crash
   * mid-restore simply resumes on next startup and never double-counts.
   */
  _restoreAncestry(wantedDigests, reason) {
    const byPayload = new Map(); // payload hash -> {t, p, segmentId}
    const nodeOwners = new Map(); // digest -> segmentId
    const queue = [...new Set(wantedDigests)];
    const affectedObjects = new Set();
    const segmentsUsed = new Set();

    while (queue.length) {
      const digest = queue.shift();
      if (this.nodes.has(digest)) continue;
      const info = this.archivedNodes.get(digest);
      if (!info) continue; // genuinely unknown ancestor: stays suspended
      if (nodeOwners.has(digest)) continue;
      nodeOwners.set(digest, info.segment_id);
      segmentsUsed.add(info.segment_id);
      affectedObjects.add(info.object_key);

      const { frames } = this.archive.readSegment(info.segment_id); // throws on tamper
      let ownerNode = null;
      for (const f of frames) {
        if (f.type === T_ENVELOPE) {
          const ev = JSON.parse(f.payload.toString('utf8'));
          if (ev.digest === digest) { ownerNode = { kind: 'change', ev, raw: f }; break; }
        } else if (f.type === T_DECISION) {
          const ev = JSON.parse(f.payload.toString('utf8'));
          if (ev.node.digest === digest) { ownerNode = { kind: 'merge', ev, raw: f }; break; }
        }
      }
      if (!ownerNode) {
        throw new ArchiveError('ARCHIVE_INDEX_MISMATCH', 'indexed node missing from its segment', { node_digest: digest });
      }
      byPayload.set(sha256Hex(ownerNode.raw.payload), { t: ownerNode.raw.type, p: ownerNode.raw.payload.toString('utf8'), segmentId: info.segment_id });

      const parents = ownerNode.kind === 'change' ? ownerNode.ev.predecessor_digests : ownerNode.ev.node.parents;
      for (const p of parents) if (!this.nodes.has(p) && !nodeOwners.has(p)) queue.push(p);
    }

    if (!byPayload.size) return { restored_nodes: 0, restored_receipts: 0, segments: [] };

    const events = [...byPayload.values()].map(({ t, p }) => ({ t, p }));
    const wrap = {
      object_keys: [...affectedObjects].sort(),
      sources: [...segmentsUsed].sort(),
      reason,
      restored_at: nowIso(),
      events,
    };
    this.wal.append(T_RESTORED, JSON.stringify(wrap));
    for (const e of events) this._replay(e.t, Buffer.from(e.p, 'utf8'));
    for (const key of affectedObjects) this._reconcileObjectCurrent(key);

    const restoredNodes = events.filter((e) => e.t === T_ENVELOPE || e.t === T_DECISION).length;
    const restoredReceipts = events.filter((e) => e.t === T_DECISION).length;
    this.restoreLog.unshift({ at: wrap.restored_at, reason, object_keys: wrap.object_keys, restored_nodes: restoredNodes, sources: wrap.sources });
    if (this.restoreLog.length > 50) this.restoreLog.length = 50;
    return { restored_nodes: restoredNodes, restored_receipts: restoredReceipts, segments: wrap.sources };
  }

  /** Recompute an object's current pointer after out-of-order restored frames. */
  _reconcileObjectCurrent(key) {
    let latest = null;
    for (const r of this.receipts.values()) {
      if (r.object_key === key && (!latest || r.seq > latest.seq)) latest = r;
    }
    const obj = this._obj(key);
    if (latest) {
      obj.currentReceipt = latest.hash;
      obj.currentNode = latest.decision_node;
    }
  }

  async restoreObject(rawBody) {
    return this.lock.runExclusive(() => {
      const body = rawBody || {};
      const key = body.object_key;
      if (typeof key !== 'string' || !TOKEN_RE.test(key)) {
        throw new ApiError(400, 'BAD_REQUEST', 'object_key must be a 1-128 char token');
      }
      if (!this.objects.has(key) && ![...this.archivedNodes.values()].some((n) => n.object_key === key)) {
        throw new ApiError(404, 'OBJECT_NOT_FOUND', 'unknown object');
      }
      const wanted = [...this.archivedNodes.keys()].filter((d) => this.archivedNodes.get(d).object_key === key);
      try {
        const result = this._restoreAncestry(wanted, 'manual_restore');
        return { status: result.restored_nodes ? 'restored' : 'already_hot', object_key: key, ...result };
      } catch (e) {
        if (e instanceof ArchiveError) throw new ApiError(409, e.code, e.message, e.details);
        throw e;
      }
    });
  }

  // ============================================================= COLD READS --

  /**
   * Segments (newest first) carrying an object's history. Only the newest stub
   * stays in the hot WAL, so older links are followed through the segment
   * manifests' chain_anchor (each verified before being trusted).
   */
  _segmentsForObject(key) {
    const out = [];
    const stub = this.checkpoints.get(key);
    if (!stub) return out;
    let segmentId = stub.segment_id;
    let guard = 0;
    while (segmentId && guard++ < 100000) {
      const { manifest, frames } = this.archive.readSegment(segmentId); // throws on tamper
      out.push({ stub: segmentId === stub.segment_id ? stub : null, manifest, frames });
      segmentId = manifest.chain_anchor || null;
    }
    return out;
  }

  _coldNodesForObject(key) {
    const nodes = new Map();
    for (const part of this._segmentsForObject(key)) {
      for (const f of part.frames) {
        if (f.type === T_ENVELOPE) {
          const ev = JSON.parse(f.payload.toString('utf8'));
          nodes.set(ev.digest, this._nodeFromEnvelopeEvent(ev));
        } else if (f.type === T_DECISION) {
          const ev = JSON.parse(f.payload.toString('utf8'));
          nodes.set(ev.node.digest, ev.node);
        }
      }
    }
    return nodes;
  }

  _nodeFromEnvelopeEvent(ev) {
    return {
      digest: ev.digest,
      kind: 'change',
      object_key: ev.object_key,
      site: ev.site,
      site_seq: ev.site_seq,
      region: ev.coverage_region,
      change_digest: ev.change_digest,
      predecessors: ev.predecessor_digests,
      request_key: ev.request_key,
    };
  }

  /** All receipts for an object: hot chain followed by cold (segment) chain. */
  _receiptsForObject(key) {
    const byHash = new Map();
    for (const r of this.receipts.values()) if (r.object_key === key) byHash.set(r.hash, r);
    for (const part of this._segmentsForObject(key)) {
      for (const f of part.frames) {
        if (f.type === T_DECISION) {
          const ev = JSON.parse(f.payload.toString('utf8'));
          if (ev.receipt.object_key === key) byHash.set(ev.receipt.hash, ev.receipt);
        }
      }
    }
    return [...byHash.values()].sort((a, b) => a.seq - b.seq);
  }

  _coldReceiptsForObjects(keyFilter) {
    const out = [];
    const keys = keyFilter ? [keyFilter] : [...this.checkpoints.keys()];
    for (const key of keys) {
      if (!this.checkpoints.has(key)) continue;
      try {
        for (const part of this._segmentsForObject(key)) {
          for (const f of part.frames) {
            if (f.type === T_DECISION) {
              const ev = JSON.parse(f.payload.toString('utf8'));
              if (!keyFilter || ev.receipt.object_key === keyFilter) out.push(ev.receipt);
            }
          }
        }
      } catch (e) {
        if (e instanceof ArchiveError) throw e;
        throw e;
      }
    }
    return out;
  }

  // ------------------------------------------------------------------ views --

  _resultFor(objectKey, nodeDigest) {
    const obj = this.objects.get(objectKey);
    const children = this._childrenIndex();
    const { tips, suspended } = obj ? this._objectTips(objectKey, children) : { tips: [], suspended: [] };
    const out = {
      object_key: objectKey,
      node_digest: nodeDigest,
      status: 'unknown',
      tips,
      suspended_count: suspended.length,
    };
    if (suspended.some((s) => s.digest === nodeDigest)) {
      out.status = 'suspended';
      out.missing_predecessors = suspended.find((s) => s.digest === nodeDigest).missing;
    } else if (obj?.hasOpenConflict) {
      out.status = 'conflict_open';
      out.case_id = obj.openCaseId;
    } else if (obj?.currentReceipt) {
      out.status = 'settled';
      out.current_receipt = obj.currentReceipt;
      out.current_decision_node = obj.currentNode;
    } else {
      out.status = 'pending';
    }
    return out;
  }

  async listObjects() {
    return this.lock.runExclusive(() => {
      const children = this._childrenIndex();
      return [...this.objects.keys()].sort().map((key) => {
        const obj = this._obj(key);
        const { tips, suspended } = this._objectTips(key, children);
        const stub = this.checkpoints.get(key);
        return {
          object_key: key,
          status: obj.hasOpenConflict ? 'conflict' : suspended.length ? 'suspended' : obj.currentReceipt ? 'settled' : 'pending',
          tips,
          suspended,
          current_receipt: obj.currentReceipt,
          open_case: obj.openCaseId,
          checkpoint: stub ? {
            boundary_seq: stub.boundary_seq,
            segment_id: stub.segment_id,
            verified: this.segmentHealth.get(stub.segment_id)?.ok !== false,
          } : null,
        };
      });
    });
  }

  async getObject(key) {
    return this.lock.runExclusive(() => {
      if (!this.objects.has(key) && !this.checkpoints.has(key)) {
        throw new ApiError(404, 'OBJECT_NOT_FOUND', 'unknown object');
      }
      const obj = this._obj(key);
      const children = this._childrenIndex();
      const { tips, suspended } = this._objectTips(key, children);

      // Merge hot graph nodes with cold (archived) nodes; same digest = same node.
      let cold;
      try {
        cold = this._coldNodesForObject(key); // throws on archive tamper
      } catch (e) {
        if (e instanceof ArchiveError) throw new ApiError(409, e.code, e.message, e.details);
        throw e;
      }
      const merged = new Map(cold);
      for (const n of this.nodes.values()) if (n.object_key === key) merged.set(n.digest, n);

      const nodes = [...merged.values()]
        .map((n) => {
          const archived = !this.nodes.has(n.digest);
          return n.kind === 'change'
            ? {
              digest: n.digest,
              kind: 'change',
              site: n.site,
              site_seq: n.site_seq,
              coverage_region: n.region,
              change_digest: n.change_digest,
              predecessors: n.predecessors,
              request_key: n.request_key,
              suspended: n.predecessors.some((p) => !this.nodes.has(p) && !this.archivedNodes.has(p)),
              archived,
            }
            : {
              digest: n.digest,
              kind: 'merge',
              decision_kind: n.decision_kind,
              parents: n.parents,
              contributions: n.contributions,
              case_id: n.case_id,
              reviewers: n.reviewers,
              archived,
            };
        })
        .sort((a, b) => (a.digest < b.digest ? -1 : 1));
      const edges = [];
      for (const n of nodes) {
        const parents = n.kind === 'change' ? n.predecessors : n.parents;
        for (const p of parents) {
          edges.push({ from: p, to: n.digest, live: this.nodes.has(p), archived: this.archivedNodes.has(p) && !this.nodes.has(p) });
        }
      }
      const stub = this.checkpoints.get(key);
      return {
        object_key: key,
        status: obj.hasOpenConflict ? 'conflict' : suspended.length ? 'suspended' : obj.currentReceipt ? 'settled' : 'pending',
        nodes,
        edges,
        tips,
        suspended,
        current: obj.currentReceipt ? this._publicReceipt(this.receipts.get(obj.currentReceipt)) : null,
        open_case: obj.openCaseId ? this._caseView(obj.openCaseId) : null,
        checkpoint: stub ? {
          boundary_seq: stub.boundary_seq,
          segment_id: stub.segment_id,
          created_at: stub.created_at,
          verified: this.segmentHealth.get(stub.segment_id)?.ok !== false,
        } : null,
      };
    });
  }

  async listCases() {
    return this.lock.runExclusive(() => {
      const children = this._childrenIndex();
      const out = [];
      for (const cs of this.reviews.values()) {
        if (cs.resolved) continue;
        const obj = this.objects.get(cs.objectKey);
        if (!obj || !obj.hasOpenConflict) continue;
        out.push(this._caseView(cs.caseId, children));
      }
      out.sort((a, b) => (a.case_id < b.case_id ? -1 : 1));
      return out;
    });
  }

  async getCase(caseId) {
    return this.lock.runExclusive(() => {
      const view = this._caseView(caseId);
      const obj = view && this.objects.get(view.object_key);
      if (!view || !obj || !obj.hasOpenConflict) {
        throw new ApiError(404, 'CASE_NOT_FOUND', 'no open conflict with this case id');
      }
      return view;
    });
  }

  _caseView(caseId, children = this._childrenIndex()) {
    const cs = this.reviews.get(caseId);
    if (!cs) return null;
    return {
      case_id: cs.caseId,
      object_key: cs.objectKey,
      disputed_regions: cs.disputed,
      candidates: cs.candidates,
      reviews: cs.reviewers.map((r) => ({ reviewer_id: r.reviewer_id, reviewer_name: r.reviewer_name, choices: r.choices })),
      awaiting: Math.max(0, 2 - cs.reviewers.length),
      tips: this._objectTips(cs.objectKey, children).tips,
    };
  }

  // -------------------------------------------------------------- receipts --

  _publicReceipt(r) {
    if (!r) return null;
    return { ...r };
  }

  _desensitise(r) {
    return {
      seq: r.seq,
      object_key: r.object_key,
      decision_kind: r.decision_kind,
      decision_node: r.decision_node,
      parents: r.parents,
      contributions: r.contributions,
      ...(r.case_id ? { case_id: r.case_id, reviewers: r.reviewers } : {}),
      prev_receipt: r.prev_receipt,
      emitted_at: r.emitted_at,
      hash: r.hash,
      sig: r.sig,
    };
  }

  async listReceipts(objectKey = null) {
    return this.lock.runExclusive(() => {
      const rows = [];
      for (const r of this.receipts.values()) {
        if (!objectKey || r.object_key === objectKey) rows.push(r);
      }
      // Cold chain: older receipts rotated into archive segments. Tamper in a
      // referenced segment surfaces as a deterministic integrity error rather
      // than silently hiding history.
      if (objectKey) {
        if (this.checkpoints.has(objectKey)) {
          try {
            for (const r of this._receiptsForObject(objectKey)) {
              if (!rows.some((x) => x.hash === r.hash)) rows.push(r);
            }
          } catch (e) {
            if (e instanceof ArchiveError) throw new ApiError(409, e.code, e.message, e.details);
            throw e;
          }
        }
      }
      rows.sort((a, b) => (a.object_key < b.object_key ? -1 : a.object_key > b.object_key ? 1 : a.seq - b.seq));
      return rows.map((r) => this._desensitise(r));
    });
  }

  async getReceipt(hash) {
    return this.lock.runExclusive(() => {
      const r = this.receipts.get(hash) || this._lookupColdReceipt(hash);
      if (!r) throw new ApiError(404, 'RECEIPT_NOT_FOUND', 'unknown receipt hash');
      return this._publicReceipt(r);
    });
  }

  _lookupColdReceipt(hash) {
    const info = this.archivedReceipts.get(hash);
    if (!info) return null;
    const frames = this.archive.readFramesByPayloadHash(info.segment_id, [info.payload_hash]);
    const ev = JSON.parse(frames[0].payload.toString('utf8'));
    return ev.receipt;
  }

  receiptChainForObject(objectKey) {
    return this._receiptsForObject(objectKey);
  }

  /**
   * Verify a (possibly externally supplied / tampered) receipt chain entry.
   * Returns {ok:true} or throws a deterministic error code.
   */
  async verifyReceipt(receipt) {
    return this.lock.runExclusive(() => this._verifyReceiptLocked(receipt));
  }

  _receiptKnown(hash) {
    return this.receipts.has(hash) || this.archivedReceipts.has(hash);
  }

  _storedReceipt(hash) {
    if (this.receipts.has(hash)) return this.receipts.get(hash);
    const cold = this.archivedReceipts.get(hash);
    if (!cold) return null;
    try {
      return this._lookupColdReceipt(hash);
    } catch (e) {
      if (e instanceof ArchiveError) throw new ApiError(409, e.code, e.message, e.details);
      throw e;
    }
  }

  _verifyReceiptLocked(receipt) {
    if (!receipt || typeof receipt !== 'object') throw new ApiError(400, 'RECEIPT_MALFORMED', 'receipt must be an object');
    const fields = [
      'seq', 'object_key', 'decision_kind', 'decision_node', 'parents', 'contributions',
      'prev_receipt', 'emitted_at',
    ];
    for (const f of fields) if (!(f in receipt)) throw new ApiError(422, 'RECEIPT_MALFORMED', `missing field ${f}`);

    const stored = this._storedReceipt(receipt.hash);
    const fieldsForHash = {
      seq: receipt.seq,
      object_key: receipt.object_key,
      decision_kind: receipt.decision_kind,
      decision_node: receipt.decision_node,
      parents: receipt.parents,
      contributions: receipt.contributions,
      ...(receipt.case_id ? { case_id: receipt.case_id, reviewers: receipt.reviewers } : {}),
      prev_receipt: receipt.prev_receipt,
      emitted_at: receipt.emitted_at,
    };

    // 1) Content hash: any edit to a covered field breaks this.
    const recomputed = digestOf(fieldsForHash);
    if (recomputed !== receipt.hash) {
      throw new ApiError(422, 'RECEIPT_HASH_MISMATCH', 'receipt hash does not match its contents');
    }
    // 2) Server signature over the same fields.
    const expectedSig = sign(this.config.receiptSecret, fieldsForHash);
    if (expectedSig !== receipt.sig) {
      throw new ApiError(422, 'RECEIPT_SIG_INVALID', 'receipt signature is invalid');
    }
    // 3) The receipt must be one this server emitted (hot or cold).
    if (!stored) {
      throw new ApiError(422, 'RECEIPT_UNKNOWN', 'receipt not found in the persisted chain');
    }
    if (stored.sig !== receipt.sig || stored.hash !== receipt.hash) {
      throw new ApiError(422, 'RECEIPT_HASH_MISMATCH', 'submitted receipt differs from stored receipt');
    }
    // 4) Chain linkage.
    if (receipt.prev_receipt !== stored.prev_receipt) {
      throw new ApiError(422, 'RECEIPT_CHAIN_BROKEN', 'prev_receipt linkage altered');
    }
    if (receipt.prev_receipt) {
      const prev = this._storedReceipt(receipt.prev_receipt);
      if (!prev) throw new ApiError(422, 'RECEIPT_CHAIN_BROKEN', 'previous receipt missing');
      if (prev.object_key !== receipt.object_key || prev.seq !== receipt.seq - 1) {
        throw new ApiError(422, 'RECEIPT_CHAIN_BROKEN', 'previous receipt is not the in-order predecessor');
      }
    }
    return { ok: true, object_key: receipt.object_key, seq: receipt.seq, decision_node: receipt.decision_node };
  }

  // =========================================================== ARCHIVE VIEWS

  async archiveStatus() {
    return this.lock.runExclusive(() => this._archiveStatusLocked());
  }

  _fileSize(p) {
    try { return fs.statSync(p).size; } catch { return 0; }
  }

  _archiveStatusLocked() {
    const walBytes = this._fileSize(this.config.walPath);
    let archiveBytes = 0;

    // Re-verify every segment on demand so restoring a tampered file flips the
    // panel back to verified without a restart (and freshly tampered files are
    // noticed immediately).
    this.archive.ensureDirs();
    const diskIds = fs.readdirSync(this.archive.segmentDir).filter((f) => f.endsWith('.seg')).map((f) => f.slice(0, -4));
    for (const id of diskIds) {
      try {
        const { manifest } = this.archive.readSegment(id);
        this.archive.remember(manifest);
        this.segmentHealth.set(id, { ok: true });
      } catch (e) {
        if (e instanceof ArchiveError) this.segmentHealth.set(id, { ok: false, error: e.code });
        else throw e;
      }
    }

    const idToObject = new Map();
    for (const m of this.archive.listManifests()) idToObject.set(m.segment_id, m.object_key);
    const segments = diskIds.sort().map((id) => {
      const m = this.archive.get(id);
      const bytes = this._fileSize(this.archive.segmentPath(id));
      archiveBytes += bytes;
      const health = this.segmentHealth.get(id) || { ok: false, error: 'ARCHIVE_UNREADABLE' };
      return {
        segment_id: id,
        object_key: m?.object_key || idToObject.get(id) || null,
        boundary_receipt_seq: m?.boundary_receipt_seq ?? null,
        chain_anchor: m?.chain_anchor ?? null,
        frame_count: m?.entries.length ?? 0,
        envelope_count: m?.envelope_index?.length ?? 0,
        content_digest: m?.content_digest ?? null,
        created_at: m?.created_at ?? null,
        bytes,
        verified: health.ok !== false,
        error: health.ok === false ? health.error : null,
      };
    });

    // Checkpoint chains whose referenced segment is unhealthy (incl. a segment
    // whose manifest can no longer be parsed).
    for (const stub of this.checkpoints.values()) {
      let sid = stub.segment_id;
      let guard = 0;
      while (sid && guard++ < 100000) {
        const h = this.segmentHealth.get(sid);
        if (h && !h.ok && !segments.some((s) => s.segment_id === sid)) {
          segments.push({
            segment_id: sid, object_key: stub.object_key, boundary_receipt_seq: null,
            chain_anchor: null, frame_count: 0, envelope_count: 0, content_digest: null,
            created_at: null, bytes: this._fileSize(this.archive.segmentPath(sid)),
            verified: false, error: h.error,
          });
        }
        const mm = this.archive.get(sid);
        sid = mm?.chain_anchor || null;
      }
    }

    const checkpoints = [...this.checkpoints.values()]
      .sort((a, b) => (a.object_key < b.object_key ? -1 : 1))
      .map((stub) => ({
        object_key: stub.object_key,
        boundary_seq: stub.boundary_seq,
        segment_id: stub.segment_id,
        chain_anchor: stub.chain_anchor,
        created_at: stub.created_at,
        archived_nodes: (stub.archived_nodes || []).length,
        verified: this.segmentHealth.get(stub.segment_id)?.ok !== false,
      }));

    const blockers = [];
    for (const s of segments) {
      if (!s.verified) blockers.push({ kind: 'segment_unverified', segment_id: s.segment_id, object_key: s.object_key, reason: s.error });
    }
    for (const marker of this.archive.listTaskMarkers()) {
      blockers.push({ kind: 'checkpoint_incomplete', task_id: marker.task_id, object_key: marker.object_key, phase: marker.phase });
    }
    for (const key of [...this.objects.keys()].sort()) {
      const reasons = this._checkpointBlockers(key, null);
      if (reasons.length && reasons[0] !== 'not_settled') blockers.push({ kind: 'object_not_archivable', object_key: key, reasons });
    }

    const objectProgress = [...this.checkpoints.keys()].sort().map((key) => this._restoreProgress(key));

    return {
      policy: { retain_decisions: this.config.retainDecisions || 0 },
      hot: {
        wal_bytes: walBytes,
        nodes: this.nodes.size,
        envelopes: this.requestIndex.size,
        receipts: this.receipts.size,
      },
      cold: { archive_bytes: archiveBytes, segment_count: segments.length, segments },
      checkpoints,
      tasks: this.archive.listTaskMarkers(),
      restore: { recent: this.restoreLog.slice(0, 20), objects: objectProgress },
      blockers,
    };
  }

  _restoreProgress(key) {
    const stub = this.checkpoints.get(key);
    const archived = (stub?.archived_nodes || []).filter((n) => (n.object_key || key) === key);
    // The boundary node is always materialised (it anchors the live view); any
    // other archived node that has been restored into hot storage counts too.
    let materialized = 0;
    for (const n of archived) if (this.nodes.has(n.digest)) materialized++;
    if (stub && this.nodes.has(stub.boundary_node.digest)) materialized++;
    const segmentStates = [];
    for (const part of this._safeSegmentsForObject(key)) {
      const health = this.segmentHealth.get(part.manifest.segment_id) || { ok: true };
      segmentStates.push({ segment_id: part.manifest.segment_id, verified: health.ok !== false, error: health.error || null });
    }
    const total = archived.length + (stub ? 1 : 0);
    return {
      object_key: key,
      archived_nodes: total,
      materialized_nodes: materialized,
      pending_nodes: Math.max(0, total - materialized),
      segments: segmentStates,
    };
  }

  _safeSegmentsForObject(key) {
    try {
      return this._segmentsForObject(key);
    } catch (e) {
      if (e instanceof ArchiveError) return [];
      throw e;
    }
  }

  async listProofs() {
    // Scan disk (not just cached manifests) so tampered segments are visible to
    // inspectors as unverifiable proofs rather than silently disappearing.
    return this.lock.runExclusive(() => {
      this.archive.ensureDirs();
      const out = [];
      for (const f of fs.readdirSync(this.archive.segmentDir)) {
        if (!f.endsWith('.seg')) continue;
        const id = f.slice(0, -4);
        const m = this.archive.get(id);
        if (m) {
          out.push(this.archive.proofView(m));
        } else {
          const h = this.segmentHealth.get(id) || { ok: false, error: 'ARCHIVE_UNREADABLE' };
          out.push({
            segment_id: id, object_key: null, boundary_receipt_seq: null, created_at: null,
            frame_count: 0, content_digest: null, chain_anchor: null, envelope_count: 0,
            verified: false, error: h.error, sig: null,
          });
        }
      }
      return out.sort((a, b) => (a.segment_id < b.segment_id ? 1 : a.segment_id > b.segment_id ? -1 : 0));
    });
  }

  async getProof(segmentId) {
    return this.lock.runExclusive(() => {
      try {
        const { manifest } = this.archive.readSegment(segmentId);
        this.archive.remember(manifest);
        return this.archive.proofView(manifest);
      } catch (e) {
        if (e instanceof ArchiveError) throw new ApiError(409, e.code, e.message, e.details);
        throw e;
      }
    });
  }

  /**
   * Inspector-safe proof pack: manifest metadata + per-frame digests, with the
   * envelope index (request keys / stations / sequences) fully redacted. The
   * pack is verified immediately before delivery; tamper is a stable 409.
   */
  async downloadProofPack(segmentId) {
    return this.lock.runExclusive(() => {
      let verified;
      try {
        verified = this.archive.readSegment(segmentId);
      } catch (e) {
        if (e instanceof ArchiveError) throw new ApiError(409, e.code, e.message, e.details);
        throw e;
      }
      const m = verified.manifest;
      return {
        format: m.format,
        segment_id: m.segment_id,
        object_key: m.object_key,
        boundary_receipt_seq: m.boundary_receipt_seq,
        created_at: m.created_at,
        chain_anchor: m.chain_anchor,
        content_digest: m.content_digest,
        frame_count: m.entries.length,
        envelope_count: (m.envelope_index || []).length,
        entries: m.entries, // type / sha256 / length only — no identity fields
        sig: m.sig,
        verified_at: nowIso(),
        verification: { ok: true },
      };
    });
  }

  /** Raw segment bytes for operator download (contains stations/request keys). */
  async readSegmentRaw(segmentId) {
    return this.lock.runExclusive(() => {
      try {
        const { manifest, frames } = this.archive.readSegment(segmentId);
        return {
          manifest,
          buffer: fs.readFileSync(this.archive.segmentPath(segmentId)),
          frame_count: frames.length,
        };
      } catch (e) {
        if (e instanceof ArchiveError) throw new ApiError(409, e.code, e.message, e.details);
        throw e;
      }
    });
  }
}
