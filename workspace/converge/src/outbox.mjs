import crypto from 'node:crypto';
import fs from 'node:fs';
import { Wal } from './wal.mjs';
import { Mutex } from './mutex.mjs';
import { canonical, digestOf } from './crypto-util.mjs';

/**
 * Reliable event subscription (transactional outbox) for downstream
 * compliance systems.
 *
 * Durability model
 * ----------------
 *  - Business frames (decision / checkpoint stub) and the matching outbox
 *    event frames are appended to the business WAL in ONE write+fsync batch
 *    (appendWithBusiness). A crash can therefore never expose a committed
 *    decision that has no durable notification; a torn tail removes the whole
 *    batch, and startup reconciliation regenerates any event whose business
 *    frame survived an exotic partial write.
 *  - Outbox event frames (business WAL type 6) are never removed by the
 *    hot/cold trim: they are the global event log and the per-subscription
 *    sequence authority.
 *  - Per-subscription fan-out state lives in a SECOND append-only log
 *    (outbox.wal, magic 'OE'): subscription documents (T_SUB) and delivery
 *    items (T_ITEM), latest-write-wins by key.
 *
 * Ordering / cursors
 * ------------------
 *  - Events carry a globally monotonic `seq` and a stable `event_id`.
 *  - Each subscription has contiguous delivery items by event seq. The
 *    confirmed cursor only advances across an acked prefix: it can never move
 *    backwards and unacked events are never skipped. A dead item is the head
 *    of the queue and blocks everything behind it.
 *  - A dispatch BATCH binds a signing-key version at creation; retries of the
 *    same batch keep that version, the next batch (and a manual dead-letter
 *    resend, which opens a new batch) uses the then-current version.
 *
 * Failure isolation
 * -----------------
 *  - The HTTP pump never runs inside the business mutex and every delivery
 *    error is contained: a slow/dead callback can only delay notifications,
 *    never envelope submission, adjudication or history queries.
 *  - Timeouts and non-2xx responses use capped exponential backoff; after the
 *    attempt cap the item enters the dead-letter queue for manual resend with
 *    its original event id and business order intact.
 */

// Business-WAL frame carrying one durable outbox event. Never trimmed.
export const T_OUTBOX_EVENT = 6;

// Outbox-WAL ('OE') frame types.
const T_SUB = 1;
const T_ITEM = 2;

export const OUTBOX_MAGIC = Buffer.from([0x4f, 0x45]); // 'OE'

export const EVENT_KINDS = ['decision.created', 'case.closed', 'cold_segment.generated'];

const KEY_RE = /^[A-Za-z0-9:._\-+/=]{16,256}$/;
const SUB_ID_RE = /^sub-[0-9a-f]{12}$/;

export class OutboxError extends Error {
  constructor(status, code, message, details = undefined) {
    super(message);
    this.status = status;
    this.code = code;
    this.details = details;
  }
}

function nowIso() {
  return new Date().toISOString();
}
function randomId(prefix) {
  return `${prefix}-${crypto.randomBytes(6).toString('hex')}`;
}
function stableEventId(kind, parts) {
  return digestOf({ kind, ...parts });
}

/** Stable event ids for the three business moments. Deterministic so a
 *  reconciliation pass and a live append produce the identical id. */
export const eventIds = {
  decisionCreated: (receiptHash) => stableEventId('decision.created', { receipt_hash: receiptHash }),
  caseClosed: (caseId, receiptHash) => stableEventId('case.closed', { case_id: caseId, receipt_hash: receiptHash }),
  coldSegment: (segmentId) => stableEventId('cold_segment.generated', { segment_id: segmentId }),
};

export class OutboxManager {
  constructor(config, businessWal) {
    this.config = config;
    this.businessWal = businessWal;
    // Derive a default path for hand-built configs (tests construct one inline)
    // while loadConfig's explicit outboxPath still wins when present.
    const outboxPath = config.outboxPath
      || (businessWal?.filePath ? `${businessWal.filePath.replace(/converge\.wal$/, '')}outbox.wal` : undefined);
    this.wal = new Wal(outboxPath, OUTBOX_MAGIC);
    this.lock = new Mutex();

    // Defensive defaults so a minimal inline config (verification scripts) works.
    this.deliveryTimeoutMs = config.deliveryTimeoutMs ?? 4000;
    this.deliveryBackoffBaseMs = config.deliveryBackoffBaseMs ?? 200;
    this.deliveryBackoffMaxMs = config.deliveryBackoffMaxMs ?? 30000;
    this.deliveryMaxAttempts = config.deliveryMaxAttempts ?? 5;
    this.deliveryBatchSize = config.deliveryBatchSize ?? 4;
    this.deliveryLeaseMs = config.deliveryLeaseMs ?? 60000;
    this.pumpIntervalMs = config.pumpIntervalMs ?? 500;

    this.events = new Map(); // event_seq -> event doc
    this.maxSeq = 0;
    this._ids = new Set(); // durable event ids (dedup / reconciliation)

    this.subs = new Map(); // sub_id -> subscription doc
    this.items = new Map(); // `${sub_id}|${event_seq}` -> item doc

    this._pumping = new Set(); // sub ids with an in-process dispatch cycle
    this._timer = null;
  }

  // ------------------------------------------------------------- lifecycle --

  open() {
    this.wal.open();
    const { frames, truncatedBytes } = this.wal.readAllFrames();
    if (truncatedBytes > 0) {
      const total = fs.statSync(this.wal.filePath).size;
      this.wal.truncate(total - truncatedBytes);
    }
    for (const f of frames) this._replay(f.type, f.payload);
  }

  /** Replay one durable outbox event frame (business WAL type 6). */
  indexReplayedEvent(ev) {
    if (this._ids.has(ev.event_id)) return;
    this._ids.add(ev.event_id);
    this.events.set(ev.seq, ev);
    if (ev.seq > this.maxSeq) this.maxSeq = ev.seq;
  }

  /**
   * Called by the store AFTER business-WAL replay + settlement recovery:
   * close any "business committed but event frame missing" gap deterministically
   * and ensure every subscription has its fan-out items.
   *
   * `descriptors` must be in business-WAL append order:
   *   {kind, event_id, object_key, business_cursor, occurred_at, data}
   */
  reconcile(descriptors) {
    for (const d of descriptors) {
      if (!this._ids.has(d.event_id)) {
        this._appendEventOnly(d);
      }
    }
    for (const ev of [...this.events.values()].sort((a, b) => a.seq - b.seq)) {
      this._fanoutLocked(ev);
    }
  }

  startPump() {
    if (this.pumpIntervalMs <= 0) return;
    this._timer = setInterval(() => {
      this.pumpAllSubscriptions().catch((e) => console.error('outbox pump error:', e?.message || e));
    }, this.pumpIntervalMs);
    this._timer.unref?.();
  }

  close() {
    if (this._timer) { clearInterval(this._timer); this._timer = null; }
    this.wal.close();
  }

  // -------------------------------------------------------------- replay ---

  _replay(type, payloadBuf) {
    const doc = JSON.parse(payloadBuf.toString('utf8'));
    if (type === T_SUB) {
      this.subs.set(doc.id, doc);
    } else if (type === T_ITEM) {
      // Latest frame wins; an explicit tombstone deletes.
      if (doc._deleted) this.items.delete(doc.id);
      else this.items.set(doc.id, doc);
    }
  }

  _persistSub(sub) {
    this.wal.append(T_SUB, JSON.stringify(sub));
    this.subs.set(sub.id, sub);
  }

  _persistItem(item) {
    this.wal.append(T_ITEM, JSON.stringify(item));
    this.items.set(item.id, item);
  }

  // ------------------------------------------------------- event enqueue ---

  /** Build an event doc (assigns the next global seq) and claim that seq.
   *  Caller holds the business mutex, so seq assignment is serialised with
   *  every business move; incrementing maxSeq immediately means a multi-event
   *  batch (adjudication -> decision + case-closed) gets distinct seqs. */
  _makeEvent(desc) {
    if (this._ids.has(desc.event_id)) {
      // Reconciliation-safe: same business moment never gets two events.
      return this.eventsById(desc.event_id);
    }
    const seq = this.maxSeq + 1;
    this.maxSeq = seq;
    const ev = {
      event_id: desc.event_id,
      seq,
      kind: desc.kind,
      object_key: desc.object_key,
      business_cursor: desc.business_cursor,
      occurred_at: desc.occurred_at || nowIso(),
      data: desc.data || {},
      logged_at: nowIso(),
    };
    return ev;
  }

  eventsById(eventId) {
    for (const ev of this.events.values()) if (ev.event_id === eventId) return ev;
    return null;
  }

  /**
   * The transactional outbox primitive. MUST be called while the caller holds
   * the business mutex (business move not yet durable). Business frames and
   * event frames land as a single fsync; the events become indexable and are
   * fanned out to subscriptions immediately afterwards.
   */
  appendWithBusiness(businessEntries, descriptors) {
    const events = descriptors.map((d) => this._makeEvent(d));
    const entries = [
      ...businessEntries,
      ...events.map((ev) => [T_OUTBOX_EVENT, JSON.stringify(ev)]),
    ];
    this.businessWal.appendBatch(entries); // single write + fsync
    for (const ev of events) {
      this._ids.add(ev.event_id);
      this.events.set(ev.seq, ev);
      this.maxSeq = Math.max(this.maxSeq, ev.seq);
    }
    // Fan-out persistence is best-effort post-commit: if it fails (disk), the
    // event is durable and reconcile() on the next process repairs the items.
    // It must never turn a committed business move into a user-visible failure.
    for (const ev of events) {
      try { this._fanoutLocked(ev); } catch (e) { console.error('outbox fanout error:', e?.message || e); }
    }
    return events;
  }

  /** Reconciliation path: append only the event frame(s) (no business frame). */
  _appendEventOnly(desc) {
    const ev = this._makeEvent(desc);
    this.businessWal.append(T_OUTBOX_EVENT, JSON.stringify(ev));
    this._ids.add(ev.event_id);
    this.events.set(ev.seq, ev);
    this.maxSeq = Math.max(this.maxSeq, ev.seq);
  }

  /** Create one pending delivery item per subscription that elected this kind
   *  and existed before the event (start_after_seq). Idempotent per item key. */
  _fanoutLocked(ev) {
    for (const sub of this.subs.values()) {
      if (sub.status === 'retired') continue;
      if (sub.start_after_seq >= ev.seq) continue;
      if (!sub.event_kinds.includes(ev.kind)) continue;
      const id = `${sub.id}|${ev.seq}`;
      if (this.items.has(id)) continue;
      const now = nowIso();
      this._persistItem({
        id, subscription_id: sub.id, event_seq: ev.seq, event_id: ev.event_id,
        kind: ev.kind, object_key: ev.object_key,
        state: 'pending', attempts: 0, resend_count: 0,
        available_at: now, lease_until: null, batch_id: null, key_version: null,
        last_http_status: null, last_error: null, last_envelope: null,
        created_at: now, updated_at: now, confirmed_at: null, dead_at: null,
      });
    }
  }

  // ---------------------------------------------------- subscription admin --

  _redacted(sub, extra = {}) {
    return {
      id: sub.id,
      callback_url: sub.callback_url,
      event_kinds: sub.event_kinds,
      status: sub.status,
      current_key_version: sub.current_key_version,
      start_after_seq: sub.start_after_seq,
      created_at: sub.created_at,
      updated_at: sub.updated_at,
      ...extra,
    };
  }

  createSubscription(body) {
    return this.lock.runExclusive(() => this._createSubscriptionLocked(body || {}));
  }

  _createSubscriptionLocked(body) {
    if (!body || typeof body !== 'object' || Array.isArray(body)) {
      throw new OutboxError(400, 'BAD_REQUEST', 'subscription must be a JSON object');
    }
    const url = body.callback_url;
    if (typeof url !== 'string' || !/^https?:\/\//.test(url) || url.length > 2048) {
      throw new OutboxError(400, 'BAD_REQUEST', 'callback_url must be an http(s) URL');
    }
    try {
      const u = new URL(url);
      if (!u.hostname) throw new Error();
    } catch {
      throw new OutboxError(400, 'BAD_REQUEST', 'callback_url is not a valid URL');
    }
    let kinds = body.event_kinds;
    if (!Array.isArray(kinds) || !kinds.length) {
      throw new OutboxError(400, 'BAD_REQUEST', 'event_kinds must be a non-empty array');
    }
    const badKind = kinds.find((k) => !EVENT_KINDS.includes(k));
    if (badKind) throw new OutboxError(400, 'BAD_REQUEST', `unknown event kind ${badKind}`, { allowed: EVENT_KINDS });
    kinds = [...new Set(kinds)];
    const key = body.signing_key;
    if (typeof key !== 'string' || !KEY_RE.test(key)) {
      throw new OutboxError(400, 'BAD_REQUEST', 'signing_key must be 16-256 chars (letters/digits/:._-+/=)');
    }
    const now = nowIso();
    const sub = {
      id: randomId('sub'),
      callback_url: url,
      event_kinds: kinds,
      keys: { 1: key },
      current_key_version: 1,
      status: 'active',
      start_after_seq: this.maxSeq, // only events produced AFTER registration
      created_at: now,
      updated_at: now,
    };
    this._persistSub(sub);
    // New subscription registered while events already exist: fan out nothing
    // retrospectively; items for events after start_after_seq get created as
    // they arrive (and reconcile() covers a crash in this window).
    for (const ev of this.events.values()) this._fanoutLocked(ev);
    return this._redacted(sub);
  }

  _getSub(id) {
    if (typeof id !== 'string' || !SUB_ID_RE.test(id)) {
      throw new OutboxError(400, 'BAD_REQUEST', 'malformed subscription id');
    }
    const sub = this.subs.get(id);
    if (!sub) throw new OutboxError(404, 'SUBSCRIPTION_NOT_FOUND', 'unknown subscription');
    return sub;
  }

  pauseSubscription(id) {
    return this.lock.runExclusive(() => {
      const sub = this._getSub(id);
      if (sub.status === 'retired') throw new OutboxError(409, 'SUBSCRIPTION_RETIRED', 'subscription is retired');
      if (sub.status !== 'paused') {
        sub.status = 'paused';
        sub.updated_at = nowIso();
        this._persistSub(sub);
      }
      return this._redacted(sub);
    });
  }

  resumeSubscription(id) {
    return this.lock.runExclusive(() => {
      const sub = this._getSub(id);
      if (sub.status === 'retired') throw new OutboxError(409, 'SUBSCRIPTION_RETIRED', 'subscription is retired');
      if (sub.status !== 'active') {
        sub.status = 'active';
        sub.updated_at = nowIso();
        this._persistSub(sub);
      }
      return this._redacted(sub);
    });
  }

  rotateKey(id, body) {
    return this.lock.runExclusive(() => {
      const sub = this._getSub(id);
      const key = body?.signing_key;
      if (typeof key !== 'string' || !KEY_RE.test(key)) {
        throw new OutboxError(400, 'BAD_REQUEST', 'signing_key must be 16-256 chars (letters/digits/:._-+/=)');
      }
      // Old key versions are retained: in-flight batches stay verifiable.
      const nextVersion = sub.current_key_version + 1;
      sub.keys = { ...sub.keys, [nextVersion]: key };
      sub.current_key_version = nextVersion;
      sub.updated_at = nowIso();
      this._persistSub(sub);
      return this._redacted(sub);
    });
  }

  listSubscriptions() {
    return this.lock.runExclusive(() =>
      [...this.subs.values()].sort((a, b) => (a.created_at < b.created_at ? -1 : 1)).map((s) => this._subView(s)));
  }

  getSubscription(id) {
    return this.lock.runExclusive(() => this._subView(this._getSub(id)));
  }

  _itemsFor(subId) {
    const prefix = `${subId}|`;
    return [...this.items.values()]
      .filter((i) => i.id.startsWith(prefix))
      .sort((a, b) => a.event_seq - b.event_seq);
  }

  /** Operational view (admin/operator): no signing material, but URLs and
   *  dead-letter reasons are operational data. */
  _subView(sub) {
    const items = this._itemsFor(sub.id);
    const pending = items.filter((i) => i.state === 'pending');
    const sending = items.filter((i) => i.state === 'sending');
    const dead = items.filter((i) => i.state === 'dead');
    const acked = items.filter((i) => i.state === 'acked').sort((a, b) => b.event_seq - a.event_seq);

    // Contiguous confirmed prefix across the global event stream: the cursor
    // stops at the first item that is not acked, so it never skips an event.
    let confirmedSeq = sub.start_after_seq;
    for (const i of items.sort((a, b) => a.event_seq - b.event_seq)) {
      if (i.state === 'acked' && i.event_seq === confirmedSeq + 1) confirmedSeq = i.event_seq;
      else if (i.state === 'acked' && i.event_seq <= confirmedSeq) continue;
      else break;
    }
    const lastAck = acked[0] || null;
    return this._redacted(sub, {
      pending_count: pending.length,
      in_flight_count: sending.length,
      dead_count: dead.length,
      confirmed_event_seq: confirmedSeq,
      last_acked: lastAck ? {
        event_seq: lastAck.event_seq,
        event_id: lastAck.event_id,
        kind: lastAck.kind,
        object_key: lastAck.object_key,
        confirmed_at: lastAck.confirmed_at,
        http_status: lastAck.last_http_status,
      } : null,
      head: items.find((i) => i.state !== 'acked') ? this._itemView(items.find((i) => i.state !== 'acked')) : null,
      dead_letter: dead.map((i) => this._itemView(i)),
    });
  }

  _itemView(i) {
    return {
      event_seq: i.event_seq,
      event_id: i.event_id,
      kind: i.kind,
      object_key: i.object_key,
      state: i.state,
      attempts: i.attempts,
      resend_count: i.resend_count,
      batch_id: i.batch_id,
      key_version: i.key_version,
      available_at: i.available_at,
      lease_until: i.lease_until,
      last_http_status: i.last_http_status,
      last_error: i.last_error,
      dead_at: i.dead_at,
      confirmed_at: i.confirmed_at,
      updated_at: i.updated_at,
    };
  }

  listItems(id, opts = {}) {
    return this.lock.runExclusive(() => {
      const sub = this._getSub(id);
      let items = this._itemsFor(sub.id);
      if (opts.state) items = items.filter((i) => i.state === opts.state);
      if (opts.limit) items = items.slice(-opts.limit);
      return items.map((i) => this._itemView(i));
    });
  }

  /**
   * Manual dead-letter resend (admin only). Opens a NEW batch (so it is signed
   * with the current key version) but preserves the stable event id and the
   * event seq — business order is unchanged and the receiver dedupes by id.
   */
  redeliver(id, eventSeq) {
    return this.lock.runExclusive(() => {
      const sub = this._getSub(id);
      const seq = Number(eventSeq);
      if (!Number.isInteger(seq)) throw new OutboxError(400, 'BAD_REQUEST', 'event_seq must be an integer');
      const item = this.items.get(`${sub.id}|${seq}`);
      if (!item) throw new OutboxError(404, 'ITEM_NOT_FOUND', 'no notification item for this event');
      if (item.state !== 'dead') {
        throw new OutboxError(409, 'ITEM_NOT_DEAD', 'only dead-letter items can be manually resent', { state: item.state });
      }
      const priorReason = item.last_error;
      item.state = 'pending';
      item.attempts = 0;
      item.resend_count += 1;
      item.available_at = nowIso();
      item.lease_until = null;
      item.batch_id = null; // new batch -> current key version at next dispatch
      item.key_version = null;
      item.last_error = `manual resend #${item.resend_count}; prior dead reason: ${priorReason}`;
      item.dead_at = null;
      item.updated_at = nowIso();
      this._persistItem(item);
      return this._itemView(item);
    });
  }

  // ----------------------------------------------------- inspector proofs --

  /**
   * Inspector-safe proof views: NO callback URL, NO signing keys, NO request
   * keys / site ids / image content (event payloads never contained them).
   * What remains proves what the compliance system was told and when it
   * acknowledged: stable event id, kind, object alt key, business cursor, the
   * exact signed envelope, key version and delivery confirmation.
   */
  listProofs() {
    return this.lock.runExclusive(() =>
      [...this.subs.values()].sort((a, b) => (a.created_at < b.created_at ? -1 : 1)).map((s) => {
        const view = this._subView(s);
        return {
          id: s.id,
          event_kinds: s.event_kinds,
          status: s.status,
          confirmed_event_seq: view.confirmed_event_seq,
          pending_count: view.pending_count,
          in_flight_count: view.in_flight_count,
          dead_count: view.dead_count,
          last_acked: view.last_acked,
          created_at: s.created_at,
        };
      }));
  }

  listProofItems(id, limit = 100) {
    return this.lock.runExclusive(() => {
      const sub = this._getSub(id);
      const items = this._itemsFor(sub.id)
        .filter((i) => i.state === 'acked')
        .slice(-Math.min(500, Math.max(1, limit || 100)));
      return items.map((i) => ({
        event_seq: i.event_seq,
        event_id: i.event_id,
        kind: i.kind,
        object_key: i.object_key,
        business_cursor: this.events.get(i.event_seq)?.business_cursor || null,
        occurred_at: this.events.get(i.event_seq)?.occurred_at || null,
        envelope: i.last_envelope ? JSON.parse(i.last_envelope) : null,
        delivery: {
          attempts: i.attempts,
          resend_count: i.resend_count,
          http_status: i.last_http_status,
          confirmed_at: i.confirmed_at,
        },
      }));
    });
  }

  // --------------------------------------------------------------- pump ----

  async pumpAllSubscriptions() {
    const ids = await this.lock.runExclusive(() =>
      [...this.subs.values()].filter((s) => s.status === 'active').map((s) => s.id));
    const results = [];
    for (const id of ids) {
      try { results.push(await this.pumpSubscription(id)); }
      catch (e) { results.push({ subscription_id: id, dispatched: 0, acked: 0, stopped: 'error', error: e?.message || String(e) }); }
    }
    return results;
  }

  /** Drive one subscription: reclaim dead leases, dispatch the next batch. */
  pumpSubscription(subId) {
    if (this._pumping.has(subId)) return Promise.resolve({ subscription_id: subId, dispatched: 0, acked: 0, stopped: 'already_running' });
    this._pumping.add(subId);
    return (async () => {
      try {
        const prepared = await this.lock.runExclusive(() => this._prepareBatchLocked(subId));
        if (!prepared) return { subscription_id: subId, dispatched: 0, acked: 0, stopped: 'idle' };
        if (prepared.stopped) return { subscription_id: subId, dispatched: 0, acked: 0, ...prepared };

        const { sub, batch } = prepared;
        let acked = 0;
        let stop = null;
        for (const item of batch) {
          const ev = this.events.get(item.event_seq);
          const envelope = this._buildEnvelope(sub, item, ev);
          item.last_envelope = JSON.stringify(envelope);
          const result = await this._deliver(sub, envelope);
          const finish = await this.lock.runExclusive(() =>
            this._finishItemLocked(sub, item, batch, envelope, result));
          if (finish.outcome === 'acked') { acked += 1; continue; }
          stop = { stopped: finish.outcome, event_seq: item.event_seq, event_id: item.event_id,
                   attempts: item.attempts, last_error: item.last_error };
          break;
        }
        return {
          subscription_id: subId,
          batch_id: batch[0].batch_id,
          dispatched: batch.length,
          acked,
          stopped: stop ? stop.stopped : 'batch_complete',
          ...(stop || {}),
        };
      } finally {
        this._pumping.delete(subId);
      }
    })();
  }

  /**
   * Select + bind (or resume) the next dispatch batch for a subscription.
   * A batch is a contiguous run of items from the unacked head. Items already
   * carrying a batch_id are a RESUMED batch and keep its bound key version;
   * a fresh batch binds the subscription's current key version.
   */
  _prepareBatchLocked(subId) {
    const sub = this.subs.get(subId);
    if (!sub) return null;
    if (sub.status === 'paused') return { stopped: 'paused' };
    if (sub.status === 'retired') return { stopped: 'retired' };

    let items = this._itemsFor(subId);
    const now = Date.now();

    // Crash/ack-loss protection: reclaim every expired in-flight batch. The
    // item actually being POSTed when the process died is redelivered (the
    // receiver dedupes by stable event id).
    for (const i of items) {
      if (i.state === 'sending' && i.lease_until && Date.parse(i.lease_until) <= now) {
        i.state = 'pending';
        i.lease_until = null;
        i.last_error = i.last_error || 'lease_expired_reclaimed';
        i.updated_at = nowIso();
        this._persistItem(i);
      }
    }
    items = this._itemsFor(subId);

    const head = items.find((i) => i.state !== 'acked');
    if (!head) return { stopped: 'all_confirmed' };
    if (head.state === 'dead') return { stopped: 'dead_letter', event_seq: head.event_seq, event_id: head.event_id, last_error: head.last_error };
    if (head.state === 'sending') return { stopped: 'in_flight', event_seq: head.event_seq };
    if (head.state === 'pending' && head.available_at && Date.parse(head.available_at) > now) {
      return { stopped: 'backoff', event_seq: head.event_seq, available_at: head.available_at };
    }

    let batch;
    let keyVersion;
    if (head.batch_id) {
      // Resume the open batch: every contiguous item sharing its batch id.
      batch = items.filter((i) => i.batch_id === head.batch_id && (i.state === 'pending' || i.state === 'sending'));
      keyVersion = head.key_version;
    } else {
      // New batch: contiguous available pending items, capped. A dead/sending
      // item or a not-due retry ends the run.
      batch = [];
      for (const i of items) {
        if (batch.length >= this.deliveryBatchSize) break;
        if (i.state === 'acked') continue;
        if (i.state === 'dead' || i.state === 'sending') break;
        if (i.batch_id) break; // another open batch ahead
        if (i.available_at && Date.parse(i.available_at) > now) break;
        batch.push(i);
      }
      if (!batch.length) return { stopped: 'backoff' };
      keyVersion = sub.current_key_version;
    }

    // Bind the batch durably before leaving the lock: key version and lease.
    const batchId = head.batch_id || randomId('batch');
    const leaseUntil = new Date(now + this.deliveryLeaseMs).toISOString();
    for (const i of batch) {
      i.state = 'sending';
      i.batch_id = batchId;
      i.key_version = keyVersion;
      i.lease_until = leaseUntil;
      i.updated_at = nowIso();
      this._persistItem(i);
    }
    return { sub, batch };
  }

  /** Apply one HTTP result to its item, all under the outbox lock. */
  _finishItemLocked(sub, item, batch, envelope, result) {
    // Re-read defensively: state may have changed (should not for sending items).
    const fresh = this.items.get(item.id);
    if (!fresh || fresh.batch_id !== batch[0].batch_id) return { outcome: 'superseded' };

    // Siblings bound to the same batch wait behind this item. Release them to
    // pending immediately (keeping batch_id + bound key_version — it is still
    // the same open batch) whether the head is retrying or dead, so they are
    // never left counted as in-flight waiting for the lease to expire.
    const releaseSiblings = () => {
      for (const other of batch) {
        if (other.id === item.id) continue;
        const o = this.items.get(other.id);
        if (o && o.state === 'sending' && o.batch_id === item.batch_id) {
          o.state = 'pending';
          o.lease_until = null;
          o.updated_at = nowIso();
          this._persistItem(o);
        }
      }
    };

    if (result.ok) {
      item.state = 'acked';
      item.lease_until = null;
      item.last_http_status = result.status;
      item.last_error = null;
      item.confirmed_at = nowIso();
      item.updated_at = nowIso();
      this._persistItem(item);
      return { outcome: 'acked' };
    }

    item.attempts += 1;
    item.last_http_status = result.status ?? null;
    item.last_error = result.error;
    item.updated_at = nowIso();
    if (item.attempts >= this.deliveryMaxAttempts) {
      item.state = 'dead';
      item.lease_until = null;
      item.dead_at = nowIso();
      this._persistItem(item);
      releaseSiblings();
      return { outcome: 'dead' };
    }
    // Capped exponential backoff; batch (and its key version) stays open.
    const delay = Math.min(
      this.deliveryBackoffMaxMs,
      this.deliveryBackoffBaseMs * 2 ** (item.attempts - 1));
    item.state = 'pending';
    item.available_at = new Date(Date.now() + delay).toISOString();
    item.lease_until = null;
    this._persistItem(item);
    releaseSiblings();
    return { outcome: 'retry', retry_after_ms: delay };
  }

  // ------------------------------------------------------------ delivery ----

  /**
   * The signed notification. Deliberately free of site id, request key, site
   * sequence and image content — only the object alternative key, stable ids,
   * business cursor, kind-specific desensitised data and signature.
   */
  _buildEnvelope(sub, item, ev) {
    return {
      schema: 'converge-event/v1',
      event_id: item.event_id,
      event_seq: item.event_seq,
      kind: item.kind,
      object_key: item.object_key,
      business_cursor: ev?.business_cursor || null,
      occurred_at: ev?.occurred_at || null,
      data: ev?.data || {},
      key_version: item.key_version,
      subscription_id: sub.id,
    };
  }

  async _deliver(sub, envelope) {
    const secret = sub.keys[String(envelope.key_version)];
    if (!secret) {
      // Configuration fault: fail deterministically so it lands in dead letter
      // rather than looping forever.
      return { ok: false, status: null, error: `signing key version ${envelope.key_version} missing` };
    }
    const body = JSON.stringify(envelope);
    const signature = crypto.createHmac('sha256', secret).update(canonical(envelope)).digest('hex');
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), this.deliveryTimeoutMs);
    try {
      const res = await fetch(sub.callback_url, {
        method: 'POST',
        headers: {
          'content-type': 'application/json',
          'x-converge-event': envelope.kind,
          'x-converge-event-id': envelope.event_id,
          'x-converge-key-version': String(envelope.key_version),
          'x-converge-signature': `v1=${signature}`,
        },
        body,
        signal: controller.signal,
      });
      if (res.ok) return { ok: true, status: res.status };
      const text = await safeText(res);
      return { ok: false, status: res.status, error: `HTTP ${res.status}${text ? `: ${text.slice(0, 200)}` : ''}` };
    } catch (e) {
      const aborted = e?.name === 'AbortError';
      return { ok: false, status: null, error: aborted ? `timeout after ${this.deliveryTimeoutMs}ms` : `${e?.name || 'Error'}: ${e?.message || e}` };
    } finally {
      clearTimeout(timer);
    }
  }
}

async function safeText(res) {
  try { return (await res.text()).slice(0, 200); } catch { return '';
  }
}
