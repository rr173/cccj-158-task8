import fs from 'node:fs';
import path from 'node:path';
import { Mutex } from './mutex.mjs';
import { ApiError } from './errors.mjs';
import { digestOf, sign } from './crypto-util.mjs';
import { validatePackage, buildSkeleton } from './skeleton.mjs';
import { ALGORITHMS } from './diff.mjs';

const nowIso = () => new Date().toISOString();
const KEY_RE = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/;
const STAGES = ['SKELETON_LEFT', 'SKELETON_RIGHT', 'DIFF', 'EXPLAIN', 'DONE'];

const byCreated = (a, b) =>
  a.created_at < b.created_at ? -1 : a.created_at > b.created_at ? 1 : a.task_key < b.task_key ? -1 : 1;

/**
 * One explanation node in the tamper-evident chain the EXPLAIN stage emits.
 * Fields come only from the deterministic evidence plan, so re-executing the
 * stage after a crash reproduces byte-identical nodes.
 */
function makeExplanationNode(spec, ordinal, prev) {
  const fields = { schema: 'diffdiag-explain/v1', ordinal, ...spec, prev: prev || null };
  return { ...fields, digest: digestOf(fields) };
}

/** Deterministic proof payload over everything the diagnostic stands on. */
function computeProofPayload(task) {
  const payload = {
    schema: 'diffdiag-proof/v1',
    task_key: task.task_key,
    algo_version: task.algo_version,
    left: {
      group: task.left_group,
      watermark: task.left_watermark,
      input_digest: digestOf([...task.left_inputs].sort()),
    },
    right: {
      group: task.right_group,
      watermark: task.right_watermark,
      input_digest: digestOf([...task.right_inputs].sort()),
    },
    divergence: task.diff ? task.diff.divergence : null,
    affected: task.diff ? task.diff.affected : [],
    explain_head: task.explain_head,
  };
  return payload;
}

export class DiffdiagStore {
  constructor(config) {
    this.config = config;
    this.lock = new Mutex();
    this.state = null;
  }

  static load(config) {
    fs.mkdirSync(config.dataDir, { recursive: true });
    const store = new DiffdiagStore(config);
    const tmp = config.statePath + '.tmp';
    // Residue of a crash between write and rename: the last good state file is
    // authoritative, the half-written tmp goes away.
    if (fs.existsSync(tmp)) fs.rmSync(tmp);
    if (fs.existsSync(config.statePath)) {
      let parsed = null;
      try {
        parsed = JSON.parse(fs.readFileSync(config.statePath, 'utf8'));
      } catch (e) {
        throw new Error(`diffdiag state is corrupt; refusing to start silently: ${e.message}`);
      }
      if (!parsed || parsed.schema !== 1) {
        throw new Error('diffdiag state has an unknown schema; refusing to start');
      }
      store.state = parsed;
    } else {
      store.state = store._initialState();
      store._persist();
    }
    store._recoverInterruptedTasks();
    return store;
  }

  _initialState() {
    return {
      schema: 1,
      ingest_seq: 0,
      receipts: {}, // hash -> {...receipt, ingest_seq} (global content store)
      groups: {}, // group_id -> {high_water, arrival: {hash: n}, objects: {object_key: [hash]}}
      algo_versions: [
        {
          version: 'chain-walk-v1',
          description: ALGORITHMS['chain-walk-v1'].description,
          status: 'active',
          created_at: nowIso(),
        },
        {
          version: 'skeleton-align-v2',
          description: ALGORITHMS['skeleton-align-v2'].description,
          status: 'available',
          created_at: nowIso(),
        },
      ],
      tasks: {},
    };
  }

  /** Atomic state write: tmp file + fsync + rename + directory fsync. */
  _persist() {
    const tmp = this.config.statePath + '.tmp';
    fs.writeFileSync(tmp, JSON.stringify(this.state));
    const fd = fs.openSync(tmp, 'r+');
    fs.fsyncSync(fd);
    fs.closeSync(fd);
    fs.renameSync(tmp, this.config.statePath);
    const dfd = fs.openSync(path.dirname(this.config.statePath), 'r');
    fs.fsyncSync(dfd);
    fs.closeSync(dfd);
  }

  /**
   * Crash recovery: a task that was claimed but never stage-persisted returns
   * to "not started"; a task with a persisted stage stays running and resumes
   * from exactly that stage on the next worker cycle.
   */
  _recoverInterruptedTasks() {
    let changed = false;
    for (const t of Object.values(this.state.tasks)) {
      if (t.state === 'running' && t.stage === null) {
        t.state = 'queued';
        t.started_at = null;
        changed = true;
      }
    }
    if (changed) this._persist();
  }

  _maybeCrash(point) {
    if (this.config.crashPoint === point) process.exit(3);
  }

  // ------------------------------------------------------------- ingest --

  _groupIndexes(groupId) {
    const receiptsByHash = new Map(Object.entries(this.state.receipts));
    const chains = new Map();
    const hashByObjSeq = new Map();
    const nodeIndex = new Map();
    const g = this.state.groups[groupId];
    if (g) {
      for (const [obj, hashes] of Object.entries(g.objects)) {
        chains.set(obj, [...hashes]);
        const nm = new Map();
        for (const h of hashes) {
          const r = this.state.receipts[h];
          if (!r) continue;
          hashByObjSeq.set(obj + '#' + r.seq, h);
          nm.set(r.decision_node, h);
        }
        nodeIndex.set(obj, nm);
      }
    }
    return { receiptsByHash, chains, hashByObjSeq, nodeIndex };
  }

  /**
   * Ingest one receipt package into a group. Validation is whole-package and
   * happens entirely before any mutation: a broken package (bad digest
   * format, bad chain signature, duplicate sequence, two bodies for one
   * node, unresolvable predecessor, cycle) is rejected as a unit and leaves
   * the store byte-identical.
   */
  async ingestPackage(body) {
    return this.lock.runExclusive(() => {
      if (!body || typeof body !== 'object' || Array.isArray(body)) {
        throw new ApiError(400, 'PACKAGE_MALFORMED', 'package must be a JSON object');
      }
      const groupId = body.group;
      if (typeof groupId !== 'string' || !KEY_RE.test(groupId)) {
        throw new ApiError(400, 'PACKAGE_MALFORMED', 'group must be a key string');
      }
      const known = this._groupIndexes(groupId);
      const sorted = validatePackage(body, known, this.config.receiptSecret);

      if (!this.state.groups[groupId]) {
        this.state.groups[groupId] = { high_water: 0, arrival: {}, objects: {} };
      }
      const g = this.state.groups[groupId];
      const obj = body.object_key;
      if (!g.objects[obj]) g.objects[obj] = [];
      let accepted = 0;
      let duplicates = 0;
      for (const r of sorted) {
        if (g.arrival[r.hash] !== undefined) {
          duplicates++;
          continue;
        }
        if (!this.state.receipts[r.hash]) {
          this.state.ingest_seq += 1;
          this.state.receipts[r.hash] = { ...r, ingest_seq: this.state.ingest_seq };
        }
        g.high_water += 1;
        g.arrival[r.hash] = g.high_water;
        g.objects[obj].push(r.hash);
        accepted++;
      }
      this._persist();
      return { group: groupId, object_key: obj, accepted, duplicates, watermark: g.high_water };
    });
  }

  async listObjects() {
    return this.lock.runExclusive(() => {
      const rows = [];
      for (const [gid, g] of Object.entries(this.state.groups)) {
        for (const [obj, hashes] of Object.entries(g.objects)) {
          const head = this.state.receipts[hashes[hashes.length - 1]];
          rows.push({
            group: gid,
            object_key: obj,
            receipts: hashes.length,
            head_seq: head ? head.seq : null,
            head_hash: head ? head.hash : null,
            watermark: g.high_water,
          });
        }
      }
      rows.sort((a, b) =>
        a.group < b.group ? -1 : a.group > b.group ? 1 : a.object_key < b.object_key ? -1 : 1);
      return rows;
    });
  }

  // ------------------------------------------------------ algorithm versions --

  async listAlgoVersions() {
    return this.lock.runExclusive(() => this.state.algo_versions.map((v) => ({ ...v })));
  }

  _activeAlgoVersion() {
    const active = this.state.algo_versions.find((v) => v.status === 'active');
    return active ? active.version : null;
  }

  async registerAlgoVersion(body) {
    return this.lock.runExclusive(() => {
      const version = body && body.version;
      if (typeof version !== 'string' || !KEY_RE.test(version)) {
        throw new ApiError(400, 'ALGO_MALFORMED', 'version must be a key string');
      }
      if (!ALGORITHMS[version]) {
        throw new ApiError(422, 'ALGO_VERSION_UNKNOWN', 'no implemented algorithm carries this version');
      }
      if (this.state.algo_versions.some((v) => v.version === version)) {
        throw new ApiError(409, 'ALGO_VERSION_EXISTS', 'version is already registered');
      }
      const row = {
        version,
        description: typeof body.description === 'string' ? body.description : ALGORITHMS[version].description,
        status: 'available',
        created_at: nowIso(),
      };
      this.state.algo_versions.push(row);
      this._persist();
      return row;
    });
  }

  async activateAlgoVersion(body) {
    return this.lock.runExclusive(() => {
      const version = body && body.version;
      const row = this.state.algo_versions.find((v) => v.version === version);
      if (!row) throw new ApiError(404, 'ALGO_VERSION_UNKNOWN', 'version is not registered');
      for (const v of this.state.algo_versions) v.status = v.version === version ? 'active' : 'available';
      this._persist();
      return { active: version };
    });
  }

  // ---------------------------------------------------------- comparisons --

  _freezeGroupInputs(groupId, side) {
    const g = this.state.groups[groupId];
    if (!g) throw new ApiError(404, 'GROUP_UNKNOWN', 'input group does not exist', { side });
    const inputs = [];
    for (const hashes of Object.values(g.objects)) inputs.push(...hashes);
    inputs.sort();
    if (inputs.length === 0) {
      throw new ApiError(409, 'INPUT_GROUP_EMPTY', 'input group has no receipts', { side });
    }
    // The watermark is the group's arrival high-water mark at this instant;
    // anything arriving later carries a higher arrival number and is excluded.
    return { inputs, watermark: g.high_water };
  }

  /**
   * Create a comparison task. In one critical section: pin both read
   * watermarks, freeze both input sets and pin the active algorithm version.
   * Same task_key + same inputs replays the original task; same key with any
   * input changed is a conflict; concurrent creates leave exactly one task.
   */
  async createComparison(body) {
    return this.lock.runExclusive(() => {
      if (!body || typeof body !== 'object' || Array.isArray(body)) {
        throw new ApiError(400, 'COMPARISON_MALFORMED', 'body must be a JSON object');
      }
      const { task_key, left_group, right_group } = body;
      if (typeof task_key !== 'string' || !KEY_RE.test(task_key)) {
        throw new ApiError(400, 'COMPARISON_MALFORMED', 'task_key must be a key string');
      }
      for (const [name, g] of [['left_group', left_group], ['right_group', right_group]]) {
        if (typeof g !== 'string' || !KEY_RE.test(g)) {
          throw new ApiError(400, 'COMPARISON_MALFORMED', `${name} must be a group key`);
        }
      }
      const fingerprint = digestOf({ left_group, right_group });
      const existing = this.state.tasks[task_key];
      if (existing) {
        if (existing.input_fingerprint === fingerprint) {
          return { task: this._taskView(existing), idempotent_replay: true };
        }
        throw new ApiError(409, 'TASK_INPUT_CONFLICT', 'task key was already used with different inputs', {
          task_key,
          state: existing.state,
        });
      }
      const left = this._freezeGroupInputs(left_group, 'left');
      const right = this._freezeGroupInputs(right_group, 'right');
      const task = {
        task_key,
        input_fingerprint: fingerprint,
        left_group,
        right_group,
        left_watermark: left.watermark,
        right_watermark: right.watermark,
        left_inputs: left.inputs,
        right_inputs: right.inputs,
        algo_version: this._activeAlgoVersion(),
        state: 'queued',
        stage: null,
        created_at: nowIso(),
        started_at: null,
        finished_at: null,
        cancelled_at: null,
        skeleton_left: null,
        skeleton_right: null,
        diff: null,
        evidence_plan: null,
        explanations: [],
        explain_head: null,
        proof: null,
        error: null,
      };
      this.state.tasks[task_key] = task;
      this._persist();
      return { task: this._taskView(task), idempotent_replay: false };
    });
  }

  _taskView(t) {
    return {
      task_key: t.task_key,
      state: t.state,
      stage: t.stage,
      algo_version: t.algo_version,
      left_group: t.left_group,
      right_group: t.right_group,
      left_watermark: t.left_watermark,
      right_watermark: t.right_watermark,
      left_inputs: t.left_inputs.length,
      right_inputs: t.right_inputs.length,
      inputs_released: t.state === 'cancelled',
      proof_digest: t.proof ? t.proof.digest : null,
      explain_head: t.explain_head,
      error: t.error,
      created_at: t.created_at,
      started_at: t.started_at,
      finished_at: t.finished_at,
      cancelled_at: t.cancelled_at,
    };
  }

  async listComparisons() {
    return this.lock.runExclusive(() =>
      Object.values(this.state.tasks).sort(byCreated).map((t) => this._taskView(t)));
  }

  async getComparison(taskKey) {
    return this.lock.runExclusive(() => {
      const t = this.state.tasks[taskKey];
      if (!t) throw new ApiError(404, 'TASK_UNKNOWN', 'no such comparison task');
      return this._taskView(t);
    });
  }

  /**
   * Cancel a task that has not started. Conditional on state: a racing worker
   * claim and a cancel can never both succeed. Cancelling releases the frozen
   * inputs and stage outputs; a completed diagnostic is never touched.
   */
  async cancelComparison(taskKey) {
    return this.lock.runExclusive(() => {
      const t = this.state.tasks[taskKey];
      if (!t) throw new ApiError(404, 'TASK_UNKNOWN', 'no such comparison task');
      if (t.state === 'cancelled') return { task: this._taskView(t), idempotent_replay: true };
      if (t.state === 'queued') {
        t.state = 'cancelled';
        t.cancelled_at = nowIso();
        t.left_inputs = [];
        t.right_inputs = [];
        t.skeleton_left = null;
        t.skeleton_right = null;
        t.evidence_plan = null;
        this._persist();
        return { task: this._taskView(t), idempotent_replay: false };
      }
      if (t.state === 'running') {
        throw new ApiError(409, 'TASK_ALREADY_STARTED', 'task has started and can no longer be cancelled');
      }
      if (t.state === 'done') {
        throw new ApiError(409, 'TASK_ALREADY_DONE', 'a completed diagnostic is never cancelled or overwritten');
      }
      throw new ApiError(409, 'TASK_NOT_CANCELLABLE', 'task is in a terminal failed state');
    });
  }

  async getDiagnostic(taskKey) {
    return this.lock.runExclusive(() => {
      const t = this.state.tasks[taskKey];
      if (!t) throw new ApiError(404, 'TASK_UNKNOWN', 'no such comparison task');
      if (t.state !== 'done') {
        throw new ApiError(409, 'DIAGNOSTIC_NOT_READY', 'a diagnostic exists only once the task is done', {
          state: t.state,
          stage: t.stage,
        });
      }
      return {
        task_key: t.task_key,
        algo_version: t.algo_version,
        left_group: t.left_group,
        right_group: t.right_group,
        left_watermark: t.left_watermark,
        right_watermark: t.right_watermark,
        skeleton_left: t.skeleton_left,
        skeleton_right: t.skeleton_right,
        divergence: t.diff.divergence,
        affected: t.diff.affected,
        explanations: t.explanations,
        explain_head: t.explain_head,
        proof_digest: t.proof.digest,
      };
    });
  }

  // --------------------------------------------------------------- proofs --

  _proofView(t) {
    return {
      task_key: t.task_key,
      state: t.state,
      algo_version: t.algo_version,
      left_watermark: t.left_watermark,
      right_watermark: t.right_watermark,
      proof_digest: t.proof ? t.proof.digest : null,
      explain_head: t.explain_head,
    };
  }

  async listProofs() {
    return this.lock.runExclusive(() =>
      Object.values(this.state.tasks).sort(byCreated).map((t) => this._proofView(t)));
  }

  async getProof(taskKey) {
    return this.lock.runExclusive(() => {
      const t = this.state.tasks[taskKey];
      if (!t) throw new ApiError(404, 'PROOF_UNKNOWN', 'no proof for this task_key');
      return { ...this._proofView(t), verification: this._verifyTaskProofLocked(t) };
    });
  }

  /**
   * Recompute every layer of the proof from the stored task. Any tampering
   * with the explanation chain, the diagnostic or the proof itself surfaces
   * as a deterministic code instead of a silent pass.
   */
  _verifyTaskProofLocked(t) {
    if (t.state !== 'done' || !t.proof) return { ok: false, code: 'PROOF_ABSENT' };
    let prev = null;
    for (let i = 0; i < t.explanations.length; i++) {
      const n = t.explanations[i];
      const { digest, ...fields } = n;
      if (n.ordinal !== i || fields.prev !== prev || digestOf(fields) !== digest) {
        return { ok: false, code: 'EXPLANATION_CHAIN_BROKEN', layer: 'explanation_chain', ordinal: i };
      }
      prev = digest;
    }
    if (prev !== t.explain_head) {
      return { ok: false, code: 'EXPLANATION_CHAIN_BROKEN', layer: 'explanation_head' };
    }
    const payload = computeProofPayload(t);
    if (digestOf(payload) !== t.proof.digest) {
      return { ok: false, code: 'PROOF_MISMATCH', layer: 'proof_digest' };
    }
    if (sign(this.config.receiptSecret, payload) !== t.proof.sig) {
      return { ok: false, code: 'PROOF_SIG_INVALID', layer: 'proof_sig' };
    }
    return { ok: true, proof_digest: t.proof.digest };
  }

  async verifyProof(body) {
    return this.lock.runExclusive(() => {
      if (body && typeof body.task_key === 'string') {
        const t = this.state.tasks[body.task_key];
        if (!t) throw new ApiError(404, 'PROOF_UNKNOWN', 'no proof for this task_key');
        return { task_key: t.task_key, ...this._verifyTaskProofLocked(t) };
      }
      if (body && body.proof && typeof body.proof === 'object') {
        const p = body.proof;
        if (typeof p.task_key !== 'string' || typeof p.digest !== 'string') {
          throw new ApiError(400, 'PROOF_MALFORMED', 'proof must carry task_key and digest');
        }
        const t = this.state.tasks[p.task_key];
        if (!t || !t.proof) throw new ApiError(404, 'PROOF_UNKNOWN', 'no stored proof for this task_key');
        const internal = this._verifyTaskProofLocked(t);
        if (!internal.ok) return { task_key: t.task_key, ...internal };
        if (t.proof.digest !== p.digest) {
          return { task_key: t.task_key, ok: false, code: 'PROOF_MISMATCH', layer: 'submitted_digest' };
        }
        return { task_key: t.task_key, ok: true, proof_digest: t.proof.digest, submitted: 'matches_stored' };
      }
      throw new ApiError(400, 'PROOF_MALFORMED', 'provide task_key or proof');
    });
  }

  // --------------------------------------------------------------- worker --

  _frozenReceipts(hashes) {
    return hashes.map((h) => this.state.receipts[h]).filter(Boolean);
  }

  /** Claim the next unit of work: a resumable running task, else the oldest queued one. */
  async _claimNextTask() {
    return this.lock.runExclusive(() => {
      const tasks = Object.values(this.state.tasks).sort(byCreated);
      const resumable = tasks.find((t) => t.state === 'running');
      if (resumable) return resumable.task_key;
      const queued = tasks.find((t) => t.state === 'queued');
      if (!queued) return null;
      queued.state = 'running';
      queued.started_at = nowIso();
      this._persist();
      this._maybeCrash('after_claim');
      return queued.task_key;
    });
  }

  /**
   * Advance one task through its persisted stages. Every stage commits its
   * output and the stage marker in one atomic persist, so a crash leaves the
   * task either at the last completed stage (resume) or — if nothing was ever
   * persisted — back at not-started. Explanation nodes are appended one
   * atomic persist at a time and resume skips ordinals already present, so a
   * node is never written twice.
   */
  async _runTaskStages(taskKey) {
    for (;;) {
      const done = await this.lock.runExclusive(() => this._advanceStageLocked(taskKey));
      if (done) return;
    }
  }

  _advanceStageLocked(taskKey) {
    const t = this.state.tasks[taskKey];
    if (!t || t.state !== 'running') return true;

    if (t.stage === null) {
      t.skeleton_left = buildSkeleton(this._frozenReceipts(t.left_inputs));
      t.stage = 'SKELETON_LEFT';
      this._persist();
      this._maybeCrash('after_stage:SKELETON_LEFT');
      return false;
    }
    if (t.stage === 'SKELETON_LEFT') {
      t.skeleton_right = buildSkeleton(this._frozenReceipts(t.right_inputs));
      t.stage = 'SKELETON_RIGHT';
      this._persist();
      this._maybeCrash('after_stage:SKELETON_RIGHT');
      return false;
    }
    if (t.stage === 'SKELETON_RIGHT') {
      const algo = ALGORITHMS[t.algo_version];
      if (!algo) {
        t.state = 'failed';
        t.error = { code: 'ALGO_VERSION_UNKNOWN', message: 'pinned algorithm version is not implemented' };
        this._persist();
        return true;
      }
      const result = algo.run(
        { receipts: this._frozenReceipts(t.left_inputs), skeleton: t.skeleton_left },
        { receipts: this._frozenReceipts(t.right_inputs), skeleton: t.skeleton_right });
      t.diff = { divergence: result.divergence, affected: result.affected };
      t.evidence_plan = result.evidence;
      t.stage = 'DIFF';
      this._persist();
      this._maybeCrash('after_stage:DIFF');
      return false;
    }
    if (t.stage === 'DIFF') {
      const plan = t.evidence_plan || [];
      while (t.explanations.length < plan.length) {
        const ordinal = t.explanations.length;
        const node = makeExplanationNode(plan[ordinal], ordinal, t.explain_head);
        t.explanations.push(node);
        t.explain_head = node.digest;
        this._persist();
        this._maybeCrash(`during_explain:${ordinal + 1}`);
      }
      t.stage = 'EXPLAIN';
      this._persist();
      this._maybeCrash('after_stage:EXPLAIN');
      return false;
    }
    if (t.stage === 'EXPLAIN') {
      const payload = computeProofPayload(t);
      t.proof = { ...payload, digest: digestOf(payload), sig: sign(this.config.receiptSecret, payload) };
      t.state = 'done';
      t.stage = 'DONE';
      t.finished_at = nowIso();
      this._persist();
      return true;
    }
    return true;
  }

  /** Drive the worker until no queued or resumable task remains. */
  async runWorkerCycle() {
    const processed = [];
    for (;;) {
      const key = await this._claimNextTask();
      if (!key) break;
      await this._runTaskStages(key);
      const t = this.state.tasks[key];
      processed.push({ task_key: key, state: t ? t.state : 'unknown' });
    }
    return processed;
  }
}

export { STAGES };
