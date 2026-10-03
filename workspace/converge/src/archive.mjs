import fs from 'node:fs';
import path from 'node:path';
import { ARCHIVE_MAGIC, encodeFrame, parseFrames } from './wal.mjs';
import { digestOf, sha256Hex, sign } from './crypto-util.mjs';

/**
 * Cold-archive layer.
 *
 * A checkpoint copies the frame-ancestry of an object's decisions up to a
 * boundary receipt into a content-addressed, read-only archive *segment*:
 *
 *   <dataDir>/archive/segments/<segmentId>.seg
 *
 * Segment layout is the same framing as the WAL (magic 'CA' instead of 'CV'),
 * materialised frames first, terminated by a type-9 MANIFEST frame. The manifest
 * lists every included frame's payload hash plus a chain anchor to the previous
 * segment, and is itself HMAC-signed with the receipt secret. Segments are
 * WORM: files are chmod 0444 and never rewritten.
 *
 * Crash phases (a marker file <dataDir>/archive/tasks/<taskId>.json tracks each
 * in-flight checkpoint; recovery is resume-or-rollback):
 *   PLAN      -> segment file written + fsynced
 *   VERIFIED  -> hot checkpoint stub durably appended to the WAL
 *   TRIMMED   -> hot WAL rotated without the archived frames (space reclaimed)
 *
 * Because a segment is content-addressed and immutable, a repeated/recovered
 * checkpoint never deletes twice: the trim only removes frames whose payload
 * hash belongs to the checkpoint's archived set.
 */

export const T_ARCHIVE_MANIFEST = 9;

export class ArchiveError extends Error {
  constructor(code, message, details = undefined) {
    super(message);
    this.code = code;
    this.details = details;
  }
}

export function isFsPermissionError(e) {
  return e && (e.code === 'EACCES' || e.code === 'EPERM' || e.code === 'EROFS');
}

export class ArchiveManager {
  constructor(config) {
    this.config = config;
    this.dir = path.join(config.dataDir, 'archive');
    this.segmentDir = path.join(this.dir, 'segments');
    this.taskDir = path.join(this.dir, 'tasks');
    this.segments = new Map(); // segmentId -> manifest (verified)
  }

  ensureDirs() {
    fs.mkdirSync(this.segmentDir, { recursive: true });
    fs.mkdirSync(this.taskDir, { recursive: true });
  }

  segmentPath(segmentId) {
    return path.join(this.segmentDir, `${segmentId}.seg`);
  }

  taskPath(taskId) {
    return path.join(this.taskDir, `${taskId}.json`);
  }

  // ------------------------------------------------------------- task marker --

  createTaskMarker(taskId, doc) {
    this.ensureDirs();
    const p = this.taskPath(taskId);
    const tmp = p + '.tmp';
    fs.writeFileSync(tmp, JSON.stringify(doc, null, 2));
    fs.renameSync(tmp, p);
  }

  updateTaskMarker(taskId, patch) {
    const p = this.taskPath(taskId);
    const doc = { ...JSON.parse(fs.readFileSync(p, 'utf8')), ...patch, updated_at: new Date().toISOString() };
    const tmp = p + '.tmp';
    fs.writeFileSync(tmp, JSON.stringify(doc, null, 2));
    fs.renameSync(tmp, p);
    return doc;
  }

  readTaskMarker(taskId) {
    try {
      return JSON.parse(fs.readFileSync(this.taskPath(taskId), 'utf8'));
    } catch (e) {
      if (e.code === 'ENOENT') return null;
      throw e;
    }
  }

  listTaskMarkers() {
    this.ensureDirs();
    return fs.readdirSync(this.taskDir)
      .filter((f) => f.endsWith('.json'))
      .map((f) => JSON.parse(fs.readFileSync(path.join(this.taskDir, f), 'utf8')))
      .sort((a, b) => ((a.created_at || '') < (b.created_at || '') ? -1 : 1));
  }

  removeTaskMarker(taskId) {
    try { fs.unlinkSync(this.taskPath(taskId)); } catch (e) { if (e.code !== 'ENOENT') throw e; }
  }

  // -------------------------------------------------------------- segments --

  /**
   * Build + fsync a segment. envelopeIndex carries the durable bindings of
   * archived envelopes (request keys / site seqs) so hot storage can reclaim the
   * raw envelope frames without losing idempotency and sequence authority.
   *
   * The segment id is the digest of the canonical manifest core, so identical
   * content always lands in the same file — a retry never creates a second
   * segment. The HMAC is over that same core.
   */
  buildSegment({ objectKey, boundaryReceiptSeq, chainAnchor, frames, envelopeIndex, taskId }) {
    this.ensureDirs();
    const entries = frames.map((f) => ({
      type: f.type,
      payload_hash: sha256Hex(f.payload),
      payload_length: f.payload.length,
    }));
    const core = {
      format: 'converge-archive/v1',
      object_key: objectKey,
      boundary_receipt_seq: boundaryReceiptSeq,
      created_at: new Date().toISOString(),
      chain_anchor: chainAnchor,
      entries,
      content_digest: digestOf(entries),
      envelope_index: envelopeIndex,
    };
    const segmentId = digestOf(core);
    const manifest = { ...core, segment_id: segmentId, sig: sign(this.config.receiptSecret, core) };

    const dest = this.segmentPath(segmentId);
    if (fs.existsSync(dest)) {
      // Content-addressed WORM: an identical segment from a retried task.
      const existing = this.readSegment(segmentId);
      return { segmentId, manifest: existing.manifest, reused: true };
    }
    const tmp = dest + '.tmp';
    try {
      const fd = fs.openSync(tmp, 'w');
      try {
        for (const f of frames) {
          const buf = encodeFrame(ARCHIVE_MAGIC, f.type, f.payload);
          fs.writeSync(fd, buf, 0, buf.length);
        }
        const trailer = encodeFrame(ARCHIVE_MAGIC, T_ARCHIVE_MANIFEST, Buffer.from(JSON.stringify(manifest)));
        fs.writeSync(fd, trailer, 0, trailer.length);
        fs.fsyncSync(fd);
      } finally {
        fs.closeSync(fd);
      }
      fs.chmodSync(tmp, 0o444);
      fs.renameSync(tmp, dest);
    } catch (e) {
      try { fs.unlinkSync(tmp); } catch { /* best effort */ }
      if (isFsPermissionError(e)) {
        throw new ArchiveError('ARCHIVE_PERMISSION', 'insufficient filesystem permission to write archive', { task_id: taskId });
      }
      throw e;
    }
    const verified = this.readSegment(segmentId); // never trust an unverified write
    return { segmentId, manifest: verified.manifest, reused: false };
  }

  /**
   * Read a segment and verify every layer:
   *   file framing/CRC -> manifest signature -> entries present + ordered ->
   *   per-frame payload hash -> content digest -> segment id.
   */
  readSegment(segmentId) {
    const filePath = this.segmentPath(segmentId);
    let buf;
    try {
      buf = fs.readFileSync(filePath);
    } catch (e) {
      if (e.code === 'ENOENT') throw new ArchiveError('ARCHIVE_MISSING', 'archive segment is missing', { segment_id: segmentId });
      if (isFsPermissionError(e)) throw new ArchiveError('ARCHIVE_PERMISSION', 'archive segment not readable', { segment_id: segmentId });
      throw e;
    }
    let parsed;
    try {
      parsed = parseFrames(buf, ARCHIVE_MAGIC, { allowTornTail: false });
    } catch (e) {
      throw new ArchiveError('ARCHIVE_TRUNCATED', `archive segment framing is broken: ${e.message}`, { segment_id: segmentId });
    }
    if (!parsed.frames.length) {
      throw new ArchiveError('ARCHIVE_TRUNCATED', 'archive segment has no frames', { segment_id: segmentId });
    }
    const trailer = parsed.frames[parsed.frames.length - 1];
    if (trailer.type !== T_ARCHIVE_MANIFEST) {
      throw new ArchiveError('ARCHIVE_TRUNCATED', 'archive segment is missing its manifest trailer', { segment_id: segmentId });
    }
    const dataFrames = parsed.frames.slice(0, -1);
    let manifest;
    try {
      manifest = JSON.parse(trailer.payload.toString('utf8'));
    } catch {
      throw new ArchiveError('ARCHIVE_TAMPERED', 'archive manifest is not valid JSON', { segment_id: segmentId });
    }
    if (manifest.segment_id !== segmentId) {
      throw new ArchiveError('ARCHIVE_TAMPERED', 'segment id does not match its manifest', { segment_id: segmentId });
    }
    const coreFields = [
      'format', 'object_key', 'boundary_receipt_seq', 'created_at', 'chain_anchor',
      'entries', 'content_digest', 'envelope_index',
    ];
    const core = {};
    for (const k of coreFields) core[k] = manifest[k];
    if (manifest.sig !== sign(this.config.receiptSecret, core)) {
      throw new ArchiveError('ARCHIVE_TAMPERED', 'archive manifest signature is invalid', { segment_id: segmentId });
    }
    if (!Array.isArray(manifest.entries) || manifest.entries.length !== dataFrames.length) {
      throw new ArchiveError('ARCHIVE_INDEX_MISMATCH', 'archive entry count does not match frames', { segment_id: segmentId });
    }
    for (let i = 0; i < dataFrames.length; i++) {
      const f = dataFrames[i];
      const ent = manifest.entries[i];
      if (!ent) throw new ArchiveError('ARCHIVE_INDEX_MISMATCH', 'missing manifest entry', { segment_id: segmentId, index: i });
      if (ent.type !== f.type) {
        throw new ArchiveError('ARCHIVE_INDEX_MISMATCH', 'frame type differs from index', { segment_id: segmentId, index: i });
      }
      if (ent.payload_hash !== sha256Hex(f.payload) || ent.payload_length !== f.payload.length) {
        throw new ArchiveError('ARCHIVE_TAMPERED', 'frame content differs from indexed digest', { segment_id: segmentId, index: i });
      }
    }
    if (digestOf(manifest.entries.map((e) => ({ type: e.type, payload_hash: e.payload_hash, payload_length: e.payload_length }))) !== manifest.content_digest) {
      throw new ArchiveError('ARCHIVE_TAMPERED', 'segment content digest mismatch', { segment_id: segmentId });
    }
    if (digestOf(core) !== segmentId) {
      throw new ArchiveError('ARCHIVE_TAMPERED', 'segment content id mismatch', { segment_id: segmentId });
    }
    return { manifest, frames: dataFrames.map((f) => ({ type: f.type, payload: f.payload })) };
  }

  remember(manifest) { this.segments.set(manifest.segment_id, manifest); }
  has(segmentId) { return this.segments.has(segmentId); }
  get(segmentId) { return this.segments.get(segmentId) || null; }
  listManifests() {
    return [...this.segments.values()].sort((a, b) => ((a.created_at || '') < (b.created_at || '') ? -1 : 1));
  }

  // ------------------------------------------------------------- proof pack --

  /** Verify a segment on demand; reports tamper as a structured result. */
  verifySegment(segmentId) {
    try {
      this.readSegment(segmentId);
      return { ok: true, segment_id: segmentId };
    } catch (e) {
      if (e instanceof ArchiveError) return { ok: false, error: e.code, segment_id: segmentId };
      throw e;
    }
  }

  /**
   * Desensitised proof view for inspectors: no site ids, request keys or site
   * sequences, only digests, counts, anchors and verification results.
   */
  proofView(manifest) {
    return {
      segment_id: manifest.segment_id,
      object_key: manifest.object_key,
      boundary_receipt_seq: manifest.boundary_receipt_seq,
      created_at: manifest.created_at,
      frame_count: manifest.entries.length,
      content_digest: manifest.content_digest,
      chain_anchor: manifest.chain_anchor,
      envelope_count: Array.isArray(manifest.envelope_index) ? manifest.envelope_index.length : 0,
      verified: this.verifySegment(manifest.segment_id).ok,
      sig: manifest.sig,
    };
  }

  /** Read selected frames from a verified segment by payload hash. */
  readFramesByPayloadHash(segmentId, wantedHashes) {
    const { frames } = this.readSegment(segmentId);
    const byHash = new Map();
    for (const f of frames) byHash.set(sha256Hex(f.payload), f);
    const out = [];
    for (const h of wantedHashes) {
      if (!byHash.has(h)) {
        throw new ArchiveError('ARCHIVE_INDEX_MISMATCH', 'indexed frame missing from segment', { segment_id: segmentId, payload_hash: h });
      }
      out.push(byHash.get(h));
    }
    return out;
  }
}
