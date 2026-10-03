import fs from 'node:fs';
import path from 'node:path';
import { crc32 } from './crypto-util.mjs';

/**
 * Append-only write-ahead log.
 *
 * Frame layout on disk:
 *   [magic 2B][type 1B][length 4B BE][payload length B][crc32 4B BE over type+length+payload]
 *
 * Recovery semantics:
 *  - a short final frame (process died mid write) is truncated away
 *  - a complete final frame with a bad CRC is truncated away (torn write)
 *  - a bad frame anywhere before a valid frame is fatal: refusing to continue
 *    avoids silently skipping a committed event.
 */

export const WAL_MAGIC = Buffer.from([0x43, 0x56]); // 'CV'
export const ARCHIVE_MAGIC = Buffer.from([0x43, 0x41]); // 'CA'
const HEADER_LEN = 2 + 1 + 4;
const CRC_LEN = 4;
export const MAX_PAYLOAD = 8 * 1024 * 1024;

export class WalError extends Error {}

/** Encode one frame (magic-prefixed, CRC-terminated) for either the WAL or an archive segment. */
export function encodeFrame(magic, type, payloadBuf) {
  const body = Buffer.from(payloadBuf);
  if (body.length > MAX_PAYLOAD) throw new WalError('frame too large');
  const typeBuf = Buffer.from([type & 0xff]);
  const lenBuf = Buffer.alloc(4);
  lenBuf.writeUInt32BE(body.length, 0);
  const crcInput = Buffer.concat([typeBuf, lenBuf, body]);
  const crcBuf = Buffer.alloc(4);
  crcBuf.writeUInt32BE(crc32(crcInput), 0);
  return Buffer.concat([magic, crcInput, crcBuf]);
}

/**
 * Parse every frame in a buffer.
 * Returns { frames: [{type, payload(Buffer), start, end, dataStart}], truncatedBytes }.
 * With { fatalOnInnerBadFrame:false } a bad final frame is reported as a torn
 * tail; an inner bad frame is always fatal.
 */
export function parseFrames(buf, expectedMagic = WAL_MAGIC, { allowTornTail = true } = {}) {
  const frames = [];
  let off = 0;
  let truncatedBytes = 0;
  while (off < buf.length) {
    const frameStart = off;
    if (buf.length - off < HEADER_LEN) {
      if (allowTornTail) { truncatedBytes = buf.length - frameStart; break; }
      throw new WalError(`short frame at offset ${off}`);
    }
    if (buf[off] !== expectedMagic[0] || buf[off + 1] !== expectedMagic[1]) {
      throw new WalError(`bad magic at offset ${off}${frames.length ? ' (corruption before committed frames)' : ''}`);
    }
    const type = buf[off + 2];
    const length = buf.readUInt32BE(off + 3, 4);
    if (length > MAX_PAYLOAD) throw new WalError(`frame length implausible at offset ${off}`);
    const end = off + HEADER_LEN + length + CRC_LEN;
    if (end > buf.length) {
      if (allowTornTail) { truncatedBytes = buf.length - frameStart; break; }
      throw new WalError(`truncated frame at offset ${off}`);
    }
    const wantCrc = buf.readUInt32BE(end - CRC_LEN, 4);
    const gotCrc = crc32(buf.subarray(off + 2, end - CRC_LEN));
    if (wantCrc !== gotCrc) {
      if (allowTornTail && end === buf.length) { truncatedBytes = end - frameStart; break; }
      throw new WalError(`crc mismatch at offset ${off}; later frames exist, refusing to skip`);
    }
    frames.push({ type, payload: buf.subarray(off + HEADER_LEN, end - CRC_LEN), start: frameStart, end, dataStart: off + HEADER_LEN });
    off = end;
  }
  return { frames, truncatedBytes };
}

function fsyncDir(dir) {
  try {
    const fd = fs.openSync(dir, 'r');
    try { fs.fsyncSync(fd); } finally { fs.closeSync(fd); }
  } catch {
    /* directory fsync is best effort (e.g. unsupported FS) */
  }
}

export class Wal {
  constructor(filePath, magic = WAL_MAGIC) {
    this.filePath = filePath;
    // Every file carries exactly one magic; the outbox log uses its own ('OE')
    // so a frame can never be mistaken across the two durable logs.
    this.magic = magic;
    this.fd = null;
    this.fsyncOnAppend = true;
  }

  open() {
    fs.mkdirSync(path.dirname(this.filePath), { recursive: true });
    // A leftover replacement temp means a crash before the atomic rename:
    // the canonical WAL was never replaced, so discard the temp.
    const tmp = this.filePath + '.replace';
    try { fs.unlinkSync(tmp); } catch (e) { if (e.code !== 'ENOENT') throw e; }
    this.fd = fs.openSync(this.filePath, 'a+');
  }

  append(type, payload) {
    const frame = encodeFrame(this.magic, type, Buffer.from(payload));
    if (!this.fd) throw new WalError('wal not open');
    fs.writeSync(this.fd, frame, 0, frame.length);
    fs.fsyncSync(this.fd);
  }

  /**
   * Atomically append several frames in one write + fsync. Used by the
   * transactional outbox: the business frame(s) and the outbox event frame(s)
   * must land as one durable result, so a crash can never leave a visible
   * business decision with no corresponding notification.
   */
  appendBatch(entries) {
    if (!this.fd) throw new WalError('wal not open');
    const buf = Buffer.concat(entries.map(([type, payload]) =>
      encodeFrame(this.magic, type, Buffer.from(payload))));
    fs.writeSync(this.fd, buf, 0, buf.length);
    fs.fsyncSync(this.fd);
  }

  close() {
    if (this.fd) {
      try { fs.fsyncSync(this.fd); } catch { /* ignore on close */ }
      fs.closeSync(this.fd);
      this.fd = null;
    }
  }

  /** Read all frames. Returns { frames:[{type,payload,start,end}], truncatedBytes }. */
  readAllFrames() {
    let buf;
    try {
      buf = fs.readFileSync(this.filePath);
    } catch (e) {
      if (e.code === 'ENOENT') return { frames: [], truncatedBytes: 0 };
      throw e;
    }
    return parseFrames(buf, this.magic, { allowTornTail: true });
  }

  /** Backwards-compatible wrapper used by the store. */
  readAll() {
    const { frames, truncatedBytes } = this.readAllFrames();
    return {
      events: frames.map((f) => ({ type: f.type, payload: f.payload })),
      truncatedBytes,
    };
  }

  /**
   * Atomically replace the whole WAL with `frames` (hot-storage trim).
   * Writes a temp file, fsyncs file and directory, then rename(2)s over the
   * canonical path. A crash at any point leaves exactly one of:
   *   - old canonical WAL + temp (resume discards temp, redo the trim)
   *   - new canonical WAL (trim already durable)
   */
  replaceFrames(frames) {
    const dir = path.dirname(this.filePath);
    const tmp = this.filePath + '.replace';
    const fd = fs.openSync(tmp, 'w');
    try {
      for (const f of frames) {
        const out = encodeFrame(this.magic, f.type, Buffer.from(f.payload));
        fs.writeSync(fd, out, 0, out.length);
      }
      fs.fsyncSync(fd);
    } finally {
      fs.closeSync(fd);
    }
    fsyncDir(dir);
    fs.renameSync(tmp, this.filePath);
    fsyncDir(dir);
    if (this.fd) {
      try { fs.closeSync(this.fd); } catch { /* reopened below */ }
    }
    this.fd = fs.openSync(this.filePath, 'a+');
  }

  /** Drop the tail (used to remove a torn frame before appending). */
  truncate(newLength) {
    fs.truncateSync(this.filePath, newLength);
    if (this.fd) {
      fs.closeSync(this.fd);
      this.fd = fs.openSync(this.filePath, 'a+');
    }
  }
}
