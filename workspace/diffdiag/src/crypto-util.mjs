import crypto from 'node:crypto';

/**
 * Canonical JSON: object keys sorted recursively, null kept, no whitespace.
 * Byte-identical to the encoding converge uses for receipt hashes, so a
 * receipt signed by converge verifies here without any transcoding.
 */
export function canonical(value) {
  if (value === null || typeof value !== 'object') {
    return JSON.stringify(value);
  }
  if (Array.isArray(value)) {
    return '[' + value.map(canonical).join(',') + ']';
  }
  const keys = Object.keys(value).sort();
  return '{' + keys.map((k) => JSON.stringify(k) + ':' + canonical(value[k])).join(',') + '}';
}

export function sha256Hex(canonicalOrBytes) {
  const h = crypto.createHash('sha256');
  h.update(canonicalOrBytes);
  return h.digest('hex');
}

/** Digest of a canonicalizable value: sha256 of its canonical encoding. */
export function digestOf(value) {
  return sha256Hex(canonical(value));
}

/** HMAC-SHA256 over canonical payload with the shared receipt secret. */
export function sign(secret, value) {
  return crypto.createHmac('sha256', secret).update(canonical(value)).digest('hex');
}

export function constantTimeEqual(a, b) {
  if (typeof a !== 'string' || typeof b !== 'string') return false;
  const ab = Buffer.from(a);
  const bb = Buffer.from(b);
  if (ab.length !== bb.length) return false;
  return crypto.timingSafeEqual(ab, bb);
}
