import { digestOf, sign } from './crypto-util.mjs';
import { ApiError } from './errors.mjs';

const HEX64 = /^[0-9a-f]{64}$/;
const MAX_RECEIPTS_PER_PACKAGE = 10000;

// A sanitised converge decision receipt carries exactly these fields and no
// others. Anything outside this whitelist (site, site_seq, request_key, pixel
// payloads, ...) is refused so no unsanitised content can ever enter the
// diagnostic pipeline — and therefore never an error page either.
const ALLOWED_FIELDS = new Set([
  'seq', 'object_key', 'decision_kind', 'decision_node', 'parents', 'contributions',
  'prev_receipt', 'emitted_at', 'hash', 'sig', 'case_id', 'reviewers',
]);
const REQUIRED_FIELDS = [
  'seq', 'object_key', 'decision_kind', 'decision_node', 'parents', 'contributions',
  'prev_receipt', 'emitted_at', 'hash', 'sig',
];

/** The fields converge hashes and signs (everything except hash/sig). */
export function receiptFieldsOf(r) {
  return {
    seq: r.seq,
    object_key: r.object_key,
    decision_kind: r.decision_kind,
    decision_node: r.decision_node,
    parents: r.parents,
    contributions: r.contributions,
    ...(r.case_id !== undefined ? { case_id: r.case_id, reviewers: r.reviewers } : {}),
    prev_receipt: r.prev_receipt,
    emitted_at: r.emitted_at,
  };
}

function bad(status, code, message, details) {
  throw new ApiError(status, code, message, details);
}

function checkHex(value, index, field) {
  if (typeof value !== 'string' || !HEX64.test(value)) {
    bad(422, 'DIGEST_FORMAT_INVALID', 'digest field must be 64 lowercase hex characters', { index, field });
  }
}

/**
 * Shape and digest-format validation for one receipt. Error details carry only
 * field names / indices / sequence numbers — never values from the package.
 */
function validateReceiptShape(r, index) {
  if (!r || typeof r !== 'object' || Array.isArray(r)) {
    bad(422, 'RECEIPT_MALFORMED', 'receipt must be an object', { index });
  }
  for (const k of Object.keys(r)) {
    if (!ALLOWED_FIELDS.has(k)) {
      bad(422, 'RECEIPT_FIELD_FORBIDDEN', 'field is not part of the sanitised receipt schema', { index, field: k });
    }
  }
  for (const f of REQUIRED_FIELDS) {
    if (!(f in r)) bad(422, 'RECEIPT_MALFORMED', 'required receipt field missing', { index, field: f });
  }
  if (!Number.isInteger(r.seq) || r.seq < 1) {
    bad(422, 'RECEIPT_MALFORMED', 'seq must be a positive integer', { index });
  }
  if (typeof r.object_key !== 'string' || r.object_key.length < 1 || r.object_key.length > 128) {
    bad(422, 'RECEIPT_MALFORMED', 'object_key must be a string of 1..128 characters', { index });
  }
  if (typeof r.decision_kind !== 'string' || !r.decision_kind) {
    bad(422, 'RECEIPT_MALFORMED', 'decision_kind must be a non-empty string', { index });
  }
  checkHex(r.decision_node, index, 'decision_node');
  checkHex(r.hash, index, 'hash');
  checkHex(r.sig, index, 'sig');
  if (r.prev_receipt !== null) checkHex(r.prev_receipt, index, 'prev_receipt');
  if (!Array.isArray(r.parents)) bad(422, 'RECEIPT_MALFORMED', 'parents must be an array', { index });
  const seenParents = new Set();
  for (const p of r.parents) {
    checkHex(p, index, 'parents');
    if (seenParents.has(p)) bad(422, 'RECEIPT_MALFORMED', 'duplicate parent entry', { index });
    seenParents.add(p);
  }
  if (!Array.isArray(r.contributions)) bad(422, 'RECEIPT_MALFORMED', 'contributions must be an array', { index });
  for (const c of r.contributions) {
    if (!c || typeof c !== 'object' || Array.isArray(c)) {
      bad(422, 'RECEIPT_MALFORMED', 'contribution must be an object', { index });
    }
    for (const k of Object.keys(c)) {
      if (k !== 'region' && k !== 'change_digest') {
        bad(422, 'RECEIPT_FIELD_FORBIDDEN', 'contribution field is not part of the sanitised schema', { index, field: k });
      }
    }
    if (typeof c.region !== 'string' || !c.region || c.region.length > 256) {
      bad(422, 'RECEIPT_MALFORMED', 'contribution region must be a string', { index });
    }
    checkHex(c.change_digest, index, 'contributions.change_digest');
  }
  if (typeof r.emitted_at !== 'string' || !r.emitted_at) {
    bad(422, 'RECEIPT_MALFORMED', 'emitted_at must be a non-empty string', { index });
  }
  if ('case_id' in r) {
    if (typeof r.case_id !== 'string' || !Array.isArray(r.reviewers) || r.reviewers.some((x) => typeof x !== 'string')) {
      bad(422, 'RECEIPT_MALFORMED', 'case_id must come with a reviewers string array', { index });
    }
  } else if ('reviewers' in r) {
    bad(422, 'RECEIPT_MALFORMED', 'reviewers without case_id', { index });
  }
}

/**
 * Whole-package validation. Pure: touches nothing, returns the normalised
 * receipt list (ascending by seq) on success. Any violation rejects the
 * ENTIRE package — the caller commits nothing in that case.
 *
 *   known = {
 *     receiptsByHash: Map<hash, receipt>,        // global content store
 *     chains:         Map<object_key, [hash]>,   // this group's chains
 *     hashByObjSeq:   Map<'obj#seq', hash>,      // this group
 *     nodeIndex:      Map<object_key, Map<node, hash>>,  // this group
 *   }
 */
export function validatePackage(body, known, secret) {
  if (!body || typeof body !== 'object' || Array.isArray(body)) {
    bad(400, 'PACKAGE_MALFORMED', 'package must be a JSON object');
  }
  for (const k of Object.keys(body)) {
    if (k !== 'group' && k !== 'object_key' && k !== 'receipts') {
      bad(400, 'PACKAGE_MALFORMED', 'unknown package field', { field: k });
    }
  }
  const obj = body.object_key;
  if (typeof obj !== 'string' || obj.length < 1 || obj.length > 128) {
    bad(400, 'PACKAGE_MALFORMED', 'object_key must be a string of 1..128 characters');
  }
  if (!Array.isArray(body.receipts) || body.receipts.length === 0) {
    bad(400, 'PACKAGE_MALFORMED', 'receipts must be a non-empty array');
  }
  if (body.receipts.length > MAX_RECEIPTS_PER_PACKAGE) {
    bad(400, 'PACKAGE_TOO_LARGE', 'too many receipts in one package');
  }
  const receipts = body.receipts;

  // 1) shape + digest formats (also enforces the sanitised-field whitelist)
  receipts.forEach((r, i) => {
    validateReceiptShape(r, i);
    if (r.object_key !== obj) {
      bad(422, 'RECEIPT_OBJECT_MISMATCH', 'receipt object_key differs from the package object_key', { index: i });
    }
  });

  // 2) content hash + chain signature, per receipt
  receipts.forEach((r, i) => {
    const fields = receiptFieldsOf(r);
    if (digestOf(fields) !== r.hash) {
      bad(422, 'RECEIPT_HASH_MISMATCH', 'receipt hash does not match its contents', { index: i, seq: r.seq });
    }
    if (sign(secret, fields) !== r.sig) {
      bad(422, 'RECEIPT_SIG_INVALID', 'receipt signature is invalid', { index: i, seq: r.seq });
    }
  });

  // 3) package-internal uniqueness: no duplicate seq, no duplicate receipt,
  //    and never two different bodies for the same decision node
  const seqSeen = new Map();
  const hashSeen = new Set();
  const nodeSeen = new Map();
  receipts.forEach((r, i) => {
    if (seqSeen.has(r.seq)) {
      bad(422, 'SEQ_DUPLICATE', 'two receipts carry the same sequence number', { seq: r.seq });
    }
    seqSeen.set(r.seq, i);
    if (hashSeen.has(r.hash)) {
      bad(422, 'RECEIPT_DUPLICATE', 'the identical receipt appears twice in one package', { index: i });
    }
    hashSeen.add(r.hash);
    const prevHash = nodeSeen.get(r.decision_node);
    if (prevHash !== undefined && prevHash !== r.hash) {
      bad(422, 'NODE_BODY_CONFLICT', 'the same decision node is carried by two different receipts', { seq: r.seq });
    }
    nodeSeen.set(r.decision_node, r.hash);
  });

  // 4) conflicts against what this group already persists
  for (const r of receipts) {
    const storedHash = known.hashByObjSeq.get(obj + '#' + r.seq);
    if (storedHash && storedHash !== r.hash) {
      bad(422, 'SEQ_DUPLICATE', 'a different receipt is already persisted for this sequence number', { seq: r.seq });
    }
    const storedNodeHash = known.nodeIndex.get(obj)?.get(r.decision_node);
    if (storedNodeHash && storedNodeHash !== r.hash) {
      bad(422, 'NODE_BODY_CONFLICT', 'decision node is already persisted with a different receipt body', { seq: r.seq });
    }
  }

  // 5) chain linkage: contiguous seqs, prev_receipt forms one anchored chain
  const sorted = [...receipts].sort((a, b) => a.seq - b.seq);
  for (let i = 1; i < sorted.length; i++) {
    if (sorted[i].seq !== sorted[i - 1].seq + 1) {
      bad(422, 'SEQ_GAP', 'receipt sequence numbers must be contiguous', { seq: sorted[i].seq });
    }
    if (sorted[i].prev_receipt !== sorted[i - 1].hash) {
      bad(422, 'RECEIPT_CHAIN_BROKEN', 'prev_receipt does not reference the in-order predecessor', { seq: sorted[i].seq });
    }
  }
  const first = sorted[0];
  const storedChain = known.chains.get(obj) || [];
  if (first.prev_receipt === null) {
    if (first.seq !== 1) {
      bad(422, 'RECEIPT_CHAIN_BROKEN', 'a genesis receipt (prev_receipt null) must have seq 1', { seq: first.seq });
    }
  } else {
    const anchor = known.receiptsByHash.get(first.prev_receipt);
    if (!anchor || storedChain[first.seq - 2] !== first.prev_receipt) {
      bad(422, 'RECEIPT_CHAIN_BROKEN', 'first receipt references an unknown or out-of-order predecessor', { seq: first.seq });
    }
  }

  // 6) topology. converge receipts reference two kinds of predecessors:
  //    earlier DECISION nodes of the same object (internal edges — these have
  //    receipts in the chain) and change/envelope nodes (external references —
  //    content digests that never carry a receipt). Internal edges must stay
  //    inside this object and strictly precede their child; a cycle among
  //    decision nodes is rejected. External references are opaque digests and
  //    become external nodes of the skeleton.
  const nodeSeq = new Map(); // decision node -> {object_key, seq}
  for (const [chainObj, hashes] of known.chains) {
    for (const h of hashes) {
      const sr = known.receiptsByHash.get(h);
      if (sr) nodeSeq.set(sr.decision_node, { object_key: chainObj, seq: sr.seq });
    }
  }
  for (const r of sorted) nodeSeq.set(r.decision_node, { object_key: obj, seq: r.seq });
  for (const r of sorted) {
    for (const p of r.parents) {
      const ref = nodeSeq.get(p);
      if (!ref) continue; // external change-node reference
      if (ref.object_key !== obj) {
        bad(422, 'TOPOLOGY_PARENT_CROSS_OBJECT', 'parent decision node belongs to a different object', { seq: r.seq });
      }
      if (ref.seq >= r.seq) {
        bad(422, 'TOPOLOGY_CYCLE', 'a predecessor does not precede its child in causal order', { seq: r.seq });
      }
    }
  }
  // Explicit Kahn pass over internal edges as a defence-in-depth cycle check.
  {
    const decisionNodes = new Set(nodeSeq.keys());
    const indeg = new Map([...decisionNodes].map((n) => [n, 0]));
    const children = new Map([...decisionNodes].map((n) => [n, []]));
    const allReceipts = [
      ...[...known.chains.values()].flat().map((h) => known.receiptsByHash.get(h)).filter(Boolean),
      ...sorted,
    ];
    for (const r of allReceipts) {
      for (const p of r.parents) {
        if (decisionNodes.has(p)) {
          children.get(p).push(r.decision_node);
          indeg.set(r.decision_node, indeg.get(r.decision_node) + 1);
        }
      }
    }
    const ready = [...decisionNodes].filter((n) => indeg.get(n) === 0);
    let visited = 0;
    while (ready.length) {
      const n = ready.pop();
      visited++;
      for (const c of children.get(n)) {
        indeg.set(c, indeg.get(c) - 1);
        if (indeg.get(c) === 0) ready.push(c);
      }
    }
    if (visited !== decisionNodes.size) {
      bad(422, 'TOPOLOGY_CYCLE', 'predecessor relations contain a cycle');
    }
  }

  return sorted;
}

/**
 * Compile the receipts of one comparison side into a causal skeleton: nodes
 * keyed by content digest, edges from predecessor relations, deterministic
 * topological order. Two sides compiled this way are directly comparable.
 */
export function buildSkeleton(receipts) {
  const sorted = [...receipts].sort((a, b) =>
    a.object_key < b.object_key ? -1 : a.object_key > b.object_key ? 1 : a.seq - b.seq);
  const nodes = new Map();
  const edgeSet = new Set();
  for (const r of sorted) {
    if (!nodes.has(r.decision_node)) {
      nodes.set(r.decision_node, {
        digest: r.decision_node,
        external: false,
        object_key: r.object_key,
        seq: r.seq,
        decision_kind: r.decision_kind,
        parents: [...r.parents].sort(),
        contributions: r.contributions.map((c) => ({ region: c.region, change_digest: c.change_digest })),
      });
    }
    for (const p of r.parents) edgeSet.add(p + '>' + r.decision_node);
  }
  // Predecessors without a receipt are external change-node references:
  // opaque digest nodes that close the causal skeleton.
  for (const r of sorted) {
    for (const p of r.parents) {
      if (!nodes.has(p)) nodes.set(p, { digest: p, external: true, parents: [] });
    }
  }
  const edges = [...edgeSet].sort().map((s) => s.split('>'));
  const children = new Map([...nodes.keys()].map((n) => [n, []]));
  const indeg = new Map([...nodes.keys()].map((n) => [n, 0]));
  for (const [p, c] of edges) {
    if (children.has(p)) children.get(p).push(c);
    if (indeg.has(c)) indeg.set(c, indeg.get(c) + 1);
  }
  // Deterministic Kahn: the ready set is always processed in digest order.
  const ready = [...nodes.keys()].filter((n) => indeg.get(n) === 0).sort();
  const topo = [];
  while (ready.length) {
    const n = ready.shift();
    topo.push(n);
    for (const c of [...children.get(n)].sort()) {
      indeg.set(c, indeg.get(c) - 1);
      if (indeg.get(c) === 0) {
        ready.push(c);
        ready.sort();
      }
    }
  }
  const nodeList = [...nodes.values()].sort((a, b) => (a.digest < b.digest ? -1 : 1));
  const roots = nodeList.filter((n) => n.parents.length === 0).map((n) => n.digest);
  const objects = [...new Set(sorted.map((r) => r.object_key))].sort();
  const skeleton = { objects, nodes: nodeList, edges, roots, topo };
  skeleton.digest = digestOf({ nodes: nodeList, edges, roots, topo });
  return skeleton;
}
