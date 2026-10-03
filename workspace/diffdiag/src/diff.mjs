/**
 * Pluggable comparison algorithms. A task pins the algorithm version that was
 * active when it was created; switching the active version only affects tasks
 * created afterwards, and every version stays available for proof
 * verification of older tasks.
 *
 * Each algorithm receives the frozen inputs of both sides
 *   { receipts: [...], skeleton: {...} }
 * and returns a deterministic result:
 *   { divergence, affected, evidence }
 * where `evidence` is the ordered plan the EXPLAIN stage turns into chained,
 * sanitised explanation nodes. Nothing here ever carries site identifiers,
 * request keys or image content — only digests, sequence numbers, object keys
 * and synthetic region coordinates.
 */

function byObject(receipts) {
  const m = new Map();
  for (const r of receipts) {
    if (!m.has(r.object_key)) m.set(r.object_key, []);
    m.get(r.object_key).push(r);
  }
  for (const arr of m.values()) arr.sort((a, b) => a.seq - b.seq);
  return m;
}

function depthMap(skeleton) {
  const m = new Map();
  skeleton.topo.forEach((d, i) => m.set(d, i));
  return m;
}

/** chain-walk-v1: align per-object receipt chains by sequence number. */
function runChainWalk(left, right) {
  const L = byObject(left.receipts);
  const R = byObject(right.receipts);
  const objects = [...new Set([...L.keys(), ...R.keys()])].sort();

  let divergence = null;
  for (const obj of objects) {
    const lc = L.get(obj) || [];
    const rc = R.get(obj) || [];
    const n = Math.max(lc.length, rc.length);
    for (let i = 0; i < n; i++) {
      const lh = lc[i] ? lc[i].hash : null;
      const rh = rc[i] ? rc[i].hash : null;
      if (lh !== rh) {
        divergence = {
          kind: 'chain_divergence',
          object_key: obj,
          seq: i + 1,
          left: lh ? { receipt: lh, node: lc[i].decision_node } : null,
          right: rh ? { receipt: rh, node: rc[i].decision_node } : null,
        };
        break;
      }
    }
    if (divergence) break;
  }

  const affected = [];
  if (divergence) {
    for (const [side, chains] of [['left', L], ['right', R]]) {
      for (const r of chains.get(divergence.object_key) || []) {
        if (r.seq >= divergence.seq) {
          affected.push({ node_ref: r.decision_node, side, object_key: r.object_key, seq: r.seq });
        }
      }
    }
    affected.sort((a, b) => a.seq - b.seq || (a.side < b.side ? -1 : a.side > b.side ? 1 : 0));
  }

  const evidence = [];
  if (divergence) {
    evidence.push({
      kind: 'divergence',
      object_key: divergence.object_key,
      seq: divergence.seq,
      left_node: divergence.left ? divergence.left.node : null,
      right_node: divergence.right ? divergence.right.node : null,
    });
    for (const a of affected) {
      evidence.push({ kind: 'downstream', node_ref: a.node_ref, side: a.side, object_key: a.object_key, seq: a.seq });
    }
    evidence.push({ kind: 'context', object_key: divergence.object_key, common_prefix_upto: divergence.seq - 1 });
  } else {
    evidence.push({ kind: 'verdict', result: 'identical', objects: objects.length });
  }
  return { divergence, affected, evidence };
}

/** skeleton-align-v2: align causal skeletons by node digest in topological order. */
function runSkeletonAlign(left, right) {
  const L = left.skeleton;
  const R = right.skeleton;
  const lDepth = depthMap(L);
  const rDepth = depthMap(R);
  const rSet = new Set(R.topo);
  const lSet = new Set(L.topo);

  const candidates = [];
  for (const d of L.topo) if (!rSet.has(d)) candidates.push({ node_ref: d, side: 'left', depth: lDepth.get(d) });
  for (const d of R.topo) if (!lSet.has(d)) candidates.push({ node_ref: d, side: 'right', depth: rDepth.get(d) });
  candidates.sort((a, b) =>
    a.depth - b.depth || (a.node_ref < b.node_ref ? -1 : a.node_ref > b.node_ref ? 1 : a.side < b.side ? -1 : 1));

  const divergence = candidates.length
    ? { kind: 'node_divergence', node_ref: candidates[0].node_ref, side: candidates[0].side, depth: candidates[0].depth }
    : null;

  const affected = [];
  if (divergence) {
    const skel = divergence.side === 'left' ? L : R;
    const depth = divergence.side === 'left' ? lDepth : rDepth;
    const children = new Map();
    for (const [p, c] of skel.edges) {
      if (!children.has(p)) children.set(p, []);
      children.get(p).push(c);
    }
    const seen = new Set([divergence.node_ref]);
    const queue = [divergence.node_ref];
    while (queue.length) {
      const n = queue.shift();
      for (const c of (children.get(n) || []).slice().sort()) {
        if (!seen.has(c)) {
          seen.add(c);
          queue.push(c);
        }
      }
    }
    seen.delete(divergence.node_ref);
    for (const d of seen) affected.push({ node_ref: d, side: divergence.side, depth: depth.get(d) });
    affected.sort((a, b) => a.depth - b.depth || (a.node_ref < b.node_ref ? -1 : 1));
  }

  const evidence = [];
  if (divergence) {
    evidence.push({ kind: 'divergence', node_ref: divergence.node_ref, side: divergence.side, depth: divergence.depth });
    for (const a of affected) {
      evidence.push({ kind: 'downstream', node_ref: a.node_ref, side: a.side, depth: a.depth });
    }
    const skel = divergence.side === 'left' ? L : R;
    const otherSet = divergence.side === 'left' ? rSet : lSet;
    const commonAncestors = skel.edges
      .filter(([, c]) => c === divergence.node_ref)
      .map(([p]) => p)
      .filter((p) => otherSet.has(p))
      .sort();
    evidence.push({ kind: 'context', common_ancestors: commonAncestors });
  } else {
    evidence.push({ kind: 'verdict', result: 'identical', objects: L.objects.length });
  }
  return { divergence, affected, evidence };
}

export const ALGORITHMS = {
  'chain-walk-v1': {
    description: 'Align per-object receipt chains by sequence; the first mismatched sequence is the divergence.',
    run: runChainWalk,
  },
  'skeleton-align-v2': {
    description: 'Align causal skeletons by node digest in topological order; the first one-sided node is the divergence.',
    run: runSkeletonAlign,
  },
};
