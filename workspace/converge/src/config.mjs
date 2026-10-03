import path from 'node:path';

export const ROLES = ['operator', 'reviewer', 'inspector', 'admin'];

/**
 * Static role tokens. Override via environment for deployment; defaults exist so
 * the service and verification script work out of the box.
 */
export function loadConfig(env = process.env) {
  const dataDir = env.CONVERGE_DATA_DIR || path.join(process.cwd(), 'data');
  return {
    host: env.CONVERGE_HOST || '127.0.0.1',
    port: Number(env.CONVERGE_PORT || 8080),
    dataDir,
    walPath: path.join(dataDir, 'converge.wal'),
    outboxPath: path.join(dataDir, 'outbox.wal'),
    receiptSecret: env.CONVERGE_RECEIPT_SECRET || 'dev-receipt-secret-change-me',
    // Quiet period before an object settles: coalesces concurrent reports into
    // one decision. Determinism of the result does not depend on this value.
    settleDelayMs: Number(env.CONVERGE_SETTLE_MS || 25),
    // Retention policy: an object must hold at least this many decisions in HOT
    // storage after a checkpoint. A checkpoint targeting an older boundary is
    // rejected (RETENTION_POLICY_REJECTS). 0 allows archiving the whole history.
    retainDecisions: Math.max(0, Number(env.CONVERGE_RETAIN_DECISIONS || 0)),
    // Injected crash points for recovery tests (no-op in normal operation):
    //   'after_segment'  process-exit-equivalent failure once the segment exists
    //   'after_stub'     failure once the checkpoint stub is durable, pre-trim
    crashPoint: env.CONVERGE_CRASH_POINT || null,

    // ------------------------------------------------ reliable subscriptions --
    // The background pump ticks at this interval and reclaims leases of batches
    // whose worker process died. Set interval to 0 (tests) to disable the timer;
    // deliveries are then driven explicitly via POST /api/subscriptions/pump.
    pumpIntervalMs: Math.max(0, Number(env.CONVERGE_PUMP_INTERVAL_MS || 500)),
    // Per-delivery HTTP timeout (timeouts are retried like non-2xx responses).
    deliveryTimeoutMs: Math.max(50, Number(env.CONVERGE_DELIVERY_TIMEOUT_MS || 4000)),
    // Capped exponential backoff: base * 2^(attempts-1), capped at max.
    deliveryBackoffBaseMs: Math.max(1, Number(env.CONVERGE_BACKOFF_BASE_MS || 200)),
    deliveryBackoffMaxMs: Math.max(1, Number(env.CONVERGE_BACKOFF_MAX_MS || 30000)),
    deliveryMaxAttempts: Math.max(1, Number(env.CONVERGE_DELIVERY_MAX_ATTEMPTS || 5)),
    deliveryBatchSize: Math.max(1, Number(env.CONVERGE_DELIVERY_BATCH_SIZE || 4)),
    // An in-flight batch older than this is reclaimable (worker died / ack lost).
    deliveryLeaseMs: Math.max(50, Number(env.CONVERGE_DELIVERY_LEASE_MS || 60000)),

    tokens: {
      [env.OPERATOR_TOKEN || 'operator-token']: 'operator',
      [env.REVIEWER1_TOKEN || 'reviewer1-token']: 'reviewer',
      [env.REVIEWER2_TOKEN || 'reviewer2-token']: 'reviewer',
      [env.INSPECTOR_TOKEN || 'inspector-token']: 'inspector',
      [env.ADMIN_TOKEN || 'admin-token']: 'admin',
    },
    reviewerIdentities: {
      [env.REVIEWER1_TOKEN || 'reviewer1-token']: env.REVIEWER1_NAME || 'reviewer-01',
      [env.REVIEWER2_TOKEN || 'reviewer2-token']: env.REVIEWER2_NAME || 'reviewer-02',
    },
  };
}
