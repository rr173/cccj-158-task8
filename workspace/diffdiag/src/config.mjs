import path from 'node:path';

export const ROLES = ['operator', 'analyst', 'observer'];

/**
 * Static role tokens. Override via environment for deployment; defaults exist so
 * the service and the verification script work out of the box.
 */
export function loadConfig(env = process.env) {
  const dataDir = env.DIFFDIAG_DATA_DIR || path.join(process.cwd(), 'data');
  return {
    host: env.DIFFDIAG_HOST || '127.0.0.1',
    port: Number(env.DIFFDIAG_PORT || 8090),
    dataDir,
    statePath: path.join(dataDir, 'diffdiag-state.json'),
    // Must match the secret converge signs its decision receipts with.
    receiptSecret:
      env.DIFFDIAG_RECEIPT_SECRET || env.CONVERGE_RECEIPT_SECRET || 'dev-receipt-secret-change-me',
    // 0 => the worker only runs when driven via POST /api/worker/run (ops/tests).
    workerIntervalMs: Math.max(0, Number(env.DIFFDIAG_WORKER_INTERVAL_MS || 0)),
    // Test hook only: 'after_claim' | 'after_stage:<STAGE>' | 'during_explain:<n>'.
    crashPoint: env.DIFFDIAG_CRASH_POINT || null,
    tokens: {
      [env.OPERATOR_TOKEN || 'operator-token']: 'operator',
      [env.ANALYST_TOKEN || 'analyst-token']: 'analyst',
      [env.OBSERVER_TOKEN || 'observer-token']: 'observer',
    },
  };
}
