import http from 'node:http';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { loadConfig } from './config.mjs';
import { DiffdiagStore } from './store.mjs';
import { ApiError } from './errors.mjs';
import { constantTimeEqual } from './crypto-util.mjs';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const PUBLIC_DIR = path.join(__dirname, '..', 'public');
const MAX_BODY = 8 * 1024 * 1024;

const config = loadConfig();
const store = DiffdiagStore.load(config);

const ALL = ['operator', 'analyst', 'observer'];

/**
 * Role matrix:
 *   operator — ingest packages, manage algorithm versions, cancel tasks, drive
 *              the worker; reads operational views (never the diagnostic tree)
 *   analyst  — select two groups, create comparisons, read the divergence tree
 *              and the sanitised evidence
 *   observer — read proof summaries and verify proofs; nothing else
 */
const ROUTES = [
  { m: 'GET', re: /^\/api\/whoami$/, roles: ALL, h: (role) => ({ role }) },
  { m: 'POST', re: /^\/api\/packages$/, roles: ['operator'], h: (_r, _p, body) => store.ingestPackage(body) },
  { m: 'GET', re: /^\/api\/objects$/, roles: ['operator', 'analyst'], h: () => store.listObjects() },
  { m: 'GET', re: /^\/api\/algo-versions$/, roles: ['operator', 'analyst'], h: () => store.listAlgoVersions() },
  { m: 'POST', re: /^\/api\/algo-versions$/, roles: ['operator'], h: (_r, _p, body) => store.registerAlgoVersion(body) },
  { m: 'POST', re: /^\/api\/algo-versions\/activate$/, roles: ['operator'], h: (_r, _p, body) => store.activateAlgoVersion(body) },
  { m: 'POST', re: /^\/api\/comparisons$/, roles: ['analyst'], h: (_r, _p, body) => store.createComparison(body) },
  { m: 'GET', re: /^\/api\/comparisons$/, roles: ['operator', 'analyst'], h: () => store.listComparisons() },
  { m: 'GET', re: /^\/api\/comparisons\/([^/]+)\/diagnostic$/, roles: ['analyst'], h: (_r, p) => store.getDiagnostic(dec(p[0])) },
  { m: 'GET', re: /^\/api\/comparisons\/([^/]+)$/, roles: ['operator', 'analyst'], h: (_r, p) => store.getComparison(dec(p[0])) },
  { m: 'POST', re: /^\/api\/comparisons\/([^/]+)\/cancel$/, roles: ['operator'], h: (_r, p) => store.cancelComparison(dec(p[0])) },
  { m: 'POST', re: /^\/api\/worker\/run$/, roles: ['operator'], h: async () => ({ processed: await store.runWorkerCycle() }) },
  { m: 'GET', re: /^\/api\/proofs$/, roles: ALL, h: () => store.listProofs() },
  { m: 'GET', re: /^\/api\/proofs\/([^/]+)$/, roles: ALL, h: (_r, p) => store.getProof(dec(p[0])) },
  { m: 'POST', re: /^\/api\/proofs\/verify$/, roles: ALL, h: (_r, _p, body) => store.verifyProof(body) },
];

function dec(s) {
  try {
    return decodeURIComponent(s);
  } catch {
    return s;
  }
}

function authenticate(req) {
  const h = req.headers.authorization || '';
  const m = /^Bearer (.+)$/.exec(h);
  if (!m) return null;
  for (const [token, role] of Object.entries(config.tokens)) {
    if (constantTimeEqual(m[1], token)) return role;
  }
  return null;
}

function send(res, status, body) {
  const payload = body === undefined ? '' : JSON.stringify(body);
  res.writeHead(status, {
    'Content-Type': 'application/json; charset=utf-8',
    'Cache-Control': 'no-store',
  });
  res.end(payload);
}

/**
 * Error responses carry a stable code and a static message (plus, at most,
 * field names / indices / sequence numbers in details). Request payloads are
 * never reflected, so an error page cannot echo input package content.
 */
function sendError(res, err) {
  if (err instanceof ApiError) {
    const body = { error: { code: err.code, message: err.message } };
    if (err.details !== undefined) body.error.details = err.details;
    send(res, err.status, body);
    return;
  }
  console.error('internal error:', err);
  send(res, 500, { error: { code: 'INTERNAL', message: 'internal error' } });
}

function readBody(req) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    let size = 0;
    req.on('data', (c) => {
      size += c.length;
      if (size > MAX_BODY) {
        reject(new ApiError(413, 'PAYLOAD_TOO_LARGE', 'request body too large'));
        req.destroy();
        return;
      }
      chunks.push(c);
    });
    req.on('end', () => {
      if (!chunks.length) return resolve(undefined);
      try {
        resolve(JSON.parse(Buffer.concat(chunks).toString('utf8')));
      } catch {
        reject(new ApiError(400, 'BODY_MALFORMED', 'request body must be valid JSON'));
      }
    });
    req.on('error', () => reject(new ApiError(400, 'BODY_MALFORMED', 'could not read request body')));
  });
}

const STATIC = {
  '/': ['index.html', 'text/html; charset=utf-8'],
  '/index.html': ['index.html', 'text/html; charset=utf-8'],
  '/app.js': ['app.js', 'text/javascript; charset=utf-8'],
  '/styles.css': ['styles.css', 'text/css; charset=utf-8'],
};

function sendFile(res, file, type) {
  fs.readFile(path.join(PUBLIC_DIR, file), (err, data) => {
    if (err) return send(res, 404, { error: { code: 'NOT_FOUND', message: 'unknown resource' } });
    res.writeHead(200, { 'Content-Type': type, 'Cache-Control': 'no-store' });
    res.end(data);
  });
}

const server = http.createServer(async (req, res) => {
  try {
    const u = new URL(req.url, 'http://localhost');
    const pathname = u.pathname;

    if (req.method === 'GET' && STATIC[pathname]) {
      const [file, type] = STATIC[pathname];
      return sendFile(res, file, type);
    }
    if (req.method === 'GET' && pathname === '/healthz') return send(res, 200, { ok: true });
    if (!pathname.startsWith('/api/')) {
      return send(res, 404, { error: { code: 'NOT_FOUND', message: 'unknown resource' } });
    }

    const role = authenticate(req);
    if (!role) {
      return send(res, 401, { error: { code: 'UNAUTHENTICATED', message: 'a valid bearer token is required' } });
    }
    const route = ROUTES.find((r) => r.m === req.method && r.re.test(pathname));
    if (!route) return send(res, 404, { error: { code: 'NOT_FOUND', message: 'unknown resource' } });
    if (!route.roles.includes(role)) {
      return send(res, 403, { error: { code: 'FORBIDDEN', message: 'this role may not perform the requested action' } });
    }

    const body = req.method === 'POST' ? await readBody(req) : undefined;
    const result = await route.h(role, route.re.exec(pathname).slice(1), body);
    send(res, 200, result === undefined ? { ok: true } : result);
  } catch (e) {
    sendError(res, e);
  }
});

if (config.workerIntervalMs > 0) {
  const timer = setInterval(async () => {
    try {
      await store.runWorkerCycle();
    } catch (e) {
      console.error('worker cycle failed:', e);
    }
  }, config.workerIntervalMs);
  timer.unref();
}

server.listen(config.port, config.host, () => {
  console.log(`diffdiag listening on http://${config.host}:${config.port} (data: ${config.dataDir})`);
});
