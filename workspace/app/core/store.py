"""持久化层：SQLite 事务状态 + 内容寻址 Blob 存储（WORM）。

关键不变量：
- Blob 一经写入不可变（按 sha256 寻址，文件以 0444 落盘）。
- versions(study_id, version_no) 唯一：同影像并发修订只能得到不同版本号。
- 所有状态推进使用条件 UPDATE（带期望状态），靠 rowcount 检测竞态。
- 队列领取、回链登记均为单事务原子操作。
"""
from __future__ import annotations

import json
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path

from .. import config
from . import crypto


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def now_iso() -> str:
    return utcnow().isoformat()


class ConflictError(Exception):
    """乐观锁/状态机冲突（并发竞争失败）。"""


class NotFound(Exception):
    pass


_tls = threading.local()


def get_conn(reset: bool = False) -> sqlite3.Connection:
    """返回线程内连接；路径随 MEDDEID_HOME 变化或 reset 时自动重连。"""
    conn = getattr(_tls, "conn", None)
    if conn is not None and (reset or getattr(_tls, "db_path", None) != str(config.DB_PATH)):
        conn.close()
        conn = None
        _tls.conn = None
    if conn is None:
        config.ensure_dirs()
        conn = sqlite3.connect(str(config.DB_PATH), timeout=30, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute("PRAGMA synchronous=NORMAL")
        _tls.conn = conn
        _tls.db_path = str(config.DB_PATH)
    return conn


SCHEMA = """
CREATE TABLE IF NOT EXISTS objects (
    cas_id TEXT PRIMARY KEY,
    sha256 TEXT NOT NULL,
    size INTEGER NOT NULL,
    kind TEXT NOT NULL,
    created_by TEXT, created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS patient_pseudonyms (
    identity_fp TEXT PRIMARY KEY,
    pseudo_patient_id TEXT NOT NULL UNIQUE,
    pseudo_patient_name TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS studies (
    id TEXT PRIMARY KEY,
    source_cas_id TEXT NOT NULL,
    source_sha256 TEXT NOT NULL,
    frozen_summary_cas_id TEXT NOT NULL,
    identity_fp TEXT NOT NULL,
    pseudo_patient_id TEXT NOT NULL,
    pseudo_patient_name TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'active',
    latest_version_id TEXT,
    created_by TEXT, created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS identity_revisions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    study_id TEXT NOT NULL REFERENCES studies(id),
    seq INTEGER NOT NULL,
    reason TEXT, envelope TEXT NOT NULL,
    created_by TEXT, created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS policies (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    version INTEGER NOT NULL,
    content_cas_id TEXT NOT NULL,
    created_by TEXT, created_at TEXT NOT NULL, retired_at TEXT,
    UNIQUE(name, version)
);
CREATE TABLE IF NOT EXISTS versions (
    id TEXT PRIMARY KEY,
    study_id TEXT NOT NULL REFERENCES studies(id),
    version_no INTEGER NOT NULL,
    policy_id TEXT NOT NULL REFERENCES policies(id),
    parent_version_id TEXT,
    status TEXT NOT NULL DEFAULT 'processing',
    derived_cas_id TEXT,
    steps_json TEXT NOT NULL DEFAULT '[]',
    qc_status TEXT NOT NULL DEFAULT 'pending',
    qc_report_json TEXT,
    qc_by TEXT, qc_at TEXT, qc_note TEXT,
    error TEXT,
    created_by TEXT, created_at TEXT NOT NULL, published_at TEXT,
    UNIQUE(study_id, version_no)
);
CREATE TABLE IF NOT EXISTS jobs (
    id TEXT PRIMARY KEY,
    study_id TEXT NOT NULL,
    version_id TEXT NOT NULL,
    job_type TEXT NOT NULL DEFAULT 'derive',
    status TEXT NOT NULL DEFAULT 'queued',
    attempts INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 3,
    lease_owner TEXT, lease_until TEXT,
    available_at TEXT,
    payload_json TEXT NOT NULL DEFAULT '{}',
    last_error TEXT,
    created_at TEXT NOT NULL, updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_jobs_pick ON jobs(status, lease_until);
CREATE TABLE IF NOT EXISTS study_locks (
    study_id TEXT PRIMARY KEY,
    owner TEXT NOT NULL, acquired_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS reidentifications (
    id TEXT PRIMARY KEY,
    study_id TEXT NOT NULL REFERENCES studies(id),
    version_id TEXT NOT NULL REFERENCES versions(id),
    purpose TEXT NOT NULL,
    requested_by TEXT NOT NULL,
    approved_by TEXT,
    window_start TEXT NOT NULL,
    window_end TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'requested',
    envelope TEXT,
    share_a_fp TEXT,
    share_b_fp TEXT,
    export_cas_id TEXT,
    exported_at TEXT,
    revoke_reason TEXT,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS relinks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    reid_id TEXT NOT NULL REFERENCES reidentifications(id),
    step_no INTEGER NOT NULL,
    role TEXT NOT NULL,
    actor TEXT NOT NULL,
    share_fp TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE(reid_id, step_no)
);
CREATE TABLE IF NOT EXISTS audit_log (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    ts TEXT NOT NULL,
    actor TEXT NOT NULL,
    action TEXT NOT NULL,
    study_id TEXT,
    reid_id TEXT,
    detail_json TEXT NOT NULL,
    prev_mac TEXT NOT NULL,
    mac TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS audit_snapshots (
    -- 验真快照：服务端在确定截止点（cutoff_seq）取数后冻结，永不更新。
    -- idempotency_key 唯一：刷新/重复点击/并发创建都只能取回同一快照。
    id TEXT PRIMARY KEY,
    idempotency_key TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL,
    created_by TEXT NOT NULL,
    filters_json TEXT NOT NULL,
    cutoff_seq INTEGER NOT NULL,
    cutoff_ts TEXT,
    anchor_mac TEXT NOT NULL,
    record_count INTEGER NOT NULL,
    first_seq INTEGER,
    last_seq INTEGER,
    pack_cas_id TEXT NOT NULL,
    pack_size INTEGER NOT NULL,
    manifest_sha256 TEXT NOT NULL,
    records_sha256 TEXT NOT NULL,
    frozen_result_json TEXT NOT NULL
);
"""


def init_db(reset: bool = False) -> None:
    config.ensure_dirs()
    conn = get_conn(reset=reset)
    conn.executescript(SCHEMA)


# ---------- 内容寻址存储 ----------

def blob_put(kind: str, data: bytes, actor: str | None = None) -> str:
    digest = crypto.sha256_hex(data)
    shard = config.BLOB_DIR / digest[:2] / digest
    if not shard.exists():
        shard.parent.mkdir(parents=True, exist_ok=True)
        tmp = shard.with_suffix(".tmp")
        tmp.write_bytes(data)
        tmp.chmod(0o444)
        tmp.replace(shard)  # 原子落位；并发同内容以先到者为准
        shard.chmod(0o444)
        get_conn().execute(
            "INSERT OR IGNORE INTO objects(cas_id,sha256,size,kind,created_by,created_at) VALUES(?,?,?,?,?,?)",
            (digest, digest, len(data), kind, actor, now_iso()),
        )
    return digest


def blob_get(cas_id: str) -> bytes:
    path = config.BLOB_DIR / cas_id[:2] / cas_id
    if not path.exists():
        raise NotFound(f"blob {cas_id} missing")
    return path.read_bytes()


def blob_path(cas_id: str) -> Path:
    return config.BLOB_DIR / cas_id[:2] / cas_id


def sealed_blob_put(kind: str, data: bytes, actor: str | None = None) -> str:
    """落盘即加密：Blob 内容为信封密文（原始 DICOM、冻结摘要等含 PHI 数据）。"""
    envelope = json.dumps(crypto.seal_envelope(data)).encode()
    return blob_put(kind, envelope, actor)


def sealed_blob_get(cas_id: str) -> bytes:
    return crypto.open_envelope(json.loads(blob_get(cas_id)))


# ---------- 查询辅助 ----------

def one(query: str, args=()):
    row = get_conn().execute(query, args).fetchone()
    if row is None:
        raise NotFound(query)
    return row


def maybe(query: str, args=()):
    return get_conn().execute(query, args).fetchone()


def all_rows(query: str, args=()):
    return get_conn().execute(query, args).fetchall()


# ---------- 审计链 ----------

def audit(actor: str, action: str, *, study_id: str | None = None,
          reid_id: str | None = None, detail: dict | None = None) -> dict:
    """追加一条哈希链审计记录，返回整行（含 seq/mac）。

    若调用方已持有显式事务则加入该事务（随调用方提交/回滚）；
    否则自行开启 IMMEDIATE 事务，串行化写入防链分叉。
    """
    conn = get_conn()
    joined = conn.in_transaction
    if not joined:
        conn.execute("BEGIN IMMEDIATE")
    try:
        ts = now_iso()
        last = conn.execute("SELECT mac FROM audit_log ORDER BY seq DESC LIMIT 1").fetchone()
        prev = last["mac"] if last else "GENESIS"
        detail_json = json.dumps(detail or {}, sort_keys=True, separators=(",", ":"))
        canonical = json.dumps(
            [ts, actor, action, study_id, reid_id, detail_json], sort_keys=True, separators=(",", ":")
        )
        mac = crypto.audit_mac(prev, canonical)
        cur = conn.execute(
            "INSERT INTO audit_log(ts,actor,action,study_id,reid_id,detail_json,prev_mac,mac)"
            " VALUES(?,?,?,?,?,?,?,?)",
            (ts, actor, action, study_id, reid_id, detail_json, prev, mac),
        )
        seq = cur.lastrowid
        if not joined:
            conn.commit()
        return {"seq": seq, "ts": ts, "actor": actor, "action": action,
                "study_id": study_id, "reid_id": reid_id, "detail": json.loads(detail_json),
                "prev_mac": prev, "mac": mac}
    except Exception:
        if not joined:
            conn.rollback()
        raise


def verify_audit_chain() -> dict:
    rows = all_rows("SELECT * FROM audit_log ORDER BY seq")
    prev = "GENESIS"
    for r in rows:
        canonical = json.dumps(
            [r["ts"], r["actor"], r["action"], r["study_id"], r["reid_id"], r["detail_json"]],
            sort_keys=True, separators=(",", ":"),
        )
        expect = crypto.audit_mac(prev, canonical)
        if not crypto.hmac_compare(expect, r["mac"]) or r["prev_mac"] != prev:
            return {"ok": False, "broken_at": r["seq"], "count": len(rows)}
        prev = r["mac"]
    return {"ok": True, "count": len(rows)}
