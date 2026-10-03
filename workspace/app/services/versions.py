"""派生版本管理。

不可变规则：
- versions(study_id, version_no) 唯一，版本号在病例锁内单调分配；
- 策略升级/重新处理一律新建版本，旧版本行与其 Blob 永不覆盖；
- 版本内容在 qc_status='passed' 之前绝不发布；latest_version_id 只指向已发布版本。
"""
from __future__ import annotations

import json
import uuid

from ..core import store
from . import locks, policies as policies_svc


class QCBlocked(store.ConflictError):
    pass


def create_version(study_id: str, actor: str, *, policy_id: str | None = None,
                   parent_version_id: str | None = None,
                   reason: str = "re-derivation",
                   fault_inject: str | None = None) -> dict:
    """在病例锁内分配新版本号并入队。并发修订只会得到不同版本号，不会互相覆盖。"""
    owner = f"{actor}:{uuid.uuid4().hex[:8]}"
    locks.acquire(study_id, owner)
    conn = store.get_conn()
    try:
        conn.execute("BEGIN IMMEDIATE")
        study = conn.execute("SELECT * FROM studies WHERE id=?", (study_id,)).fetchone()
        if study is None:
            conn.rollback()
            raise store.NotFound(study_id)
        if policy_id is None:
            pol = policies_svc.latest_policy("project-default")
            policy_id = pol["id"]
        else:
            pol = policies_svc.get_policy(policy_id)
        row = conn.execute(
            "SELECT COALESCE(MAX(version_no),0) AS v FROM versions WHERE study_id=?",
            (study_id,)).fetchone()
        version_no = row["v"] + 1
        version_id = f"ver_{uuid.uuid4().hex[:12]}"
        conn.execute(
            "INSERT INTO versions(id,study_id,version_no,policy_id,parent_version_id,"
            " status,created_by,created_at) VALUES(?,?,?,?,?, 'processing',?,?)",
            (version_id, study_id, version_no, policy_id, parent_version_id,
             actor, store.now_iso()))
        payload = {"fault_inject": fault_inject} if fault_inject else {}
        job_id = enqueue_locked(conn, study_id, version_id, "derive", payload)
        store.audit(actor, "version.create", study_id=study_id, detail={
            "version_id": version_id, "version_no": version_no,
            "policy_id": policy_id, "policy_name_version":
                f"{pol['name']}@{pol['version']}", "parent": parent_version_id,
            "reason": reason, "job_id": job_id})
        conn.commit()
        return get_version(version_id)
    except Exception:
        conn.rollback()
        raise
    finally:
        locks.release(study_id, owner)


def enqueue_locked(conn, study_id: str, version_id: str, job_type: str,
                   payload: dict) -> str:
    job_id = f"job_{uuid.uuid4().hex[:12]}"
    now = store.now_iso()
    conn.execute(
        "INSERT INTO jobs(id,study_id,version_id,job_type,status,attempts,"
        " payload_json,created_at,updated_at,available_at)"
        " VALUES(?,?,?,?,'queued',0,?,?,?,?)",
        (job_id, study_id, version_id, job_type,
         json.dumps(payload), now, now, now))
    return job_id


def get_version(version_id: str) -> dict:
    r = store.one("SELECT * FROM versions WHERE id=?", (version_id,))
    out = dict(r)
    out["steps"] = json.loads(r["steps_json"])
    out["qc_report"] = json.loads(r["qc_report_json"]) if r["qc_report_json"] else None
    return out


def list_versions(study_id: str) -> list[dict]:
    rows = store.all_rows("SELECT id FROM versions WHERE study_id=? ORDER BY version_no",
                          (study_id,))
    return [get_version(r["id"]) for r in rows]


def list_qc_queue(status: str | None = None) -> list[dict]:
    q = ("SELECT v.*, s.pseudo_patient_id, s.pseudo_patient_name FROM versions v"
         " JOIN studies s ON s.id=v.study_id WHERE 1=1")
    args: tuple = ()
    if status:
        q += " AND v.qc_status=?"; args = (status,)
    q += " ORDER BY v.created_at DESC"
    out = []
    for r in store.all_rows(q, args):
        d = dict(r)
        d["steps"] = json.loads(r["steps_json"])
        d["qc_report"] = json.loads(r["qc_report_json"]) if r["qc_report_json"] else None
        out.append(d)
    return out


# ---------- worker 侧状态推进（条件 UPDATE，防并发发布半成品） ----------

def mark_processing_failed(version_id: str, error: str, steps: list) -> None:
    conn = store.get_conn()
    conn.execute(
        "UPDATE versions SET status='failed', error=?, steps_json=? WHERE id=?",
        (error, json.dumps(steps), version_id))


def persist_steps(version_id: str, steps: list) -> None:
    """失败尝试只追加处理步骤，版本仍保持 processing 以允许后续重试发布。"""
    conn = store.get_conn()
    conn.execute(
        "UPDATE versions SET steps_json=? WHERE id=? AND status='processing'",
        (json.dumps(steps), version_id))


def propose_qc(version_id: str, derived_cas_id: str, steps: list,
               qc_report: dict) -> str:
    """根据自动质检结果把版本推进到 passed/failed。返回新 qc_status。"""
    conn = store.get_conn()
    conn.execute("BEGIN IMMEDIATE")
    try:
        cur = conn.execute(
            "UPDATE versions SET derived_cas_id=?, steps_json=?, qc_report_json=?,"
            " qc_status=CASE WHEN ?=1 THEN 'passed' ELSE 'failed' END,"
            " status=CASE WHEN ?=1 THEN 'ready' ELSE 'quarantined' END,"
            " error=CASE WHEN ?=1 THEN NULL ELSE 'auto_qc_failed' END"
            " WHERE id=? AND status='processing'",
            (derived_cas_id, json.dumps(steps), json.dumps(qc_report),
             1 if qc_report["pass"] else 0,
             1 if qc_report["pass"] else 0,
             1 if qc_report["pass"] else 0, version_id))
        if cur.rowcount != 1:
            conn.rollback()
            raise QCBlocked(f"version {version_id} not in processing state")
        conn.commit()
        return "passed" if qc_report["pass"] else "failed"
    except Exception:
        conn.rollback()
        raise


# ---------- 人工复核 ----------

def manual_qc(version_id: str, actor: str, approve: bool, note: str = "") -> dict:
    """技师对自动质检已通过的版本做人工放行/否决。否决后必须重新派生新版本。"""
    conn = store.get_conn()
    conn.execute("BEGIN IMMEDIATE")
    try:
        ver = conn.execute("SELECT * FROM versions WHERE id=?", (version_id,)).fetchone()
        if ver is None:
            conn.rollback(); raise store.NotFound(version_id)
        if ver["qc_status"] != "passed" or ver["status"] not in ("ready", "published"):
            conn.rollback()
            raise QCBlocked("only auto-QC-passed versions await manual review")
        if approve:
            conn.execute(
                "UPDATE versions SET status='published', qc_status='approved',"
                " published_at=?, qc_by=?, qc_at=?, qc_note=? WHERE id=?",
                (store.now_iso(), actor, store.now_iso(), note, version_id))
            conn.execute(
                "UPDATE studies SET latest_version_id=? WHERE id="
                "(SELECT study_id FROM versions WHERE id=?)",
                (version_id, version_id))
        else:
            conn.execute(
                "UPDATE versions SET status='quarantined', qc_status='rejected',"
                " qc_by=?, qc_at=?, qc_note=? WHERE id=?",
                (actor, store.now_iso(), note, version_id))
            # 最新版本被否决则回退指针，避免 latest 指向不可用版本
            conn.execute(
                "UPDATE studies SET latest_version_id=("
                " SELECT id FROM versions WHERE study_id=? AND status='published'"
                " ORDER BY version_no DESC LIMIT 1) WHERE id=? AND latest_version_id=?",
                (ver["study_id"], ver["study_id"], version_id))
        store.audit(actor, "qc.manual", study_id=ver["study_id"],
                    detail={"version_id": version_id, "approve": approve, "note": note})
        conn.commit()
        return get_version(version_id)
    except Exception:
        conn.rollback()
        raise
