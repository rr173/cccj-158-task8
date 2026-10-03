"""处理队列：租约领取、指数退避重试、死信隔离。

半成品安全保证：
- 派生 Blob 只有在自动质检通过后才会挂到 versions 行并进入 ready；
  处理失败只留下 failed/quarantined 版本行与处理步骤，绝不更新 latest 指针。
- 同一任务同一时刻只有一个 worker 持租约；worker 崩溃后租约到期才可被重新领取。
- 达到重试上限的任务进入 dead 状态，需人工显式 requeue，不会静默重发。
"""
from __future__ import annotations

import io
import json
import time
import uuid
from datetime import timedelta

import pydicom

from .. import config
from ..core import dicomutil, store
from . import policies as policies_svc
from . import versions as versions_svc


class JobStateError(store.ConflictError):
    pass


def claim(worker_id: str, lease_seconds: int = config.JOB_LEASE_SECONDS,
          ignore_backoff: bool = False) -> dict | None:
    conn = store.get_conn()
    conn.execute("BEGIN IMMEDIATE")
    try:
        now_dt = store.utcnow()
        now, lease_until = now_dt.isoformat(), (now_dt + timedelta(seconds=lease_seconds)).isoformat()
        if ignore_backoff:
            row = conn.execute(
                "SELECT * FROM jobs WHERE status IN ('queued','retry')"
                " ORDER BY created_at LIMIT 1").fetchone()
        else:
            row = conn.execute(
                "SELECT * FROM jobs WHERE status IN ('queued','retry') AND available_at<=?"
                " ORDER BY created_at LIMIT 1", (now,)).fetchone()
        if row is None:
            # 回收过期租约：worker 崩溃保护
            expired = conn.execute(
                "SELECT * FROM jobs WHERE status='running' AND lease_until<=?"
                " ORDER BY updated_at LIMIT 1", (now,)).fetchone()
            if expired is None:
                conn.commit()
                return None
            row = expired
            reason = "lease_expired_reclaimed"
        else:
            reason = "claimed"
        conn.execute(
            "UPDATE jobs SET status='running', lease_owner=?, lease_until=?,"
            " attempts=attempts+1, updated_at=? WHERE id=? AND status IN ('queued','retry','running')",
            (worker_id, lease_until, now, row["id"]))
        store.audit(worker_id, "job.claim", study_id=row["study_id"],
                    detail={"job_id": row["id"], "reason": reason,
                            "attempt": row["attempts"] + 1})
        conn.commit()
        return dict(conn.execute("SELECT * FROM jobs WHERE id=?", (row["id"],)).fetchone())
    except Exception:
        conn.rollback()
        raise


def _reschedule(job: dict, error: str, *, fatal: bool) -> str:
    conn = store.get_conn()
    attempts = job["attempts"]
    if fatal or attempts >= job["max_attempts"]:
        conn.execute(
            "UPDATE jobs SET status='dead', last_error=?, lease_owner=NULL,"
            " lease_until=NULL, updated_at=? WHERE id=?",
            (error, store.now_iso(), job["id"]))
        return "dead"
    backoff = min(2 ** attempts, 30)
    available = (store.utcnow() + timedelta(seconds=backoff)).isoformat()
    conn.execute(
        "UPDATE jobs SET status='retry', last_error=?, lease_owner=NULL,"
        " lease_until=NULL, available_at=?, updated_at=? WHERE id=?",
        (error, available, store.now_iso(), job["id"]))
    return "retry"


def fail_job(job: dict, error: str, *, fatal: bool = False) -> str:
    """处理失败：有步骤就先落步骤，再重试/死信。绝不发布。"""
    new_status = _reschedule(job, error, fatal=fatal)
    ver = versions_svc.get_version(job["version_id"])
    if new_status == "dead":
        versions_svc.mark_processing_failed(job["version_id"], error, ver["steps"])
    store.audit(job["lease_owner"] or "system", "job.fail",
                study_id=job["study_id"],
                detail={"job_id": job["id"], "error": error, "next": new_status})
    return new_status


def complete_job(job: dict) -> None:
    conn = store.get_conn()
    conn.execute(
        "UPDATE jobs SET status='done', lease_owner=NULL, lease_until=NULL,"
        " updated_at=? WHERE id=?", (store.now_iso(), job["id"]))
    store.audit(job["lease_owner"] or "system", "job.done",
                study_id=job["study_id"], detail={"job_id": job["id"]})


def requeue(job_id: str, actor: str, payload_override: dict | None = None) -> dict:
    conn = store.get_conn()
    conn.execute("BEGIN IMMEDIATE")
    try:
        row = conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
        if row is None:
            conn.rollback(); raise store.NotFound(job_id)
        if row["status"] != "dead":
            conn.rollback()
            raise JobStateError("only dead jobs can be manually requeued")
        now = store.now_iso()
        payload = (json.dumps(payload_override) if payload_override is not None
                   else row["payload_json"])
        conn.execute(
            "UPDATE jobs SET status='queued', attempts=0, last_error=NULL,"
            " payload_json=?, lease_owner=NULL, lease_until=NULL, available_at=?,"
            " updated_at=? WHERE id=?",
            (payload, now, now, job_id))
        conn.execute(
            "UPDATE versions SET status='processing', qc_status='pending',"
            " error=NULL WHERE id=? AND status='failed'", (row["version_id"],))
        store.audit(actor, "job.requeue", study_id=row["study_id"],
                    detail={"job_id": job_id, "version_id": row["version_id"]})
        conn.commit()
        return dict(conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone())
    except Exception:
        conn.rollback()
        raise


def list_jobs(status: str | None = None) -> list[dict]:
    q = "SELECT * FROM jobs"
    args: tuple = ()
    if status:
        q += " WHERE status=?"; args = (status,)
    q += " ORDER BY created_at DESC"
    return [dict(r) for r in store.all_rows(q, args)]


# ---------- 实际处理 ----------

def _execute_derive(job: dict) -> None:
    payload = json.loads(job["payload_json"] or "{}")
    study = store.one("SELECT * FROM studies WHERE id=?", (job["study_id"],))
    ver = store.one("SELECT * FROM versions WHERE id=?", (job["version_id"],))
    pol = policies_svc.get_policy(ver["policy_id"])
    policy = pol["content"]

    source = store.sealed_blob_get(study["source_cas_id"])
    steps: list[dict] = []
    try:
        # 1) 标签清理 + 替代身份替换
        t0 = time.time()
        data, tag_steps = dicomutil.apply_tag_policy(
            source, policy, study["pseudo_patient_id"], study["pseudo_patient_name"])
        steps.extend(tag_steps)
        steps.append({"step": "step_timing", "detail": {"tag_ms": int((time.time() - t0) * 1000)}})

        # 2) 像素文字遮盖
        t0 = time.time()
        mode = payload.get("fault_inject")
        if mode == "redaction":
            # 遮盖子系统异常：任务进入重试/死信，版本不发布
            det = dicomutil.detect_burned_text(
                pydicom.dcmread(io.BytesIO(data), force=True).pixel_array)
            steps.append({"step": "pixel_redaction",
                          "detail": {"attempted": True, "outcome": "redaction_failure",
                                     "detected_before": det["present"]}})
            raise RuntimeError("redaction subsystem failure: burned text left in pixels")
        if mode == "qc_leak":
            # 遮盖被错误跳过：流水线继续，由质检门把半成品隔离（不发布）
            steps.append({"step": "pixel_redaction",
                          "detail": {"attempted": False, "outcome": "skipped"}})
        else:
            data, red_detail = dicomutil.redact_burned_text(data)
            red_detail["ms"] = int((time.time() - t0) * 1000)
            steps.append({"step": "pixel_redaction", "detail": red_detail})

        # 3) 自动质检（发布门）
        qc_report = dicomutil.quality_check(
            data, policy, study["pseudo_patient_id"], study["pseudo_patient_name"])
        steps.append({"step": "auto_qc", "detail": {"pass": qc_report["pass"],
                                                     "problems": qc_report["problems"]}})

        # 4) 无论通过与否都先落内容寻址 Blob（失败证据也可审计），再条件推进状态
        derived_cas = store.sealed_blob_put("derived_dicom", data, job["lease_owner"])
        steps.append({"step": "blob_store", "detail": {"cas_id": derived_cas}})
        qc_status = versions_svc.propose_qc(job["version_id"], derived_cas, steps, qc_report)
        store.audit(job["lease_owner"], "derive.auto_qc", study_id=study["id"],
                    detail={"version_id": job["version_id"], "qc": qc_status,
                            "problems": qc_report["problems"]})
    except Exception as exc:
        # 失败时保留已完成步骤以便排障；不写 derived_cas、不发布、不移动 latest
        if steps:
            versions_svc.persist_steps(job["version_id"], steps)
        raise


def run_once(worker_id: str | None = None, *, ignore_backoff: bool = False) -> dict | None:
    worker_id = worker_id or f"worker-{uuid.uuid4().hex[:6]}"
    job = claim(worker_id, ignore_backoff=ignore_backoff)
    if job is None:
        return None
    try:
        if job["job_type"] == "derive":
            _execute_derive(job)
        complete_job(job)
        return {"job_id": job["id"], "outcome": "done"}
    except Exception as exc:
        job = dict(store.one("SELECT * FROM jobs WHERE id=?", (job["id"],)))
        outcome = fail_job(job, f"{type(exc).__name__}: {exc}")
        return {"job_id": job["id"], "outcome": outcome, "error": str(exc)}


def run_loop(max_cycles: int | None = None, idle_sleep: float = 0.05) -> int:
    cycles = processed = 0
    while max_cycles is None or cycles < max_cycles:
        result = run_once()
        cycles += 1
        if result is None:
            if max_cycles is not None:
                break
            time.sleep(idle_sleep)
            continue
        processed += 1
        if result["outcome"] == "retry" and max_cycles is not None:
            # 测试场景：退避中的任务等下一轮回收；这里直接结束让调用方决定
            break
    return processed
