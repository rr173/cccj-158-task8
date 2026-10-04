"""多租户诊断作业：配额冻结、加权轮转、租约与用量账本。

关键不变量：
- 受理与“冻结团队 + 预计成本 + 令牌校验”在同一个立即事务中完成；不满足条件时
  不留下作业或账本孤立行。
- 领取只依据当前团队配置（并发额度、暂停位、权重）；配置变更不抢占已运行槽位。
- 调度顺序按团队代码确定，使用每轮权重票据实现加权轮转，单一团队的突发任务
  不能无限越过其他团队。
- 作业领取、完成、取消与失败均由条件 UPDATE 判定，只有一个状态推进成功。
- 用量账本以 (job_id, event_type) 去重；重试、重复完成通知和崩溃恢复都不会
  重复结算。
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
import uuid
from datetime import datetime, timedelta, timezone

from ..core import store


class DiagnosticError(Exception):
    """请求被确定性拒绝（通常映射为 HTTP 409）。"""


class DiagnosticAuthzError(DiagnosticError):
    """访问不属于调用者的团队/作业（通常映射为 HTTP 403）。"""



# ---------- 基础工具 ----------

def _now() -> datetime:
    return store.utcnow()


def _iso(dt: datetime) -> str:
    return dt.isoformat()


def canonical_body(body: dict) -> str:
    return json.dumps(body, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def billing_period(ts: str | None = None) -> str:
    if ts:
        dt = datetime.fromisoformat(ts)
    else:
        dt = _now()
    return dt.astimezone(timezone.utc).strftime("%Y-%m")


def _job_id() -> str:
    return f"diag_{uuid.uuid4().hex}"


def _team_id() -> str:
    return f"team_{uuid.uuid4().hex[:10]}"


def _get_team_row(conn, team_id: str):
    row = conn.execute("SELECT * FROM diagnostic_teams WHERE id=?", (team_id,)).fetchone()
    if row is None:
        raise store.NotFound(team_id)
    return row


def get_team(team_id: str) -> dict:
    return dict(store.one("SELECT * FROM diagnostic_teams WHERE id=?", (team_id,)))


def _get_job_row(conn, job_id: str):
    row = conn.execute("SELECT * FROM diagnostic_jobs WHERE id=?", (job_id,)).fetchone()
    if row is None:
        raise store.NotFound(job_id)
    return row


def get_job(job_id: str) -> dict:
    return dict(store.one("SELECT * FROM diagnostic_jobs WHERE id=?", (job_id,)))


def _contains_member(members_json: str, username: str) -> bool:
    try:
        return username in json.loads(members_json or "[]")
    except json.JSONDecodeError:
        return False


def _is_unique_violation(exc: Exception) -> bool:
    return isinstance(exc, sqlite3.IntegrityError)


# ---------- 团队配置 ----------

def create_team(name: str, code: str, *, concurrency_limit: int, wait_limit: int,
                weight: int, members: list[str], actor: str,
                team_id: str | None = None) -> dict:
    if concurrency_limit < 0 or wait_limit < 0 or weight <= 0:
        raise DiagnosticError("invalid team quota or weight")
    tid = team_id or _team_id()
    now = _iso(_now())
    conn = store.get_conn()
    conn.execute("BEGIN IMMEDIATE")
    try:
        conn.execute(
            "INSERT INTO diagnostic_teams(id,name,code,concurrency_limit,wait_limit,"
            "weight,paused,rr_credits,members_json,created_at,updated_at)"
            " VALUES(?,?,?,?,?,?,'0',?,?,?,?)",
            (tid, name, code, int(concurrency_limit), int(wait_limit), int(weight),
             int(weight), json.dumps(sorted(set(members)), ensure_ascii=False), now, now))
        store.audit(actor, "diagnostic.team.create", detail={
            "team_id": tid, "code": code, "concurrency_limit": concurrency_limit,
            "wait_limit": wait_limit, "weight": weight})
        conn.commit()
        return get_team(tid)
    except Exception as exc:
        conn.rollback()
        if _is_unique_violation(exc):
            raise DiagnosticError("team code or id already exists") from exc
        raise


def list_teams() -> list[dict]:
    return [dict(r) for r in store.all_rows(
        "SELECT * FROM diagnostic_teams ORDER BY code, id")]


def update_team(team_id: str, actor: str, *, concurrency_limit: int | None = None,
                wait_limit: int | None = None, weight: int | None = None,
                paused: bool | None = None, members: list[str] | None = None) -> dict:
    conn = store.get_conn()
    conn.execute("BEGIN IMMEDIATE")
    try:
        team = _get_team_row(conn, team_id)
        new_limit = team["concurrency_limit"] if concurrency_limit is None else int(concurrency_limit)
        new_wait = team["wait_limit"] if wait_limit is None else int(wait_limit)
        new_weight = team["weight"] if weight is None else int(weight)
        if new_limit < 0 or new_wait < 0 or new_weight <= 0:
            raise DiagnosticError("invalid team quota or weight")

        # 已经领取的运行槽不做迁移；新额度只影响之后的可领取判断。
        # 立即把当前轮转票据夹到新权重内，增重则在下一轮重置时获得完整新权重。
        credits = team["rr_credits"]
        if weight is not None:
            credits = min(credits, weight)
        paused_value = team["paused"] if paused is None else ("1" if paused else "0")
        members_json = team["members_json"] if members is None else json.dumps(
            sorted(set(members)), ensure_ascii=False)
        now = _iso(_now())
        conn.execute(
            "UPDATE diagnostic_teams SET concurrency_limit=?, wait_limit=?, weight=?,"
            " rr_credits=?, paused=?, members_json=?, updated_at=? WHERE id=?",
            (new_limit, new_wait, new_weight, credits, paused_value, members_json,
             now, team_id))
        store.audit(actor, "diagnostic.team.update", detail={
            "team_id": team_id, "concurrency_limit": new_limit,
            "wait_limit": new_wait, "weight": new_weight,
            "paused": paused_value == "1"})
        conn.commit()
        return get_team(team_id)
    except Exception:
        conn.rollback()
        raise


# ---------- 受理：冻结团队和预计成本 ----------

def submit_job(team_id: str, requester: str, body: dict, *,
               request_token: str, estimated_cost: int,
               max_attempts: int = 3) -> dict:
    if not request_token:
        raise DiagnosticError("request_token is required")
    if estimated_cost <= 0:
        raise DiagnosticError("estimated_cost must be positive")
    body = body or {}
    body_json = canonical_body(body)
    fp = hashlib.sha256(body_json.encode("utf-8")).hexdigest()
    jid = _job_id()
    now_dt = _now()
    now = _iso(now_dt)

    conn = store.get_conn()
    conn.execute("BEGIN IMMEDIATE")
    try:
        team = _get_team_row(conn, team_id)
        if not _contains_member(team["members_json"], requester):
            raise DiagnosticAuthzError("requester is not a member of team")

        existing = conn.execute(
            "SELECT * FROM diagnostic_jobs WHERE team_id=? AND request_token=?",
            (team_id, request_token)).fetchone()
        if existing is not None:
            if existing["body_sha256"] != fp:
                # 同键不同体必须拒绝；原作业保持不变。
                raise DiagnosticError("request_token already exists with a different body")
            conn.commit()
            out = dict(existing)
            out["idempotent_replay"] = True
            return out

        waiting = conn.execute(
            "SELECT COALESCE(SUM(estimated_cost),0), COUNT(*) FROM diagnostic_jobs"
            " WHERE team_id=? AND status IN ('queued','retry')",
            (team_id,)).fetchone()
        if waiting[1] >= team["wait_limit"]:
            # 受理后即进入等待队列；等待名额满时拒绝，避免 job/ledger 孤立记录。
            raise DiagnosticError("team waiting limit reached")
        next_seq = conn.execute(
            "SELECT COALESCE(MAX(seq),0)+1 FROM diagnostic_jobs").fetchone()[0]
        conn.execute(
            "INSERT INTO diagnostic_jobs(id,seq,team_id,requester,request_token,body_json,"
            "body_sha256,status,estimated_cost,attempts,max_attempts,available_at,"
            "created_at,updated_at)"
            " VALUES(?,?,?,?,?,?,?,'queued',?,0,?,?,?,?)",
            (jid, next_seq, team_id, requester, request_token, body_json, fp,
             int(estimated_cost), int(max_attempts), now, now, now))
        _insert_ledger(conn, jid, team_id, "reserve", int(estimated_cost), requester, now)
        store.audit(requester, "diagnostic.job.submit", detail={
            "job_id": jid, "team_id": team_id, "estimated_cost": estimated_cost,
            "request_token_sha256": hashlib.sha256(request_token.encode()).hexdigest()})
        conn.commit()
        out = get_job(jid)
        out["idempotent_replay"] = False
        return out
    except Exception as exc:
        conn.rollback()
        if _is_unique_violation(exc):
            prior = store.maybe(
                "SELECT * FROM diagnostic_jobs WHERE team_id=? AND request_token=?",
                (team_id, request_token))
            if prior is not None and prior["body_sha256"] == fp:
                out = dict(prior)
                out["idempotent_replay"] = True
                return out
            raise DiagnosticError("request_token already exists with a different body")
        raise


def _insert_ledger(conn, job_id: str, team_id: str, event_type: str,
                   amount: int, actor: str, ts: str | None = None) -> int:
    ts = ts or _iso(_now())
    cur = conn.execute(
        "INSERT INTO diagnostic_ledger(job_id,team_id,period,event_type,amount,"
        "actor,created_at) VALUES(?,?,?,?,?,?,?)",
        (job_id, team_id, billing_period(ts), event_type, int(amount), actor, ts))
    return cur.lastrowid


# ---------- 条件状态推进和账本 ----------

def cancel_job(job_id: str, requester: str, *, actor_is_member: bool = True) -> dict:
    conn = store.get_conn()
    conn.execute("BEGIN IMMEDIATE")
    try:
        job = _get_job_row(conn, job_id)
        if actor_is_member and job["requester"] != requester:
            raise DiagnosticAuthzError("can only cancel own job")
        cur = conn.execute(
            "UPDATE diagnostic_jobs SET status='cancelled', updated_at=? "
            "WHERE id=? AND status IN ('queued','retry')",
            (_iso(_now()), job_id))
        if cur.rowcount != 1:
            # 与领取并发时，只有一方推进；运行中/终态均不可取消。
            raise DiagnosticError(f"job is not waiting: {job['status']}")
        _insert_ledger(conn, job_id, job["team_id"], "reserve_release",
                       -job["estimated_cost"], requester)
        store.audit(requester, "diagnostic.job.cancel", detail={"job_id": job_id})
        conn.commit()
        return get_job(job_id)
    except Exception:
        conn.rollback()
        raise


def _release_reservation(conn, job, actor: str, event_type: str = "reserve_release") -> None:
    exists = conn.execute(
        "SELECT 1 FROM diagnostic_ledger WHERE job_id=? AND event_type=?",
        (job["id"], event_type)).fetchone()
    if exists is None:
        _insert_ledger(conn, job["id"], job["team_id"], event_type,
                       -job["estimated_cost"], actor)


def record_result(job_id: str, worker_id: str, result: dict, *,
                  actual_cost: int | None = None) -> dict | None:
    """执行进程在租约内封存结果。重复通知返回原封存记录，绝不重算。"""
    result = result or {}
    result_json = canonical_body(result)
    digest = hashlib.sha256(result_json.encode("utf-8")).hexdigest()
    conn = store.get_conn()
    conn.execute("BEGIN IMMEDIATE")
    try:
        job = _get_job_row(conn, job_id)
        prior = conn.execute(
            "SELECT * FROM diagnostic_results WHERE job_id=?", (job_id,)).fetchone()
        if prior is not None:
            conn.commit()
            out = dict(prior)
            out["idempotent_replay"] = True
            return out
        if job["status"] != "running" or job["lease_owner"] != worker_id:
            raise DiagnosticError("worker does not hold an active lease")
        # 内容寻址密文在事务外也不可变；先落 WORM，再在状态事务中锚定。
        conn.commit()
    except Exception:
        conn.rollback()
        raise

    cas_id = store.sealed_blob_put(
        "diagnostic_result", result_json.encode("utf-8"), worker_id)

    conn = store.get_conn()
    conn.execute("BEGIN IMMEDIATE")
    try:
        job = _get_job_row(conn, job_id)
        prior = conn.execute(
            "SELECT * FROM diagnostic_results WHERE job_id=?", (job_id,)).fetchone()
        if prior is not None:
            conn.commit()
            out = dict(prior)
            out["idempotent_replay"] = True
            return out
        if job["status"] != "running" or job["lease_owner"] != worker_id:
            # 租约刚好过期：WORM 成为可审计证据，但不锚定给已经失去租约的进程。
            raise DiagnosticError("worker does not hold an active lease")
        cost = job["estimated_cost"] if actual_cost is None else int(actual_cost)
        if cost <= 0:
            raise DiagnosticError("actual_cost must be positive")
        now = _iso(_now())
        conn.execute(
            "INSERT INTO diagnostic_results(job_id,result_cas_id,result_sha256,"
            "actual_cost,sealed_by,sealed_at) VALUES(?,?,?,?,?,?)",
            (job_id, cas_id, digest, cost, worker_id, now))
        store.audit(worker_id, "diagnostic.result.seal", detail={
            "job_id": job_id, "cas_id": cas_id, "actual_cost": cost})
        conn.commit()
        out = dict(store.one("SELECT * FROM diagnostic_results WHERE job_id=?", (job_id,)))
        out["idempotent_replay"] = False
        return out
    except Exception as exc:
        conn.rollback()
        if _is_unique_violation(exc):
            prior = store.maybe(
                "SELECT * FROM diagnostic_results WHERE job_id=?", (job_id,))
            if prior is not None:
                out = dict(prior)
                out["idempotent_replay"] = True
                return out
        raise


def complete_job(job_id: str, worker_id: str | None = None) -> dict:
    """按已封存结果结算；没有结果不能完成，重复完成通知不重复扣费。"""
    conn = store.get_conn()
    conn.execute("BEGIN IMMEDIATE")
    try:
        job = _get_job_row(conn, job_id)
        if job["status"] == "done":
            conn.commit()
            out = get_job(job_id)
            out["idempotent_replay"] = True
            return out
        result = conn.execute(
            "SELECT * FROM diagnostic_results WHERE job_id=?", (job_id,)).fetchone()
        if result is None:
            raise DiagnosticError("cannot complete a job without sealed result")
        cur = conn.execute(
            "UPDATE diagnostic_jobs SET status='done', lease_owner=NULL,"
            " lease_until=NULL, actual_cost=?, updated_at=? WHERE id=? AND status='running'",
            (result["actual_cost"], _iso(_now()), job_id))
        if cur.rowcount != 1:
            raise DiagnosticError(f"job cannot complete from status {job['status']}")
        _release_reservation(conn, job, worker_id or job["lease_owner"] or "system")
        _insert_ledger(conn, job_id, job["team_id"], "usage",
                       result["actual_cost"], worker_id or "system")
        store.audit(worker_id or "system", "diagnostic.job.complete",
                    detail={"job_id": job_id, "actual_cost": result["actual_cost"]})
        conn.commit()
        out = get_job(job_id)
        out["idempotent_replay"] = False
        return out
    except Exception:
        conn.rollback()
        raise


def fail_job(job_id: str, worker_id: str, error: str, *, fatal: bool = False) -> dict:
    conn = store.get_conn()
    conn.execute("BEGIN IMMEDIATE")
    try:
        job = _get_job_row(conn, job_id)
        result = conn.execute(
            "SELECT * FROM diagnostic_results WHERE job_id=?", (job_id,)).fetchone()
        if job["status"] == "done":
            conn.commit()
            out = get_job(job_id)
            out["idempotent_replay"] = True
            return out
        if result is not None:
            # 结果已封存：失败/重复通知不能触发再次计算，直接完成并只结算一次。
            if job["status"] == "running":
                conn.execute(
                    "UPDATE diagnostic_jobs SET status='done', lease_owner=NULL,"
                    " lease_until=NULL, actual_cost=?, updated_at=? WHERE id=?",
                    (result["actual_cost"], _iso(_now()), job_id))
                _release_reservation(conn, job, worker_id)
                _insert_ledger(conn, job_id, job["team_id"], "usage",
                               result["actual_cost"], worker_id)
                store.audit(worker_id, "diagnostic.job.complete_after_error",
                            detail={"job_id": job_id})
            conn.commit()
            out = get_job(job_id)
            out["idempotent_replay"] = True
            return out

        attempts = job["attempts"] + 1
        now_dt = _now()
        now = _iso(now_dt)
        if fatal or attempts >= job["max_attempts"]:
            cur = conn.execute(
                "UPDATE diagnostic_jobs SET status='failed', attempts=?,"
                " last_error=?, lease_owner=NULL, lease_until=NULL, updated_at=?"
                " WHERE id=? AND status='running'",
                (attempts, error, now, job_id))
            if cur.rowcount != 1:
                raise DiagnosticError(f"job cannot fail from status {job['status']}")
            _release_reservation(conn, job, worker_id)
            store.audit(worker_id, "diagnostic.job.fail",
                        detail={"job_id": job_id, "terminal": True, "error": error})
        else:
            available = _iso(now_dt + timedelta(seconds=min(2 ** attempts, 30)))
            cur = conn.execute(
                "UPDATE diagnostic_jobs SET status='retry', attempts=?,"
                " last_error=?, lease_owner=NULL, lease_until=NULL, available_at=?,"
                " updated_at=? WHERE id=? AND status='running'",
                (attempts, error, available, now, job_id))
            if cur.rowcount != 1:
                raise DiagnosticError(f"job cannot retry from status {job['status']}")
            # 退避中仍保留受理时预留；不释放、不重复冻结。
            store.audit(worker_id, "diagnostic.job.retry",
                        detail={"job_id": job_id, "attempt": attempts, "error": error})
        conn.commit()
        return get_job(job_id)
    except Exception:
        conn.rollback()
        raise


# ---------- 租约恢复 ----------

def _recover_expired_leases(conn, now_value: str) -> int:
    rows = conn.execute(
        "SELECT j.*, r.result_cas_id AS recovered_cas_id,"
        " r.actual_cost AS recovered_cost FROM diagnostic_jobs j"
        " LEFT JOIN diagnostic_results r ON r.job_id=j.id"
        " WHERE j.status='running' AND j.lease_until<=? ORDER BY j.lease_until, j.seq",
        (now_value,)).fetchall()
    recovered = 0
    for row in rows:
        if row["recovered_cas_id"] is not None:
            # 进程可能在 complete 前崩溃：结果已封存，直接终态结算且只扣一次。
            conn.execute(
                "UPDATE diagnostic_jobs SET status='done', lease_owner=NULL,"
                " lease_until=NULL, actual_cost=?, updated_at=? WHERE id=? AND status='running'",
                (row["recovered_cost"], now_value, row["id"]))
            _release_reservation(conn, row, "scheduler")
            usage_exists = conn.execute(
                "SELECT 1 FROM diagnostic_ledger WHERE job_id=? AND event_type='usage'",
                (row["id"],)).fetchone()
            if usage_exists is None:
                _insert_ledger(conn, row["id"], row["team_id"], "usage",
                               row["recovered_cost"], "scheduler")
            store.audit("scheduler", "diagnostic.job.recover_sealed",
                        detail={"job_id": row["id"], "actual_cost": row["recovered_cost"]})
        else:
            # 无封存结果：回到原队列位置，等待再次领取；不增加 attempts。
            conn.execute(
                "UPDATE diagnostic_jobs SET status='queued', lease_owner=NULL,"
                " lease_until=NULL, last_error=COALESCE(last_error,'lease expired'),"
                " available_at=?, updated_at=? WHERE id=? AND status='running'",
                (now_value, now_value, row["id"]))
            store.audit("scheduler", "diagnostic.job.lease_requeue",
                        detail={"job_id": row["id"]})
        recovered += 1
    return recovered

# ---------- 确定的加权轮转领取 ----------

def claim(worker_id: str, lease_seconds: int = 600, *,
          now_override: datetime | None = None) -> dict | None:
    now_dt = now_override or _now()
    now_value = _iso(now_dt)
    lease_until = _iso(now_dt + timedelta(seconds=lease_seconds))
    conn = store.get_conn()
    conn.execute("BEGIN IMMEDIATE")
    try:
        # 租约到期先恢复；随后轮转结果才是确定状态。
        _recover_expired_leases(conn, now_value)

        def refresh_active():
            rows = conn.execute(
                "SELECT j.* FROM diagnostic_teams t"
                " JOIN diagnostic_jobs j ON j.team_id=t.id"
                " WHERE t.paused=0 AND j.status IN ('queued','retry')"
                " AND j.available_at<=? AND j.seq=("
                " SELECT MIN(j2.seq) FROM diagnostic_jobs j2 WHERE j2.team_id=t.id"
                " AND j2.status IN ('queued','retry') AND j2.available_at<=?)"
                " ORDER BY t.code, t.id", (now_value, now_value)).fetchall()
            result = []
            for job in rows:
                team = conn.execute(
                    "SELECT * FROM diagnostic_teams WHERE id=?",
                    (job["team_id"],)).fetchone()
                running_cost = conn.execute(
                    "SELECT COALESCE(SUM(estimated_cost),0) FROM diagnostic_jobs"
                    " WHERE team_id=? AND status='running'",
                    (job["team_id"],)).fetchone()[0]
                feasible = running_cost + job["estimated_cost"] <= team["concurrency_limit"]
                result.append((team, job, feasible))
            return result

        active_teams = refresh_active()
        # 本轮还有可启动且有票据的团队时继续；若仅有票团队都被容量卡住，
        # 而零票团队可启动，则开新轮，避免单团队运行槽长期挡住其他团队。
        if active_teams and not any(
                int(t["rr_credits"]) > 0 and feasible
                for t, _, feasible in active_teams) and any(
                int(t["rr_credits"]) <= 0 and feasible
                for t, _, feasible in active_teams):
            for t, _, _ in active_teams:
                conn.execute("UPDATE diagnostic_teams SET rr_credits=weight WHERE id=?",
                             (t["id"],))
            active_teams = refresh_active()

        chosen_team = chosen_job = None
        for team, job, feasible in active_teams:
            fresh_team = conn.execute(
                "SELECT * FROM diagnostic_teams WHERE id=?", (team["id"],)).fetchone()
            if int(fresh_team["rr_credits"]) <= 0 or not feasible:
                continue
            chosen_team, chosen_job = fresh_team, job
            break

        if chosen_job is None:
            conn.commit()
            return None

        cur = conn.execute(
            "UPDATE diagnostic_jobs SET status='running', lease_owner=?,"
            " lease_until=?, attempts=attempts+1, available_at=NULL, updated_at=?"
            " WHERE id=? AND status IN ('queued','retry') AND available_at<=?",
            (worker_id, lease_until, now_value, chosen_job["id"], now_value))
        if cur.rowcount != 1:
            conn.rollback()
            return claim(worker_id, lease_seconds, now_override=now_dt)
        conn.execute("UPDATE diagnostic_teams SET rr_credits=rr_credits-1 WHERE id=?",
                     (chosen_team["id"],))
        store.audit(worker_id, "diagnostic.job.claim", detail={
            "job_id": chosen_job["id"], "team_id": chosen_team["id"],
            "team_code": chosen_team["code"],
            "remaining_rr_credits": int(chosen_team["rr_credits"]) - 1})
        conn.commit()
        return get_job(chosen_job["id"])
    except Exception:
        conn.rollback()
        raise


# ---------- Worker 辅助 ----------

def run_once(worker_id: str | None = None, *, lease_seconds: int = 600,
             executor=None) -> dict | None:
    worker_id = worker_id or f"diag-worker-{uuid.uuid4().hex[:6]}"
    job = claim(worker_id, lease_seconds=lease_seconds)
    if job is None:
        return None
    try:
        if executor is not None:
            result, actual_cost = executor(job)
        else:
            payload = json.loads(job["body_json"])
            fault = payload.get("fault_inject")
            if fault == "fail":
                raise RuntimeError("injected diagnostic failure")
            result = {
                "job_id": job["id"], "team_id": job["team_id"],
                "checksum": job["body_sha256"], "ok": True,
                "summary": f"diagnostic analysis {payload.get('kind', 'generic')}"}
            actual_cost = job["estimated_cost"]
        record_result(job["id"], worker_id, result, actual_cost=actual_cost)
        complete_job(job["id"], worker_id)
        return {"job_id": job["id"], "team_id": job["team_id"],
                "outcome": "done", "worker_id": worker_id}
    except Exception as exc:
        fresh = get_job(job["id"])
        if fresh["status"] != "running" or fresh["lease_owner"] != worker_id:
            return {"job_id": job["id"], "team_id": job["team_id"],
                    "outcome": fresh["status"], "worker_id": worker_id,
                    "note": "lease lost before failure was recorded"}
        outcome = fail_job(job["id"], worker_id,
                           f"{type(exc).__name__}: {exc}")["status"]
        return {"job_id": job["id"], "team_id": job["team_id"],
                "outcome": outcome, "worker_id": worker_id,
                "error": str(exc)}


# ---------- 查询视图 ----------

def _team_usage(conn, team_id: str, period: str | None = None) -> dict:
    where = "team_id=? AND event_type='usage'"
    args: list = [team_id]
    if period:
        where += " AND period=?"
        args.append(period)
    row = conn.execute(
        f"SELECT COALESCE(SUM(amount),0) amount, COALESCE(COUNT(*),0) events"
        f" FROM diagnostic_ledger WHERE {where}", args).fetchone()
    return {"usage_cost": row["amount"], "billed_jobs": row["events"]}


def _team_view(conn, team) -> dict:
    out = dict(team)
    out["members"] = json.loads(team["members_json"] or "[]")
    out.pop("members_json", None)
    reserved = conn.execute(
        "SELECT COALESCE(SUM(estimated_cost),0), COUNT(*) FROM diagnostic_jobs"
        " WHERE team_id=? AND status IN ('queued','retry')",
        (team["id"],)).fetchone()
    running = conn.execute(
        "SELECT COALESCE(SUM(estimated_cost),0), COUNT(*) FROM diagnostic_jobs"
        " WHERE team_id=? AND status='running'", (team["id"],)).fetchone()
    out["waiting_cost"] = reserved[0]
    out["waiting_jobs"] = reserved[1]
    out["running_cost"] = running[0]
    out["running_jobs"] = running[1]
    out["occupied_cost"] = reserved[0] + running[0]
    out["available_cost"] = max(0, team["concurrency_limit"] - running[0])
    out.update(_team_usage(conn, team["id"]))
    return out


def team_views() -> list[dict]:
    conn = store.get_conn()
    teams = conn.execute(
        "SELECT * FROM diagnostic_teams ORDER BY code, id").fetchall()
    return [_team_view(conn, t) for t in teams]


def rotation_view() -> list[dict]:
    views = team_views()
    if not views:
        return []
    # 当前一轮按固定团队顺序展示票据；0 票据团队会在下一轮重置为权重。
    if all(v["rr_credits"] <= 0 for v in views if not v["paused"] and v["waiting_jobs"]):
        for v in views:
            v["next_round_credits"] = v["weight"] if not v["paused"] else v["rr_credits"]
    else:
        for v in views:
            v["next_round_credits"] = v["rr_credits"]
    order = sorted(views, key=lambda v: (v["code"], v["id"]))
    for i, v in enumerate(order, 1):
        v["rotation_position"] = i
        v["eligible_now"] = (not v["paused"] and v["waiting_jobs"] > 0
                             and v["rr_credits"] > 0)
    return order


def _sanitize_job(row, *, include_body: bool = False, viewer: str | None = None) -> dict:
    out = dict(row)
    if include_body:
        try:
            out["body"] = json.loads(row["body_json"])
        except json.JSONDecodeError:
            out["body"] = None
    else:
        out["body"] = None
    out.pop("body_json", None)
    if viewer is not None and out.get("requester") != viewer:
        out["request_token"] = None
    return out


def list_jobs(team_id: str | None = None, status: str | None = None,
              *, include_body: bool = False, viewer: str | None = None) -> list[dict]:
    q = "SELECT * FROM diagnostic_jobs"
    where, args = [], []
    if team_id:
        where.append("team_id=?"); args.append(team_id)
    if status:
        where.append("status=?"); args.append(status)
    if where:
        q += " WHERE " + " AND ".join(where)
    q += " ORDER BY seq DESC"
    return [_sanitize_job(r, include_body=include_body, viewer=viewer)
            for r in store.all_rows(q, tuple(args))]


def get_result(job_id: str) -> dict:
    row = store.one("SELECT * FROM diagnostic_results WHERE job_id=?", (job_id,))
    out = dict(row)
    out["result"] = json.loads(store.sealed_blob_get(row["result_cas_id"]).decode("utf-8"))
    return out


def usage(team_id: str | None = None, period: str | None = None,
          *, include_events: bool = True) -> list[dict]:
    conn = store.get_conn()
    teams = conn.execute("SELECT * FROM diagnostic_teams ORDER BY code, id").fetchall()
    if team_id is not None:
        teams = [t for t in teams if t["id"] == team_id]
    output = []
    for team in teams:
        args: list = []
        where = "team_id=?"
        args.append(team["id"])
        if period:
            where += " AND period=?"
            args.append(period)
        sums = conn.execute(
            f"SELECT period, COALESCE(SUM(CASE WHEN event_type='usage' THEN amount ELSE 0 END),0)"
            f" usage_cost, COALESCE(SUM(CASE WHEN event_type='usage' THEN 1 ELSE 0 END),0)"
            f" billed_jobs, COALESCE(SUM(CASE WHEN event_type='reserve' THEN amount ELSE 0 END),0)"
            f" reserved_cost, COALESCE(SUM(CASE WHEN event_type='reserve_release' THEN amount ELSE 0 END),0)"
            f" released_cost FROM diagnostic_ledger WHERE {where} GROUP BY period ORDER BY period",
            tuple(args)).fetchall()
        periods = [dict(r) for r in sums]
        item = {"team_id": team["id"], "team_code": team["code"],
                "team_name": team["name"], "periods": periods}
        if include_events:
            ev_args = list(args)
            ev_rows = conn.execute(
                f"SELECT seq,job_id,period,event_type,amount,actor,created_at"
                f" FROM diagnostic_ledger WHERE {where} ORDER BY seq",
                tuple(ev_args)).fetchall()
            item["events"] = [dict(r) for r in ev_rows]
        output.append(item)
    return output


def usage_summary(period: str | None = None) -> list[dict]:
    # 观察者只核验团队/周期级聚合，不暴露团队成员、作业、令牌或正文。
    return [{"team_code": x["team_code"], "periods": x["periods"]}
            for x in usage(period=period, include_events=False)]


def seed_default_teams(actor: str = "system") -> None:
    defaults = [
        ("Alpha Analysis", "alpha", ["analyst-a"], 2, 10, 1),
        ("Beta Analysis", "beta", ["analyst-b"], 4, 10, 2),
    ]
    for name, code, members, limit, wait, weight in defaults:
        if store.maybe("SELECT 1 FROM diagnostic_teams WHERE code=?", (code,)) is None:
            create_team(name, code, concurrency_limit=limit, wait_limit=wait,
                        weight=weight, members=members, actor=actor,
                        team_id=f"team_{code}")
