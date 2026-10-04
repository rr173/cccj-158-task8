"""诊断作业：多租户配额、加权轮转公平排队、用量账本。

核心不变量：
- 受理（submit）在单个 IMMEDIATE 事务内冻结：所属团队、预估成本、请求正文指纹。
  * request_token 唯一：令牌与正文完全相同 → 幂等返回原作业（不重复计费/排队）；
    令牌相同而正文（或预估成本）变化 → 拒绝，不落任何记录。
  * 等待作业数已达团队 wait_cap，或单个成本超过并发额度 → 拒绝，不产生孤立记录。
- 调度器（claim）使用确定性平滑加权轮转（smooth WRR）：信用持久化，
  某团队长期领取份额正比于权重且领取交错均匀，持续提交不能长期挤占其他团队。
- 额度/权重调整只作用于之后的领取；已经占用的执行槽不被强制转移（不抢占）。
  暂停团队不累积信用、游标位置保留，恢复后从原轮转位置继续参与。
- 执行进程退出后租约到期的作业回到 waiting 可被再次领取；但已经封存的结果
  （done+sealed_at）不能被再次计算或重复扣减用量（完成入账走唯一约束）。
- 取消等待作业立即释放等待占用；取消与领取并发时由条件 UPDATE 决定，只有一方推进。
- 用量按团队 + 结算周期（UTC YYYY-MM）累加，主键 (period, team_id, job_id)：
  重复完成通知、重启恢复、失败重试都不能造成双重计费。
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
import uuid
from datetime import datetime, timedelta, timezone

from .. import config
from ..core import crypto, store

# ---------- 异常 ----------


class DiagError(Exception):
    """诊断子系统通用业务错误。"""


class QuotaError(DiagError):
    """受理/排队容量被拒（等待上限、成本超过额度等）。"""


class TokenConflict(DiagError):
    """同一令牌但请求内容变化：确定性拒绝。"""

    def __init__(self, message: str, existing_job_id: str):
        super().__init__(message)
        self.existing_job_id = existing_job_id


class JobStateError(DiagError):
    """作业当前状态不允许该推进。"""

    def __init__(self, message: str, *, status: str | None = None):
        super().__init__(message)
        self.status = status


WAITING = "waiting"
RUNNING = "running"
DONE = "done"
FAILED = "failed"
CANCELLED = "cancelled"
WAIT_EXPIRED = "wait_expired"

TERMINAL = {DONE, FAILED, CANCELLED, WAIT_EXPIRED}

DEFAULT_TEAMS = [
    # id, name, quota, wait_cap, weight
    ("team-alpha", "Alpha 影像分析组", 2, 100, 1),
    ("team-beta", "Beta 病理分析组", 1, 100, 2),
    ("team-gamma", "Gamma 组学分析组", 3, 100, 1),
]


def _iso_after(now_dt: datetime, seconds: int) -> str:
    return (now_dt + timedelta(seconds=seconds)).isoformat()


def _period(dt: datetime | None = None) -> str:
    dt = dt or datetime.now(timezone.utc)
    return dt.astimezone(timezone.utc).strftime("%Y-%m")


# ---------- 团队管理 ----------

def ensure_defaults(actor: str = "system") -> None:
    """幂等播种默认团队与调度器单例行。"""
    conn = store.get_conn()
    conn.execute("BEGIN IMMEDIATE")
    try:
        now = store.now_iso()
        for tid, name, quota, cap, weight in DEFAULT_TEAMS:
            conn.execute(
                "INSERT INTO diag_teams(id,name,concurrency_quota,wait_cap,weight,"
                " paused,created_by,created_at,updated_at)"
                " VALUES(?,?,?,?,?,0,?,?,?)"
                " ON CONFLICT(id) DO NOTHING",
                (tid, name, quota, cap, weight, actor, now, now))
        conn.execute(
            "INSERT INTO diag_scheduler(id,claim_count,cursor_team_id,credits_json,"
            " updated_at) VALUES(1,0,NULL,'{}',?) ON CONFLICT(id) DO NOTHING", (now,))
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def create_team(team_id: str, name: str, *, concurrency_quota: int, wait_cap: int,
                weight: int, actor: str) -> dict:
    _require_positive(concurrency=concurrency_quota, wait_cap=wait_cap, weight=weight)
    conn = store.get_conn()
    now = store.now_iso()
    conn.execute("BEGIN IMMEDIATE")
    try:
        if conn.execute("SELECT 1 FROM diag_teams WHERE id=? OR name=?",
                        (team_id, name)).fetchone():
            conn.rollback()
            raise DiagError(f"team already exists: {team_id}")
        conn.execute(
            "INSERT INTO diag_teams(id,name,concurrency_quota,wait_cap,weight,"
            " paused,created_by,created_at,updated_at)"
            " VALUES(?,?,?,?,?,0,?,?,?)",
            (team_id, name, concurrency_quota, wait_cap, weight, actor, now, now))
        store.audit(actor, "diag.team.create", detail={
            "team_id": team_id, "concurrency_quota": concurrency_quota,
            "wait_cap": wait_cap, "weight": weight})
        conn.commit()
        return get_team(team_id)
    except Exception:
        conn.rollback()
        raise


def _require_positive(**values: int) -> None:
    for key, val in values.items():
        if not isinstance(val, int) or val < 1:
            raise DiagError(f"{key} must be a positive integer, got {val!r}")


def set_quota(team_id: str, concurrency_quota: int, actor: str) -> dict:
    """调整并发额度：只作用于之后的领取；运行中的执行槽不被强制转移。"""
    _require_positive(concurrency=concurrency_quota)
    return _update_team(team_id, actor, {"concurrency_quota": concurrency_quota},
                        "diag.team.quota")


def set_weight(team_id: str, weight: int, actor: str) -> dict:
    """调整权重：影响之后的轮转；暂停团队也保留其轮转信用。"""
    _require_positive(weight=weight)
    return _update_team(team_id, actor, {"weight": weight}, "diag.team.weight")


def set_wait_cap(team_id: str, wait_cap: int, actor: str) -> dict:
    _require_positive(wait_cap=wait_cap)
    return _update_team(team_id, actor, {"wait_cap": wait_cap}, "diag.team.wait_cap")


def set_paused(team_id: str, paused: bool, actor: str) -> dict:
    """暂停/恢复：不清空队列、不动轮转信用；恢复后从原轮转位置继续。"""
    return _update_team(team_id, actor, {"paused": 1 if paused else 0},
                        "diag.team.pause" if paused else "diag.team.resume")


def _update_team(team_id: str, actor: str, fields: dict, action: str) -> dict:
    conn = store.get_conn()
    conn.execute("BEGIN IMMEDIATE")
    try:
        row = conn.execute("SELECT * FROM diag_teams WHERE id=?", (team_id,)).fetchone()
        if row is None:
            conn.rollback()
            raise store.NotFound(team_id)
        sets = ", ".join(f"{k}=?" for k in fields)
        conn.execute(
            f"UPDATE diag_teams SET {sets}, updated_at=? WHERE id=?",
            (*fields.values(), store.now_iso(), team_id))
        store.audit(actor, action, detail={"team_id": team_id, **fields})
        conn.commit()
        return get_team(team_id)
    except Exception:
        conn.rollback()
        raise


def team_for_user(username: str) -> str:
    team_id = config.USER_TEAM.get(username)
    if team_id is None:
        raise DiagError(f"user {username} is not a member of any analysis team")
    if store.maybe("SELECT 1 FROM diag_teams WHERE id=?", (team_id,)) is None:
        raise store.NotFound(team_id)
    return team_id


def get_team(team_id: str) -> dict:
    return dict(store.one("SELECT * FROM diag_teams WHERE id=?", (team_id,)))


def list_teams() -> list[dict]:
    return [dict(r) for r in store.all_rows("SELECT * FROM diag_teams ORDER BY id")]


# ---------- 占用度量 ----------

def occupancy(team_id: str) -> dict:
    conn = store.get_conn()
    row = conn.execute(
        "SELECT"
        " COALESCE(SUM(CASE WHEN status='running' THEN estimated_cost END),0) AS running_cost,"
        " COALESCE(SUM(CASE WHEN status='waiting' THEN estimated_cost END),0) AS waiting_cost,"
        " SUM(CASE WHEN status='running' THEN 1 ELSE 0 END) AS running_jobs,"
        " SUM(CASE WHEN status='waiting' THEN 1 ELSE 0 END) AS waiting_jobs"
        " FROM diag_jobs WHERE team_id=? AND status IN ('waiting','running')",
        (team_id,)).fetchone()
    return {"running_cost": row["running_cost"], "waiting_cost": row["waiting_cost"],
            "running_jobs": row["running_jobs"] or 0,
            "waiting_jobs": row["waiting_jobs"] or 0}


def teams_overview() -> list[dict]:
    out = []
    for team in list_teams():
        occ = occupancy(team["id"])
        out.append({**team, **occ,
                    "running_available": team["concurrency_quota"] - occ["running_cost"]})
    return out


# ---------- 正文规范化与成本 ----------

def canonical_body(body: dict) -> bytes:
    """确定性序列化：同语义请求（忽略 token）必须逐字节相同。"""
    payload = {k: body[k] for k in sorted(body) if k != "request_token"}
    return json.dumps(payload, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False).encode()


def estimate_cost(body: dict) -> int:
    """预估成本只由正文内容决定（令牌不参与），保证重放可核对。"""
    explicit = body.get("estimated_cost")
    if explicit is not None:
        cost = explicit
    else:
        # 确定性成本模型：基础 1 + 每项分析种类 1（仅演示成本会计）
        kinds = body.get("analyses") or body.get("analysis") or []
        if isinstance(kinds, str):
            kinds = [kinds]
        cost = 1 + len(kinds)
    if not isinstance(cost, int) or cost < 1:
        raise DiagError("estimated_cost must be a positive integer")
    return int(cost)


# ---------- 受理 ----------

def submit_job(username: str, body: dict, *, wait_seconds: int | None = None) -> dict:
    """受理诊断作业：冻结团队与预计成本；令牌/正文幂等与冲突检测。

    超等待上限或成本超额度时拒绝且不落任何记录（无孤立记录）。
    """
    team_id = team_for_user(username)
    token = body.get("request_token")
    if not token or not isinstance(token, str):
        raise DiagError("request_token is required")
    raw = canonical_body(body)
    body_sha = crypto.sha256_hex(raw)
    cost = estimate_cost(body)
    wait_seconds = (config.DIAG_WAIT_DEADLINE_SECONDS if wait_seconds is None
                    else wait_seconds)
    if not isinstance(wait_seconds, int) or wait_seconds < 0:
        raise DiagError("wait_seconds must be a non-negative integer")

    conn = store.get_conn()
    conn.execute("BEGIN IMMEDIATE")
    try:
        team = conn.execute("SELECT * FROM diag_teams WHERE id=?", (team_id,)).fetchone()
        if team is None:
            conn.rollback()
            raise store.NotFound(team_id)
        existing = conn.execute(
            "SELECT * FROM diag_jobs WHERE request_token=?", (token,)).fetchone()
        if existing is not None:
            conn.rollback()
            return _idempotent_or_conflict(existing, body_sha, cost, username)

        if cost > team["concurrency_quota"]:
            conn.rollback()
            raise QuotaError(
                f"estimated_cost {cost} exceeds team concurrency quota "
                f"{team['concurrency_quota']}; job cannot ever run")
        waiting_n = conn.execute(
            "SELECT COUNT(*) AS c FROM diag_jobs WHERE team_id=? AND status='waiting'",
            (team_id,)).fetchone()["c"]
        if waiting_n >= team["wait_cap"]:
            conn.rollback()
            raise QuotaError(
                f"team {team_id} wait cap reached: {waiting_n}/{team['wait_cap']}"
                " waiting jobs; reservation rejected")

        now_dt = store.utcnow()
        now = now_dt.isoformat()
        deadline = None if wait_seconds == 0 else _iso_after(now_dt, wait_seconds)
        job_id = f"djob_{uuid.uuid4().hex[:12]}"
        # 正文含可能敏感的诊断参数：信封加密后落 WORM 内容寻址存储
        body_cas = store.sealed_blob_put("diag_request", raw, username)
        parameters = {k: body[k] for k in sorted(body)
                      if k not in ("request_token", "estimated_cost", "fault_inject")}
        conn.execute(
            "INSERT INTO diag_jobs(id,team_id,submitted_by,request_token,body_cas_id,"
            " body_sha256,parameters_json,estimated_cost,status,wait_cap,wait_deadline,"
            " max_attempts,available_at,created_at,updated_at)"
            " VALUES(?,?,?,?,?,?,?,?,'waiting',?,?,3,?,?,?)",
            (job_id, team_id, username, token, body_cas, body_sha,
             json.dumps(parameters, sort_keys=True, separators=(",", ":")),
             cost, team["wait_cap"], deadline, now, now, now))
        store.audit(username, "diag.job.submit", detail={
            "diag_job_id": job_id, "team_id": team_id,
            "request_token": token, "body_sha256": body_sha,
            "estimated_cost": cost, "wait_cap": team["wait_cap"],
            "wait_deadline": deadline})
        conn.commit()
        return get_job(job_id)
    except Exception:
        conn.rollback()
        raise


def _idempotent_or_conflict(existing, body_sha: str, cost: int,
                            username: str) -> dict:
    # 令牌相同而内容变化（正文指纹或成本）→ 拒绝，绝不新建、绝不覆盖冻结值
    if existing["body_sha256"] != body_sha or existing["estimated_cost"] != cost:
        raise TokenConflict(
            "request_token already used with a different request body/estimated cost",
            existing_job_id=existing["id"])
    # 令牌+正文完全相同 → 指向原作业；提交者也必须一致
    if existing["submitted_by"] != username or existing["team_id"] != team_for_user(username):
        raise TokenConflict(
            "request_token already bound to another team/submitter",
            existing_job_id=existing["id"])
    job = dict(existing)
    job["idempotent_replay"] = True
    return job


# ---------- 取消（条件 UPDATE：与领取竞争时只有一方推进） ----------

def cancel_job(job_id: str, username: str) -> dict:
    conn = store.get_conn()
    conn.execute("BEGIN IMMEDIATE")
    try:
        row = conn.execute("SELECT * FROM diag_jobs WHERE id=?", (job_id,)).fetchone()
        if row is None:
            conn.rollback()
            raise store.NotFound(job_id)
        if row["submitted_by"] != username:
            conn.rollback()
            # 越权与不存在同形：不泄露存在性
            raise store.NotFound(job_id)
        cur = conn.execute(
            "UPDATE diag_jobs SET status='cancelled', updated_at=?,"
            " lease_owner=NULL, lease_until=NULL"
            " WHERE id=? AND status='waiting'",
            (store.now_iso(), job_id))
        if cur.rowcount != 1:
            conn.rollback()
            # 已被领取/已终结：取消落败，不推进状态
            raise JobStateError(
                f"job is {row['status']}, only waiting jobs can be cancelled",
                status=row["status"])
        store.audit(username, "diag.job.cancel", detail={
            "diag_job_id": job_id, "team_id": row["team_id"],
            "released_cost": row["estimated_cost"]})
        conn.commit()
        return get_job(job_id)
    except Exception:
        conn.rollback()
        raise


# ---------- 等待上限（wait_deadline）清扫 ----------

def sweep_wait_expired(actor: str = "scheduler") -> list[str]:
    """把超过等待上限的 waiting 作业终结为 wait_expired 并释放等待占用。

    显式终结（而非删除）：不留孤儿，账本/审计可核对；终态不再占用任何额度。
    """
    conn = store.get_conn()
    conn.execute("BEGIN IMMEDIATE")
    try:
        now = store.now_iso()
        rows = conn.execute(
            "SELECT * FROM diag_jobs WHERE status='waiting'"
            " AND wait_deadline IS NOT NULL AND wait_deadline<=?",
            (now,)).fetchall()
        ids = []
        for r in rows:
            cur = conn.execute(
                "UPDATE diag_jobs SET status='wait_expired', updated_at=?"
                " WHERE id=? AND status='waiting'", (now, r["id"]))
            if cur.rowcount == 1:
                ids.append(r["id"])
                store.audit(actor, "diag.job.wait_expired", detail={
                    "diag_job_id": r["id"], "team_id": r["team_id"],
                    "wait_deadline": r["wait_deadline"]})
        conn.commit()
        return ids
    except Exception:
        conn.rollback()
        raise


# ---------- 加权轮转领取（平滑加权轮转，跨进程确定） ----------

def _team_ids_ordered() -> list[str]:
    return [r["id"] for r in store.all_rows("SELECT id FROM diag_teams ORDER BY id")]


def _eligible_team_ids(conn, teams: dict, now: str, respect_backoff: bool) -> list[str]:
    """当前真正能被服务的团队：未暂停、有空闲运行成本、有可取（含退避到期）作业。

    只有可服务团队参与信用累积：暂停/满载团队的信用不增长，恢复后从既有信用
    继续（队列与轮转位置都保留），不会突然插队或长期霸占。
    """
    eligible = []
    for tid in sorted(teams):
        team = teams[tid]
        if team["paused"]:
            continue
        running_cost = conn.execute(
            "SELECT COALESCE(SUM(estimated_cost),0) AS c FROM diag_jobs"
            " WHERE team_id=? AND status='running'", (tid,)).fetchone()["c"]
        avail = team["concurrency_quota"] - running_cost
        if avail <= 0:
            continue
        q = ("SELECT 1 FROM diag_jobs WHERE team_id=? AND status='waiting'"
             " AND estimated_cost<=?")
        args: list = [tid, avail]
        if respect_backoff:
            q += " AND (available_at IS NULL OR available_at<=?)"
            args.append(now)
        q += " LIMIT 1"
        if conn.execute(q, args).fetchone() is not None:
            eligible.append(tid)
    return eligible


def _desc_key(team_id: str) -> tuple:
    # 信用并列时按团队 id 升序确定性打破平局（max 取键最大，故用负 ord 反转）
    return tuple(-ord(c) for c in team_id)


def claim(worker_id: str, lease_seconds: int = config.DIAG_JOB_LEASE_SECONDS,
          *, respect_backoff: bool = True) -> dict | None:
    """按确定性平滑加权轮转（smooth WRR）领取一个作业；无可领取时返回 None。

    单事务内：清扫超等待上限 → 回收到期租约（保留排队位置）→
    对每个当前可服务团队累加 weight 信用，选信用最高者领取，并从其信用扣除
    可服务团队权重之和。长期份额正比于权重、领取交错均匀；信用持久化，
    跨进程顺序确定，某团队持续提交也不能长期挤占其他团队。
    """
    conn = store.get_conn()
    conn.execute("BEGIN IMMEDIATE")
    try:
        now_dt = store.utcnow()
        now, lease_until = now_dt.isoformat(), _iso_after(now_dt, lease_seconds)

        # 1) 等待上限：超期的等待作业终结（不参与本轮）
        expired = conn.execute(
            "SELECT id,team_id,wait_deadline FROM diag_jobs WHERE status='waiting'"
            " AND wait_deadline IS NOT NULL AND wait_deadline<=?",
            (now,)).fetchall()
        for r in expired:
            conn.execute(
                "UPDATE diag_jobs SET status='wait_expired', updated_at=?"
                " WHERE id=? AND status='waiting'", (now, r["id"]))
            store.audit(worker_id, "diag.job.wait_expired", detail={
                "diag_job_id": r["id"], "team_id": r["team_id"],
                "wait_deadline": r["wait_deadline"]})

        # 2) 执行进程退出保护：租约到期的 running 作业回到 waiting（保留排队位置）。
        #    已封存结果（sealed_at 非空）的作业不回收：结果不能再次计算。
        reclaimed = conn.execute(
            "SELECT id,team_id FROM diag_jobs WHERE status='running'"
            " AND lease_until<=? AND sealed_at IS NULL", (now,)).fetchall()
        for r in reclaimed:
            conn.execute(
                "UPDATE diag_jobs SET status='waiting', lease_owner=NULL,"
                " lease_until=NULL, updated_at=? WHERE id=?", (now, r["id"]))
            store.audit(worker_id, "diag.job.lease_reclaimed", detail={
                "diag_job_id": r["id"], "team_id": r["team_id"]})

        # 3) 平滑加权轮转选择
        sched = conn.execute("SELECT * FROM diag_scheduler WHERE id=1").fetchone()
        if sched is None:
            conn.execute(
                "INSERT INTO diag_scheduler(id,claim_count,cursor_team_id,credits_json,"
                " updated_at) VALUES(1,0,NULL,'{}',?)", (now,))
            sched = conn.execute("SELECT * FROM diag_scheduler WHERE id=1").fetchone()
        credits = json.loads(sched["credits_json"] or "{}")
        teams = {r["id"]: dict(r) for r in
                 conn.execute("SELECT * FROM diag_teams").fetchall()}
        eligible = _eligible_team_ids(conn, teams, now, respect_backoff)
        if not eligible:
            conn.execute("UPDATE diag_scheduler SET updated_at=? WHERE id=1", (now,))
            conn.commit()
            return None

        for tid in eligible:
            credits[tid] = credits.get(tid, 0) + teams[tid]["weight"]
        chosen = max(eligible, key=lambda t: (credits[t], _desc_key(t)))
        weight_sum = sum(teams[t]["weight"] for t in eligible)
        credits[chosen] -= weight_sum
        team = teams[chosen]

        running_cost = conn.execute(
            "SELECT COALESCE(SUM(estimated_cost),0) AS c FROM diag_jobs"
            " WHERE team_id=? AND status='running'", (chosen,)).fetchone()["c"]
        avail = team["concurrency_quota"] - running_cost
        q = ("SELECT * FROM diag_jobs WHERE team_id=? AND status='waiting'"
             " AND estimated_cost<=?")
        args: list = [chosen, avail]
        if respect_backoff:
            q += " AND (available_at IS NULL OR available_at<=?)"
            args.append(now)
        q += " ORDER BY created_at, id LIMIT 1"
        cand = conn.execute(q, args).fetchone()
        if cand is None:
            conn.rollback()  # eligible 判定与取作业同事务，理论不可达
            return None

        # 条件 UPDATE 领取：与取消竞争时只有一方成功
        cur = conn.execute(
            "UPDATE diag_jobs SET status='running', lease_owner=?, lease_until=?,"
            " attempts=attempts+1, available_at=NULL, updated_at=?"
            " WHERE id=? AND status='waiting'",
            (worker_id, lease_until, now, cand["id"]))
        if cur.rowcount != 1:
            # 极端并发下落败（取消抢先）：不推进，交由下一轮
            conn.rollback()
            return claim(worker_id, lease_seconds, respect_backoff=respect_backoff)

        claim_count = sched["claim_count"] + 1
        conn.execute(
            "UPDATE diag_scheduler SET claim_count=?, cursor_team_id=?,"
            " credits_json=?, updated_at=? WHERE id=1",
            (claim_count, chosen, json.dumps(credits, sort_keys=True), now))
        store.audit(worker_id, "diag.job.claim", detail={
            "diag_job_id": cand["id"], "team_id": chosen,
            "claim_no": claim_count, "weight": team["weight"],
            "credit_after": credits[chosen], "eligible_weight_sum": weight_sum,
            "reason": "lease_expired_reclaimed" if cand["lease_until"] else "claimed"})
        conn.commit()
        return get_job(cand["id"])
    except Exception:
        conn.rollback()
        raise


def rotation_view() -> dict:
    """运维视图：轮转信用、下一个被领取团队、各团队占用与可领取性。"""
    conn = store.get_conn()
    sched = conn.execute("SELECT * FROM diag_scheduler WHERE id=1").fetchone()
    credits = json.loads(sched["credits_json"] or "{}") if sched else {}
    now = store.now_iso()
    teams_rows = {r["id"]: dict(r) for r in
                  conn.execute("SELECT * FROM diag_teams").fetchall()}
    eligible = _eligible_team_ids(conn, teams_rows, now, True)
    projected = dict(credits)
    for tid in eligible:
        projected[tid] = projected.get(tid, 0) + teams_rows[tid]["weight"]
    next_team = (max(eligible, key=lambda t: (projected[t], _desc_key(t)))
                 if eligible else None)
    teams = []
    for tid in _team_ids_ordered():
        team = get_team(tid)
        occ = occupancy(tid)
        teams.append({**team, **occ,
                      "credit": credits.get(tid, 0),
                      "eligible_now": tid in eligible,
                      "running_available":
                          team["concurrency_quota"] - occ["running_cost"]})
    order = sorted(teams, key=lambda x: (-projected.get(x["id"], 0), x["id"]))
    return {"claim_count": sched["claim_count"] if sched else 0,
            "next_team_id": next_team,
            "eligible": eligible,
            "order": [t["id"] for t in order], "teams": teams}


# ---------- 执行与结果封存 ----------

def _job_body(job: dict) -> dict:
    cas = job.get("body_cas_id")
    if cas is None:
        # 传入的是 get_job/claim 的脱敏视图：从库中取封存正文引用
        cas = store.one("SELECT body_cas_id FROM diag_jobs WHERE id=?",
                        (job["id"],))["body_cas_id"]
    return json.loads(store.sealed_blob_get(cas).decode())


def execute_job(job: dict) -> dict:
    """确定性“诊断计算”：只读冻结的请求正文，产出结果报告（不落盘）。

    fault_inject=execute_error：模拟处理异常（触发重试/失败，不封存结果）。
    """
    body = _job_body(job)
    if body.get("fault_inject") == "execute_error":
        raise RuntimeError("injected execution failure")
    kinds = body.get("analyses") or body.get("analysis") or ["general"]
    if isinstance(kinds, str):
        kinds = [kinds]
    findings = []
    for k in kinds:
        seed = hashlib.sha256(job["id"].encode() + k.encode()).hexdigest()[:8]
        findings.append({"analysis": k, "score": int(seed, 16) % 100,
                         "verdict": "within_limits"})
    return {
        "schema": "diag-result/v1",
        "job_id": job["id"],
        "team_id": job["team_id"],
        "request_sha256": job["body_sha256"],
        "findings": findings,
        "produced_by": job["lease_owner"],
        "produced_at": store.now_iso(),
    }


def complete_job(job_id: str, worker_id: str, result: dict) -> dict:
    """封存结果并按作业唯一键入账（幂等完成通知）。

    - 作业须由 worker_id 持有效租约且未封存；
    - 成功只可能入账一次：(period, team_id, job_id) 唯一约束 + 条件状态推进；
    - 重复完成通知（含重启后重发）返回原结果与 already_billed=true，不重复扣减；
    - 非持有者/过期租约/他人结果一律拒绝（封存结果不能被再次计算或覆盖）。
    """
    conn = store.get_conn()
    conn.execute("BEGIN IMMEDIATE")
    try:
        now_dt = store.utcnow()
        now = now_dt.isoformat()
        row = conn.execute("SELECT * FROM diag_jobs WHERE id=?", (job_id,)).fetchone()
        if row is None:
            conn.rollback()
            raise store.NotFound(job_id)

        # 已经封存：绝不再次计算/覆盖；重复通知幂等返回，不重复计费
        if row["status"] == DONE and row["sealed_at"] is not None:
            ledger = conn.execute(
                "SELECT * FROM diag_team_usage WHERE team_id=? AND job_id=?",
                (row["team_id"], job_id)).fetchone()
            conn.rollback()
            return {"job_id": job_id, "status": DONE, "idempotent_replay": True,
                    "already_billed": True,
                    "result_cas_id": row["result_cas_id"],
                    "billed_cost": ledger["cost"] if ledger else 0,
                    "period": ledger["period"] if ledger else None}

        if row["status"] != RUNNING:
            conn.rollback()
            raise JobStateError(f"job is {row['status']}, expected running",
                                status=row["status"])
        if row["lease_owner"] != worker_id:
            conn.rollback()
            raise JobStateError("job lease is held by another worker",
                                status=row["status"])
        if row["lease_until"] is None or row["lease_until"] <= now:
            conn.rollback()
            raise JobStateError("job lease expired before completion",
                                status=row["status"])
        if result.get("job_id") not in (None, job_id):
            conn.rollback()
            raise DiagError("result is bound to a different job")

        final_cost = row["estimated_cost"]
        raw = json.dumps(result, sort_keys=True, separators=(",", ":"),
                         ensure_ascii=False).encode()
        result_sha = crypto.sha256_hex(raw)
        # 封存结果：信封加密 + WORM 0444；第二次封存必走上面的幂等分支
        result_cas = store.sealed_blob_put("diag_result", raw, worker_id)

        # 条件推进 + 入账同一事务：要么都发生，要么都不发生
        cur = conn.execute(
            "UPDATE diag_jobs SET status='done', result_cas_id=?, result_sha256=?,"
            " sealed_at=?, billed_at=?, lease_owner=NULL, lease_until=NULL,"
            " updated_at=? WHERE id=? AND status='running' AND lease_owner=?",
            (result_cas, result_sha, now, now, now, job_id, worker_id))
        if cur.rowcount != 1:
            conn.rollback()
            raise JobStateError("lost the completion race; job no longer running"
                                " under this lease", status=RUNNING)
        period = _period(now_dt)
        try:
            conn.execute(
                "INSERT INTO diag_team_usage(period,team_id,job_id,cost,billed_at)"
                " VALUES(?,?,?,?,?)",
                (period, row["team_id"], job_id, final_cost, now))
        except sqlite3.IntegrityError:
            # 重启/重复通知竞态：账本已存在（绝无双扣），以既存入账为准
            conn.rollback()
            existing_usage = store.one(
                "SELECT * FROM diag_team_usage WHERE team_id=? AND job_id=?",
                (row["team_id"], job_id))
            return {"job_id": job_id, "status": DONE, "idempotent_replay": True,
                    "already_billed": True, "result_cas_id": result_cas,
                    "billed_cost": existing_usage["cost"],
                    "period": existing_usage["period"]}
        store.audit(worker_id, "diag.job.done", detail={
            "diag_job_id": job_id, "team_id": row["team_id"],
            "result_cas_id": result_cas, "result_sha256": result_sha,
            "period": period, "cost": final_cost})
        conn.commit()
        return {"job_id": job_id, "status": DONE, "idempotent_replay": False,
                "already_billed": False, "result_cas_id": result_cas,
                "result_sha256": result_sha, "billed_cost": final_cost,
                "period": period}
    except Exception:
        conn.rollback()
        raise


# ---------- 失败/重试 ----------

def fail_job(job: dict, error: str, *, actor: str | None = None) -> str:
    """执行失败：未到上限→回 waiting 退避重试（释放运行槽，不计费）；
    达上限→终态 failed（释放运行槽，不计费）。失败重试永不双重计费。"""
    conn = store.get_conn()
    conn.execute("BEGIN IMMEDIATE")
    try:
        now = store.now_iso()
        row = conn.execute("SELECT * FROM diag_jobs WHERE id=?", (job["id"],)).fetchone()
        if row is None:
            conn.rollback()
            raise store.NotFound(job["id"])
        attempts = row["attempts"]
        actor = actor or row["lease_owner"] or "system"
        if attempts >= row["max_attempts"]:
            conn.execute(
                "UPDATE diag_jobs SET status='failed', last_error=?,"
                " lease_owner=NULL, lease_until=NULL, updated_at=? WHERE id=?",
                (error, now, job["id"]))
            next_status = FAILED
        else:
            avail = (store.utcnow()
                     + timedelta(seconds=config.DIAG_RETRY_BACKOFF_SECONDS)).isoformat()
            conn.execute(
                "UPDATE diag_jobs SET status='waiting', last_error=?,"
                " lease_owner=NULL, lease_until=NULL, available_at=?, updated_at=?"
                " WHERE id=?", (error, avail, now, job["id"]))
            next_status = WAITING
        store.audit(actor, "diag.job.fail", detail={
            "diag_job_id": job["id"], "team_id": row["team_id"],
            "attempts": attempts, "next": next_status, "error": error})
        conn.commit()
        return next_status
    except Exception:
        conn.rollback()
        raise


# ---------- worker 单次循环 ----------

def run_once(worker_id: str | None = None, *, execute: bool = True,
             lease_seconds: int = config.DIAG_JOB_LEASE_SECONDS) -> dict | None:
    """领取并（可选）执行一个诊断作业。

    execute=False 时只领取（测试编排崩溃/租约过期用）；
    execute_error 故障注入下作业失败重试/失败，绝不封存结果、不计费。
    """
    worker_id = worker_id or f"diag-worker-{uuid.uuid4().hex[:6]}"
    job = claim(worker_id, lease_seconds)
    if job is None:
        return None
    if not execute:
        return {"job_id": job["id"], "outcome": "claimed", "worker": worker_id}
    try:
        # 执行需要读取封存正文：从库里取完整行（get_job 视图已剔除 body_cas_id）
        raw_job = dict(store.one("SELECT * FROM diag_jobs WHERE id=?", (job["id"],)))
        result = execute_job(raw_job)
        sealed = complete_job(job["id"], worker_id, result)
        return {"job_id": job["id"], "outcome": "done", "worker": worker_id,
                "idempotent_replay": sealed.get("idempotent_replay", False)}
    except Exception as exc:
        fresh = dict(store.one("SELECT * FROM diag_jobs WHERE id=?", (job["id"],)))
        next_status = fail_job(fresh, f"{type(exc).__name__}: {exc}",
                               actor=worker_id)
        return {"job_id": job["id"], "outcome": next_status,
                "error": str(exc), "worker": worker_id}


# ---------- 查询 ----------

def _viewer_can_access(job: dict, viewer: str | None, roles: tuple[str, ...]) -> bool:
    if viewer is None:
        return True
    if {"admin", "operator"} & set(roles):
        return True
    try:
        team_id = team_for_user(viewer)
    except (DiagError, store.NotFound):
        return False
    return job["team_id"] == team_id and job["submitted_by"] == viewer


def get_job(job_id: str, *, viewer: str | None = None,
            roles: tuple[str, ...] = ()) -> dict:
    row = store.one("SELECT * FROM diag_jobs WHERE id=?", (job_id,))
    job = dict(row)
    job["parameters"] = json.loads(row["parameters_json"])
    job.pop("body_cas_id", None)
    if viewer is not None and not _viewer_can_access(job, viewer, roles):
        # 分析者只能读本人作业；越权与不存在同形
        raise store.NotFound(job_id)
    return job


def get_job_result(job_id: str, *, viewer: str | None = None,
                   roles: tuple[str, ...] = ()) -> dict:
    """取已封存结果（信封解封）。仅终态 done 可取；只允许本人或管理员。"""
    job = get_job(job_id, viewer=viewer, roles=roles)
    if job["status"] != DONE or not job["result_cas_id"]:
        raise JobStateError(f"job is {job['status']}; result not sealed",
                            status=job["status"])
    result = json.loads(store.sealed_blob_get(job["result_cas_id"]).decode())
    return {"job_id": job_id, "result": result, "result_sha256": job["result_sha256"]}


def list_jobs(team_id: str | None = None, status: str | None = None,
              *, viewer: str | None = None, roles: tuple[str, ...] = ()) -> list[dict]:
    q = "SELECT id FROM diag_jobs WHERE 1=1"
    args: list = []
    if team_id:
        q += " AND team_id=?"; args.append(team_id)
    if status:
        q += " AND status=?"; args.append(status)
    if viewer is not None and not ({"admin", "operator"} & set(roles)):
        tid = team_for_user(viewer)
        q += " AND team_id=? AND submitted_by=?"
        args.extend([tid, viewer])
    q += " ORDER BY created_at DESC"
    return [get_job(r["id"]) for r in store.all_rows(q, tuple(args))]


# ---------- 用量账本 ----------

def usage_detail(period: str | None = None, team_id: str | None = None) -> dict:
    """用量明细（运维/管理员）：逐条入账记录 + 团队周期汇总。"""
    period = period or _period()
    q = ("SELECT u.period,u.team_id,u.job_id,u.cost,u.billed_at,"
         " j.submitted_by,j.status AS job_status,j.estimated_cost"
         " FROM diag_team_usage u JOIN diag_jobs j ON j.id=u.job_id"
         " WHERE u.period=?")
    args: list = [period]
    if team_id:
        q += " AND u.team_id=?"; args.append(team_id)
    q += " ORDER BY u.billed_at"
    entries = [dict(r) for r in store.all_rows(q, tuple(args))]
    return {"period": period, "entries": entries, "totals": usage_totals(period)}


def usage_totals(period: str | None = None) -> list[dict]:
    period = period or _period()
    rows = store.all_rows(
        "SELECT team_id, COUNT(*) AS jobs, SUM(cost) AS cost"
        " FROM diag_team_usage WHERE period=? GROUP BY team_id ORDER BY team_id",
        (period,))
    return [{"period": period, "team_id": r["team_id"], "jobs": r["jobs"],
             "cost": r["cost"]} for r in rows]


def _team_alias(period: str, team_id: str) -> str:
    """观察者侧的稳定脱敏别名：不暴露团队 id/名称；同周期可核对、跨周期不可链接。"""
    mac = crypto.audit_mac("DIAG-USAGE-ALIAS", f"{period}:{team_id}")
    return "team-" + mac[:12]


def usage_summary_masked(period: str | None = None) -> dict:
    """观察者专用：脱敏用量摘要。只有按周期聚合的数字与稳定别名，
    无团队 id/名称、无作业 id、无提交者。"""
    period = period or _period()
    totals = usage_totals(period)
    rows = [{"team_alias": _team_alias(period, t["team_id"]),
             "jobs": t["jobs"], "cost": t["cost"]} for t in totals]
    rows.sort(key=lambda x: x["team_alias"])
    return {"period": period, "granularity": "team-period aggregate",
            "teams": rows,
            "total_jobs": sum(r["jobs"] for r in rows),
            "total_cost": sum(r["cost"] for r in rows)}
