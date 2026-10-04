"""多租户配额与加权轮转：公平排队、等待上限、令牌冲突、降额/暂停、
租约回收、完成崩溃与计费去重、取消/领取竞态的服务级并发测试。
"""
import threading
import time
import types

import pytest


@pytest.fixture()
def D(home):
    from app.core import store
    from app.services import diagnostics as diag
    return types.SimpleNamespace(store=store, diag=diag)


def _submit(diag, user, token, *, cost=1, wait=3600, **extra):
    body = {"request_token": token, "estimated_cost": cost}
    body.update(extra)
    return diag.submit_job(user, body, wait_seconds=wait)


def _claim_seq(diag, n):
    out = []
    for _ in range(n):
        j = diag.claim("w")
        out.append(j["team_id"].replace("team-", "") if j else None)
    return out


# ---------- 跨团队轮转：持续提交不能长期挤占他人 ----------

def test_cross_team_round_robin_interleaves(D):
    d = D.diag
    for t in ("team-alpha", "team-beta", "team-gamma"):
        d.set_quota(t, 10, "op")
    # 三团队同时持续提交（各 4 个），beta 权重 2
    for i in range(4):
        _submit(d, "alpha", f"a{i}")
        _submit(d, "beta", f"b{i}")
        _submit(d, "gamma", f"g{i}")
    seq = _claim_seq(d, 12)
    assert seq.count("alpha") == 4 and seq.count("gamma") == 4
    # beta 权重 2：在供给充足的前段拿得更密；任何团队都不能连拿超过其权重份额
    assert seq[:4].count("beta") >= 2
    # 关键公平性：前 3 次领取必须覆盖三个不同团队（无团队连续霸占）
    assert len(set(seq[:3])) == 3
    # 每个团队最早作业的等待位次都不会被某团队挤到 6 名之后
    assert min(i for i, t in enumerate(seq) if t == "alpha") <= 2
    assert min(i for i, t in enumerate(seq) if t == "gamma") <= 2


def test_single_team_burst_cannot_starve_others(D):
    d = D.diag
    for t in ("team-alpha", "team-beta", "team-gamma"):
        d.set_quota(t, 10, "op")
    # alpha 突发 20 个在前
    for i in range(20):
        _submit(d, "alpha", f"burst{i}")
    _submit(d, "beta", "b-late")
    _submit(d, "gamma", "g-late")
    seq = _claim_seq(d, 6)
    # beta/gamma 的作业都在头 5 次领取内被服务，尽管 alpha 排队 20 个
    assert "beta" in seq and "gamma" in seq
    assert seq.count("alpha") <= 4


def test_weights_determine_long_run_share(D):
    d = D.diag
    for t in ("team-alpha", "team-beta", "team-gamma"):
        d.set_quota(t, 50, "op")
    d.set_weight("team-alpha", 1, "op")
    d.set_weight("team-beta", 3, "op")
    d.set_weight("team-gamma", 1, "op")
    for i in range(25):
        _submit(d, "alpha", f"a{i}")
        _submit(d, "beta", f"b{i}")
        _submit(d, "gamma", f"g{i}")
    seq = _claim_seq(d, 25)
    from collections import Counter
    c = Counter(seq)
    # 权重 1:3:1 → beta 约占 60%；下界远高于均分
    assert c["beta"] >= 12
    assert c["alpha"] <= 8 and c["gamma"] <= 8


# ---------- 等待上限 ----------

def test_wait_cap_rejects_without_orphan_record(D):
    d = D.diag
    d.set_wait_cap("team-alpha", 2, "op")
    j1 = _submit(d, "alpha", "t1")
    j2 = _submit(d, "alpha", "t2")
    with pytest.raises(d.QuotaError):
        _submit(d, "alpha", "t3")
    # 被拒请求没有产生任何记录；等待队列仍是 2
    waiting = d.list_jobs("team-alpha", "waiting")
    assert {j["id"] for j in waiting} == {j1["id"], j2["id"]}
    # 令牌在拒绝后仍可被正常使用（拒绝没有冻结该令牌）：先取消一个释放等待位
    d.cancel_job(j1["id"], "alpha")
    j3 = _submit(d, "alpha", "t3")
    assert j3["status"] == "waiting"


def test_wait_deadline_expires_and_releases_slot(D):
    d = D.diag
    d.set_wait_cap("team-alpha", 1, "op")
    j = _submit(d, "alpha", "soon", wait=1)  # 1 秒等待上限，稍后到期
    time.sleep(1.02)
    expired = d.sweep_wait_expired("op")
    assert j["id"] in expired
    assert d.get_job(j["id"])["status"] == "wait_expired"
    # 终态作业不再计入等待上限：新作业可以受理
    j2 = _submit(d, "alpha", "next")
    assert j2["status"] == "waiting"
    # wait_expired 不占运行/等待成本
    occ = d.occupancy("team-alpha")
    assert occ["waiting_jobs"] == 1 and occ["waiting_cost"] == 1


def test_claim_sweeps_expired_waiting_jobs(D):
    d = D.diag
    d.set_quota("team-alpha", 10, "op")
    stale = _submit(d, "alpha", "old", wait=1)
    time.sleep(1.02)
    _submit(d, "beta", "fresh")
    claimed = d.claim("w")
    # 超等待上限的 alpha 作业在领取事务内终结，beta 作业被领取
    assert claimed["team_id"] == "team-beta"
    assert d.get_job(stale["id"])["status"] == "wait_expired"


def test_cost_exceeding_quota_rejected_without_record(D):
    d = D.diag
    with pytest.raises(d.QuotaError):
        _submit(d, "beta", "huge", cost=5)  # beta 额度 1
    assert d.list_jobs("team-beta") == []


# ---------- 令牌幂等与冲突 ----------

def test_identical_token_and_body_returns_same_job(D):
    d = D.diag
    body = {"request_token": "tok-x", "estimated_cost": 1,
            "analyses": ["lung"], "note": "n"}
    j1 = d.submit_job("alpha", body)
    j2 = d.submit_job("alpha", dict(body))
    assert j1["id"] == j2["id"] and j2["idempotent_replay"] is True
    assert len(d.list_jobs("team-alpha")) == 1


def test_same_token_changed_body_rejected(D):
    d = D.diag
    j = _submit(d, "alpha", "dup", note="v1")
    with pytest.raises(d.TokenConflict) as ei:
        _submit(d, "alpha", "dup", note="v2-different")
    assert ei.value.existing_job_id == j["id"]
    # 预估成本变化也算内容变化
    with pytest.raises(d.TokenConflict):
        d.submit_job("alpha", {"request_token": "dup", "estimated_cost": 1})
    # 原作业未被改动
    assert d.get_job(j["id"])["estimated_cost"] == 1


def test_same_token_across_teams_rejected(D):
    d = D.diag
    _submit(d, "alpha", "shared-token")
    with pytest.raises(d.TokenConflict):
        _submit(d, "beta", "shared-token")


# ---------- 运行中降额：不抢占，只影响之后领取 ----------

def test_quota_lower_does_not_preempt_running_slots(D):
    d = D.diag
    d.set_quota("team-alpha", 3, "op")
    _submit(d, "alpha", "r1", cost=1)
    _submit(d, "alpha", "r2", cost=1)
    j1 = d.claim("w")
    j2 = d.claim("w")
    assert j1 and j2
    # 运行中把额度降到 1：已占用的 2 个槽不被强制转移
    d.set_quota("team-alpha", 1, "op")
    running = [x for x in d.list_jobs("team-alpha", "running")]
    assert len(running) == 2
    # 之后的领取：alpha 已超额，不可再领
    _submit(d, "alpha", "r3", cost=1)
    got = d.claim("w")
    assert got is None or got["team_id"] != "team-alpha"
    # 升额后新作业立即可被领取（调整只作用于之后）
    d.set_quota("team-alpha", 3, "op")
    got = d.claim("w")
    assert got["team_id"] == "team-alpha"


# ---------- 暂停 / 恢复：保留队列位置 ----------

def test_pause_preserves_queue_position_and_resume_rotates(D):
    d = D.diag
    for t in ("team-alpha", "team-beta", "team-gamma"):
        d.set_quota(t, 10, "op")
    _submit(d, "alpha", "a")
    _submit(d, "beta", "b1")
    _submit(d, "beta", "b2")
    _submit(d, "gamma", "g")
    d.set_paused("team-beta", True, "op")
    seq = _claim_seq(d, 3)
    assert "beta" not in seq  # 暂停期间不被领取
    # 队列位置保留：作业仍是 waiting
    assert len(d.list_jobs("team-beta", "waiting")) == 2
    d.set_paused("team-beta", False, "op")
    after = _claim_seq(d, 3)
    assert "beta" in after
    # 暂停期间可以受理新作业（受理不要求团队 active），恢复后照常轮转
    _submit(d, "beta", "b3")
    assert d.claim("w")["team_id"] == "team-beta"


def test_pause_does_not_accrue_credits_for_burst_on_resume(D):
    d = D.diag
    for t in ("team-alpha", "team-beta", "team-gamma"):
        d.set_quota(t, 20, "op")
    d.set_paused("team-beta", True, "op")
    for i in range(6):
        _submit(d, "alpha", f"a{i}")
        _submit(d, "gamma", f"g{i}")
    _claim_seq(d, 6)  # beta 暂停期间他人持续被服务
    for i in range(6):
        _submit(d, "beta", f"b{i}")
    d.set_paused("team-beta", False, "op")
    seq = _claim_seq(d, 6)
    # 恢复后 beta 不会凭累积信用连拿 6 个；他人仍在轮转中
    assert seq.count("beta") <= 4
    assert "alpha" in seq or "gamma" in seq


# ---------- 租约过期再领取 ----------

def test_expired_lease_reclaimed_and_reruns(D):
    d = D.diag
    j = _submit(d, "alpha", "crash")
    claimed = d.claim("crashed", lease_seconds=0)
    assert claimed["status"] == "running"
    time.sleep(0.02)
    # 执行进程退出：租约到期，作业回到可领取
    again = d.claim("new-worker", lease_seconds=60)
    assert again["id"] == j["id"] and again["lease_owner"] == "new-worker"
    assert again["attempts"] == 2  # 领取次数累加


def test_sealed_result_never_reclaimed_or_recomputed(D):
    d = D.diag
    j = _submit(d, "alpha", "done-job")
    r = d.run_once("w")
    assert r["outcome"] == "done"
    # 即使租约时间已过（直接改库模拟），done+sealed 作业不回收、不能再算
    conn = D.store.get_conn()
    conn.execute("UPDATE diag_jobs SET lease_until='2000-01-01T00:00:00+00:00'"
                 " WHERE id=?", (j["id"],))
    _submit(d, "beta", "other")
    got = d.claim("w")
    assert got["id"] != j["id"]
    assert d.get_job(j["id"])["status"] == "done"
    # 账本只有一条
    totals = {t["team_id"]: t for t in d.usage_totals()}
    assert totals["team-alpha"]["jobs"] == 1


# ---------- 完成后崩溃 / 重复完成通知：计费去重 ----------

def test_duplicate_completion_notification_bills_once(D):
    d = D.diag
    j = _submit(d, "alpha", "double-notify")
    claimed = d.claim("w", lease_seconds=600)
    result = d.execute_job(claimed)
    s1 = d.complete_job(j["id"], "w", result)
    assert s1["already_billed"] is False
    # 重启恢复 / 重复通知：幂等返回，不重复扣减
    s2 = d.complete_job(j["id"], "w", result)
    assert s2["already_billed"] is True and s2["idempotent_replay"] is True
    assert s2["billed_cost"] == s1["billed_cost"]
    rows = D.store.all_rows(
        "SELECT * FROM diag_team_usage WHERE job_id=?", (j["id"],))
    assert len(rows) == 1


def test_failure_retries_never_bill(D):
    d = D.diag
    _submit(d, "alpha", "flaky", fault_inject="execute_error")
    r = None
    for i in range(3):
        r = d.run_once("w")  # execute_error → 前两次 waiting 重试，第三次 failed
        assert r["outcome"] in ("waiting", "failed")
    assert r["outcome"] == "failed"
    assert d.usage_totals() == []  # 从未成功 → 不计费
    # 失败终态释放运行槽：他人可继续
    _submit(d, "beta", "after")
    got = d.claim("w")
    assert got["team_id"] == "team-beta"


def test_completion_after_lease_lost_is_rejected(D):
    d = D.diag
    j = _submit(d, "alpha", "stale-owner")
    c1 = d.claim("w1", lease_seconds=0)
    time.sleep(0.02)
    # w1 崩溃；w2 接管租约
    c2 = d.claim("w2", lease_seconds=600)
    assert c2["id"] == j["id"]
    # w1 醒来提交旧结果：被拒（持有者已变），不能覆盖/入账
    stale_result = d.execute_job(c1)
    with pytest.raises(d.JobStateError):
        d.complete_job(j["id"], "w1", stale_result)


def test_crash_after_compute_before_notify_recomputes_and_bills_once(D):
    """执行进程算出结果、未及通知就退出：无 sealed_at，租约到期回到 waiting，
    新进程重新计算并完成；迟到的旧通知被拒，用量只入账一次。"""
    import sqlite3
    d = D.diag
    j = _submit(d, "alpha", "crash-midway")
    c1 = d.claim("crashed", lease_seconds=0)
    stale_result = d.execute_job(c1)  # 已计算但未封存/通知
    time.sleep(0.02)
    # 新进程看到到期租约并接管
    c2 = d.claim("restarted", lease_seconds=600)
    assert c2["id"] == j["id"] and c2["attempts"] == 2
    # 旧进程迟到通知：拒
    with pytest.raises(d.JobStateError):
        d.complete_job(j["id"], "crashed", stale_result)
    # 新进程重算并完成，计费一次
    sealed = d.complete_job(j["id"], "restarted", d.execute_job(c2))
    assert sealed["already_billed"] is False
    conn = D.store.get_conn()
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO diag_team_usage(period,team_id,job_id,cost,billed_at)"
            " VALUES(?,?,?,?,?)",
            (sealed["period"], "team-alpha", j["id"], 999, "2026-10-01T00:00:00+00:00"))
    totals = d.usage_totals(sealed["period"])
    assert len(totals) == 1 and totals[0]["jobs"] == 1 and totals[0]["cost"] == 1
    assert d.get_job(j["id"])["status"] == "done"


# ---------- 取消 / 领取竞态 ----------

def test_cancel_waiting_releases_reservation(D):
    d = D.diag
    d.set_wait_cap("team-alpha", 1, "op")
    j = _submit(d, "alpha", "c1")
    out = d.cancel_job(j["id"], "alpha")
    assert out["status"] == "cancelled"
    # 立即返还等待预留：可再受理
    j2 = _submit(d, "alpha", "c2")
    assert j2["status"] == "waiting"


def test_cancel_and_claim_race_only_one_advances(D):
    d = D.diag
    d.set_quota("team-alpha", 10, "op")
    n = 8
    jobs = [_submit(d, "alpha", f"race{i}") for i in range(n)]
    outcomes: list[tuple[str, str]] = []
    lock = threading.Lock()
    barrier = threading.Barrier(2 * n)

    def do_cancel(j):
        barrier.wait()
        try:
            d.cancel_job(j["id"], "alpha")
            with lock:
                outcomes.append((j["id"], "cancel"))
        except d.JobStateError:
            with lock:
                outcomes.append((j["id"], "cancel_lost"))

    def do_claim():
        barrier.wait()
        got = d.claim("w-race")
        if got:
            with lock:
                outcomes.append((got["id"], "claim"))

    threads = [threading.Thread(target=do_cancel, args=(j,)) for j in jobs]
    threads += [threading.Thread(target=do_claim) for _ in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    by_job: dict[str, set] = {}
    for jid, kind in outcomes:
        by_job.setdefault(jid, set()).add(kind)
    # 每个作业：取消与领取互斥，绝不同时成功
    for j in jobs:
        assert not ({"cancel", "claim"} <= by_job.get(j["id"], set()))
    kinds = [k for _, k in outcomes]
    # 高并发下两类推进都应出现；任何作业最多被领取一次
    assert "claim" in kinds and "cancel" in kinds
    claimed = [jid for jid, k in outcomes if k == "claim"]
    assert len(claimed) == len(set(claimed))


def test_cannot_cancel_running_job(D):
    d = D.diag
    j = _submit(d, "alpha", "running")
    d.claim("w")
    with pytest.raises(d.JobStateError) as ei:
        d.cancel_job(j["id"], "alpha")
    assert ei.value.status == "running"
    assert d.get_job(j["id"])["status"] == "running"


def test_cancel_other_users_job_is_not_found(D):
    d = D.diag
    j = _submit(d, "alpha", "mine")
    with pytest.raises(d.store.NotFound):
        d.cancel_job(j["id"], "beta")


# ---------- 用量账本 ----------

def test_usage_ledger_accumulates_per_team_and_period(D):
    d = D.diag
    d.set_quota("team-alpha", 10, "op")
    for i in range(3):
        _submit(d, "alpha", f"a{i}", cost=2)
    for _ in range(3):
        d.run_once("w")
    _submit(d, "beta", "b1", cost=1)
    d.run_once("w")
    detail = d.usage_detail()
    totals = {t["team_id"]: t for t in detail["totals"]}
    assert totals["team-alpha"]["jobs"] == 3 and totals["team-alpha"]["cost"] == 6
    assert totals["team-beta"]["jobs"] == 1 and totals["team-beta"]["cost"] == 1
    assert len(detail["entries"]) == 4


def test_masked_usage_summary_has_no_identifiers(D):
    d = D.diag
    _submit(d, "alpha", "a1")
    d.run_once("w")
    s = d.usage_summary_masked()
    assert s["total_jobs"] == 1 and s["total_cost"] == 1
    blob = str(s)
    assert "team-alpha" not in blob and "alpha" not in blob
    assert "djob_" not in blob
    # 别名稳定且可核对
    s2 = d.usage_summary_masked()
    assert s["teams"] == s2["teams"]
    assert all(t["team_alias"].startswith("team-") for t in s["teams"])


# ---------- 角色隔离（服务层身份约束） ----------

def test_team_membership_and_visibility(D):
    d = D.diag
    ja = _submit(d, "alpha", "a-job")
    _submit(d, "beta", "b-job")
    # 分析者只见本人（同团队）作业
    mine = d.list_jobs(viewer="alpha", roles=("analyst",))
    assert [j["id"] for j in mine] == [ja["id"]]
    with pytest.raises(d.store.NotFound):
        d.get_job(_submit(d, "beta", "b2")["id"], viewer="alpha",
                  roles=("analyst",))
    # 运维可见全部
    ops = d.list_jobs(viewer="operator", roles=("operator",))
    assert len(ops) == 3
    # 观察者不接触作业明细
    with pytest.raises((d.DiagError, d.store.NotFound)):
        d.team_for_user("observer")


# ---------- 轮转确定性（重启恢复） ----------

def test_rotation_state_persists_and_is_deterministic(D):
    d = D.diag
    for t in ("team-alpha", "team-beta", "team-gamma"):
        d.set_quota(t, 10, "op")
    for i in range(6):
        _submit(d, "alpha", f"a{i}")
        _submit(d, "beta", f"b{i}")
        _submit(d, "gamma", f"g{i}")
    first = _claim_seq(d, 5)
    # 用 rotation_view 预测下一个团队并实际领取，必须一致
    nxt = d.rotation_view()["next_team_id"]
    got = d.claim("w")
    assert got["team_id"] == nxt
    # 已领取 6，轮换顺序持续确定：再次预测仍准确
    nxt = d.rotation_view()["next_team_id"]
    got = d.claim("w")
    assert got["team_id"] == nxt
    assert first[0] in ("alpha", "beta", "gamma")
