"""多租户诊断调度：公平轮转、配额、租约恢复、账本去重与角色隔离。"""
import base64
import threading
import time
import types

import pytest
from fastapi.testclient import TestClient


@pytest.fixture()
def D(home):
    from app.services import diagnostics
    return diagnostics


@pytest.fixture()
def teams(D):
    alpha = next(t for t in D.list_teams() if t["code"] == "alpha")
    beta = next(t for t in D.list_teams() if t["code"] == "beta")
    # 测试中统一成单成本、小等待队列，轮转语义更直观。
    D.update_team(alpha["id"], "operator", concurrency_limit=2, wait_limit=300, weight=1)
    D.update_team(beta["id"], "operator", concurrency_limit=4, wait_limit=300, weight=2)
    return types.SimpleNamespace(
        alpha=dict(D.get_team(alpha["id"]), members=["analyst-a"]),
        beta=dict(D.get_team(beta["id"]), members=["analyst-b"]))


def _submit(D, team, seq, user=None, cost=1):
    return D.submit_job(
        team["id"], user or team["members"][0],
        {"seq": seq, "kind": "ct"}, request_token=f"tok-{team['code']}-{seq}",
        estimated_cost=cost)


def _ids(jobs):
    return [j["id"] for j in jobs]


def test_cross_team_weighted_round_robin_and_single_team_burst(D, teams):
    a_jobs = [_submit(D, teams.alpha, i) for i in range(3)]
    b_jobs = [_submit(D, teams.beta, i) for i in range(3)]

    order = []
    for _ in range(5):
        job = D.claim("w")
        assert job is not None
        order.append((job["team_id"], job["id"]))

    # 第一轮：alpha 1 次、beta 2 次；alpha 下一轮仍能插入，不被 beta 突发饿死。
    assert [team for team, _ in order] == [
        teams.alpha["id"], teams.beta["id"], teams.beta["id"],
        teams.alpha["id"], teams.beta["id"]]
    assert order[0][1] == a_jobs[0]["id"]
    assert [jid for _, jid in order[1:3]] == _ids(b_jobs[:2])
    assert order[3][1] == a_jobs[1]["id"]


def test_waiting_limit_admission_leaves_no_orphan(D, teams):
    for i in range(3):
        _submit(D, teams.alpha, i)
    with pytest.raises(D.DiagnosticError):
        _submit(D, teams.alpha, 99)
    conn = D.store.get_conn()
    assert conn.execute(
        "SELECT COUNT(*) FROM diagnostic_jobs WHERE team_id=?",
        (teams.alpha["id"],)).fetchone()[0] == 3
    # 三条 reserve；被拒绝的第四请求没有作业或账本。
    assert conn.execute(
        "SELECT COUNT(*) FROM diagnostic_ledger WHERE team_id=? AND event_type='reserve'",
        (teams.alpha["id"],)).fetchone()[0] == 3


def test_waiting_capacity_limit_rejects_admission_without_orphan(D, teams):
    # 等待上限按等待槽位解释：并发额度 1、等待上限 1 时，只允许 1 个作业排队。
    limited = D.create_team("Limited", "limited", concurrency_limit=1, wait_limit=1,
                            weight=1, members=["analyst-a"], actor="operator",
                            team_id="team_limited")
    D.submit_job(limited["id"], "analyst-a", {"i": 0},
                 request_token="lim-0", estimated_cost=1)
    with pytest.raises(D.DiagnosticError):
        D.submit_job(limited["id"], "analyst-a", {"i": 9},
                     request_token="lim-over", estimated_cost=1)
    conn = D.store.get_conn()
    assert conn.execute(
        "SELECT COUNT(*) FROM diagnostic_jobs WHERE team_id=?",
        (limited["id"],)).fetchone()[0] == 1
    assert conn.execute(
        "SELECT COUNT(*) FROM diagnostic_ledger WHERE team_id=? AND event_type='reserve'",
        (limited["id"],)).fetchone()[0] == 1


def test_request_token_same_body_replays_and_changed_body_rejected(D, teams):
    first = D.submit_job(teams.alpha["id"], "analyst-a", {"x": 1},
                         request_token="same", estimated_cost=1)
    replay = D.submit_job(teams.alpha["id"], "analyst-a", {"x": 1},
                          request_token="same", estimated_cost=1)
    assert replay["id"] == first["id"] and replay["idempotent_replay"] is True
    with pytest.raises(D.DiagnosticError):
        D.submit_job(teams.alpha["id"], "analyst-a", {"x": 2},
                     request_token="same", estimated_cost=1)


def test_quota_reduction_only_affects_later_claims(D, teams):
    running = _submit(D, teams.alpha, 1)
    queued = _submit(D, teams.alpha, 2, cost=2)
    assert D.claim("w")["id"] == running["id"]
    D.update_team(teams.alpha["id"], "operator", concurrency_limit=1)
    # 已占用槽位不迁移；成本 2 的等待作业按新额度无法领取。
    assert D.claim("w") is None
    assert D.get_job(queued["id"])["status"] == "queued"
    # 释放运行槽后，仍因预计成本大于新额度等待，而不会被强行启动。
    D.record_result(running["id"], "w", {"ok": True})
    D.complete_job(running["id"], "w")
    assert D.claim("w") is None
    assert D.get_job(queued["id"])["status"] == "queued"


def test_pause_preserves_queue_position_then_resumes(D, teams):
    a1, a2 = _submit(D, teams.alpha, 1), _submit(D, teams.alpha, 2)
    b1 = _submit(D, teams.beta, 1)
    D.update_team(teams.alpha["id"], "operator", paused=True)
    assert D.claim("w")["id"] == b1["id"]
    assert D.claim("w") is None
    D.update_team(teams.alpha["id"], "operator", paused=False)
    assert D.claim("w")["id"] == a1["id"]
    assert D.claim("w")["id"] == a2["id"]


def test_lease_expired_reclaim_and_sealed_result_not_recomputed(D, teams):
    job = _submit(D, teams.alpha, 1)
    claimed = D.claim("crashed", lease_seconds=0)
    assert claimed["id"] == job["id"]
    time.sleep(0.01)
    D.record_result(job["id"], "crashed", {"ok": True}, actual_cost=3)
    # 模拟进程在 complete 前退出；下一次领取直接封存结算，不再执行正文。
    recovered = D.claim("restart", lease_seconds=60)
    assert recovered is None
    fresh = D.get_job(job["id"])
    assert fresh["status"] == "done" and fresh["actual_cost"] == 3
    events = D.store.all_rows(
        "SELECT event_type,amount FROM diagnostic_ledger WHERE job_id=?", (job["id"],))
    counts = {}
    for row in events:
        counts[row["event_type"]] = counts.get(row["event_type"], 0) + 1
    assert counts == {"reserve": 1, "reserve_release": 1, "usage": 1}
    result = D.get_result(job["id"])
    assert result["actual_cost"] == 3


def test_unsealed_expired_lease_returns_to_queue_for_reclaim(D, teams):
    job = _submit(D, teams.alpha, 1)
    assert D.claim("crashed", lease_seconds=0)["id"] == job["id"]
    time.sleep(0.01)
    assert D.claim("new", lease_seconds=60)["id"] == job["id"]
    assert D.get_job(job["id"])["attempts"] == 2


def test_cancel_claim_race_only_one_transition(D, teams):
    job = _submit(D, teams.alpha, 1)
    outcomes = []
    barrier = threading.Barrier(2)

    def cancel():
        barrier.wait()
        try:
            outcomes.append(("cancel", D.cancel_job(job["id"], "analyst-a")["status"]))
        except Exception as exc:
            outcomes.append(("cancel_err", str(exc)))

    def claim_job():
        barrier.wait()
        claimed = D.claim("w")
        outcomes.append(("claim", claimed["id"] if claimed else None))

    threads = [threading.Thread(target=cancel), threading.Thread(target=claim_job)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    status = D.get_job(job["id"])["status"]
    assert status in ("cancelled", "running")
    if status == "cancelled":
        assert ("claim", None) in outcomes
        ledger = D.store.all_rows(
            "SELECT event_type, amount FROM diagnostic_ledger WHERE job_id=?", (job["id"],))
        assert {r["event_type"] for r in ledger} == {"reserve", "reserve_release"}
    else:
        assert any(kind == "cancel_err" for kind, _ in outcomes)
        assert D.store.all_rows(
            "SELECT event_type FROM diagnostic_ledger WHERE job_id=? AND event_type='reserve_release'",
            (job["id"],)) == []


def test_retry_completion_duplicates_bill_once(D, teams):
    job = _submit(D, teams.alpha, 1)
    claimed = D.claim("w")
    D.fail_job(job["id"], "w", "transient")
    assert D.get_job(job["id"])["status"] == "retry"

    # 跳过指数退避窗口后重试并封存。
    from datetime import timedelta
    later = D._now() + timedelta(seconds=60)
    claimed2 = D.claim("w", now_override=later)
    assert claimed2["id"] == job["id"] and claimed2["attempts"] == 2
    D.record_result(job["id"], "w", {"ok": True})
    D.complete_job(job["id"], "w")
    replay = D.complete_job(job["id"], "w")
    assert replay["idempotent_replay"] is True
    D.fail_job(job["id"], "w", "late duplicate failure")

    rows = D.store.all_rows(
        "SELECT event_type,amount FROM diagnostic_ledger WHERE job_id=? ORDER BY seq",
        (job["id"],))
    assert [(r["event_type"], r["amount"]) for r in rows] == [
        ("reserve", 1), ("reserve_release", -1), ("usage", 1)]


@pytest.fixture()
def client(home):
    from app.web.api import app
    with TestClient(app) as c:
        yield c


def _basic(user, pw):
    return {"Authorization": "Basic " + base64.b64encode(f"{user}:{pw}".encode()).decode()}


CRED = {
    "operator": _basic("operator", "operator-pw-123"),
    "a": _basic("analyst-a", "analyst-a-pw-123"),
    "b": _basic("analyst-b", "analyst-b-pw-123"),
    "observer": _basic("observer", "observer-pw-123"),
    "tech": _basic("tech", "tech-pw-123"),
}


def _team_id(client, code):
    return next(t["id"] for t in client.get(
        "/api/diagnostic/teams", headers=CRED["operator"]).json() if t["code"] == code)


def _api_submit(client, cred, team_id, token, body=None, cost=1):
    return client.post("/api/diagnostic/jobs", headers=cred, json={
        "team_id": team_id, "request_token": token,
        "estimated_cost": cost, "body": body or {"k": token}})


def test_http_role_isolation_and_sanitized_observer_summary(client):
    alpha = _team_id(client, "alpha")
    beta = _team_id(client, "beta")
    assert _api_submit(client, CRED["a"], alpha, "a1").status_code == 200
    assert _api_submit(client, CRED["b"], beta, "b1").status_code == 200

    # analyst-a 不能向 beta 提交，也不能看 beta；技术角色不能访问诊断接口。
    assert _api_submit(client, CRED["a"], beta, "cross").status_code == 403
    assert client.get(f"/api/diagnostic/jobs?team_id={beta}",
                      headers=CRED["a"]).status_code == 403
    assert client.get("/api/diagnostic/teams", headers=CRED["tech"]).status_code == 403

    teams_a = client.get("/api/diagnostic/teams", headers=CRED["a"]).json()
    assert {t["code"] for t in teams_a} == {"alpha"}
    jobs_a = client.get("/api/diagnostic/jobs", headers=CRED["a"]).json()
    assert len(jobs_a) == 1 and jobs_a[0]["request_token"] == "a1"
    assert all(j["body"] is None for j in jobs_a)

    # 观察者只见团队代码/周期聚合，不含成员、作业、令牌或正文。
    summary = client.get("/api/diagnostic/usage-summary", headers=CRED["observer"])
    assert summary.status_code == 200
    item = next(x for x in summary.json() if x["team_code"] == "alpha")
    assert set(item) == {"team_code", "periods"}
    assert client.get("/api/diagnostic/jobs", headers=CRED["observer"]).status_code == 403


def test_http_operator_adjusts_pauses_and_runs_jobs(client):
    alpha = _team_id(client, "alpha")
    beta = _team_id(client, "beta")
    for i in range(2):
        assert _api_submit(client, CRED["a"], alpha, f"a{i}").status_code == 200
    for i in range(3):
        assert _api_submit(client, CRED["b"], beta, f"b{i}").status_code == 200

    pause = client.post(f"/api/diagnostic/teams/{alpha}",
                        headers=CRED["operator"], json={"paused": True})
    assert pause.status_code == 200 and pause.json()["paused"] == 1
    run = client.post("/api/diagnostic/scheduler/run",
                      headers=CRED["operator"], json={"cycles": 3})
    assert run.status_code == 200
    teams = {j["team_id"]: j for j in run.json()["processed"]}
    assert set(teams) == {beta}

    resume = client.post(f"/api/diagnostic/teams/{alpha}",
                         headers=CRED["operator"], json={"paused": False, "weight": 3})
    assert resume.status_code == 200 and resume.json()["weight"] == 3
    usage = client.get("/api/diagnostic/usage", headers=CRED["operator"]).json()
    beta_usage = next(x for x in usage if x["team_code"] == "beta")
    assert sum(p["usage_cost"] for p in beta_usage["periods"]) == 3
    assert all({"reserve", "reserve_release", "usage"}
               >= {e["event_type"] for e in beta_usage["events"]})
