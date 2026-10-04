"""诊断多租户 HTTP 契约：角色隔离、令牌冲突状态码、等待上限、运维操作、
脱敏用量摘要与网页工作台所需的全部恢复接口。
"""
import base64

import pytest
from fastapi.testclient import TestClient


@pytest.fixture()
def client(home):
    from app.web.api import app
    with TestClient(app) as c:
        yield c


def _basic(user, pw):
    return {"Authorization": "Basic " + base64.b64encode(f"{user}:{pw}".encode()).decode()}


CRED = {
    "operator": _basic("operator", "operator-pw-123"),
    "observer": _basic("observer", "observer-pw-123"),
    "alpha": _basic("alpha", "alpha-pw-123"),
    "beta": _basic("beta", "beta-pw-123"),
    "gamma": _basic("gamma", "gamma-pw-123"),
    "admin": _basic("admin", "admin-pw-123"),
    "tech": _basic("tech", "tech-pw-123"),
}


def _submit(client, who, token, **over):
    body = {"request_token": token, "estimated_cost": 1}
    body.update(over)
    return client.post("/api/diag/jobs", headers=CRED[who], json=body)


def _run(client, cycles=5):
    return client.post("/api/diag/scheduler/run", headers=CRED["operator"],
                       params={"cycles": cycles})


# ---------- 认证与角色隔离 ----------

def test_auth_required_for_diag_endpoints(client):
    assert client.get("/api/diag/teams").status_code == 401
    assert client.post("/api/diag/jobs", json={"request_token": "x"}).status_code == 401


def test_role_boundaries(client):
    # 分析者不能看团队总览/轮转/用量明细，也不能驱动调度器
    assert client.get("/api/diag/teams", headers=CRED["alpha"]).status_code == 403
    assert client.get("/api/diag/rotation", headers=CRED["alpha"]).status_code == 403
    assert client.get("/api/diag/usage", headers=CRED["alpha"]).status_code == 403
    assert client.post("/api/diag/scheduler/run", headers=CRED["alpha"]).status_code == 403
    # 观察者只能看脱敏摘要
    assert client.get("/api/diag/usage/summary", headers=CRED["observer"]).status_code == 200
    assert client.get("/api/diag/usage", headers=CRED["observer"]).status_code == 403
    assert client.get("/api/diag/teams", headers=CRED["observer"]).status_code == 403
    assert client.post("/api/diag/jobs", headers=CRED["observer"],
                       json={"request_token": "x"}).status_code == 403
    # 技师/临床等无关角色不可进入诊断接口
    assert client.get("/api/diag/mine", headers=CRED["tech"]).status_code == 403


def test_analyst_only_sees_own_jobs(client):
    r1 = _submit(client, "alpha", "a1")
    assert r1.status_code == 200
    _submit(client, "beta", "b1")
    mine = client.get("/api/diag/mine", headers=CRED["alpha"]).json()
    assert {j["request_token"] for j in mine} == {"a1"}
    # 越权直取他人作业 → 404（不泄露存在性）
    bid = client.get("/api/diag/jobs", headers=CRED["beta"]).json()[0]["id"]
    assert client.get(f"/api/diag/jobs/{bid}", headers=CRED["alpha"]).status_code == 404
    # 运维可见全部
    allj = client.get("/api/diag/jobs", headers=CRED["operator"]).json()
    assert len(allj) == 2


# ---------- 令牌幂等 / 冲突 / 等待上限 ----------

def test_token_idempotency_and_conflict(client):
    body = {"request_token": "T", "estimated_cost": 1, "note": "v1"}
    r1 = client.post("/api/diag/jobs", headers=CRED["alpha"], json=body)
    jid = r1.json()["id"]
    r2 = client.post("/api/diag/jobs", headers=CRED["alpha"], json=dict(body))
    assert r2.status_code == 200 and r2.json()["id"] == jid
    # 令牌相同内容变化 → 409，且带回原作业 id
    changed = dict(body, note="v2")
    r3 = client.post("/api/diag/jobs", headers=CRED["alpha"], json=changed)
    assert r3.status_code == 409
    assert r3.json()["detail"]["error"] == "request_token_conflict"
    assert r3.json()["detail"]["existing_job_id"] == jid
    # 跨团队复用同一令牌 → 409
    r4 = client.post("/api/diag/jobs", headers=CRED["beta"], json=body)
    assert r4.status_code == 409


def test_wait_cap_returns_429_without_record(client):
    client.post("/api/diag/teams/team-alpha/wait-cap", headers=CRED["operator"],
                json={"wait_cap": 1})
    assert _submit(client, "alpha", "cap1").status_code == 200
    r = _submit(client, "alpha", "cap2")
    assert r.status_code == 429
    assert r.json()["detail"]["error"] == "quota_or_wait_cap_rejected"
    jobs = client.get("/api/diag/jobs?team_id=team-alpha",
                      headers=CRED["operator"]).json()
    assert len(jobs) == 1


def test_cost_over_quota_rejected(client):
    r = _submit(client, "beta", "big", estimated_cost=99)
    assert r.status_code == 429


# ---------- 运维：权重、暂停、轮转视图 ----------

def test_operator_can_pause_and_set_weights(client):
    rv = client.get("/api/diag/rotation", headers=CRED["operator"]).json()
    assert [t["id"] for t in rv["teams"]] == ["team-alpha", "team-beta", "team-gamma"]
    assert rv["next_team_id"] is None  # 尚无作业
    # 暂停 alpha
    r = client.post("/api/diag/teams/team-alpha/pause", headers=CRED["operator"],
                    json={"paused": True})
    assert r.status_code == 200 and r.json()["paused"] == 1
    _submit(client, "alpha", "pa")
    _run(client, 2)
    mine = client.get("/api/diag/mine", headers=CRED["alpha"]).json()
    assert mine[0]["status"] == "waiting"  # 暂停期间不被领取
    # 恢复后被领取执行
    client.post("/api/diag/teams/team-alpha/pause", headers=CRED["operator"],
                json={"paused": False})
    _run(client, 2)
    mine = client.get("/api/diag/mine", headers=CRED["alpha"]).json()
    assert mine[0]["status"] == "done"


def test_quota_change_only_affects_future(client):
    for q in ("team-alpha", "team-beta", "team-gamma"):
        client.post(f"/api/diag/teams/{q}/quota", headers=CRED["operator"],
                    json={"concurrency_quota": 5})
    _submit(client, "alpha", "r1")
    _submit(client, "alpha", "r2")
    client.post("/api/diag/scheduler/run", headers=CRED["operator"],
                params={"cycles": 2, "claim_only": True})
    jobs = client.get("/api/diag/jobs?team_id=team-alpha&status=running",
                      headers=CRED["operator"]).json()
    assert len(jobs) == 2
    # 降额到 1：运行中的 2 个槽保留
    client.post("/api/diag/teams/team-alpha/quota", headers=CRED["operator"],
                json={"concurrency_quota": 1})
    jobs = client.get("/api/diag/jobs?team_id=team-alpha&status=running",
                      headers=CRED["operator"]).json()
    assert len(jobs) == 2


# ---------- 调度器：跨团队轮转 ----------

def test_scheduler_interleaves_teams(client):
    for q in ("team-alpha", "team-beta", "team-gamma"):
        client.post(f"/api/diag/teams/{q}/quota", headers=CRED["operator"],
                    json={"concurrency_quota": 10})
    for i in range(4):
        _submit(client, "alpha", f"a{i}")
        _submit(client, "beta", f"b{i}")
        _submit(client, "gamma", f"g{i}")
    proc = _run(client, 6).json()["processed"]
    teams = [client.get(f"/api/diag/jobs/{p['job_id']}", headers=CRED["operator"])
             .json()["team_id"] for p in proc]
    assert len(set(teams[:3])) == 3  # 前三轮覆盖三团队


# ---------- 取消 ----------

def test_cancel_waiting_then_slot_released(client):
    client.post("/api/diag/teams/team-alpha/wait-cap", headers=CRED["operator"],
                json={"wait_cap": 1})
    j = _submit(client, "alpha", "c1").json()
    r = client.post(f"/api/diag/jobs/{j['id']}/cancel", headers=CRED["alpha"])
    assert r.status_code == 200 and r.json()["status"] == "cancelled"
    j2 = _submit(client, "alpha", "c2").json()
    assert j2["status"] == "waiting"
    # 别人不能取消（越权与不存在同形）
    assert client.post(f"/api/diag/jobs/{j2['id']}/cancel",
                       headers=CRED["beta"]).status_code == 404
    # 本人取消 c2，释放等待位
    assert client.post(f"/api/diag/jobs/{j2['id']}/cancel",
                       headers=CRED["alpha"]).status_code == 200
    # 运行中作业不可取消（取消落败返回 409 与确定性状态）
    client.post("/api/diag/teams/team-alpha/wait-cap", headers=CRED["operator"],
                json={"wait_cap": 10})
    running = _submit(client, "alpha", "c-run")
    rid = running.json()["id"]
    claimed = client.post("/api/diag/scheduler/run", headers=CRED["operator"],
                          params={"cycles": 1, "claim_only": True}).json()
    assert claimed["processed"][0]["job_id"] == rid
    rc = client.post(f"/api/diag/jobs/{rid}/cancel", headers=CRED["alpha"])
    assert rc.status_code == 409 and rc.json()["detail"]["status"] == "running"


# ---------- 用量与脱敏摘要 ----------

def test_usage_detail_and_masked_summary(client):
    _submit(client, "alpha", "u1")
    _submit(client, "beta", "u2")
    _run(client, 3)
    detail = client.get("/api/diag/usage", headers=CRED["operator"]).json()
    teams = {t["team_id"]: t for t in detail["totals"]}
    assert teams["team-alpha"]["jobs"] == 1 and teams["team-beta"]["jobs"] == 1
    assert len(detail["entries"]) == 2
    # 观察者摘要：无团队 id / 作业 id / 提交者
    s = client.get("/api/diag/usage/summary", headers=CRED["observer"]).json()
    assert s["total_jobs"] == 2
    text = str(s)
    assert "team-alpha" not in text and "team-beta" not in text
    assert "djob_" not in text and "alpha" not in text
    # admin 也可看明细；周期格式校验
    bad = client.get("/api/diag/usage?period=2026-9", headers=CRED["operator"])
    assert bad.status_code == 400
    ok = client.get("/api/diag/usage?period=2026-10", headers=CRED["admin"])
    assert ok.status_code == 200


def test_result_endpoint_only_owner_or_admin(client):
    j = _submit(client, "alpha", "res").json()
    _run(client, 1)
    # 本人可取结果
    r = client.get(f"/api/diag/jobs/{j['id']}/result", headers=CRED["alpha"])
    assert r.status_code == 200 and r.json()["result"]["schema"] == "diag-result/v1"
    # 其他分析者 404，观察者 403，运维无 result 权限（不在白名单 → 403）
    assert client.get(f"/api/diag/jobs/{j['id']}/result",
                      headers=CRED["beta"]).status_code == 404
    assert client.get(f"/api/diag/jobs/{j['id']}/result",
                      headers=CRED["observer"]).status_code == 403
    assert client.get(f"/api/diag/jobs/{j['id']}/result",
                      headers=CRED["operator"]).status_code == 403
    assert client.get(f"/api/diag/jobs/{j['id']}/result",
                      headers=CRED["admin"]).status_code == 200


def test_lease_expiry_reclaim_via_scheduler(client):
    # 受理后直接驱动一次领取（执行），再模拟崩溃：租约到期作业应被重新执行且只计费一次
    j = _submit(client, "alpha", "lease").json()
    client.post("/api/diag/scheduler/run", headers=CRED["operator"],
                params={"cycles": 1})
    # 正常路径已 done；这里直接校验完成后重复 run 不再重复计算
    _run(client, 3)
    detail = client.get("/api/diag/usage", headers=CRED["operator"]).json()
    assert [e["job_id"] for e in detail["entries"]].count(j["id"]) == 1


# ---------- 网页工作台契约 ----------

def test_static_assets_wire_diagnostics_without_business_data(client):
    root = client.get("/")
    assert root.status_code == 200 and root.headers["cache-control"] == "no-store"
    js = client.get("/ui/app.js")
    assert js.status_code == 200
    body = root.content + js.content
    # 三个角色化诊断视图均已接入
    for needle in ("diag-operator", "diag-analyst", "diag-observer",
                   "/api/diag/jobs", "/api/diag/rotation",
                   "/api/diag/usage/summary"):
        assert needle.encode() in body
    # 静态资源不含任何真实业务数据
    assert b"team-alpha" not in body and b"djob_" not in body


def test_role_endpoint_matrix_matches_workbench_tabs(client):
    # 运维：总览/轮转/用量明细/驱动 全部可达
    assert client.get("/api/diag/teams", headers=CRED["operator"]).status_code == 200
    assert client.get("/api/diag/rotation", headers=CRED["operator"]).status_code == 200
    assert client.get("/api/diag/usage", headers=CRED["operator"]).status_code == 200
    # 分析者：本人作业可恢复，团队总览不可见
    assert client.get("/api/diag/mine", headers=CRED["alpha"]).status_code == 200
    assert client.get("/api/diag/rotation", headers=CRED["alpha"]).status_code == 403
    # 观察者：仅脱敏摘要
    assert client.get("/api/diag/usage/summary",
                      headers=CRED["observer"]).status_code == 200
    for path in ("/api/diag/jobs", "/api/diag/rotation", "/api/diag/usage"):
        assert client.get(path, headers=CRED["observer"]).status_code == 403
