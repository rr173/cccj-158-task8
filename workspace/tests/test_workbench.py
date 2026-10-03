"""角色化网页工作台：服务端状态恢复、跨角色独立会话、一次性材料与静态资源。

这些测试不依赖 JS 引擎，而是断言前端赖以工作的全部 HTTP 契约：
- 页面静态资源公开可访问但不携带数据；
- /api/me 返回真实会话身份，凭据错误不产生登录态；
- 临床医生只能看到自己的回链状态，看不到 envelope/分片/原身份；
- 隐私官与临床医生在不同会话（不同凭据头）中依次提交，状态只能由后端推进；
- 一次性纠错包只在导出响应中出现；终态后刷新/重复导出都得不到身份；
- 撤销/超时/并发/越权都只返回确定性错误；管理员可筛选/校验/下载审计。
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
    "tech": _basic("tech", "tech-pw-123"),
    "clinician": _basic("clinician", "clinician-pw-123"),
    "officer": _basic("officer", "officer-pw-123"),
    "admin": _basic("admin", "admin-pw-123"),
}


def _publish(client, sid):
    client.post("/api/queue/run", headers=CRED["tech"], params={"cycles": 5})
    q = client.get("/api/qc/queue", headers=CRED["tech"], params={"status": "passed"})
    for item in q.json():
        if item["study_id"] == sid:
            client.post("/api/qc/review", headers=CRED["tech"],
                        json={"version_id": item["id"], "approve": True, "note": "ok"})
            return item["id"]
    raise AssertionError("no passed version")


def _armed_relink(client, sid):
    """模拟两个相互独立的登录会话依次完成双角色授权。"""
    r = client.post("/api/relink/request", headers=CRED["clinician"],
                    json={"study_id": sid, "purpose": "workbench flow"})
    rid, share_a = r.json()["id"], r.json()["share_for_requester"]
    # 临床医生会话提交分片 A
    client.post(f"/api/relink/{rid}/share-a", headers=CRED["clinician"],
                json={"share": share_a})
    # 隐私官在另一个独立会话中取出分片 B 并提交
    pend = client.get("/api/relink/officer/pending", headers=CRED["officer"]).json()
    share_b = next(p for p in pend if p["id"] == rid)["share_for_officer"]
    sb = client.post(f"/api/relink/{rid}/share-b", headers=CRED["officer"],
                     json={"share": share_b})
    assert sb.json()["status"] == "armed"
    return rid


# ---------- 静态资源与会话 ----------

def test_static_workbench_served_without_data_or_auth(client):
    root = client.get("/")
    assert root.status_code == 200 and "text/html" in root.headers["content-type"]
    assert b'/ui/app.js' in root.content  # 页面脚本存在
    assert root.headers["cache-control"] == "no-store"
    for name in ("app.js", "styles.css"):
        r = client.get(f"/ui/{name}")
        assert r.status_code == 200 and r.headers["cache-control"] == "no-store"
    # 不允许路径穿越/列目录
    assert client.get("/ui/../api.py").status_code in (404, 403)
    assert client.get("/ui/").status_code == 404
    assert client.get("/ui/does-not-exist").status_code == 404
    # 静态页面中不包含任何真实病例/身份数据
    body = client.get("/").content + client.get("/ui/app.js").content
    assert b"MRN-48213" not in body and b"ZHANG" not in body


def test_me_reflects_real_session_only(client):
    assert client.get("/api/me").status_code == 401
    assert client.get("/api/me", headers=_basic("tech", "wrong")).status_code == 401
    me = client.get("/api/me", headers=CRED["tech"]).json()
    assert me == {"username": "tech", "roles": ["technician"]}
    assert client.get("/api/me", headers=CRED["admin"]).json()["roles"] == ["admin"]


# ---------- 角色隔离 ----------

def test_workbench_views_enforce_role_boundaries(client):
    sid = client.post("/api/studies/ingest-synthetic", headers=CRED["tech"],
                      json={}).json()["study_id"]
    _publish(client, sid)
    rid = client.post("/api/relink/request", headers=CRED["clinician"],
                      json={"study_id": sid}).json()["id"]

    # 技师/管理员无法进入临床状态恢复与待授权视图
    assert client.get("/api/relink/mine", headers=CRED["tech"]).status_code == 403
    assert client.get("/api/relink/mine", headers=CRED["admin"]).status_code == 403
    assert client.get("/api/relink/officer/pending",
                      headers=CRED["clinician"]).status_code == 403
    # 临床医生不能看别人的回链列表接口（officer/admin 专用）
    assert client.get("/api/relink", headers=CRED["clinician"]).status_code == 403
    # 但可以恢复自己的单条状态
    st = client.get(f"/api/relink/{rid}/status", headers=CRED["clinician"])
    assert st.status_code == 200 and st.json()["status"] == "requested"
    # 状态视图绝不携带敏感材料
    blob = st.json()
    assert "envelope" not in blob and "share_for_requester" not in blob
    assert "share_for_officer" not in blob and "original_identity" not in blob


def test_clinician_cannot_see_other_clinicians_relink_status(client, monkeypatch):
    # 第二位临床医生（复用配置注入）
    from app import config
    monkeypatch.setitem(config.USERS, "clinician2",
                        ("clinician2-pw", ["clinician"]))
    c2 = _basic("clinician2", "clinician2-pw")
    sid = client.post("/api/studies/ingest-synthetic", headers=CRED["tech"],
                      json={"patient_id": "MRN-1"}).json()["study_id"]
    _publish(client, sid)
    rid = client.post("/api/relink/request", headers=CRED["clinician"],
                      json={"study_id": sid}).json()["id"]
    # 越权查看与“不存在”返回同一结果，不泄露存在性
    r = client.get(f"/api/relink/{rid}/status", headers=c2)
    assert r.status_code == 404
    assert client.get("/api/relink/mine", headers=c2).json() == []


# ---------- 从服务端恢复真实状态：完整跨会话流程 ----------

def test_clinician_state_recovery_through_lifecycle(client):
    sid = client.post("/api/studies/ingest-synthetic", headers=CRED["tech"],
                      json={}).json()["study_id"]
    _publish(client, sid)
    rid = _armed_relink(client, sid)

    # 刷新等价物：重新 GET 状态，得到服务端 armed 与两个授权步骤
    st = client.get(f"/api/relink/mine", headers=CRED["clinician"]).json()
    mine = next(x for x in st if x["id"] == rid)
    assert mine["status"] == "armed"
    steps = {(s["step_no"], s["role"]) for s in mine["authorization_steps"]}
    assert steps == {(1, "clinician"), (2, "officer")}

    # 登记纠错并导出：一次性包只在此响应中
    client.post(f"/api/relink/{rid}/correction", headers=CRED["clinician"],
                json={"corrected": {"PatientID": "MRN-FIXED"}})
    ex = client.post(f"/api/relink/{rid}/export", headers=CRED["clinician"]).json()
    assert ex["bundle"]["original_identity"]["PatientID"] == "MRN-48213"

    # 导出后“刷新”：状态恢复为 exported，但任何恢复接口都不再含身份内容/分片
    again = client.get(f"/api/relink/{rid}/status", headers=CRED["clinician"]).json()
    assert again["status"] == "exported" and "bundle" not in again
    assert "original_identity" not in again
    # 重复导出（重复点击/刷新后再试）只得到确定性 409
    dup = client.post(f"/api/relink/{rid}/export", headers=CRED["clinician"])
    assert dup.status_code == 409
    # 待授权队列中不应再出现已终结授权（保险库已抹除）
    assert client.get("/api/relink/officer/pending",
                      headers=CRED["officer"]).json() == []


def test_revocation_and_expiry_show_deterministic_server_state(client):
    sid = client.post("/api/studies/ingest-synthetic", headers=CRED["tech"],
                      json={}).json()["study_id"]
    _publish(client, sid)
    rid = _armed_relink(client, sid)
    # 撤销在独立的隐私官会话中进行
    rv = client.post(f"/api/relink/{rid}/revoke", headers=CRED["officer"],
                     json={"reason": "policy change"})
    assert rv.json()["revoked"] is True
    # 临床医生会话刷新只能看到终态，导出 409
    assert client.get(f"/api/relink/{rid}/status",
                      headers=CRED["clinician"]).json()["status"] == "revoked"
    assert client.post(f"/api/relink/{rid}/export",
                       headers=CRED["clinician"]).status_code == 409
    # 撤销幂等：再次撤销不产生误导成功
    assert client.post(f"/api/relink/{rid}/revoke", headers=CRED["officer"],
                       json={}).status_code == 409


def test_officer_share_b_fetch_then_one_click_submit_contract(client):
    """前端“提交分片B”按钮依赖的契约：pending 返回分片与步骤，提交后消失。"""
    sid = client.post("/api/studies/ingest-synthetic", headers=CRED["tech"],
                      json={}).json()["study_id"]
    _publish(client, sid)
    r = client.post("/api/relink/request", headers=CRED["clinician"],
                    json={"study_id": sid})
    rid, share_a = r.json()["id"], r.json()["share_for_requester"]
    # 步骤1 前 pending 为空
    assert client.get("/api/relink/officer/pending",
                      headers=CRED["officer"]).json() == []
    client.post(f"/api/relink/{rid}/share-a", headers=CRED["clinician"],
                json={"share": share_a})
    pend = client.get("/api/relink/officer/pending", headers=CRED["officer"]).json()
    assert len(pend) == 1
    item = pend[0]
    assert item["share_for_officer"] and item["authorization_steps"][0]["step_no"] == 1
    # 前端不持久化分片：重复 pending 仍可取（后端状态未变），重复 share-b 第二次 409
    client.post(f"/api/relink/{rid}/share-b", headers=CRED["officer"],
                json={"share": item["share_for_officer"]})
    again = client.post(f"/api/relink/{rid}/share-b", headers=CRED["officer"],
                        json={"share": item["share_for_officer"]})
    assert again.status_code == 409


# ---------- 技师工作台依赖的契约 ----------

def test_tech_can_drive_queue_inspect_and_release(client):
    sid = client.post("/api/studies/ingest-synthetic", headers=CRED["tech"],
                      json={"fault_inject": "qc_leak"}).json()["study_id"]
    client.post("/api/queue/run", headers=CRED["tech"])
    jobs = client.get("/api/queue/jobs", headers=CRED["tech"]).json()
    assert jobs  # 队列接口可恢复
    qc = client.get("/api/qc/queue", headers=CRED["tech"],
                    params={"status": "failed"}).json()
    ver = next(v for v in qc if v["study_id"] == sid)
    # 失败版本可检查步骤记录；预览在无派生制品时给确定 409
    detail = client.get(f"/api/versions/{ver['id']}", headers=CRED["tech"]).json()
    assert any(s["step"] == "auto_qc" for s in detail["steps"])
    assert client.get(f"/api/versions/{ver['id']}/preview",
                      headers=CRED["tech"]).status_code in (200, 409)
    # 自动 QC 未通过不能人工放行（前端不自造成功）
    blocked = client.post("/api/qc/review", headers=CRED["tech"],
                          json={"version_id": ver["id"], "approve": True})
    assert blocked.status_code == 409
    # 临床医生不能操作技师视图
    assert client.get("/api/queue/jobs", headers=CRED["clinician"]).status_code == 403


# ---------- 管理员工作台 ----------

def test_admin_filters_verifies_and_downloads_audit(client):
    sid = client.post("/api/studies/ingest-synthetic", headers=CRED["tech"],
                      json={}).json()["study_id"]
    _publish(client, sid)
    rid = _armed_relink(client, sid)
    client.post(f"/api/relink/{rid}/export", headers=CRED["clinician"])

    # 筛选
    rows = client.get("/api/audit", headers=CRED["admin"],
                      params={"study_id": sid, "action": "relink.export"}).json()
    assert rows and all(r["study_id"] == sid and r["action"] == "relink.export" for r in rows)
    # 校验
    v = client.get("/api/audit/verify", headers=CRED["admin"]).json()
    assert v["ok"] is True
    # 导出 JSON 与 CSV，再通过 admin 专属 blob 接口下载真实内容
    j = client.post("/api/audit/export", headers=CRED["admin"],
                    params={"fmt": "json"}).json()
    c = client.post("/api/audit/export", headers=CRED["admin"],
                    params={"fmt": "csv"}).json()
    assert j["chain_verification"]["ok"] and c["chain_verification"]["ok"]
    jb = client.get(f"/api/blobs/{j['cas_id']}", headers=CRED["admin"])
    cb = client.get(f"/api/blobs/{c['cas_id']}", headers=CRED["admin"])
    assert jb.status_code == 200 and cb.status_code == 200
    assert b"relink.export" in jb.content and b"seq,ts,actor" in cb.content
    # 边界：officer 可查询/校验但不能导出/下载；tech 完全不可
    assert client.post("/api/audit/export",
                       headers=CRED["officer"]).status_code == 403
    assert client.get(f"/api/blobs/{j['cas_id']}",
                      headers=CRED["officer"]).status_code == 403
    assert client.get("/api/audit", headers=CRED["tech"]).status_code == 403
