"""HTTP API 层：认证、角色、端到端流转、质检界面与审计导出。"""
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


def _ingest(client, fault=None):
    r = client.post("/api/studies/ingest-synthetic",
                    headers=CRED["tech"], json={"fault_inject": fault})
    assert r.status_code == 200, r.text
    return r.json()["study_id"]


def _publish(client, sid):
    r = client.post("/api/queue/run", headers=CRED["tech"], params={"cycles": 5})
    assert r.status_code == 200
    q = client.get("/api/qc/queue", headers=CRED["tech"], params={"status": "passed"})
    for item in q.json():
        if item["study_id"] == sid:
            rr = client.post("/api/qc/review", headers=CRED["tech"],
                             json={"version_id": item["id"], "approve": True, "note": "ok"})
            assert rr.status_code == 200
            return item["id"]
    raise AssertionError("no passed version found")


def test_auth_required_and_role_enforcement(client):
    assert client.get("/health").status_code == 200
    assert client.get("/api/studies").status_code == 401
    assert client.get("/api/studies", headers=_basic("tech", "wrong")).status_code == 401
    # clinician 不能入库
    r = client.post("/api/studies/ingest-synthetic", headers=CRED["clinician"], json={})
    assert r.status_code == 403
    # technician 不能审计导出
    assert client.post("/api/audit/export", headers=CRED["tech"]).status_code == 403


def test_full_flow_with_qc_ui_and_preview(client):
    sid = _ingest(client)
    assert _publish(client, sid)
    detail = client.get(f"/api/studies/{sid}", headers=CRED["tech"]).json()
    assert detail["study"]["latest_version_id"]
    ver_id = detail["study"]["latest_version_id"]
    # 质检界面可看预览图与质检报告
    png = client.get(f"/api/versions/{ver_id}/preview", headers=CRED["tech"])
    assert png.status_code == 200 and png.headers["content-type"] == "image/png"
    assert len(png.content) > 500
    vd = client.get(f"/api/versions/{ver_id}", headers=CRED["tech"]).json()
    assert vd["qc_report"]["pass"] is True
    steps = {s["step"] for s in vd["steps"]}
    assert {"tag_delete", "tag_replace", "private_tags_purged",
            "pixel_redaction", "auto_qc"} <= steps


def test_redaction_failure_retry_then_success(client):
    sid = _ingest(client, fault="redaction")
    client.post("/api/queue/run", headers=CRED["tech"],
                params={"cycles": 3, "ignore_backoff": True})
    jobs = client.get("/api/queue/jobs", headers=CRED["tech"]).json()
    assert any(j["status"] == "dead" for j in jobs)
    dead = next(j for j in jobs if j["status"] == "dead")
    # 运维修复：requeue 时清空故障注入，重新处理后正常发布
    rq = client.post("/api/queue/requeue", headers=CRED["tech"],
                     json={"job_id": dead["id"], "clear_fault": True})
    assert rq.status_code == 200
    assert _publish(client, sid)


def test_qc_leak_never_published(client):
    sid = _ingest(client, fault="qc_leak")
    client.post("/api/queue/run", headers=CRED["tech"])
    q = client.get("/api/qc/queue", headers=CRED["tech"], params={"status": "failed"}).json()
    assert any(v["study_id"] == sid for v in q)
    study = client.get(f"/api/studies/{sid}", headers=CRED["tech"]).json()["study"]
    assert study["latest_version_id"] is None


def test_policy_upgrade_endpoint_creates_version(client):
    sid = _ingest(client)
    _publish(client, sid)
    r = client.post("/api/policies", headers=CRED["admin"],
                    json={"name": "project-default", "level": "minimal"})
    assert r.status_code == 200 and r.json()["version"] == 2
    d = client.post("/api/versions/derive", headers=CRED["tech"],
                    json={"study_id": sid, "reason": "upgrade"})
    assert d.status_code == 200 and d.json()["version_no"] == 2
    client.post("/api/queue/run", headers=CRED["tech"])
    _publish(client, sid)
    versions = client.get(f"/api/studies/{sid}", headers=CRED["tech"]).json()["versions"]
    assert [v["version_no"] for v in versions] == [1, 2]
    assert versions[0]["status"] == versions[1]["status"] == "published"


def test_relink_two_role_flow_export_invalidation_and_audit(client):
    sid = _ingest(client)
    _publish(client, sid)
    # clinician 请求 → 拿 share A
    r = client.post("/api/relink/request", headers=CRED["clinician"],
                    json={"study_id": sid, "purpose": "M&M correction", "window_minutes": 30})
    assert r.status_code == 200
    reid_id, share_a = r.json()["id"], r.json()["share_for_requester"]
    # step1 未交前，officer 待批准队列为空
    assert client.get("/api/relink/officer/pending", headers=CRED["officer"]).json() == []
    sa = client.post(f"/api/relink/{reid_id}/share-a", headers=CRED["clinician"],
                     json={"share": share_a})
    assert sa.status_code == 200
    pend = client.get("/api/relink/officer/pending", headers=CRED["officer"]).json()
    assert len(pend) == 1 and pend[0]["id"] == reid_id
    share_b = pend[0]["share_for_officer"]
    sb = client.post(f"/api/relink/{reid_id}/share-b", headers=CRED["officer"],
                     json={"share": share_b})
    assert sb.json()["status"] == "armed"
    # 纠错并导出
    client.post(f"/api/relink/{reid_id}/correction", headers=CRED["clinician"],
                json={"corrected": {"PatientID": "MRN-FIXED"}})
    ex = client.post(f"/api/relink/{reid_id}/export", headers=CRED["clinician"])
    assert ex.status_code == 200
    bundle = ex.json()["bundle"]
    assert bundle["original_identity"]["PatientID"] == "MRN-48213"
    assert bundle["corrected_identity"]["PatientID"] == "MRN-FIXED"
    # 立即失效
    again = client.post(f"/api/relink/{reid_id}/export", headers=CRED["clinician"])
    assert again.status_code == 409
    # 普通列表看不到任何身份内容
    lst = client.get("/api/relink", headers=CRED["admin"]).json()
    assert all("envelope" not in x for x in lst)

    # 审计链与导出
    assert client.get("/api/audit/verify", headers=CRED["admin"]).json()["ok"] is True
    exp = client.post("/api/audit/export", headers=CRED["admin"]).json()
    assert exp["chain_verification"]["ok"] is True and exp["records"] > 5
    actions = {e["action"] for e in client.get("/api/audit", headers=CRED["admin"]).json()}
    assert {"relink.request", "relink.step1", "relink.step2_armed",
            "relink.export"} <= actions
    # officer 也可查审计但不能导出
    assert client.get("/api/audit", headers=CRED["officer"]).status_code == 200
    assert client.post("/api/audit/export", headers=CRED["officer"]).status_code == 403


def test_revoke_beats_or_blocks_export(client):
    sid = _ingest(client)
    _publish(client, sid)
    r = client.post("/api/relink/request", headers=CRED["clinician"],
                    json={"study_id": sid, "purpose": "x"})
    reid_id, share_a = r.json()["id"], r.json()["share_for_requester"]
    client.post(f"/api/relink/{reid_id}/share-a", headers=CRED["clinician"],
                json={"share": share_a})
    share_b = client.get("/api/relink/officer/pending",
                         headers=CRED["officer"]).json()[0]["share_for_officer"]
    client.post(f"/api/relink/{reid_id}/share-b", headers=CRED["officer"],
                json={"share": share_b})
    rv = client.post(f"/api/relink/{reid_id}/revoke", headers=CRED["officer"],
                     json={"reason": "emergency revoke"})
    assert rv.json()["status"] == "revoked"
    ex = client.post(f"/api/relink/{reid_id}/export", headers=CRED["clinician"])
    assert ex.status_code == 409
