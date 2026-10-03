"""验真快照：确定截止点冻结、幂等、角色边界、篡改可核对、并发不混入。

覆盖需求逐条：
- admin 按筛选条件与当前审计序号创建不可变快照；页面可查范围/链锚/记录数/完整性；
  下载含清单与记录的验真包；officer 只能查询与复核，不能创建或下载。
- 服务端确定截止点取数：冻结后并发写入的新审计事件不改变快照。
- 同一幂等键重复请求（刷新/重复点击/并发）返回原快照；同键不同参数确定性冲突。
- 筛选为空得到记录数 0 但仍冻结链锚的可核对结果。
- 原链被篡改：创建被拒（409 确定状态）；既有快照现场验真标记锚/行不一致；
  验真包文件被改：详情与下载都给出确定状态，绝不交付被改内容。
- 既有审计查询、链校验、JSON/CSV 导出接口保持可用。
"""
import base64
import io
import json
import os
import threading
import zipfile

import pytest
from fastapi.testclient import TestClient


@pytest.fixture()
def client(home):
    from app.web.api import app
    with TestClient(app) as c:
        yield c


@pytest.fixture()
def M(home):
    import types
    from app.core import dicomutil, store
    from app.services import (ingest as ingest_svc, queue, snapshots,
                              versions as versions_svc)
    return types.SimpleNamespace(
        dicomutil=dicomutil, store=store, ingest=ingest_svc, queue=queue,
        snapshots=snapshots, versions=versions_svc)


def _basic(user, pw):
    return {"Authorization": "Basic " + base64.b64encode(f"{user}:{pw}".encode()).decode()}


CRED = {
    "tech": _basic("tech", "tech-pw-123"),
    "clinician": _basic("clinician", "clinician-pw-123"),
    "officer": _basic("officer", "officer-pw-123"),
    "admin": _basic("admin", "admin-pw-123"),
}


def _seed_audit_activity(client):
    """产生入库→处理→放行→回链导出的一串审计事件，返回 (sid, reid_id)。"""
    sid = client.post("/api/studies/ingest-synthetic", headers=CRED["tech"],
                      json={}).json()["study_id"]
    client.post("/api/queue/run", headers=CRED["tech"], params={"cycles": 5})
    q = client.get("/api/qc/queue", headers=CRED["tech"],
                   params={"status": "passed"}).json()
    vid = next(v["id"] for v in q if v["study_id"] == sid)
    client.post("/api/qc/review", headers=CRED["tech"],
                json={"version_id": vid, "approve": True, "note": "ok"})
    r = client.post("/api/relink/request", headers=CRED["clinician"],
                    json={"study_id": sid, "purpose": "snap flow"})
    rid, share_a = r.json()["id"], r.json()["share_for_requester"]
    client.post(f"/api/relink/{rid}/share-a", headers=CRED["clinician"],
                json={"share": share_a})
    pend = client.get("/api/relink/officer/pending",
                      headers=CRED["officer"]).json()
    share_b = next(p["share_for_officer"] for p in pend if p["id"] == rid)
    client.post(f"/api/relink/{rid}/share-b", headers=CRED["officer"],
                json={"share": share_b})
    client.post(f"/api/relink/{rid}/export", headers=CRED["clinician"])
    return sid, rid


def _create(client, key, **filters):
    return client.post("/api/audit/snapshots", headers=CRED["admin"],
                       json={"idempotency_key": key, **filters})


# ---------- 角色边界 ----------

def test_officer_can_query_and_review_but_not_create_or_download(client):
    _seed_audit_activity(client)
    created = _create(client, "k-admin-only")
    assert created.status_code == 200
    sid = created.json()["id"]

    # officer：列表 / 单条 / 现场验真均可（复核）
    lst = client.get("/api/audit/snapshots", headers=CRED["officer"])
    assert lst.status_code == 200 and len(lst.json()) == 1
    view = lst.json()[0]
    assert view["verification"]["ok"] is True
    one = client.get(f"/api/audit/snapshots/{sid}", headers=CRED["officer"])
    assert one.status_code == 200 and one.json()["id"] == sid

    # officer 不能创建、不能下载验真包；tech 完全不可
    assert client.post("/api/audit/snapshots", headers=CRED["officer"],
                       json={"idempotency_key": "x"}).status_code == 403
    assert client.get(f"/api/audit/snapshots/{sid}/pack",
                      headers=CRED["officer"]).status_code == 403
    assert client.get("/api/audit/snapshots",
                      headers=CRED["tech"]).status_code == 403
    # 未认证 → 401
    assert client.get("/api/audit/snapshots").status_code == 401


# ---------- 冻结内容与范围 ----------

def test_snapshot_freezes_scope_anchor_counts_and_pack_contents(client):
    sid, rid = _seed_audit_activity(client)
    head = client.get("/api/audit/verify",
                      headers=CRED["admin"]).json()["count"]

    r = _create(client, "freeze-all")
    assert r.status_code == 200
    snap = r.json()
    assert snap["cutoff_seq"] == head
    assert snap["record_count"] == head
    assert snap["first_seq"] == 1 and snap["last_seq"] == head
    assert snap["anchor_mac"] and snap["anchor_mac"] != "GENESIS"
    assert snap["created_by"] == "admin"
    assert snap["idempotent_replay"] is False
    assert snap["verification"]["ok"] is True
    for flag in ("pack_file_ok", "manifest_ok", "records_digests_ok",
                 "frozen_chain_ok", "anchor_intact",
                 "frozen_rows_match_live", "live_chain_ok"):
        assert snap["verification"][flag] is True

    # 筛选快照：范围与记录数只含匹配行，锚仍是当时链尾
    # （首个快照的创建本身会追加一条 audit.snapshot.create，链头前进 1）
    head2 = client.get("/api/audit/verify",
                       headers=CRED["admin"]).json()["count"]
    rf = _create(client, "freeze-filtered", action="relink.export")
    sf = rf.json()
    assert sf["cutoff_seq"] == head2
    assert sf["record_count"] == 1
    assert sf["verification"]["ok"] is True

    # 验真包：ZIP 含清单与记录；清单摘要、逐条摘要、记录根都可独立重算
    p = client.get(f"/api/audit/snapshots/{snap['id']}/pack",
                   headers=CRED["admin"])
    assert p.status_code == 200
    assert p.headers["content-type"] == "application/zip"
    assert snap["id"] in p.headers["content-disposition"]
    zf = zipfile.ZipFile(io.BytesIO(p.content))
    assert set(zf.namelist()) == {"manifest.json", "records.jsonl", "README.txt"}
    import hashlib
    manifest = json.loads(zf.read("manifest.json"))
    records_bytes = zf.read("records.jsonl")
    assert hashlib.sha256(records_bytes).hexdigest() == \
        manifest["records_root"]["sha256"]
    lines = [json.loads(x) for x in records_bytes.decode().splitlines()]
    assert len(lines) == head
    for entry, rec in zip(manifest["records"], lines):
        assert entry["seq"] == rec["seq"]
        assert entry["sha256"] == hashlib.sha256(
            json.dumps(rec, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
    assert manifest["scope"]["filters"] == {}
    # 包内不含任何原身份值（relink.export 审计 detail 只有 cas_id/字段名清单，
    # 与既有 /api/audit 接口可见范围一致，绝不含真实 PatientID/姓名）
    assert b"MRN-48213" not in records_bytes
    assert b"ZHANG" not in records_bytes
    # 导出物 cas_id 在审计行里以明文引用属正常（信封密文 blob 的寻址 id）
    assert b"MRN-48213" not in zf.read("manifest.json")


def test_filter_with_no_matches_freezes_verifiable_empty_scope(client):
    _seed_audit_activity(client)
    r = _create(client, "empty-match", actor="actor-that-never-acted")
    assert r.status_code == 200
    snap = r.json()
    assert snap["record_count"] == 0
    assert snap["first_seq"] is None and snap["last_seq"] is None
    # 链锚仍然冻结，验真仍然通过
    assert snap["anchor_mac"] and snap["verification"]["ok"] is True

    # 空库（无任何审计事件）也能冻结 GENESIS 锚
    from app.core import store
    conn = store.get_conn()
    conn.execute("DELETE FROM audit_log")
    r0 = _create(client, "empty-chain")
    assert r0.status_code == 200
    s0 = r0.json()
    assert s0["cutoff_seq"] == 0 and s0["record_count"] == 0
    assert s0["anchor_mac"] == "GENESIS"
    assert s0["verification"]["ok"] is True


# ---------- 幂等：刷新 / 重复点击 / 并发 ----------

def test_same_idempotency_key_returns_original_snapshot(client):
    _seed_audit_activity(client)
    first = _create(client, "idem-key-1").json()
    # 冻结之后再制造新审计事件并重复请求（等价于刷新/重复点击）
    client.post("/api/audit/export", headers=CRED["admin"])
    again = _create(client, "idem-key-1")
    assert again.status_code == 200
    s2 = again.json()
    assert s2["id"] == first["id"]
    assert s2["idempotent_replay"] is True
    assert s2["cutoff_seq"] == first["cutoff_seq"]
    assert s2["record_count"] == first["record_count"]
    assert s2["pack_cas_id"] == first["pack_cas_id"]
    # 快照表仍只有一行
    rows = client.get("/api/audit/snapshots",
                      headers=CRED["admin"]).json()
    assert len(rows) == 1

    # 同键不同参数 → 确定性 409，文案带回原快照 id 供核对
    conflict = _create(client, "idem-key-1", actor="someone-else")
    assert conflict.status_code == 409
    assert first["id"] in conflict.json()["detail"]

    # 非法幂等键
    bad = client.post("/api/audit/snapshots", headers=CRED["admin"],
                      json={"idempotency_key": "../bad key!"})
    assert bad.status_code in (409, 422)


def test_concurrent_creation_same_key_creates_exactly_one(M):
    data = M.dicomutil.build_synthetic_dicom()
    out = M.ingest.ingest_study(data, "tech")
    M.queue.run_once()
    v = M.versions.list_versions(out["study_id"])[0]
    M.versions.manual_qc(v["id"], "tech", True)

    results, errors = [], []
    barrier = threading.Barrier(8)

    def worker(i):
        barrier.wait()
        try:
            results.append(M.snapshots.create_snapshot(
                f"admin{i}", "race-same-key", {}))
        except Exception as exc:  # pragma: no cover - 并发不应产生异常
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors
    assert len({r["id"] for r in results}) == 1
    assert sum(1 for r in results if not r.get("idempotent_replay")) == 1
    assert M.store.one("SELECT count(*) c FROM audit_snapshots")["c"] == 1


def test_concurrent_audit_writes_never_mix_into_frozen_snapshot(M):
    data = M.dicomutil.build_synthetic_dicom()
    out = M.ingest.ingest_study(data, "tech")
    M.queue.run_once()
    v = M.versions.list_versions(out["study_id"])[0]
    M.versions.manual_qc(v["id"], "tech", True)

    snaps, errors = [], []
    barrier = threading.Barrier(6)

    def noise():
        barrier.wait()
        for _ in range(30):
            M.store.audit("noise", "noise.action")

    def freeze(i):
        barrier.wait()
        try:
            snaps.append(M.snapshots.create_snapshot("admin", f"key-{i}", {}))
        except Exception as exc:  # pragma: no cover
            errors.append(exc)

    threads = [threading.Thread(target=freeze, args=(i,)) for i in range(5)]
    threads.append(threading.Thread(target=noise))
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors and len(snaps) == 5

    # 每个快照内部自洽：全量无筛选时 last_seq 必须正好等于 cutoff，
    # 记录数必须等于 cutoff（并发新事件要么在 cutoff 前、要么被排除）
    for s in snaps:
        assert s["last_seq"] == s["cutoff_seq"]
        assert s["record_count"] == s["cutoff_seq"]
        assert M.snapshots.verify_snapshot(s)["ok"] is True
    # 各快照截止点互不相同也无妨，但每个 id/包 唯一且不可变
    assert len({s["id"] for s in snaps}) == 5
    assert len({s["pack_cas_id"] for s in snaps}) == 5


# ---------- 篡改：原链 / 验真包 ----------

def test_create_refused_when_live_chain_already_tampered(client):
    _seed_audit_activity(client)
    from app.core import store
    conn = store.get_conn()
    target = conn.execute("SELECT max(seq) s FROM audit_log").fetchone()["s"]
    row = conn.execute("SELECT mac FROM audit_log WHERE seq=?",
                       (target,)).fetchone()
    forged = ("0" if row["mac"][0] != "0" else "1") + row["mac"][1:]
    conn.execute("UPDATE audit_log SET mac=? WHERE seq=?", (forged, target))

    r = _create(client, "after-tamper")
    assert r.status_code == 409
    detail = r.json()["detail"]
    assert detail["error"] == "audit_chain_broken"
    assert detail["broken_at"] == target
    # 确定性状态不含任何身份内容
    assert "MRN" not in json.dumps(detail)
    # 没有产生快照行
    assert client.get("/api/audit/snapshots",
                      headers=CRED["admin"]).json() == []


def test_existing_snapshot_reports_tampered_live_chain_deterministically(client):
    _seed_audit_activity(client)
    snap = _create(client, "before-tamper").json()

    # 在截止点之后追加事件：快照仍完全验真，只报告新增条数
    client.post("/api/audit/export", headers=CRED["admin"])
    v = client.get(f"/api/audit/snapshots/{snap['id']}",
                   headers=CRED["admin"]).json()["verification"]
    assert v["ok"] is True and v["new_events_after_cutoff"] >= 1

    # 篡改截止点行：冻结包自身仍完好，但锚与冻结行交叉核对失败
    from app.core import store
    conn = store.get_conn()
    row = conn.execute("SELECT mac FROM audit_log WHERE seq=?",
                       (snap["cutoff_seq"],)).fetchone()
    forged = ("0" if row["mac"][0] != "0" else "1") + row["mac"][1:]
    conn.execute("UPDATE audit_log SET mac=? WHERE seq=?",
                 (forged, snap["cutoff_seq"]))

    view = client.get(f"/api/audit/snapshots/{snap['id']}",
                      headers=CRED["admin"]).json()
    v2 = view["verification"]
    assert v2["ok"] is False
    assert v2["pack_file_ok"] and v2["frozen_chain_ok"]  # 冻结包本身没被动
    assert v2["anchor_intact"] is False
    assert v2["frozen_rows_match_live"] is False
    assert v2["broken_at"] == snap["cutoff_seq"]
    # officer 复核同样看到确定状态
    off = client.get(f"/api/audit/snapshots/{snap['id']}",
                     headers=CRED["officer"]).json()["verification"]
    assert off["ok"] is False and off["anchor_intact"] is False


def test_tampered_pack_file_is_detected_and_download_refused(client):
    _seed_audit_activity(client)
    snap = _create(client, "pack-tamper").json()

    from app import config
    cas = snap["pack_cas_id"]
    path = config.BLOB_DIR / cas[:2] / cas
    payload = bytearray(path.read_bytes())
    payload[120] ^= 0xFF  # 翻转 ZIP 内容字节
    os.chmod(path, 0o644)
    path.write_bytes(bytes(payload))
    os.chmod(path, 0o444)

    # 详情：包文件摘要层即暴露断裂
    v = client.get(f"/api/audit/snapshots/{snap['id']}",
                   headers=CRED["admin"]).json()["verification"]
    assert v["ok"] is False and v["pack_file_ok"] is False
    assert v["reason"] == "pack_file_sha_mismatch"

    # 下载被确定性拒绝，错误不回显包内容
    d = client.get(f"/api/audit/snapshots/{snap['id']}/pack",
                   headers=CRED["admin"])
    assert d.status_code == 409
    assert d.json()["detail"]["error"] == "verify_pack_tampered"

    # 通用 blob 接口不提供验真包，避免绕过交付前验真
    assert client.get(f"/api/blobs/{cas}",
                      headers=CRED["admin"]).status_code == 404


def test_pack_internal_content_tampering_is_detected(M):
    """包内记录/清单被替换（即便外层 WORM 锚被同步伪造的极端情形）也会暴露。"""
    data = M.dicomutil.build_synthetic_dicom()
    out = M.ingest.ingest_study(data, "tech")
    M.queue.run_once()
    v = M.versions.list_versions(out["study_id"])[0]
    M.versions.manual_qc(v["id"], "tech", True)

    snap = M.snapshots.create_snapshot("admin", "inner-tamper", {})
    from app.services import snapshots as svc
    pack = M.store.blob_get(snap["pack_cas_id"])
    zf = zipfile.ZipFile(io.BytesIO(pack))
    records = [json.loads(x) for x in zf.read("records.jsonl").decode().splitlines()]
    records[0]["action"] = "forged.action"
    new_records = ("\n".join(json.dumps(r, sort_keys=True) for r in records)
                   + "\n").encode()
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as out_zip:
        out_zip.writestr("manifest.json", zf.read("manifest.json"))
        out_zip.writestr("records.jsonl", new_records)
        out_zip.writestr("README.txt", zf.read("README.txt"))
    forged = buf.getvalue()

    with pytest.raises(svc.PackTampered):
        svc._read_pack(forged)

    # 缺文件 / 加文件同样被拒
    buf2 = io.BytesIO()
    with zipfile.ZipFile(buf2, "w", zipfile.ZIP_DEFLATED) as z2:
        z2.writestr("manifest.json", zf.read("manifest.json"))
        z2.writestr("records.jsonl", zf.read("records.jsonl"))
    with pytest.raises(svc.PackTampered):
        svc._read_pack(buf2.getvalue())


# ---------- 既有审计接口保持可用 + 审计可追溯 ----------

def test_existing_audit_interfaces_still_work_and_snapshot_is_audited(client):
    sid, rid = _seed_audit_activity(client)

    # 既有查询 / 校验 / 导出均不受影响
    rows = client.get("/api/audit", headers=CRED["admin"],
                      params={"action": "relink.export"}).json()
    assert rows and all(r["action"] == "relink.export" for r in rows)
    assert client.get("/api/audit/verify",
                      headers=CRED["admin"]).json()["ok"] is True
    j = client.post("/api/audit/export", headers=CRED["admin"],
                    params={"fmt": "json"}).json()
    c = client.post("/api/audit/export", headers=CRED["admin"],
                    params={"fmt": "csv"}).json()
    assert j["chain_verification"]["ok"] and c["chain_verification"]["ok"]

    _create(client, "audited-create")
    # 创建快照本身被哈希链记录
    actions = {r["action"] for r in
               client.get("/api/audit", headers=CRED["admin"]).json()}
    assert "audit.snapshot.create" in actions

    snap_id = client.get("/api/audit/snapshots",
                         headers=CRED["admin"]).json()[0]["id"]
    client.get(f"/api/audit/snapshots/{snap_id}/pack", headers=CRED["admin"])
    detail_actions = client.get(
        "/api/audit", headers=CRED["admin"],
        params={"action": "audit.snapshot.download"}).json()
    assert detail_actions and detail_actions[0]["detail"]["snapshot_id"] == snap_id


def test_snapshot_unknown_id_is_404(client):
    assert client.get("/api/audit/snapshots/snp_does_not_exist",
                      headers=CRED["admin"]).status_code == 404
    assert client.get("/api/audit/snapshots/snp_does_not_exist/pack",
                      headers=CRED["admin"]).status_code == 404
