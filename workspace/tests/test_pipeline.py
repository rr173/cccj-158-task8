"""端到端：入库冻结、队列处理、质检发布、策略升级只产生新版本。"""
import types

import pytest


@pytest.fixture()
def M(home):
    from app.core import dicomutil, store
    from app.services import (audit as audit_svc, ingest as ingest_svc,
                              policies as policies_svc, queue, versions as versions_svc)
    return types.SimpleNamespace(
        dicomutil=dicomutil, store=store, audit=audit_svc, ingest=ingest_svc,
        policies=policies_svc, queue=queue, versions=versions_svc)


def _ingest_and_process(M, fault=None):
    data = M.dicomutil.build_synthetic_dicom()
    out = M.ingest.ingest_study(data, "tech", fault_inject=fault)
    assert out["idempotent"] is False
    r = M.queue.run_once()
    assert r and r["outcome"] == "done"
    return out["study_id"]


def test_ingest_freezes_summary_and_publishes_v1(M):
    sid = _ingest_and_process(M)
    study = M.ingest.get_study(sid)
    summary = M.ingest.frozen_summary(sid)
    assert summary["banner_text_detected"] is True  # 原始冻结时有烧录文字
    assert "ZHANG" in summary["identity"]["PatientName"]

    versions = M.versions.list_versions(sid)
    assert len(versions) == 1
    v = versions[0]
    assert v["version_no"] == 1 and v["status"] == "ready" and v["qc_status"] == "passed"

    # 自动质检通过后人工放行才发布
    v2 = M.versions.manual_qc(v["id"], "tech", True, note="ok")
    assert v2["status"] == "published" and v2["qc_status"] == "approved"
    study = M.ingest.get_study(sid)
    assert study["latest_version_id"] == v["id"]

    # 派生 DICOM 已彻底去标识
    derived = M.store.sealed_blob_get(v["derived_cas_id"])
    qc = M.dicomutil.quality_check(derived, M.dicomutil.default_policy(),
                                   study["pseudo_patient_id"], study["pseudo_patient_name"])
    assert qc["pass"] is True
    assert b"ZHANG" not in derived and b"MRN-48213" not in derived
    # 原始冻结 Blob 仍含身份（封存不动）
    source = M.store.sealed_blob_get(study["source_cas_id"])
    assert b"ZHANG" in source


def test_policy_upgrade_creates_new_version_never_overwrites(M):
    sid = _ingest_and_process(M)
    v1 = M.versions.list_versions(sid)[0]
    M.versions.manual_qc(v1["id"], "tech", True)
    old_cas = v1["derived_cas_id"]

    M.policies.register_policy("project-default",
                               M.dicomutil.default_policy("minimal"), "admin")
    M.versions.create_version(sid, "tech", reason="policy upgrade")
    r = M.queue.run_once()
    assert r["outcome"] == "done"
    M.versions.manual_qc(M.versions.list_versions(sid)[1]["id"], "tech", True)

    vs = M.versions.list_versions(sid)
    assert [x["version_no"] for x in vs] == [1, 2]
    assert vs[0]["derived_cas_id"] == old_cas  # 旧版本 Blob 未被覆盖
    assert vs[1]["derived_cas_id"] != old_cas
    assert M.ingest.get_study(sid)["latest_version_id"] == vs[1]["id"]


def test_ingest_idempotent_on_same_bytes(M):
    data = M.dicomutil.build_synthetic_dicom()
    a = M.ingest.ingest_study(data, "tech")
    b = M.ingest.ingest_study(data, "tech")
    assert a["study_id"] == b["study_id"] and b["idempotent"] is True


def test_stable_pseudonym_same_identity(M):
    d1 = M.dicomutil.build_synthetic_dicom(accession="ACC-A")
    d2 = M.dicomutil.build_synthetic_dicom(accession="ACC-B")  # 同患者不同号
    a = M.ingest.ingest_study(d1, "tech")
    b = M.ingest.ingest_study(d2, "tech")
    sa, sb = M.ingest.get_study(a["study_id"]), M.ingest.get_study(b["study_id"])
    assert sa["pseudo_patient_id"] == sb["pseudo_patient_id"]


def test_qc_leak_quarantines_half_baked(M):
    sid = _ingest_and_process(M, fault="qc_leak")
    v = M.versions.list_versions(sid)[0]
    assert v["status"] == "quarantined" and v["qc_status"] == "failed"
    assert M.ingest.get_study(sid)["latest_version_id"] is None  # 半成品绝不发布
    M.versions.create_version(sid, "tech", reason="retry after quarantine")
    r = M.queue.run_once()
    assert r["outcome"] == "done"
    v2 = M.versions.list_versions(sid)[1]
    assert v2["status"] == "ready"


def test_blob_store_is_worm(M):
    sid = _ingest_and_process(M)
    study = M.ingest.get_study(sid)
    path = M.store.blob_path(study["source_cas_id"])
    assert oct(path.stat().st_mode & 0o777) == "0o444"


def test_audit_chain_intact(M):
    _ingest_and_process(M)
    assert M.store.verify_audit_chain()["ok"] is True
    export = M.audit.export_jsonl("admin")
    assert export["chain_verification"]["ok"] is True and export["records"] >= 3
