"""受控回链的授权语义与身份保护。"""
import types

import pytest


@pytest.fixture()
def M(home):
    from app.core import dicomutil, store
    from app.services import (ingest as ingest_svc, queue, reidentify,
                              versions as versions_svc)
    from app.services.vault import vault
    return types.SimpleNamespace(
        dicomutil=dicomutil, store=store, ingest=ingest_svc, queue=queue,
        reidentify=reidentify, versions=versions_svc, vault=vault)


def _study(M):
    data = M.dicomutil.build_synthetic_dicom()
    out = M.ingest.ingest_study(data, "tech")
    M.queue.run_once()
    v = M.versions.list_versions(out["study_id"])[0]
    M.versions.manual_qc(v["id"], "tech", True)
    return out["study_id"]


def _armed(M, sid):
    req = M.reidentify.request_relink(sid, "clinician", "clinical correction",
                                      window_minutes=30)
    reid_id, share_a = req["id"], req["share_for_requester"]
    M.reidentify.submit_share(reid_id, "clinician", "clinician", share_a)
    share_b = M.reidentify.officer_pending("officer")[0]["share_for_officer"]
    M.reidentify.submit_share(reid_id, "officer", "officer", share_b)
    return reid_id


def test_full_relink_correction_export_then_invalid(M):
    sid = _study(M)
    reid_id = _armed(M, sid)
    M.reidentify.submit_correction(
        reid_id, "clinician",
        {"PatientID": "MRN-48213-FIX", "PatientName": "ZHANG^WEIGUO"})
    result = M.reidentify.export_relink(reid_id, "clinician")
    assert result["status"] == "exported"
    assert result["bundle"]["original_identity"]["PatientID"] == "MRN-48213"
    assert result["bundle"]["corrected_identity"]["PatientID"] == "MRN-48213-FIX"
    # 导出后立即失效：保险库清空、状态 exported、二次导出被拒
    assert M.vault.get(reid_id) is None
    with pytest.raises(M.store.ConflictError):
        M.reidentify.export_relink(reid_id, "clinician")
    # 清单接口不回传信封密文
    lst = M.reidentify.list_relinks()
    assert all("envelope" not in x for x in lst)
    assert "envelope" not in M.reidentify.get_relink(reid_id)


def test_wrong_share_rejected(M):
    sid = _study(M)
    req = M.reidentify.request_relink(sid, "clinician", "x")
    reid_id = req["id"]
    with pytest.raises(M.store.ConflictError):
        M.reidentify.submit_share(reid_id, "clinician", "clinician", "a" * 44)
    # 隐私官队列在 step1 未登记前不出现该授权
    assert M.reidentify.officer_pending("officer") == []


def test_only_requester_can_export(M):
    sid = _study(M)
    reid_id = _armed(M, sid)
    with pytest.raises(M.store.ConflictError):
        M.reidentify.export_relink(reid_id, "other-clinician")


def test_revocation_before_arming_kills_authorization(M):
    sid = _study(M)
    req = M.reidentify.request_relink(sid, "clinician", "x")
    reid_id, share_a = req["id"], req["share_for_requester"]
    res = M.reidentify.revoke_relink(reid_id, "officer", "denied")
    assert res["revoked"] is True
    assert M.vault.get(reid_id) is None
    with pytest.raises(M.store.ConflictError):
        M.reidentify.submit_share(reid_id, "clinician", "clinician", share_a)
    with pytest.raises(M.store.ConflictError):
        M.reidentify.export_relink(reid_id, "clinician")


def test_relink_is_bound_to_purpose_study_window(M):
    sid = _study(M)
    req = M.reidentify.request_relink(sid, "clinician", "specific purpose",
                                      window_minutes=30)
    row = M.reidentify.get_relink(req["id"])
    assert row["purpose"] == "specific purpose"
    assert row["study_id"] == sid and row["window_end"] > row["window_start"]


def test_no_relink_without_published_version(M):
    data = M.dicomutil.build_synthetic_dicom()
    out = M.ingest.ingest_study(data, "tech")  # 不运行 worker
    with pytest.raises(M.store.ConflictError):
        M.reidentify.request_relink(out["study_id"], "clinician", "x")
