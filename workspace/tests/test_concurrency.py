"""并发安全：同影像并发修订、批量重试、队列租约、回链撤销竞态。"""
import threading
import time
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


def _published_study(M):
    data = M.dicomutil.build_synthetic_dicom()
    out = M.ingest.ingest_study(data, "tech")
    M.queue.run_once()
    v = M.versions.list_versions(out["study_id"])[0]
    M.versions.manual_qc(v["id"], "tech", True)
    return out["study_id"]


def _arm(M, sid):
    req = M.reidentify.request_relink(sid, "clinician", "correction", window_minutes=30)
    reid_id, share_a = req["id"], req["share_for_requester"]
    M.reidentify.submit_share(reid_id, "clinician", "clinician", share_a)
    share_b = M.reidentify.officer_pending("officer")[0]["share_for_officer"]
    M.reidentify.submit_share(reid_id, "officer", "officer", share_b)
    return reid_id


def test_concurrent_revisions_get_distinct_version_numbers(M):
    sid = _published_study(M)
    errors, results = [], []

    def derive(i):
        try:
            v = M.versions.create_version(sid, f"tech{i}", reason=f"concurrent {i}")
            results.append(v["version_no"])
        except Exception as exc:
            errors.append(exc)

    threads = [threading.Thread(target=derive, args=(i,)) for i in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # 部分请求可能因病例锁被拒；成功者的版本号必须互不相同
    assert len(results) + len(errors) == 6
    assert len(results) == len(set(results))
    assert len(results) >= 2
    nums = sorted(v["version_no"] for v in M.versions.list_versions(sid))
    assert nums == list(range(1, len(nums) + 1))


def test_batch_retry_no_duplicate_claims(M):
    for i in range(5):
        data = M.dicomutil.build_synthetic_dicom(accession=f"ACC-{i}")
        M.ingest.ingest_study(data, "tech", fault_inject="redaction")

    claimed = []
    barrier = threading.Barrier(5)

    def worker(wid):
        barrier.wait()
        job = M.queue.claim(f"worker-{wid}")
        claimed.append(job["id"] if job else None)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    got = [j for j in claimed if j]
    assert len(got) == 5 and len(set(got)) == 5  # 无任务被两个 worker 同时领取


def test_dead_job_requires_manual_requeue_and_never_publishes(M):
    data = M.dicomutil.build_synthetic_dicom()
    out = M.ingest.ingest_study(data, "tech", fault_inject="redaction")
    sid, vid = out["study_id"], out["version"]["id"]
    for _ in range(3):
        r = M.queue.run_once(ignore_backoff=True)
        assert r is not None
    jobs = M.queue.list_jobs()
    assert jobs[0]["status"] == "dead"
    v = M.versions.get_version(vid)
    assert v["status"] == "failed" and M.ingest.get_study(sid)["latest_version_id"] is None

    # 运维修复故障注入后，手工 requeue 才能继续
    M.queue.requeue(jobs[0]["id"], "tech")
    conn = M.store.get_conn()
    conn.execute("UPDATE jobs SET payload_json='{}' WHERE id=?", (jobs[0]["id"],))
    r = M.queue.run_once()
    assert r["outcome"] == "done"
    assert M.versions.get_version(vid)["status"] == "ready"


def test_lease_expiry_reclaim_after_worker_crash(M):
    data = M.dicomutil.build_synthetic_dicom()
    M.ingest.ingest_study(data, "tech")
    job = M.queue.claim("crashed-worker", lease_seconds=0)
    assert job["status"] == "running"
    time.sleep(0.02)
    reclaimed = M.queue.claim("new-worker", lease_seconds=60)
    assert reclaimed["id"] == job["id"] and reclaimed["lease_owner"] == "new-worker"


def test_revoke_export_race_only_one_wins(M):
    sid = _published_study(M)
    reid_id = _arm(M, sid)
    assert M.reidentify.get_relink(reid_id)["status"] == "armed"
    outcomes = []
    barrier = threading.Barrier(2)

    def do_export():
        barrier.wait()
        try:
            outcomes.append(("export", M.reidentify.export_relink(reid_id, "clinician")))
        except Exception as exc:
            outcomes.append(("export_err", str(exc)))

    def do_revoke():
        barrier.wait()
        try:
            outcomes.append(("revoke", M.reidentify.revoke_relink(reid_id, "officer", "race")))
        except Exception as exc:
            outcomes.append(("revoke_err", str(exc)))

    t1 = threading.Thread(target=do_export)
    t2 = threading.Thread(target=do_revoke)
    t1.start(); t2.start(); t1.join(); t2.join()

    status = M.reidentify.get_relink(reid_id)["status"]
    assert status in ("exported", "revoked")
    exports = [o for o in outcomes if o[0] == "export"]
    assert len(exports) <= 1
    if status == "exported":
        assert len(exports) == 1
        assert M.vault.get(reid_id) is None  # 分片已抹除
        with pytest.raises(M.store.ConflictError):
            M.reidentify.export_relink(reid_id, "clinician")
    else:
        # 撤销获胜：导出被拒，导出物不存在
        assert exports == []
        assert M.reidentify.get_relink(reid_id)["export_cas_id"] is None


def test_window_expiry_blocks_export_and_purges_shares(M):
    sid = _published_study(M)
    req = M.reidentify.request_relink(sid, "clinician", "x", window_minutes=5)
    reid_id, share_a = req["id"], req["share_for_requester"]
    M.reidentify.submit_share(reid_id, "clinician", "clinician", share_a)
    share_b = M.reidentify.officer_pending("officer")[0]["share_for_officer"]

    # 时间窗推到过去后补第二片 → 直接过期
    conn = M.store.get_conn()
    conn.execute("UPDATE reidentifications SET window_end=? WHERE id=?",
                 ("2000-01-01T00:00:00+00:00", reid_id))
    with pytest.raises(M.store.ConflictError):
        M.reidentify.submit_share(reid_id, "officer", "officer", share_b)
    assert M.reidentify.get_relink(reid_id)["status"] == "expired"
    assert M.vault.get(reid_id) is None
    with pytest.raises(M.store.ConflictError):
        M.reidentify.export_relink(reid_id, "clinician")
