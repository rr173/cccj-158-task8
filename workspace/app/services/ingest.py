"""影像入库：冻结原始 DICOM 摘要、分配稳定替代身份、排队首版派生。

原始 DICOM 与冻结摘要均以加密信封 Blob 落盘；冻结后任何处理都不得回写它们。
入库按源文件 sha256 幂等：重复上传同字节影像直接返回既有病例。
"""
from __future__ import annotations

import json
import uuid

from ..core import crypto, dicomutil, store
from . import locks, versions as versions_svc


def _allocate_pseudo(conn, identity_fp: str) -> tuple[str, str]:
    row = conn.execute(
        "SELECT pseudo_patient_id, pseudo_patient_name FROM patient_pseudonyms"
        " WHERE identity_fp=?", (identity_fp,)).fetchone()
    if row:
        return row["pseudo_patient_id"], row["pseudo_patient_name"]
    n = conn.execute("SELECT COUNT(*) AS c FROM patient_pseudonyms").fetchone()["c"] + 1
    pseudo_id = f"PID-{n:06d}"
    pseudo_name = f"RESEARCH^SUBJ-{n:06d}"
    conn.execute(
        "INSERT INTO patient_pseudonyms(identity_fp,pseudo_patient_id,pseudo_patient_name,created_at)"
        " VALUES(?,?,?,?)",
        (identity_fp, pseudo_id, pseudo_name, store.now_iso()))
    return pseudo_id, pseudo_name


def ingest_study(data: bytes, actor: str, policy_id: str | None = None,
                 *, source_label: str = "upload", fault_inject: str | None = None) -> dict:
    summary = dicomutil.freeze_summary(data)
    identity = summary["identity"]
    fp = crypto.dicom_identity_fingerprint(identity)
    source_digest = crypto.sha256_hex(data)

    conn = store.get_conn()
    conn.execute("BEGIN IMMEDIATE")
    duplicate = None
    try:
        dup = conn.execute("SELECT id FROM studies WHERE source_sha256=?",
                           (source_digest,)).fetchone()
        if dup:
            duplicate = dup["id"]
        else:
            source_cas = store.sealed_blob_put("source_dicom", data, actor)
            summary_cas = store.sealed_blob_put(
                "frozen_summary", json.dumps(summary, sort_keys=True).encode(), actor)
            pseudo_id, pseudo_name = _allocate_pseudo(conn, fp)
            study_id = f"sty_{uuid.uuid4().hex[:12]}"
            conn.execute(
                "INSERT INTO studies(id,source_cas_id,source_sha256,"
                " frozen_summary_cas_id,identity_fp,"
                " pseudo_patient_id,pseudo_patient_name,created_by,created_at)"
                " VALUES(?,?,?,?,?,?,?,?,?)",
                (study_id, source_cas, source_digest, summary_cas, fp,
                 pseudo_id, pseudo_name, actor, store.now_iso()))
            store.audit(actor, "study.ingest", study_id=study_id, detail={
                "source_label": source_label,
                "source_sha256": source_digest,
                "frozen_summary_sha256": summary_cas,
                "burned_text_on_ingest": summary["banner_text_detected"],
                "elements_frozen": len(summary["elements"]),
            })
        conn.commit()
    except Exception:
        conn.rollback()
        raise

    if duplicate:
        return {"study_id": duplicate, "idempotent": True}

    created = versions_svc.create_version(
        study_id, actor, policy_id=policy_id,
        reason="initial de-identification", fault_inject=fault_inject)
    return {"study_id": study_id, "idempotent": False,
            "pseudo_patient_id": pseudo_id, "version": created}


def get_study(study_id: str) -> dict:
    r = store.one("SELECT * FROM studies WHERE id=?", (study_id,))
    return dict(r)


def frozen_summary(study_id: str) -> dict:
    r = store.one("SELECT frozen_summary_cas_id FROM studies WHERE id=?", (study_id,))
    return json.loads(store.sealed_blob_get(r["frozen_summary_cas_id"]))


def list_studies() -> list[dict]:
    return [dict(r) for r in store.all_rows(
        "SELECT id,pseudo_patient_id,pseudo_patient_name,status,latest_version_id,created_at"
        " FROM studies ORDER BY created_at")]
