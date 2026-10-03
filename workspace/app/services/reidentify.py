"""受控回链（临床纠错）。

授权生命周期：requested → armed → exported（立即终结）
                      ↘ revoked（任意时点终结）／expired（时间窗到期）

双角色授权（步骤绑定 reid_id，UNIQUE(reid_id, step_no) 防重复登记）：
  step1  clinician 提交分片 A（请求发起时返回给临床医生本人）
  step2  officer   提交分片 B（隐私官从待批准队列领取）
两片齐备且通过保险库指纹比对才 armed。授权严格绑定 study_id + purpose + 时间窗。

导出/撤销竞态：二者均为单条条件 UPDATE（期望单一前置状态），
只有一方赢；导出成功后内存分片立即抹除，撤销永远不能产生第二份导出。
导出物是包含原身份与纠错的加密信封 Blob（reidentified_bundle），同样 WORM。
"""
from __future__ import annotations

import hashlib
import json
import uuid
from datetime import datetime, timedelta

from cryptography.fernet import Fernet

from ..core import crypto, store
from . import vault as vault_mod


class ReidStateError(store.ConflictError):
    pass


def _fp(share: str) -> str:
    return hashlib.sha256(share.encode()).hexdigest()


def _row(reid_id: str):
    return store.one("SELECT * FROM reidentifications WHERE id=?", (reid_id,))


def request_relink(study_id: str, actor: str, purpose: str,
                   window_minutes: int = 30) -> dict:
    """临床医生发起回链：生成会话密钥信封（密文即入库）与双分片。

    分片 A 仅在本响应中交付临床医生；分片 B 由内存保险库托管给隐私官。
    未武装前数据库中只有密文，没有会话密钥的任何一部分。
    """
    study = store.one("SELECT * FROM studies WHERE id=?", (study_id,))
    if study["latest_version_id"] is None:
        raise ReidStateError("study has no published de-identified version")
    now = store.utcnow()
    window_end = now + timedelta(minutes=max(5, min(window_minutes, 240)))
    reid_id = f"reid_{uuid.uuid4().hex[:12]}"

    session_key = Fernet.generate_key()
    share_a, share_b = crypto.split_secret(session_key)
    inner = {"study_id": study_id, "purpose": purpose, "requested_by": actor,
             "created_at": now.isoformat()}
    token = crypto.seal_with_key(session_key, json.dumps(inner).encode()).decode()

    conn = store.get_conn()
    conn.execute(
        "INSERT INTO reidentifications(id,study_id,version_id,purpose,requested_by,"
        " window_start,window_end,status,envelope,share_a_fp,created_at)"
        " VALUES(?,?,?,?,?,?,?, 'requested', ?, ?, ?)",
        (reid_id, study_id, study["latest_version_id"], purpose, actor,
         now.isoformat(), window_end.isoformat(), token, _fp(share_a), now.isoformat()))
    vault_mod.vault.escrow(reid_id, None, share_b,
                           ttl_seconds=int((window_end - now).total_seconds()))
    store.audit(actor, "relink.request", study_id=study_id, reid_id=reid_id,
                detail={"purpose": purpose, "window_end": window_end.isoformat()})
    out = dict(_row(reid_id))
    out.pop("envelope", None)
    out["share_for_requester"] = share_a  # 仅本次响应中交付临床医生分片
    out["share_fingerprint"] = _fp(share_a)
    return out


def submit_share(reid_id: str, actor: str, role: str, share: str) -> dict:
    """登记一个授权分片。两角色分别在 step1/step2 提交，不可互相替代。"""
    row = _row(reid_id)
    entry = vault_mod.vault.get(reid_id)
    if entry is None:
        raise ReidStateError("authorization window expired or already terminated")

    conn = store.get_conn()
    conn.execute("BEGIN IMMEDIATE")
    try:
        cur = conn.execute("SELECT * FROM reidentifications WHERE id=?",
                           (reid_id,)).fetchone()
        if cur["status"] != "requested":
            conn.rollback()
            raise ReidStateError(f"cannot submit share in status {cur['status']}")

        if role == "clinician":
            step_no = 1
            if row["requested_by"] != actor:
                conn.rollback()
                raise ReidStateError("only the requesting clinician may present share A")
            if _fp(share) != cur["share_a_fp"]:
                conn.rollback(); _deny(actor, row, role)
                raise ReidStateError("share A fingerprint mismatch")
            try:
                conn.execute(
                    "INSERT INTO relinks(reid_id,step_no,role,actor,share_fp,created_at)"
                    " VALUES(?,?,?,?,?,?)",
                    (reid_id, 1, "clinician", actor, _fp(share), store.now_iso()))
            except Exception:
                conn.rollback()
                raise ReidStateError("step1 already recorded")
            vault_mod.vault.set_share_a(reid_id, share)
            conn.commit()
            store.audit(actor, "relink.step1", study_id=row["study_id"], reid_id=reid_id,
                        detail={"role": role})
            return _safe_row(reid_id)

        if role != "officer":
            conn.rollback()
            raise ReidStateError("role must be clinician or officer")
        step_no = 2
        if _fp(share) != _fp(entry.share_b):
            conn.rollback(); _deny(actor, row, role)
            raise ReidStateError("share B fingerprint mismatch")
        if entry.share_a is None:
            conn.rollback()
            raise ReidStateError("clinician share A not yet presented")
        if store.utcnow().isoformat() > row["window_end"]:
            conn.rollback()
            vault_mod.vault.purge(reid_id)
            conn.execute("UPDATE reidentifications SET status='expired' WHERE id=?",
                         (reid_id,))
            store.audit(actor, "relink.expired", study_id=row["study_id"], reid_id=reid_id,
                        detail={"at": "step2"})
            raise ReidStateError("authorization window has ended")
        try:
            conn.execute(
                "INSERT INTO relinks(reid_id,step_no,role,actor,share_fp,created_at)"
                " VALUES(?,?,?,?,?,?)",
                (reid_id, 2, "officer", actor, _fp(share), store.now_iso()))
        except Exception:
            conn.rollback()
            raise ReidStateError("step2 already recorded")
        # 重组会话密钥并就地校验：能解开信封才允许武装
        session_key = crypto.combine_shares(entry.share_a, entry.share_b)
        try:
            crypto.open_with_key(session_key, row["envelope"].encode())
        except ValueError:
            conn.rollback()
            raise ReidStateError("share recombination failed to open envelope")
        conn.execute(
            "UPDATE reidentifications SET share_b_fp=?, status='armed' WHERE id=?"
            " AND status='requested'", (_fp(share), reid_id))
        conn.commit()
        store.audit(actor, "relink.step2_armed", study_id=row["study_id"], reid_id=reid_id,
                    detail={"role": role})
        return _safe_row(reid_id)
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise


def _deny(actor: str, row, role: str) -> None:
    store.audit(actor, "relink.share_denied", study_id=row["study_id"],
                reid_id=row["id"], detail={"role": role, "reason": "fingerprint_mismatch"})


def _safe_row(reid_id: str) -> dict:
    out = dict(_row(reid_id))
    out.pop("envelope", None)
    return out


def _steps(reid_id: str) -> list[dict]:
    """授权步骤记录（仅角色与时间，分片指纹不在普通视图返回）。"""
    rows = store.all_rows(
        "SELECT step_no,role,created_at FROM relinks WHERE reid_id=? ORDER BY step_no",
        (reid_id,))
    return [{"step_no": r["step_no"], "role": r["role"], "created_at": r["created_at"]}
            for r in rows]


def status_view(reid_id: str, *, viewer: str, roles: tuple[str, ...]) -> dict:
    """工作台状态恢复视图：只含服务端真实状态，绝不含 envelope/分片/原身份。

    临床医生仅能查看自己发起的回链；隐私官/管理员可查看任意回链。
    """
    row = _row(reid_id)  # NotFound 由调用层处理
    if "clinician" in roles and "officer" not in roles and "admin" not in roles:
        if row["requested_by"] != viewer:
            raise store.NotFound(reid_id)
    out = _safe_row(reid_id)
    out["authorization_steps"] = _steps(reid_id)
    return out


def list_for_requester(clinician: str) -> list[dict]:
    """临床医生本人发起的全部回链（服务端恢复用，绝不携带身份内容）。"""
    rows = store.all_rows(
        "SELECT id FROM reidentifications WHERE requested_by=? ORDER BY created_at DESC",
        (clinician,))
    out = []
    for r in rows:
        d = _safe_row(r["id"])
        d["authorization_steps"] = _steps(r["id"])
        out.append(d)
    return out


def officer_pending(officer: str) -> list[dict]:
    """隐私官待批准队列：step1（临床医生）已登记、step2 未登记的回链。

    分片 B 仅存在于内存保险库中，只在本接口对隐私官角色返回。
    """
    rows = store.all_rows(
        "SELECT r.id FROM reidentifications r"
        " WHERE r.status='requested' AND EXISTS ("
        "   SELECT 1 FROM relinks l WHERE l.reid_id=r.id AND l.step_no=1)"
        " AND NOT EXISTS ("
        "   SELECT 1 FROM relinks l WHERE l.reid_id=r.id AND l.step_no=2)")
    out = []
    for r in rows:
        e = vault_mod.vault.get(r["id"])
        if e is None:
            continue
        d = _safe_row(r["id"])
        d["share_for_officer"] = e.share_b  # 仅授权角色调用本接口时返回
        d["authorization_steps"] = _steps(r["id"])
        out.append(d)
    return out


def _load_identity(study_id: str) -> dict:
    r = store.one("SELECT frozen_summary_cas_id FROM studies WHERE id=?", (study_id,))
    summary = json.loads(store.sealed_blob_get(r["frozen_summary_cas_id"]))
    return summary["identity"]


def _check_window(row) -> None:
    if store.utcnow() > datetime.fromisoformat(row["window_end"]):
        raise ReidStateError("authorization window has ended")


def submit_correction(reid_id: str, actor: str, corrected: dict) -> dict:
    """armed 窗口内登记临床纠错（仅保存纠错后的映射，信封加密）。"""
    row = _row(reid_id)
    if row["status"] != "armed":
        raise ReidStateError("authorization not active")
    _check_window(row)
    if actor != row["requested_by"]:
        raise ReidStateError("only the requesting clinician may record a correction")
    allowed = {"PatientID", "PatientName", "PatientBirthDate", "PatientSex"}
    clean = {k: str(v) for k, v in corrected.items() if k in allowed}
    conn = store.get_conn()
    conn.execute(
        "INSERT INTO identity_revisions(study_id,seq,reason,envelope,created_by,created_at)"
        " SELECT ?,COALESCE(MAX(seq),0)+1,'clinical_correction',?,?,?"
        " FROM identity_revisions WHERE study_id=?",
        (row["study_id"], crypto.seal_json(clean), actor, store.now_iso(), row["study_id"]))
    store.audit(actor, "relink.correction", study_id=row["study_id"], reid_id=reid_id,
                detail={"fields": sorted(clean.keys()), "values_logged": False})
    return dict(_row(reid_id))


def export_relink(reid_id: str, actor: str) -> dict:
    """导出回链结果。armed → exported 的条件 UPDATE 保证撤销/过期/重复导出互斥。

    导出成功后保险库分片立即抹除；非 armed 状态一律拒绝，绝不泄露身份。
    """
    conn = store.get_conn()
    conn.execute("BEGIN IMMEDIATE")
    try:
        row = conn.execute("SELECT * FROM reidentifications WHERE id=?",
                           (reid_id,)).fetchone()
        if row is None:
            conn.rollback(); raise store.NotFound(reid_id)
        if actor != row["requested_by"]:
            conn.rollback(); raise ReidStateError("only the requester may export")
        if row["status"] != "armed":
            conn.rollback()
            raise ReidStateError(f"export refused in status {row['status']}")
        if store.utcnow().isoformat() > row["window_end"]:
            conn.execute("UPDATE reidentifications SET status='expired' WHERE id=?",
                         (reid_id,))
            conn.commit()
            vault_mod.vault.purge(reid_id)
            store.audit(actor, "relink.expired", study_id=row["study_id"], reid_id=reid_id,
                        detail={"at": "export_attempt"})
            raise ReidStateError("authorization window has ended")

        # 双分片必须仍在保险库中（已导出/撤销会被抹除），重组会话密钥
        entry = vault_mod.vault.get(reid_id)
        if entry is None or entry.share_a is None:
            conn.rollback()
            raise ReidStateError("authorization shares are no longer held")
        try:
            session_key = crypto.combine_shares(entry.share_a, entry.share_b)
            crypto.open_with_key(session_key, row["envelope"].encode())
        except ValueError:
            conn.rollback()
            raise ReidStateError("share recombination failed")

        identity = _load_identity(row["study_id"])
        study = conn.execute("SELECT * FROM studies WHERE id=?",
                             (row["study_id"],)).fetchone()
        corr = conn.execute(
            "SELECT envelope FROM identity_revisions WHERE study_id=? AND reason='"
            "clinical_correction' ORDER BY seq DESC LIMIT 1", (row["study_id"],)).fetchone()
        corrected = crypto.open_json(corr["envelope"]) if corr else None

        bundle = {
            "reid_id": reid_id,
            "study_id": row["study_id"],
            "purpose": row["purpose"],
            "exported_by": actor,
            "exported_at": store.now_iso(),
            "window_end": row["window_end"],
            "original_identity": identity,
            "corrected_identity": corrected,
            "pseudo_patient_id": study["pseudo_patient_id"],
        }
        blob = store.sealed_blob_put(
            "reidentified_bundle", json.dumps(bundle, ensure_ascii=False).encode(), actor)
        cur = conn.execute(
            "UPDATE reidentifications SET status='exported', export_cas_id=?,"
            " exported_at=? WHERE id=? AND status='armed'",
            (blob, store.now_iso(), reid_id))
        if cur.rowcount != 1:
            # 撤销方抢先：回滚，绝不产出导出
            conn.rollback()
            vault_mod.vault.purge(reid_id)
            raise ReidStateError("lost export race: authorization terminated")
        store.audit(actor, "relink.export", study_id=row["study_id"], reid_id=reid_id,
                    detail={"export_cas_id": blob, "had_correction": corrected is not None,
                            "bundle_fields": sorted(bundle.keys())})
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    # 事务提交后立即抹除内存分片：导出即失效
    vault_mod.vault.purge(reid_id)
    return {"reid_id": reid_id, "status": "exported", "export_cas_id": blob,
            "bundle": bundle}


def revoke_relink(reid_id: str, actor: str, reason: str) -> dict:
    """撤销：对任何未终结状态生效，与导出竞争时只有一方胜出。"""
    conn = store.get_conn()
    conn.execute("BEGIN IMMEDIATE")
    try:
        row = conn.execute("SELECT status FROM reidentifications WHERE id=?",
                           (reid_id,)).fetchone()
        if row is None:
            conn.rollback(); raise store.NotFound(reid_id)
        cur = conn.execute(
            "UPDATE reidentifications SET status='revoked', revoke_reason=?"
            " WHERE id=? AND status IN ('requested','armed')",
            (reason, reid_id))
        won = cur.rowcount == 1
        if won:
            store.audit(actor, "relink.revoke", reid_id=reid_id,
                        detail={"reason": reason, "previous_status": row["status"]})
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    vault_mod.vault.purge(reid_id)  # 无论输赢都清理本进程分片
    return {"reid_id": reid_id, "revoked": won, "status": _row(reid_id)["status"]}


def expire_if_due(reid_id: str, *, actor: str = "system") -> str | None:
    row = _row(reid_id)
    if row["status"] in ("requested", "armed") and store.utcnow().isoformat() > row["window_end"]:
        conn = store.get_conn()
        cur = conn.execute(
            "UPDATE reidentifications SET status='expired' WHERE id=?"
            " AND status IN ('requested','armed') AND window_end<?",
            (reid_id, store.now_iso()))
        vault_mod.vault.purge(reid_id)
        if cur.rowcount == 1:
            store.audit(actor, "relink.expired", study_id=row["study_id"], reid_id=reid_id,
                        detail={"at": "sweep"})
            return "expired"
    return None


def sweep_expired(actor: str = "system") -> list[str]:
    rows = store.all_rows(
        "SELECT id FROM reidentifications WHERE status IN ('requested','armed')")
    out = []
    for r in rows:
        if expire_if_due(r["id"], actor=actor):
            out.append(r["id"])
    for rid in vault_mod.vault.purge_expired():
        out.append(rid + "(vault-ttl)")
    return out


def list_relinks(status: str | None = None, study_id: str | None = None) -> list[dict]:
    q = ("SELECT id,study_id,version_id,purpose,requested_by,window_start,window_end,"
         " status,export_cas_id,exported_at,revoke_reason,created_at,"
         " share_a_fp,share_b_fp FROM reidentifications WHERE 1=1")
    args: list = []
    if status:
        q += " AND status=?"; args.append(status)
    if study_id:
        q += " AND study_id=?"; args.append(study_id)
    q += " ORDER BY created_at DESC"
    return [dict(r) for r in store.all_rows(q, tuple(args))]


def get_relink(reid_id: str) -> dict:
    return _safe_row(reid_id)
