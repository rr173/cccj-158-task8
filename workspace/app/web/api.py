"""平台 REST API。

角色：
  technician  入库、重试、人工质检
  clinician   发起回链、提交分片 A、纠错、导出
  officer     双角色第二授权人、撤销
  admin       策略登记、审计导出
"""
from __future__ import annotations

import io
import json
import re
from pathlib import Path
from typing import Optional

from fastapi import Depends, FastAPI, HTTPException
from fastapi.responses import FileResponse, Response
from pydantic import BaseModel, Field

from ..core import dicomutil, store
from ..services import (audit as audit_svc, ingest as ingest_svc,
                        policies as policies_svc, queue, reidentify,
                        snapshots as snap_svc, versions as versions_svc)
from .auth import authenticate, require_role

app = FastAPI(title="医学影像去标识化与受控回链平台", version="1.0.0")

_WEB_DIR = Path(__file__).resolve().parent / "static"
# 工作台静态资源白名单：不做目录列举、不允许路径穿越
_STATIC_FILES = {"index.html", "app.js", "styles.css"}
_SAFE_STATIC = re.compile(r"^[a-z0-9_.-]+$")


@app.middleware("http")
async def _no_store(request, call_next):
    # 所有页面/接口数据一律不缓存：刷新与角色切换后只能重新从服务端恢复状态
    resp = await call_next(request)
    resp.headers["Cache-Control"] = "no-store"
    return resp


@app.on_event("startup")
def _startup():
    store.init_db()
    # 默认项目策略 v1（不存在时播种）
    try:
        policies_svc.latest_policy("project-default")
    except store.NotFound:
        policies_svc.register_policy(
            "project-default", dicomutil.default_policy("strict"), "system")


@app.get("/health")
def health():
    return {"status": "ok"}


# ---------- 工作台（服务端渲染的纯静态 SPA；状态一律从下列 API 恢复） ----------

@app.get("/", include_in_schema=False)
def workbench_root():
    return FileResponse(_WEB_DIR / "index.html", media_type="text/html")


@app.get("/ui/{name}", include_in_schema=False)
def workbench_asset(name: str):
    if name not in _STATIC_FILES or not _SAFE_STATIC.match(name):
        raise HTTPException(status_code=404)
    target = (_WEB_DIR / name).resolve()
    if _WEB_DIR.resolve() not in target.parents or not target.is_file():
        raise HTTPException(status_code=404)
    media = {"index.html": "text/html", "app.js": "application/javascript",
            "styles.css": "text/css"}[name]
    return FileResponse(target, media_type=media)


@app.get("/api/me", tags=["session"])
def whoami(user=Depends(authenticate)):
    """当前登录会话的真实身份与角色；工作台据此渲染，绝不信任前端自报角色。"""
    return {"username": user["username"], "roles": user["roles"]}


# ---------- 模型 ----------

class IngestBody(BaseModel):
    patient_id: str = "MRN-48213"
    patient_name: str = "ZHANG^WEI"
    accession: str = "ACC-77881"
    policy_id: Optional[str] = None
    fault_inject: Optional[str] = Field(
        None, description="redaction=遮盖子系统异常; qc_leak=跳过遮盖以验证质检门")


class PolicyBody(BaseModel):
    name: str = "project-default"
    level: str = "strict"
    content: Optional[dict] = None
    version: Optional[int] = None


class RequeueBody(BaseModel):
    job_id: str
    clear_fault: bool = True


class QCBody(BaseModel):
    version_id: str
    approve: bool
    note: str = ""


class DeriveBody(BaseModel):
    study_id: str
    policy_id: Optional[str] = None
    reason: str = "policy upgrade re-derivation"


class RelinkBody(BaseModel):
    study_id: str
    purpose: str = "clinical correction 2026-09"
    window_minutes: int = 30


class ShareBody(BaseModel):
    share: str


class CorrectionBody(BaseModel):
    corrected: dict


class RevokeBody(BaseModel):
    reason: str = "privacy officer revocation"


class SnapshotBody(BaseModel):
    idempotency_key: str = Field(..., description="幂等键：同键重发返回原快照")
    actor: Optional[str] = None
    action: Optional[str] = None
    study_id: Optional[str] = None
    reid_id: Optional[str] = None
    limit: Optional[int] = Field(None, ge=1, le=100_000)


def _conflict(exc: Exception):
    raise HTTPException(status_code=409, detail=str(exc))


# ---------- 策略 ----------

@app.post("/api/policies", tags=["policies"])
def create_policy(body: PolicyBody, user=Depends(require_role("admin"))):
    content = body.content or dicomutil.default_policy(body.level)
    return policies_svc.register_policy(body.name, content, user["username"],
                                        version=body.version)


@app.get("/api/policies", tags=["policies"])
def list_policies(user=Depends(require_role("technician", "clinician", "officer", "admin"))):
    return policies_svc.list_policies()


# ---------- 入库与队列 ----------

@app.post("/api/studies/ingest-synthetic", tags=["studies"])
def ingest_synthetic(body: IngestBody, user=Depends(require_role("technician"))):
    data = dicomutil.build_synthetic_dicom(
        patient_id=body.patient_id, patient_name=body.patient_name,
        accession=body.accession)
    try:
        return ingest_svc.ingest_study(
            data, user["username"], body.policy_id,
            fault_inject=body.fault_inject)
    except store.ConflictError as exc:
        _conflict(exc)


@app.post("/api/studies/ingest", tags=["studies"])
def ingest_upload(file: bytes = b"", user=Depends(require_role("technician")),
                  x_policy_id: Optional[str] = None,
                  x_fault_inject: Optional[str] = None):
    if not file:
        raise HTTPException(status_code=400, detail="empty DICOM payload")
    return ingest_svc.ingest_study(file, user["username"], x_policy_id,
                                   fault_inject=x_fault_inject)


@app.get("/api/studies", tags=["studies"])
def list_studies(user=Depends(require_role("technician", "clinician", "officer", "admin"))):
    return ingest_svc.list_studies()


@app.get("/api/studies/{study_id}", tags=["studies"])
def get_study(study_id: str, user=Depends(require_role("technician", "clinician", "admin"))):
    try:
        study = ingest_svc.get_study(study_id)
    except store.NotFound:
        raise HTTPException(404, "study not found")
    study.pop("identity_fp", None)  # 不向前端返回身份指纹以外的原身份
    return {"study": study, "versions": versions_svc.list_versions(study_id)}


@app.get("/api/studies/{study_id}/frozen-summary", tags=["studies"])
def frozen_summary(study_id: str, user=Depends(require_role("technician", "admin"))):
    try:
        summary = ingest_svc.frozen_summary(study_id)
    except store.NotFound:
        raise HTTPException(404, "study not found")
    summary.get("identity", {}).clear()  # 冻结摘要可查结构，但不通过普通接口暴露原身份
    return summary


@app.post("/api/versions/derive", tags=["versions"])
def derive(body: DeriveBody, user=Depends(require_role("technician"))):
    try:
        return versions_svc.create_version(
            body.study_id, user["username"], policy_id=body.policy_id, reason=body.reason)
    except store.ConflictError as exc:
        _conflict(exc)
    except store.NotFound:
        raise HTTPException(404, "study not found")


# ---------- 处理队列 / Worker ----------

@app.post("/api/queue/run", tags=["queue"])
def run_worker(cycles: int = 1, ignore_backoff: bool = False,
               user=Depends(require_role("technician", "admin"))):
    results = []
    for _ in range(max(1, cycles)):
        r = queue.run_once(ignore_backoff=ignore_backoff)
        if r is None:
            break
        results.append(r)
    return {"processed": results}


@app.get("/api/queue/jobs", tags=["queue"])
def get_jobs(status: Optional[str] = None,
             user=Depends(require_role("technician", "admin"))):
    return queue.list_jobs(status)


@app.post("/api/queue/requeue", tags=["queue"])
def requeue(body: RequeueBody, user=Depends(require_role("technician"))):
    try:
        return queue.requeue(body.job_id, user["username"],
                             payload_override={} if body.clear_fault else None)
    except store.ConflictError as exc:
        _conflict(exc)
    except store.NotFound:
        raise HTTPException(404, "job not found")


# ---------- 质检界面 ----------

@app.get("/api/qc/queue", tags=["qc"])
def qc_queue(status: Optional[str] = None,
             user=Depends(require_role("technician", "admin"))):
    return versions_svc.list_qc_queue(status)


@app.get("/api/versions/{version_id}", tags=["qc"])
def version_detail(version_id: str, user=Depends(require_role("technician", "admin"))):
    try:
        return versions_svc.get_version(version_id)
    except store.NotFound:
        raise HTTPException(404, "version not found")


@app.get("/api/versions/{version_id}/preview", tags=["qc"])
def version_preview(version_id: str, user=Depends(require_role("technician", "admin"))):
    ver = versions_svc.get_version(version_id)
    if not ver.get("derived_cas_id"):
        raise HTTPException(409, "no derived artifact yet")
    data = store.sealed_blob_get(ver["derived_cas_id"])
    return Response(content=dicomutil.render_preview_png(data), media_type="image/png")


@app.post("/api/qc/review", tags=["qc"])
def qc_review(body: QCBody, user=Depends(require_role("technician"))):
    try:
        return versions_svc.manual_qc(body.version_id, user["username"],
                                      body.approve, body.note)
    except store.ConflictError as exc:
        _conflict(exc)
    except store.NotFound:
        raise HTTPException(404, "version not found")


# ---------- 受控回链 ----------

@app.post("/api/relink/request", tags=["relink"])
def relink_request(body: RelinkBody, user=Depends(require_role("clinician"))):
    try:
        return reidentify.request_relink(body.study_id, user["username"],
                                         body.purpose, body.window_minutes)
    except store.ConflictError as exc:
        _conflict(exc)
    except store.NotFound:
        raise HTTPException(404, "study not found")


@app.post("/api/relink/{reid_id}/share-a", tags=["relink"])
def relink_share_a(reid_id: str, body: ShareBody,
                   user=Depends(require_role("clinician"))):
    try:
        return reidentify.submit_share(reid_id, user["username"], "clinician", body.share)
    except store.ConflictError as exc:
        _conflict(exc)
    except store.NotFound:
        raise HTTPException(404, "reidentification not found")


@app.get("/api/relink/officer/pending", tags=["relink"])
def officer_pending(user=Depends(require_role("officer"))):
    return reidentify.officer_pending(user["username"])


@app.get("/api/relink/mine", tags=["relink"])
def relink_mine(user=Depends(require_role("clinician"))):
    """临床医生工作台：从服务端恢复本人发起的全部回链真实状态。"""
    return reidentify.list_for_requester(user["username"])


@app.get("/api/relink/{reid_id}/status", tags=["relink"])
def relink_status(reid_id: str, user=Depends(
        require_role("clinician", "officer", "admin"))):
    """单条回链的确定性状态恢复（含双角色授权步骤），不含任何身份内容。"""
    try:
        return reidentify.status_view(
            reid_id, viewer=user["username"], roles=tuple(user["roles"]))
    except store.NotFound:
        # 越权查看他人回链与不存在回链返回同样结果，不泄露存在性
        raise HTTPException(404, "reidentification not found")


@app.post("/api/relink/{reid_id}/share-b", tags=["relink"])
def relink_share_b(reid_id: str, body: ShareBody,
                   user=Depends(require_role("officer"))):
    try:
        return reidentify.submit_share(reid_id, user["username"], "officer", body.share)
    except store.ConflictError as exc:
        _conflict(exc)
    except store.NotFound:
        raise HTTPException(404, "reidentification not found")


@app.post("/api/relink/{reid_id}/correction", tags=["relink"])
def relink_correction(reid_id: str, body: CorrectionBody,
                      user=Depends(require_role("clinician"))):
    try:
        return reidentify.submit_correction(
            reid_id, user["username"], body.corrected)
    except store.ConflictError as exc:
        _conflict(exc)
    except store.NotFound:
        raise HTTPException(404, "reidentification not found")


@app.post("/api/relink/{reid_id}/export", tags=["relink"])
def relink_export(reid_id: str, user=Depends(require_role("clinician"))):
    try:
        return reidentify.export_relink(reid_id, user["username"])
    except store.ConflictError as exc:
        _conflict(exc)
    except store.NotFound:
        raise HTTPException(404, "reidentification not found")


@app.post("/api/relink/{reid_id}/revoke", tags=["relink"])
def relink_revoke(reid_id: str, body: RevokeBody,
                  user=Depends(require_role("officer", "admin"))):
    try:
        result = reidentify.revoke_relink(reid_id, user["username"], body.reason)
    except store.NotFound:
        raise HTTPException(404, "reidentification not found")
    if not result["revoked"] and result["status"] in ("exported", "revoked", "expired"):
        # 竞态落败或已终结：返回 409，不产生误导
        raise HTTPException(status_code=409,
                            detail=f"revocation did not take effect: {result['status']}")
    return result


@app.post("/api/relink/sweep", tags=["relink"])
def relink_sweep(user=Depends(require_role("officer", "admin"))):
    return {"expired": reidentify.sweep_expired(user["username"])}


@app.get("/api/relink", tags=["relink"])
def relink_list(status: Optional[str] = None, study_id: Optional[str] = None,
                user=Depends(require_role("officer", "admin"))):
    return reidentify.list_relinks(status=status, study_id=study_id)


# ---------- 审计导出 ----------

@app.get("/api/audit", tags=["audit"])
def audit_list(actor: Optional[str] = None, action: Optional[str] = None,
               study_id: Optional[str] = None, reid_id: Optional[str] = None,
               user=Depends(require_role("admin", "officer"))):
    return audit_svc.query(actor=actor, action=action, study_id=study_id,
                           reid_id=reid_id)


@app.get("/api/audit/verify", tags=["audit"])
def audit_verify(user=Depends(require_role("admin", "officer"))):
    return store.verify_audit_chain()


@app.post("/api/audit/export", tags=["audit"])
def audit_export(fmt: str = "json", user=Depends(require_role("admin"))):
    if fmt == "csv":
        return audit_svc.export_csv(user["username"])
    return audit_svc.export_jsonl(user["username"])


@app.get("/api/blobs/{cas_id}", tags=["audit"])
def download_blob(cas_id: str, user=Depends(require_role("admin"))):
    # 验真包只能走带验真的专用下载接口，通用 blob 接口不提供该类证据包
    row = store.maybe("SELECT kind FROM objects WHERE cas_id=?", (cas_id,))
    if row is not None and row["kind"] == "audit_verify_pack":
        raise HTTPException(status_code=404, detail="use verified pack endpoint")
    try:
        data = store.blob_get(cas_id)
    except store.NotFound:
        raise HTTPException(404, "blob not found")
    return Response(content=data, media_type="application/octet-stream")


# ---------- 验真快照（admin 冻结/下载；officer 查询/复核） ----------

def _snap_view(snap: dict, *, replay: bool = False) -> dict:
    """对外视图：元数据 + 现场重算的完整性结果；不内联包文件内容。"""
    verification = snap_svc.verify_snapshot(snap)
    out = dict(snap)
    out["idempotent_replay"] = bool(replay or snap.get("idempotent_replay"))
    out["verification"] = verification
    return out


@app.post("/api/audit/snapshots", tags=["audit-snapshots"])
def create_snapshot(body: SnapshotBody, user=Depends(require_role("admin"))):
    """按筛选条件与当前审计序号在服务端确定截止点冻结不可变快照。"""
    filters = {"actor": body.actor, "action": body.action,
               "study_id": body.study_id, "reid_id": body.reid_id,
               "limit": body.limit}
    try:
        snap = snap_svc.create_snapshot(
            user["username"], body.idempotency_key, filters)
    except snap_svc.ChainBroken as exc:
        # 原链已被篡改：拒绝冻结，返回可核对的确定状态
        raise HTTPException(status_code=409, detail={
            "error": "audit_chain_broken", "broken_at": exc.broken_at,
            "chain_rows": exc.count,
            "message": "refuse to freeze a tampered audit chain"})
    except snap_svc.SnapshotError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    return _snap_view(snap)


@app.get("/api/audit/snapshots", tags=["audit-snapshots"])
def list_snapshots(user=Depends(require_role("admin", "officer"))):
    """隐私官可查询全部快照的范围/链锚/记录数/现场完整性结果（复核用）。"""
    return [_snap_view(s) for s in snap_svc.list_snapshots()]


@app.get("/api/audit/snapshots/{snapshot_id}", tags=["audit-snapshots"])
def get_snapshot(snapshot_id: str,
                 user=Depends(require_role("admin", "officer"))):
    try:
        snap = snap_svc.get_snapshot(snapshot_id)
    except snap_svc.SnapshotNotFound:
        raise HTTPException(404, "snapshot not found")
    return _snap_view(snap)


@app.get("/api/audit/snapshots/{snapshot_id}/pack", tags=["audit-snapshots"])
def download_snapshot_pack(snapshot_id: str,
                           user=Depends(require_role("admin"))):
    """下载包含清单与记录的验真包；交付前强制验真，被改动则确定性拒绝。"""
    try:
        snap = snap_svc.get_snapshot(snapshot_id)
        pack = snap_svc.get_verified_pack(snap)
    except snap_svc.SnapshotNotFound:
        raise HTTPException(404, "snapshot not found")
    except snap_svc.PackTampered as exc:
        raise HTTPException(status_code=409, detail={
            "error": "verify_pack_tampered", "snapshot_id": snapshot_id,
            "message": str(exc)})
    store.audit(user["username"], "audit.snapshot.download",
                detail={"snapshot_id": snapshot_id,
                        "pack_cas_id": snap["pack_cas_id"]})
    headers = {"Content-Disposition":
               f'attachment; filename="audit-verify-{snapshot_id}.zip"'}
    return Response(content=pack, media_type="application/zip",
                    headers=headers)
