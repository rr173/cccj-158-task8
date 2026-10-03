"""审计验真快照：在确定截止点冻结筛选结果，生成可独立核对的验真包。

不变量：
- 创建必须在单个 IMMEDIATE 事务内完成：确定 cutoff（当时链尾 seq）→ 校验
  全链完整 → 按截止点取数 → 冻结落包，事务提交前并发写入的审计事件会被
  BEGIN IMMEDIATE 阻塞，绝不混入已冻结结果。
- 幂等键唯一（DB UNIQUE 约束）：同一幂等键再次请求返回原快照；同键不同
  参数返回确定性冲突。
- 快照行与 WORM 验真包一经创建永不更新；清单（manifest）对包内全部文件
  与全部记录逐级摘要，包被改动后 verify 给出确定性断裂点（不含任何身份
  内容——审计行本身就只有既有审计接口允许查询的字段）。
- 验真包不做信封加密：它是"拿去外部核对"的证据包，内容与既有审计查询
  接口可见范围一致（无原身份），下载仅限 admin。
"""
from __future__ import annotations

import io
import json
import re
import zipfile
import uuid

from ..core import crypto, store

# 验真包格式版本；包结构不兼容变化时升版
PACK_FORMAT = "audit-verify-pack/v1"
FILTER_KEYS = ("actor", "action", "study_id", "reid_id")
_IDEMPOTENCY_RE = re.compile(r"^[A-Za-z0-9._:-]{1,128}$")


class SnapshotError(Exception):
    """快照参数/状态错误（确定性，文案不含身份内容）。"""


class ChainBroken(SnapshotError):
    """创建时原链已被篡改：拒绝冻结一条不可信的链。"""

    def __init__(self, broken_at: int | None, count: int):
        super().__init__(
            f"audit chain verification failed at seq {broken_at} (chain rows: {count})")
        self.broken_at = broken_at
        self.count = count


class SnapshotNotFound(SnapshotError):
    pass


# ---------- 规范化与摘要原语 ----------

def _norm_filters(filters: dict | None) -> dict:
    """只承认既有审计查询的四个筛选字段；空串归一化为不存在（"筛选为空"）。"""
    out: dict = {}
    for k in FILTER_KEYS:
        v = (filters or {}).get(k)
        if v is None:
            continue
        v = str(v).strip()
        if v:
            out[k] = v
    limit = (filters or {}).get("limit")
    if limit is not None:
        try:
            limit = int(limit)
        except (TypeError, ValueError):
            raise SnapshotError("limit must be an integer")
        if not 1 <= limit <= 100_000:
            raise SnapshotError("limit out of range (1..100000)")
        out["limit"] = limit
    return out


def _canonical(obj) -> bytes:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":")).encode()


def _row_canonical(r) -> bytes:
    """与 store.verify_audit_chain 完全一致的行规范化（含原始 detail_json）。"""
    return json.dumps(
        [r["ts"], r["actor"], r["action"], r["study_id"], r["reid_id"],
         r["detail_json"]],
        sort_keys=True, separators=(",", ":")).encode()


def _record_public(r) -> dict:
    """验真包中的记录：与既有 /api/audit 查询字段一致（无新增敏感面）。"""
    return {"seq": r["seq"], "ts": r["ts"], "actor": r["actor"],
            "action": r["action"], "study_id": r["study_id"],
            "reid_id": r["reid_id"], "detail": json.loads(r["detail_json"]),
            "prev_mac": r["prev_mac"], "mac": r["mac"]}


def _record_digest(rec: dict) -> str:
    return crypto.sha256_hex(_canonical(rec))


# ---------- 验真包构建 ----------

def _build_pack(snapshot: dict, records: list[dict]) -> tuple[bytes, str, str]:
    """组装验真包（ZIP 内清单+记录，确定性字节序），返回 (包字节, manifest_hash, records_hash)。

    records.jsonl 每条附带在清单中的 sha256，可被独立核对；
    manifest.json 记录 scope/anchor/逐记录摘要/文件摘要；
    manifest 自身再做一次 sha256（与包的 CAS 摘要一致，双重锚定）。
    """
    entries = [{"seq": r["seq"], "sha256": _record_digest(r)} for r in records]
    records_lines = [json.dumps(r, ensure_ascii=False, sort_keys=True)
                     for r in records]
    records_bytes = ("\n".join(records_lines) + ("\n" if records_lines else "")).encode()
    records_sha = crypto.sha256_hex(records_bytes)

    manifest = {
        "format": PACK_FORMAT,
        "snapshot_id": snapshot["id"],
        "idempotency_key": snapshot["idempotency_key"],
        "created_at": snapshot["created_at"],
        "created_by": snapshot["created_by"],
        "scope": {
            "filters": snapshot["filters"],
            "cutoff_seq": snapshot["cutoff_seq"],
            "cutoff_ts": snapshot["cutoff_ts"],
            "first_seq": snapshot["first_seq"],
            "last_seq": snapshot["last_seq"],
            "record_count": snapshot["record_count"],
        },
        "anchor": {"seq": snapshot["cutoff_seq"], "ts": snapshot["cutoff_ts"],
                   "mac": snapshot["anchor_mac"]},
        "records_root": {"alg": "sha256/records.jsonl", "sha256": records_sha,
                         "count": len(records)},
        "records": entries,
        "created_integrity": snapshot["created_integrity"],
    }
    manifest_bytes = json.dumps(manifest, ensure_ascii=False, indent=2,
                                sort_keys=True).encode()
    manifest_sha = crypto.sha256_hex(manifest_bytes)

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("manifest.json", manifest_bytes)
        zf.writestr("records.jsonl", records_bytes)
        zf.writestr("README.txt", (
            f"{PACK_FORMAT}\n"
            f"snapshot: {snapshot['id']}\n"
            "Verify: manifest.records[].sha256 == sha256(records.jsonl line); "
            "records_root.sha256 == sha256(records.jsonl); "
            "rebuild each mac = HMAC(prev_mac, canonical(row)) from prev_mac=GENESIS.\n"
            "This pack contains audit metadata only; no original identity content.\n"
        ).encode())
    return buf.getvalue(), manifest_sha, records_sha


# ---------- 截止点取数（必须在 IMMEDIATE 事务内调用） ----------

def _freeze(conn, actor: str, idem_key: str, filters: dict) -> dict:
    # 1) 幂等：同键直接返回原快照（永不复用既有事务，仅只读查询）
    existing = conn.execute(
        "SELECT * FROM audit_snapshots WHERE idempotency_key=?",
        (idem_key,)).fetchone()
    if existing is not None:
        snap = _snapshot_row_to_dict(existing)
        if snap["filters"] != filters:
            raise SnapshotError(
                f"idempotency key already used by snapshot {snap['id']} "
                "with different filters")
        snap["idempotent_replay"] = True
        return snap

    # 2) 确定截止点：当时链尾即 cutoff；全链必须完整才允许冻结
    head = conn.execute(
        "SELECT seq, ts, mac FROM audit_log ORDER BY seq DESC LIMIT 1").fetchone()
    chain = store.verify_audit_chain()
    if not chain["ok"]:
        raise ChainBroken(chain["broken_at"], chain["count"])

    q = "SELECT * FROM audit_log WHERE seq <= ?"
    args: list = [head["seq"] if head else 0]
    for k in FILTER_KEYS:
        if k in filters:
            q += f" AND {k}=?"; args.append(filters[k])
    q += " ORDER BY seq ASC LIMIT ?"
    args.append(filters.get("limit", 100_000))
    rows = conn.execute(q, tuple(args)).fetchall()

    records = [_record_public(r) for r in rows]
    now = store.now_iso()
    snap_id = f"snp_{uuid.uuid4().hex[:16]}"
    snap = {
        "id": snap_id,
        "idempotency_key": idem_key,
        "created_at": now,
        "created_by": actor,
        "filters": filters,
        "cutoff_seq": head["seq"] if head else 0,
        "cutoff_ts": head["ts"] if head else None,
        "anchor_mac": head["mac"] if head else "GENESIS",
        "record_count": len(records),
        "first_seq": records[0]["seq"] if records else None,
        "last_seq": records[-1]["seq"] if records else None,
    }
    # 创建时冻结的完整性结论（重验结果在 verify 中独立得出）
    snap["created_integrity"] = {
        "chain_ok_at_cutoff": True,
        "chain_count_at_cutoff": chain["count"],
        "anchor_seq": snap["cutoff_seq"],
        "record_count": len(records),
    }

    # 3) 冻结落 WORM（内容寻址，0444）；清单摘要入库
    pack_bytes, manifest_sha, records_sha = _build_pack(snap, records)
    cas = store.blob_put("audit_verify_pack", pack_bytes, actor)
    snap["pack_cas_id"] = cas
    snap["pack_size"] = len(pack_bytes)
    snap["manifest_sha256"] = manifest_sha
    snap["records_sha256"] = records_sha

    # 4) 快照行与本次操作的审计事件在同一事务落盘
    conn.execute(
        "INSERT INTO audit_snapshots(id,idempotency_key,created_at,created_by,"
        "filters_json,cutoff_seq,cutoff_ts,anchor_mac,record_count,first_seq,"
        "last_seq,pack_cas_id,pack_size,manifest_sha256,records_sha256,"
        "frozen_result_json) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (snap["id"], snap["idempotency_key"], snap["created_at"], snap["created_by"],
         json.dumps(snap["filters"], sort_keys=True), snap["cutoff_seq"],
         snap["cutoff_ts"], snap["anchor_mac"], snap["record_count"],
         snap["first_seq"], snap["last_seq"], snap["pack_cas_id"],
         snap["pack_size"], snap["manifest_sha256"], snap["records_sha256"],
         json.dumps(snap["created_integrity"], sort_keys=True)))
    store.audit(
        actor, "audit.snapshot.create",
        detail={"snapshot_id": snap["id"], "cutoff_seq": snap["cutoff_seq"],
                "record_count": snap["record_count"], "pack_cas_id": cas,
                "filters": snap["filters"]})
    return snap


def create_snapshot(actor: str, idem_key: str, filters: dict | None = None) -> dict:
    """在服务端确定的截止点冻结验真快照（幂等键决定是否复用）。"""
    if not idem_key or not _IDEMPOTENCY_RE.match(str(idem_key)):
        raise SnapshotError(
            "idempotency_key required (1-128 chars: letters, digits, . _ : -)")
    filters = _norm_filters(filters)
    conn = store.get_conn()
    # IMMEDIATE 立即取写锁：与审计写入互斥，保证取数截止点确定且不混入新事件
    conn.execute("BEGIN IMMEDIATE")
    try:
        snap = _freeze(conn, actor, str(idem_key), filters)
        replay = snap.pop("idempotent_replay", False)
        conn.commit()
        snap["idempotent_replay"] = replay
        return snap
    except Exception:
        conn.rollback()
        raise


# ---------- 读取 ----------

def _snapshot_row_to_dict(r) -> dict:
    return {"id": r["id"], "idempotency_key": r["idempotency_key"],
            "created_at": r["created_at"], "created_by": r["created_by"],
            "filters": json.loads(r["filters_json"]),
            "cutoff_seq": r["cutoff_seq"], "cutoff_ts": r["cutoff_ts"],
            "anchor_mac": r["anchor_mac"], "record_count": r["record_count"],
            "first_seq": r["first_seq"], "last_seq": r["last_seq"],
            "pack_cas_id": r["pack_cas_id"], "pack_size": r["pack_size"],
            "manifest_sha256": r["manifest_sha256"],
            "records_sha256": r["records_sha256"],
            "created_integrity": json.loads(r["frozen_result_json"])}


def list_snapshots(limit: int = 200) -> list[dict]:
    rows = store.all_rows(
        "SELECT * FROM audit_snapshots ORDER BY created_at DESC, id DESC LIMIT ?",
        (max(1, min(int(limit), 1000)),))
    return [_snapshot_row_to_dict(r) for r in rows]


def get_snapshot(snapshot_id: str) -> dict:
    try:
        r = store.one("SELECT * FROM audit_snapshots WHERE id=?", (snapshot_id,))
    except store.NotFound:
        raise SnapshotNotFound(f"snapshot {snapshot_id} not found")
    return _snapshot_row_to_dict(r)


# ---------- 验真包解包与验真 ----------

class PackTampered(SnapshotError):
    """验真包内容被改动（ZIP/文件/清单/记录任一摘要不符）。"""


def _read_pack(pack_bytes: bytes) -> tuple[dict, list[dict]]:
    """按当前格式解包并逐层核对包内容；任何不符都抛 PackTampered。"""
    try:
        zf = zipfile.ZipFile(io.BytesIO(pack_bytes))
        names = set(zf.namelist())
        if names != {"manifest.json", "records.jsonl", "README.txt"}:
            raise PackTampered("unexpected file layout in verify pack")
        manifest_bytes = zf.read("manifest.json")
        records_bytes = zf.read("records.jsonl")
    except (zipfile.BadZipFile, KeyError) as exc:
        raise PackTampered(f"verify pack unreadable: {exc}") from exc

    try:
        manifest = json.loads(manifest_bytes)
        lines = [ln for ln in records_bytes.decode().splitlines() if ln]
        records = [json.loads(ln) for ln in lines]
    except (ValueError, UnicodeDecodeError) as exc:
        raise PackTampered(f"verify pack malformed: {exc}") from exc

    if manifest.get("format") != PACK_FORMAT:
        raise PackTampered("unsupported pack format")
    # 清单自身摘要无法在包内自证：与快照行外部锚 manifest_sha256 的核对在
    # verify_snapshot 中完成（WORM 包字节 + 外部 DB 锚双锚定）。
    if crypto.sha256_hex(records_bytes) != \
            manifest.get("records_root", {}).get("sha256"):
        raise PackTampered("records.jsonl digest mismatch")
    entries = manifest.get("records")
    if not isinstance(entries, list) or len(entries) != len(records):
        raise PackTampered("manifest record index mismatch")
    for entry, rec in zip(entries, records):
        if entry.get("seq") != rec.get("seq") \
                or entry.get("sha256") != _record_digest(rec):
            raise PackTampered(f"record digest mismatch at seq {rec.get('seq')}")
    scope = manifest.get("scope", {})
    if scope.get("record_count") != len(records):
        raise PackTampered("scope record_count mismatch")
    return manifest, records


def _verify_frozen_chain(records: list[dict], anchor_mac: str,
                         cutoff_seq: int) -> dict:
    """重放冻结记录的 HMAC 链；若该批记录覆盖到 cutoff（筛选无洞时），
    链尾 MAC 必须等于冻结锚。带筛选的子集只需逐条 prev/自洽。"""
    by_seq = {r["seq"]: r for r in records}
    for rec in records:
        prev = rec["prev_mac"]
        expect = crypto.audit_mac(prev, json.dumps(
            [rec["ts"], rec["actor"], rec["action"], rec["study_id"],
             rec["reid_id"], json.dumps(rec["detail"], sort_keys=True,
                                        separators=(",", ":"))],
            sort_keys=True, separators=(",", ":")))
        if not crypto.hmac_compare(expect, rec["mac"]):
            return {"ok": False, "broken_at": rec["seq"],
                    "reason": "frozen_record_mac_mismatch"}
        # 与前一条冻结记录的链接（相邻 seq 都在集合中时必须接上）
        if rec["seq"] > 1 and (rec["seq"] - 1) in by_seq:
            if by_seq[rec["seq"] - 1]["mac"] != prev:
                return {"ok": False, "broken_at": rec["seq"],
                        "reason": "frozen_chain_link_mismatch"}
    anchor_ok = (not records) or records[-1]["seq"] != cutoff_seq \
        or crypto.hmac_compare(records[-1]["mac"], anchor_mac)
    if not anchor_ok:
        return {"ok": False, "broken_at": records[-1]["seq"],
                "reason": "frozen_anchor_mismatch"}
    return {"ok": True, "checked": len(records)}


def verify_snapshot(snap: dict) -> dict:
    """对已冻结快照执行完整验真：包文件 → 包内清单/记录 → 冻结 HMAC 链
    → 与现库交叉核对（锚行、冻结行、链尾、全链）。

    返回确定性结论；任何一层不符都给 ok=False 与具体断裂层，不抛身份信息。
    """
    result = {"snapshot_id": snap["id"], "ok": False,
              "pack_file_ok": False, "manifest_ok": False,
              "records_digests_ok": False, "frozen_chain_ok": False,
              "anchor_intact": None, "frozen_rows_match_live": None,
              "live_chain_ok": None, "live_chain_count": None,
              "broken_at": None, "reason": None}

    # 1) 包文件：存在且字节摘要仍等于 CAS（WORM 文件被改即暴露）
    try:
        pack_bytes = store.blob_get(snap["pack_cas_id"])
    except store.NotFound:
        result["reason"] = "verify_pack_blob_missing"
        return result
    if crypto.sha256_hex(pack_bytes) != snap["pack_cas_id"]:
        result["reason"] = "pack_file_sha_mismatch"
        return result
    result["pack_file_ok"] = True

    # 2) 包内容：清单/记录逐级摘要 + 与快照行外部锚核对
    try:
        manifest, records = _read_pack(pack_bytes)
    except PackTampered as exc:
        result["reason"] = str(exc)
        return result
    if crypto.sha256_hex(json.dumps(manifest, ensure_ascii=False, indent=2,
                                   sort_keys=True).encode()) \
            != snap["manifest_sha256"]:
        result["reason"] = "manifest external anchor mismatch"
        return result
    result["manifest_ok"] = True
    result["records_digests_ok"] = True

    if manifest["snapshot_id"] != snap["id"] \
            or manifest["scope"]["cutoff_seq"] != snap["cutoff_seq"] \
            or manifest["anchor"]["mac"] != snap["anchor_mac"]:
        result["reason"] = "manifest scope/anchor does not match snapshot row"
        return result

    # 3) 冻结 HMAC 链自洽
    chain_check = _verify_frozen_chain(
        records, snap["anchor_mac"], snap["cutoff_seq"])
    result["frozen_chain_ok"] = chain_check["ok"]
    if not chain_check["ok"]:
        result["broken_at"] = chain_check.get("broken_at")
        result["reason"] = chain_check.get("reason")
        return result

    # 4) 与当前库交叉核对（原链被事后篡改时在此暴露）
    conn = store.get_conn()
    anchor_row = conn.execute(
        "SELECT seq, ts, mac FROM audit_log WHERE seq=?",
        (snap["cutoff_seq"],)).fetchone() if snap["cutoff_seq"] else None
    if snap["cutoff_seq"] == 0:
        result["anchor_intact"] = snap["anchor_mac"] == "GENESIS"
    elif anchor_row is not None and crypto.hmac_compare(
            anchor_row["mac"], snap["anchor_mac"]):
        result["anchor_intact"] = True
    else:
        result["anchor_intact"] = False
        if result["broken_at"] is None:
            result["broken_at"] = snap["cutoff_seq"]

    # 逐条与现库比对冻结行（冻结集即筛选结果，规模可控）
    live_ok = True
    for rec in records:
        row = conn.execute(
            "SELECT ts,actor,action,study_id,reid_id,detail_json,prev_mac,mac"
            " FROM audit_log WHERE seq=?", (rec["seq"],)).fetchone()
        if row is None:
            live_ok = False
            result["broken_at"] = rec["seq"]
            result["reason"] = "frozen row missing from live chain"
            break
        if not crypto.hmac_compare(row["mac"], rec["mac"]) \
                or row["ts"] != rec["ts"] or row["actor"] != rec["actor"] \
                or row["action"] != rec["action"] \
                or row["study_id"] != rec["study_id"] \
                or row["reid_id"] != rec["reid_id"] \
                or row["detail_json"] != json.dumps(
                    rec["detail"], sort_keys=True, separators=(",", ":")) \
                or row["prev_mac"] != rec["prev_mac"]:
            live_ok = False
            result["broken_at"] = rec["seq"]
            result["reason"] = "frozen row changed in live chain"
            break
    result["frozen_rows_match_live"] = live_ok
    if not result["anchor_intact"]:
        result["reason"] = "anchor row missing or changed in live chain"
        return result
    if not live_ok:
        return result

    live = store.verify_audit_chain()
    result["live_chain_count"] = live["count"]
    result["live_chain_ok"] = live["ok"]
    if not live["ok"]:
        result["broken_at"] = live["broken_at"]
        result["reason"] = ("live chain broken before/at cutoff"
                            if live["broken_at"] <= snap["cutoff_seq"]
                            else "live chain broken after cutoff (frozen scope intact)")
        return result

    head = conn.execute("SELECT seq FROM audit_log ORDER BY seq DESC LIMIT 1").fetchone()
    head_seq = head["seq"] if head else 0
    result["live_head_seq"] = head_seq
    result["new_events_after_cutoff"] = max(0, head_seq - snap["cutoff_seq"])
    result["ok"] = True
    result["reason"] = None
    return result


def get_verified_pack(snap: dict) -> bytes:
    """下载前验真：包缺失/被改都返回确定性错误，绝不把被改内容当证据交付。"""
    try:
        pack_bytes = store.blob_get(snap["pack_cas_id"])
    except store.NotFound:
        raise PackTampered("verify pack blob missing")
    if crypto.sha256_hex(pack_bytes) != snap["pack_cas_id"]:
        raise PackTampered("verify pack file has been modified")
    try:
        _read_pack(pack_bytes)
    except PackTampered:
        raise
    return pack_bytes
