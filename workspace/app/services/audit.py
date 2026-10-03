"""审计导出：哈希链完整性校验 + JSONL/CSV 导出。

审计记录只追加、不可改；每条携带前一条 HMAC，篡改任意行都会在校验中暴露断链。
导出文件落内容寻址存储，自身可复核。
"""
from __future__ import annotations

import csv
import io
import json

from ..core import store


def query(actor: str | None = None, action: str | None = None,
          study_id: str | None = None, reid_id: str | None = None,
          limit: int = 1000) -> list[dict]:
    q = "SELECT * FROM audit_log WHERE 1=1"
    args: list = []
    if actor:
        q += " AND actor=?"; args.append(actor)
    if action:
        q += " AND action=?"; args.append(action)
    if study_id:
        q += " AND study_id=?"; args.append(study_id)
    if reid_id:
        q += " AND reid_id=?"; args.append(reid_id)
    q += " ORDER BY seq DESC LIMIT ?"; args.append(limit)
    rows = []
    for r in store.all_rows(q, tuple(args)):
        d = dict(r)
        d["detail"] = json.loads(r["detail_json"])
        d.pop("detail_json", None)
        rows.append(d)
    return rows


def export_jsonl(actor: str = "admin", *, filters: dict | None = None) -> dict:
    filters = filters or {}
    verification = store.verify_audit_chain()
    rows_asc = list(reversed(query(limit=filters.get("limit", 100000),
                                   **{k: v for k, v in filters.items() if k != "limit"})))
    buf = io.StringIO()
    for r in rows_asc:
        buf.write(json.dumps(r, ensure_ascii=False, sort_keys=True) + "\n")
    payload = {
        "exported_at": store.now_iso(),
        "exported_by": actor,
        "chain_verification": verification,
        "record_count": len(rows_asc),
        "records": [json.loads(line) for line in buf.getvalue().splitlines()],
    }
    data = json.dumps(payload, ensure_ascii=False, indent=2).encode()
    cas = store.blob_put("audit_export_json", data, actor)
    store.audit(actor, "audit.export", detail={"format": "json", "cas_id": cas,
                                               "records": len(rows_asc),
                                               "chain_ok": verification["ok"]})
    return {"cas_id": cas, "size": len(data), "records": len(rows_asc),
            "chain_verification": verification}


def export_csv(actor: str = "admin", *, filters: dict | None = None) -> dict:
    filters = filters or {}
    verification = store.verify_audit_chain()
    rows = query(limit=filters.get("limit", 100000),
                 **{k: v for k, v in filters.items() if k != "limit"})
    out = io.StringIO()
    writer = csv.writer(out)
    writer.writerow(["seq", "ts", "actor", "action", "study_id", "reid_id", "mac"])
    for r in rows:
        writer.writerow([r["seq"], r["ts"], r["actor"], r["action"],
                         r["study_id"] or "", r["reid_id"] or "", r["mac"]])
    data = out.getvalue().encode()
    cas = store.blob_put("audit_export_csv", data, actor)
    store.audit(actor, "audit.export", detail={"format": "csv", "cas_id": cas,
                                               "records": len(rows),
                                               "chain_ok": verification["ok"]})
    return {"cas_id": cas, "size": len(data), "records": len(rows),
            "chain_verification": verification}
