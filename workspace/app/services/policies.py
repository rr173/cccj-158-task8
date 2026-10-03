"""项目策略注册中心：策略以 (name, version) 不可变登记，升级只产生新版本。"""
from __future__ import annotations

import json
import uuid

from ..core import store


def register_policy(name: str, content: dict, actor: str,
                    version: int | None = None) -> dict:
    conn = store.get_conn()
    conn.execute("BEGIN IMMEDIATE")
    try:
        if version is None:
            row = conn.execute(
                "SELECT COALESCE(MAX(version),0) AS v FROM policies WHERE name=?", (name,)
            ).fetchone()
            version = row["v"] + 1
        blob = store.sealed_blob_put("policy", json.dumps(content).encode(), actor)
        pid = f"pol_{uuid.uuid4().hex[:12]}"
        try:
            conn.execute(
                "INSERT INTO policies(id,name,version,content_cas_id,created_by,created_at)"
                " VALUES(?,?,?,?,?,?)",
                (pid, name, version, blob, actor, store.now_iso()),
            )
        except Exception:
            conn.rollback()
            raise
        store.audit(actor, "policy.register", detail={"name": name, "version": version,
                                                        "policy_id": pid})
        conn.commit()
        return get_policy(pid)
    except Exception:
        conn.rollback()
        raise


def get_policy(policy_id: str) -> dict:
    r = store.one("SELECT * FROM policies WHERE id=?", (policy_id,))
    content = json.loads(store.sealed_blob_get(r["content_cas_id"]))
    return {"id": r["id"], "name": r["name"], "version": r["version"],
            "retired_at": r["retired_at"], "content": content,
            "created_by": r["created_by"], "created_at": r["created_at"]}


def latest_policy(name: str) -> dict:
    r = store.one(
        "SELECT * FROM policies WHERE name=? AND retired_at IS NULL"
        " ORDER BY version DESC LIMIT 1", (name,))
    return get_policy(r["id"])


def list_policies(name: str | None = None) -> list[dict]:
    q = "SELECT id FROM policies WHERE retired_at IS NULL"
    args: tuple = ()
    if name:
        q += " AND name=?"; args = (name,)
    q += " ORDER BY name, version"
    return [get_policy(r["id"]) for r in store.all_rows(q, args)]


def retire_policy(policy_id: str, actor: str) -> None:
    conn = store.get_conn()
    cur = conn.execute("UPDATE policies SET retired_at=? WHERE id=? AND retired_at IS NULL",
                       (store.now_iso(), policy_id))
    store.audit(actor, "policy.retire", detail={"policy_id": policy_id, "rows": cur.rowcount})
