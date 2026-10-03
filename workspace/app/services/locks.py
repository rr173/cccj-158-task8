"""病例级互斥锁：串行化同一影像的并发修订。

锁行使用 INSERT 抢占；崩溃后租约过期可被接管（带 owner 校验，避免双接管）。
所有"创建新版本"的写路径都必须在锁内执行。
"""
from __future__ import annotations

from datetime import timedelta

from ..core import store
from .. import config


class StudyLocked(store.ConflictError):
    pass


def acquire(study_id: str, owner: str) -> None:
    conn = store.get_conn()
    conn.execute("BEGIN IMMEDIATE")
    try:
        row = conn.execute("SELECT * FROM study_locks WHERE study_id=?", (study_id,)).fetchone()
        if row is not None:
            if row["owner"] == owner:
                conn.commit()
                return
            lease_end = store.utcnow() - timedelta(seconds=config.STUDY_LOCK_STALE_SECONDS)
            stale = row["acquired_at"] < lease_end.isoformat()
            if stale:
                cur = conn.execute(
                    "UPDATE study_locks SET owner=?, acquired_at=? "
                    "WHERE study_id=? AND owner=?",
                    (owner, store.now_iso(), study_id, row["owner"]))
                if cur.rowcount == 1:
                    conn.commit()
                    return
            conn.rollback()
            raise StudyLocked(f"study {study_id} is locked by another revision")
        conn.execute("INSERT INTO study_locks(study_id,owner,acquired_at) VALUES(?,?,?)",
                     (study_id, owner, store.now_iso()))
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def release(study_id: str, owner: str) -> None:
    conn = store.get_conn()
    conn.execute("DELETE FROM study_locks WHERE study_id=? AND owner=?", (study_id, owner))
