"""回链分片的进程内保险库。

分片绝不落库：只存在于内存，授权终止（导出/撤销/过期）或 TTL 到期即抹除。
生产多副本部署应替换为带 TTL 的集中式保密保险库（如 Redis over TLS + KMS wrap）。
"""
from __future__ import annotations

import threading
import time
from dataclasses import dataclass

from .. import config


@dataclass
class VaultEntry:
    share_a: str | None
    share_b: str
    expires_at: float


class ShareVault:
    def __init__(self, ttl_seconds: int = config.SHARE_VAULT_TTL_SECONDS):
        self._data: dict[str, VaultEntry] = {}
        self._lock = threading.Lock()
        self._ttl = ttl_seconds

    def escrow(self, reid_id: str, share_a: str | None, share_b: str,
               ttl_seconds: int | None = None) -> None:
        with self._lock:
            self._data[reid_id] = VaultEntry(
                share_a, share_b, time.time() + (ttl_seconds or self._ttl))

    def set_share_a(self, reid_id: str, share_a: str) -> None:
        with self._lock:
            e = self._data.get(reid_id)
            if e is not None and e.expires_at >= time.time():
                e.share_a = share_a

    def get(self, reid_id: str) -> VaultEntry | None:
        with self._lock:
            e = self._data.get(reid_id)
            if e is None:
                return None
            if e.expires_at < time.time():
                self._data.pop(reid_id, None)
                return None
            return e

    def purge(self, reid_id: str) -> bool:
        with self._lock:
            return self._data.pop(reid_id, None) is not None

    def purge_expired(self) -> list[str]:
        now = time.time()
        with self._lock:
            dead = [k for k, e in self._data.items() if e.expires_at < now]
            for k in dead:
                self._data.pop(k, None)
            return dead


vault = ShareVault()
