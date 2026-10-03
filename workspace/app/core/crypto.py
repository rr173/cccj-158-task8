"""加密原语。

- 映射数据信封加密：每份数据用独立 DEK，DEK 由 KEK 加密落盘（at-rest 保护）。
- 回链密钥双角色 Shamir 风格 XOR 分片：技术上用 XOR 双分片（secret = A xor B），
  足以满足"双角色各自持有独立分片、必须同时到齐"的授权语义。
- HMAC：审计链防篡改。
- 口令哈希：PBKDF2-HMAC-SHA256。
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
from pathlib import Path

from cryptography.fernet import Fernet, InvalidToken
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.primitives import hashes

from .. import config

KEK_FILE = config.SECRETS_DIR / "kek.key"


def b64e(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode("ascii")


def b64d(txt: str) -> bytes:
    return base64.urlsafe_b64decode(txt.encode("ascii"))


# ---------- 信封加密 ----------

def _load_kek() -> bytes:
    if not KEK_FILE.exists():
        config.ensure_dirs()
        KEK_FILE.write_bytes(Fernet.generate_key())
        os.chmod(KEK_FILE, 0o600)
    return KEK_FILE.read_bytes()


def seal_envelope(plaintext: bytes) -> dict:
    """用临时 DEK 加密明文，DEK 再由 KEK 包装。返回可 JSON 化的信封。"""
    dek = Fernet.generate_key()
    token = Fernet(dek).encrypt(plaintext)
    wrapped = Fernet(_load_kek()).encrypt(dek)
    return {"alg": "fernet-envelope/v1", "dek": wrapped.decode(), "ct": token.decode()}


def open_envelope(env: dict) -> bytes:
    try:
        dek = Fernet(_load_kek()).decrypt(env["dek"].encode())
        return Fernet(dek).decrypt(env["ct"].encode())
    except InvalidToken as exc:  # pragma: no cover - 损坏时显式失败
        raise ValueError("envelope decryption failed") from exc


def seal_json(obj) -> str:
    return json.dumps(seal_envelope(json.dumps(obj, separators=(",", ":"), sort_keys=True).encode()))


def open_json(blob: str):
    return json.loads(open_envelope(json.loads(blob)))


# ---------- 显式会话密钥（回链双分片保护的信封） ----------

def seal_with_key(key: bytes, plaintext: bytes) -> bytes:
    return Fernet(key).encrypt(plaintext)


def open_with_key(key: bytes, token: bytes) -> bytes:
    try:
        return Fernet(key).decrypt(token)
    except InvalidToken as exc:
        raise ValueError("session envelope decryption failed") from exc


# ---------- 双角色密钥分片 ----------

def split_secret(secret: bytes) -> tuple[str, str]:
    """把 secret 拆成两个独立分片：share_a, share_b。需要二者才能还原。"""
    share_a = os.urandom(len(secret))
    share_b = bytes(x ^ y for x, y in zip(secret, share_a))
    return b64e(share_a), b64e(share_b)


def combine_shares(share_a: str, share_b: str) -> bytes:
    a, b = b64d(share_a), b64d(share_b)
    if len(a) != len(b):
        raise ValueError("share length mismatch")
    return bytes(x ^ y for x, y in zip(a, b))


# ---------- 口令与摘要 ----------

def hash_password(password: str, salt: bytes | None = None) -> tuple[str, str]:
    salt = salt or os.urandom(16)
    kdf = PBKDF2HMAC(algorithm=hashes.SHA256(), length=32, salt=salt, iterations=200_000)
    return b64e(kdf.derive(password.encode())), b64e(salt)


def verify_password(password: str, stored_hash: str, stored_salt: str) -> bool:
    kdf = PBKDF2HMAC(algorithm=hashes.SHA256(), length=32, salt=b64d(stored_salt), iterations=200_000)
    try:
        kdf.verify(password.encode(), b64d(stored_hash))
        return True
    except Exception:
        return False


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def dicom_identity_fingerprint(identity: dict) -> str:
    """原身份关键标识的稳定指纹，用于不暴露身份前提下的去重/比对。"""
    keys = ("PatientID", "PatientName", "PatientBirthDate")
    material = "|".join(str(identity.get(k, "")) for k in keys)
    return hashlib.sha256(material.encode("utf-8")).hexdigest()[:32]


# ---------- 审计链 HMAC ----------

AUDIT_KEY_FILE = config.SECRETS_DIR / "audit.key"


def _audit_key() -> bytes:
    if not AUDIT_KEY_FILE.exists():
        config.ensure_dirs()
        AUDIT_KEY_FILE.write_bytes(os.urandom(32))
        os.chmod(AUDIT_KEY_FILE, 0o600)
    return AUDIT_KEY_FILE.read_bytes()


def audit_mac(prev_mac: str, canonical_row: str) -> str:
    return hmac.new(_audit_key(), (prev_mac + "." + canonical_row).encode(), hashlib.sha256).hexdigest()


def hmac_compare(a: str, b: str) -> bool:
    return hmac.compare_digest(a, b)
