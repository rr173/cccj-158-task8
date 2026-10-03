"""Web 层：HTTP Basic 认证与角色控制（演示用账户，生产对接 IdP/MFA）。"""
from __future__ import annotations

import secrets

from fastapi import Depends, Header, HTTPException, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials

from .. import config

security = HTTPBasic()


def authenticate(credentials: HTTPBasicCredentials = Depends(security)) -> dict:
    record = config.USERS.get(credentials.username)
    if record is None or not secrets.compare_digest(record[0], credentials.password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            headers={"WWW-Authenticate": "Basic"},
            detail="invalid credentials")
    return {"username": credentials.username, "roles": record[1]}


def require_role(*roles: str):
    def checker(user: dict = Depends(authenticate)) -> dict:
        if not any(r in user["roles"] for r in roles):
            raise HTTPException(status_code=403, detail=f"requires role: {','.join(roles)}")
        return user
    return checker
