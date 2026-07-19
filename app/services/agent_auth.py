from __future__ import annotations

import os
import secrets
from typing import Optional

from fastapi import HTTPException, Security, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

SCENE_API_TOKEN_ENV = "SCENE_API_TOKEN"

_bearer = HTTPBearer(auto_error=False)


async def require_agent_api_token(
    credentials: Optional[HTTPAuthorizationCredentials] = Security(_bearer),
) -> None:
    expected = os.environ.get(SCENE_API_TOKEN_ENV)
    if not expected:
        return
    if credentials is None or credentials.scheme.lower() != "bearer":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing bearer token",
            headers={"WWW-Authenticate": "Bearer"},
        )
    if not secrets.compare_digest(credentials.credentials, expected):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Invalid bearer token",
        )
