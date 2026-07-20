from __future__ import annotations

import os
import secrets
from typing import Optional

from fastapi import Header, HTTPException, Security, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

SCENE_API_TOKEN_ENV = "SCENE_API_TOKEN"
SCENE_API_TOKEN_HEADER = "X-SCENE-API-Token"

_bearer = HTTPBearer(auto_error=False)


async def require_agent_api_token(
    credentials: Optional[HTTPAuthorizationCredentials] = Security(_bearer),
    header_token: Optional[str] = Header(default=None, alias=SCENE_API_TOKEN_HEADER),
) -> None:
    expected = os.environ.get(SCENE_API_TOKEN_ENV)
    if not expected:
        return

    supplied_tokens = []
    if credentials is not None and credentials.scheme.lower() == "bearer":
        supplied_tokens.append(credentials.credentials)
    if header_token is not None:
        supplied_tokens.append(header_token)

    if not supplied_tokens:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing API token",
            headers={"WWW-Authenticate": "Bearer"},
        )
    if not any(secrets.compare_digest(token, expected) for token in supplied_tokens):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Invalid API token",
        )
