"""
app/auth.py — Supabase Auth JWT verification and FastAPI dependency.

When AUTH_ENABLED=false (default): injects DEV_USER, no JWT required.
When AUTH_ENABLED=true: verifies Bearer JWT with SUPABASE_JWT_SECRET.
"""
from __future__ import annotations

from typing import Optional

import jwt
from fastapi import HTTPException, Header
from jwt import ExpiredSignatureError, InvalidTokenError

from config.settings import settings
from utils.logger import get_logger

logger = get_logger(__name__)

# Fixed dev user injected when AUTH_ENABLED=false
DEV_USER = {
    "id": "00000000-0000-0000-0000-000000000000",
    "email": "dev@local",
    "role": "authenticated",
}


def verify_jwt(token: str, jwt_secret: str | None = None) -> dict:
    """
    Verify a Supabase JWT and return the decoded payload.
    Raises HTTPException(401) on any verification failure.

    Args:
        token:      Raw JWT string (without "Bearer" prefix)
        jwt_secret: Secret to verify with. Defaults to settings.SUPABASE_JWT_SECRET.
    """
    if not token:
        raise HTTPException(status_code=401, detail="No token provided")

    secret = jwt_secret or settings.SUPABASE_JWT_SECRET
    if not secret:
        raise HTTPException(
            status_code=401,
            detail="SUPABASE_JWT_SECRET not configured — set AUTH_ENABLED=false for local dev",
        )

    try:
        payload = jwt.decode(
            token,
            secret,
            algorithms=["HS256"],
            audience="authenticated",
        )
        return payload
    except ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired")
    except InvalidTokenError as e:
        raise HTTPException(status_code=401, detail=f"Invalid token: {e}")
    except Exception as e:
        raise HTTPException(status_code=401, detail=f"Token verification failed: {e}")


def get_current_user_dependency(
    auth_enabled: bool,
    authorization: Optional[str],
) -> dict:
    """
    Core auth logic. Extracted from the FastAPI dependency for testability.

    Args:
        auth_enabled: Whether JWT verification is enforced
        authorization: Value of Authorization header (or None)

    Returns:
        User payload dict with at minimum: id/sub, email, role
    """
    if not auth_enabled:
        return DEV_USER

    if not authorization:
        raise HTTPException(
            status_code=401,
            detail="Authorization header required. Format: 'Bearer <token>'",
        )

    scheme, _, token = authorization.partition(" ")
    if scheme.lower() != "bearer" or not token:
        raise HTTPException(
            status_code=401,
            detail="Authorization header must use Bearer scheme. Format: 'Bearer <token>'",
        )

    return verify_jwt(token)


def get_current_user(
    authorization: Optional[str] = Header(default=None),
) -> dict:
    """
    FastAPI dependency. Inject into protected endpoints with:
        current_user: dict = Depends(get_current_user)
    """
    return get_current_user_dependency(
        auth_enabled=settings.AUTH_ENABLED,
        authorization=authorization,
    )
