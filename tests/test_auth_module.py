import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pytest
import jwt
import time
from fastapi import HTTPException


def _make_jwt(secret: str, sub: str = "user-123", email: str = "test@example.com",
              expired: bool = False) -> str:
    now = int(time.time())
    payload = {
        "sub": sub,
        "email": email,
        "role": "authenticated",
        "aud": "authenticated",
        "iat": now,
        "exp": now - 3600 if expired else now + 3600,
    }
    return jwt.encode(payload, secret, algorithm="HS256")


class TestVerifyJwt:
    def test_valid_jwt_returns_payload(self):
        from app.auth import verify_jwt
        secret = "test-secret-32-chars-long-enough!!"
        token = _make_jwt(secret)
        payload = verify_jwt(token, jwt_secret=secret)
        assert payload["sub"] == "user-123"
        assert payload["email"] == "test@example.com"

    def test_expired_jwt_raises_401(self):
        from app.auth import verify_jwt
        secret = "test-secret-32-chars-long-enough!!"
        token = _make_jwt(secret, expired=True)
        with pytest.raises(HTTPException) as exc_info:
            verify_jwt(token, jwt_secret=secret)
        assert exc_info.value.status_code == 401

    def test_wrong_secret_raises_401(self):
        from app.auth import verify_jwt
        token = _make_jwt("correct-secret-32-chars-long!!!!")
        with pytest.raises(HTTPException) as exc_info:
            verify_jwt(token, jwt_secret="wrong-secret-32-chars-long!!!")
        assert exc_info.value.status_code == 401

    def test_malformed_token_raises_401(self):
        from app.auth import verify_jwt
        with pytest.raises(HTTPException) as exc_info:
            verify_jwt("not.a.jwt", jwt_secret="any-secret")
        assert exc_info.value.status_code == 401

    def test_empty_token_raises_401(self):
        from app.auth import verify_jwt
        with pytest.raises(HTTPException) as exc_info:
            verify_jwt("", jwt_secret="any-secret")
        assert exc_info.value.status_code == 401


class TestDevUser:
    def test_dev_user_has_required_fields(self):
        from app.auth import DEV_USER
        assert "id" in DEV_USER
        assert "email" in DEV_USER
        assert "role" in DEV_USER

    def test_dev_user_is_authenticated_role(self):
        from app.auth import DEV_USER
        assert DEV_USER["role"] == "authenticated"

    def test_dev_user_has_fixed_id(self):
        from app.auth import DEV_USER
        # Should be a valid UUID-like string
        assert len(DEV_USER["id"]) > 0


class TestGetCurrentUserDependencyLogic:
    def test_auth_disabled_returns_dev_user(self):
        from app.auth import get_current_user_dependency, DEV_USER
        user = get_current_user_dependency(auth_enabled=False, authorization=None)
        assert user["email"] == DEV_USER["email"]

    def test_auth_enabled_missing_header_raises_401(self):
        from app.auth import get_current_user_dependency
        with pytest.raises(HTTPException) as exc_info:
            get_current_user_dependency(auth_enabled=True, authorization=None)
        assert exc_info.value.status_code == 401

    def test_auth_enabled_bad_scheme_raises_401(self):
        from app.auth import get_current_user_dependency
        with pytest.raises(HTTPException) as exc_info:
            get_current_user_dependency(auth_enabled=True, authorization="Basic dXNlcjpwYXNz")
        assert exc_info.value.status_code == 401

    def test_auth_enabled_valid_token_returns_payload(self):
        from app.auth import get_current_user_dependency
        import os
        os.environ["SUPABASE_JWT_SECRET"] = "test-secret-32-chars-long-enough!!"
        # Reload settings to pick up the env var
        import importlib
        import config.settings as s
        import app.auth as a
        importlib.reload(s)
        a.settings = s.settings

        secret = "test-secret-32-chars-long-enough!!"
        token = _make_jwt(secret, sub="user-456")
        user = get_current_user_dependency(auth_enabled=True, authorization=f"Bearer {token}")
        assert user["sub"] == "user-456"
