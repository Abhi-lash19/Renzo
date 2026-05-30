import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pytest
import jwt
import time
from unittest.mock import patch
from fastapi.testclient import TestClient


def _make_jwt(secret: str, sub: str = "user-123") -> str:
    now = int(time.time())
    return jwt.encode({
        "sub": sub,
        "email": "test@example.com",
        "role": "authenticated",
        "aud": "authenticated",
        "iat": now,
        "exp": now + 3600,
    }, secret, algorithm="HS256")


@pytest.fixture
def client_auth_disabled(tmp_path, monkeypatch):
    """AUTH_ENABLED=false — all protected endpoints work without JWT."""
    import os
    os.environ.pop("AUTH_ENABLED", None)  # ensure not set
    monkeypatch.setenv("AUTH_ENABLED", "false")

    import storage.db as db_module
    import storage.db_manager as dm
    db_file = tmp_path / "test_auth_disabled.db"
    monkeypatch.setattr(db_module, "DB_PATH", db_file)
    monkeypatch.setattr(dm, "DB_PATH", str(db_file))
    dm.db_manager._sqlite_conn = None
    dm.db_manager._initialized = False

    import importlib
    import config.settings as s
    import app.auth as a
    importlib.reload(s)
    monkeypatch.setattr(a, "settings", s.settings)

    from app.main import app
    with TestClient(app) as c:
        yield c


@pytest.fixture
def client_auth_enabled(tmp_path, monkeypatch):
    """AUTH_ENABLED=true + SUPABASE_JWT_SECRET set — JWT required."""
    monkeypatch.setenv("AUTH_ENABLED", "true")
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-chars-long-enough!!")

    import storage.db as db_module
    import storage.db_manager as dm
    db_file = tmp_path / "test_auth_enabled.db"
    monkeypatch.setattr(db_module, "DB_PATH", db_file)
    monkeypatch.setattr(dm, "DB_PATH", str(db_file))
    dm.db_manager._sqlite_conn = None
    dm.db_manager._initialized = False

    import importlib
    import config.settings as s
    import app.auth as a
    importlib.reload(s)
    monkeypatch.setattr(a, "settings", s.settings)

    from app.main import app
    with TestClient(app) as c:
        yield c


class TestHealthIsAlwaysOpen:
    def test_health_works_without_auth_when_enabled(self, client_auth_enabled):
        assert client_auth_enabled.get("/v1/health").status_code == 200

    def test_health_works_without_auth_when_disabled(self, client_auth_disabled):
        assert client_auth_disabled.get("/v1/health").status_code == 200


class TestProtectedEndpointsAuthDisabled:
    def test_run_job_finder_works_without_header(self, client_auth_disabled):
        with patch("app.api.v1.endpoints.jobs.fetch_all_jobs", return_value=[]), \
             patch("app.api.v1.endpoints.jobs.process_jobs", return_value=0):
            response = client_auth_disabled.post("/v1/run-job-finder")
        assert response.status_code == 202

    def test_feedback_works_without_header(self, client_auth_disabled):
        response = client_auth_disabled.post(
            "/v1/feedback", json={"job_id": "j1", "action": "viewed"}
        )
        assert response.status_code == 200


class TestProtectedEndpointsAuthEnabled:
    def test_run_job_finder_no_auth_returns_401(self, client_auth_enabled):
        response = client_auth_enabled.post("/v1/run-job-finder")
        assert response.status_code == 401

    def test_feedback_no_auth_returns_401(self, client_auth_enabled):
        response = client_auth_enabled.post(
            "/v1/feedback", json={"job_id": "j1", "action": "viewed"}
        )
        assert response.status_code == 401

    def test_get_run_no_auth_returns_401(self, client_auth_enabled):
        response = client_auth_enabled.get("/v1/runs/some-run-id")
        assert response.status_code == 401

    def test_run_job_finder_with_valid_jwt_returns_202(self, client_auth_enabled):
        secret = "test-secret-32-chars-long-enough!!"
        token = _make_jwt(secret)
        with patch("app.api.v1.endpoints.jobs.fetch_all_jobs", return_value=[]), \
             patch("app.api.v1.endpoints.jobs.process_jobs", return_value=0):
            response = client_auth_enabled.post(
                "/v1/run-job-finder",
                headers={"Authorization": f"Bearer {token}"}
            )
        assert response.status_code == 202

    def test_feedback_with_valid_jwt_returns_200(self, client_auth_enabled):
        secret = "test-secret-32-chars-long-enough!!"
        token = _make_jwt(secret)
        response = client_auth_enabled.post(
            "/v1/feedback",
            json={"job_id": "j1", "action": "viewed"},
            headers={"Authorization": f"Bearer {token}"}
        )
        assert response.status_code == 200
