import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def client(tmp_path, monkeypatch):
    import storage.db as db_module
    import storage.db_manager as dm
    db_file = tmp_path / "test_auth_api.db"
    monkeypatch.setattr(db_module, "DB_PATH", db_file)
    monkeypatch.setattr(dm, "DB_PATH", str(db_file))
    dm.db_manager._sqlite_conn = None
    dm.db_manager._initialized = False
    from app.main import app
    with TestClient(app) as c:
        yield c


class TestAuthEndpointsDevMode:
    """Tests with AUTH_ENABLED=false (default, no Supabase required)."""

    def test_register_returns_200(self, client):
        response = client.post("/v1/auth/register", json={
            "email": "test@example.com",
            "password": "Password123!"
        })
        assert response.status_code == 200

    def test_register_returns_access_token(self, client):
        response = client.post("/v1/auth/register", json={
            "email": "test@example.com",
            "password": "Password123!"
        })
        data = response.json()
        assert "access_token" in data
        assert "token_type" in data
        assert data["token_type"] == "bearer"

    def test_login_returns_200(self, client):
        response = client.post("/v1/auth/login", json={
            "email": "test@example.com",
            "password": "Password123!"
        })
        assert response.status_code == 200

    def test_login_returns_access_token(self, client):
        response = client.post("/v1/auth/login", json={
            "email": "test@example.com",
            "password": "Password123!"
        })
        assert "access_token" in response.json()

    def test_me_returns_200_without_auth_header(self, client):
        """In dev mode, /me works without any auth header."""
        response = client.get("/v1/auth/me")
        assert response.status_code == 200

    def test_me_returns_user_info(self, client):
        response = client.get("/v1/auth/me")
        data = response.json()
        assert "id" in data
        assert "email" in data

    def test_register_invalid_email_returns_422(self, client):
        response = client.post("/v1/auth/register", json={
            "email": "not-an-email",
            "password": "Password123!"
        })
        assert response.status_code == 422

    def test_register_short_password_returns_422(self, client):
        response = client.post("/v1/auth/register", json={
            "email": "test@example.com",
            "password": "short"
        })
        assert response.status_code == 422
