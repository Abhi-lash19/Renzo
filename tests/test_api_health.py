import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def client(tmp_path, monkeypatch):
    """TestClient backed by a fresh temp-file SQLite DB."""
    import storage.db as db_module
    import storage.db_manager as dm
    db_file = tmp_path / "test_api.db"
    monkeypatch.setattr(db_module, "DB_PATH", db_file)
    monkeypatch.setattr(dm, "DB_PATH", str(db_file))
    dm.db_manager._sqlite_conn = None
    dm.db_manager._initialized = False
    from app.main import app
    with TestClient(app) as c:
        yield c


class TestHealthEndpoint:
    def test_health_returns_200(self, client):
        response = client.get("/v1/health")
        assert response.status_code == 200

    def test_health_returns_ok(self, client):
        response = client.get("/v1/health")
        assert response.json()["status"] == "ok"

    def test_health_returns_json(self, client):
        response = client.get("/v1/health")
        assert response.headers["content-type"].startswith("application/json")
