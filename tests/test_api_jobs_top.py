import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pytest
import importlib
from fastapi.testclient import TestClient


@pytest.fixture
def client(tmp_path, monkeypatch):
    import storage.db as db_module
    import storage.db_manager as dm
    import app.auth as auth_module
    import config.settings as settings_module

    db_file = tmp_path / "test_jobs_top.db"
    monkeypatch.setattr(db_module, "DB_PATH", db_file)
    monkeypatch.setattr(dm, "DB_PATH", str(db_file))
    dm.db_manager._sqlite_conn = None
    dm.db_manager._initialized = False

    monkeypatch.setenv("AUTH_ENABLED", "false")
    monkeypatch.setenv("EMBEDDINGS_ENABLED", "false")
    monkeypatch.setenv("EMBEDDING_PROVIDER", "mock")
    importlib.reload(settings_module)
    monkeypatch.setattr(auth_module, "settings", settings_module.settings)

    from app.main import app
    with TestClient(app) as c:
        yield c


class TestGetJobsTop:
    def test_returns_200(self, client):
        assert client.get("/v1/jobs/top").status_code == 200

    def test_returns_list(self, client):
        assert isinstance(client.get("/v1/jobs/top").json(), list)

    def test_empty_db_returns_empty_list(self, client):
        assert client.get("/v1/jobs/top").json() == []

    def test_accepts_limit_param(self, client):
        assert client.get("/v1/jobs/top?limit=5").status_code == 200

    def test_not_401_in_dev_mode(self, client):
        assert client.get("/v1/jobs/top").status_code != 401

    def test_invalid_limit_returns_422(self, client):
        # limit must be >= 1
        response = client.get("/v1/jobs/top?limit=0")
        assert response.status_code == 422

    def test_with_jobs_returns_correct_shape(self, tmp_path, monkeypatch):
        import storage.db as db_module
        import storage.db_manager as dm
        import app.auth as auth_module
        import config.settings as settings_module

        db_file = tmp_path / "test_jobs_top_shape.db"
        monkeypatch.setattr(db_module, "DB_PATH", db_file)
        monkeypatch.setattr(dm, "DB_PATH", str(db_file))
        dm.db_manager._sqlite_conn = None
        dm.db_manager._initialized = False
        monkeypatch.setenv("AUTH_ENABLED", "false")
        monkeypatch.setenv("EMBEDDINGS_ENABLED", "false")
        monkeypatch.setenv("EMBEDDING_PROVIDER", "mock")
        importlib.reload(settings_module)
        monkeypatch.setattr(auth_module, "settings", settings_module.settings)

        from storage.db import init_db
        from storage.repository import JobRepository
        from pipeline.models import Job
        from datetime import datetime

        init_db()
        repo = JobRepository()
        job = Job(
            job_id="top_shape_001", title="Python Dev", company="TestCo",
            location="Remote", description="Python AWS backend",
            url="https://example.com/top/shape/1", source="test",
            posted_at=datetime.utcnow(), fetched_at=datetime.utcnow(),
        )
        job.score = 7.5
        repo.insert_job(job)

        from app.main import app
        with TestClient(app) as c:
            response = c.get("/v1/jobs/top")
        assert response.status_code == 200
        data = response.json()
        if data:
            item = data[0]
            for field in ("job_id", "fused_score", "retrieval_mode", "vector_score"):
                assert field in item, f"Missing field: {field}"
            assert item["retrieval_mode"] == "keyword_only"
