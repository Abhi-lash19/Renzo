import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def client(tmp_path, monkeypatch):
    import storage.db as db_module
    import storage.db_manager as dm
    db_file = tmp_path / "test_feedback_api.db"
    monkeypatch.setattr(db_module, "DB_PATH", db_file)
    monkeypatch.setattr(dm, "DB_PATH", str(db_file))
    dm.db_manager._sqlite_conn = None
    dm.db_manager._initialized = False
    from app.main import app
    with TestClient(app) as c:
        yield c


class TestFeedbackEndpoint:
    def test_valid_viewed_returns_200(self, client):
        response = client.post("/v1/feedback", json={"job_id": "test-001", "action": "viewed"})
        assert response.status_code == 200

    def test_valid_applied_returns_200(self, client):
        response = client.post("/v1/feedback", json={"job_id": "test-001", "action": "applied"})
        assert response.status_code == 200

    def test_valid_ignored_returns_200(self, client):
        response = client.post("/v1/feedback", json={"job_id": "test-001", "action": "ignored"})
        assert response.status_code == 200

    def test_response_has_recorded_field(self, client):
        response = client.post("/v1/feedback", json={"job_id": "test-001", "action": "viewed"})
        assert "recorded" in response.json()

    def test_invalid_action_returns_422(self, client):
        response = client.post("/v1/feedback", json={"job_id": "test-001", "action": "deleted"})
        assert response.status_code == 422

    def test_missing_job_id_returns_422(self, client):
        response = client.post("/v1/feedback", json={"action": "viewed"})
        assert response.status_code == 422

    def test_empty_body_returns_422(self, client):
        response = client.post("/v1/feedback", json={})
        assert response.status_code == 422

    def test_feedback_for_unknown_job_returns_recorded_false(self, client):
        """record_interaction returns False when job_id doesn't exist (FK constraint)."""
        response = client.post("/v1/feedback", json={"job_id": "nonexistent-job-id", "action": "viewed"})
        assert response.status_code == 200
        assert response.json()["recorded"] is False
