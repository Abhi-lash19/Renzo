import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pytest
from unittest.mock import patch
from fastapi.testclient import TestClient


@pytest.fixture
def client(tmp_path, monkeypatch):
    import storage.db as db_module
    import storage.db_manager as dm
    db_file = tmp_path / "test_jobs_api.db"
    monkeypatch.setattr(db_module, "DB_PATH", db_file)
    monkeypatch.setattr(dm, "DB_PATH", str(db_file))
    dm.db_manager._sqlite_conn = None
    dm.db_manager._initialized = False
    from app.main import app
    with TestClient(app) as c:
        yield c


class TestRunJobFinder:
    def test_post_returns_202(self, client):
        with patch("app.api.v1.endpoints.jobs.fetch_all_jobs", return_value=[]), \
             patch("app.api.v1.endpoints.jobs.process_jobs", return_value=0):
            response = client.post("/v1/run-job-finder")
        assert response.status_code == 202

    def test_post_returns_run_id(self, client):
        with patch("app.api.v1.endpoints.jobs.fetch_all_jobs", return_value=[]), \
             patch("app.api.v1.endpoints.jobs.process_jobs", return_value=0):
            response = client.post("/v1/run-job-finder")
        data = response.json()
        assert "run_id" in data
        assert len(data["run_id"]) > 0

    def test_post_returns_queued_status(self, client):
        with patch("app.api.v1.endpoints.jobs.fetch_all_jobs", return_value=[]), \
             patch("app.api.v1.endpoints.jobs.process_jobs", return_value=0):
            response = client.post("/v1/run-job-finder")
        assert response.json()["status"] == "queued"

    def test_run_stored_in_db(self, client):
        """After POST, the run_id should be queryable via GET."""
        with patch("app.api.v1.endpoints.jobs.fetch_all_jobs", return_value=[]), \
             patch("app.api.v1.endpoints.jobs.process_jobs", return_value=0):
            post_resp = client.post("/v1/run-job-finder")
        run_id = post_resp.json()["run_id"]
        get_resp = client.get(f"/v1/runs/{run_id}")
        assert get_resp.status_code == 200


class TestGetRun:
    def test_get_unknown_run_returns_404(self, client):
        response = client.get("/v1/runs/nonexistent-id")
        assert response.status_code == 404

    def test_get_existing_run_returns_200(self, client):
        with patch("app.api.v1.endpoints.jobs.fetch_all_jobs", return_value=[]), \
             patch("app.api.v1.endpoints.jobs.process_jobs", return_value=0):
            post_resp = client.post("/v1/run-job-finder")
        run_id = post_resp.json()["run_id"]
        response = client.get(f"/v1/runs/{run_id}")
        assert response.status_code == 200

    def test_get_run_has_required_fields(self, client):
        with patch("app.api.v1.endpoints.jobs.fetch_all_jobs", return_value=[]), \
             patch("app.api.v1.endpoints.jobs.process_jobs", return_value=0):
            post_resp = client.post("/v1/run-job-finder")
        run_id = post_resp.json()["run_id"]
        data = client.get(f"/v1/runs/{run_id}").json()
        for key in ("run_id", "status", "created_at"):
            assert key in data

    def test_get_completed_run_has_result_list(self, client):
        """When a run completes, GET /runs/{run_id} returns the result array."""
        import json
        from storage.repository import JobRepository
        from storage.db import init_db
        import storage.db_manager as dm

        # Create a run and manually set it to complete with sample result_json
        repo = JobRepository()
        run_id = "completed-test-run"
        repo.create_job_run(run_id)
        sample_result = json.dumps([{
            "job_id": "job-001",
            "title": "Python Dev",
            "company": "TestCo",
            "location": "Remote",
            "url": "https://example.com/job/1",
            "score": 8.5,
            "match_type": "strong",
        }])
        repo.update_run_status(run_id, "complete", result_json=sample_result)

        response = client.get(f"/v1/runs/{run_id}")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "complete"
        assert data["result"] is not None
        assert len(data["result"]) == 1
        assert data["result"][0]["job_id"] == "job-001"
