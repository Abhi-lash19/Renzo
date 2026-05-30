import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import sqlite3
import pytest
from unittest.mock import MagicMock, patch
from storage.repository import JobRepository


@pytest.fixture
def repo():
    """Create an in-memory SQLite DB with job_runs table, patch db_manager, return repository."""
    conn = sqlite3.connect(":memory:")
    conn.execute("PRAGMA foreign_keys = ON")
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS job_runs (
            run_id       TEXT PRIMARY KEY,
            status       TEXT NOT NULL DEFAULT 'queued',
            created_at   DATETIME NOT NULL,
            started_at   DATETIME,
            completed_at DATETIME,
            result_json  TEXT
        );
    """)
    conn.commit()

    mock_ctx = MagicMock()
    mock_ctx.__enter__ = MagicMock(return_value=conn)
    mock_ctx.__exit__ = MagicMock(return_value=False)

    mock_dm = MagicMock()
    mock_dm.connection.return_value = mock_ctx

    with patch("storage.repository.db_manager", mock_dm):
        yield JobRepository()

    conn.close()


class TestCreateJobRun:
    def test_create_returns_true(self, repo):
        assert repo.create_job_run("run-001") is True

    def test_create_duplicate_returns_false(self, repo):
        repo.create_job_run("run-002")
        assert repo.create_job_run("run-002") is False

    def test_created_run_has_queued_status(self, repo):
        repo.create_job_run("run-003")
        run = repo.get_job_run("run-003")
        assert run is not None
        assert run["status"] == "queued"

    def test_created_run_has_created_at(self, repo):
        repo.create_job_run("run-010")
        run = repo.get_job_run("run-010")
        assert run["created_at"] is not None


class TestUpdateRunStatus:
    def test_update_to_running(self, repo):
        repo.create_job_run("run-004")
        assert repo.update_run_status("run-004", "running") is True
        assert repo.get_job_run("run-004")["status"] == "running"

    def test_update_to_complete_with_result(self, repo):
        repo.create_job_run("run-005")
        assert repo.update_run_status("run-005", "complete", result_json='{"jobs": []}') is True
        run = repo.get_job_run("run-005")
        assert run["status"] == "complete"
        assert run["result_json"] == '{"jobs": []}'

    def test_update_to_failed(self, repo):
        repo.create_job_run("run-006")
        assert repo.update_run_status("run-006", "failed") is True
        assert repo.get_job_run("run-006")["status"] == "failed"

    def test_update_nonexistent_returns_false(self, repo):
        assert repo.update_run_status("nonexistent", "running") is False

    def test_invalid_status_returns_false(self, repo):
        repo.create_job_run("run-007")
        assert repo.update_run_status("run-007", "banana") is False


class TestGetJobRun:
    def test_get_nonexistent_returns_none(self, repo):
        assert repo.get_job_run("does-not-exist") is None

    def test_get_returns_dict_with_required_keys(self, repo):
        repo.create_job_run("run-008")
        run = repo.get_job_run("run-008")
        assert isinstance(run, dict)
        for key in ("run_id", "status", "created_at", "started_at", "completed_at", "result_json"):
            assert key in run

    def test_get_returns_correct_run_id(self, repo):
        repo.create_job_run("run-009")
        run = repo.get_job_run("run-009")
        assert run["run_id"] == "run-009"
