import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pytest
import importlib
from unittest.mock import patch
from fastapi.testclient import TestClient
from pathlib import Path as P


MIGRATIONS_DIR = P(__file__).resolve().parent.parent / "storage" / "migrations"


@pytest.fixture
def client(tmp_path, monkeypatch):
    import storage.db as db_module
    import storage.db_manager as dm
    import app.auth as auth_module
    import config.settings as settings_module

    db_file = tmp_path / "test_phase3.db"
    monkeypatch.setattr(db_module, "DB_PATH", db_file)
    monkeypatch.setattr(dm, "DB_PATH", str(db_file))
    dm.db_manager._sqlite_conn = None
    dm.db_manager._initialized = False

    monkeypatch.setenv("AUTH_ENABLED", "false")
    importlib.reload(settings_module)
    monkeypatch.setattr(auth_module, "settings", settings_module.settings)

    from app.main import app
    with TestClient(app) as c:
        yield c


class TestPhase3CompleteFlow:
    def test_health_is_open(self, client):
        assert client.get("/v1/health").status_code == 200

    def test_register_login_me_pipeline_feedback_flow(self, client):
        """Full happy-path: register -> login -> /me -> run pipeline -> poll status -> feedback."""
        # Register (dev mode -> mock token)
        reg = client.post("/v1/auth/register", json={
            "email": "user@example.com",
            "password": "Password123!"
        })
        assert reg.status_code == 200
        assert "access_token" in reg.json()

        # Login (dev mode -> mock token)
        login = client.post("/v1/auth/login", json={
            "email": "user@example.com",
            "password": "Password123!"
        })
        assert login.status_code == 200

        # Get current user
        me = client.get("/v1/auth/me")
        assert me.status_code == 200
        me_data = me.json()
        assert "id" in me_data
        assert "email" in me_data

        # Trigger a pipeline run
        with patch("app.api.v1.endpoints.jobs.fetch_all_jobs", return_value=[]), \
             patch("app.api.v1.endpoints.jobs.process_jobs", return_value=0):
            run_resp = client.post("/v1/run-job-finder")
        assert run_resp.status_code == 202
        run_data = run_resp.json()
        assert "run_id" in run_data
        assert run_data["status"] == "queued"
        run_id = run_data["run_id"]

        # Poll status
        status_resp = client.get(f"/v1/runs/{run_id}")
        assert status_resp.status_code == 200
        status_data = status_resp.json()
        assert status_data["run_id"] == run_id
        assert "status" in status_data
        assert "created_at" in status_data

        # Record feedback (job may not exist in DB, recorded=False is ok)
        fb = client.post("/v1/feedback", json={"job_id": "job-001", "action": "viewed"})
        assert fb.status_code == 200
        assert "recorded" in fb.json()

    def test_unknown_run_returns_404(self, client):
        assert client.get("/v1/runs/definitely-not-real").status_code == 404

    def test_invalid_action_returns_422(self, client):
        resp = client.post("/v1/feedback", json={"job_id": "j1", "action": "deleted"})
        assert resp.status_code == 422

    def test_register_invalid_email_returns_422(self, client):
        resp = client.post("/v1/auth/register", json={"email": "bad", "password": "Password123!"})
        assert resp.status_code == 422


class TestMigrationFilesContent:
    def test_all_four_migrations_present(self):
        from storage.migrations.runner import MigrationRunner
        runner = MigrationRunner(MIGRATIONS_DIR)
        migrations = runner.load_migrations()
        assert len(migrations) == 5

    def test_001_creates_all_core_tables(self):
        sql = (MIGRATIONS_DIR / "001_initial_schema.sql").read_text()
        for table in ("jobs", "job_skills", "missing_skills", "job_hashes",
                      "user_interactions", "job_runs"):
            assert table in sql, f"Migration 001 missing table: {table}"

    def test_002_adds_user_id_to_three_tables(self):
        sql = (MIGRATIONS_DIR / "002_add_users.sql").read_text()
        assert sql.count("user_id") >= 3

    def test_003_uses_vector_1024_dimensions(self):
        sql = (MIGRATIONS_DIR / "003_pgvector.sql").read_text()
        assert "VECTOR(1024)" in sql or "vector(1024)" in sql.lower()

    def test_004_creates_policies_for_three_tables(self):
        sql = (MIGRATIONS_DIR / "004_rls_policies.sql").read_text()
        assert sql.upper().count("CREATE POLICY") >= 3

    def test_migration_runner_loads_in_order(self):
        from storage.migrations.runner import MigrationRunner
        runner = MigrationRunner(MIGRATIONS_DIR)
        migrations = runner.load_migrations()
        names = [m[0] for m in migrations]
        assert names == sorted(names)


class TestPhase3NewAPIEndpoints:
    def test_auth_register_endpoint_exists(self, client):
        """Endpoint is reachable (not 404/405)."""
        resp = client.post("/v1/auth/register", json={"email": "a@b.com", "password": "12345678"})
        assert resp.status_code != 404
        assert resp.status_code != 405

    def test_auth_login_endpoint_exists(self, client):
        resp = client.post("/v1/auth/login", json={"email": "a@b.com", "password": "12345678"})
        assert resp.status_code != 404
        assert resp.status_code != 405

    def test_auth_me_endpoint_exists(self, client):
        resp = client.get("/v1/auth/me")
        assert resp.status_code != 404
        assert resp.status_code != 405
