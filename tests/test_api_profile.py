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

    db_file = tmp_path / "test_profile_api.db"
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


SAMPLE_TXT_RESUME = b"""
John Smith
john@example.com | Bengaluru, India

SUMMARY
Backend developer with 3 years of experience in Python and AWS.

TECHNICAL SKILLS
Python, FastAPI, AWS Lambda, Docker, PostgreSQL, Kubernetes, Terraform, REST APIs, Microservices

WORK EXPERIENCE
Software Engineer at TechCorp (2021-2024)
- Built event-driven microservices using Python and AWS SQS
- Designed REST APIs using FastAPI and PostgreSQL

PROJECTS
Real-time Analytics Dashboard
- Built using Python, Kafka, and PostgreSQL
"""


class TestUploadResume:
    def test_upload_txt_returns_201(self, client):
        response = client.post(
            "/v1/profile/upload",
            files={"file": ("resume.txt", SAMPLE_TXT_RESUME, "text/plain")},
        )
        assert response.status_code == 201

    def test_upload_returns_profile_shape(self, client):
        response = client.post(
            "/v1/profile/upload",
            files={"file": ("resume.txt", SAMPLE_TXT_RESUME, "text/plain")},
        )
        data = response.json()
        assert "core_skills" in data
        assert "secondary_skills" in data
        assert "weighted_skills" in data
        assert "all_skills" in data

    def test_upload_extracts_python_skill(self, client):
        response = client.post(
            "/v1/profile/upload",
            files={"file": ("resume.txt", SAMPLE_TXT_RESUME, "text/plain")},
        )
        data = response.json()
        all_skills = data.get("core_skills", []) + data.get("secondary_skills", [])
        assert any("python" in s.lower() for s in all_skills)

    def test_upload_source_is_upload(self, client):
        response = client.post(
            "/v1/profile/upload",
            files={"file": ("resume.txt", SAMPLE_TXT_RESUME, "text/plain")},
        )
        assert response.json()["source"] == "upload"

    def test_upload_empty_file_returns_400(self, client):
        response = client.post(
            "/v1/profile/upload",
            files={"file": ("resume.txt", b"", "text/plain")},
        )
        assert response.status_code == 400

    def test_upload_unsupported_format_returns_422(self, client):
        response = client.post(
            "/v1/profile/upload",
            files={"file": ("resume.xlsx", b"fake xlsx data", "application/vnd.ms-excel")},
        )
        assert response.status_code == 422

    def test_upload_weighted_skills_are_floats(self, client):
        response = client.post(
            "/v1/profile/upload",
            files={"file": ("resume.txt", SAMPLE_TXT_RESUME, "text/plain")},
        )
        ws = response.json().get("weighted_skills", {})
        for k, v in ws.items():
            assert isinstance(v, float), f"Weight for {k!r} should be float, got {type(v)}"


class TestGetProfile:
    def test_get_returns_200(self, client):
        assert client.get("/v1/profile/").status_code == 200

    def test_get_returns_profile_shape(self, client):
        data = client.get("/v1/profile/").json()
        assert "core_skills" in data
        assert "weighted_skills" in data

    def test_get_after_upload_returns_stored_profile(self, client):
        client.post(
            "/v1/profile/upload",
            files={"file": ("resume.txt", SAMPLE_TXT_RESUME, "text/plain")},
        )
        response = client.get("/v1/profile/")
        assert response.status_code == 200
        data = response.json()
        # Source should be "upload" (not "file" fallback)
        assert data.get("source") == "upload"


class TestUpdateProfile:
    def test_put_returns_200(self, client):
        response = client.put(
            "/v1/profile/",
            json={"core_skills": ["python", "aws", "fastapi"]},
        )
        assert response.status_code == 200

    def test_put_source_is_manual(self, client):
        response = client.put("/v1/profile/", json={"name": "Test User"})
        assert response.json()["source"] == "manual"

    def test_put_preserves_unupdated_fields(self, client):
        client.post(
            "/v1/profile/upload",
            files={"file": ("resume.txt", SAMPLE_TXT_RESUME, "text/plain")},
        )
        response = client.put("/v1/profile/", json={"name": "Updated Name"})
        data = response.json()
        assert data["name"] == "Updated Name"
        assert len(data.get("core_skills", [])) > 0

    def test_put_recomputes_all_skills(self, client):
        response = client.put(
            "/v1/profile/",
            json={"core_skills": ["python", "rust"], "secondary_skills": ["figma"]},
        )
        data = response.json()
        all_set = set(data.get("all_skills", []))
        assert len(all_set) > 0


class TestProfileEndpointsDevMode:
    def test_upload_not_401(self, client):
        response = client.post(
            "/v1/profile/upload",
            files={"file": ("resume.txt", SAMPLE_TXT_RESUME, "text/plain")},
        )
        assert response.status_code != 401

    def test_get_not_401(self, client):
        assert client.get("/v1/profile/").status_code != 401

    def test_put_not_401(self, client):
        assert client.put("/v1/profile/", json={"name": "Test"}).status_code != 401
