import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pytest
import importlib
from fastapi.testclient import TestClient
from resume.parser import parse_resume
from resume.builder import build_profile


FULL_RESUME = """
Jane Developer
jane@example.com | github.com/janedeveloper | Remote

SUMMARY
Backend engineer with 3 years building distributed systems and cloud infrastructure.

TECHNICAL SKILLS
Python, FastAPI, AWS Lambda, S3, SQS, SNS, Docker, Kubernetes, PostgreSQL, Redis,
Terraform, GitHub Actions, REST APIs, Microservices, Event-driven Architecture

WORK EXPERIENCE
Backend Engineer — CloudScale (2021–2024)
- Architected event-driven microservices handling 1M+ events/day using Python and AWS SQS
- Built REST APIs with FastAPI and PostgreSQL serving 500K daily requests
- Reduced infrastructure costs 40% via Terraform automation

PROJECTS
Job Intelligence System
- Built a job matching engine using Python, FastAPI, and PostgreSQL
- Deployed on AWS ECS with Docker and GitHub Actions CI/CD

EDUCATION
B.Tech Computer Science — NIT Warangal (2017–2021)
"""


class TestFullExtractionPipeline:
    def test_end_to_end_text_to_profile(self):
        parsed = parse_resume(FULL_RESUME)
        profile = build_profile(parsed)
        assert isinstance(profile, dict)
        assert len(profile["core_skills"]) > 0
        assert len(profile["all_skills"]) > 0
        assert len(profile["weighted_skills"]) > 0
        assert all(isinstance(v, float) for v in profile["weighted_skills"].values())

    def test_profile_compatible_with_build_match_data(self):
        from utils.matching_engine import build_match_data
        from pipeline.models import Job
        from datetime import datetime

        profile = build_profile(parse_resume(FULL_RESUME))
        job = Job(
            job_id="test_ph4_001",
            title="Backend Engineer",
            company="TestCo",
            location="Remote",
            description="Python, FastAPI, AWS Lambda, Microservices",
            url="https://example.com/job/ph4",
            source="test",
            posted_at=datetime.utcnow(),
            fetched_at=datetime.utcnow(),
        )
        match_data = build_match_data(job, profile)
        assert isinstance(match_data, dict)
        assert "skill_score_raw" in match_data

    def test_python_in_core_skills(self):
        profile = build_profile(parse_resume(FULL_RESUME))
        core_lower = [s.lower() for s in profile["core_skills"]]
        assert any("python" in s for s in core_lower)

    def test_experience_level_is_valid(self):
        profile = build_profile(parse_resume(FULL_RESUME))
        assert profile["experience_level"] in ("0-2 years", "2-5 years", "5+ years")

    def test_weighted_skills_values_in_range(self):
        profile = build_profile(parse_resume(FULL_RESUME))
        for skill, weight in profile["weighted_skills"].items():
            assert 0.0 <= weight <= 1.0, f"{skill}: {weight}"


@pytest.fixture
def api_client(tmp_path, monkeypatch):
    import storage.db as db_module
    import storage.db_manager as dm
    import app.auth as auth_module
    import config.settings as settings_module

    db_file = tmp_path / "test_phase4_int.db"
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


class TestPhase4APIFlow:
    def test_upload_get_update_flow(self, api_client):
        """Upload -> GET profile -> PUT update -> verify."""
        upload = api_client.post(
            "/v1/profile/upload",
            files={"file": ("resume.txt", FULL_RESUME.encode(), "text/plain")},
        )
        assert upload.status_code == 201
        assert len(upload.json()["core_skills"]) > 0

        get_resp = api_client.get("/v1/profile/")
        assert get_resp.status_code == 200
        assert get_resp.json()["source"] == "upload"

        update_resp = api_client.put(
            "/v1/profile/",
            json={"name": "Jane Developer", "exclude_keywords": ["frontend", "mobile"]},
        )
        assert update_resp.status_code == 200
        data = update_resp.json()
        assert data["name"] == "Jane Developer"
        assert "frontend" in data["exclude_keywords"]

    def test_get_without_upload_falls_back_to_file(self, api_client):
        """Without an upload, GET profile returns file-based profile."""
        resp = api_client.get("/v1/profile/")
        assert resp.status_code == 200
        # File profile has source="file"
        assert resp.json()["source"] == "file"
