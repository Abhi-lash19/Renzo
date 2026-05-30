"""
Pytest configuration: sys.path setup and shared fixtures.
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pytest
from datetime import datetime
from pipeline.models import Job


@pytest.fixture
def make_job():
    """Factory fixture for creating Job test objects."""
    def _make(
        title="Python Backend Developer",
        description="python aws backend microservices",
        url="https://example.com/job/1",
        job_id="test_001",
        posted_at=None,
        source="test",
        company="TestCo",
        location="Remote",
    ):
        return Job(
            job_id=job_id,
            title=title,
            company=company,
            location=location,
            description=description,
            url=url,
            source=source,
            posted_at=posted_at or datetime.utcnow(),
            fetched_at=datetime.utcnow(),
        )
    return _make


@pytest.fixture
def make_profile():
    """Factory fixture for creating profile dicts."""
    def _make(core_skills=None, exclude_keywords=None, weighted_skills=None):
        core = core_skills or ["python", "aws"]
        return {
            "core_skills": core,
            "secondary_skills": ["docker"],
            "preferred_roles": ["backend developer"],
            "target_roles": ["backend", "python"],
            "exclude_keywords": exclude_keywords or ["angular", "frontend"],
            "bonus_keywords": ["startup", "remote"],
            "preferred_keywords": ["backend", "api"],
            "projects": [],
            "experience": [],
            "location": "remote",
            "weighted_skills": weighted_skills or {s: 1.0 for s in core},
        }
    return _make
