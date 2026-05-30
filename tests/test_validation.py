"""
Tests for utils/validation.py — validate_job and validate_profile.

Tests verify:
  - valid jobs and profiles pass without raising
  - missing url, title, or description on a job raises RenzoValidationError
  - missing or empty core_skills / weighted_skills on profile raises RenzoValidationError
  - all raised exceptions are catchable as RenzoError (base class)
"""

import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import unittest

from core.exceptions import RenzoError, RenzoValidationError
from pipeline.models import Job
from utils.validation import validate_job, validate_profile


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_job(**overrides) -> Job:
    defaults = dict(
        job_id="val_test_001",
        title="Python Backend Developer",
        company="Acme Corp",
        location="Remote",
        description="Build scalable Python microservices with AWS.",
        url="https://example.com/jobs/1",
        source="test",
        posted_at=datetime.utcnow(),
        fetched_at=datetime.utcnow(),
    )
    defaults.update(overrides)
    return Job(**defaults)


def _make_profile(**overrides) -> dict:
    defaults = {
        "core_skills": ["python", "aws"],
        "weighted_skills": {"python": 1.0, "aws": 0.8},
    }
    defaults.update(overrides)
    return defaults


# ---------------------------------------------------------------------------
# validate_job tests
# ---------------------------------------------------------------------------

class TestValidateJob(unittest.TestCase):

    def test_valid_job_passes(self):
        """A fully populated job should not raise any exception."""
        job = _make_job()
        # Should complete without raising
        validate_job(job)

    def test_empty_url_raises(self):
        """url='' should raise RenzoValidationError."""
        job = _make_job(url="")
        with self.assertRaises(RenzoValidationError):
            validate_job(job)

    def test_none_url_raises(self):
        """job.url = None after construction should raise RenzoValidationError."""
        job = _make_job()
        job.url = None
        with self.assertRaises(RenzoValidationError):
            validate_job(job)

    def test_empty_title_raises(self):
        """title='' should raise RenzoValidationError."""
        job = _make_job(title="")
        with self.assertRaises(RenzoValidationError):
            validate_job(job)

    def test_none_title_raises(self):
        """job.title = None after construction should raise RenzoValidationError."""
        job = _make_job()
        job.title = None
        with self.assertRaises(RenzoValidationError):
            validate_job(job)

    def test_empty_description_raises(self):
        """description='' should raise RenzoValidationError."""
        job = _make_job(description="")
        with self.assertRaises(RenzoValidationError):
            validate_job(job)

    def test_exception_is_renzo_error(self):
        """RenzoValidationError must be catchable as its RenzoError superclass."""
        job = _make_job(url="")
        with self.assertRaises(RenzoError):
            validate_job(job)


# ---------------------------------------------------------------------------
# validate_profile tests
# ---------------------------------------------------------------------------

class TestValidateProfile(unittest.TestCase):

    def test_valid_profile_passes(self):
        """A fully populated profile should not raise any exception."""
        profile = _make_profile()
        validate_profile(profile)

    def test_empty_core_skills_raises(self):
        """core_skills=[] should raise RenzoValidationError."""
        profile = _make_profile(core_skills=[])
        with self.assertRaises(RenzoValidationError):
            validate_profile(profile)

    def test_missing_core_skills_key_raises(self):
        """Profile dict without 'core_skills' key should raise RenzoValidationError."""
        profile = _make_profile()
        del profile["core_skills"]
        with self.assertRaises(RenzoValidationError):
            validate_profile(profile)

    def test_empty_weighted_skills_raises(self):
        """weighted_skills={} should raise RenzoValidationError."""
        profile = _make_profile(weighted_skills={})
        with self.assertRaises(RenzoValidationError):
            validate_profile(profile)

    def test_profile_exception_is_renzo_error(self):
        """RenzoValidationError from profile validation must be catchable as RenzoError."""
        profile = _make_profile(core_skills=[])
        with self.assertRaises(RenzoError):
            validate_profile(profile)


if __name__ == "__main__":
    unittest.main()
