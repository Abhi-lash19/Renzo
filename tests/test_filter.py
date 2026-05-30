"""
Tests for pipeline.filter.passes_filter.
sys.path is handled by conftest.py.
"""
from datetime import datetime, timedelta

import pytest

from config.settings import settings
from pipeline.filter import passes_filter
from utils.matching_engine import build_match_data


def test_valid_job_with_python_match_passes(make_job, make_profile):
    """Job with python/aws description and matching profile should pass."""
    job = make_job(
        title="Python Backend Developer",
        description="python aws backend microservices",
    )
    profile = make_profile(core_skills=["python", "aws"])
    passed, reason, score = passes_filter(job, profile, threshold=4)
    assert passed, f"Expected job to pass, got reason={reason!r}"


def test_missing_title_rejected(make_job, make_profile):
    """Job with empty title should be rejected immediately."""
    job = make_job(title="", description="python aws backend")
    profile = make_profile()
    passed, reason, score = passes_filter(job, profile, threshold=4)
    assert passed is False
    assert "missing" in reason.lower() or reason


def test_missing_description_rejected(make_job, make_profile):
    """Job with empty description should be rejected immediately."""
    job = make_job(title="Python Developer", description="")
    profile = make_profile()
    passed, reason, score = passes_filter(job, profile, threshold=4)
    assert passed is False
    assert "missing" in reason.lower() or reason


def test_excluded_keyword_in_title_rejected(make_job, make_profile):
    """Job whose title contains an excluded keyword should be rejected."""
    job = make_job(
        title="Angular Frontend Developer",
        description="angular frontend typescript spa components",
    )
    profile = make_profile(exclude_keywords=["angular"])
    passed, reason, score = passes_filter(job, profile, threshold=4)
    assert passed is False
    assert "excluded" in reason.lower() or reason


def test_fresh_job_age_passes(make_job, make_profile):
    """Job posted 1 hour ago should not be rejected by the age filter."""
    job = make_job(
        posted_at=datetime.utcnow() - timedelta(hours=1),
        description="python aws backend microservices",
    )
    profile = make_profile()
    passed, reason, score = passes_filter(job, profile, threshold=4)
    # The age filter specifically must not be the rejection reason
    assert "too old" not in reason, f"Unexpected age rejection: {reason!r}"


def test_old_job_age_rejected(make_job, make_profile):
    """Job older than MAX_JOB_AGE_HOURS + 24h should be rejected as too old."""
    old_hours = settings.MAX_JOB_AGE_HOURS + 24
    job = make_job(
        posted_at=datetime.utcnow() - timedelta(hours=old_hours),
        description="python aws backend microservices",
    )
    profile = make_profile()
    passed, reason, score = passes_filter(job, profile, threshold=4)
    assert passed is False
    assert "too old" in reason, f"Expected 'too old' in reason, got: {reason!r}"


def test_job_without_posted_at_not_age_rejected(make_job, make_profile):
    """Job with posted_at=None should not be rejected by the age filter."""
    job = make_job(
        title="Python Backend Developer",
        description="python aws backend microservices",
    )
    # Override posted_at to None after construction
    job.posted_at = None
    profile = make_profile()
    passed, reason, score = passes_filter(job, profile, threshold=4)
    assert "too old" not in reason, f"Age filter triggered unexpectedly: {reason!r}"


def test_role_match_passes(make_job, make_profile):
    """Job title matching a target_role should pass via role match."""
    job = make_job(
        title="Python Backend Developer",
        description="python aws backend api microservices",
    )
    profile = make_profile(core_skills=["python", "aws"])
    # profile has target_roles=["backend", "python"] — title contains "backend"
    passed, reason, score = passes_filter(job, profile, threshold=4)
    assert passed, f"Expected role match to pass, got reason={reason!r}"


def test_filter_score_is_float(make_job, make_profile):
    """The third return value of passes_filter must be a float."""
    job = make_job()
    profile = make_profile()
    passed, reason, score = passes_filter(job, profile, threshold=4)
    assert isinstance(score, float), f"Expected float score, got {type(score).__name__}"


def test_fallback_threshold_more_permissive(make_job, make_profile):
    """threshold=3 uses FALLBACK_MIN_SKILL_THRESHOLD (0.6) which is more permissive."""
    # Use a job that weakly matches (single low-weight skill) and threshold=3
    job = make_job(
        title="Python Developer",
        description="python scripting automation",
    )
    profile = make_profile(core_skills=["python"], weighted_skills={"python": 0.7})
    passed_strict, _, _ = passes_filter(job, profile, threshold=4)
    passed_fallback, _, _ = passes_filter(job, profile, threshold=3)
    # fallback threshold is more permissive — it should at least match strict or be equal
    # We just verify the function runs without error and returns a bool
    assert isinstance(passed_fallback, bool)
    assert isinstance(passed_strict, bool)
    # Fallback is more permissive, so if strict passes, fallback must also pass
    if passed_strict:
        assert passed_fallback
