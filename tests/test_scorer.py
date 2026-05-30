"""
Tests for pipeline/scorer.py — the weighted scoring engine.

Tests verify:
  - score is always in [0, 10]
  - the weighted formula is applied correctly
  - score_breakdown contains all expected keys
  - missing match_data raises ValueError
  - score is written back onto the job object
  - each individual component (skill, recency, role, keyword, bonus) contributes correctly
"""

import sys
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import unittest

from pipeline.models import Job
from pipeline.scorer import score_job, calculate_skill_score, calculate_recency_score, calculate_role_score, calculate_keyword_score, calculate_bonus_score
from utils.matching_engine import build_match_data


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_job(
    title: str = "Python Backend Developer",
    description: str = "python and aws backend microservices",
    posted_at: datetime | None = None,
    source: str = "test",
) -> Job:
    return Job(
        job_id="score_test_001",
        title=title,
        company="TestCo",
        location="Remote",
        description=description,
        url="https://example.com/job/1",
        source=source,
        posted_at=posted_at if posted_at is not None else datetime.utcnow(),
        fetched_at=datetime.utcnow(),
    )


def _make_profile(
    core_skills: list | None = None,
    secondary_skills: list | None = None,
    exclude_keywords: list | None = None,
) -> dict:
    return {
        "core_skills": core_skills or ["python", "aws"],
        "secondary_skills": secondary_skills or ["docker"],
        "preferred_roles": ["backend developer"],
        "target_roles": ["backend", "python"],
        "exclude_keywords": exclude_keywords if exclude_keywords is not None else ["angular", "frontend"],
        "bonus_keywords": ["startup", "remote"],
        "preferred_keywords": ["backend", "api", "microservices"],
        "projects": [],
        "experience": [],
        "location": "remote",
        "weighted_skills": {"python": 1.0, "aws": 1.0, "docker": 0.6},
    }


def _scored_job(title="Python Backend Developer",
                description="python and aws backend microservices",
                posted_at=None,
                profile=None) -> Job:
    """Build a job with match_data set and score computed."""
    job = _make_job(title=title, description=description, posted_at=posted_at)
    p = profile or _make_profile()
    build_match_data(job, p)
    score_job(job, p)
    return job


# ---------------------------------------------------------------------------
# Test cases
# ---------------------------------------------------------------------------

class TestScoreRange(unittest.TestCase):
    """Final score must always be in [0, 10]."""

    def test_good_match_score_in_range(self):
        job = _scored_job()
        self.assertGreaterEqual(job.score, 0.0)
        self.assertLessEqual(job.score, 10.0)

    def test_no_match_score_in_range(self):
        job = _scored_job(
            title="PHP Wordpress Developer",
            description="PHP, Wordpress, and CSS only.",
        )
        self.assertGreaterEqual(job.score, 0.0)
        self.assertLessEqual(job.score, 10.0)

    def test_perfect_match_score_above_5(self):
        # Fresh job, matching all core skills and target role → should score well
        job = _scored_job(
            description="python aws backend microservices api developer",
            posted_at=datetime.utcnow() - timedelta(minutes=30),
        )
        self.assertGreater(job.score, 5.0)

    def test_irrelevant_old_job_score_near_zero(self):
        job = _scored_job(
            title="PHP Wordpress Developer",
            description="PHP and CSS only, no backend.",
            posted_at=datetime.utcnow() - timedelta(hours=200),
        )
        self.assertLess(job.score, 3.0)


class TestScoreStoredOnJob(unittest.TestCase):
    """score_job() must write the score back onto job.score."""

    def test_score_attribute_updated(self):
        job = _make_job()
        profile = _make_profile()
        build_match_data(job, profile)
        initial_score = job.score
        score_job(job, profile)
        # score_job() should have set a value (0.0 is valid but must be set)
        self.assertIsInstance(job.score, float)

    def test_score_job_returns_float(self):
        job = _make_job()
        profile = _make_profile()
        build_match_data(job, profile)
        result = score_job(job, profile)
        self.assertIsInstance(result, float)

    def test_return_value_matches_job_score(self):
        job = _make_job()
        profile = _make_profile()
        build_match_data(job, profile)
        returned = score_job(job, profile)
        self.assertEqual(returned, job.score)


class TestScoreBreakdown(unittest.TestCase):
    """score_breakdown must be a dict with all expected component keys."""

    EXPECTED_KEYS = {
        "skill_score",
        "recency_score",
        "role_score",
        "keyword_score",
        "bonus_score",
        "learning_score",
        "focus_boost",
        "base_score",
        "skill_score_raw",
        "skill_max_score",
    }

    def test_breakdown_keys_present(self):
        job = _scored_job()
        for key in self.EXPECTED_KEYS:
            self.assertIn(key, job.score_breakdown, f"Key '{key}' missing from score_breakdown")

    def test_breakdown_values_are_numeric(self):
        job = _scored_job()
        for key, value in job.score_breakdown.items():
            self.assertIsInstance(value, (int, float), f"score_breakdown['{key}'] is not numeric")

    def test_breakdown_base_score_in_range(self):
        job = _scored_job()
        self.assertGreaterEqual(job.score_breakdown["base_score"], 0.0)
        self.assertLessEqual(job.score_breakdown["base_score"], 10.0)


class TestMissingMatchDataFallback(unittest.TestCase):
    """
    score_job() catches all internal errors and returns 0.0 to prevent pipeline
    crashes.  When match_data is absent it does NOT propagate the ValueError —
    it logs it and defaults to score=0.0.
    """

    def test_empty_match_data_returns_zero(self):
        job = _make_job()
        job.match_data = {}  # empty dict — _get_match_data guard fires internally
        profile = _make_profile()
        result = score_job(job, profile)
        self.assertEqual(result, 0.0)
        self.assertEqual(job.score, 0.0)

    def test_none_match_data_returns_zero(self):
        job = _make_job()
        job.match_data = None
        profile = _make_profile()
        result = score_job(job, profile)
        self.assertEqual(result, 0.0)
        self.assertEqual(job.score, 0.0)

    def test_fallback_breakdown_has_zero_values(self):
        job = _make_job()
        job.match_data = None
        score_job(job, _make_profile())
        for key in ("skill_score", "recency_score", "role_score", "keyword_score"):
            self.assertEqual(job.score_breakdown.get(key), 0.0, f"Expected 0.0 for {key}")


class TestWeightedFormula(unittest.TestCase):
    """
    Verify scoring components are individually non-negative and are bounded by
    their expected weight in the formula (45/25/15/10/5).

    We cannot test the exact formula output end-to-end because many inputs are
    continuous (recency from age, etc.), but we can verify monotonicity:
    a job matching MORE skills should always score higher than one matching fewer,
    all else equal.
    """

    def test_more_skills_matched_higher_score(self):
        profile = _make_profile(core_skills=["python", "aws", "docker"])

        job_all = _make_job(description="python aws docker backend")
        build_match_data(job_all, profile)
        score_job(job_all, profile)

        job_one = _make_job(description="python only backend")
        build_match_data(job_one, profile)
        score_job(job_one, profile)

        self.assertGreater(job_all.score, job_one.score)

    def test_fresh_job_scores_higher_than_old(self):
        profile = _make_profile()
        desc = "python and aws backend developer"

        job_fresh = _make_job(description=desc, posted_at=datetime.utcnow() - timedelta(hours=1))
        build_match_data(job_fresh, profile)
        score_job(job_fresh, profile)

        job_old = _make_job(description=desc, posted_at=datetime.utcnow() - timedelta(hours=200))
        build_match_data(job_old, profile)
        score_job(job_old, profile)

        self.assertGreater(job_fresh.score, job_old.score)

    def test_title_role_match_increases_score(self):
        profile = _make_profile()

        job_titled = _make_job(
            title="Python Backend Developer",
            description="python and aws",
        )
        build_match_data(job_titled, profile)
        score_job(job_titled, profile)

        job_no_title = _make_job(
            title="Technical Position",
            description="python and aws",
        )
        build_match_data(job_no_title, profile)
        score_job(job_no_title, profile)

        self.assertGreaterEqual(job_titled.score, job_no_title.score)


class TestIndividualComponents(unittest.TestCase):
    """
    Verify each component calculation function returns a value in [0, 1]
    (or [0, 2] for bonus which has BONUS_SCORE_MAX=2).
    """

    def _job_with_match(self, description="python and aws backend") -> Job:
        job = _make_job(description=description)
        build_match_data(job, _make_profile())
        return job

    def test_skill_score_in_unit_interval(self):
        job = self._job_with_match()
        score = calculate_skill_score(job)
        self.assertGreaterEqual(score, 0.0)
        self.assertLessEqual(score, 1.0)

    def test_recency_score_in_unit_interval(self):
        job = self._job_with_match()
        score = calculate_recency_score(job)
        self.assertGreaterEqual(score, 0.0)
        self.assertLessEqual(score, 1.0)

    def test_role_score_in_unit_interval(self):
        job = self._job_with_match()
        score = calculate_role_score(job)
        self.assertGreaterEqual(score, 0.0)
        self.assertLessEqual(score, 1.0)

    def test_keyword_score_in_unit_interval(self):
        job = self._job_with_match()
        score = calculate_keyword_score(job)
        self.assertGreaterEqual(score, 0.0)
        self.assertLessEqual(score, 1.0)

    def test_bonus_score_non_negative(self):
        job = self._job_with_match()
        score = calculate_bonus_score(job)
        self.assertGreaterEqual(score, 0.0)

    def test_all_zeros_for_empty_match_data(self):
        job = _make_job()
        # Manually set a minimal but valid match_data so guard passes
        job.match_data = {
            "skill_score_raw": 0.0,
            "skill_max_score": 0.0,
            "recency_score": 0.0,
            "role_match_score": 0.0,
            "keyword_score": 0.0,
            "keyword_max_score": 0.0,
            "bonus_score": 0.0,
            "bonus_max_score": 2.0,
            "learning_score": 0.0,
            "focus_boost": 0,
            "excluded": False,
            "matched_skills": [],
            "missing_skills": [],
            "normalized_skills": [],
        }
        self.assertEqual(calculate_skill_score(job), 0.0)
        self.assertEqual(calculate_recency_score(job), 0.0)
        self.assertEqual(calculate_role_score(job), 0.0)
        self.assertEqual(calculate_keyword_score(job), 0.0)
        self.assertEqual(calculate_bonus_score(job), 0.0)


class TestExcludedJobRoleScore(unittest.TestCase):
    """Excluded jobs should receive 0.0 for role_score regardless of title."""

    def test_excluded_job_has_zero_role_score(self):
        job = _make_job(
            title="Python Backend Developer",
            description="Angular frontend developer with React.",
        )
        profile = _make_profile(exclude_keywords=["angular"])
        build_match_data(job, profile)
        role_score = calculate_role_score(job)
        self.assertEqual(role_score, 0.0)


if __name__ == "__main__":
    unittest.main()
