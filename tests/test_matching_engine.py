"""
Tests for utils/matching_engine.py — the core skill matching and scoring-input engine.

Each test is self-contained: it builds a minimal Job and profile, calls
build_match_data(), and asserts on the returned dict and/or the job's mutated
fields.  No network calls, no database.
"""

import sys
from datetime import datetime, timedelta
from pathlib import Path

# Ensure project root is importable when running pytest from any directory.
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import unittest

from pipeline.models import Job
from utils.matching_engine import build_match_data


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_job(
    title: str = "Python Backend Developer",
    company: str = "Acme Corp",
    description: str = "We need python and aws experience.",
    source: str = "test",
    posted_at: datetime | None = None,
    url: str = "https://example.com/job/1",
    job_id: str = "test_001",
) -> Job:
    return Job(
        job_id=job_id,
        title=title,
        company=company,
        location="Remote",
        description=description,
        url=url,
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
        "core_skills": core_skills if core_skills is not None else ["python", "aws"],
        "secondary_skills": secondary_skills if secondary_skills is not None else ["docker"],
        "preferred_roles": ["backend developer"],
        "target_roles": ["backend", "python"],
        "exclude_keywords": exclude_keywords if exclude_keywords is not None else ["angular", "frontend"],
        "bonus_keywords": ["startup", "remote"],
        "preferred_keywords": ["backend", "api"],
        "projects": [],
        "experience": [],
        "location": "remote",
        "weighted_skills": {"python": 1.0, "aws": 1.0, "docker": 0.6},
    }


# ---------------------------------------------------------------------------
# Test cases
# ---------------------------------------------------------------------------

class TestMatchDataStructure(unittest.TestCase):
    """build_match_data() must always return a dict with the required keys."""

    REQUIRED_KEYS = {
        "matched_skills",
        "missing_skills",
        "skill_score_raw",
        "skill_max_score",
        "keyword_score",
        "role_match_score",
        "recency_score",
        "excluded",
        "normalized_skills",
        "learning_score",
    }

    def test_all_required_keys_present(self):
        job = _make_job()
        profile = _make_profile()
        result = build_match_data(job, profile)
        for key in self.REQUIRED_KEYS:
            self.assertIn(key, result, f"Key '{key}' missing from match_data")

    def test_returns_dict(self):
        result = build_match_data(_make_job(), _make_profile())
        self.assertIsInstance(result, dict)

    def test_matched_and_missing_are_disjoint(self):
        result = build_match_data(_make_job(), _make_profile())
        matched = set(result["matched_skills"])
        missing = set(result["missing_skills"])
        self.assertTrue(
            matched.isdisjoint(missing),
            f"matched and missing overlap: {matched & missing}",
        )

    def test_matched_plus_missing_equals_all_normalized(self):
        result = build_match_data(_make_job(), _make_profile())
        matched = set(result["matched_skills"])
        missing = set(result["missing_skills"])
        normalized = set(result["normalized_skills"])
        self.assertEqual(matched | missing, normalized)


class TestSkillMatching(unittest.TestCase):
    """Skill detection in job text."""

    def test_core_skill_detected_in_description(self):
        job = _make_job(description="Strong python and aws backend skills required.")
        result = build_match_data(job, _make_profile())
        self.assertIn("python", result["matched_skills"])
        self.assertIn("aws", result["matched_skills"])

    def test_skill_absent_lands_in_missing(self):
        # Use a neutral title so the skill can only match via description.
        job = _make_job(title="Software Role", description="React frontend position with TypeScript.")
        result = build_match_data(job, _make_profile(core_skills=["python"]))
        self.assertIn("python", result["missing_skills"])
        self.assertNotIn("python", result["matched_skills"])

    def test_synonym_expansion_k8s_matches_kubernetes(self):
        job = _make_job(description="Experience with k8s orchestration required.")
        profile = _make_profile(core_skills=["kubernetes"])
        result = build_match_data(job, profile)
        self.assertIn("kubernetes", result["matched_skills"])

    def test_synonym_expansion_nodejs_variants(self):
        job = _make_job(description="We use node.js and python for backend services.")
        profile = _make_profile(core_skills=["nodejs"])
        result = build_match_data(job, profile)
        self.assertIn("nodejs", result["matched_skills"])

    def test_negative_context_skill_not_matched(self):
        # "no python required" — python appears but in negative context
        job = _make_job(description="No python required. Java and Kotlin only.")
        result = build_match_data(job, _make_profile(core_skills=["python"]))
        # The negative context detection should exclude this match.
        # If it does fire, python lands in missing_skills.
        self.assertNotIn("python", result["matched_skills"])

    def test_skill_in_title_also_matched(self):
        job = _make_job(
            title="Senior Python Engineer",
            description="Backend role with cloud experience.",
        )
        result = build_match_data(job, _make_profile(core_skills=["python"]))
        self.assertIn("python", result["matched_skills"])

    def test_empty_description_no_matched_skills(self):
        # Use a neutral title and empty description — no skills should match.
        job = _make_job(title="Open Position", description="")
        result = build_match_data(job, _make_profile())
        self.assertEqual(result["matched_skills"], [])

    def test_case_insensitive_matching(self):
        job = _make_job(description="PYTHON and AWS are required.")
        result = build_match_data(job, _make_profile())
        self.assertIn("python", result["matched_skills"])
        self.assertIn("aws", result["matched_skills"])


class TestSkillScores(unittest.TestCase):
    """skill_score_raw and skill_max_score semantics."""

    def test_skill_score_raw_positive_when_skills_matched(self):
        job = _make_job(description="python and aws required")
        result = build_match_data(job, _make_profile())
        self.assertGreater(result["skill_score_raw"], 0.0)

    def test_skill_score_raw_zero_when_no_skills_matched(self):
        job = _make_job(
            title="Frontend React Engineer",
            description="React, TypeScript, CSS expertise needed.",
        )
        result = build_match_data(job, _make_profile(core_skills=["python", "aws"]))
        self.assertEqual(result["skill_score_raw"], 0.0)

    def test_skill_max_score_positive_when_profile_has_skills(self):
        result = build_match_data(_make_job(), _make_profile())
        self.assertGreater(result["skill_max_score"], 0.0)

    def test_core_skill_weight_higher_than_secondary(self):
        # A job matching only a core skill should have higher raw score than
        # a job matching only a secondary skill, all else equal.
        # Use neutral titles so skill matching only comes from the description.
        profile = _make_profile(core_skills=["python"], secondary_skills=["docker"])

        job_core = _make_job(title="Open Role", description="python developer needed")
        job_secondary = _make_job(title="Open Role", description="docker containerization experience")

        result_core = build_match_data(job_core, profile)
        result_secondary = build_match_data(job_secondary, profile)

        self.assertGreater(
            result_core["skill_score_raw"],
            result_secondary["skill_score_raw"],
        )


class TestRoleMatching(unittest.TestCase):
    """role_match_score and excluded flag."""

    def test_role_match_score_1_when_target_role_in_title(self):
        job = _make_job(title="Python Backend Developer")
        result = build_match_data(job, _make_profile())
        self.assertEqual(result["role_match_score"], 1.0)

    def test_role_match_score_0_when_no_target_role(self):
        job = _make_job(
            title="Frontend Designer",
            description="Figma, Sketch, and CSS design work.",
        )
        profile = _make_profile()
        # Override to ensure no target role match
        profile["target_roles"] = ["backend", "python", "software engineer"]
        result = build_match_data(job, profile)
        self.assertEqual(result["role_match_score"], 0.0)

    def test_excluded_true_when_exclude_keyword_in_text(self):
        job = _make_job(description="Angular frontend developer with React experience.")
        result = build_match_data(job, _make_profile(exclude_keywords=["angular"]))
        self.assertTrue(result["excluded"])

    def test_excluded_false_for_clean_backend_job(self):
        job = _make_job(description="Python backend microservices with AWS.")
        result = build_match_data(job, _make_profile())
        self.assertFalse(result["excluded"])


class TestRecencyScore(unittest.TestCase):
    """recency_score tiers from matching engine."""

    def test_fresh_job_score_1(self):
        job = _make_job(posted_at=datetime.utcnow() - timedelta(hours=1))
        result = build_match_data(job, _make_profile())
        self.assertEqual(result["recency_score"], 1.0)

    def test_job_under_24h_score_1(self):
        job = _make_job(posted_at=datetime.utcnow() - timedelta(hours=23))
        result = build_match_data(job, _make_profile())
        self.assertEqual(result["recency_score"], 1.0)

    def test_job_25h_old_score_0_7(self):
        job = _make_job(posted_at=datetime.utcnow() - timedelta(hours=25))
        result = build_match_data(job, _make_profile())
        self.assertEqual(result["recency_score"], 0.7)

    def test_job_80h_old_score_0_4(self):
        job = _make_job(posted_at=datetime.utcnow() - timedelta(hours=80))
        result = build_match_data(job, _make_profile())
        self.assertEqual(result["recency_score"], 0.4)

    def test_job_over_168h_score_0(self):
        job = _make_job(posted_at=datetime.utcnow() - timedelta(hours=200))
        result = build_match_data(job, _make_profile())
        self.assertEqual(result["recency_score"], 0.0)

    def test_no_posted_at_treated_as_fresh(self):
        # The engine's _get_job_age_hours() returns 0.0 when posted_at is None,
        # which falls in the <24h tier → recency_score = 1.0 (treated as just posted).
        job = _make_job(posted_at=None)
        job.posted_at = None
        result = build_match_data(job, _make_profile())
        self.assertEqual(result["recency_score"], 1.0)


class TestMatchDataProjectedOntoJob(unittest.TestCase):
    """build_match_data() must project results onto the job object."""

    def test_job_skills_set_after_build(self):
        job = _make_job(description="python and aws backend")
        build_match_data(job, _make_profile())
        self.assertIsInstance(job.skills, list)
        self.assertIn("python", job.skills)

    def test_job_missing_skills_set_after_build(self):
        job = _make_job(description="No relevant skills here at all.")
        profile = _make_profile(core_skills=["python", "aws"])
        build_match_data(job, profile)
        self.assertIsInstance(job.missing_skills, list)
        self.assertTrue(len(job.missing_skills) > 0)

    def test_job_match_data_attribute_set(self):
        job = _make_job()
        build_match_data(job, _make_profile())
        self.assertIsNotNone(job.match_data)
        self.assertIsInstance(job.match_data, dict)


class TestAgeFilter(unittest.TestCase):
    """
    Age filter behaviour in pipeline/filter.py.
    These tests exercise passes_filter() which now enforces MAX_JOB_AGE_HOURS.
    """

    def _passes(self, job, profile=None):
        from pipeline.filter import passes_filter
        p = profile or _make_profile()
        # Ensure match_data is built so the filter can proceed past age check
        build_match_data(job, p)
        passed, reason, _ = passes_filter(job, p)
        return passed, reason

    def test_fresh_job_not_rejected_by_age(self):
        from config.settings import settings
        job = _make_job(
            description="python and aws backend",
            posted_at=datetime.utcnow() - timedelta(hours=1),
        )
        passed, reason = self._passes(job)
        self.assertNotIn("too old", reason)

    def test_old_job_rejected_by_age_filter(self):
        from config.settings import settings
        # Create a job older than MAX_JOB_AGE_HOURS (default 6)
        job = _make_job(
            description="python and aws backend",
            posted_at=datetime.utcnow() - timedelta(hours=settings.MAX_JOB_AGE_HOURS + 24),
        )
        passed, reason = self._passes(job)
        self.assertFalse(passed)
        self.assertIn("too old", reason)

    def test_job_without_posted_at_not_age_filtered(self):
        job = _make_job(description="python and aws backend", posted_at=None)
        job.posted_at = None
        build_match_data(job, _make_profile())
        from pipeline.filter import passes_filter
        # The age filter must NOT fire when posted_at is None.
        # The job may still be rejected on skill grounds but not age.
        passed, reason, _ = passes_filter(job, _make_profile())
        self.assertNotIn("too old", reason)


class TestTransferableSkillsInMatchData:
    """Verify transferable_skills and transferable_count appear in build_match_data output."""

    def _make_profile(self, core_skills):
        return {
            "core_skills": core_skills,
            "secondary_skills": [],
            "all_skills": core_skills,
            "weighted_skills": {s: 1.0 for s in core_skills},
            "cloud": [], "devops": [],
            "preferred_roles": ["backend developer"],
            "target_roles": ["backend"],
            "exclude_keywords": [],
            "bonus_keywords": [],
            "preferred_keywords": [],
            "projects": [], "experience": [],
            "location": "remote", "remote_preferred": True,
        }

    def test_transferable_skills_key_in_match_data(self):
        from pipeline.models import Job
        from datetime import datetime
        job = Job(
            job_id="ts_001", title="Backend Dev", company="Co",
            location="Remote", description="We use flask and postgresql",
            url="https://example.com/ts001", source="test",
            posted_at=datetime.utcnow(), fetched_at=datetime.utcnow(),
        )
        match_data = build_match_data(job, self._make_profile(["fastapi", "postgresql"]))
        assert "transferable_skills" in match_data

    def test_transferable_count_key_in_match_data(self):
        from pipeline.models import Job
        from datetime import datetime
        job = Job(
            job_id="ts_002", title="Flask Dev", company="Co",
            location="Remote", description="flask rest api postgresql",
            url="https://example.com/ts002", source="test",
            posted_at=datetime.utcnow(), fetched_at=datetime.utcnow(),
        )
        match_data = build_match_data(job, self._make_profile(["fastapi"]))
        assert "transferable_count" in match_data
        assert isinstance(match_data["transferable_count"], int)
        assert match_data["transferable_count"] >= 0

    def test_transferable_skills_is_list(self):
        from pipeline.models import Job
        from datetime import datetime
        job = Job(
            job_id="ts_003", title="Dev", company="Co",
            location="Remote", description="python aws backend",
            url="https://example.com/ts003", source="test",
            posted_at=datetime.utcnow(), fetched_at=datetime.utcnow(),
        )
        match_data = build_match_data(job, self._make_profile(["fastapi"]))
        assert isinstance(match_data["transferable_skills"], list)

    def test_fastapi_user_flask_job_finds_transferable(self):
        from pipeline.models import Job
        from datetime import datetime
        job = Job(
            job_id="ts_004", title="Flask Backend Dev", company="Co",
            location="Remote",
            description="We use flask for our backend REST API development and postgresql",
            url="https://example.com/ts004", source="test",
            posted_at=datetime.utcnow(), fetched_at=datetime.utcnow(),
        )
        match_data = build_match_data(job, self._make_profile(["fastapi"]))
        if "flask" in match_data.get("missing_skills", []):
            job_skills_bridged = [t["job_skill"] for t in match_data.get("transferable_skills", [])]
            assert "flask" in job_skills_bridged

    def test_transferable_dicts_have_required_keys(self):
        from pipeline.models import Job
        from datetime import datetime
        job = Job(
            job_id="ts_005", title="Flask Dev", company="Co",
            location="Remote", description="flask postgresql backend REST api",
            url="https://example.com/ts005", source="test",
            posted_at=datetime.utcnow(), fetched_at=datetime.utcnow(),
        )
        match_data = build_match_data(job, self._make_profile(["fastapi"]))
        for t in match_data.get("transferable_skills", []):
            assert "profile_skill" in t
            assert "job_skill" in t
            assert "confidence" in t


if __name__ == "__main__":
    unittest.main()
