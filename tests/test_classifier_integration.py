import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pytest
from pipeline.classifier import classify_job, TRANSFERABLE_BOOST_MIN, STRETCH_THRESHOLD


class TestClassifierWithTransferableCount:
    def test_transferable_promotes_learning_to_stretch(self):
        result = classify_job(score=3.0, transferable_count=TRANSFERABLE_BOOST_MIN)
        assert result == "stretch"

    def test_one_transferable_does_not_promote(self):
        result = classify_job(score=3.0, transferable_count=1)
        assert result == "learning"

    def test_zero_transferable_stays_learning(self):
        result = classify_job(score=3.0, transferable_count=0)
        assert result == "learning"

    def test_strong_score_not_affected_by_transferable(self):
        result = classify_job(score=7.5, transferable_count=0)
        assert result == "strong"

    def test_stretch_score_unaffected(self):
        result = classify_job(score=5.5, transferable_count=0)
        assert result == "stretch"


class TestEnrichJobsUsesRealTransferableCount:
    """verify enrich_jobs() passes transferable_count from match_data to classify_job."""

    def test_low_score_job_with_transferable_gets_stretch(self):
        from pipeline.models import Job
        from pipeline.orchestrator import enrich_jobs
        from datetime import datetime

        job = Job(
            job_id="tc_001", title="Flask Dev", company="TestCo",
            location="Remote",
            description="We need flask and sqlite experience for backend development",
            url="https://example.com/tc001", source="test",
            posted_at=datetime.utcnow(), fetched_at=datetime.utcnow(),
        )
        job.score = 3.5  # below STRETCH_THRESHOLD (5.0)
        job.match_data = {
            "matched_skills": ["fastapi"],
            "missing_skills": ["flask"],
            "skill_score_raw": 0.5,
            "skill_max_score": 1.0,
            "skill_overlap": 1,
            "role_match_score": 0.0,
            "excluded": False,
            "matched_keywords": [],
            "keyword_score": 0.0,
            "role_match": False,
            "title_match": 0.0,
            "experience_match": 1.0,
            "normalized_skills": ["fastapi"],
            "rejected_skills": {},
            "weight_assignment": {"fastapi": 1.0},
            "skill_alias_matches": {},
            "profile_skill_map": {},
            "matched_preferred_keywords": [],
            "matched_bonus_keywords": [],
            "keyword_max_score": 0.0,
            "keyword_weights": {},
            "recency_score": 1.0,
            "bonus_score": 0.0,
            "bonus_max_score": 2.0,
            "focus_boost": 0,
            "learning_score": 0.0,
            "preferred_skill_hits": [],
            "ignored_skill_hits": [],
            "preferred_company_match": False,
            "preferred_role_match": False,
            "similarity_to_applied": 0.0,
            "learning_breakdown": {},
            # Phase 6 keys — 2 transferable skills should promote to "stretch"
            "transferable_skills": [
                {"profile_skill": "fastapi", "job_skill": "flask", "confidence": 0.9},
                {"profile_skill": "postgresql", "job_skill": "sqlite", "confidence": 0.8},
            ],
            "transferable_count": 2,
        }
        job.skills = ["fastapi"]
        job.missing_skills = ["flask"]
        job.score_breakdown = {}

        profile = {
            "core_skills": ["fastapi", "postgresql"],
            "secondary_skills": [],
            "all_skills": ["fastapi", "postgresql"],
            "weighted_skills": {"fastapi": 1.0, "postgresql": 0.6},
            "cloud": [], "devops": [],
            "preferred_roles": ["backend"],
            "target_roles": ["backend"],
            "exclude_keywords": [],
            "bonus_keywords": [],
            "preferred_keywords": [],
            "projects": [], "experience": [],
            "location": "remote", "remote_preferred": True,
        }

        enriched, _ = enrich_jobs([job], profile)
        assert len(enriched) == 1
        assert enriched[0].match_type == "stretch", (
            f"Expected 'stretch' but got '{enriched[0].match_type}'. "
            "Check enrich_jobs() reads transferable_count from match_data."
        )

    def test_low_score_job_without_transferable_stays_learning(self):
        from pipeline.models import Job
        from pipeline.orchestrator import enrich_jobs
        from datetime import datetime

        job = Job(
            job_id="tc_002", title="Unrelated Dev", company="TestCo",
            location="Remote", description="We need cobol and fortran experience",
            url="https://example.com/tc002", source="test",
            posted_at=datetime.utcnow(), fetched_at=datetime.utcnow(),
        )
        job.score = 2.0
        job.match_data = {
            "matched_skills": ["python"],
            "missing_skills": ["cobol"],
            "skill_score_raw": 0.1,
            "skill_max_score": 1.0,
            "skill_overlap": 1,
            "role_match_score": 0.0,
            "excluded": False,
            "matched_keywords": [],
            "keyword_score": 0.0,
            "role_match": False,
            "title_match": 0.0,
            "experience_match": 1.0,
            "normalized_skills": ["python"],
            "rejected_skills": {},
            "weight_assignment": {"python": 1.0},
            "skill_alias_matches": {},
            "profile_skill_map": {},
            "matched_preferred_keywords": [],
            "matched_bonus_keywords": [],
            "keyword_max_score": 0.0,
            "keyword_weights": {},
            "recency_score": 1.0,
            "bonus_score": 0.0,
            "bonus_max_score": 2.0,
            "focus_boost": 0,
            "learning_score": 0.0,
            "preferred_skill_hits": [],
            "ignored_skill_hits": [],
            "preferred_company_match": False,
            "preferred_role_match": False,
            "similarity_to_applied": 0.0,
            "learning_breakdown": {},
            "transferable_skills": [],
            "transferable_count": 0,
        }
        job.skills = ["python"]
        job.missing_skills = ["cobol"]
        job.score_breakdown = {}

        profile = {
            "core_skills": ["python"],
            "secondary_skills": [],
            "all_skills": ["python"],
            "weighted_skills": {"python": 1.0},
            "cloud": [], "devops": [],
            "preferred_roles": ["developer"],
            "target_roles": ["developer"],
            "exclude_keywords": [],
            "bonus_keywords": [],
            "preferred_keywords": [],
            "projects": [], "experience": [],
            "location": "remote", "remote_preferred": True,
        }

        enriched, _ = enrich_jobs([job], profile)
        assert len(enriched) == 1
        assert enriched[0].match_type == "learning"
