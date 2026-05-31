import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pytest
from datetime import datetime
from pipeline.models import Job
from utils.matching_engine import build_match_data
from pipeline.classifier import classify_job, STRONG_THRESHOLD, STRETCH_THRESHOLD
from intelligence.skill_adjacency import get_transferable_skills, SKILL_GRAPH
from intelligence.skill_gap import compute_skill_gap


def _make_job(job_id, title, description):
    return Job(
        job_id=job_id, title=title, company="TestCo",
        location="Remote", description=description,
        url=f"https://example.com/{job_id}", source="test",
        posted_at=datetime.utcnow(), fetched_at=datetime.utcnow(),
    )


def _make_profile(core_skills):
    return {
        "core_skills": core_skills,
        "secondary_skills": [],
        "all_skills": core_skills,
        "weighted_skills": {s: 1.0 for s in core_skills},
        "cloud": [], "devops": [],
        "preferred_roles": ["backend developer"],
        "target_roles": ["backend", "developer"],
        "exclude_keywords": [],
        "bonus_keywords": [],
        "preferred_keywords": [],
        "projects": [], "experience": [],
        "location": "remote", "remote_preferred": True,
    }


class TestTransferableSkillsEndToEnd:
    def test_fastapi_user_flask_job_has_transferable(self):
        """User with fastapi, job requires flask — should find transferable skill."""
        job = _make_job("e2e_001", "Flask Backend Dev",
                        "We use flask, postgresql, and redis for our backend API")
        profile = _make_profile(["fastapi", "postgresql"])
        match_data = build_match_data(job, profile)

        assert "transferable_skills" in match_data
        transferable_pairs = [(t["profile_skill"], t["job_skill"])
                              for t in match_data["transferable_skills"]]
        # With the fix: flask is in the job text and in SKILL_GRAPH, and fastapi→flask
        # is a known adjacency, so the transfer should always be detected regardless
        # of whether flask appears in missing_skills.
        assert ("fastapi", "flask") in transferable_pairs, (
            f"Expected ('fastapi', 'flask') in transferable_pairs, got: {transferable_pairs}. "
            f"missing_skills={match_data.get('missing_skills', [])}"
        )

    def test_transferable_count_is_int(self):
        job = _make_job("e2e_002", "Flask Dev",
                        "Looking for flask developer with REST API experience")
        profile = _make_profile(["fastapi"])
        match_data = build_match_data(job, profile)
        assert isinstance(match_data["transferable_count"], int)

    def test_aws_user_gcp_job(self):
        job = _make_job("e2e_003", "GCP Cloud Engineer",
                        "Cloud engineer with gcp experience needed for cloud infrastructure work")
        profile = _make_profile(["aws"])
        match_data = build_match_data(job, profile)
        transferable_pairs = [(t["profile_skill"], t["job_skill"])
                              for t in match_data.get("transferable_skills", [])]
        # With the fix: gcp is in the job text and in SKILL_GRAPH, and aws→gcp is
        # a known adjacency, so the transfer should always be detected.
        assert ("aws", "gcp") in transferable_pairs, (
            f"Expected ('aws', 'gcp') in transferable_pairs, got: {transferable_pairs}. "
            f"missing_skills={match_data.get('missing_skills', [])}"
        )

    def test_no_adjacent_gives_zero_transferable(self):
        job = _make_job("e2e_004", "Data Scientist",
                        "Looking for pandas numpy data scientist machine learning analytics")
        profile = _make_profile(["kubernetes"])
        match_data = build_match_data(job, profile)
        pairs = [(t["profile_skill"], t["job_skill"])
                 for t in match_data.get("transferable_skills", [])]
        assert ("kubernetes", "pandas") not in pairs


class TestClassifierABComparison:
    """A/B: same score, different transferable counts → different classification."""

    def test_no_transferable_learning(self):
        assert classify_job(STRETCH_THRESHOLD - 1.0, transferable_count=0) == "learning"

    def test_with_transferable_stretch(self):
        assert classify_job(STRETCH_THRESHOLD - 1.0, transferable_count=2) == "stretch"

    def test_strong_unaffected(self):
        assert classify_job(STRONG_THRESHOLD, transferable_count=10) == "strong"

    def test_zero_score_many_transferable_is_stretch(self):
        assert classify_job(0.0, transferable_count=5) == "stretch"


class TestSkillGapReportWithTransferable:
    def test_compute_skill_gap_has_transferable_section(self):
        job = _make_job("sg_001", "Flask Backend",
                        "flask postgresql rest api development backend")
        profile = _make_profile(["fastapi", "postgresql"])
        build_match_data(job, profile)  # populates job.match_data
        gap = compute_skill_gap(job, profile)

        assert "transferable_skills" in gap
        assert isinstance(gap["transferable_skills"], list)

    def test_transferable_in_gap_matches_match_data(self):
        job = _make_job("sg_002", "Flask API Dev",
                        "flask rest api backend postgresql database")
        profile = _make_profile(["fastapi"])
        match_data = build_match_data(job, profile)
        gap = compute_skill_gap(job, profile)

        assert len(gap["transferable_skills"]) == len(
            match_data.get("transferable_skills", [])
        )


class TestSkillGraphCoverage:
    def test_common_backend_pairs_present(self):
        important_pairs = [
            ("fastapi", "flask"),
            ("aws", "gcp"),
            ("postgresql", "mysql"),
            ("kafka", "rabbitmq"),
            ("terraform", "pulumi"),
            ("github actions", "gitlab ci"),
            ("kubernetes", "eks"),
            ("docker", "podman"),
        ]
        for ps, js in important_pairs:
            adj = SKILL_GRAPH.get(ps, {})
            assert js in adj, f"Missing adjacency: {ps} → {js}"

    def test_total_confidence_per_skill_reasonable(self):
        for skill, adj in SKILL_GRAPH.items():
            total = sum(adj.values())
            assert total < 15.0, f"Unreasonably high total confidence for {skill!r}: {total}"

    def test_graph_is_large_enough(self):
        assert len(SKILL_GRAPH) >= 50
