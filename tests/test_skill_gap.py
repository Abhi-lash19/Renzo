import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pytest
from intelligence.skill_gap import compute_skill_gap


def _make_job(transferable=None):
    from pipeline.models import Job
    from datetime import datetime
    job = Job(
        job_id="sg_001", title="Dev", company="Co",
        location="Remote", description="python flask",
        url="https://example.com/sg/1", source="test",
        posted_at=datetime.utcnow(), fetched_at=datetime.utcnow(),
    )
    job.match_data = {
        "matched_skills": ["python"],
        "missing_skills": ["flask"],
        "transferable_skills": transferable if transferable is not None else [],
    }
    return job


class TestComputeSkillGap:
    def test_returns_dict(self):
        assert isinstance(compute_skill_gap(_make_job(), {}), dict)

    def test_has_matched_skills_key(self):
        assert "matched_skills" in compute_skill_gap(_make_job(), {})

    def test_has_missing_skills_key(self):
        assert "missing_skills" in compute_skill_gap(_make_job(), {})

    def test_has_transferable_skills_key(self):
        assert "transferable_skills" in compute_skill_gap(_make_job(), {})

    def test_transferable_skills_from_match_data(self):
        ts = [{"profile_skill": "fastapi", "job_skill": "flask", "confidence": 0.9}]
        result = compute_skill_gap(_make_job(transferable=ts), {})
        assert result["transferable_skills"] == ts

    def test_empty_transferable_returns_empty_list(self):
        result = compute_skill_gap(_make_job(transferable=[]), {})
        assert result["transferable_skills"] == []

    def test_no_match_data_returns_empty_transferable(self):
        from pipeline.models import Job
        from datetime import datetime
        job = Job(
            job_id="sg_nmd", title="Dev", company="Co",
            location="Remote", description="python",
            url="https://example.com/sg/nmd", source="test",
            posted_at=datetime.utcnow(), fetched_at=datetime.utcnow(),
        )
        # No match_data set
        result = compute_skill_gap(job, {})
        assert "transferable_skills" in result
        assert result["transferable_skills"] == []
