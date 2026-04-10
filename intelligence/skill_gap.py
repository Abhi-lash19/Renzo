from typing import Dict, List
from utils.text_utils import normalize_text
from utils.logger import get_logger

logger = get_logger(__name__)


def compute_skill_gap(job, profile: Dict[str, List[str]]) -> Dict[str, List[str]]:
    """Compute job-required skills matched by profile skills, with missing items highlighted."""
    job_required = {
        normalize_text(skill): skill
        for skill in (job.skills or [])
        if skill and skill.strip()
    }

    profile_skills = {
        normalize_text(skill)
        for skill in profile.get("core_skills", []) + profile.get("secondary_skills", [])
        if skill and skill.strip()
    }

    matched = [original for normalized, original in job_required.items() if normalized in profile_skills]
    missing = [original for normalized, original in job_required.items() if normalized not in profile_skills]

    job.missing_skills = sorted(missing)
    logger.debug(
        f"[SKILL_GAP] job_id={getattr(job, 'job_id', 'unknown')} "
        f"matched_count={len(matched)} missing_count={len(missing)}"
    )

    return {
        "matched_skills": sorted(matched),
        "missing_skills": sorted(missing),
    }
